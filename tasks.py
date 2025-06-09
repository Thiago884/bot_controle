from datetime import datetime, timedelta
import asyncio
import logging
from main import bot
from discord.ext import tasks
import discord
from io import BytesIO
from typing import Optional
from utils import generate_activity_graph
import time
import random

logger = logging.getLogger('inactivity_bot')

@tasks.loop(hours=24)
async def inactivity_check():
    """Verifica a inatividade dos membros e remove cargos se necessário"""
    await bot.wait_until_ready()
    
    # Verificar rate limit antes de começar
    wait_time = await bot.check_rate_limit('inactivity_check')
    if wait_time == -1:
        logger.warning("Inactivity check cancelado devido a rate limit")
        return
    elif wait_time > 0:
        logger.info(f"Aguardando {wait_time} segundos antes de verificar inatividade")
        await asyncio.sleep(wait_time)
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        logger.info("Nenhum cargo monitorado definido - verificação de inatividade ignorada")
        return
    
    processed_members = 0
    members_with_roles_removed = 0
    batch_size = 50  # Processar membros em lotes para evitar sobrecarga
    
    for guild in bot.guilds:
        try:
            # Obter todos os membros de uma vez (se possível)
            try:
                members = guild.members
            except Exception as e:
                logger.error(f"Erro ao obter membros da guilda {guild.name}: {e}")
                continue
            
            # Processar membros em lotes
            for i in range(0, len(members), batch_size):
                batch = members[i:i + batch_size]
                await process_member_batch(batch, guild, required_minutes, required_days, monitoring_period, tracked_roles)
                
                processed_members += len(batch)
                
                # Pequena pausa entre lotes para evitar rate limits
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erro ao processar guilda {guild.name}: {e}")
            continue
    
    logger.info(f"Verificação de inatividade concluída. Membros processados: {processed_members}, Cargos removidos: {members_with_roles_removed}")

async def process_member_batch(members, guild, required_minutes, required_days, monitoring_period, tracked_roles):
    """Processa um lote de membros para verificação de inatividade"""
    for member in members:
        try:
            # Verificar whitelist usando cache
            cache_key = f"{member.id}_{guild.id}"
            cached_data = await bot.get_cached_user_data(member.id, guild.id)
            
            if cached_data and cached_data.get('whitelisted', False):
                continue
                
            # Verificar se usuário está na whitelist
            if member.id in bot.config['whitelist']['users']:
                await bot.set_cached_user_data(member.id, guild.id, {'whitelisted': True})
                continue
                
            # Verificar se tem cargos whitelisted
            if any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                await bot.set_cached_user_data(member.id, guild.id, {'whitelisted': True})
                continue
                
            # Verificar se tem cargos monitorados
            if not any(role.id in tracked_roles for role in member.roles):
                continue
            
            # Verificar último período verificado (com cache)
            last_check = await get_last_period_check_with_cache(member.id, guild.id)
            now = datetime.now(bot.timezone)
            
            if last_check and last_check['period_end'] and now < last_check['period_end']:
                continue
            
            # Definir período de verificação
            period_end = now
            period_start = period_end - timedelta(days=monitoring_period)
            
            # Obter sessões de voz no período (com cache)
            sessions = await get_voice_sessions_with_cache(member.id, guild.id, period_start, period_end)
            
            # Verificar requisitos
            meets_requirements = False
            valid_days = set()
            
            if sessions:
                for session in sessions:
                    if session['duration'] >= required_minutes * 60:
                        day = session['join_time'].replace(tzinfo=bot.timezone).date()
                        valid_days.add(day)
                
                meets_requirements = len(valid_days) >= required_days
            
            # Registrar verificação
            await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
            
            # Atualizar cache
            await bot.set_cached_user_data(member.id, guild.id, {
                'last_check': datetime.now(bot.timezone),
                'meets_requirements': meets_requirements,
                'valid_days': len(valid_days)
            })
            
            # Ações para quem não cumpriu os requisitos
            if not meets_requirements:
                roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                if roles_to_remove:
                    try:
                        # Verificar rate limit para modificação de cargos
                        wait_time = await bot.check_rate_limit('modify_roles')
                        if wait_time == -1:
                            logger.warning("Pulando remoção de cargos devido a rate limit")
                            continue
                        elif wait_time > 0:
                            await asyncio.sleep(wait_time)
                        
                        await member.remove_roles(*roles_to_remove)
                        await bot.send_warning(member, 'final')
                        await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                        
                        # Gerar relatório gráfico
                        report_file = await generate_activity_report(member, sessions)
                        
                        log_message = (
                            f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                            f"Dias válidos: {len(valid_days)}/{required_days}\n"
                            f"Sessões no período: {len(sessions)}"
                        )
                        
                        if report_file:
                            await bot.log_action(
                                "Cargo Removido",
                                member,
                                log_message,
                                file=report_file
                            )
                        else:
                            await bot.log_action(
                                "Cargo Removido",
                                member,
                                log_message
                            )
                        
                        await bot.notify_roles(
                            f"🚨 Cargos removidos de {member.mention} por inatividade: " +
                            ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                        
                    except discord.Forbidden:
                        await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                    except Exception as e:
                        logger.error(f"Erro ao remover cargos de {member}: {e}")
        
        except Exception as e:
            logger.error(f"Erro ao verificar inatividade para {member}: {e}")

async def get_last_period_check_with_cache(user_id: int, guild_id: int):
    """Obtém a última verificação de período com cache"""
    cached_data = await bot.get_cached_user_data(user_id, guild_id)
    if cached_data and 'last_check' in cached_data:
        return {
            'period_start': cached_data.get('last_check') - timedelta(days=bot.config['monitoring_period']),
            'period_end': cached_data.get('last_check'),
            'meets_requirements': cached_data.get('meets_requirements', False)
        }
    
    # Se não estiver em cache, buscar do banco de dados
    return await bot.db.get_last_period_check(user_id, guild_id)

async def get_voice_sessions_with_cache(user_id: int, guild_id: int, start_date: datetime, end_date: datetime):
    """Obtém sessões de voz com cache"""
    cache_key = f"{user_id}_{guild_id}_sessions"
    cached_data = await bot.get_cached_user_data(user_id, guild_id)
    
    if cached_data and 'sessions' in cached_data:
        cached_sessions = cached_data['sessions']
        # Verificar se as sessões em cache cobrem o período solicitado
        if (cached_sessions and 
            cached_sessions[0]['join_time'] <= start_date and 
            cached_sessions[-1]['leave_time'] >= end_date):
            return [s for s in cached_sessions if start_date <= s['join_time'] <= end_date]
    
    # Se não estiver em cache ou não cobrir o período, buscar do banco de dados
    sessions = await bot.db.get_voice_sessions(user_id, guild_id, start_date, end_date)
    
    # Atualizar cache
    if sessions:
        await bot.set_cached_user_data(user_id, guild_id, {
            'sessions': sessions,
            'last_updated': datetime.now(bot.timezone)
        })
    
    return sessions

@tasks.loop(hours=24)
async def check_warnings():
    """Verifica e envia avisos de inatividade para membros"""
    await bot.wait_until_ready()
    
    # Verificar rate limit antes de começar
    wait_time = await bot.check_rate_limit('check_warnings')
    if wait_time == -1:
        logger.warning("Check warnings cancelado devido a rate limit")
        return
    elif wait_time > 0:
        logger.info(f"Aguardando {wait_time} segundos antes de verificar avisos")
        await asyncio.sleep(wait_time)
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    warnings_config = bot.config.get('warnings', {})
    
    if not tracked_roles or not warnings_config:
        logger.info("Cargos monitorados ou configurações de aviso não definidos - verificação ignorada")
        return
    
    first_warning_days = warnings_config.get('first_warning', 3)
    second_warning_days = warnings_config.get('second_warning', 1)
    
    warnings_sent = {'first': 0, 'second': 0}
    batch_size = 50  # Processar membros em lotes
    
    for guild in bot.guilds:
        try:
            # Obter todos os membros de uma vez (se possível)
            try:
                members = guild.members
            except Exception as e:
                logger.error(f"Erro ao obter membros da guilda {guild.name}: {e}")
                continue
            
            # Processar membros em lotes
            for i in range(0, len(members), batch_size):
                batch = members[i:i + batch_size]
                await process_warning_batch(batch, guild, first_warning_days, second_warning_days, warnings_sent)
                
                # Pequena pausa entre lotes para evitar rate limits
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erro ao processar guilda {guild.name}: {e}")
            continue
    
    logger.info(f"Verificação de avisos concluída. Avisos enviados: Primeiro={warnings_sent['first']}, Segundo={warnings_sent['second']}")

async def process_warning_batch(members, guild, first_warning_days, second_warning_days, warnings_sent):
    """Processa um lote de membros para envio de avisos"""
    for member in members:
        try:
            # Verificar whitelist usando cache
            cached_data = await bot.get_cached_user_data(member.id, guild.id)
            
            if cached_data and cached_data.get('whitelisted', False):
                continue
                
            if member.id in bot.config['whitelist']['users']:
                await bot.set_cached_user_data(member.id, guild.id, {'whitelisted': True})
                continue
                
            if any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                await bot.set_cached_user_data(member.id, guild.id, {'whitelisted': True})
                continue
                
            if not any(role.id in bot.config['tracked_roles'] for role in member.roles):
                continue
            
            # Obter última verificação com cache
            last_check = await get_last_period_check_with_cache(member.id, guild.id)
            if not last_check:
                continue
            
            # Calcular dias restantes
            period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
            days_remaining = (period_end - datetime.now(bot.timezone)).days
            
            # Obter último aviso
            last_warning = await bot.db.get_last_warning(member.id, guild.id)
            
            # Verificar necessidade de avisos
            if days_remaining <= first_warning_days and (
                not last_warning or last_warning[0] != 'first'):
                
                # Verificar rate limit antes de enviar DM
                wait_time = await bot.check_rate_limit('send_dm')
                if wait_time == -1:
                    continue
                elif wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                await bot.send_warning(member, 'first')
                warnings_sent['first'] += 1
            
            elif days_remaining <= second_warning_days and (
                not last_warning or last_warning[0] != 'second'):
                
                # Verificar rate limit antes de enviar DM
                wait_time = await bot.check_rate_limit('send_dm')
                if wait_time == -1:
                    continue
                elif wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                await bot.send_warning(member, 'second')
                warnings_sent['second'] += 1
                
        except Exception as e:
            logger.error(f"Erro ao verificar avisos para {member}: {e}")

@tasks.loop(hours=24)
async def cleanup_members():
    """Remove membros inativos que estão sem cargos há muito tempo"""
    await bot.wait_until_ready()
    
    # Verificar rate limit antes de começar
    wait_time = await bot.check_rate_limit('cleanup_members')
    if wait_time == -1:
        logger.warning("Cleanup members cancelado devido a rate limit")
        return
    elif wait_time > 0:
        logger.info(f"Aguardando {wait_time} segundos antes de limpar membros")
        await asyncio.sleep(wait_time)
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        logger.info("Expulsão de membros inativos desativada na configuração")
        return
    
    cutoff_date = datetime.now(bot.timezone) - timedelta(days=kick_after_days)
    members_kicked = 0
    batch_size = 50  # Processar membros em lotes
    
    for guild in bot.guilds:
        try:
            # Obter todos os membros de uma vez (se possível)
            try:
                members = guild.members
            except Exception as e:
                logger.error(f"Erro ao obter membros da guilda {guild.name}: {e}")
                continue
            
            # Processar membros em lotes
            for i in range(0, len(members), batch_size):
                batch = members[i:i + batch_size]
                await process_kick_batch(batch, guild, cutoff_date, members_kicked)
                
                # Pequena pausa entre lotes para evitar rate limits
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erro ao processar guilda {guild.name}: {e}")
            continue
    
    logger.info(f"Limpeza de membros concluída. Membros expulsos: {members_kicked}")

async def process_kick_batch(members, guild, cutoff_date, members_kicked):
    """Processa um lote de membros para possível expulsão"""
    for member in members:
        try:
            # Verificar whitelist
            if member.id in bot.config['whitelist']['users']:
                continue
                
            # Verificar se tem apenas o cargo @everyone
            if len(member.roles) == 1:
                joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
                if joined_at and joined_at < cutoff_date:
                    try:
                        # Verificar rate limit para kick
                        wait_time = await bot.check_rate_limit('kick_member')
                        if wait_time == -1:
                            logger.warning("Pulando expulsão devido a rate limit")
                            continue
                        elif wait_time > 0:
                            await asyncio.sleep(wait_time)
                        
                        await member.kick(reason=f"Sem cargos há mais de {bot.config['kick_after_days']} dias")
                        await bot.db.log_kicked_member(member.id, guild.id, f"Sem cargos há mais de {bot.config['kick_after_days']} dias")
                        await bot.log_action(
                            "Membro Expulso",
                            member,
                            f"Motivo: Sem cargos há mais de {bot.config['kick_after_days']} dias\n"
                            f"Entrou no servidor em: {joined_at.strftime('%d/%m/%Y')}"
                        )
                        await bot.notify_roles(
                            f"👢 {member.mention} foi expulso por estar sem cargos há mais de {bot.config['kick_after_days']} dias")
                        
                        members_kicked += 1
                        
                    except discord.Forbidden:
                        await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
                    except Exception as e:
                        logger.error(f"Erro ao expulsar membro {member}: {e}")
        except Exception as e:
            logger.error(f"Erro ao verificar membro para expulsão {member}: {e}")

@tasks.loop(hours=24)
async def database_backup():
    """Executa backup diário do banco de dados"""
    await bot.wait_until_ready()
    
    # Verificar rate limit antes de começar
    wait_time = await bot.check_rate_limit('database_backup')
    if wait_time == -1:
        logger.warning("Database backup cancelado devido a rate limit")
        return
    elif wait_time > 0:
        logger.info(f"Aguardando {wait_time} segundos antes de fazer backup")
        await asyncio.sleep(wait_time)
    
    if not hasattr(bot, 'db_backup'):
        from database import DatabaseBackup
        bot.db_backup = DatabaseBackup(bot.db)
    
    try:
        success = await bot.db_backup.create_backup()
        if success:
            await bot.log_action("Backup do Banco de Dados", None, "Backup diário realizado com sucesso")
            logger.info("Backup do banco de dados concluído com sucesso")
        else:
            logger.error("Falha ao criar backup do banco de dados")
    except Exception as e:
        logger.error(f"Erro ao executar backup do banco de dados: {e}")
        await bot.log_action("Erro no Backup", None, f"Falha ao criar backup: {str(e)}")

@tasks.loop(hours=24)
async def cleanup_old_data():
    """Limpa dados antigos do banco de dados"""
    await bot.wait_until_ready()
    
    # Verificar rate limit antes de começar
    wait_time = await bot.check_rate_limit('cleanup_data')
    if wait_time == -1:
        logger.warning("Cleanup old data cancelado devido a rate limit")
        return
    elif wait_time > 0:
        logger.info(f"Aguardando {wait_time} segundos antes de limpar dados antigos")
        await asyncio.sleep(wait_time)
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=60)  # 2 meses
        
        async with bot.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Limpar sessões de voz antigas
                await cursor.execute("DELETE FROM voice_sessions WHERE leave_time < %s", (cutoff_date,))
                voice_deleted = cursor.rowcount
                
                # Limpar avisos antigos
                await cursor.execute("DELETE FROM user_warnings WHERE warning_date < %s", (cutoff_date,))
                warnings_deleted = cursor.rowcount
                
                # Limpar registros de cargos removidos antigos
                await cursor.execute("DELETE FROM removed_roles WHERE removal_date < %s", (cutoff_date,))
                roles_deleted = cursor.rowcount
                
                # Limpar membros expulsos antigos
                await cursor.execute("DELETE FROM kicked_members WHERE kick_date < %s", (cutoff_date,))
                kicks_deleted = cursor.rowcount
                
                await conn.commit()
                
                log_message = (
                    f"Limpeza de dados antigos concluída: "
                    f"Sessões de voz: {voice_deleted}, "
                    f"Avisos: {warnings_deleted}, "
                    f"Cargos removidos: {roles_deleted}, "
                    f"Expulsões: {kicks_deleted}"
                )
                logger.info(log_message)
                await bot.log_action("Limpeza de Dados", None, log_message)
                
                # Invalidar cache após limpeza
                await bot.invalidate_cache('user_data')
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await bot.log_action("Erro na Limpeza de Dados", None, f"Falha ao limpar dados antigos: {str(e)}")

@tasks.loop(minutes=5)
async def monitor_api_limits():
    """Monitora os limites da API do Discord em tempo real"""
    await bot.wait_until_ready()
    
    try:
        # Obter estatísticas atuais
        async with bot.rate_limit_lock:
            global_stats = bot.rate_limit_stats['global']
            endpoint_stats = bot.rate_limit_stats['endpoints']
            
        # Preparar relatório
        report_lines = [
            "📊 **Status de Rate Limits da API**",
            f"• Global: {global_stats['count']} hits (max {global_stats['max_retries']})",
            "🔍 Por Endpoint:"
        ]
        
        for endpoint, stats in endpoint_stats.items():
            report_lines.append(
                f"• {endpoint}: {stats['count']} hits (max {stats['max_retries']})"
            )
        
        # Adicionar análise de tendência
        warning_threshold = 0.7  # 70% do limite
        warning_lines = []
        
        if global_stats['count'] > global_stats['max_retries'] * warning_threshold:
            warning_lines.append("⚠️ **ALERTA**: Aproximando dos limites globais da API")
            
        for endpoint, stats in endpoint_stats.items():
            if stats['count'] > stats['max_retries'] * warning_threshold:
                warning_lines.append(
                    f"⚠️ **ALERTA**: Endpoint {endpoint} com {stats['count']}/{stats['max_retries']} hits"
                )
        
        # Enviar para canal de logs se houver problemas ou a cada 12 relatórios (1 hora)
        if warning_lines or monitor_api_limits.current_loop % 12 == 0:
            embed = discord.Embed(
                title="Monitor de Rate Limits",
                description="\n".join(report_lines + warning_lines),
                color=discord.Color.blue() if not warning_lines else discord.Color.orange(),
                timestamp=datetime.now(bot.timezone)
            )
            
            await bot.log_action("Monitor API", None, embed=embed)
            
    except Exception as e:
        logger.error(f"Erro no monitoramento da API: {e}")

async def generate_activity_report(member: discord.Member, sessions: list) -> Optional[discord.File]:
    """Gera um relatório gráfico de atividade e retorna como discord.File"""
    if not sessions:
        return None

    try:
        # Verificar rate limit para geração de gráficos
        wait_time = await bot.check_rate_limit('generate_graph')
        if wait_time == -1:
            logger.warning("Geração de gráfico cancelada devido a rate limit")
            return None
        elif wait_time > 0:
            await asyncio.sleep(wait_time)
        
        return await generate_activity_graph(member, sessions)
    except Exception as e:
        logger.error(f"Erro ao gerar relatório gráfico: {e}")
        return None

async def _execute_force_check(member: discord.Member):
    """Executa uma verificação forçada de inatividade para um membro específico"""
    try:
        # Verificar rate limit antes de começar
        wait_time = await bot.check_rate_limit('force_check')
        if wait_time == -1:
            return {'error': 'Rate limit excedido, tente novamente mais tarde'}
        elif wait_time > 0:
            await asyncio.sleep(wait_time)
        
        guild = member.guild
        required_minutes = bot.config['required_minutes']
        required_days = bot.config['required_days']
        monitoring_period = bot.config['monitoring_period']
        
        # Definir período de verificação
        period_end = datetime.now(bot.timezone)
        period_start = period_end - timedelta(days=monitoring_period)
        
        # Obter sessões de voz no período (com cache)
        sessions = await get_voice_sessions_with_cache(member.id, guild.id, period_start, period_end)
        
        # Verificar requisitos
        meets_requirements = False
        valid_days = set()
        
        if sessions:
            for session in sessions:
                if session['duration'] >= required_minutes * 60:
                    day = session['join_time'].replace(tzinfo=bot.timezone).date()
                    valid_days.add(day)
            
            meets_requirements = len(valid_days) >= required_days
        
        # Registrar verificação
        await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
        
        # Atualizar cache
        await bot.set_cached_user_data(member.id, guild.id, {
            'last_check': datetime.now(bot.timezone),
            'meets_requirements': meets_requirements,
            'valid_days': len(valid_days),
            'sessions': sessions
        })
        
        return {
            'meets_requirements': meets_requirements,
            'valid_days': len(valid_days),
            'required_days': required_days,
            'sessions_count': len(sessions),
            'period_start': period_start,
            'period_end': period_end
        }
        
    except Exception as e:
        logger.error(f"Erro na verificação forçada para {member}: {e}")
        raise

def setup_tasks():
    """Configura e inicia todas as tarefas agendadas"""
    # Configurar horários específicos para minimizar impacto
    inactivity_check.change_interval(time=datetime.time(hour=3, minute=0))  # 3 AM
    check_warnings.change_interval(time=datetime.time(hour=6, minute=0))    # 6 AM
    cleanup_members.change_interval(time=datetime.time(hour=9, minute=0))   # 9 AM
    database_backup.change_interval(time=datetime.time(hour=0, minute=0))   # Midnight
    cleanup_old_data.change_interval(time=datetime.time(hour=1, minute=0))  # 1 AM
    
    # Iniciar todas as tarefas
    inactivity_check.start()
    check_warnings.start()
    cleanup_members.start()
    database_backup.start()
    cleanup_old_data.start()
    monitor_api_limits.start()
    
    logger.info("Todas as tarefas agendadas foram iniciadas")