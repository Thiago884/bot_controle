from datetime import datetime, timedelta
import asyncio
import logging
from main import bot
from discord.ext import tasks
import discord
from io import BytesIO
from typing import Optional, Dict, List
from utils import generate_activity_graph
from collections import defaultdict

logger = logging.getLogger('inactivity_bot')

class TaskMetrics:
    """Class to track and report task performance metrics"""
    def __init__(self):
        self.execution_times = defaultdict(list)
        self.error_counts = defaultdict(int)
        self.success_counts = defaultdict(int)
    
    def record_execution(self, task_name: str, duration: float):
        self.execution_times[task_name].append(duration)
        # Keep only the last 100 executions for each task
        if len(self.execution_times[task_name]) > 100:
            self.execution_times[task_name].pop(0)
    
    def record_error(self, task_name: str):
        self.error_counts[task_name] += 1
    
    def record_success(self, task_name: str):
        self.success_counts[task_name] += 1
    
    def get_metrics(self, task_name: str) -> Dict:
        times = self.execution_times.get(task_name, [])
        return {
            'last_24h_errors': self.error_counts.get(task_name, 0),
            'last_24h_successes': self.success_counts.get(task_name, 0),
            'avg_time': sum(times)/len(times) if times else 0,
            'min_time': min(times) if times else 0,
            'max_time': max(times) if times else 0,
            'last_10_avg': sum(times[-10:])/len(times[-10:]) if len(times) >= 10 else 0
        }

# Initialize task metrics
task_metrics = TaskMetrics()

def log_task_metrics(task_name: str):
    """Decorator to log task execution metrics"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                result = await func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                task_metrics.record_execution(task_name, duration)
                task_metrics.record_success(task_name)
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                task_metrics.record_execution(task_name, duration)
                task_metrics.record_error(task_name)
                logger.error(f"Error in {task_name}: {e}", exc_info=True)
                raise
        return wrapper
    return decorator

@tasks.loop(hours=24)
@log_task_metrics("inactivity_check")
async def inactivity_check():
    """Verifica a inatividade dos membros e remove cargos se necessário"""
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        logger.info("Nenhum cargo monitorado definido - verificação de inatividade ignorada")
        return
    
    processed_members = 0
    members_with_roles_removed = 0
    batch_size = bot._batch_processing_size
    
    for guild in bot.guilds:
        # Process members in batches to avoid rate limits
        members = list(guild.members)
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            await asyncio.gather(*[process_member_inactivity(member, guild, required_minutes, 
                required_days, monitoring_period, tracked_roles) for member in batch])
            
            # Small delay between batches
            await asyncio.sleep(bot.rate_limit_delay)
    
    logger.info(f"Verificação de inatividade concluída. Membros processados: {processed_members}, Cargos removidos: {members_with_roles_removed}")

async def process_member_inactivity(member: discord.Member, guild: discord.Guild, 
                                  required_minutes: int, required_days: int, 
                                  monitoring_period: int, tracked_roles: List[int]):
    """Process inactivity check for a single member"""
    try:
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users'] or \
           any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
            return
            
        # Verificar se tem cargos monitorados
        if not any(role.id in tracked_roles for role in member.roles):
            return
        
        processed_members += 1
        
        # Verificar último período verificado
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        now = datetime.now(bot.timezone)
        
        if last_check:
            last_period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
            if now < last_period_end:
                return
        
        # Definir período de verificação
        period_end = now
        period_start = period_end - timedelta(days=monitoring_period)
        
        # Obter sessões de voz no período
        sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
        
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
        
        # Ações para quem não cumpriu os requisitos
        if not meets_requirements:
            roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
            if roles_to_remove:
                try:
                    await member.remove_roles(*roles_to_remove)
                    await bot.send_warning(member, 'final')
                    await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                    
                    # Gerar relatório gráfico
                    report_file = await generate_activity_report(member, sessions)
                    
                    log_message = (
                        f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                        f"Sessões no período: {len(sessions)}\n"
                        f"Dias válidos: {len(valid_days)}/{required_days}"
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
                    
                    members_with_roles_removed += 1
                    
                except discord.Forbidden:
                    await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                except Exception as e:
                    logger.error(f"Erro ao remover cargos de {member}: {e}")
    
    except Exception as e:
        logger.error(f"Erro ao verificar inatividade para {member}: {e}")

@tasks.loop(hours=24)
@log_task_metrics("check_warnings")
async def check_warnings():
    """Verifica e envia avisos de inatividade para membros"""
    await bot.wait_until_ready()
    
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
    batch_size = bot._batch_processing_size
    
    for guild in bot.guilds:
        members = list(guild.members)
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            await asyncio.gather(*[process_member_warnings(member, guild, tracked_roles, 
                first_warning_days, second_warning_days, warnings_sent) for member in batch])
            
            await asyncio.sleep(bot.rate_limit_delay)
    
    logger.info(f"Verificação de avisos concluída. Avisos enviados: Primeiro={warnings_sent['first']}, Segundo={warnings_sent['second']}")

async def process_member_warnings(member: discord.Member, guild: discord.Guild, 
                                tracked_roles: List[int], first_warning_days: int, 
                                second_warning_days: int, warnings_sent: Dict):
    """Process warnings for a single member"""
    try:
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users'] or \
           any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
            return
            
        # Verificar se tem cargos monitorados
        if not any(role.id in tracked_roles for role in member.roles):
            return
        
        # Obter última verificação
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        if not last_check:
            return
        
        # Calcular dias restantes
        period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
        days_remaining = (period_end - datetime.now(bot.timezone)).days
        
        # Obter último aviso
        last_warning = await bot.db.get_last_warning(member.id, guild.id)
        
        # Verificar necessidade de avisos
        if days_remaining <= first_warning_days and (
            not last_warning or last_warning[0] != 'first'):
            await bot.send_warning(member, 'first')
            warnings_sent['first'] += 1
        
        elif days_remaining <= second_warning_days and (
            not last_warning or last_warning[0] != 'second'):
            await bot.send_warning(member, 'second')
            warnings_sent['second'] += 1
            
    except Exception as e:
        logger.error(f"Erro ao verificar avisos para {member}: {e}")

@tasks.loop(hours=24)
@log_task_metrics("cleanup_members")
async def cleanup_members():
    """Remove membros inativos que estão sem cargos há muito tempo"""
    await bot.wait_until_ready()
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        logger.info("Expulsão de membros inativos desativada na configuração")
        return
    
    cutoff_date = datetime.now(bot.timezone) - timedelta(days=kick_after_days)
    members_kicked = 0
    batch_size = bot._batch_processing_size
    
    for guild in bot.guilds:
        members = list(guild.members)
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            await asyncio.gather(*[process_member_cleanup(member, guild, cutoff_date, 
                kick_after_days, members_kicked) for member in batch])
            
            await asyncio.sleep(bot.rate_limit_delay)
    
    logger.info(f"Limpeza de membros concluída. Membros expulsos: {members_kicked}")

async def process_member_cleanup(member: discord.Member, guild: discord.Guild, 
                               cutoff_date: datetime, kick_after_days: int, 
                               members_kicked: int):
    """Process cleanup for a single member"""
    try:
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users']:
            return
            
        # Verificar se tem apenas o cargo @everyone
        if len(member.roles) == 1:
            joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
            if joined_at and joined_at < cutoff_date:
                try:
                    await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                    await bot.db.log_kicked_member(member.id, guild.id, f"Sem cargos há mais de {kick_after_days} dias")
                    await bot.log_action(
                        "Membro Expulso",
                        member,
                        f"Motivo: Sem cargos há mais de {kick_after_days} dias\n"
                        f"Entrou no servidor em: {joined_at.strftime('%d/%m/%Y')}"
                    )
                    await bot.notify_roles(
                        f"👢 {member.mention} foi expulso por estar sem cargos há mais de {kick_after_days} dias")
                    
                    members_kicked += 1
                    
                except discord.Forbidden:
                    await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
                except Exception as e:
                    logger.error(f"Erro ao expulsar membro {member}: {e}")
    except Exception as e:
        logger.error(f"Erro ao verificar membro para expulsão {member}: {e}")

@tasks.loop(hours=24)
@log_task_metrics("database_backup")
async def database_backup():
    """Executa backup diário do banco de dados"""
    await bot.wait_until_ready()
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
@log_task_metrics("cleanup_old_data")
async def cleanup_old_data():
    """Limpa dados antigos do banco de dados"""
    await bot.wait_until_ready()
    
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
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await bot.log_action("Erro na Limpeza de Dados", None, f"Falha ao limpar dados antigos: {str(e)}")

@tasks.loop(minutes=1)
@log_task_metrics("monitor_rate_limits")
async def monitor_rate_limits():
    """Monitora e ajusta dinamicamente os rate limits"""
    await bot.wait_until_ready()
    
    try:
        # Verificar uso atual
        global_usage = 1 - (bot.rate_limit_buckets['global']['remaining'] / bot.rate_limit_buckets['global']['limit'])
        
        # Ajustar dinamicamente o tamanho dos lotes
        if global_usage > 0.8:  # Se estiver usando mais de 80% do rate limit
            bot._batch_processing_size = max(5, bot._batch_processing_size - 2)
            logger.info(f"Reduzindo tamanho do lote para {bot._batch_processing_size} devido a alto uso de rate limits")
        elif global_usage < 0.3:  # Se estiver usando menos de 30%
            bot._batch_processing_size = min(20, bot._batch_processing_size + 2)
            logger.info(f"Aumentando tamanho do lote para {bot._batch_processing_size} devido a baixo uso de rate limits")
            
        # Registrar estatísticas
        await bot.log_action(
            "Monitoramento de Rate Limits",
            None,
            f"Uso global: {global_usage*100:.1f}%\n"
            f"Tamanho do lote atual: {bot._batch_processing_size}\n"
            f"Delay atual: {bot.rate_limit_delay:.2f}s"
        )
    except Exception as e:
        logger.error(f"Erro no monitoramento de rate limits: {e}")

@tasks.loop(hours=24)
@log_task_metrics("report_metrics")
async def report_metrics():
    """Report daily task metrics"""
    await bot.wait_until_ready()
    
    try:
        metrics_report = []
        for task_name in ['inactivity_check', 'check_warnings', 'cleanup_members', 
                         'database_backup', 'cleanup_old_data', 'monitor_rate_limits']:
            metrics = task_metrics.get_metrics(task_name)
            metrics_report.append(
                f"**{task_name}**:\n"
                f"- Execuções bem-sucedidas: {metrics['last_24h_successes']}\n"
                f"- Erros: {metrics['last_24h_errors']}\n"
                f"- Tempo médio: {metrics['avg_time']:.2f}s\n"
                f"- Últimas 10 execuções: {metrics['last_10_avg']:.2f}s\n"
            )
        
        await bot.log_action(
            "Relatório de Métricas Diárias",
            None,
            "\n".join(metrics_report)
        )
        
        # Reset counts for the new day
        task_metrics.error_counts.clear()
        task_metrics.success_counts.clear()
    except Exception as e:
        logger.error(f"Erro ao gerar relatório de métricas: {e}")

async def generate_activity_report(member: discord.Member, sessions: list) -> Optional[discord.File]:
    """Gera um relatório gráfico de atividade e retorna como discord.File"""
    if not sessions:
        return None

    try:
        return await generate_activity_graph(member, sessions)
    except Exception as e:
        logger.error(f"Erro ao gerar relatório gráfico: {e}")
        return None

async def _execute_force_check(member: discord.Member):
    """Executa uma verificação forçada de inatividade para um membro específico"""
    try:
        guild = member.guild
        required_minutes = bot.config['required_minutes']
        required_days = bot.config['required_days']
        monitoring_period = bot.config['monitoring_period']
        
        # Definir período de verificação
        period_end = datetime.now(bot.timezone)
        period_start = period_end - timedelta(days=monitoring_period)
        
        # Obter sessões de voz no período
        sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
        
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
    inactivity_check.start()
    check_warnings.start()
    cleanup_members.start()
    database_backup.start()
    cleanup_old_data.start()
    monitor_rate_limits.start()
    report_metrics.start()
    
    logger.info("Todas as tarefas agendadas foram iniciadas")