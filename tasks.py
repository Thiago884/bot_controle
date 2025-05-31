from datetime import datetime, timedelta
import asyncio
import logging
from main import bot

logger = logging.getLogger('inactivity_bot')

@tasks.loop(hours=24)
async def inactivity_check():
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        return
    
    for guild in bot.guilds:
        for member in guild.members:
            if member.id in bot.config['whitelist']['users'] or \
               any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                continue
                
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = await bot.db.get_last_period_check(member.id, guild.id)
                    now = datetime.now(bot.timezone)
                    
                    if last_check:
                        last_period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                        if now < last_period_end:
                            continue
                    
                    period_end = now
                    period_start = period_end - timedelta(days=monitoring_period)
                    
                    sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
                    
                    meets_requirements = False
                    if sessions:
                        valid_days = set()
                        for session in sessions:
                            if session['duration'] >= required_minutes * 60:
                                day = session['join_time'].replace(tzinfo=bot.timezone).date()
                                valid_days.add(day)
                        
                        meets_requirements = len(valid_days) >= required_days
                    
                    await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
                    
                    if not meets_requirements:
                        roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                        if roles_to_remove:
                            try:
                                await member.remove_roles(*roles_to_remove)
                                await bot.send_warning(member, 'final')
                                await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                                await bot.log_action(
                                    "Cargo Removido",
                                    member,
                                    f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}")
                                await bot.notify_roles(
                                    f"🚨 Cargos removidos de {member.mention} por inatividade: " +
                                    ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                            except discord.Forbidden:
                                await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                            except Exception as e:
                                logger.error(f"Erro ao remover cargos de {member}: {e}")
                except Exception as e:
                    logger.error(f"Erro ao verificar inatividade para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def check_warnings():
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    warnings_config = bot.config.get('warnings', {})
    
    if not tracked_roles or not warnings_config:
        return
    
    first_warning_days = warnings_config.get('first_warning', 3)
    second_warning_days = warnings_config.get('second_warning', 1)
    
    for guild in bot.guilds:
        for member in guild.members:
            if member.id in bot.config['whitelist']['users'] or \
               any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                continue
                
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = await bot.db.get_last_period_check(member.id, guild.id)
                    if not last_check:
                        continue
                    
                    period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                    days_remaining = (period_end - datetime.now(bot.timezone)).days
                    
                    last_warning = await bot.db.get_last_warning(member.id, guild.id)
                    
                    if days_remaining <= first_warning_days and (
                        not last_warning or last_warning[0] != 'first'):
                        await bot.send_warning(member, 'first')
                    
                    elif days_remaining <= second_warning_days and (
                        not last_warning or last_warning[0] != 'second'):
                        await bot.send_warning(member, 'second')
                except Exception as e:
                    logger.error(f"Erro ao verificar avisos para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def cleanup_members():
    await bot.wait_until_ready()
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        return
    
    cutoff_date = datetime.now(bot.timezone) - timedelta(days=kick_after_days)
    
    for guild in bot.guilds:
        for member in guild.members:
            try:
                if member.id in bot.config['whitelist']['users']:
                    continue
                    
                if len(member.roles) == 1:
                    joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
                    if joined_at and joined_at < cutoff_date:
                        try:
                            await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                            await bot.db.log_kicked_member(member.id, guild.id, f"Sem cargos há mais de {kick_after_days} dias")
                            await bot.log_action(
                                "Membro Expulso",
                                member,
                                f"Motivo: Sem cargos há mais de {kick_after_days} dias")
                            await bot.notify_roles(
                                f"👢 {member.mention} foi expulso por estar sem cargos há mais de {kick_after_days} dias")
                        except discord.Forbidden:
                            await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
                        except Exception as e:
                            logger.error(f"Erro ao expulsar membro {member}: {e}")
            except Exception as e:
                logger.error(f"Erro ao verificar membro para expulsão {member}: {e}")
                try:
                    bot.db.reconnect()
                except Exception as db_error:
                    logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def database_backup():
    await bot.wait_until_ready()
    if not hasattr(bot, 'db_backup'):
        from database import DatabaseBackup
        bot.db_backup = DatabaseBackup(bot.db)
    
    success = await bot.db_backup.create_backup()
    if success:
        await bot.log_action("Backup do Banco de Dados", None, "Backup diário realizado com sucesso")

@tasks.loop(hours=24)
async def cleanup_old_data():
    await bot.wait_until_ready()
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=60)  # 2 meses
        
        cursor = None
        try:
            cursor = bot.db.connection.cursor()
            
            # Limpar sessões de voz antigas
            cursor.execute("DELETE FROM voice_sessions WHERE leave_time < %s", (cutoff_date,))
            voice_deleted = cursor.rowcount
            
            # Limpar avisos antigos
            cursor.execute("DELETE FROM user_warnings WHERE warning_date < %s", (cutoff_date,))
            warnings_deleted = cursor.rowcount
            
            # Limpar registros de cargos removidos antigos
            cursor.execute("DELETE FROM removed_roles WHERE removal_date < %s", (cutoff_date,))
            roles_deleted = cursor.rowcount
            
            # Limpar membros expulsos antigos
            cursor.execute("DELETE FROM kicked_members WHERE kick_date < %s", (cutoff_date,))
            kicks_deleted = cursor.rowcount
            
            bot.db.connection.commit()
            
            log_message = (
                f"Limpeza de dados antigos concluída: "
                f"Sessões de voz: {voice_deleted}, "
                f"Avisos: {warnings_deleted}, "
                f"Cargos removidos: {roles_deleted}, "
                f"Expulsões: {kicks_deleted}"
            )
            logger.info(log_message)
            await bot.log_action("Limpeza de Dados", None, log_message)
        finally:
            if cursor:
                cursor.close()
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")