from datetime import datetime, timedelta
import asyncio
import logging
from main import bot
from discord.ext import tasks, commands
import discord
from io import BytesIO
from typing import Optional, Dict, List
from utils import generate_activity_graph
from collections import defaultdict
import time

logger = logging.getLogger('inactivity_bot')

class PerformanceMetrics:
    def __init__(self):
        self.db_query_times = []
        self.api_call_times = []
        self.task_execution_times = []  # Adicionando para rastrear tempos de execução de tasks
        self.last_flush = time.time()
    
    def record_db_query(self, duration):
        self.db_query_times.append(duration)
        self._maybe_flush()
    
    def record_api_call(self, duration):
        self.api_call_times.append(duration)
        self._maybe_flush()
    
    def record_task_execution(self, task_name, duration):
        """Registra o tempo de execução de uma task"""
        self.task_execution_times.append((task_name, duration))
        self._maybe_flush()
    
    def _maybe_flush(self):
        if time.time() - self.last_flush > 300:  # 5 minutes
            self._flush_metrics()
            self.last_flush = time.time()
    
    def _flush_metrics(self):
        if self.db_query_times:
            avg_db = sum(self.db_query_times) / len(self.db_query_times)
            max_db = max(self.db_query_times)
            logger.info(f"Métricas DB: Avg={avg_db:.3f}s, Max={max_db:.3f}s, Samples={len(self.db_query_times)}")
            self.db_query_times = []
        
        if self.api_call_times:
            avg_api = sum(self.api_call_times) / len(self.api_call_times)
            max_api = max(self.api_call_times)
            logger.info(f"Métricas API: Avg={avg_api:.3f}s, Max={max_api:.3f}s, Samples={len(self.api_call_times)}")
            self.api_call_times = []
        
        if self.task_execution_times:
            # Agrupar por nome de task
            task_stats = {}
            for task_name, duration in self.task_execution_times:
                if task_name not in task_stats:
                    task_stats[task_name] = []
                task_stats[task_name].append(duration)
            
            # Logar estatísticas por task
            for task_name, durations in task_stats.items():
                avg = sum(durations) / len(durations)
                max_d = max(durations)
                logger.info(f"Métricas Task {task_name}: Avg={avg:.3f}s, Max={max_d:.3f}s, Samples={len(durations)}")
            
            self.task_execution_times = []

# Global instance
perf_metrics = PerformanceMetrics()

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
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                perf_metrics.record_task_execution(task_name, duration)
                task_metrics.record_success(task_name)
                return result
            except Exception as e:
                duration = time.time() - start_time
                perf_metrics.record_task_execution(task_name, duration)
                task_metrics.record_error(task_name)
                logger.error(f"Error in {task_name}: {e}", exc_info=True)
                raise
        return wrapper
    return decorator

class BatchProcessor:
    def __init__(self, bot):
        self.bot = bot
        self.batcher = DynamicBatcher()

    async def process_inactivity_batch(self, members: list[discord.Member]):
        """Processa um lote de membros de uma vez."""
        try:
            batch = members[:self.batcher.batch_size]
            results = await self._real_process_batch(batch)
            await self.batcher.adjust_batch_size()  # Aumenta se tudo ok
            return results
        except discord.RateLimited:
            await self.batcher.handle_rate_limit()  # Reduz se houver rate limit
            raise

    async def _real_process_batch(self, members):
        """Processa um lote de membros de forma otimizada."""
        if not members:
            return []
            
        guild = members[0].guild  # Assume que todos são do mesmo servidor
        batch_results = []

        # Consulta única para todos os membros do lote
        user_ids = [m.id for m in members]
        start_time = time.time()
        last_checks = await self.bot.db.get_last_periods_batch(user_ids, guild.id)
        perf_metrics.record_db_query(time.time() - start_time)
        
        # Processa cada membro em paralelo
        tasks = []
        for member in members:
            task = self._process_member_optimized(member, last_checks.get(member.id, {}))
            tasks.append(task)
        
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        return batch_results

    async def _process_member_optimized(self, member, last_check):
        """Versão otimizada do process_member_inactivity."""
        result = {'processed': 0, 'removed': 0}
        
        try:
            # Verificar whitelist
            if member.id in self.bot.config['whitelist']['users'] or \
               any(role.id in self.bot.config['whitelist']['roles'] for role in member.roles):
                return result
                
            # Verificar se tem cargos monitorados
            tracked_roles = self.bot.config['tracked_roles']
            if not any(role.id in tracked_roles for role in member.roles):
                return result
            
            result['processed'] = 1
            
            now = datetime.now(self.bot.timezone)
            
            # Se não há verificação anterior ou o período acabou
            if not last_check or now >= last_check.get('period_end', datetime.min).replace(tzinfo=self.bot.timezone):
                # Definir período de verificação (últimos X dias)
                monitoring_period = self.bot.config['monitoring_period']
                period_end = now
                period_start = now - timedelta(days=monitoring_period)
                
                # Obter sessões de voz no período
                start_time = time.time()
                sessions = await self.bot.db.get_voice_sessions(
                    member.id, member.guild.id,
                    period_start,
                    period_end
                )
                perf_metrics.record_db_query(time.time() - start_time)
                
                # Verificar requisitos do período
                required_minutes = self.bot.config['required_minutes']
                required_days = self.bot.config['required_days']
                
                meets_requirements = False
                valid_days = set()
                
                if sessions:
                    for session in sessions:
                        if session['duration'] >= required_minutes * 60:
                            day = session['join_time'].replace(tzinfo=self.bot.timezone).date()
                            valid_days.add(day)
                    
                    meets_requirements = len(valid_days) >= required_days
                
                # Ações para quem não cumpriu os requisitos
                if not meets_requirements:
                    roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                    
                    if roles_to_remove:
                        try:
                            # Registrar novo período antes de remover cargos
                            await self.bot.db.log_period_check(
                                member.id, member.guild.id, 
                                period_start, period_end, 
                                meets_requirements
                            )
                            
                            # Remover cargos
                            start_time = time.time()
                            await member.remove_roles(*roles_to_remove)
                            perf_metrics.record_api_call(time.time() - start_time)
                            
                            # Enviar mensagem de aviso final via DM
                            await self.bot.send_warning(member, 'final')
                            
                            # Registrar cargos removidos
                            start_time = time.time()
                            await self.bot.db.log_removed_roles(
                                member.id, member.guild.id, 
                                [r.id for r in roles_to_remove]
                            )
                            perf_metrics.record_db_query(time.time() - start_time)
                            
                            # Gerar relatório gráfico
                            report_file = await generate_activity_report(member, sessions)
                            
                            log_message = (
                                f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                                f"Sessões no período: {len(sessions)}\n"
                                f"Dias válidos: {len(valid_days)}/{required_days}"
                            )
                            
                            if report_file:
                                await self.bot.log_action(
                                    "Cargo Removido",
                                    member,
                                    log_message,
                                    file=report_file
                                )
                            else:
                                await self.bot.log_action(
                                    "Cargo Removido",
                                    member,
                                    log_message
                                )
                            
                            await self.bot.notify_roles(
                                f"🚨 Cargos removidos de {member.mention} por inatividade: " +
                                ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                            
                            result['removed'] = 1
                            
                        except discord.Forbidden:
                            await self.bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                        except Exception as e:
                            logger.error(f"Erro ao remover cargos de {member}: {e}")
                else:
                    # Registrar que cumpriu os requisitos
                    await self.bot.db.log_period_check(
                        member.id, member.guild.id, 
                        period_start, period_end, 
                        meets_requirements
                    )
            
            # Definir novo período de verificação (futuro)
            new_period_end = now + timedelta(days=self.bot.config['monitoring_period'])
            new_period_start = now
            
            # Registrar novo período de verificação
            await self.bot.db.log_period_check(
                member.id, member.guild.id, 
                new_period_start, new_period_end, 
                False
            )
        
        except Exception as e:
            logger.error(f"Erro ao verificar inatividade para {member}: {e}")
        
        return result

class DynamicBatcher:
    def __init__(self):
        self.batch_size = 10  # Valor inicial
        self.min_batch = 5    # Mínimo seguro
        self.max_batch = 50   # Máximo permitido
        self.last_rate_limit = None

    async def adjust_batch_size(self):
        """Ajusta dinamicamente o tamanho do lote."""
        now = time.time()

        # Se houve rate limit recentemente, reduz o batch
        if self.last_rate_limit and (now - self.last_rate_limit) < 60:
            self.batch_size = max(self.min_batch, self.batch_size - 5)
        else:
            # Senão, aumenta gradualmente
            self.batch_size = min(self.max_batch, self.batch_size + 2)

    async def handle_rate_limit(self):
        """Chamado quando um rate limit é detectado."""
        self.last_rate_limit = time.time()
        await self.adjust_batch_size()

def prioritize_members(members: list[discord.Member]) -> list[discord.Member]:
    """Ordena membros para processar os mais prováveis de estarem inativos primeiro."""
    return sorted(
        members,
        key=lambda m: (
            m.joined_at.timestamp() if m.joined_at else 0,  # Mais antigos primeiro
            len(m.roles),  # Membros com menos cargos primeiro
        ),
        reverse=False
    )

async def execute_task_with_persistent_interval(task_name: str, monitoring_period: int, task_func: callable):
    """Executa a task mantendo intervalo persistente"""
    await bot.wait_until_ready()
    
    while True:
        try:
            # Verificar última execução
            last_exec = await bot.db.get_last_task_execution(task_name)
            now = datetime.utcnow()
            
            # Calcular se deve executar agora
            should_execute = False
            
            if not last_exec:  # Primeira execução
                should_execute = True
                logger.info(f"Primeira execução da task {task_name}")
            else:
                # Verificar se o período de monitoramento foi reduzido
                if last_exec['monitoring_period'] > monitoring_period:
                    should_execute = True
                    logger.info(f"Período de monitoramento reduzido para {task_name} - executando")
                # Ou se passou o tempo mínimo desde a última execução (ajustado para 1 hora)
                elif (now - last_exec['last_execution']) >= timedelta(hours=1):
                    should_execute = True
                    logger.debug(f"Intervalo suficiente passou para {task_name} - executando")
            
            if should_execute:
                logger.info(f"Executando task {task_name}...")
                start_time = time.time()
                await task_func()
                perf_metrics.record_task_execution(task_name, time.time() - start_time)
                await bot.db.log_task_execution(task_name, monitoring_period)
                logger.info(f"Task {task_name} concluída com sucesso")
            
            # Esperar 15 minutos antes de verificar novamente (ajustável)
            await asyncio.sleep(900)
                
        except Exception as e:
            logger.error(f"Erro na task {task_name}: {e}", exc_info=True)
            await asyncio.sleep(600)  # Esperar 10 minutos antes de tentar novamente

async def execute_task_with_retry(task_name: str, task_func: callable, max_retries: int = 3):
    """Executa uma task com tentativas de recuperação."""
    retries = 0
    while retries < max_retries:
        try:
            await task_func()
            break
        except Exception as e:
            retries += 1
            logger.error(f"Falha na task {task_name} (tentativa {retries}/{max_retries}): {e}")
            if retries >= max_retries:
                logger.error(f"Task {task_name} falhou após {max_retries} tentativas.")
                await bot.log_action(
                    "Falha na Task",
                    None,
                    f"Task {task_name} falhou após {max_retries} tentativas: {e}"
                )
            await asyncio.sleep(60 * retries)  # Espera exponencial

@bot.event
async def on_shutdown():
    """Executa ações de limpeza antes do desligamento do bot."""
    logger.info("Bot está sendo desligado - executando limpeza...")
    await bot.log_action("Desligamento", None, "Bot está sendo desligado")
    await save_task_states()
    await emergency_backup()

@bot.event
async def on_resumed():
    """Executa ações quando o bot reconecta após uma queda."""
    logger.info("Bot reconectado após queda")
    await bot.log_action("Reconexão", None, "Bot reconectado após queda")
    await check_missed_periods()  # Verificar períodos perdidos durante a queda

@bot.event
async def on_disconnect():
    """Executa ações quando o bot é desconectado."""
    logger.warning("Bot desconectado - realizando backup emergencial...")
    await emergency_backup()

def handle_exception(loop, context):
    """Captura exceções não tratadas."""
    logger.error(f"Exceção não tratada: {context['message']}", exc_info=context.get('exception'))
    asyncio.create_task(bot.log_action(
        "Exceção Não Tratada",
        None,
        f"Exceção: {context.get('exception')}\nMensagem: {context['message']}"
    ))

async def save_task_states():
    """Salva o estado atual das tasks no banco de dados."""
    try:
        for task_name in ['inactivity_check', 'check_warnings', 'cleanup_members']:
            last_exec = await bot.db.get_last_task_execution(task_name)
            if last_exec:
                await bot.db.log_task_execution(task_name, last_exec['monitoring_period'])
        logger.info("Estados das tasks salvos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar estados das tasks: {e}")

async def load_task_states():
    """Carrega o estado das tasks do banco de dados."""
    try:
        for task_name in ['inactivity_check', 'check_warnings', 'cleanup_members']:
            last_exec = await bot.db.get_last_task_execution(task_name)
            if last_exec:
                await bot.db.log_task_execution(task_name, last_exec['monitoring_period'])
        logger.info("Estados das tasks carregados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao carregar estados das tasks: {e}")

async def emergency_backup():
    """Realiza um backup emergencial dos dados críticos."""
    try:
        logger.info("Iniciando backup emergencial...")
        if not hasattr(bot, 'db_backup'):
            from database import DatabaseBackup
            bot.db_backup = DatabaseBackup(bot.db)
        await bot.db_backup.create_backup()
        logger.info("Backup emergencial concluído com sucesso.")
    except Exception as e:
        logger.error(f"Falha no backup emergencial: {e}")

async def health_check():
    """Wrapper para a task com intervalo persistente"""
    await execute_task_with_persistent_interval(
        "health_check",
        1,  # Verifica a cada hora
        _health_check
    )

async def _health_check():
    """Verifica a saúde do bot e reinicia tasks se necessário."""
    await bot.wait_until_ready()
    try:
        # Verifica se as tasks estão rodando
        active_tasks = {t._name if hasattr(t, '_name') else '' for t in asyncio.all_tasks()}
        
        expected_tasks = {
            'inactivity_check_wrapper',
            'check_warnings_wrapper', 
            'cleanup_members_wrapper',
            'database_backup_wrapper',
            'cleanup_old_data_wrapper',
            'monitor_rate_limits_wrapper',
            'report_metrics_wrapper',
            'health_check_wrapper',
            'check_previous_periods_wrapper'  # Nova task adicionada
        }

        for task_name in expected_tasks:
            if task_name not in active_tasks:
                logger.warning(f"Task {task_name} não está ativa - reiniciando...")
                if task_name == 'inactivity_check_wrapper':
                    bot.loop.create_task(inactivity_check(), name='inactivity_check_wrapper')
                elif task_name == 'check_warnings_wrapper':
                    bot.loop.create_task(check_warnings(), name='check_warnings_wrapper')
                elif task_name == 'cleanup_members_wrapper':
                    bot.loop.create_task(cleanup_members(), name='cleanup_members_wrapper')
                elif task_name == 'database_backup_wrapper':
                    bot.loop.create_task(database_backup(), name='database_backup_wrapper')
                elif task_name == 'cleanup_old_data_wrapper':
                    bot.loop.create_task(cleanup_old_data(), name='cleanup_old_data_wrapper')
                elif task_name == 'monitor_rate_limits_wrapper':
                    bot.loop.create_task(monitor_rate_limits(), name='monitor_rate_limits_wrapper')
                elif task_name == 'report_metrics_wrapper':
                    bot.loop.create_task(report_metrics(), name='report_metrics_wrapper')
                elif task_name == 'health_check_wrapper':
                    bot.loop.create_task(health_check(), name='health_check_wrapper')
                elif task_name == 'check_previous_periods_wrapper':
                    bot.loop.create_task(check_previous_periods(), name='check_previous_periods_wrapper')

        await bot.log_action("Verificação de Saúde", None, f"Tasks ativas: {', '.join(t for t in active_tasks if t)}")
    except Exception as e:
        logger.error(f"Erro na verificação de saúde: {e}")

@log_task_metrics("inactivity_check")
async def _inactivity_check():
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
    
    for guild in bot.guilds:
        try:
            # Obter todos os membros com cargos monitorados
            members_with_roles = []
            for member in guild.members:
                if any(role.id in tracked_roles for role in member.roles):
                    members_with_roles.append(member)
            
            # Priorizar membros para processamento
            prioritized_members = prioritize_members(members_with_roles)
            processor = BatchProcessor(bot)
            
            # Processar em lotes otimizados
            for i in range(0, len(prioritized_members), processor.batcher.batch_size):
                batch = prioritized_members[i:i + processor.batcher.batch_size]
                results = await processor.process_inactivity_batch(batch)
                
                # Atualizar contadores
                for result in results:
                    if not isinstance(result, Exception):
                        processed_members += result.get('processed', 0)
                        members_with_roles_removed += result.get('removed', 0)
                
                # Pequeno delay entre lotes para evitar rate limits
                await asyncio.sleep(bot.rate_limit_delay)
                
        except Exception as e:
            logger.error(f"Erro ao verificar inatividade na guild {guild.name}: {e}")
            continue
    
    logger.info(f"Verificação de inatividade concluída. Membros processados: {processed_members}, Cargos removidos: {members_with_roles_removed}")

async def inactivity_check():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "inactivity_check", 
        monitoring_period,
        _inactivity_check
    ), name='inactivity_check_wrapper')
    return task

@log_task_metrics("check_warnings")
async def _check_warnings():
    """Lógica original da task"""
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

async def check_warnings():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "check_warnings", 
        monitoring_period,
        _check_warnings
    ), name='check_warnings_wrapper')
    return task

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
        start_time = time.time()
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        perf_metrics.record_db_query(time.time() - start_time)
        
        if not last_check:
            return
        
        # Calcular dias restantes
        period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
        days_remaining = (period_end - datetime.now(bot.timezone)).days
        
        # Obter último aviso
        start_time = time.time()
        last_warning = await bot.db.get_last_warning(member.id, guild.id)
        perf_metrics.record_db_query(time.time() - start_time)
        
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

@log_task_metrics("cleanup_members")
async def _cleanup_members():
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        logger.info("Expulsão de membros inativos desativada na configuração")
        return
    
    cutoff_date = datetime.now(bot.timezone) - timedelta(days=kick_after_days)
    members_kicked = 0
    batch_size = bot._batch_processing_size
    
    for guild in bot.guilds:
        members = prioritize_members(list(guild.members))  # Priorizar membros mais antigos/sem cargos
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            await asyncio.gather(*[process_member_cleanup(member, guild, cutoff_date, 
                kick_after_days, members_kicked) for member in batch])
            
            await asyncio.sleep(bot.rate_limit_delay)
    
    logger.info(f"Limpeza de membros concluída. Membros expulsos: {members_kicked}")

async def cleanup_members():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_members", 
        monitoring_period,
        _cleanup_members
    ), name='cleanup_members_wrapper')
    return task

async def process_member_cleanup(member: discord.Member, guild: discord.Guild, 
                               cutoff_date: datetime, kick_after_days: int, 
                               members_kicked: int):
    """Process cleanup for a single member"""
    try:
        # Verificar whitelist apenas para usuários, não para roles
        if member.id in bot.config['whitelist']['users']:
            return
            
        # Verificar se tem apenas o cargo @everyone (len=1) ou nenhum cargo (len=0)
        if len(member.roles) <= 1:  # Considera @everyone como um cargo
            joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
            
            if joined_at and joined_at < cutoff_date:
                try:
                    # Verificar se já foi expulso antes
                    start_time = time.time()
                    last_kick = await bot.db.get_last_kick(member.id, guild.id)
                    perf_metrics.record_db_query(time.time() - start_time)
                    
                    if last_kick and (datetime.now(bot.timezone) - last_kick['kick_date']).days < kick_after_days:
                        return
                        
                    start_time = time.time()
                    await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                    perf_metrics.record_api_call(time.time() - start_time)
                    
                    start_time = time.time()
                    await bot.db.log_kicked_member(
                        member.id, guild.id, 
                        f"Sem cargos há mais de {kick_after_days} dias"
                    )
                    perf_metrics.record_db_query(time.time() - start_time)
                    
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

@log_task_metrics("database_backup")
async def _database_backup():
    """Lógica original da task"""
    await bot.wait_until_ready()
    if not hasattr(bot, 'db_backup'):
        from database import DatabaseBackup
        bot.db_backup = DatabaseBackup(bot.db)
    
    try:
        start_time = time.time()
        success = await bot.db_backup.create_backup()
        perf_metrics.record_db_query(time.time() - start_time)
        
        if success:
            await bot.log_action("Backup do Banco de Dados", None, "Backup diário realizado com sucesso")
            logger.info("Backup do banco de dados concluído com sucesso")
        else:
            logger.error("Falha ao criar backup do banco de dados")
    except Exception as e:
        logger.error(f"Erro ao executar backup do banco de dados: {e}")
        await bot.log_action("Erro no Backup", None, f"Falha ao criar backup: {str(e)}")

async def database_backup():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 1  # Backup should run daily regardless of monitoring period
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "database_backup", 
        monitoring_period,
        _database_backup
    ), name='database_backup_wrapper')
    return task

@log_task_metrics("cleanup_old_data")
async def _cleanup_old_data():
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=60)  # 2 meses
        
        start_time = time.time()
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
        
        perf_metrics.record_db_query(time.time() - start_time)
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await bot.log_action("Erro na Limpeza de Dados", None, f"Falha ao limpar dados antigos: {str(e)}")

async def cleanup_old_data():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 1  # Cleanup should run daily regardless of monitoring period
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_old_data", 
        monitoring_period,
        _cleanup_old_data
    ), name='cleanup_old_data_wrapper')
    return task

@log_task_metrics("monitor_rate_limits")
async def _monitor_rate_limits():
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    try:
        # Verificar uso atual
        global_bucket = bot.rate_limit_buckets['global']
        global_usage = 1 - (global_bucket['remaining'] / global_bucket['limit'])
        
        # Ajustar dinamicamente o tamanho dos lotes
        previous_batch_size = bot._batch_processing_size
        
        if global_usage > 0.8:  # Se estiver usando mais de 80% do rate limit
            bot._batch_processing_size = max(5, bot._batch_processing_size - 2)
        elif global_usage < 0.3:  # Se estiver usando menos de 30%
            bot._batch_processing_size = min(20, bot._batch_processing_size + 2)
        
        # Só enviar notificação se houver mudança significativa ou situação crítica
        should_notify = False
        notification_message = ""
        
        # Verificar se houve mudança no tamanho do lote
        if bot._batch_processing_size != previous_batch_size:
            should_notify = True
            notification_message += (
                f"📊 **Ajuste de Rate Limits**\n"
                f"Tamanho do lote alterado de {previous_batch_size} para {bot._batch_processing_size}\n"
            )
        
        # Verificar se está próximo do limite global
        if global_usage > 0.7:
            should_notify = True
            notification_message += (
                f"⚠️ **Alerta de Uso Elevado**\n"
                f"Uso global: {global_usage*100:.1f}%\n"
                f"Remaining: {global_bucket['remaining']}/{global_bucket['limit']}\n"
                f"Reset em: {max(0, global_bucket['reset_at'] - time.time()):.0f}s\n"
            )
        
        # Enviar notificação se necessário
        if should_notify:
            await bot.log_action(
                "Monitoramento de Rate Limits",
                None,
                notification_message + 
                f"Delay atual: {bot.rate_limit_delay:.2f}s"
            )
            
    except Exception as e:
        logger.error(f"Erro no monitoramento de rate limits: {e}")

async def monitor_rate_limits():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 1  # Rate limit monitoring should run frequently regardless of monitoring period
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "monitor_rate_limits", 
        monitoring_period,
        _monitor_rate_limits
    ), name='monitor_rate_limits_wrapper')
    return task

@log_task_metrics("report_metrics")
async def _report_metrics():
    """Lógica original da task"""
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
            "\n".join(metrics_report))
        
        # Reset counts for the new day
        task_metrics.error_counts.clear()
        task_metrics.success_counts.clear()
    except Exception as e:
        logger.error(f"Erro ao gerar relatório de métricas: {e}")

async def report_metrics():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 1  # Metrics reporting should run daily regardless of monitoring period
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "report_metrics", 
        monitoring_period,
        _report_metrics
    ), name='report_metrics_wrapper')
    return task

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
        start_time = time.time()
        sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
        perf_metrics.record_db_query(time.time() - start_time)
        
        # Verificar requisitos
        meets_requirements = False
        valid_days = set()
        
        if sessions:
            for session in sessions:
                if session['duration'] >= required_minutes * 60:
                    day = session['join_time'].replace(tzinfo=bot.timezone).date()
                    valid_days.add(day)
            
            meets_requirements = len(valid_days) >= required_days
        else:
            # Usuário não tem nenhuma sessão registrada - automaticamente não cumpre
            meets_requirements = False
        
        # Registrar verificação
        start_time = time.time()
        await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
        perf_metrics.record_db_query(time.time() - start_time)
        
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

async def check_missed_periods():
    """Verifica e processa períodos que deveriam ter sido verificados durante a queda do bot"""
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        return

    for guild in bot.guilds:
        # Obter todos os membros com cargos monitorados de forma mais eficiente
        members_with_roles = await bot.db.get_members_with_tracked_roles(guild.id, tracked_roles)
        if not members_with_roles:
            continue
            
        # Processar em lotes
        batch_size = bot._batch_processing_size
        for i in range(0, len(members_with_roles), batch_size):
            batch = members_with_roles[i:i + batch_size]
            await asyncio.gather(*[
                process_member_missed_periods(member_id, guild, required_minutes, 
                                            required_days, monitoring_period, tracked_roles)
                for member_id in batch
            ])
            await asyncio.sleep(bot.rate_limit_delay)

async def process_member_missed_periods(member_id: int, guild: discord.Guild, 
                                      required_minutes: int, required_days: int,
                                      monitoring_period: int, tracked_roles: List[int]):
    """Processa períodos perdidos para um único membro"""
    try:
        member = guild.get_member(member_id)
        if not member:
            return
            
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users'] or \
           any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
            return
            
        # Obter todos os períodos não verificados
        now = datetime.now(bot.timezone)
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        
        if not last_check:
            # Nunca foi verificado - criar um novo período
            new_period_end = now + timedelta(days=monitoring_period)
            await bot.db.log_period_check(member.id, guild.id, now, new_period_end, False)
            return
            
        # Calcular quantos períodos completos foram perdidos
        period_duration = timedelta(days=monitoring_period)
        last_period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
        missed_periods = []
        
        current_start = last_period_end
        while current_start < now:
            current_end = min(current_start + period_duration, now)
            missed_periods.append((current_start, current_end))
            current_start = current_end
            
        # Processar cada período perdido
        for period_start, period_end in missed_periods:
            # Verificar atividade no período
            sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
            meets_requirements = False
            valid_days = set()
            
            if sessions:
                for session in sessions:
                    if session['duration'] >= required_minutes * 60:
                        day = session['join_time'].replace(tzinfo=bot.timezone).date()
                        valid_days.add(day)
                
                meets_requirements = len(valid_days) >= required_days
            
            # Registrar verificação do período
            await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
            
            # Se não cumpriu, remover cargos
            if not meets_requirements and any(role.id in tracked_roles for role in member.roles):
                roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                
                try:
                    await member.remove_roles(*roles_to_remove)
                    await bot.send_warning(member, 'final')
                    await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                    
                    report_file = await generate_activity_report(member, sessions)
                    log_message = (
                        f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                        f"Sessões no período: {len(sessions)}\n"
                        f"Dias válidos: {len(valid_days)}/{required_days}\n"
                        f"Período: {period_start.strftime('%d/%m/%Y')} a {period_end.strftime('%d/%m/%Y')}"
                    )
                    
                    if report_file:
                        await bot.log_action(
                            "Cargo Removido (Período Perdido)",
                            member,
                            log_message,
                            file=report_file
                        )
                    else:
                        await bot.log_action(
                            "Cargo Removido (Período Perdido)",
                            member,
                            log_message
                        )
                    
                    await bot.notify_roles(
                        f"🚨 Cargos removidos de {member.mention} por inatividade no período perdido: " +
                        ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                    
                except discord.Forbidden:
                    await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                except Exception as e:
                    logger.error(f"Erro ao remover cargos de {member}: {e}")
        
        # Criar novo período atual
        new_period_end = now + timedelta(days=monitoring_period)
        await bot.db.log_period_check(member.id, guild.id, now, new_period_end, False)
            
    except Exception as e:
        logger.error(f"Erro ao verificar períodos perdidos para {member_id}: {e}")

@log_task_metrics("check_previous_periods")
async def _check_previous_periods():
    """Verifica usuários que não cumpriram requisitos em períodos anteriores"""
    await bot.wait_until_ready()
    
    logger.info("Iniciando verificação de períodos anteriores...")
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        logger.info("Nenhum cargo monitorado definido - verificação ignorada")
        return
    
    processed_members = 0
    members_with_roles_removed = 0
    
    for guild in bot.guilds:
        try:
            logger.info(f"Verificando guild: {guild.name} (ID: {guild.id})")
            
            # Obter todos os membros com cargos monitorados
            members_with_roles = []
            for member in guild.members:
                if any(role.id in tracked_roles for role in member.roles):
                    members_with_roles.append(member)
            
            logger.debug(f"Encontrados {len(members_with_roles)} membros com cargos monitorados")
            
            # Processar em lotes otimizados
            batch_size = bot._batch_processing_size
            for i in range(0, len(members_with_roles), batch_size):
                batch = members_with_roles[i:i + batch_size]
                results = await asyncio.gather(*[
                    process_member_previous_periods(member, guild, 
                                                  required_minutes, required_days,
                                                  monitoring_period, tracked_roles)
                    for member in batch
                ], return_exceptions=True)
                
                # Atualizar contadores
                for result in results:
                    if not isinstance(result, Exception):
                        processed_members += result.get('processed', 0)
                        members_with_roles_removed += result.get('removed', 0)
                
                # Pequeno delay entre lotes para evitar rate limits
                await asyncio.sleep(bot.rate_limit_delay)
                
        except Exception as e:
            logger.error(f"Erro ao verificar períodos anteriores na guild {guild.name}: {e}")
            continue
    
    logger.info(f"Verificação de períodos anteriores concluída. Membros processados: {processed_members}, Cargos removidos: {members_with_roles_removed}")

async def check_previous_periods():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "check_previous_periods", 
        monitoring_period,
        _check_previous_periods
    ), name='check_previous_periods_wrapper')
    return task

async def process_member_previous_periods(member: discord.Member, guild: discord.Guild,
                                        required_minutes: int, required_days: int,
                                        monitoring_period: int, tracked_roles: List[int]):
    """Processa um membro para verificar períodos anteriores"""
    result = {'processed': 0, 'removed': 0}
    
    try:
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users'] or \
           any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
            logger.debug(f"Usuário {member} está na whitelist - ignorando")
            return result
            
        # Verificar se tem cargos monitorados
        if not any(role.id in tracked_roles for role in member.roles):
            logger.debug(f"Usuário {member} não tem cargos monitorados - ignorando")
            return result
        
        result['processed'] = 1
        
        now = datetime.now(bot.timezone)
        
        # Obter todos os períodos verificados onde não cumpriu os requisitos
        start_time = time.time()
        async with bot.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    SELECT period_start, period_end 
                    FROM checked_periods
                    WHERE user_id = %s AND guild_id = %s
                    AND meets_requirements = FALSE
                    ORDER BY period_start
                ''', (member.id, guild.id))
                failed_periods = await cursor.fetchall()
        perf_metrics.record_db_query(time.time() - start_time)
        
        logger.debug(f"Usuário {member} tem {len(failed_periods)} períodos falhos")
        
        # Se houver períodos onde não cumpriu os requisitos
        if failed_periods:
            # Verificar se já teve cargos removidos para esses períodos
            start_time = time.time()
            async with bot.db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('''
                        SELECT role_id 
                        FROM removed_roles
                        WHERE user_id = %s AND guild_id = %s
                    ''', (member.id, guild.id))
                    already_removed = {r['role_id'] for r in await cursor.fetchall()}
            perf_metrics.record_db_query(time.time() - start_time)
            
            logger.debug(f"Usuário {member} já teve removidos: {already_removed}")
            
            # Verificar quais cargos monitorados ainda não foram removidos
            roles_to_remove = [
                role for role in member.roles 
                if role.id in tracked_roles and role.id not in already_removed
            ]
            
            if roles_to_remove:
                logger.info(f"Preparando para remover cargos de {member}: {[r.name for r in roles_to_remove]}")
                try:
                    # Remover cargos
                    start_time = time.time()
                    await member.remove_roles(*roles_to_remove)
                    perf_metrics.record_api_call(time.time() - start_time)
                    
                    # Enviar mensagem de aviso final via DM
                    await bot.send_warning(member, 'final')
                    
                    # Registrar cargos removidos
                    start_time = time.time()
                    await bot.db.log_removed_roles(
                        member.id, guild.id, 
                        [r.id for r in roles_to_remove]
                    )
                    perf_metrics.record_db_query(time.time() - start_time)
                    
                    # Gerar relatório gráfico com os períodos falhos
                    all_sessions = []
                    for period in failed_periods:
                        sessions = await bot.db.get_voice_sessions(
                            member.id, guild.id,
                            period['period_start'],
                            period['period_end']
                        )
                        all_sessions.extend(sessions)
                    
                    report_file = await generate_activity_report(member, all_sessions)
                    
                    log_message = (
                        f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                        f"Períodos falhos: {len(failed_periods)}\n"
                        f"Primeiro período falho: {failed_periods[0]['period_start'].strftime('%d/%m/%Y')}\n"
                        f"Último período falho: {failed_periods[-1]['period_end'].strftime('%d/%m/%Y')}"
                    )
                    
                    if report_file:
                        await bot.log_action(
                            "Cargo Removido (Períodos Anteriores)",
                            member,
                            log_message,
                            file=report_file
                        )
                    else:
                        await bot.log_action(
                            "Cargo Removido (Períodos Anteriores)",
                            member,
                            log_message
                        )
                    
                    await bot.notify_roles(
                        f"🚨 Cargos removidos de {member.mention} por inatividade em períodos anteriores: " +
                        ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                    
                    result['removed'] = 1
                    
                except discord.Forbidden:
                    await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                except Exception as e:
                    logger.error(f"Erro ao remover cargos de {member}: {e}")
    
    except Exception as e:
        logger.error(f"Erro ao verificar períodos anteriores para {member}: {e}")
    
    return result