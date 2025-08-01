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
import pytz

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
        self.max_concurrent = 5  # Limite de operações concorrentes

    async def process_inactivity_batch(self, members: list[discord.Member]):
        """Processa um lote de membros de uma vez com limite concorrente."""
        if not members:
            return []
            
        # Usar semáforo para limitar concorrência
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_member(member):
            async with semaphore:
                return await self._process_member_optimized(member, {})
        
        # Processar em lotes menores
        batch_size = min(self.batcher.batch_size, 10)
        results = []
        
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            batch_results = await asyncio.gather(
                *(process_member(member) for member in batch),
                return_exceptions=True
            )
            results.extend(batch_results)
            await asyncio.sleep(self.bot.rate_limit_delay)  # Delay entre lotes
            
        return results

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
            
            now = datetime.now(pytz.UTC)
            
            # Obter a data em que o usuário recebeu o cargo monitorado mais recente
            role_assignment_times = []
            for role in member.roles:
                if role.id in tracked_roles:
                    try:
                        assigned_time = await self.bot.db.get_role_assigned_time(member.id, member.guild.id, role.id)
                        if assigned_time:
                            role_assignment_times.append(assigned_time)
                        else:
                            # Se não tem registro, criar um com a data atual
                            await self.bot.db.log_role_assignment(member.id, member.guild.id, role.id)
                            role_assignment_times.append(now)
                            logger.info(f"Criado registro de atribuição para cargo {role.id} do usuário {member.id}")
                    except Exception as e:
                        logger.error(f"Erro ao obter data de atribuição para cargo {role.id}: {e}")
                        continue
            
            if not role_assignment_times:
                # Se não encontrou nenhuma atribuição, registrar todas com a data atual
                for role in member.roles:
                    if role.id in tracked_roles:
                        try:
                            await self.bot.db.log_role_assignment(member.id, member.guild.id, role.id)
                            role_assignment_times.append(now)
                        except Exception as e:
                            logger.error(f"Erro ao registrar atribuição de cargo {role.id}: {e}")
                            continue
                
                if not role_assignment_times:
                    return result
            
            role_assignment_time = min(role_assignment_times)  # Usar a atribuição mais antiga
            
            monitoring_period = self.bot.config['monitoring_period']
            period_end = role_assignment_time + timedelta(days=monitoring_period)
            
            # Se o período ainda não terminou, não fazer nada
            if now < period_end:
                return result
                
            # Se o período acabou, verificar atividade
            period_start = period_end - timedelta(days=monitoring_period)
            
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
                        day = session['join_time'].date()
                        valid_days.add(day)
                
                meets_requirements = len(valid_days) >= required_days
            
            roles_to_remove = []
            # Ações para quem não cumpriu os requisitos
            if not meets_requirements:
                roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                
                if roles_to_remove:
                    try:
                        # Verificar permissões
                        if not member.guild.me.guild_permissions.manage_roles:
                            raise discord.Forbidden("Bot não tem permissão para gerenciar cargos")
                            
                        # Verificar hierarquia de cargos
                        top_role = member.guild.me.top_role
                        for role in roles_to_remove:
                            if role >= top_role:
                                raise discord.Forbidden(f"Não posso remover cargo {role.name} - acima da minha hierarquia")
                        
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
                        
                        # Notificar administradores por DM
                        admin_embed = discord.Embed(
                            title="🚨 Cargos Removidos por Inatividade",
                            description=f"Os cargos de {member.mention} foram removidos por inatividade.",
                            color=discord.Color.dark_red(),
                            timestamp=datetime.now(pytz.utc)
                        )
                        admin_embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                        admin_embed.add_field(name="Usuário", value=f"{member.mention} (`{member.id}`)", inline=False)
                        admin_embed.add_field(name="Cargos Removidos", value=", ".join([r.mention for r in roles_to_remove]), inline=False)
                        admin_embed.set_footer(text=f"Servidor: {member.guild.name}")
                        
                        await self.bot.notify_admins_dm(member.guild, embed=admin_embed)

                        # Registrar cargos removidos
                        start_time = time.time()
                        await self.bot.db.log_removed_roles(
                            member.id, member.guild.id, 
                            [r.id for r in roles_to_remove]
                        )
                        perf_metrics.record_db_query(time.time() - start_time)
                        
                        log_message = (
                            f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                            f"Sessões no período: {len(sessions)}\n"
                            f"Dias válidos: {len(valid_days)}/{required_days}\n"
                            f"Período: {period_start.strftime('%d/%m/%Y')} a {period_end.strftime('%d/%m/%Y')}"
                        )
                        
                        await self.bot.log_action(
                            "Cargo Removido",
                            member,
                            log_message
                        )
                        
                        await self.bot.notify_roles(
                            f"🚨 Cargos removidos de {member.mention} por inatividade: " +
                            ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                        
                        result['removed'] = 1
                        
                    except discord.Forbidden as e:
                        logger.error(f"Permissões insuficientes para remover cargos de {member}: {e}")
                        await self.bot.log_action("Erro ao Remover Cargo", member, f"Permissões insuficientes: {e}")
                    except Exception as e:
                        logger.error(f"Erro ao remover cargos de {member}: {e}")
            else:
                # Registrar que cumpriu os requisitos
                await self.bot.db.log_period_check(
                    member.id, member.guild.id, 
                    period_start, period_end, 
                    meets_requirements
                )
            
            # CORREÇÃO: A lógica para decidir sobre um novo período deve verificar se, APÓS a remoção,
            # o usuário AINDA possui algum cargo monitorado.
            
            # Obter o conjunto de cargos que o membro tem atualmente (pode estar desatualizado)
            current_member_roles_ids = {r.id for r in member.roles}
            # Obter o conjunto de cargos que acabamos de remover
            removed_roles_ids = {r.id for r in roles_to_remove}
            # Calcular os cargos restantes
            remaining_roles_ids = current_member_roles_ids - removed_roles_ids
            
            # Verificar se algum dos cargos restantes é um cargo monitorado
            still_has_tracked_roles = any(role_id in tracked_roles for role_id in remaining_roles_ids)

            if now >= period_end and still_has_tracked_roles:
                # Definir novo período de verificação (futuro)
                new_period_end = now + timedelta(days=monitoring_period)
                new_period_start = now
                
                # Registrar novo período de verificação
                await self.bot.db.log_period_check(
                    member.id, member.guild.id, 
                    new_period_start, new_period_end, 
                    False  # Assume que começa não cumprindo
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

async def get_time_without_roles(member: discord.Member) -> Optional[timedelta]:
    """Obtém há quanto tempo o membro está sem cargos (exceto @everyone)"""
    try:
        # Se o membro tem mais que o cargo @everyone (len=1)
        if len(member.roles) > 1:
            return None
            
        # Obter a data em que o membro perdeu todos os cargos (exceto @everyone)
        last_role_removal = await bot.db.get_last_role_removal(member.id, member.guild.id)
        
        # CORREÇÃO: Verificar se last_role_removal e a data existem
        if last_role_removal and last_role_removal.get('removal_date'):
            removal_date = last_role_removal['removal_date']
            if removal_date.tzinfo is None:
                removal_date = removal_date.replace(tzinfo=pytz.UTC)
            return datetime.now(pytz.UTC) - removal_date
            
        # Se não há registro de remoção de cargos, usar a data de entrada no servidor
        if member.joined_at:
            joined_at = member.joined_at.replace(tzinfo=pytz.UTC) if member.joined_at.tzinfo is None else member.joined_at
            return datetime.now(pytz.UTC) - joined_at
            
        return None
    except Exception as e:
        logger.error(f"Erro ao verificar tempo sem cargos para {member}: {e}", exc_info=True) # Adicionado exc_info
        return None

async def voice_event_processor():
    """Processa eventos de voz da fila principal"""
    await bot.wait_until_ready()
    while True:
        try:
            event = await bot.voice_event_queue.get()
            await bot._process_voice_batch([event])
            bot.voice_event_queue.task_done()
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Erro no processador de eventos de voz: {e}")
            await asyncio.sleep(1)

async def execute_task_with_persistent_interval(task_name: str, monitoring_period: int, task_func: callable, force_check: bool = False):
    """Executa a task mantendo intervalo persistente de 24h"""
    await bot.wait_until_ready()
    
    # Esperar até que o banco de dados esteja inicializado
    while not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        await asyncio.sleep(5)
        logger.info(f"Aguardando inicialização do banco de dados para a task {task_name}...")
    
    while True:
        try:
            # Verificar última execução
            last_exec = None
            if hasattr(bot.db, 'get_last_task_execution'):
                try:
                    last_exec = await bot.db.get_last_task_execution(task_name)
                except AttributeError:
                    logger.warning(f"Método get_last_task_execution não disponível no banco de dados")
            
            now = datetime.now(pytz.UTC)  # Usar UTC
            
            # Calcular se deve executar agora
            should_execute = False
            
            if not last_exec:  # Primeira execução
                should_execute = True
                logger.info(f"Primeira execução da task {task_name}")
            else:
                # Garantir que last_exec_time está com timezone (aware)
                last_exec_time = last_exec['last_execution']
                if last_exec_time.tzinfo is None:
                    last_exec_time = last_exec_time.replace(tzinfo=pytz.UTC)
                
                time_since_last = now - last_exec_time
                if time_since_last >= timedelta(hours=24) or force_check:
                    should_execute = True
                    logger.info(f"24h passaram - executando task {task_name}")
            
            if should_execute:
                logger.info(f"Executando task {task_name}...")
                start_time = time.time()
                await task_func()
                perf_metrics.record_task_execution(task_name, time.time() - start_time)

                # Registrar execução se o método estiver disponível
                if hasattr(bot.db, 'log_task_execution'):
                    try:
                        period_to_log = monitoring_period
                        if task_name in ['inactivity_check', 'check_warnings', 'cleanup_members', 'check_previous_periods']:
                            period_to_log = bot.config.get('monitoring_period', monitoring_period)
                        
                        await bot.db.log_task_execution(task_name, period_to_log)
                    except AttributeError:
                        logger.warning(f"Método log_task_execution não disponível no banco de dados")
                
                logger.info(f"Task {task_name} concluída com sucesso")
            
            # Esperar 1h antes de verificar novamente (evita loops muito rápidos)
            await asyncio.sleep(3600)
                
        except Exception as e:
            logger.error(f"Erro na task {task_name}: {e}", exc_info=True)
            await asyncio.sleep(3600)  # Esperar 1h antes de tentar novamente

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
    
    # Limpar filas e reinicializar contadores
    await bot.clear_queues()
    
    await bot.log_action("Reconexão", None, "Bot reconectado após queda - Filas reinicializadas")
    await check_missed_periods()
    await process_pending_voice_events()

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
        # Verificar se o banco de dados está disponível
        if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
            logger.error("Banco de dados não inicializado - pulando salvamento de estados")
            return

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
        # Verificar se o banco de dados está disponível
        if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
            logger.error("Banco de dados não inicializado - pulando carregamento de estados")
            return

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
        if not hasattr(bot, 'db_backup') or bot.db_backup is None:
            try:
                if not hasattr(bot, 'db') or not bot.db:
                    logger.error("Banco de dados não disponível para backup")
                    return False
                    
                from database import DatabaseBackup
                bot.db_backup = DatabaseBackup(bot.db)
                await asyncio.sleep(1)  # Garante inicialização
            except Exception as e:
                logger.error(f"Erro ao criar instância de DatabaseBackup: {e}")
                return False
        
        try:
            if hasattr(bot.db_backup, 'create_backup'):
                start_time = time.time()
                success = await bot.db_backup.create_backup()
                perf_metrics.record_db_query(time.time() - start_time)
                
                if success:
                    logger.info("Backup emergencial concluído com sucesso.")
                    return True
                else:
                    logger.error("Backup emergencial falhou.")
                    return False
            else:
                logger.error("Método create_backup não disponível no DatabaseBackup")
                return False
        except Exception as e:
            logger.error(f"Falha no backup emergencial: {e}")
            return False
    except Exception as e:
        logger.error(f"Erro inesperado durante backup emergencial: {e}")
        return False

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
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando verificação de saúde")
        return

    try:
        # Verificar última execução das tasks críticas
        critical_tasks = ['inactivity_check', 'check_warnings', 'cleanup_members', 'check_previous_periods']
        for task_name in critical_tasks:
            last_exec = await bot.db.get_last_task_execution(task_name)
            if last_exec:
                last_exec_time = last_exec['last_execution']
                if last_exec_time.tzinfo is None:
                    last_exec_time = last_exec_time.replace(tzinfo=pytz.UTC)
                
                time_since_last = datetime.now(pytz.UTC) - last_exec_time
                if time_since_last > timedelta(hours=26):  # 2 horas de tolerância
                    logger.warning(f"Task {task_name} não executou nos últimos {time_since_last}")
                    await bot.log_action(
                        "Alerta de Task",
                        None,
                        f"Task {task_name} não executou nos últimos {time_since_last}"
                    )
        
        # Obter todas as tasks ativas por nome
        active_tasks = {t.get_name() for t in asyncio.all_tasks() if t.get_name()}
        
        expected_tasks = {
            'inactivity_check_wrapper',
            'check_warnings_wrapper', 
            'cleanup_members_wrapper',
            'database_backup_wrapper',
            'cleanup_old_data_wrapper',
            'monitor_rate_limits_wrapper',
            'report_metrics_wrapper',
            'health_check_wrapper',
            'check_previous_periods_wrapper',
            'queue_processor',
            'db_pool_monitor',
            'periodic_health_check',
            'audio_state_checker',
            'process_pending_voice_events',
            'check_current_voice_members',
            'detect_missing_voice_leaves',
            'cleanup_ghost_sessions_wrapper',
            'register_role_assignments_wrapper'
        }

        for task_name in expected_tasks:
            if task_name not in active_tasks:
                logger.warning(f"Task {task_name} não está ativa - reiniciando...")
                if task_name == 'inactivity_check_wrapper':
                    asyncio.create_task(inactivity_check(), name='inactivity_check_wrapper')
                elif task_name == 'check_warnings_wrapper':
                    asyncio.create_task(check_warnings(), name='check_warnings_wrapper')
                elif task_name == 'cleanup_members_wrapper':
                    asyncio.create_task(cleanup_members(), name='cleanup_members_wrapper')
                elif task_name == 'database_backup_wrapper':
                    asyncio.create_task(database_backup(), name='database_backup_wrapper')
                elif task_name == 'cleanup_old_data_wrapper':
                    asyncio.create_task(cleanup_old_data(), name='cleanup_old_data_wrapper')
                elif task_name == 'monitor_rate_limits_wrapper':
                    asyncio.create_task(monitor_rate_limits(), name='monitor_rate_limits_wrapper')
                elif task_name == 'report_metrics_wrapper':
                    asyncio.create_task(report_metrics(), name='report_metrics_wrapper')
                elif task_name == 'health_check_wrapper':
                    asyncio.create_task(health_check(), name='health_check_wrapper')
                elif task_name == 'check_previous_periods_wrapper':
                    asyncio.create_task(check_previous_periods(), name='check_previous_periods_wrapper')
                elif task_name == 'queue_processor':
                    asyncio.create_task(bot.process_queues(), name='queue_processor')
                elif task_name == 'db_pool_monitor':
                    asyncio.create_task(bot.monitor_db_pool(), name='db_pool_monitor')
                elif task_name == 'periodic_health_check':
                    asyncio.create_task(bot.periodic_health_check(), name='periodic_health_check')
                elif task_name == 'audio_state_checker':
                    asyncio.create_task(bot.check_audio_states(), name='audio_state_checker')
                elif task_name == 'process_pending_voice_events':
                    asyncio.create_task(process_pending_voice_events(), name='process_pending_voice_events')
                elif task_name == 'check_current_voice_members':
                    asyncio.create_task(check_current_voice_members(), name='check_current_voice_members')
                elif task_name == 'detect_missing_voice_leaves':
                    asyncio.create_task(detect_missing_voice_leaves(), name='detect_missing_voice_leaves')
                elif task_name == 'cleanup_ghost_sessions_wrapper':
                    asyncio.create_task(cleanup_ghost_sessions(), name='cleanup_ghost_sessions_wrapper')
                elif task_name == 'register_role_assignments_wrapper':
                    asyncio.create_task(register_role_assignments(), name='register_role_assignments_wrapper')

        await bot.log_action("Verificação de Saúde", None, f"Tasks ativas: {', '.join(t for t in active_tasks if t)}")
    except Exception as e:
        logger.error(f"Erro na verificação de saúde: {e}")

@log_task_metrics("inactivity_check")
async def _inactivity_check():
    """Verifica a inatividade dos membros e remove cargos se necessário"""
    await bot.wait_until_ready()
    
    # Verificar configuração essencial
    if 'tracked_roles' not in bot.config or not bot.config['tracked_roles']:
        logger.info("Nenhum cargo monitorado definido - verificação de inatividade ignorada")
        return
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando verificação de inatividade")
        return
    
    # Log de depuração para configurações
    logger.debug(f"Configuração atual: {bot.config}")
    logger.debug(f"Monitoring period: {bot.config.get('monitoring_period')}")
    
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
            results = await processor.process_inactivity_batch(prioritized_members)
            
            # Atualizar contadores
            for result in results:
                if not isinstance(result, Exception):
                    processed_members += result.get('processed', 0)
                    members_with_roles_removed += result.get('removed', 0)
                
        except Exception as e:
            logger.error(f"Erro ao verificar inatividade na guild {guild.name}: {e}")
            continue
    
    logger.info(f"Verificação de inatividade concluída. Membros processados: {processed_members}, Cargos removidos: {members_with_roles_removed}")

async def inactivity_check():
    """Wrapper para a task com intervalo de 24h"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(
        execute_task_with_persistent_interval(
            "inactivity_check", 
            monitoring_period,
            _inactivity_check
        ), 
        name='inactivity_check_wrapper'
    )
    return task

@log_task_metrics("check_warnings")
async def _check_warnings():
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    # Verificar configuração essencial
    if 'tracked_roles' not in bot.config or not bot.config['tracked_roles'] or 'warnings' not in bot.config:
        logger.info("Cargos monitorados ou configurações de aviso não definidos - verificação ignorada")
        return
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando verificação de avisos")
        return
    
    # Log de depuração para configurações
    logger.debug(f"Configuração atual: {bot.config}")
    logger.debug(f"Monitoring period: {bot.config.get('monitoring_period')}")
    
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
        
        # Obter data de atribuição do cargo mais antigo
        role_assignment_times = []
        for role in member.roles:
            if role.id in tracked_roles:
                try:
                    assigned_time = await bot.db.get_role_assigned_time(member.id, guild.id, role.id)
                    if assigned_time:
                        role_assignment_times.append(assigned_time)
                    else:
                        # Se não tem registro, criar um com a data atual
                        await bot.db.log_role_assignment(member.id, guild.id, role.id)
                        assigned_time = datetime.now(pytz.UTC)
                        role_assignment_times.append(assigned_time)
                except Exception as e:
                    logger.error(f"Erro ao obter data de atribuição para cargo {role.id}: {e}")
                    continue
        
        if not role_assignment_times:
            # Se não encontrou data de atribuição, registrar agora e usar a data atual
            for role in member.roles:
                if role.id in tracked_roles:
                    try:
                        await bot.db.log_role_assignment(member.id, guild.id, role.id)
                        assigned_time = datetime.now(pytz.UTC)
                        role_assignment_times.append(assigned_time)
                    except Exception as e:
                        logger.error(f"Erro ao registrar atribuição de cargo {role.id}: {e}")
                        continue
            
            if not role_assignment_times:
                return
        
        role_assignment_time = min(role_assignment_times)  # Usar a atribuição mais antiga
        
        # Obter última verificação
        start_time = time.time()
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        perf_metrics.record_db_query(time.time() - start_time)
        
        if not last_check:
            # Se não tem verificação anterior, criar um novo período
            monitoring_period = bot.config['monitoring_period']
            period_end = role_assignment_time + timedelta(days=monitoring_period)
            await bot.db.log_period_check(
                member.id, guild.id, 
                role_assignment_time,
                period_end, 
                False
            )
            return
        
        # Calcular dias restantes e obter datas do período
        period_start = last_check['period_start']
        if period_start.tzinfo is None:
             period_start = period_start.replace(tzinfo=pytz.UTC)
        period_end = last_check['period_end']
        if period_end.tzinfo is None:  # Garantir timezone
            period_end = period_end.replace(tzinfo=pytz.UTC)
        
        now = datetime.now(pytz.UTC)
        if period_end < now:
            # Período já terminou, não enviar avisos
            return
            
        days_remaining = (period_end - now).days
            
        # CORREÇÃO: Usar a nova função e refinar a lógica de verificação
        # Obter último aviso no período atual
        # NOTA: Assumimos que bot.db.get_last_warning_in_period(user, guild, period_start) foi implementado em database.py
        # Esta função deve retornar uma tupla (warning_type, warning_date) ou None.
        last_warning_in_period = await bot.db.get_last_warning_in_period(member.id, guild.id, period_start)
        last_warning_type = last_warning_in_period[0] if last_warning_in_period else None
        
        # Condição para o primeiro aviso
        if days_remaining <= first_warning_days and not last_warning_in_period:
            await bot.send_warning(member, 'first')
            warnings_sent['first'] += 1
        
        # Condição para o segundo aviso
        elif days_remaining <= second_warning_days and last_warning_type == 'first':
            await bot.send_warning(member, 'second')
            warnings_sent['second'] += 1
            
    except Exception as e:
        logger.error(f"Erro ao verificar avisos para {member}: {e}")

@log_task_metrics("cleanup_members")
async def _cleanup_members(force_check: bool = False):
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando limpeza de membros")
        return
    
    # Log de depuração para configurações
    logger.debug(f"Configuração atual: {bot.config}")
    logger.debug(f"Kick after days: {bot.config.get('kick_after_days')}")
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        logger.info("Expulsão de membros inativos desativada na configuração")
        return
    
    members_kicked = 0
    batch_size = bot._batch_processing_size
    
    for guild in bot.guilds:
        members = prioritize_members(list(guild.members))  # Priorizar membros mais antigos/sem cargos
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            results = await asyncio.gather(*[
                process_member_cleanup(member, guild, kick_after_days) 
                for member in batch
            ])
            
            members_kicked += sum(results)  # Soma os resultados booleanos (True = 1, False = 0)
            
            await asyncio.sleep(bot.rate_limit_delay)
    
    logger.info(f"Limpeza de membros concluída. Membros expulsos: {members_kicked}")

async def cleanup_members(force_check: bool = False):
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = bot.config['monitoring_period']
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_members", 
        monitoring_period,
        _cleanup_members,
        force_check=force_check
    ), name='cleanup_members_wrapper')
    return task

async def process_member_cleanup(member: discord.Member, guild: discord.Guild, 
                               kick_after_days: int):
    """Process cleanup for a single member and returns whether member was kicked"""
    try:
        # Verificar whitelist (usuários isentos)
        if member.id in bot.config['whitelist']['users']:
            return False
            
        # Verificar se tem apenas @everyone (len=1) ou nenhum cargo (len=0)
        if len(member.roles) <= 1:  
            # Obter há quanto tempo está sem cargos
            time_without_roles = await get_time_without_roles(member)
            
            if not time_without_roles:
                return False
                
            # Verificar se o membro ainda está no servidor
            if member not in guild.members:
                return False
                
            # Verificar se já foi expulso recentemente
            last_kick = await bot.db.get_last_kick(member.id, guild.id)
            if last_kick and last_kick.get('kick_date') and (datetime.now(pytz.UTC) - last_kick['kick_date']).days < kick_after_days:
                return False
                
            # 🔴 **LÓGICA PRINCIPAL**: Expulsar se passou mais tempo que kick_after_days sem cargos
            if time_without_roles >= timedelta(days=kick_after_days):
                try:
                    # Verificar permissões antes de tentar expulsar
                    if not guild.me.guild_permissions.kick_members:
                        await bot.log_action("Erro ao Expulsar", member, "Bot não tem permissão para expulsar membros")
                        return False
                        
                    if guild.me.top_role <= member.top_role:
                        await bot.log_action("Erro ao Expulsar", member, "Hierarquia de cargos impede a expulsão")
                        return False

                    await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                    
                    # Notificar administradores por DM
                    admin_embed = discord.Embed(
                        title="👢 Membro Expulso por Inatividade",
                        description=f"{member.mention} foi expulso do servidor.",
                        color=discord.Color.from_rgb(156, 39, 176), # Roxo
                        timestamp=datetime.now(pytz.utc)
                    )
                    admin_embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                    admin_embed.add_field(name="Usuário", value=f"{member.mention} (`{member.id}`)", inline=False)
                    admin_embed.add_field(name="Motivo", value=f"Sem cargos monitorados por mais de {kick_after_days} dias.", inline=False)
                    admin_embed.set_footer(text=f"Servidor: {guild.name}")
                    
                    await bot.notify_admins_dm(guild, embed=admin_embed)

                    # Registrar no banco de dados
                    await bot.db.log_kicked_member(
                        member.id, guild.id, 
                        f"Sem cargos há mais de {kick_after_days} dias"
                    )
                    
                    # Logar a ação
                    await bot.log_action(
                        "Membro Expulso",
                        member,
                        f"Motivo: Sem cargos há mais de {kick_after_days} dias\n"
                        f"Tempo sem cargos: {time_without_roles.days} dias"
                    )
                    await bot.notify_roles(
                        f"👢 {member.mention} foi expulso por estar sem cargos há mais de {kick_after_days} dias")
                    
                    return True
                    
                except discord.Forbidden:
                    await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
                except discord.HTTPException as e:
                    await bot.log_action("Erro ao Expulsar", member, f"Erro HTTP: {e}")
                except Exception as e:
                    logger.error(f"Erro ao expulsar membro {member}: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro ao verificar membro para expulsão {member}: {e}")
        return False

@log_task_metrics("database_backup")
async def _database_backup():
    """Lógica original da task"""
    await bot.wait_until_ready()
    
    if not bot.db or not bot.db._is_initialized:
        logger.error("Banco não inicializado - pulando backup")
        return
        
    try:
        if not hasattr(bot, 'db_backup') or bot.db_backup is None:
            from database import DatabaseBackup
            bot.db_backup = DatabaseBackup(bot.db)
            await asyncio.sleep(1)  # Garante inicialização
            
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
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando limpeza de dados antigos")
        return
    
    try:
        # Usar UTC para o cutoff_date
        cutoff_date = datetime.now(pytz.UTC) - timedelta(days=60)
        
        # Processar em lotes menores para evitar timeout
        tables_to_clean = [
            ('voice_sessions', 'leave_time'),
            ('user_warnings', 'warning_date'),
            ('removed_roles', 'removal_date'),
            ('kicked_members', 'kick_date'),
            ('rate_limit_logs', 'log_date'),
            ('pending_voice_events', 'event_time'),
            ('role_assignments', 'assigned_at')
        ]
        
        deleted_counts = {}
        
        for table, date_field in tables_to_clean:
            try:
                start_time = time.time()
                # Usar a sintaxe correta do PostgreSQL para DELETE com LIMIT
                if table == 'voice_sessions':
                    result = await bot.db.execute_query(
                        f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE {date_field} < $1 ORDER BY id LIMIT 1000)",
                        (cutoff_date,)
                    )
                else:
                    result = await bot.db.execute_query(
                        f"DELETE FROM {table} WHERE {date_field} < $1",
                        (cutoff_date,)
                    )
                perf_metrics.record_db_query(time.time() - start_time)
                deleted_counts[table] = int(result.split()[1])
                logger.info(f"Removidos {deleted_counts[table]} registros de {table}")
            except Exception as e:
                logger.error(f"Erro ao limpar tabela {table}: {e}")
                deleted_counts[table] = 0
        
        log_message = (
            f"Limpeza de dados antigos concluída: " +
            ", ".join([f"{table}: {count}" for table, count in deleted_counts.items()])
        )
        
        await bot.log_action("Limpeza de Dados", None, log_message)
        return log_message
        
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await bot.log_action(
            "Erro na Limpeza de Dados", 
            None, 
            f"Falha ao limpar dados antigos: {str(e)}"
        )

async def cleanup_old_data():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 7  # Limpeza de dados deve rodar semanalmente
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
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando relatório de métricas")
        return
    
    try:
        metrics_report = []
        for task_name in ['inactivity_check', 'check_warnings', 'cleanup_members', 
                         'database_backup', 'cleanup_old_data', 'monitor_rate_limits',
                         'cleanup_ghost_sessions', 'register_role_assignments']:
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
        # Verificar se o banco de dados está disponível
        if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
            logger.error("Banco de dados não inicializado - pulando verificação forçada")
            return None

        guild = member.guild
        required_minutes = bot.config['required_minutes']
        required_days = bot.config['required_days']
        monitoring_period = bot.config['monitoring_period']
        
        # Definir período de verificação em UTC
        period_end = datetime.now(pytz.UTC)
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
                    day = session['join_time'].date()  # Já está em UTC
                    valid_days.add(day)
            
            meets_requirements = len(valid_days) >= required_days
        
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
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando verificação de períodos perdidos")
        return
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        return

    for guild in bot.guilds:
        # Obter todos os membros com cargos monitorados de forma mais eficiente
        if hasattr(bot.db, 'get_members_with_tracked_roles'):
            members_with_roles = await bot.db.get_members_with_tracked_roles(guild.id, tracked_roles)
        else:
            # Fallback: verificar manualmente
            members_with_roles = [m.id for m in guild.members if any(r.id in tracked_roles for r in m.roles)]
            
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
        now = datetime.now(pytz.UTC)
        last_check = await bot.db.get_last_period_check(member.id, guild.id)
        
        if not last_check:
            # Nunca foi verificado - criar um novo período
            new_period_end = now + timedelta(days=monitoring_period)
            await bot.db.log_period_check(member.id, guild.id, now, new_period_end, False)
            return
            
        # Calcular quantos períodos completos foram perdidos
        period_duration = timedelta(days=monitoring_period)
        last_period_end = last_check['period_end']
        missed_periods = []
        
        current_start = last_period_end
        while current_start < now:
            current_end = min(current_start + period_duration, now)
            missed_periods.append((current_start, current_end))
            current_start = current_end
        
        initial_roles_ids = {r.id for r in member.roles}
        cumulatively_removed_roles_ids = set()

        # Processar cada período perdido
        for period_start, period_end in missed_periods:
            # Verificar atividade no período
            sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
            meets_requirements = False
            valid_days = set()
            
            if sessions:
                for session in sessions:
                    if session['duration'] >= required_minutes * 60:
                        day = session['join_time'].date()  # Já está em UTC
                        valid_days.add(day)
                
                meets_requirements = len(valid_days) >= required_days
            
            # Registrar verificação do período
            await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
            
            # Se não cumpriu, remover cargos
            if not meets_requirements and any(role.id in tracked_roles for role in member.roles):
                roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                cumulatively_removed_roles_ids.update(r.id for r in roles_to_remove)
                
                try:
                    await member.remove_roles(*roles_to_remove)
                    await bot.send_warning(member, 'final')
                    await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                    
                    # Notificar administradores por DM
                    admin_embed = discord.Embed(
                        title="🚨 Cargos Removidos (Período Perdido)",
                        description=f"Os cargos de {member.mention} foram removidos por inatividade durante um período em que o bot esteve offline.",
                        color=discord.Color.dark_red(),
                        timestamp=datetime.now(pytz.utc)
                    )
                    admin_embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                    admin_embed.add_field(name="Usuário", value=f"{member.mention} (`{member.id}`)", inline=False)
                    admin_embed.add_field(name="Cargos Removidos", value=", ".join([r.mention for r in roles_to_remove]), inline=False)
                    admin_embed.set_footer(text=f"Servidor: {guild.name}")
                    
                    await bot.notify_admins_dm(guild, embed=admin_embed)

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
        
        # CORREÇÃO: A lógica para decidir sobre um novo período deve verificar se, APÓS a remoção,
        # o usuário AINDA possui algum cargo monitorado.
        remaining_roles_ids = initial_roles_ids - cumulatively_removed_roles_ids
        still_has_tracked_roles = any(role_id in tracked_roles for role_id in remaining_roles_ids)
        
        if still_has_tracked_roles:
            new_period_end = now + timedelta(days=monitoring_period)
            await bot.db.log_period_check(member.id, guild.id, now, new_period_end, False)
            
    except Exception as e:
        logger.error(f"Erro ao verificar períodos perdidos para {member_id}: {e}")

@log_task_metrics("check_previous_periods")
async def _check_previous_periods():
    """Verifica usuários que não cumpriram requisitos em períodos anteriores"""
    await bot.wait_until_ready()
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando verificação de períodos anteriores")
        return
    
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
                    process_member_previous_periods(member, guild, required_minutes, required_days,
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
    """Processa um membro para verificar períodos anteriores com lógica de perdão de dívida."""
    result = {'processed': 0, 'removed': 0}
    
    try:
        # Verificar whitelist
        if member.id in bot.config['whitelist']['users'] or \
           any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
            return result
            
        # Verificar se tem cargos monitorados
        if not any(role.id in tracked_roles for role in member.roles):
            return result
        
        result['processed'] = 1
        
        now = datetime.now(pytz.UTC)
        
        # Obter data de atribuição do cargo mais antigo para contexto
        role_assignment_time = now
        try:
            async with bot.db.pool.acquire() as conn:
                fetched_time = await conn.fetchval('''
                    SELECT MIN(assigned_at) FROM role_assignments
                    WHERE user_id = $1 AND guild_id = $2 AND role_id = ANY($3)
                ''', member.id, guild.id, tracked_roles)
            if fetched_time:
                role_assignment_time = fetched_time
        except Exception as e:
            logger.error(f"Erro ao obter data de atribuição inicial para {member.display_name}: {e}")

        # Obter todos os períodos verificados onde não cumpriu os requisitos
        failed_periods = []
        try:
            async with bot.db.pool.acquire() as conn:
                failed_periods = await conn.fetch('''
                    SELECT period_start, period_end 
                    FROM checked_periods
                    WHERE user_id = $1 AND guild_id = $2
                    AND meets_requirements = FALSE
                    AND period_end <= $3    -- Apenas períodos que já terminaram
                    ORDER BY period_start
                ''', member.id, guild.id, now)
        except Exception as e:
            logger.error(f"Erro ao buscar períodos falhos para {member}: {e}")
            return result
        
        if failed_periods:
            roles_to_remove = [
                role for role in member.roles if role.id in tracked_roles
            ]

            if not roles_to_remove:
                return result

            should_remove_roles = False
            first_failed_period = None
            
            for period in failed_periods:
                period_start = period['period_start'].replace(tzinfo=pytz.utc)
                period_end = period['period_end'].replace(tzinfo=pytz.utc)

                sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
                
                valid_days = {session['join_time'].date() for session in sessions if session['duration'] >= required_minutes * 60}
                
                meets_requirements_on_recheck = len(valid_days) >= required_days

                if not meets_requirements_on_recheck:
                    # <<< INÍCIO DA NOVA LÓGICA >>>

                    # Obter a data de atribuição mais RECENTE dos cargos monitorados que o membro possui.
                    most_recent_assignment_date = None
                    assignment_dates = []
                    for role in roles_to_remove:
                        assigned_at = await bot.db.get_role_assigned_time(member.id, guild.id, role.id)
                        if assigned_at:
                            assignment_dates.append(assigned_at)
                    
                    if assignment_dates:
                        most_recent_assignment_date = max(assignment_dates)

                    # A NOVA CONDIÇÃO: A remoção só ocorre se o cargo foi atribuído ANTES do fim do período de falha.
                    if most_recent_assignment_date and most_recent_assignment_date < period_end:
                        # JUSTO: O membro tinha o cargo durante o período de falha. A remoção é válida.
                        should_remove_roles = True
                        first_failed_period = period
                        break # Encontrou uma falha válida, pode parar de verificar.
                    else:
                        # PERDÃO: O cargo foi atribuído DEPOIS da falha. Perdoar a dívida.
                        logger.info(f"Dívida de inatividade perdoada para {member.display_name}. Cargo atribuído ({most_recent_assignment_date}) após o fim do período de falha ({period_end}).")
                        
                        # Marca o período antigo como "OK" para não ser verificado de novo.
                        await bot.db.log_period_check(member.id, guild.id, period_start, period_end, True)
                        
                        # Inicia um novo ciclo de monitoramento a partir da data de atribuição para ser justo.
                        new_period_start = most_recent_assignment_date or now
                        new_period_end = new_period_start + timedelta(days=monitoring_period)
                        await bot.db.log_period_check(member.id, guild.id, new_period_start, new_period_end, False)
                        
                        should_remove_roles = False # Garante que a remoção não ocorra.
                        # Continue para o próximo período falho, caso haja outro.
                        continue

                    # <<< FIM DA NOVA LÓGICA >>>
                else:
                    # Se a revalidação passou, o registro original estava errado. Corrigi-lo.
                    logger.info(f"Corrigindo período falho para {member.display_name} após revalidação. Período: {period_start.strftime('%d/%m/%Y')}")
                    await bot.db.log_period_check(member.id, guild.id, period_start, period_end, True)

            if should_remove_roles:
                logger.info(f"Removendo cargos de {member.display_name} por falha de atividade confirmada no período de {first_failed_period['period_start'].strftime('%d/%m/%Y')}.")
                try:
                    await member.remove_roles(*roles_to_remove, reason="Remoção automática por inatividade em período anterior.")
                    await bot.send_warning(member, 'final')
                    await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                    
                    log_message = (
                        f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}\n"
                        f"Falha confirmada no período: {first_failed_period['period_start'].strftime('%d/%m/%Y')} a {first_failed_period['period_end'].strftime('%d/%m/%Y')}"
                    )
                    await bot.log_action("Cargo Removido (Inatividade Passada)", member, log_message)
                    result['removed'] = 1
                except Exception as e:
                    logger.error(f"Erro ao remover cargos de {member} (inatividade passada): {e}")

    except Exception as e:
        logger.error(f"Erro ao verificar períodos anteriores para {member.display_name}: {e}", exc_info=True)
    
    return result

async def process_pending_voice_events():
    """Processa eventos de voz pendentes que foram salvos no banco de dados"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando processamento de eventos pendentes")
        return
    
    try:
        # Obter eventos pendentes
        pending_events = await bot.db.get_pending_voice_events(limit=500)
        if not pending_events:
            logger.info("Nenhum evento de voz pendente para processar")
            return
            
        logger.info(f"Processando {len(pending_events)} eventos de voz pendentes...")
        
        # Processar em lotes
        batch_size = 50
        processed_ids = []
        
        for event in pending_events:
            try:
                guild = bot.get_guild(event['guild_id'])
                if not guild:
                    processed_ids.append(event['id'])
                    continue
                    
                member = guild.get_member(event['user_id'])
                if not member:
                    processed_ids.append(event['id'])
                    continue
                
                # Verificar se os canais existem
                before_channel = guild.get_channel(event['before_channel_id']) if event['before_channel_id'] else None
                after_channel = guild.get_channel(event['after_channel_id']) if event['after_channel_id'] else None
                
                # Criar objetos VoiceState
                before_data = {
                    'channel_id': event['before_channel_id'],
                    'self_deaf': event['before_self_deaf'],
                    'deaf': event['before_deaf'],
                    'self_mute': False,
                    'mute': False,
                    'self_stream': False,
                    'self_video': False,
                    'suppress': False,
                    'requested_to_speak_at': None,
                }
                
                after_data = {
                    'channel_id': event['after_channel_id'],
                    'self_deaf': event['after_self_deaf'],
                    'deaf': event['after_deaf'],
                    'self_mute': False,
                    'mute': False,
                    'self_stream': False,
                    'self_video': False,
                    'suppress': False,
                    'requested_to_speak_at': None,
                }
                
                # Enfileirar para processamento
                await bot.voice_event_queue.put((
                    event['event_type'],
                    member,
                    discord.VoiceState(data=before_data, channel=before_channel),
                    discord.VoiceState(data=after_data, channel=after_channel)
                ))
                
                processed_ids.append(event['id'])
                
            except Exception as e:
                logger.error(f"Erro ao processar evento pendente {event['id']}: {e}")
                continue
                
            # Marcar como processados a cada lote
            if len(processed_ids) >= batch_size:
                await bot.db.mark_events_as_processed(processed_ids)
                processed_ids = []
                await asyncio.sleep(1)  # Pequeno delay entre lotes
        
        # Marcar quaisquer eventos restantes como processados
        if processed_ids:
            await bot.db.mark_events_as_processed(processed_ids)
        
        logger.info("Processamento de eventos pendentes concluído")
        
    except Exception as e:
        logger.error(f"Erro no processamento de eventos pendentes: {e}")

@log_task_metrics("check_current_voice_members")
async def check_current_voice_members():
    """Verifica todos os canais de voz e atualiza o estado interno"""
    await bot.wait_until_ready()
    
    try:
        logger.info("Iniciando verificação de membros em canais de voz...")
        
        for guild in bot.guilds:
            for voice_channel in guild.voice_channels:
                for member in voice_channel.members:
                    if member.bot:
                        continue
                        
                    audio_key = (member.id, guild.id)
                    
                    # Se já existe sessão ativa, pular
                    if audio_key in bot.active_sessions:
                        continue
                        
                    # Criar sessão com tempo máximo de 5 minutos (estimativa conservadora)
                    max_estimated_duration = timedelta(minutes=5)
                    estimated_start = datetime.now(pytz.UTC) - max_estimated_duration
                    
                    # Obter a última entrada do banco de dados para evitar discrepâncias
                    try:
                        last_join = await bot.db.get_user_activity(member.id, guild.id)
                        if last_join and last_join.get('last_voice_join'):
                            # Usar o mais recente entre: agora-5min ou último registro no banco
                            estimated_start = min(
                                estimated_start,
                                last_join['last_voice_join']
                            )
                    except Exception as e:
                        logger.error(f"Erro ao obter última entrada para {member}: {e}")
                    
                    bot.active_sessions[audio_key] = {
                        'start_time': estimated_start,
                        'last_audio_time': datetime.now(pytz.UTC),
                        'audio_disabled': member.voice.self_deaf or member.voice.deaf,
                        'total_audio_off_time': 0,
                        'estimated': True,
                        'max_estimated_time': datetime.now(pytz.UTC) + max_estimated_duration
                    }
                    
                    logger.info(f"Sessão estimada criada para {member.display_name} no canal {voice_channel.name} - Início: {estimated_start}")
                    
        logger.info("Verificação de membros em canais de voz concluída")
        
    except Exception as e:
        logger.error(f"Erro na verificação de canais de voz: {e}")

async def detect_missing_voice_leaves():
    """Detecta sessões que provavelmente foram encerradas durante uma queda"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando detecção de sessões perdidas")
        return
    
    try:
        logger.info("Iniciando detecção de sessões de voz perdidas...")
        
        # Obter todas as sessões ativas do banco de dados
        async with bot.db.pool.acquire() as conn:
            active_sessions = await conn.fetch('''
                SELECT user_id, guild_id, last_voice_join 
                FROM user_activity 
                WHERE (last_voice_leave IS NULL OR last_voice_leave < last_voice_join)
                AND last_voice_join < NOW() - INTERVAL '10 minutes'
            ''')
            
        for session in active_sessions:
            guild = bot.get_guild(session['guild_id'])
            if not guild:
                continue
                
            member = guild.get_member(session['user_id'])
            if not member:
                continue
                
            # Verificar se o membro não está mais em um canal de voz
            if not member.voice or not member.voice.channel:
                # Garantir que last_voice_join está com timezone
                join_time = session['last_voice_join']
                if join_time.tzinfo is None:
                    join_time = join_time.replace(tzinfo=pytz.UTC)
                
                # CORREÇÃO: Remover o limite de 10 minutos e calcular a duração real.
                # A duração será o tempo desde a entrada até agora (momento da reinicialização).
                duration = (datetime.now(pytz.UTC) - join_time).total_seconds()

                # Adicionar um limite razoável para evitar sessões absurdamente longas se o bot
                # ficar offline por dias. Um limite de 24 horas é uma salvaguarda segura.
                max_duration = timedelta(hours=24).total_seconds()
                duration = min(duration, max_duration)
                
                # Registrar saída no banco de dados
                try:
                    await bot.db.log_voice_leave(member.id, guild.id, int(duration))
                    
                    # Se tínhamos uma sessão ativa, remover
                    audio_key = (member.id, guild.id)
                    if audio_key in bot.active_sessions:
                        del bot.active_sessions[audio_key]
                        
                    logger.info(f"Sessão que terminou durante a queda do bot foi encerrada para {member.display_name} com duração de {duration//60:.0f} minutos")
                    
                except Exception as e:
                    logger.error(f"Erro ao registrar saída estimada: {e}")
        
        logger.info("Detecção de sessões de voz perdidas concluída")
    except Exception as e:
        logger.error(f"Erro na detecção de sessões perdidas: {e}")

@log_task_metrics("cleanup_processed_events")
async def cleanup_processed_events():
    """Limpa eventos de voz já processados"""
    await bot.wait_until_ready()
    
    while True:
        try:
            if hasattr(bot, 'db') and bot.db and bot.db._is_initialized:
                async with bot.db.pool.acquire() as conn:
                    # Limpar eventos com mais de 7 dias
                    await conn.execute('''
                        DELETE FROM pending_voice_events
                        WHERE processed = TRUE
                        AND event_time < NOW() - INTERVAL '7 days'
                    ''')
            
            await asyncio.sleep(86400)  # Executar uma vez por dia
        except Exception as e:
            logger.error(f"Erro na limpeza de eventos processados: {e}")
            await asyncio.sleep(3600)  # Tentar novamente em 1 hora se falhar

@log_task_metrics("cleanup_ghost_sessions")
async def cleanup_ghost_sessions():
    """Limpa sessões fantasmas no banco de dados"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando limpeza de sessões fantasmas")
        return
    
    try:
        logger.info("Iniciando limpeza de sessões fantasmas...")
        
        now = datetime.now(pytz.UTC)
        to_remove = []
        
        # Limpar sessões estimadas que excederam o tempo máximo
        for key, session in bot.active_sessions.items():
            if session.get('estimated') and 'max_estimated_time' in session:
                if now > session['max_estimated_time']:
                    to_remove.append(key)
                    # Registrar saída no banco de dados
                    try:
                        duration = (session['max_estimated_time'] - session['start_time']).total_seconds()
                        await bot.db.log_voice_leave(key[0], key[1], int(duration))
                    except Exception as e:
                        logger.error(f"Erro ao registrar saída estimada: {e}")
        
        for key in to_remove:
            bot.active_sessions.pop(key, None)
            logger.info(f"Removida sessão estimada expirada para usuário {key[0]} na guild {key[1]}")
        
        # Corrigir sessões onde last_voice_join > last_voice_leave há mais de 24 horas
        async with bot.db.pool.acquire() as conn:
            ghost_sessions = await conn.fetch('''
                UPDATE user_activity 
                SET last_voice_leave = COALESCE(last_voice_join, NOW()) + INTERVAL '1 hour',
                    total_voice_time = total_voice_time + 3600
                WHERE (last_voice_leave IS NULL OR last_voice_join > last_voice_leave)
                AND (last_voice_join < NOW() - INTERVAL '24 hours')
                RETURNING user_id, guild_id, last_voice_join
            ''')
            
            if ghost_sessions:
                logger.info(f"Limpeza concluída: {len(ghost_sessions)} sessões fantasmas corrigidas")
                
                # Registrar sessões de voz para os membros afetados
                for session in ghost_sessions:
                    try:
                        join_time = session['last_voice_join']
                        if join_time.tzinfo is None:
                            join_time = join_time.replace(tzinfo=pytz.UTC)
                            
                        await conn.execute('''
                            INSERT INTO voice_sessions
                            (user_id, guild_id, join_time, leave_time, duration)
                            VALUES ($1, $2, $3, $4, $5)
                        ''', 
                        session['user_id'], 
                        session['guild_id'],
                        join_time,
                        join_time + timedelta(hours=1),
                        3600)
                    except Exception as e:
                        logger.error(f"Erro ao registrar sessão fantasma: {e}")
                        continue
    except Exception as e:
        logger.error(f"Erro na limpeza de sessões fantasmas: {e}")

async def cleanup_ghost_sessions_wrapper():
    """Wrapper para a task de limpeza de sessões fantasmas"""
    monitoring_period = 1  # Executar diariamente
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_ghost_sessions",
        monitoring_period,
        cleanup_ghost_sessions
    ), name='cleanup_ghost_sessions_wrapper')
    return task

@log_task_metrics("register_role_assignments")
async def register_role_assignments():
    """Registra datas de atribuição de cargos para membros existentes"""
    await bot.wait_until_ready()
    
    # Verificar se o banco de dados está disponível
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Banco de dados não inicializado - pulando registro de atribuições de cargos")
        return
    
    # Verificar se há cargos monitorados configurados
    if not bot.config.get('tracked_roles'):
        logger.info("Nenhum cargo monitorado definido - pulando registro de atribuições")
        return
    
    try:
        for guild in bot.guilds:
            logger.info(f"Registrando atribuições de cargos para guild {guild.name} (ID: {guild.id})")
            
            tracked_roles = bot.config['tracked_roles']
            
            # Processar em lotes menores para evitar sobrecarga
            batch_size = 20
            members = list(guild.members)
            
            for i in range(0, len(members), batch_size):
                batch = members[i:i + batch_size]
                
                # Verificar quais membros já têm registros de atribuição
                try:
                    async with bot.db.pool.acquire() as conn:
                        existing_assignments = await conn.fetch('''
                            SELECT DISTINCT user_id FROM role_assignments
                            WHERE guild_id = $1 AND role_id = ANY($2)
                        ''', guild.id, tracked_roles)
                    
                    existing_user_ids = {r['user_id'] for r in existing_assignments}
                    
                    for member in batch:
                        try:
                            # Verificar se tem cargos monitorados e não está registrado
                            if member.id not in existing_user_ids:
                                member_roles = [role for role in member.roles if role.id in tracked_roles]
                                
                                if member_roles:
                                    # Registrar data de atribuição para cada cargo
                                    for role in member_roles:
                                        try:
                                            await bot.db.log_role_assignment(member.id, guild.id, role.id)
                                            logger.debug(f"Registrado cargo {role.name} para {member.display_name}")
                                        except Exception as e:
                                            logger.error(f"Erro ao registrar cargo {role.id} para {member}: {e}")
                                            continue
                                            
                        except Exception as e:
                            logger.error(f"Erro ao processar membro {member}: {e}")
                            continue
                
                except Exception as e:
                    logger.error("Erro ao verificar atribuições existentes", exc_info=True)
                    continue
                
                # Pequeno delay entre lotes para evitar rate limits
                await asyncio.sleep(1)
                
    except Exception as e:
        logger.error(f"Erro na task register_role_assignments: {e}")

async def process_member_role_assignments(member: discord.Member, tracked_roles: List[int]):
    """Processa e registra as atribuições de cargos para um único membro"""
    try:
        for role in member.roles:
            if role.id in tracked_roles:
                try:
                    # Verificar se já existe registro no banco de dados
                    assigned_time = None
                    try:
                        assigned_time = await bot.db.get_role_assigned_time(
                            member.id, member.guild.id, role.id)
                    except Exception as e:
                        logger.error(f"Erro ao verificar atribuição de cargo {role.id} para {member.id}: {e}")
                        continue
                    
                    if not assigned_time:
                        # Registrar com a data atual
                        try:
                            await bot.db.log_role_assignment(
                                member.id, member.guild.id, role.id)
                            logger.debug(f"Registrado cargo {role.name} para {member.display_name}")
                        except Exception as e:
                            logger.error(f"Erro ao registrar atribuição de cargo {role.id} para {member.id}: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Erro ao processar cargo {role.id} para {member.display_name}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Erro ao processar atribuições de cargos para {member.display_name}: {e}")

async def register_role_assignments_wrapper():
    """Wrapper para a task com intervalo persistente"""
    monitoring_period = 1  # Executar diariamente
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "register_role_assignments",
        monitoring_period,
        register_role_assignments
    ), name='register_role_assignments_wrapper')
    return task