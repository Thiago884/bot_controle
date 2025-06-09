import discord
from discord.ext import commands
import pytz
import json
import os
import logging
import asyncio
import time
import random
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Optional
from discord.ext import tasks
import aiomysql

# Configuração do logger
def setup_logger():
    logger = logging.getLogger('inactivity_bot')
    if logger.handlers:  # Se já tem handlers, não adicione novos
        return logger
        
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - '
        'Guild:%(guild_id)s - User:%(user_id)s - %(message)s'
    )
    
    file_handler = RotatingFileHandler(
        'bot.log', 
        maxBytes=5*1024*1024,
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

class ContextFilter(logging.Filter):
    def filter(self, record):
        record.guild_id = getattr(record, 'guild_id', 'N/A')
        record.user_id = getattr(record, 'user_id', 'N/A')
        return True

logger = setup_logger()
logger.addFilter(ContextFilter())

def log_with_context(message, level=logging.INFO, guild_id=None, user_id=None):
    extra = {'guild_id': guild_id or 'N/A', 'user_id': user_id or 'N/A'}
    logger.log(level, message, extra=extra)

# Configurações iniciais
CONFIG_FILE = 'config.json'
DEFAULT_CONFIG = {
    "required_minutes": 15,
    "required_days": 2,
    "monitoring_period": 14,
    "kick_after_days": 30,
    "tracked_roles": [],
    "log_channel": 1376013013206827161,
    "notification_channel": 1224897652060196996,
    "timezone": "America/Sao_Paulo",
    "absence_channel": 1187657181194113045,
    "allowed_roles": [],
    "whitelist": {
        "users": [],
        "roles": []
    },
    "warnings": {
        "first_warning": 3,
        "second_warning": 1,
        "messages": {
            "first": "⚠️ **Aviso de Inatividade** ⚠️\nVocê está prestes a perder seus cargos por inatividade. Entre em um canal de voz por pelo menos 15 minutos em 2 dias diferentes nos próximos {days} dias para evitar isso.",
            "second": "🔴 **Último Aviso** 🔴\nVocê perderá seus cargos AMANHÃ por inatividade se não cumprir os requisitos de atividade em voz.",
            "final": "❌ **Cargos Removidos** ❌\nVocê perdeu seus cargos no servidor {guild} por inatividade. Você não cumpriu os requisitos de atividade de voz (15 minutos em 2 dias diferentes dentro de {monitoring_period} dias)."
        }
    }
}

class InactivityBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = {}
        self.timezone = pytz.timezone('America/Sao_Paulo')
        self.db = None
        self.active_sessions = {}  # { (user_id, guild_id): {'start_time': datetime, 'last_audio_time': datetime, 'audio_disabled': bool, 'audio_off_time': datetime, 'total_audio_off_time': int} }
        self.voice_event_queue = asyncio.Queue(maxsize=1000)
        self.command_queue = asyncio.Queue(maxsize=500)
        self.high_priority_queue = asyncio.Queue(maxsize=100)
        self.normal_priority_queue = asyncio.Queue(maxsize=500)
        self.low_priority_queue = asyncio.Queue(maxsize=200)
        self.voice_event_processor_task = None
        self.command_processor_task = None
        self.queue_processor_task = None
        self.rate_limited = False
        self.last_rate_limit = None
        self.rate_limit_delay = 1.0
        self.max_rate_limit_delay = 5.0
        self.rate_limit_retry_after = 1.0
        self.last_rate_limit_time = None
        self.rate_limit_count = 0
        self.max_rate_limit_retries = 3
        self.db_backup = None
        self.pool_monitor_task = None
        self._setup_complete = False
        self._last_db_check = None
        self._health_check_interval = 300
        self._last_config_save = None
        self._config_save_interval = 1800
        self._batch_processing_size = 10
        self._api_request_delay = 1.0
        self.audio_check_task = None
        
        # Sistema de cache aprimorado
        self._cache = {
            'config': {
                'data': {},
                'last_updated': {},
                'ttl': 300  # 5 minutos
            },
            'user_data': {
                'data': {},
                'last_updated': {},
                'ttl': 600  # 10 minutos
            },
            'guild_data': {
                'data': {},
                'last_updated': {},
                'ttl': 900  # 15 minutos
            }
        }
        self._cache_lock = asyncio.Lock()
        
        # Sistema avançado de rate limiting
        self.rate_limit_stats = {
            'global': {
                'last_hit': None,
                'retry_after': 0,
                'count': 0,
                'max_retries': 5,
                'backoff_base': 1.5
            },
            'endpoints': {
                'send_message': {
                    'last_hit': None,
                    'retry_after': 0,
                    'count': 0,
                    'max_retries': 3,
                    'backoff_base': 1.2
                },
                'modify_roles': {
                    'last_hit': None,
                    'retry_after': 0,
                    'count': 0,
                    'max_retries': 3,
                    'backoff_base': 1.3
                }
            }
        }
        self.rate_limit_lock = asyncio.Lock()

    async def get_cached_config(self, guild_id: int):
        """Obtém configuração do cache se válida"""
        async with self._cache_lock:
            cache = self._cache['config']
            if guild_id in cache['data']:
                if time.time() - cache['last_updated'][guild_id] < cache['ttl']:
                    return cache['data'][guild_id]
        return None
        
    async def set_cached_config(self, guild_id: int, config: dict):
        """Armazena configuração no cache"""
        async with self._cache_lock:
            self._cache['config']['data'][guild_id] = config
            self._cache['config']['last_updated'][guild_id] = time.time()
            
    async def get_cached_user_data(self, user_id: int, guild_id: int):
        """Obtém dados de usuário do cache se válidos"""
        cache_key = f"{user_id}_{guild_id}"
        async with self._cache_lock:
            cache = self._cache['user_data']
            if cache_key in cache['data']:
                if time.time() - cache['last_updated'][cache_key] < cache['ttl']:
                    return cache['data'][cache_key]
        return None
        
    async def set_cached_user_data(self, user_id: int, guild_id: int, data: dict):
        """Armazena dados de usuário no cache"""
        cache_key = f"{user_id}_{guild_id}"
        async with self._cache_lock:
            self._cache['user_data']['data'][cache_key] = data
            self._cache['user_data']['last_updated'][cache_key] = time.time()
            
    async def invalidate_cache(self, cache_type: str, key: str = None):
        """Invalida cache completo ou um item específico"""
        async with self._cache_lock:
            if cache_type in self._cache:
                if key:
                    if key in self._cache[cache_type]['data']:
                        del self._cache[cache_type]['data'][key]
                    if key in self._cache[cache_type]['last_updated']:
                        del self._cache[cache_type]['last_updated'][key]
                else:
                    self._cache[cache_type]['data'] = {}
                    self._cache[cache_type]['last_updated'] = {}

    async def check_rate_limit(self, endpoint: str = 'global') -> float:
        """Verifica se há rate limit e retorna tempo de espera necessário"""
        async with self.rate_limit_lock:
            stats = self.rate_limit_stats.get(endpoint, self.rate_limit_stats['global'])
            
            if stats['last_hit'] and (time.time() - stats['last_hit']) < stats['retry_after']:
                stats['count'] += 1
                if stats['count'] > stats['max_retries']:
                    return -1  # Indica que devemos abortar
                
                # Backoff exponencial com jitter
                backoff = min(
                    stats['backoff_base'] ** stats['count'] + random.uniform(0, 0.1),
                    60  # Máximo de 60 segundos
                )
                return backoff
                
            return 0  # Sem rate limit ativo
            
    async def update_rate_limit(self, endpoint: str, retry_after: float):
        """Atualiza estatísticas de rate limit"""
        async with self.rate_limit_lock:
            if endpoint not in self.rate_limit_stats['endpoints']:
                self.rate_limit_stats['endpoints'][endpoint] = {
                    'last_hit': time.time(),
                    'retry_after': retry_after,
                    'count': 1,
                    'max_retries': 3,
                    'backoff_base': 1.2
                }
            else:
                stats = self.rate_limit_stats['endpoints'][endpoint]
                stats['last_hit'] = time.time()
                stats['retry_after'] = retry_after
                stats['count'] += 1
                
            # Atualizar também o rate limit global
            self.rate_limit_stats['global']['last_hit'] = time.time()
            self.rate_limit_stats['global']['retry_after'] = retry_after
            self.rate_limit_stats['global']['count'] += 1

    async def setup_hook(self):
        """Configuração assíncrona que é executada após o login mas antes de conectar ao websocket"""
        if not self._setup_complete:
            self._setup_complete = True
            
            self.load_config()
            await self.initialize_db()
            
            from database import DatabaseBackup
            self.db_backup = DatabaseBackup(self.db)
            
            try:
                synced = await self.tree.sync()
                logger.info(f"Comandos slash sincronizados: {len(synced)} comandos")
            except Exception as e:
                logger.error(f"Erro ao sincronizar comandos slash: {e}")
            
            from tasks import setup_tasks
            setup_tasks()
            
            self.voice_event_processor_task = asyncio.create_task(self.process_voice_events())
            self.command_processor_task = asyncio.create_task(self.process_commands_queue())
            self.queue_processor_task = asyncio.create_task(self.process_queues())
            self.pool_monitor_task = asyncio.create_task(self.monitor_db_pool())
            self.health_check_task = asyncio.create_task(self.periodic_health_check())
            self.audio_check_task = asyncio.create_task(self.check_audio_states())

    async def check_audio_states(self):
        """Verifica periodicamente o estado do áudio dos membros em canais de voz"""
        while True:
            try:
                for guild in self.guilds:
                    for voice_channel in guild.voice_channels:
                        for member in voice_channel.members:
                            audio_key = (member.id, guild.id)
                            if audio_key in self.active_sessions:
                                current_audio_state = member.voice.self_deaf
                                
                                # Se o áudio está desativado e não estávamos rastreando
                                if current_audio_state and not self.active_sessions[audio_key]['audio_disabled']:
                                    self.active_sessions[audio_key]['audio_disabled'] = True
                                    self.active_sessions[audio_key]['audio_off_time'] = datetime.utcnow()
                                    
                                    # Log detalhado
                                    time_in_voice = (datetime.utcnow() - self.active_sessions[audio_key]['start_time']).total_seconds()
                                    embed = discord.Embed(
                                        title="🔇 Áudio Desativado (Verificação Periódica)",
                                        color=discord.Color.orange(),
                                        timestamp=datetime.now(self.timezone))
                                    embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                                    embed.add_field(name="Usuário", value=member.mention, inline=True)
                                    embed.add_field(name="Canal", value=voice_channel.name, inline=True)
                                    embed.add_field(name="Tempo em voz", value=f"{int(time_in_voice//60)} minutos {int(time_in_voice%60)} segundos", inline=False)
                                    embed.set_footer(text=f"ID: {member.id}")
                                    
                                    await self.high_priority_queue.put((
                                        self.get_channel(self.config['log_channel']),
                                        None,
                                        embed,
                                        None,
                                        "high"
                                    ))
                                
                                # Se o áudio está ativado mas estava marcado como desativado
                                elif not current_audio_state and self.active_sessions[audio_key]['audio_disabled']:
                                    self.active_sessions[audio_key]['audio_disabled'] = False
                                    if 'audio_off_time' in self.active_sessions[audio_key]:
                                        audio_off_duration = (datetime.utcnow() - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                                        self.active_sessions[audio_key]['total_audio_off_time'] = \
                                            self.active_sessions[audio_key].get('total_audio_off_time', 0) + audio_off_duration
                                        del self.active_sessions[audio_key]['audio_off_time']
                                        
                                        # Log detalhado
                                        total_time = (datetime.utcnow() - self.active_sessions[audio_key]['start_time']).total_seconds()
                                        embed = discord.Embed(
                                            title="🔊 Áudio Reativado (Verificação Periódica)",
                                            color=discord.Color.green(),
                                            timestamp=datetime.now(self.timezone))
                                        embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                                        embed.add_field(name="Usuário", value=member.mention, inline=True)
                                        embed.add_field(name="Canal", value=voice_channel.name, inline=True)
                                        embed.add_field(name="Tempo sem áudio", 
                                                      value=f"{int(audio_off_duration//60)} minutos {int(audio_off_duration%60)} segundos", 
                                                      inline=True)
                                        embed.add_field(name="Tempo total em voz", 
                                                      value=f"{int(total_time//60)} minutos {int(total_time%60)} segundos", 
                                                      inline=True)
                                        embed.set_footer(text=f"ID: {member.id}")
                                        
                                        await self.high_priority_queue.put((
                                            self.get_channel(self.config['log_channel']),
                                            None,
                                            embed,
                                            None,
                                            "high"
                                        ))
                
                await asyncio.sleep(30)  # Verificar a cada 30 segundos
            except Exception as e:
                logger.error(f"Erro ao verificar estados de áudio: {e}")
                await asyncio.sleep(60)

    async def initialize_db(self):
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                from database import Database
                self.db = Database()
                await self.db.initialize()
                logger.info("Banco de dados inicializado com sucesso")
                
                await self.db.create_tables()
                
                db_config = await self.db.load_config(0)
                if db_config:
                    self.config.update(db_config)
                    await self.save_config()
                    logger.info("Configuração carregada do banco de dados")
                
                break
            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if attempt == max_retries - 1:
                    raise
                sleep_time = initial_delay * (2 ** attempt)
                logger.info(f"Tentando novamente em {sleep_time} segundos...")
                await asyncio.sleep(sleep_time)

    async def monitor_db_pool(self):
        """Monitora o status do pool de conexões do banco de dados"""
        while True:
            try:
                if hasattr(self, 'db') and self.db:
                    pool_status = await self.db.check_pool_status()
                    if pool_status:
                        log_message = (
                            f"Status do pool: {pool_status['used']} conexões ativas de "
                            f"{pool_status['size']} (máx: {pool_status['maxsize']})"
                        )
                        logger.info(log_message)
                        
                        if pool_status['used'] > 20:
                            logger.warning("Aproximando do limite de conexões - considerando otimizações")
                            await self.log_action(
                                "Monitoramento do Pool",
                                None,
                                f"Uso elevado de conexões: {pool_status['used']}/{pool_status['maxsize']}"
                            )
                
                await asyncio.sleep(300)
            except Exception as e:
                log_with_context(f"Erro no monitoramento do pool: {e}", logging.ERROR)
                await asyncio.sleep(60)

    async def periodic_health_check(self):
        """Verificação periódica de saúde do bot"""
        while True:
            try:
                queue_status = {
                    'voice_events': self.voice_event_queue.qsize(),
                    'commands': self.command_queue.qsize(),
                    'high_priority': self.high_priority_queue.qsize(),
                    'normal_priority': self.normal_priority_queue.qsize(),
                    'low_priority': self.low_priority_queue.qsize()
                }
                
                if queue_status['voice_events'] > 500:
                    logger.warning(f"Fila de eventos de voz grande: {queue_status['voice_events']}")
                if queue_status['commands'] > 200:
                    logger.warning(f"Fila de comandos grande: {queue_status['commands']}")
                
                if hasattr(self, 'db') and self.db:
                    try:
                        async with self.db.pool.acquire() as conn:
                            async with conn.cursor() as cursor:
                                await cursor.execute("SELECT 1")
                                await cursor.fetchone()
                        logger.debug("Health check: Banco de dados respondendo")
                    except Exception as e:
                        logger.error(f"Health check falhou para o banco de dados: {e}")
                        await self.log_action(
                            "Erro de Saúde",
                            None,
                            f"Falha na conexão com o banco de dados: {str(e)}"
                        )
                
                if (self._last_config_save is None or 
                    (datetime.now() - self._last_config_save).total_seconds() > self._config_save_interval):
                    await self.save_config()
                    self._last_config_save = datetime.now()
                
                await asyncio.sleep(self._health_check_interval)
            except Exception as e:
                logger.error(f"Erro no health check: {e}")
                await asyncio.sleep(60)

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    self.config = json.load(f)
                logger.info("Configuração carregada do arquivo local")
            else:
                self.config = DEFAULT_CONFIG
                logger.info("Usando configuração padrão")
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(self.config, f, indent=4)
            
            self.timezone = pytz.timezone(self.config.get('timezone', 'America/Sao_Paulo'))
            
            if 'notification_channel' not in self.config or not self.config['notification_channel']:
                self.config['notification_channel'] = 1224897652060196996
            if 'log_channel' not in self.config or not self.config['log_channel']:
                self.config['log_channel'] = 1376013013206827161
                
            logger.info("Configuração carregada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            self.config = DEFAULT_CONFIG
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)

    async def save_config(self):
        """Salva a configuração com tratamento de erros e rate limiting"""
        if not hasattr(self, 'config') or not self.config:
            return
            
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            if hasattr(self, 'db') and self.db:
                try:
                    await self.db.save_config(0, self.config)
                    logger.info("Configuração salva no banco de dados com sucesso")
                except Exception as db_error:
                    logger.error(f"Erro ao salvar configuração no banco de dados: {db_error}")
                    
            self._last_config_save = datetime.now()
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            raise

    async def safe_send(self, channel, content=None, embed=None, file=None, retry_count=0):
        """Método seguro para enviar mensagens com tratamento de rate limits"""
        # Verificar rate limit antes de tentar
        wait_time = await self.check_rate_limit('send_message')
        if wait_time == -1:
            logger.warning("Máximo de tentativas de rate limit atingido para envio de mensagem")
            return False
        elif wait_time > 0:
            await asyncio.sleep(wait_time)
            
        try:
            await channel.send(content=content, embed=embed, file=file)
            
            # Resetar contadores se bem-sucedido
            async with self.rate_limit_lock:
                if 'send_message' in self.rate_limit_stats['endpoints']:
                    self.rate_limit_stats['endpoints']['send_message']['count'] = 0
                self.rate_limit_stats['global']['count'] = 0
                
            return True
            
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = float(e.response.headers.get('Retry-After', 5))
                logger.warning(f"Rate limit atingido. Tentando novamente em {retry_after} segundos")
                
                await self.update_rate_limit('send_message', retry_after)
                
                if retry_count < self.rate_limit_stats['endpoints']['send_message']['max_retries']:
                    await asyncio.sleep(retry_after)
                    return await self.safe_send(channel, content, embed, file, retry_count + 1)
                    
                logger.error("Máximo de tentativas de rate limit atingido")
                return False
            raise

    async def process_queues(self):
        """Processa as filas de mensagens com diferentes prioridades"""
        while True:
            try:
                # Processar alta prioridade primeiro
                if not self.high_priority_queue.empty():
                    item = await self.high_priority_queue.get()
                    await self._process_queue_item(item)
                    self.high_priority_queue.task_done()
                    continue
                
                # Depois normal priority
                if not self.normal_priority_queue.empty():
                    item = await self.normal_priority_queue.get()
                    await self._process_queue_item(item)
                    self.normal_priority_queue.task_done()
                    continue
                
                # Por último low priority
                if not self.low_priority_queue.empty():
                    item = await self.low_priority_queue.get()
                    await self._process_queue_item(item)
                    self.low_priority_queue.task_done()
                    continue
                
                # Se todas as filas estiverem vazias, esperar um pouco
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Erro no processador de filas: {e}")
                await asyncio.sleep(1)

    async def _process_queue_item(self, item):
        """Processa um item da fila"""
        channel, content, embed, file, priority = item
        try:
            await self.safe_send(channel, content, embed, file)
        except Exception as e:
            logger.error(f"Erro ao processar item da fila: {e}")

    async def log_action(self, action: str, member: Optional[discord.Member] = None, details: str = None, file: discord.File = None, embed: discord.Embed = None):
        """Registra uma ação no canal de logs com tratamento de rate limit"""
        try:
            channel = self.get_channel(self.config.get('log_channel', 1376013013206827161))
            if not channel:
                logger.warning("Canal de logs não encontrado")
                return
                
            # Se um embed foi fornecido, usá-lo diretamente
            if embed is not None:
                await self.high_priority_queue.put((
                    channel,
                    None,
                    embed,
                    None,
                    "high"
                ))
                return
                
            # Definir cor baseada no tipo de ação
            if "Áudio Desativado" in action:
                color = discord.Color.orange()
                icon = "🔇"
            elif "Áudio Reativado" in action:
                color = discord.Color.green()
                icon = "🔊"
            elif "Erro" in action:
                color = discord.Color.red()
                icon = "❌"
            elif "Aviso" in action:
                color = discord.Color.gold()
                icon = "⚠️"
            else:
                color = discord.Color.blue()
                icon = "ℹ️"
                
            embed = discord.Embed(
                title=f"{icon} {action}",
                color=color,
                timestamp=datetime.now(self.timezone))
            
            if member is not None:
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="Usuário", value=member.mention, inline=True)
                embed.add_field(name="ID", value=f"`{member.id}`", inline=True)
            
            if details:
                # Formatar detalhes para melhor legibilidade
                if '\n' in details:
                    details = details.replace('\n', '\n• ')
                    embed.add_field(name="Detalhes", value=f"• {details}", inline=False)
                else:
                    embed.add_field(name="Detalhes", value=details, inline=False)
            
            # Adicionar à fila de alta prioridade
            await self.high_priority_queue.put((
                channel,
                None,
                embed,
                None,
                "high"
            ))
            
        except Exception as e:
            logger.error(f"Erro ao registrar ação no log: {e}")

    async def notify_roles(self, message: str, is_warning: bool = False):
        """Envia notificação para os cargos com tratamento de rate limit"""
        try:
            channel_id = self.config.get('notification_channel') if is_warning else self.config.get('log_channel')
            if not channel_id:
                logger.warning("Canal de notificação não configurado")
                return
                
            channel = self.get_channel(channel_id)
            if not channel:
                logger.warning(f"Canal de notificação {channel_id} não encontrado")
                return
                
            # Criar embed para notificações
            if is_warning:
                embed = discord.Embed(
                    title="⚠️ Aviso de Inatividade",
                    description=message,
                    color=discord.Color.gold(),
                    timestamp=datetime.now(self.timezone))
                priority = "high"
            else:
                embed = discord.Embed(
                    title="ℹ️ Notificação",
                    description=message,
                    color=discord.Color.blue(),
                    timestamp=datetime.now(self.timezone))
                priority = "normal"
            
            await self.high_priority_queue.put((channel, None, embed, None, priority))
            
        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")
            await self.log_action("Erro de Notificação", None, f"Falha ao enviar mensagem: {str(e)}")

    async def send_dm(self, member: discord.Member, message: str):
        """Envia mensagem direta com tratamento de erros e rate limiting"""
        try:
            # Criar embed para DMs
            embed = discord.Embed(
                description=message,
                color=discord.Color.blue(),
                timestamp=datetime.now(self.timezone))
            
            # Adicionar à fila de baixa prioridade para evitar rate limits
            await self.low_priority_queue.put((member, None, embed, None, "low"))
        except Exception as e:
            logger.error(f"Erro ao enviar DM para {member}: {e}")

    async def send_warning(self, member: discord.Member, warning_type: str):
        """Envia aviso de inatividade com mensagem configurável"""
        try:
            warnings_config = self.config.get('warnings', {})
            messages = warnings_config.get('messages', {})
            
            if warning_type == 'first' and 'first' in messages:
                message = messages['first'].format(
                    days=self.config['warnings']['first_warning'],
                    monitoring_period=self.config['monitoring_period'])
            elif warning_type == 'second' and 'second' in messages:
                message = messages['second']
            elif warning_type == 'final' and 'final' in messages:
                message = messages['final'].format(
                    guild=member.guild.name,
                    monitoring_period=self.config['monitoring_period'])
            else:
                return
            
            # Criar embed para avisos
            if warning_type == 'first':
                title = "⚠️ Primeiro Aviso"
                color = discord.Color.gold()
            elif warning_type == 'second':
                title = "🔴 Último Aviso"
                color = discord.Color.red()
            else:
                title = "❌ Cargos Removidos"
                color = discord.Color.dark_red()
            
            embed = discord.Embed(
                title=title,
                description=message,
                color=color,
                timestamp=datetime.now(self.timezone))
            
            embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url if member.guild.icon else None)
            
            await self.send_dm(member, embed.description)
            await self.db.log_warning(member.id, member.guild.id, warning_type)
            await self.log_action(f"Aviso Enviado ({warning_type})", member)
        except Exception as e:
            logger.error(f"Erro ao enviar aviso para {member}: {e}")

    async def process_voice_events(self):
        """Processa eventos de voz em lotes para melhor eficiência"""
        while True:
            try:
                batch = []
                for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                    batch.append(await self.voice_event_queue.get())
                
                await self._process_voice_batch(batch)
                
                for _ in batch:
                    self.voice_event_queue.task_done()
                    
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)

    async def _process_voice_batch(self, batch):
        """Processa um lote de eventos de voz"""
        processed = {}
        
        for event in batch:
            event_type, member, before, after = event
            key = (member.id, member.guild.id)
            
            if key not in processed:
                processed[key] = {
                    'member': member,
                    'events': []
                }
            processed[key]['events'].append((before, after))
        
        for user_data in processed.values():
            try:
                await self._process_user_voice_events(user_data['member'], user_data['events'])
            except Exception as e:
                logger.error(f"Erro ao processar eventos para {user_data['member']}: {e}")

    async def _process_user_voice_events(self, member, events):
        """Processa eventos de voz para um único usuário"""
        absence_channel_id = self.config.get('absence_channel')
        audio_key = (member.id, member.guild.id)
        
        for before, after in events:
            if member.id in self.config['whitelist']['users'] or \
               any(role.id in self.config['whitelist']['roles'] for role in member.roles):
                continue
            
            # Verificar mudanças no estado do áudio (self_deaf)
            if before.channel and after.channel and before.channel == after.channel:
                if before.self_deaf != after.self_deaf:
                    if audio_key in self.active_sessions:
                        # Áudio foi desativado
                        if after.self_deaf and not before.self_deaf:
                            self.active_sessions[audio_key]['audio_disabled'] = True
                            self.active_sessions[audio_key]['audio_off_time'] = datetime.utcnow()
                            
                            # Log detalhado
                            time_in_voice = (datetime.utcnow() - self.active_sessions[audio_key]['start_time']).total_seconds()
                            embed = discord.Embed(
                                title="🔇 Áudio Desativado",
                                color=discord.Color.orange(),
                                timestamp=datetime.now(self.timezone))
                            embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                            embed.add_field(name="Usuário", value=member.mention, inline=True)
                            embed.add_field(name="Canal", value=after.channel.name, inline=True)
                            embed.add_field(name="Tempo em voz", 
                                          value=f"{int(time_in_voice//60)} minutos {int(time_in_voice%60)} segundos", 
                                          inline=False)
                            embed.set_footer(text=f"ID: {member.id}")
                            
                            await self.high_priority_queue.put((
                                self.get_channel(self.config['log_channel']),
                                None,
                                embed,
                                None,
                                "high"
                            ))
                        
                        # Áudio foi reativado
                        elif not after.self_deaf and before.self_deaf:
                            self.active_sessions[audio_key]['audio_disabled'] = False
                            if 'audio_off_time' in self.active_sessions[audio_key]:
                                audio_off_duration = (datetime.utcnow() - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                                self.active_sessions[audio_key]['total_audio_off_time'] = \
                                    self.active_sessions[audio_key].get('total_audio_off_time', 0) + audio_off_duration
                                del self.active_sessions[audio_key]['audio_off_time']
                                
                                # Log detalhado
                                total_time = (datetime.utcnow() - self.active_sessions[audio_key]['start_time']).total_seconds()
                                embed = discord.Embed(
                                    title="🔊 Áudio Reativado",
                                    color=discord.Color.green(),
                                    timestamp=datetime.now(self.timezone))
                                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                                embed.add_field(name="Usuário", value=member.mention, inline=True)
                                embed.add_field(name="Canal", value=after.channel.name, inline=True)
                                embed.add_field(name="Tempo sem áudio", 
                                              value=f"{int(audio_off_duration//60)} minutos {int(audio_off_duration%60)} segundos", 
                                              inline=True)
                                embed.add_field(name="Tempo total em voz", 
                                              value=f"{int(total_time//60)} minutos {int(total_time%60)} segundos", 
                                              inline=True)
                                embed.set_footer(text=f"ID: {member.id}")
                                
                                await self.high_priority_queue.put((
                                    self.get_channel(self.config['log_channel']),
                                    None,
                                    embed,
                                    None,
                                    "high"
                                ))
            
            # Verificar se after.channel existe antes de acessar .name
            if before.channel is None and after.channel is not None:
                if after.channel.id == absence_channel_id:
                    continue
                    
                try:
                    await self.db.log_voice_join(member.id, member.guild.id)
                    self.active_sessions[audio_key] = {
                        'start_time': datetime.utcnow(),
                        'last_audio_time': datetime.utcnow(),
                        'audio_disabled': after.self_deaf,
                        'total_audio_off_time': 0
                    }
                    
                    # Log detalhado
                    embed = discord.Embed(
                        title="🎤 Entrou em Voz",
                        color=discord.Color.green(),
                        timestamp=datetime.now(self.timezone))
                    embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                    embed.add_field(name="Usuário", value=member.mention, inline=True)
                    embed.add_field(name="Canal", value=after.channel.name, inline=True)
                    embed.add_field(name="Estado do Áudio", 
                                  value="🔇 Mudo" if after.self_deaf else "🔊 Ativo", 
                                  inline=True)
                    embed.set_footer(text=f"ID: {member.id}")
                    
                    await self.high_priority_queue.put((
                        self.get_channel(self.config['log_channel']),
                        None,
                        embed,
                        None,
                        "high"
                    ))
                except Exception as e:
                    logger.error(f"Erro ao registrar entrada em voz: {e}")
                    await self.log_action("Erro DB - Entrada em voz", member, str(e))
            
            elif before.channel is not None and after.channel is None:
                session_data = self.active_sessions.get(audio_key)
                if session_data:
                    if before.channel.id == absence_channel_id:
                        del self.active_sessions[audio_key]
                        continue
                        
                    # Calcular tempo total e subtrair o tempo com áudio desativado
                    total_time = (datetime.utcnow() - session_data['start_time']).total_seconds()
                    audio_off_time = session_data.get('total_audio_off_time', 0)
                    effective_time = total_time - audio_off_time
                    
                    if effective_time >= self.config['required_minutes'] * 60:
                        try:
                            await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
                        except Exception as e:
                            logger.error(f"Erro ao registrar saída de voz: {e}")
                            await self.log_action("Erro DB - Saída de voz", member, str(e))
                    
                    # Log detalhado
                    embed = discord.Embed(
                        title="🚪 Saiu de Voz",
                        color=discord.Color.blue(),
                        timestamp=datetime.now(self.timezone))
                    embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                    embed.add_field(name="Usuário", value=member.mention, inline=True)
                    embed.add_field(name="Canal", value=before.channel.name, inline=True)
                    embed.add_field(name="Tempo Efetivo", 
                                  value=f"{int(effective_time//60)} minutos {int(effective_time%60)} segundos", 
                                  inline=True)
                    embed.add_field(name="Tempo sem Áudio", 
                                  value=f"{int(audio_off_time//60)} minutos {int(audio_off_time%60)} segundos", 
                                  inline=True)
                    embed.set_footer(text=f"ID: {member.id}")
                    
                    await self.high_priority_queue.put((
                        self.get_channel(self.config['log_channel']),
                        None,
                        embed,
                        None,
                        "high"
                    ))
                    
                    del self.active_sessions[audio_key]
            
            elif before.channel is not None and after.channel is not None:
                if after.channel.id == absence_channel_id:
                    session_data = self.active_sessions.get(audio_key)
                    if session_data:
                        # Calcular tempo total e subtrair o tempo com áudio desativado
                        total_time = (datetime.utcnow() - session_data['start_time']).total_seconds()
                        audio_off_time = session_data.get('total_audio_off_time', 0)
                        effective_time = total_time - audio_off_time
                        
                        if effective_time >= self.config['required_minutes'] * 60:
                            try:
                                await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
                            except Exception as e:
                                logger.error(f"Erro ao registrar saída de voz (movido para ausência): {e}")
                        
                        # Log detalhado
                        embed = discord.Embed(
                            title="⏸️ Movido para Ausência",
                            color=discord.Color.light_grey(),
                            timestamp=datetime.now(self.timezone))
                        embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                        embed.add_field(name="Usuário", value=member.mention, inline=True)
                        embed.add_field(name="De", value=before.channel.name, inline=True)
                        embed.add_field(name="Para", value=after.channel.name, inline=True)
                        embed.add_field(name="Tempo Efetivo", 
                                      value=f"{int(effective_time//60)} minutos {int(effective_time%60)} segundos", 
                                      inline=True)
                        embed.add_field(name="Tempo sem Áudio", 
                                      value=f"{int(audio_off_time//60)} minutos {int(audio_off_time%60)} segundos", 
                                      inline=True)
                        embed.set_footer(text=f"ID: {member.id}")
                        
                        await self.high_priority_queue.put((
                            self.get_channel(self.config['log_channel']),
                            None,
                            embed,
                            None,
                            "high"
                        ))
                        
                        del self.active_sessions[audio_key]
                
                elif before.channel.id == absence_channel_id:
                    try:
                        await self.db.log_voice_join(member.id, member.guild.id)
                        self.active_sessions[audio_key] = {
                            'start_time': datetime.utcnow(),
                            'last_audio_time': datetime.utcnow(),
                            'audio_disabled': after.self_deaf,
                            'total_audio_off_time': 0
                        }
                        
                        # Log detalhado
                        embed = discord.Embed(
                            title="▶️ Retornou de Ausência",
                            color=discord.Color.green(),
                            timestamp=datetime.now(self.timezone))
                        embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                        embed.add_field(name="Usuário", value=member.mention, inline=True)
                        embed.add_field(name="Para", value=after.channel.name, inline=True)
                        embed.add_field(name="Estado do Áudio", 
                                      value="🔇 Mudo" if after.self_deaf else "🔊 Ativo", 
                                      inline=True)
                        embed.set_footer(text=f"ID: {member.id}")
                        
                        await self.high_priority_queue.put((
                            self.get_channel(self.config['log_channel']),
                            None,
                            embed,
                            None,
                            "high"
                        ))
                    except Exception as e:
                        logger.error(f"Erro ao registrar retorno de ausência: {e}")

    async def process_commands_queue(self):
        """Processa fila de comandos com rate limiting inteligente"""
        while True:
            try:
                interaction, command, args, kwargs = await self.command_queue.get()
                
                if self.rate_limited:
                    now = datetime.now()
                    if self.last_rate_limit and (now - self.last_rate_limit).seconds < 60:
                        try:
                            await interaction.followup.send(
                                "⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente mais tarde.")
                        except Exception as e:
                            logger.error(f"Erro ao enviar mensagem de rate limit: {e}")
                        self.command_queue.task_done()
                        continue
                    else:
                        self.rate_limited = False
                
                await asyncio.sleep(self.rate_limit_delay)
                
                try:
                    await command(interaction, *args, **kwargs)
                    
                    self.rate_limit_delay = max(self.rate_limit_delay * 0.9, 0.5)
                    
                except discord.errors.HTTPException as e:
                    if e.status == 429:
                        self.rate_limited = True
                        self.last_rate_limit = datetime.now()
                        retry_after = float(e.response.headers.get('Retry-After', 60))
                        logger.error(f"Rate limit atingido. Tentar novamente após {retry_after} segundos")
                        
                        self.rate_limit_delay = min(self.rate_limit_delay * 2, self.max_rate_limit_delay)
                        
                        await asyncio.sleep(retry_after)
                        self.rate_limited = False
                        
                        await asyncio.sleep(self.rate_limit_delay)
                        await self.command_queue.put((interaction, command, args, kwargs))
                    else:
                        raise
                except Exception as e:
                    logger.error(f"Erro ao executar comando: {e}")
                    try:
                        await interaction.followup.send(
                            "❌ Ocorreu um erro ao processar o comando.")
                    except Exception as e:
                        logger.error(f"Erro ao enviar mensagem de erro: {e}")
                
                self.command_queue.task_done()
                
            except Exception as e:
                logger.error(f"Erro no processador de comandos: {e}")
                await asyncio.sleep(1)

# Decorador para verificar cargos permitidos
def allowed_roles_only():
    async def predicate(interaction: discord.Interaction):
        if not bot.config['allowed_roles']:
            return True
            
        if interaction.user.guild_permissions.administrator:
            return True
            
        user_roles = [role.id for role in interaction.user.roles]
        if any(role_id in user_roles for role_id in bot.config['allowed_roles']):
            return True
            
        await interaction.response.send_message(
            "❌ Você não tem permissão para usar este comando.",
            ephemeral=True)
        return False
    return commands.check(predicate)

intents = discord.Intents.default()
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = InactivityBot(
    command_prefix='!', 
    intents=intents,
    max_messages=1000,
    chunk_guilds_at_startup=False,
    member_cache_flags=discord.MemberCacheFlags.none(),
    enable_debug_events=False,
    heartbeat_timeout=60.0
)

@bot.event
async def on_ready():
    logger.info(f'Bot conectado como {bot.user}')
    
    # Embed de inicialização mais bonito
    embed = discord.Embed(
        title="🤖 Bot de Controle de Atividades Iniciado",
        description=f"Conectado como {bot.user.mention}",
        color=discord.Color.green(),
        timestamp=datetime.now(bot.timezone))
    embed.add_field(name="Servidores", value=str(len(bot.guilds)), inline=True)
    embed.add_field(name="Latência", value=f"{round(bot.latency * 1000)}ms", inline=True)
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.set_footer(text="Sistema de Controle de Atividades")
    
    await bot.log_action("Inicialização", None, embed=embed)
    
    if not bot.get_channel(bot.config.get('log_channel')):
        logger.warning("Canal de logs não encontrado!")
    if not bot.get_channel(bot.config.get('notification_channel')):
        logger.warning("Canal de notificações não encontrado!")

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    try:
        await bot.voice_event_queue.put(('voice_state_update', member, before, after))
    except asyncio.QueueFull:
        logger.warning("Fila de eventos de voz cheia - evento descartado")
    except Exception as e:
        logger.error(f"Erro ao enfileirar evento de voz: {e}")

# Importar comandos
from bot_commands import *

# Iniciar o bot
if __name__ == "__main__":
    load_dotenv()
    
    required_env_vars = ['DISCORD_TOKEN', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.critical(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}")
        raise ValueError(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}")
    
    if os.getenv('RENDER', 'false').lower() == 'true':
        logger.info("Executando no Render - Configurações especiais aplicadas")
        from web_panel import keep_alive
        keep_alive()
    
    try:
        bot.run(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.critical(f"Erro ao iniciar o bot: {e}")
        raise
    finally:
        if hasattr(bot, 'db') and bot.db:
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                     bot.db.close()
            except Exception as e:
                logger.error(f"Erro ao fechar pool de conexões: {e}")