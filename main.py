import discord
from discord.ext import commands
import pytz
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional
from discord.ext import tasks
import aiomysql
import random
from collections import defaultdict
from collections import deque
from flask import Flask
import sys
import traceback

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

class RateLimitMonitor:
    def __init__(self):
        self.buckets = {}
        self.global_limits = {
            'limit': 50,
            'remaining': 50,
            'reset_at': 0
        }
        self.last_updated = 0
        self.history = deque(maxlen=100)  # Mantém histórico dos últimos 100 eventos
        self.adaptive_delay = 1.0
        self.max_delay = 10.0
        self.cooldown_until = 0
    
    def update_from_headers(self, headers):
        now = time.time()
        bucket = headers.get('X-RateLimit-Bucket', 'global')
        limit = int(headers.get('X-RateLimit-Limit', 50))
        remaining = int(headers.get('X-RateLimit-Remaining', limit))
        reset_at = float(headers.get('X-RateLimit-Reset', now + 60))
        
        if bucket == 'global':
            self.global_limits = {
                'limit': limit,
                'remaining': remaining,
                'reset_at': reset_at
            }
        else:
            self.buckets[bucket] = {
                'limit': limit,
                'remaining': remaining,
                'reset_at': reset_at,
                'last_updated': now
            }
        
        self.last_updated = now
        self.history.append({
            'time': now,
            'bucket': bucket,
            'remaining': remaining,
            'endpoint': str(headers.get('endpoint', 'unknown'))
        })
    
    def get_remaining(self, bucket='global'):
        if bucket == 'global':
            return self.global_limits['remaining']
        return self.buckets.get(bucket, {}).get('remaining', 50)
    
    def should_delay(self):
        now = time.time()
        if now < self.cooldown_until:
            return True
        
        # Verificar rate limit global
        if self.global_limits['remaining'] < 5 and now < self.global_limits['reset_at']:
            self.adaptive_delay = min(self.max_delay, self.adaptive_delay * 1.5)
            self.cooldown_until = now + self.adaptive_delay
            return True
        
        # Verificar outros buckets importantes
        for bucket, data in self.buckets.items():
            if data['remaining'] < 2 and now < data['reset_at']:
                self.adaptive_delay = min(self.max_delay, self.adaptive_delay * 1.2)
                self.cooldown_until = now + self.adaptive_delay
                return True
        
        # Reduzir gradualmente o delay quando não há rate limits
        if self.adaptive_delay > 1.0:
            self.adaptive_delay = max(1.0, self.adaptive_delay * 0.9)
        
        return False
    
    def get_status_report(self):
        now = time.time()
        report = {
            'global': {
                **self.global_limits,
                'seconds_until_reset': max(0, self.global_limits['reset_at'] - now)
            },
            'adaptive_delay': self.adaptive_delay,
            'cooldown_until': max(0, self.cooldown_until - now),
            'buckets': {}
        }
        
        for bucket, data in self.buckets.items():
            report['buckets'][bucket] = {
                'limit': data['limit'],
                'remaining': data['remaining'],
                'seconds_until_reset': max(0, data['reset_at'] - now)
            }
        
        return report

# Configurações iniciais
CONFIG_FILE = 'config.json'
DEFAULT_CONFIG = {
    "required_minutes": 15,
    "required_days": 2,
    "monitoring_period": 14,
    "kick_after_days": 30,
    "tracked_roles": [],
    "log_channel": None,
    "notification_channel": None,
    "timezone": "America/Sao_Paulo",
    "absence_channel": None,
    "allowed_roles": [],
    "whitelist": {
        "users": [],
        "roles": []
    },
    "warnings": {
        "first_warning": 3,
        "second_warning": 1,
        "messages": {
            "first": "⚠️ **Aviso de Inatividade** ⚠️\nVocê está prestes a perder seus cargos por inatividade. Entre em um canal de voz por pelo menos {required_minutes} minutos em {required_days} dias diferentes nos próximos {days} dias para evitar isso.",
            "second": "🔴 **Último Aviso** 🔴\nVocê perderá seus cargos AMANHÃ por inatividade se não cumprir os requisitos de atividade em voz ({required_minutes} minutos em {required_days} dias diferentes).",
            "final": "❌ **Cargos Removidos** ❌\nVocê perdeu seus cargos no servidor {guild} por inatividade. Você não cumpriu os requisitos de atividade de voz ({required_minutes} minutos em {required_days} dias diferentes dentro de {monitoring_period} dias)."
        }
    }
}

class SmartPriorityQueue:
    def __init__(self):
        self.queues = {
            'critical': asyncio.Queue(maxsize=20),   # Alertas e notificações urgentes
            'high': asyncio.Queue(maxsize=100),      # Comandos de administração
            'normal': asyncio.Queue(maxsize=500),    # Mensagens regulares
            'low': asyncio.Queue(maxsize=1000)       # Logs e estatísticas
        }
        self.bucket_limits = {
            'critical': 5,   # Máximo de 5 mensagens por segundo
            'high': 2,       # Máximo de 2 mensagens por segundo
            'normal': 1,     # Máximo de 1 mensagem por segundo
            'low': 0.5       # Máximo de 1 mensagem a cada 2 segundos
        }
        self.last_sent = {priority: 0 for priority in self.queues}
    
    async def get_next_message(self):
        # Verificar rate limits por prioridade
        now = time.time()
        for priority in ['critical', 'high', 'normal', 'low']:
            if not self.queues[priority].empty():
                if now - self.last_sent[priority] >= 1/self.bucket_limits[priority]:
                    self.last_sent[priority] = now
                    return await self.queues[priority].get(), priority
        return None, None
    
    async def put(self, item, priority='normal'):
        await self.queues[priority].put(item)
    
    def task_done(self, priority):
        self.queues[priority].task_done()
    
    def qsize(self):
        return {priority: q.qsize() for priority, q in self.queues.items()}

class InactivityBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        # Initialize the bot with optimized settings
        kwargs.update({
            'max_messages': 100,
            'chunk_guilds_at_startup': False,
            'member_cache_flags': discord.MemberCacheFlags.none(),
            'enable_debug_events': False,
            'heartbeat_timeout': 120.0,
            'guild_ready_timeout': 30.0,
            'connect_timeout': 60.0,
            'reconnect': True,
            'shard_count': 1,
            'shard_ids': None,
            'activity': None,
            'status': discord.Status.online
        })
        super().__init__(*args, **kwargs)
        
        # Adicione esta linha para inicializar o atributo _ready
        self._ready = asyncio.Event()
        
        # Configurações do bot
        self.config = {}
        self.timezone = pytz.timezone('America/Sao_Paulo')
        self.db = None
        self.db_connection_failed = False
        self.active_sessions = {}
        self.voice_event_queue = asyncio.Queue(maxsize=500)
        self.message_queue = SmartPriorityQueue()
        self.voice_event_processor_task = None
        self.queue_processor_task = None
        self.command_processor_task = None
        self.rate_limited = False
        self.last_rate_limit = None
        self.rate_limit_delay = 2.0
        self.max_rate_limit_delay = 30.0
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
        self._batch_processing_size = 5
        self._api_request_delay = 2.0
        self.audio_check_task = None
        self.health_check_task = None
        self._tasks_started = False
        
        # Monitor de rate limits
        self.rate_limit_monitor = RateLimitMonitor()
        self.last_rate_limit_report = 0
        self.rate_limit_report_interval = 300
        
        # Rate limit improvements
        self.rate_limit_buckets = {
            'global': {
                'limit': 50,
                'remaining': 50,
                'reset_at': 0,
                'last_update': 0
            },
            'messages': {
                'limit': 10,
                'remaining': 10,
                'reset_at': 0,
                'last_update': 0
            }
        }
        self.message_cache = {
            'embeds': defaultdict(dict),
            'responses': defaultdict(dict)
        }
        self.cache_ttl = 300

    async def send_with_fallback(self, destination, content=None, embed=None, file=None):
        """Envia mensagens com tratamento de erros e fallback para rate limits."""
        try:
            if file:
                await destination.send(content=content, embed=embed, file=file)
            elif embed:
                await destination.send(embed=embed)
            elif content:
                await destination.send(content)
        except discord.HTTPException as e:
            if e.code == 429:  # Rate limited
                retry_after = e.retry_after
                logger.warning(f"Rate limit atingido. Tentando novamente em {retry_after} segundos")
                await asyncio.sleep(retry_after)
                await self.send_with_fallback(destination, content, embed, file)
            else:
                logger.error(f"Erro ao enviar mensagem para {destination}: {e}")
                raise

    async def on_error(self, event, *args, **kwargs):
        """Tratamento de erros genéricos."""
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        logger.error(f"Exceção não tratada no evento '{event}'", exc_info=(exc_type, exc_value, exc_traceback))
        
        log_message = (
            f"**Exceção Não Tratada no Evento: `{event}`**\n"
            f"**Args:** `{args}`\n"
            f"**Kwargs:** `{kwargs}`\n"
            f"```python\n{tb_details[:1800]}\n```"
        )
        await self.log_action("Erro Crítico de Evento", details=log_message)

    async def setup_hook(self):
        """
        Este hook é executado automaticamente após o login, mas antes de o bot
        ser marcado como pronto. É o local ideal para configurações assíncronas.
        """
        # Adicione esta flag para controlar o início das tasks
        self._tasks_started = False

        if self._setup_complete:
            return
        
        self.load_config()
        await self.initialize_db()
        
        # Prossiga apenas se a conexão com o DB for bem-sucedida
        if self.db and not self.db_connection_failed:
            from database import DatabaseBackup
            self.db_backup = DatabaseBackup(self.db)
            
            # Sincronizar comandos slash
            try:
                synced = await self.tree.sync()
                logger.info(f"Comandos slash sincronizados: {len(synced)} comandos.")
            except Exception as e:
                logger.error(f"Erro ao sincronizar comandos slash: {e}")

            self._setup_complete = True
            logger.info("Setup hook concluído. A inicialização das tarefas aguardará o evento on_ready.")
        else:
            logger.critical("Falha na inicialização do banco de dados. As tarefas não serão iniciadas.")
            self.db_connection_failed = True

    async def initialize_db(self):
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                from database import Database
                self.db = Database()
                await self.db.initialize()
                logger.info("Conexão com o banco de dados estabelecida com sucesso.")
                
                async with self.db.pool.acquire() as conn:
                    await conn.ping(reconnect=True)
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                
                await self.db.create_tables()
                
                db_config = await self.db.load_config(0)
                if db_config:
                    self.config.update(db_config)
                    await self.save_config()
                    logger.info("Configuração carregada do banco de dados")
                
                return
            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if self.db and self.db.pool:
                    await self.db.close()
                    self.db = None

                if attempt == max_retries - 1:
                    logger.critical("Falha ao conectar ao banco de dados após várias tentativas.")
                    self.db_connection_failed = True
                    return
                
                sleep_time = min(initial_delay * (2 ** attempt) + random.uniform(0, 1), 30)
                logger.info(f"Tentando novamente em {sleep_time:.2f} segundos...")
                await asyncio.sleep(sleep_time)

    async def check_audio_states(self):
        await self.wait_until_ready()
        while True:
            try:
                for guild in self.guilds:
                    for voice_channel in guild.voice_channels:
                        for member in voice_channel.members:
                            audio_key = (member.id, guild.id)
                            if audio_key in self.active_sessions:
                                current_audio_state = member.voice.self_deaf or member.voice.deaf
                                
                                if current_audio_state and not self.active_sessions[audio_key]['audio_disabled']:
                                    self.active_sessions[audio_key]['audio_disabled'] = True
                                    self.active_sessions[audio_key]['audio_off_time'] = datetime.utcnow()
                                    
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
                                    
                                    await self.log_action(None, None, embed=embed)
                                
                                elif not current_audio_state and self.active_sessions[audio_key]['audio_disabled']:
                                    self.active_sessions[audio_key]['audio_disabled'] = False
                                    if 'audio_off_time' in self.active_sessions[audio_key]:
                                        audio_off_duration = (datetime.utcnow() - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                                        self.active_sessions[audio_key]['total_audio_off_time'] = \
                                            self.active_sessions[audio_key].get('total_audio_off_time', 0) + audio_off_duration
                                        del self.active_sessions[audio_key]['audio_off_time']
                                        
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
                                        
                                        await self.log_action(None, None, embed=embed)
                
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Erro ao verificar estados de áudio: {e}")
                await asyncio.sleep(60)

    async def monitor_db_pool(self):
        await self.wait_until_ready()
        while True:
            try:
                if hasattr(self, 'db') and self.db and self.db.pool:
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
        await self.wait_until_ready()
        while True:
            try:
                queue_status = self.message_queue.qsize()
                queue_status['voice_events'] = self.voice_event_queue.qsize()
                
                logger.info(f"Status das filas: {queue_status}")
                
                if queue_status['voice_events'] > 300:
                    logger.warning(f"Fila de eventos de voz grande: {queue_status['voice_events']}")
                if queue_status['normal'] > 100:
                    logger.warning(f"Fila de comandos grande: {queue_status['normal']}")
                
                if hasattr(self, 'db') and self.db and self.db.pool:
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
            
            # Garantir que chaves essenciais existam
            for key, value in DEFAULT_CONFIG.items():
                self.config.setdefault(key, value)

            logger.info("Configuração carregada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            self.config = DEFAULT_CONFIG
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)

    async def save_config(self):
        if not hasattr(self, 'config') or not self.config:
            return
            
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            if hasattr(self, 'db') and self.db and self.db.pool:
                try:
                    await self.db.save_config(0, self.config)
                    logger.info("Configuração salva no banco de dados com sucesso")
                except Exception as db_error:
                    logger.error(f"Erro ao salvar configuração no banco de dados: {db_error}")
                    
            self._last_config_save = datetime.now()
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            raise

    async def process_queues(self):
        await self.wait_until_ready()
        while True:
            try:
                if not self.voice_event_queue.empty():
                    batch = []
                    for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                        batch.append(await self.voice_event_queue.get())
                    
                    await self._process_voice_batch(batch)
                    
                    for _ in batch:
                        self.voice_event_queue.task_done()
                
                item, priority = await self.message_queue.get_next_message()
                if item is None:
                    await asyncio.sleep(0.1)
                    continue
                    
                try:
                    if isinstance(item, tuple):
                        if len(item) == 4:
                            destination, content, embed, file = item
                            if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                                await self.send_with_fallback(destination, content, embed, file)
                            else:
                                logger.warning(f"Destino inválido para mensagem: {type(destination)}")
                        elif len(item) == 2:
                            destination, embed = item
                            if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                                await self.send_with_fallback(destination, embed=embed)
                            else:
                                logger.warning(f"Destino inválido para mensagem: {type(destination)}")
                        else:
                            logger.warning(f"Item da fila em formato desconhecido: {item}")
                    elif isinstance(item, (discord.TextChannel, discord.User, discord.Member)):
                        logger.warning(f"Item da fila é um destino direto, mas não há conteúdo: {item}")
                    else:
                        logger.warning(f"Item da fila não é um destino válido: {type(item)}")
                except Exception as e:
                    logger.error(f"Erro ao processar item da fila: {e}")
                    
                self.message_queue.task_done(priority)
                    
            except Exception as e:
                logger.error(f"Erro no processador de filas: {e}")
                await asyncio.sleep(1)

    async def _process_voice_batch(self, batch):
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
        if not hasattr(self, 'config') or 'absence_channel' not in self.config:
            logger.error("Configuração do canal de ausência não encontrada")
            return

        absence_channel_id = self.config['absence_channel']
        audio_key = (member.id, member.guild.id)
        
        for before, after in events:
            try:
                # Ignorar bots
                if member.bot:
                    continue

                if member.id in self.config.get('whitelist', {}).get('users', []) or \
                   any(role.id in self.config.get('whitelist', {}).get('roles', []) for role in member.roles):
                    continue
                
                if before.channel is None and after.channel is not None and after.channel.id != absence_channel_id:
                    await self._handle_voice_join(member, after)
                
                elif before.channel is not None and after.channel is None and before.channel.id != absence_channel_id:
                    await self._handle_voice_leave(member, before)
                
                elif before.channel is not None and after.channel is not None and before.channel != after.channel:
                    await self._handle_voice_move(member, before, after, absence_channel_id)
                
                elif before.channel is not None and after.channel is not None and before.channel == after.channel:
                    if (before.self_deaf != after.self_deaf) or (before.deaf != after.deaf):
                        await self._handle_audio_change(member, before, after)

            except Exception as e:
                logger.error(f"Erro ao processar evento de voz para {member}: {e}")

    async def _handle_voice_join(self, member, after):
        try:
            await self.db.log_voice_join(member.id, member.guild.id)
            self.active_sessions[(member.id, member.guild.id)] = {
                'start_time': datetime.utcnow(),
                'last_audio_time': datetime.utcnow(),
                'audio_disabled': after.self_deaf or after.deaf,
                'total_audio_off_time': 0
            }
            
            embed = discord.Embed(
                title="🎤 Entrou em Voz",
                color=discord.Color.green(),
                timestamp=datetime.now(self.timezone))
            embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
            embed.add_field(name="Usuário", value=member.mention, inline=True)
            embed.add_field(name="Canal", value=after.channel.name, inline=True)
            embed.add_field(name="Estado do Áudio", 
                          value="🔇 Mudo" if (after.self_deaf or after.deaf) else "🔊 Ativo", 
                          inline=True)
            embed.set_footer(text=f"ID: {member.id}")
            
            await self.log_action(None, None, embed=embed)
            
        except Exception as e:
            logger.error(f"Erro ao registrar entrada em voz: {e}")
            await self.log_action(
                "Erro DB - Entrada em voz",
                member,
                str(e)
            )

    async def _handle_voice_leave(self, member, before):
        session_data = self.active_sessions.get((member.id, member.guild.id))
        if session_data:
            try:
                total_time = (datetime.utcnow() - session_data['start_time']).total_seconds()
                audio_off_time = session_data.get('total_audio_off_time', 0)
                
                if session_data.get('audio_disabled'):
                    audio_off_duration = (datetime.utcnow() - session_data.get('audio_off_time', session_data['start_time'])).total_seconds()
                    audio_off_time += audio_off_duration
                
                effective_time = total_time - audio_off_time
                
                if effective_time >= self.config['required_minutes'] * 60:
                    try:
                        await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
                    except Exception as e:
                        logger.error(f"Erro ao registrar saída de voz: {e}")
                        await self.log_action(
                            "Erro DB - Saída de voz",
                            member,
                            str(e))
                
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
                
                await self.log_action(None, None, embed=embed)
                
                del self.active_sessions[(member.id, member.guild.id)]
            except KeyError:
                pass
            except Exception as e:
                logger.error(f"Erro ao processar saída de voz: {e}")

    async def _handle_voice_move(self, member, before, after, absence_channel_id):
        audio_key = (member.id, member.guild.id)
        
        if after.channel.id == absence_channel_id and before.channel.id != absence_channel_id:
            await self._handle_voice_leave(member, before)
        
        elif before.channel.id == absence_channel_id and after.channel.id != absence_channel_id:
            await self._handle_voice_join(member, after)
        
        else:
            if audio_key in self.active_sessions:
                embed = discord.Embed(
                    title="🔄 Movido entre Canais",
                    color=discord.Color.light_grey(),
                    timestamp=datetime.now(self.timezone))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="De", value=before.channel.name, inline=True)
                embed.add_field(name="Para", value=after.channel.name, inline=True)
                embed.set_footer(text=f"ID: {member.id}")
                await self.log_action(None, None, embed=embed)

    async def _handle_audio_change(self, member, before, after):
        audio_key = (member.id, member.guild.id)
        if audio_key not in self.active_sessions:
            if after.channel is not None:
                await self._handle_voice_join(member, after)
            return

        audio_was_off = before.self_deaf or before.deaf
        audio_is_off = after.self_deaf or after.deaf

        if audio_was_off and not audio_is_off:
            self.active_sessions[audio_key]['audio_disabled'] = False
            if 'audio_off_time' in self.active_sessions[audio_key]:
                audio_off_duration = (datetime.utcnow() - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                self.active_sessions[audio_key]['total_audio_off_time'] = \
                    self.active_sessions[audio_key].get('total_audio_off_time', 0) + audio_off_duration
                del self.active_sessions[audio_key]['audio_off_time']
                
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
                
                await self.log_action(None, None, embed=embed)
        
        elif not audio_was_off and audio_is_off:
            self.active_sessions[audio_key]['audio_disabled'] = True
            self.active_sessions[audio_key]['audio_off_time'] = datetime.utcnow()
            
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
            
            await self.log_action(None, None, embed=embed)

    async def process_voice_events(self):
        await self.wait_until_ready()
        while True:
            try:
                event = await self.voice_event_queue.get()
                await self._process_voice_batch([event])
                self.voice_event_queue.task_done()
                    
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)

    async def log_action(self, action: str, member: Optional[discord.Member] = None, details: str = None, file: discord.File = None, embed: discord.Embed = None):
        try:
            log_channel_id = self.config.get('log_channel')
            if not log_channel_id:
                logger.warning("Canal de logs não configurado")
                return
                
            channel = self.get_channel(log_channel_id)
            if not channel:
                logger.warning(f"Canal de logs com ID {log_channel_id} não encontrado")
                return
                
            if embed is not None:
                await self.message_queue.put((
                    channel,
                    None,
                    embed,
                    file
                ), priority='high')
                return
                
            if action:
                if "Áudio Desativado" in str(action):
                    color = discord.Color.orange()
                    icon = "🔇"
                elif "Áudio Reativado" in str(action):
                    color = discord.Color.green()
                    icon = "🔊"
                elif "Erro" in str(action):
                    color = discord.Color.red()
                    icon = "❌"
                elif "Aviso" in str(action):
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
                    if len(details) > 1024:
                        details = details[:1021] + "..."
                    embed.add_field(name="Detalhes", value=details, inline=False)
                
                await self.message_queue.put((
                    channel,
                    None,
                    embed,
                    file
                ), priority='high')
            
        except Exception as e:
            logger.error(f"Erro ao registrar ação no log: {e}")

    async def notify_roles(self, message: str, is_warning: bool = False):
        try:
            channel_id = self.config.get('notification_channel')
            if not channel_id:
                logger.warning("Canal de notificação não configurado")
                return
                
            channel = self.get_channel(channel_id)
            if not channel:
                logger.warning(f"Canal de notificação {channel_id} não encontrado")
                return
                
            if is_warning:
                embed = discord.Embed(
                    title="⚠️ Aviso do Sistema",
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
            
            await self.message_queue.put((
                channel,
                None,
                embed,
                None
            ), priority=priority)
            
        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")
            await self.log_action("Erro de Notificação", None, f"Falha ao enviar mensagem: {str(e)}")

    async def send_dm(self, member: discord.Member, message_content: str, embed: discord.Embed):
        try:
            await self.message_queue.put((
                member,
                None,
                embed,
                None
            ), priority='low')
        except discord.Forbidden:
            logger.warning(f"Não foi possível enviar DM para {member.display_name}. (DMs desabilitadas)")
            await self.log_action(
                "Falha ao Enviar DM", 
                member, 
                "O usuário provavelmente desabilitou DMs de membros do servidor."
            )
        except Exception as e:
            logger.error(f"Erro ao enviar DM para {member}: {e}")

    async def send_warning(self, member: discord.Member, warning_type: str):
        try:
            warnings_config = self.config.get('warnings', {})
            messages = warnings_config.get('messages', {})
            
            message_template = messages.get(warning_type)
            if not message_template:
                logger.warning(f"Template de mensagem de aviso para '{warning_type}' não encontrado.")
                return

            format_args = {
                'days': warnings_config.get('first_warning', 'N/A'),
                'monitoring_period': self.config.get('monitoring_period', 'N/A'),
                'required_minutes': self.config.get('required_minutes', 'N/A'),
                'required_days': self.config.get('required_days', 'N/A'),
                'guild': member.guild.name
            }
            message = message_template.format(**format_args)
            
            if warning_type == 'first':
                title = "⚠️ Primeiro Aviso de Inatividade"
                color = discord.Color.gold()
            elif warning_type == 'second':
                title = "🔴 Último Aviso de Inatividade"
                color = discord.Color.red()
            else:
                title = "❌ Cargos Removidos por Inatividade"
                color = discord.Color.dark_red()
            
            embed = discord.Embed(
                title=title,
                description=message,
                color=color,
                timestamp=datetime.now(self.timezone))
            
            if member.guild.icon:
                embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url)
            
            await self.send_dm(member, message, embed)
            if self.db:
                await self.db.log_warning(member.id, member.guild.id, warning_type)
            await self.log_action(f"Aviso Enviado ({warning_type})", member)
        except Exception as e:
            logger.error(f"Erro ao enviar aviso para {member}: {e}")

def allowed_roles_only():
    async def predicate(interaction: discord.Interaction):
        if not bot.config.get('allowed_roles'):
            return True
            
        if interaction.user.guild_permissions.administrator:
            return True
            
        user_role_ids = {role.id for role in interaction.user.roles}
        allowed_role_ids = set(bot.config.get('allowed_roles', []))
        
        if user_role_ids.intersection(allowed_role_ids):
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
    intents=intents
)

@bot.event
async def on_ready():
    try:
        logger.info(f'Bot conectado como {bot.user}')
        logger.info(f"Latência: {round(bot.latency * 1000)}ms")
        
        # Verificar se já está pronto para evitar múltiplas inicializações
        if hasattr(bot, '_ready_set') and bot._ready_set:
            return
            
        bot._ready_set = True
        
        # --------------------------------------------------------------------
        # ADICIONE A LÓGICA DE START DAS TASKS AQUI
        # --------------------------------------------------------------------
        if not bot._tasks_started:
            logger.info("Bot está pronto. Iniciando tarefas de fundo...")
            
            # Importa as funções de tarefa diretamente de tasks.py
            from tasks import (
                inactivity_check, check_warnings, cleanup_members,
                database_backup, cleanup_old_data, monitor_rate_limits,
                report_metrics, health_check, check_missed_periods
            )
            
            # Inicia cada tarefa usando o loop do bot
            bot.loop.create_task(inactivity_check())
            bot.loop.create_task(check_warnings())
            bot.loop.create_task(cleanup_members())
            bot.loop.create_task(database_backup())
            bot.loop.create_task(cleanup_old_data())
            bot.loop.create_task(monitor_rate_limits())
            bot.loop.create_task(report_metrics())
            bot.loop.create_task(health_check())
            
            # Inicia as outras tarefas que já estavam aqui
            bot.voice_event_processor_task = bot.loop.create_task(bot.process_voice_events())
            bot.queue_processor_task = bot.loop.create_task(bot.process_queues())
            bot.pool_monitor_task = bot.loop.create_task(bot.monitor_db_pool())
            bot.health_check_task = bot.loop.create_task(bot.periodic_health_check())
            bot.audio_check_task = bot.loop.create_task(bot.check_audio_states())
            bot.loop.create_task(check_missed_periods())

            bot._tasks_started = True
            logger.info("Todas as tarefas de fundo foram agendadas com sucesso.")

        for guild in bot.guilds:
            try:
                log_channel_id = bot.config.get('log_channel')
                if log_channel_id:
                    log_channel = bot.get_channel(log_channel_id)
                    if not log_channel:
                        logger.error(f"Canal de logs (ID: {log_channel_id}) não encontrado no servidor {guild.name}.")
                
                notification_channel_id = bot.config.get('notification_channel')
                if notification_channel_id:
                    notify_channel = bot.get_channel(notification_channel_id)
                    if not notify_channel:
                        logger.error(f"Canal de notificações (ID: {notification_channel_id}) não encontrado no servidor {guild.name}.")

            except Exception as e:
                logger.error(f"Erro durante a validação de canais no on_ready para a guilda {guild.name}: {e}", exc_info=True)
        
        try:
            embed = discord.Embed(
                title="✅ Bot de Controle de Atividades Online",
                description=f"Conectado como {bot.user.mention}",
                color=discord.Color.green(),
                timestamp=datetime.now(bot.timezone))
            embed.add_field(name="Servidores", value=str(len(bot.guilds)), inline=True)
            embed.add_field(name="Latência", value=f"{round(bot.latency * 1000)}ms", inline=True)
            embed.set_thumbnail(url=bot.user.display_avatar.url)
            embed.set_footer(text="Sistema de Controle de Atividades - Operacional")
            
            await bot.log_action(None, None, embed=embed)
        except Exception as e:
            logger.error(f"Erro ao enviar embed de inicialização no on_ready: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Erro crítico no on_ready: {e}", exc_info=True)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    try:
        await bot.voice_event_queue.put(('voice_state_update', member, before, after))
    except asyncio.QueueFull:
        logger.warning("Fila de eventos de voz cheia - evento descartado")
    except Exception as e:
        logger.error(f"Erro ao enfileirar evento de voz: {e}")

# Importar comandos
from bot_commands import *

async def main():
    load_dotenv()
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    async with bot:
        await bot.start(DISCORD_TOKEN)

if __name__ == '__main__':
    asyncio.run(main())