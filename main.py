from gevent import monkey
monkey.patch_all() # ADICIONE ESTAS DUAS LINHAS NO TOPO
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
import nest_asyncio
nest_asyncio.apply()


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
            "first": "⚠️ **Aviso de Inatividade** ⚠️\nVocê está prestes a perder seus cargos por inatividade. Entre em um canal de voz por pelo menos 15 minutos em 2 dias diferentes nos próximos {days} dias para evitar isso.",
            "second": "🔴 **Último Aviso** 🔴\nVocê perderá seus cargos AMANHÃ por inatividade se não cumprir os requisitos de atividade em voz.",
            "final": "❌ **Cargos Removidos** ❌\nVocê perdeu seus cargos no servidor {guild} por inatividade. Você não cumpriu os requisitos de atividade de voz (15 minutos em 2 dias diferentes dentro de {monitoring_period} dias)."
    }
}}

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
        self.config = {}
        self.timezone = pytz.timezone('America/Sao_Paulo')
        self.db = None
        self.active_sessions = {}
        self.voice_event_queue = asyncio.Queue(maxsize=500)
        self.message_queue = SmartPriorityQueue()
        self.voice_event_processor_task = None
        self.queue_processor_task = None
        self.command_processor_task = None
        self.rate_limited = False
        self.last_rate_limit = None
        self.rate_limit_delay = 2.0
        self.max_rate_limit_delay = 30.0  # Aumentado de 10 para 30 segundos
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
        self._batch_processing_size = 5  # Reduzido de 10 para 5
        self._api_request_delay = 2.0  # Aumentado de 1.0 para 2.0 segundos
        self.audio_check_task = None
        self.health_check_task = None
        
        # Novo monitor de rate limits
        self.rate_limit_monitor = RateLimitMonitor()
        self.last_rate_limit_report = 0
        self.rate_limit_report_interval = 300  # 5 minutos
        
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
        
        # API Flask
        self.flask_app = Flask(__name__)
        self.setup_api_routes()

    def setup_api_routes(self):
        """Configura as rotas da API Flask"""
        @self.flask_app.route('/api/guild/<guild_id>')
        async def get_guild_details(guild_id):
            try:
                guild = self.get_guild(int(guild_id))
                if not guild:
                    return jsonify({'error': 'Guild not found'}), 404
                    
                # Basic guild information
                guild_data = {
                    'id': guild.id,
                    'name': guild.name,
                    'icon': str(guild.icon.url) if guild.icon else None,
                    'member_count': guild.member_count,
                    'created_at': guild.created_at.isoformat(),
                    'owner_id': guild.owner_id,
                    'description': guild.description,
                    'features': guild.features,
                    'premium_tier': guild.premium_tier,
                    'premium_subscription_count': guild.premium_subscription_count,
                    'verification_level': str(guild.verification_level),
                    'channels_count': len(guild.channels),
                    'roles_count': len(guild.roles),
                    'emojis_count': len(guild.emojis),
                    'voice_channels': [{'id': c.id, 'name': c.name} for c in guild.voice_channels]
                }
                
                return jsonify(guild_data)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500

    async def check_rate_limit(self):
        """Verifica se o bot está sendo rate limited e ajusta o comportamento"""
        now = time.time()
        if self.rate_limited and now - self.last_rate_limit > self.rate_limit_retry_after:
            self.rate_limited = False
        
        # Ajustar dinamicamente o delay com base na frequência de rate limits
        if now - self.last_rate_limit_time < 300:  # Se houve rate limit nos últimos 5 minutos
            self.rate_limit_count += 1
            if self.rate_limit_count > 3:
                self.rate_limit_delay = min(
                    self.max_rate_limit_delay,
                    self.rate_limit_delay * 1.5
                )
        else:
            self.rate_limit_count = max(0, self.rate_limit_count - 1)
            self.rate_limit_delay = max(1.0, self.rate_limit_delay * 0.9)
        
        self.last_rate_limit_time = now

    async def _handle_rate_limit(self, error):
        retry_after = float(error.response.headers.get('Retry-After', 1.0))
        bucket = error.response.headers.get('X-RateLimit-Bucket', 'global')
        
        if bucket in self.rate_limit_buckets:
            self.rate_limit_buckets[bucket]['remaining'] = 0
            self.rate_limit_buckets[bucket]['reset_at'] = time.time() + retry_after
            
        self.rate_limit_delay = min(
            self.max_rate_limit_delay,
            max(retry_after, self.rate_limit_delay * 1.5)
        )
        
        details = {
            'bucket': error.response.headers.get('X-RateLimit-Bucket', 'unknown'),
            'limit': error.response.headers.get('X-RateLimit-Limit', 'unknown'),
            'remaining': error.response.headers.get('X-RateLimit-Remaining', 'unknown'),
            'reset': error.response.headers.get('X-RateLimit-Reset', 'unknown'),
            'scope': error.response.headers.get('X-RateLimit-Scope', 'global'),
            'retry_after': retry_after,
            'endpoint': str(error.response.request_info.url)
        }
        
        await self.log_action(
            "Rate Limit Detectado",
            None,
            "\n".join(f"{k}: {v}" for k, v in details.items())
        )
        
        if details['scope'] == 'global':
            await self.notify_roles(
                f"⚠️ **Alerta de Rate Limit Global** ⚠️\n"
                f"O bot atingiu um rate limit global e pode ter desempenho reduzido.\n"
                f"Tempo de espera: {retry_after:.1f} segundos",
                is_warning=True
            )
        
        logger.warning(f"Rate limit atingido no bucket {bucket}. Retry after: {retry_after}s")

    async def can_send_message(self, priority='normal'):
        now = time.time()
        
        global_bucket = self.rate_limit_buckets['global']
        if global_bucket['remaining'] <= 1 and now < global_bucket['reset_at']:
            return False
        
        queue_status = self.message_queue.qsize()
        if priority == 'critical' and queue_status['critical'] > 10:
            return False
        elif priority == 'high' and queue_status['high'] > 50:
            return False
        
        return True

    async def send_with_fallback(self, destination, content=None, embed=None, file=None):
        max_retries = 2
        for attempt in range(max_retries):
            try:
                return await self.safe_send(destination, content, embed, file)
            except discord.errors.HTTPException as e:
                if e.status == 429 and attempt == max_retries - 1:
                    logger.warning(f"Rate limit persistente, usando fallback para webhook")
                    return await self._send_via_webhook(destination, content, embed, file)
                await asyncio.sleep((attempt + 1) * 2)

    async def _send_via_webhook(self, destination, content=None, embed=None, file=None):
        if isinstance(destination, discord.TextChannel):
            try:
                webhook = await destination.create_webhook(name="RateLimitFallback")
                try:
                    await webhook.send(
                        content=content,
                        embed=embed,
                        file=file,
                        username=self.user.name,
                        avatar_url=self.user.avatar.url
                    )
                finally:
                    await webhook.delete()
                return True
            except Exception as e:
                logger.error(f"Erro no fallback de webhook: {e}")
                return False
        return False

    async def safe_send(self, channel, content=None, embed=None, file=None, retry_count=0):
        # Verificar rate limits antes de enviar
        if self.rate_limit_monitor.should_delay():
            delay = self.rate_limit_monitor.adaptive_delay
            logger.warning(f"Rate limit detectado, aguardando {delay:.2f} segundos")
            await asyncio.sleep(delay)
        
        try:
            message = await channel.send(content=content, embed=embed, file=file)
            
            # Atualizar monitor de rate limits
            if hasattr(message, '_http') and hasattr(message._http, 'response'):
                headers = message._http.response.headers
                self.rate_limit_monitor.update_from_headers(headers)
            
            # Gerar relatório periódico
            now = time.time()
            if now - self.last_rate_limit_report > self.rate_limit_report_interval:
                self.last_rate_limit_report = now
                report = self.rate_limit_monitor.get_status_report()
                logger.info(f"Relatório de Rate Limits: {report}")
                
                # Enviar alerta se estiver perto do limite
                if report['global']['remaining'] < 10:
                    await self.log_action(
                        "Alerta de Rate Limit",
                        None,
                        f"Uso elevado de API: {report['global']['remaining']}/{report['global']['limit']} requisições restantes\n"
                        f"Reset em: {report['global']['seconds_until_reset']:.0f} segundos\n"
                        f"Delay adaptativo: {report['adaptive_delay']:.2f}s"
                    )
            
            return True
        
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = float(e.response.headers.get('Retry-After', 5))
                
                # Atualizar monitor com informações do erro
                self.rate_limit_monitor.update_from_headers(e.response.headers)
                self.rate_limit_monitor.adaptive_delay = min(
                    self.rate_limit_monitor.max_delay,
                    max(retry_after, self.rate_limit_monitor.adaptive_delay * 1.5)
                )
                self.rate_limit_monitor.cooldown_until = time.time() + retry_after
                
                logger.error(f"Rate limit atingido. Tentativa {retry_count + 1}. Retry after: {retry_after}s")
                await asyncio.sleep(retry_after)
                return await self.safe_send(channel, content, embed, file, retry_count + 1)
            
            logger.error(f"Erro HTTP ao enviar mensagem: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False

    async def setup_hook(self):
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
            self.queue_processor_task = asyncio.create_task(self.process_queues())
            self.pool_monitor_task = asyncio.create_task(self.monitor_db_pool())
            self.health_check_task = asyncio.create_task(self.periodic_health_check())
            self.audio_check_task = asyncio.create_task(self.check_audio_states())

    async def check_audio_states(self):
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

    async def initialize_db(self):
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                from database import Database
                self.db = Database()
                await self.db.initialize()
                logger.info("Banco de dados inicializado com sucesso")
                
                # Testar a conexão
                async with self.db.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                
                await self.db.create_tables()
                
                db_config = await self.db.load_config(0)
                if db_config:
                    self.config.update(db_config)
                    await self.save_config()
                    logger.info("Configuração carregada do banco de dados")
                
                # Configurar reconexão automática
                self.db.pool._reconnect_interval = 60  # Tentar reconectar a cada 60 segundos
                self.db.pool._reconnect_max_attempts = 10  # Máximo de 10 tentativas
                
                break
            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if attempt == max_retries - 1:
                    raise
                
                # Backoff exponencial com jitter
                sleep_time = min(initial_delay * (2 ** attempt) + random.uniform(0, 1), 30)
                logger.info(f"Tentando novamente em {sleep_time:.2f} segundos...")
                await asyncio.sleep(sleep_time)

    async def monitor_db_pool(self):
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
        while True:
            try:
                queue_status = self.message_queue.qsize()
                queue_status['voice_events'] = self.voice_event_queue.qsize()
                
                logger.info(f"Status das filas: {queue_status}")
                
                if queue_status['voice_events'] > 300:
                    logger.warning(f"Fila de eventos de voz grande: {queue_status['voice_events']}")
                if queue_status['normal'] > 100:
                    logger.warning(f"Fila de comandos grande: {queue_status['normal']}")
                
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
                self.config['notification_channel'] = None
            if 'log_channel' not in self.config or not self.config['log_channel']:
                self.config['log_channel'] = None
            if 'absence_channel' not in self.config:
                self.config['absence_channel'] = None
                
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

    async def process_queues(self):
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
                        await self.send_with_fallback(item)
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
                if member.id in self.config['whitelist']['users'] or \
                   any(role.id in self.config['whitelist']['roles'] for role in member.roles):
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
                effective_time = total_time - audio_off_time
                
                if effective_time >= self.config['required_minutes'] * 60:
                    try:
                        await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
                    except Exception as e:
                        logger.error(f"Erro ao registrar saída de voz: {e}")
                        await self.log_action(
                            "Erro DB - Saída de voz",
                            member,
                            str(e)
                        )
                
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
        
        if after.channel.id == absence_channel_id:
            session_data = self.active_sessions.get(audio_key)
            if session_data:
                try:
                    total_time = (datetime.utcnow() - session_data['start_time']).total_seconds()
                    audio_off_time = session_data.get('total_audio_off_time', 0)
                    effective_time = total_time - audio_off_time
                    
                    if effective_time >= self.config['required_minutes'] * 60:
                        try:
                            await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
                        except Exception as e:
                            logger.error(f"Erro ao registrar saída de voz (movido para ausência): {e}")
                    
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
                    
                    await self.log_action(None, None, embed=embed)
                    
                    del self.active_sessions[audio_key]
                except KeyError:
                    pass
        
        elif before.channel.id == absence_channel_id:
            try:
                await self.db.log_voice_join(member.id, member.guild.id)
                self.active_sessions[audio_key] = {
                    'start_time': datetime.utcnow(),
                    'last_audio_time': datetime.utcnow(),
                    'audio_disabled': after.self_deaf or after.deaf,
                    'total_audio_off_time': 0
                }
                
                embed = discord.Embed(
                    title="▶️ Retornou de Ausência",
                    color=discord.Color.green(),
                    timestamp=datetime.now(self.timezone))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="Usuário", value=member.mention, inline=True)
                embed.add_field(name="Para", value=after.channel.name, inline=True)
                embed.add_field(name="Estado do Áudio", 
                              value="🔇 Mudo" if (after.self_deaf or after.deaf) else "🔊 Ativo", 
                              inline=True)
                embed.set_footer(text=f"ID: {member.id}")
                
                await self.log_action(None, None, embed=embed)
            except Exception as e:
                logger.error(f"Erro ao registrar retorno de ausência: {e}")

    async def _handle_audio_change(self, member, before, after):
        audio_key = (member.id, member.guild.id)
        if audio_key not in self.active_sessions:
            return

        audio_was_on = not (before.self_deaf or before.deaf)
        audio_is_off = after.self_deaf or after.deaf

        if audio_was_on and audio_is_off:
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
        
        elif not audio_was_on and not audio_is_off:
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

    async def process_voice_events(self):
        while True:
            try:
                batch = []
                for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                    batch.append(await self.voice_event_queue.get())
                
                await self._process_voice_batch(batch)
                
                for _ in batch:
                    self.voice_event_queue.task_done()
                    
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)

    async def log_action(self, action: str, member: Optional[discord.Member] = None, details: str = None, file: discord.File = None, embed: discord.Embed = None):
        try:
            if not hasattr(self, 'config') or 'log_channel' not in self.config or not self.config['log_channel']:
                logger.warning("Canal de logs não configurado")
                return
                
            channel = self.get_channel(self.config['log_channel'])
            if not channel:
                logger.warning("Canal de logs não encontrado")
                return
                
            if embed is not None:
                await self.message_queue.put((
                    channel,
                    None,
                    embed,
                    file
                ), priority='high')
                return
                
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
                if '\n' in details:
                    details = details.replace('\n', '\n• ')
                    embed.add_field(name="Detalhes", value=f"• {details}", inline=False)
                else:
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
            channel_id = self.config.get('notification_channel') if is_warning else self.config.get('log_channel')
            if not channel_id:
                logger.warning("Canal de notificação não configurado")
                return
                
            channel = self.get_channel(channel_id)
            if not channel:
                logger.warning(f"Canal de notificação {channel_id} não encontrado")
                return
                
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
            
            await self.message_queue.put((
                channel,
                None,
                embed,
                None
            ), priority=priority)
            
        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")
            await self.log_action("Erro de Notificação", None, f"Falha ao enviar mensagem: {str(e)}")

    async def send_dm(self, member: discord.Member, message: str):
        try:
            embed = discord.Embed(
                description=message,
                color=discord.Color.blue(),
                timestamp=datetime.now(self.timezone))
            
            await self.message_queue.put((
                member,
                None,
                embed,
                None
            ), priority='low')
        except Exception as e:
            logger.error(f"Erro ao enviar DM para {member}: {e}")

    async def send_warning(self, member: discord.Member, warning_type: str):
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
    intents=intents
)

@bot.event
async def on_ready():
    # Garante que o bot está realmente conectado
    if not bot.is_ready():
        logger.warning("Evento on_ready chamado mas bot não está pronto")
        return
    
    logger.info(f'Bot conectado como {bot.user}')
    
    for guild in bot.guilds:
        try:
            voice_channels = [c for c in guild.channels if isinstance(c, discord.VoiceChannel)]
            if not voice_channels:
                logger.warning(f"Nenhum canal de voz encontrado em {guild.name}")
                continue
                
            if bot.config.get('log_channel'):
                log_channel = bot.get_channel(bot.config['log_channel'])
                if log_channel:
                    perms = log_channel.permissions_for(guild.me)
                    if not perms.send_messages:
                        logger.error(f"Sem permissão para enviar mensagens no canal de logs em {guild.name}")
            
            if bot.config.get('notification_channel'):
                notify_channel = bot.get_channel(bot.config['notification_channel'])
                if notify_channel:
                    perms = notify_channel.permissions_for(guild.me)
                    if not perms.send_messages:
                        logger.error(f"Sem permissão para enviar mensagens no canal de notificações em {guild.name}")
        except Exception as e:
            logger.error(f"Erro ao verificar permissões em {guild.name}: {e}")
    
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