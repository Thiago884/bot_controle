# main.py
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
from typing import Optional, List
from discord.ext import tasks
import random
from collections import defaultdict
from collections import deque
import sys
import traceback
from io import BytesIO
# Adições para o servidor web
from flask import Flask
from threading import Thread

# Importe sua classe Database
from database import Database

# Configuração do logger
def setup_logger():
    logger = logging.getLogger('inactivity_bot')
    if logger.handlers:  # Se já existem handlers, não adicione novos
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

# CORREÇÃO: A implementação antiga do RateLimitMonitor foi substituída pela versão injetada, que é mais robusta.
class RateLimitMonitor:
    def __init__(self):
        # Rastreia os limites por 'bucket' (rota da API)
        self.buckets = {}
        # Mantém um histórico recente de requisições para análise
        self.history = deque(maxlen=100)
        # Delay adaptativo que aumenta sob pressão e diminui gradualmente
        self.adaptive_delay = 1.0
        self.max_delay = 30.0  # Aumentado para 30 segundos
        # Timestamps para saber quando o bot deve entrar em modo de resfriamento
        self.cooldown_until = 0
        self.cloudflare_blocked_until = 0

    def update_from_headers(self, headers: dict):
        """Atualiza os limites com base nos headers de uma resposta da API do Discord.
        Este método é o cérebro da operação, sendo chamado a cada requisição."""
        now = time.time()
        bucket = headers.get('X-RateLimit-Bucket')

        if not bucket:
            return  # Não é uma requisição com headers de rate limit

        remaining = headers.get('X-RateLimit-Remaining')
        reset_after = headers.get('X-RateLimit-Reset-After')

        if remaining is None or reset_after is None:
            return

        try:
            remaining_i = int(remaining)
            reset_after_f = float(reset_after)
        except Exception:
            # Valores inesperados — não atualiza
            return

        self.buckets[bucket] = {
            'remaining': remaining_i,
            'reset_at': now + reset_after_f
        }
        self.history.append({'time': now, 'bucket': bucket, 'remaining': remaining_i})
        
        # Lógica proativa: se estamos com poucas requisições, começamos a desacelerar
        if remaining_i < 3:
            # Aumenta o delay adaptativo um pouco mais agressivamente
            self.adaptive_delay = min(self.max_delay, self.adaptive_delay * 1.5)
            self.cooldown_until = now + self.adaptive_delay
            logger.warning(f"Rate limit baixo no bucket {bucket} ({remaining_i} restantes). Delay adaptativo aumentado para {self.adaptive_delay:.2f}s.")

    def handle_cloudflare_block(self):
        """Ativado ao receber um erro 1015 do Cloudflare.
        Aumenta drasticamente o delay para evitar um ban temporário."""
        now = time.time()
        # Entra em modo de resfriamento por 60 segundos + um tempo aleatório
        self.cloudflare_blocked_until = now + 60 + random.uniform(5, 15)
        self.adaptive_delay = self.max_delay  # Define o delay para o máximo
        logger.critical(f"BLOQUEIO DO CLOUDFLARE DETECTADO! Entrando em modo de resfriamento por {self.cloudflare_blocked_until - now:.2f} segundos.")

    def get_delay(self) -> float:
        """Calcula o delay necessário antes da próxima operação.
        Retorna 0 se nenhuma ação for necessária."""
        now = time.time()
        
        # 1. Verifica bloqueio do Cloudflare (mais crítico)
        if now < self.cloudflare_blocked_until:
            return self.cloudflare_blocked_until - now

        # 2. Verifica se estamos em resfriamento por baixo número de requisições
        if now < self.cooldown_until:
            # Reduz o delay adaptativo gradualmente a cada verificação
            self.adaptive_delay = max(1.0, self.adaptive_delay * 0.95)
            return self.cooldown_until - now

        # 3. Reseta o delay adaptativo se não houver mais pressão
        self.adaptive_delay = max(1.0, self.adaptive_delay * 0.9)
        
        return 0.0

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
    "notification_roles_dm": [],
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
            "second": "🔴 **Último Aviso** 🔴\nVocê perderá seus cargos {days_remaining} por inatividade se não cumprir os requisitos de atividade em voz ({required_minutes} minutos em {required_days} dias diferentes).",
            "final": "❌ **Cargos Removidos** ❌\nVocê perdeu seus cargos no servidor {guild} por inatividade. Você não cumpriu os requisitos de atividade de voz ({required_minutes} minutos em {required_days} dias diferentes dentro de {monitoring_period} dias)."
        }
    }
}

class SmartPriorityQueue:
    def __init__(self):
        self.queues = {
            'critical': asyncio.Queue(maxsize=20),
            'high': asyncio.Queue(maxsize=100),
            'normal': asyncio.Queue(maxsize=500),
            'low': asyncio.Queue(maxsize=1000)
        }
        self.bucket_limits = {
            'critical': 5,
            'high': 2,
            'normal': 1,
            'low': 0.5
        }
        self.last_sent = {priority: 0 for priority in self.queues}
        self._adaptive_batch_size = 5  # Tamanho inicial do lote
        self._min_batch_size = 1
        self._max_batch_size = 10
    
    def adjust_batch_size(self, success: bool):
        """Ajusta dinamicamente o tamanho do lote baseado no sucesso"""
        if success:
            self._adaptive_batch_size = min(
                self._max_batch_size,
                self._adaptive_batch_size + 1
            )
        else:
            self._adaptive_batch_size = max(
                self._min_batch_size,
                self._adaptive_batch_size - 1
            )
    
    async def get_next_message(self):
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
    
    @property
    def batch_size(self):
        return self._adaptive_batch_size

class InactivityBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        member_cache_flags = discord.MemberCacheFlags.from_intents(kwargs.get('intents'))
        member_cache_flags.voice = True
        member_cache_flags.joined = True
        
        kwargs.update({
            'max_messages': 100,
            'chunk_guilds_at_startup': True,
            'member_cache_flags': member_cache_flags,
            'enable_debug_events': False,
            'heartbeat_timeout': 120.0,
            'guild_ready_timeout': 30.0,
            'connect_timeout': 60.0,
            'reconnect': True,
            'shard_count': 1,
            'shard_ids': None,
            'activity': None,
            'status': discord.Status.online,
        })
        super().__init__(*args, **kwargs)
        
        self.config = DEFAULT_CONFIG
        self.timezone = pytz.timezone('America/Sao_Paulo')
        self._ready = asyncio.Event()
        self.db = None
        self.db_connection_failed = False
        self.active_sessions = {}
        self.voice_event_queue = asyncio.Queue(maxsize=500)
        self.message_queue = SmartPriorityQueue()
        self.voice_event_processor_task = None
        self.queue_processor_task = None
        self.command_processor_task = None
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
        self._is_initialized = False
        
        self.rate_limit_monitor = RateLimitMonitor()
        self.last_rate_limit_report = 0
        self.rate_limit_report_interval = 300
        
        self.cache_ttl = 300
        
        self._connection_attempts = 0
        self._max_connection_attempts = 5
        self._connection_delay = 15.0
        
        self.event_counter = 0
        self.last_reconnect_time = None

    def generate_event_id(self):
        self.event_counter += 1
        return f"{int(time.time())}_{self.event_counter}"
        
    async def clear_queues(self):
        self.voice_event_queue = asyncio.Queue(maxsize=500)
        self.message_queue = SmartPriorityQueue()
        self.event_counter = 0
        self.last_reconnect_time = datetime.now(pytz.UTC)
        logger.info("Filas de eventos limpas")

    # CORREÇÃO: Lógica de reconexão aprimorada com backoff mais longo e aleatório para evitar bloqueios do Cloudflare.
    async def start(self, token: str, *, reconnect: bool = True) -> None:
        """Sobrescreve o método start para lidar com rate limits de forma robusta."""
        wait_before_next_try = 0

        while self._connection_attempts < self._max_connection_attempts:
            try:
                if wait_before_next_try > 0:
                    logger.info(f"Aguardando {wait_before_next_try:.2f} segundos antes da próxima tentativa de conexão.")
                    await asyncio.sleep(wait_before_next_try)

                self._connection_attempts += 1
                logger.info(f"Tentando conectar ao Discord (Tentativa {self._connection_attempts}/{self._max_connection_attempts})...")
                
                await super().start(token, reconnect=reconnect)
                
                logger.info("O bot foi desconectado. Encerrando o loop de conexão.")
                break

            except discord.HTTPException as e:
                if "Cloudflare" in str(e) or "1015" in str(e) or e.status == 429:
                    self.rate_limit_monitor.handle_cloudflare_block()
                    
                    base_wait = 120
                    random_factor = random.uniform(30, 90)
                    attempt_multiplier = self._connection_attempts
                    wait_before_next_try = base_wait + (random_factor * attempt_multiplier)
                    
                    logger.warning(
                        f"Bloqueio do Cloudflare/Rate limit severo detectado. "
                        f"Tentativa {self._connection_attempts}/{self._max_connection_attempts} falhou."
                    )
                else:
                    wait_before_next_try = (self._connection_delay * (2 ** (self._connection_attempts - 1))) + random.uniform(0, 5)
                    logger.error(
                        f"Erro HTTP {e.status} ao conectar. "
                        f"Tentativa {self._connection_attempts}/{self._max_connection_attempts} falhou.",
                        exc_info=True
                    )
                
                if self._connection_attempts >= self._max_connection_attempts:
                    logger.critical("Máximo de tentativas de conexão atingido devido a erro HTTP. Desistindo.")
                    raise
                    
            except Exception as e:
                wait_before_next_try = (self._connection_delay * (2 ** (self._connection_attempts - 1))) + random.uniform(0, 5)
                logger.error(
                    f"Erro inesperado ao conectar. "
                    f"Tentativa {self._connection_attempts}/{self._max_connection_attempts} falhou.",
                    exc_info=True
                )
                if self._connection_attempts >= self._max_connection_attempts:
                    logger.critical("Máximo de tentativas de conexão atingido devido a erro inesperado. Desistindo.", exc_info=True)
                    raise

    async def initialize_db(self):
        """Inicializa a conexão com o banco de dados usando a classe Database."""
        if self._is_initialized:
            return True

        try:
            self.db = Database()
            success = await self.db.initialize()

            if not success:
                logger.critical("Falha na inicialização do banco de dados")
                return False

            logger.info("Conexão com o banco de dados (via asyncpg) estabelecida com sucesso.")

            from database import DatabaseBackup
            self.db_backup = DatabaseBackup(self.db)
            logger.info("Backup do banco de dados inicializado")

            try:
                async with self.db.pool.acquire() as conn:
                    await asyncio.wait_for(conn.execute("SELECT 1"), timeout=10)
            except Exception as e:
                logger.error(f"Falha ao verificar conexão com o banco: {e}")
                self.db_connection_failed = True
                return False

            self._is_initialized = True

            await self.load_config()

            return True

        except Exception as e:
            logger.critical(f"Falha crítica ao inicializar o banco de dados: {e}", exc_info=True)
            self.db_connection_failed = True
            try:
                self.db = Database()
                self.db.pool = None
            except Exception:
                self.db = None
            return False

    async def load_config(self, guild_id: int = None):
        """Carrega configuração de forma assíncrona com tratamento melhorado"""
        try:
            if os.path.exists(CONFIG_FILE):
                try:
                    with open(CONFIG_FILE, 'r') as f:
                        file_config = json.load(f)
                        self._update_config(file_config)
                        logger.info("Configuração carregada do arquivo local")
                        logger.debug(f"Configuração carregada: {self.config}")
                except json.JSONDecodeError:
                    logger.error("Arquivo de configuração corrompido, usando padrão")
                    self._update_config(DEFAULT_CONFIG)
                except Exception as e:
                    logger.error(f"Erro ao carregar configuração do arquivo: {e}")
                    self._update_config(DEFAULT_CONFIG)
                
            if hasattr(self, 'db') and self.db and hasattr(self.db, 'load_config'):
                try:
                    if guild_id is not None:
                        try:
                            db_config = await self.db.load_config(guild_id)
                            if db_config:
                                self._update_config(db_config)
                                logger.info(f"Configuração carregada do banco para guild {guild_id}")
                                return True
                        except Exception as e:
                            logger.warning(f"Erro ao carregar configuração para guild {guild_id}: {e}")
                    
                    for guild in self.guilds:
                        try:
                            db_config = await self.db.load_config(guild.id)
                            if db_config:
                                self._update_config(db_config)
                                logger.info(f"Configuração carregada do banco para guild {guild.id}")
                                return True
                        except Exception as e:
                            logger.warning(f"Erro ao carregar configuração para guild {guild.id}: {e}")
                            continue
                except Exception as db_error:
                    logger.error(f"Erro ao carregar do banco: {db_error}")
            
            if not hasattr(self, 'config') or not self.config:
                self._update_config(DEFAULT_CONFIG)
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(DEFAULT_CONFIG, f, indent=4)
                logger.info("Configuração padrão criada")
                
            return True
            
        except Exception as e:
            logger.error(f"Erro crítico ao carregar configurações: {e}")
            self._update_config(DEFAULT_CONFIG)
            return False

    def _update_config(self, new_config: dict):
        """Atualiza a configuração garantindo que todas as chaves necessárias existam"""
        for key, value in DEFAULT_CONFIG.items():
            if key not in new_config:
                new_config[key] = value
        
        self.timezone = pytz.timezone(new_config.get('timezone', 'America/Sao_Paulo'))
        
        self.config = new_config
        logger.info("Configuração atualizada com sucesso")

    async def save_config(self, guild_id: int = None):
        """Salva configuração com cache (modificado)"""
        if not hasattr(self, 'config') or not self.config:
            return
            
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            if hasattr(self, 'db') and self.db and self.db._is_initialized:
                guilds_to_save = [guild_id] if guild_id is not None else [guild.id for guild in self.guilds]
                
                for gid in guilds_to_save:
                    await self.db.save_config(gid, self.config)
                    logger.info(f"Configuração salva no banco para guild {gid}")
                
                if hasattr(self.db, 'sync_task_periods'):
                    monitoring_period = self.config.get('monitoring_period')
                    if monitoring_period:
                        await self.db.sync_task_periods(monitoring_period)
            
            self._last_config_save = datetime.now(pytz.UTC)
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")

    async def setup_hook(self):
        """Configurações assíncronas antes do bot ficar pronto"""
        if self._setup_complete:
            return
        
        await self.load_config()
        await self.initialize_db()
        
        if self.db and not self.db_connection_failed:
            try:
                synced = await self.tree.sync()
                logger.info(f"Comandos slash sincronizados: {len(synced)} comandos.")
            except Exception as e:
                logger.error(f"Erro ao sincronizar comandos slash: {e}")

            self.voice_event_processor_task = self.loop.create_task(self.process_voice_events(), name='voice_event_processor')

            self._setup_complete = True
            logger.info("Setup hook concluído.")
        else:
            logger.critical("Falha na inicialização do banco de dados. As tarefas não serão iniciadas.")
            self.db_connection_failed = True
    
    async def send_with_fallback(self, destination, content=None, embed=None, file=None):
        """Envia mensagens com tratamento de erros e fallback para rate limits."""
        max_retries = 3
        base_delay = 2.0
        
        for attempt in range(max_retries):
            try:
                if file:
                    if isinstance(file, BytesIO):
                        file.seek(0)
                        file = discord.File(file, filename='activity_report.png')
                    await destination.send(content=content, embed=embed, file=file)
                elif embed:
                    await destination.send(embed=embed)
                elif content:
                    await destination.send(content)
                return
                
            except discord.HTTPException as e:
                if e.status == 429:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limit atingido (tentativa {attempt + 1}/{max_retries}). Tentando novamente em {delay:.2f} segundos")
                    
                    self.rate_limit_monitor.adaptive_delay = min(
                        self.rate_limit_monitor.max_delay,
                        self.rate_limit_monitor.adaptive_delay * 1.5
                    )
                    
                    await asyncio.sleep(delay)
                    continue
                    
                logger.error(f"Erro HTTP ao enviar mensagem para {destination}: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Erro inesperado ao enviar mensagem para {destination}: {e}")
                if attempt == max_retries - 1:
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
            f"**Detalhes:**\n```python\n{tb_details[:1800]}\n```"
        )
        await self.log_action("Erro Crítico de Evento", details=log_message)

    async def check_audio_states(self):
        await self.wait_until_ready()
        while True:
            try:
                for guild in self.guilds:
                    for voice_channel in guild.voice_channels:
                        for member in voice_channel.members:
                            if member.bot:
                                continue
                                
                            audio_key = (member.id, guild.id)
                            current_audio_state = member.voice.self_deaf or member.voice.deaf
                            
                            if audio_key not in self.active_sessions:
                                self.active_sessions[audio_key] = {
                                    'start_time': datetime.now(pytz.UTC),
                                    'last_audio_time': datetime.now(pytz.UTC),
                                    'audio_disabled': current_audio_state,
                                    'total_audio_off_time': 0,
                                    'estimated': False
                                }
                                continue
                                
                            if current_audio_state and not self.active_sessions[audio_key]['audio_disabled']:
                                self.active_sessions[audio_key]['audio_disabled'] = True
                                self.active_sessions[audio_key]['audio_off_time'] = datetime.now(pytz.UTC)
                                
                            elif not current_audio_state and self.active_sessions[audio_key]['audio_disabled']:
                                self.active_sessions[audio_key]['audio_disabled'] = False
                                if 'audio_off_time' in self.active_sessions[audio_key]:
                                    audio_off_duration = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                                    self.active_sessions[audio_key]['total_audio_off_time'] += audio_off_duration
                                    del self.active_sessions[audio_key]['audio_off_time']
            
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Erro ao verificar estados de áudio: {e}")
                await asyncio.sleep(60)

    async def monitor_db_pool(self):
        await self.wait_until_ready()
        while True:
            try:
                if hasattr(self, 'db') and self.db:
                    try:
                        pool_status = await self.db.check_pool_status()
                        if pool_status:
                            logger.debug(f"Status do pool de conexões: {pool_status}")
                            
                            if pool_status['freesize'] == 0 and pool_status['used'] >= pool_status['maxsize'] - 2:
                                logger.warning("Pool de conexões sobrecarregado - aumentando tamanho")
                                await self.db.pool.set_max_size(min(100, pool_status['maxsize'] + 10))
                                
                    except Exception as e:
                        logger.error(f"Health check falhou para o banco de dados: {e}")
                        await self.log_action(
                            "Erro de Saúde",
                            None,
                            f"Falha na conexão com o banco de dados: {str(e)}"
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
                
                if hasattr(self, 'db') and self.db:
                    try:
                        pool_status = await self.db.check_pool_status()
                        if pool_status:
                            logger.debug(f"Status do pool de conexões: {pool_status}")
                    except Exception as e:
                        logger.error(f"Health check falhou para o banco de dados: {e}")
                        await self.log_action(
                            "Erro de Saúde",
                            None,
                            f"Falha na conexão com o banco de dados: {str(e)}"
                        )
                
                if (self._last_config_save is None or 
                    (datetime.now(pytz.UTC) - self._last_config_save).total_seconds() > self._config_save_interval):
                    await self.save_config()
                
                await asyncio.sleep(self._health_check_interval)
            except Exception as e:
                logger.error(f"Erro no health check: {e}")
                await asyncio.sleep(60)

    # CORREÇÃO: O método process_queues antigo foi substituído pela versão injetada, que é mais inteligente.
    async def process_queues(self):
        """Processa as filas de mensagens e eventos com controle de rate limit inteligente."""
        await self.wait_until_ready()
        while True:
            try:
                # 1. Verificar se precisamos de um delay
                delay = self.rate_limit_monitor.get_delay()
                if delay > 0:
                    logger.info(f"Rate limit proativo: aguardando {delay:.2f} segundos.")
                    await asyncio.sleep(delay)
                    continue

                # 2. Processar a fila de eventos de voz (se houver)
                if not self.voice_event_queue.empty():
                    batch = []
                    # Pega um lote de eventos da fila
                    for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                        batch.append(await self.voice_event_queue.get())
                    
                    await self._process_voice_batch(batch)
                    
                    for _ in batch:
                        self.voice_event_queue.task_done()
                    # Continue para o próximo ciclo para reavaliar o delay
                    continue

                # 3. Processar a fila de mensagens (se a de voz estiver vazia)
                item, priority = await self.message_queue.get_next_message()
                if item is None:
                    await asyncio.sleep(0.5)  # Fila vazia, aguarda um pouco
                    continue
                    
                try:
                    if isinstance(item, tuple):
                        destination, content, embed, file = (None, None, None, None)
                        try:
                            if len(item) == 4:
                                destination, content, embed, file = item
                            elif len(item) == 3:
                                destination, content, embed = item
                                file = None
                            elif len(item) == 2:
                                destination, embed = item
                                content, file = None, None
                        except Exception:
                            try:
                                destination, content = item
                            except Exception:
                                logger.warning(f"Formato inesperado de item da fila: {item}")
                        
                        if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                            await self.send_with_fallback(destination, content, embed, file)
                        else:
                            logger.warning(f"Destino inválido para mensagem: {type(destination)}")
                    else:
                        logger.warning(f"Item da fila em formato desconhecido: {item}")

                except discord.HTTPException as e:
                    status = getattr(e, 'status', None)
                    if status == 429:
                        retry_after = getattr(e, 'retry_after', None)
                        logger.warning(f"Atingido rate limit (429). Headers: {getattr(e, 'response', None)}")
                        if retry_after:
                            self.rate_limit_monitor.cooldown_until = time.time() + retry_after
                            self.rate_limit_monitor.adaptive_delay = min(self.rate_limit_monitor.max_delay, self.rate_limit_monitor.adaptive_delay * 2)
                    elif "Cloudflare" in str(e) or "1015" in str(getattr(e, 'text', '')):
                        self.rate_limit_monitor.handle_cloudflare_block()
                except Exception as e:
                    logger.error(f"Erro ao processar item da fila: {e}", exc_info=True)
                finally:    
                    try:
                        self.message_queue.task_done(priority)
                    except Exception:
                        try:
                            self.message_queue.task_done()
                        except Exception:
                            pass
                    
            except Exception as e:
                logger.error(f"Erro crítico no processador de filas: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _process_voice_batch(self, batch):
        processed = {}
        
        for event in batch:
            try:
                event_type, member, before, after = event[:4]
                
                key = (member.id, member.guild.id)
                
                if key not in processed:
                    processed[key] = {
                        'member': member,
                        'events': []
                    }
                processed[key]['events'].append((before, after))
            except Exception as e:
                logger.error(f"Erro ao processar evento de voz: {e}", exc_info=True)
                continue
        
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
                if member.bot:
                    continue

                if member.id in self.config.get('whitelist', {}).get('users', []) or \
                   any(role.id in self.config.get('whitelist', {}).get('roles', []) for role in member.roles):
                    continue
                
                audio_key = (member.id, member.guild.id)
                
                if audio_key in self.active_sessions and self.active_sessions[audio_key].get('estimated'):
                    if before.channel is not None and after.channel is None:
                        estimated_start = self.active_sessions[audio_key]['start_time']
                        actual_start = max(estimated_start, datetime.now(pytz.UTC) - timedelta(hours=1))
                        self.active_sessions[audio_key]['start_time'] = actual_start
                        self.active_sessions[audio_key]['estimated'] = False
                
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
                'start_time': datetime.now(pytz.UTC),
                'last_audio_time': datetime.now(pytz.UTC),
                'audio_disabled': after.self_deaf or after.deaf,
                'total_audio_off_time': 0,
                'estimated': False
            }
            
            embed = discord.Embed(
                title="🎤 Entrou em Voz",
                color=discord.Color.green(),
                timestamp=datetime.now(pytz.UTC))
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
        if not session_data:
            return

        try:
            now = datetime.now(pytz.UTC)
            total_time = (now - session_data['start_time']).total_seconds()
            audio_off_time = session_data.get('total_audio_off_time', 0)
            
            if 'audio_off_time' in session_data:
                audio_off_duration = (now - session_data['audio_off_time']).total_seconds()
                audio_off_time += audio_off_duration
            
            effective_time = max(0, total_time - audio_off_time)
            
            try:
                await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
            except Exception as e:
                logger.error(f"Erro ao registrar saída de voz: {e}")
                await self.log_action("Erro DB - Saída de voz", member, str(e))
            
            channel_name = before.channel.name if before.channel else "Canal desconhecido"
            embed = discord.Embed(
                title="🚪 Saiu de Voz",
                color=discord.Color.blue(),
                timestamp=now)
            embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
            embed.add_field(name="Usuário", value=member.mention, inline=True)
            embed.add_field(name="Canal", value=channel_name, inline=True)
            embed.add_field(name="Tempo Efetivo", 
                          value=f"{int(effective_time//60)} minutos {int(effective_time%60)} segundos", 
                          inline=True)
            embed.add_field(name="Tempo sem Áudio", 
                          value=f"{int(audio_off_time//60)} minutos {int(audio_off_time%60)} segundos", 
                          inline=True)
            embed.set_footer(text=f"ID: {member.id}")
            
            await self.log_action(None, None, embed=embed)
            
        except Exception as e:
            logger.error(f"Erro ao processar saída de voz: {e}")
        finally:
            self.active_sessions.pop((member.id, member.guild.id), None)

    async def _handle_voice_move(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState, absence_channel_id: int):
        audio_key = (member.id, member.guild.id)
        
        if (before.channel is not None and 
            before.channel.id != absence_channel_id and 
            after.channel is not None and 
            after.channel.id == absence_channel_id):
            
            if audio_key in self.active_sessions:
                current_duration = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['start_time']).total_seconds()
                
                self.active_sessions[audio_key].update({
                    'paused': True,
                    'paused_time': datetime.now(pytz.UTC),
                    'pre_pause_duration': current_duration,
                    'paused_channel_id': before.channel.id
                })
                
                embed = discord.Embed(
                    title="⏸ Sessão Pausada (Ausência)",
                    color=discord.Color.light_grey(),
                    timestamp=datetime.now(pytz.UTC))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="Usuário", value=member.mention, inline=True)
                embed.add_field(name="De", value=before.channel.name, inline=True)
                embed.add_field(name="Para", value=after.channel.name, inline=True)
                embed.add_field(name="Tempo Ativo", 
                              value=f"{int(current_duration//60)} minutos {int(current_duration%60)} segundos", 
                              inline=False)
                embed.set_footer(text=f"ID: {member.id}")
                
                await self.log_action(None, None, embed=embed)
        
        elif (before.channel is not None and 
              after.channel is None):
            
            if before.channel.id == absence_channel_id and audio_key in self.active_sessions:
                if self.active_sessions[audio_key].get('paused'):
                    original_channel_id = self.active_sessions[audio_key].get('paused_channel_id')
                    original_channel = member.guild.get_channel(original_channel_id) if original_channel_id else None
                    
                    if original_channel:
                        before_state_data = {
                            'channel_id': original_channel.id, 'self_deaf': before.self_deaf, 'deaf': before.deaf,
                            'self_mute': before.self_mute, 'mute': before.mute, 'self_stream': False,
                            'self_video': False, 'suppress': False, 'requested_to_speak_at': None
                        }
                        
                        before_state = discord.VoiceState(data=before_state_data, channel=original_channel)
                        
                        await self._handle_voice_leave(member, before_state)
                    else:
                        await self._handle_voice_leave(member, before)
                    
                    for key in ['paused', 'paused_time', 'pre_pause_duration', 'paused_channel_id']:
                        self.active_sessions[audio_key].pop(key, None)
                else:
                    await self._handle_voice_leave(member, before)
            else:
                await self._handle_voice_leave(member, before)
        
        elif (before.channel is not None and 
              before.channel.id == absence_channel_id and 
              after.channel is not None and 
              after.channel.id != absence_channel_id):
            
            if audio_key in self.active_sessions and self.active_sessions[audio_key].get('paused'):
                pause_duration = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['paused_time']).total_seconds()
                
                self.active_sessions[audio_key]['start_time'] = datetime.now(pytz.UTC) - timedelta(
                    seconds=self.active_sessions[audio_key]['pre_pause_duration'])
                
                for key in ['paused', 'paused_time', 'pre_pause_duration', 'paused_channel_id']:
                    self.active_sessions[audio_key].pop(key, None)
                
                embed = discord.Embed(
                    title="▶️ Sessão Retomada (Voltou)",
                    color=discord.Color.green(),
                    timestamp=datetime.now(pytz.UTC))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="Usuário", value=member.mention, inline=True)
                embed.add_field(name="De", value=before.channel.name, inline=True)
                embed.add_field(name="Para", value=after.channel.name, inline=True)
                embed.add_field(name="Tempo Pausado", 
                              value=f"{int(pause_duration//60)} minutos {int(pause_duration%60)} segundos", 
                              inline=False)
                embed.set_footer(text=f"ID: {member.id}")
                
                await self.log_action(None, None, embed=embed)
        
        elif (before.channel is not None and 
              after.channel is not None and 
              before.channel != after.channel and
              before.channel.id != absence_channel_id and 
              after.channel.id != absence_channel_id):
            
            if audio_key in self.active_sessions:
                embed = discord.Embed(
                    title="🔄 Movido entre Canais",
                    color=discord.Color.light_grey(),
                    timestamp=datetime.now(pytz.UTC))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="De", value=before.channel.name, inline=True)
                embed.add_field(name="Para", value=after.channel.name, inline=True)
                embed.set_footer(text=f"ID: {member.id}")
                await self.log_action(None, None, embed=embed)
        
        elif (before.channel is not None and 
              after.channel is not None and 
              before.channel == after.channel):
            
            await self._handle_audio_change(member, before, after)

    async def _handle_audio_change(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        audio_key = (member.id, member.guild.id)
        
        if audio_key not in self.active_sessions and after.channel is not None:
            await self._handle_voice_join(member, after)
            return

        if audio_key not in self.active_sessions:
            return

        audio_was_off = before.self_deaf or before.deaf
        audio_is_off = after.self_deaf or after.deaf

        if not audio_was_off and audio_is_off:
            self.active_sessions[audio_key]['audio_disabled'] = True
            self.active_sessions[audio_key]['audio_off_time'] = datetime.now(pytz.UTC)
            
            time_in_voice = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['start_time']).total_seconds()
            
            embed = discord.Embed(
                title="🔇 Áudio Desativado",
                color=discord.Color.orange(),
                timestamp=datetime.now(pytz.UTC))
            embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
            embed.add_field(name="Usuário", value=member.mention, inline=True)
            embed.add_field(name="Canal", value=after.channel.name if after.channel else "Desconhecido", inline=True)
            embed.add_field(name="Tempo em voz", 
                          value=f"{int(time_in_voice//60)} minutos {int(time_in_voice%60)} segundos", 
                          inline=False)
            embed.set_footer(text=f"ID: {member.id}")
            
            await self.log_action(None, None, embed=embed)
        
        elif audio_was_off and not audio_is_off:
            self.active_sessions[audio_key]['audio_disabled'] = False
            if 'audio_off_time' in self.active_sessions[audio_key]:
                audio_off_duration = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['audio_off_time']).total_seconds()
                self.active_sessions[audio_key]['total_audio_off_time'] = \
                    self.active_sessions[audio_key].get('total_audio_off_time', 0) + audio_off_duration
                del self.active_sessions[audio_key]['audio_off_time']
                
                total_time = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['start_time']).total_seconds()
                
                embed = discord.Embed(
                    title="🔊 Áudio Reativado",
                    color=discord.Color.green(),
                    timestamp=datetime.now(pytz.UTC))
                embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                embed.add_field(name="Usuário", value=member.mention, inline=True)
                embed.add_field(name="Canal", value=after.channel.name if after.channel else "Desconhecido", inline=True)
                embed.add_field(name="Tempo sem áudio", 
                              value=f"{int(audio_off_duration//60)} minutos {int(audio_off_duration%60)} segundos", 
                              inline=True)
                embed.add_field(name="Tempo total em voz", 
                              value=f"{int(total_time//60)} minutos {int(total_time%60)} segundos", 
                              inline=True)
                embed.set_footer(text=f"ID: {member.id}")
                
                await self.log_action(None, None, embed=embed)

    async def process_voice_events(self):
        """Processa eventos de voz da fila"""
        await self.wait_until_ready()
        while True:
            try:
                event = await self.voice_event_queue.get()
                
                if len(event) < 6:
                    self.voice_event_queue.task_done()
                    continue
                    
                _, member, before, after, event_id, event_time = event
                
                if (datetime.now(pytz.UTC) - event_time) > timedelta(minutes=5):
                    logger.debug(f"Ignorando evento antigo: {event_id}")
                    self.voice_event_queue.task_done()
                    continue
                    
                await self._process_voice_batch([(_, member, before, after)])
                self.voice_event_queue.task_done()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)

    async def log_action(self, action: str, member: Optional[discord.Member] = None, 
                       details: str = None, file: discord.File = None, 
                       embed: discord.Embed = None):
        """Registra uma ação no canal de logs"""
        try:
            if not hasattr(self, 'config') or not self.config.get('log_channel'):
                if action:
                    logger.info(f"Ação não logada (canal não configurado): {action}")
                return
                
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
                color = discord.Color.blue()
                icon = "ℹ️"
                    
                embed = discord.Embed(
                    title=f"{icon} {action}",
                    color=color,
                    timestamp=datetime.now(pytz.UTC))
                
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
                    timestamp=datetime.now(pytz.UTC))
                priority = "high"
            else:
                embed = discord.Embed(
                    title="ℹ️ Notificação",
                    description=message,
                    color=discord.Color.blue(),
                    timestamp=datetime.now(pytz.UTC))
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

    async def notify_admins_dm(self, guild: discord.Guild, embed: discord.Embed):
        """
        Envia uma DM para membros com permissão de administrador E para membros
        com cargos configurados em 'notification_roles_dm'.
        """
        if not guild:
            logger.warning("notify_admins_dm chamada sem guilda.")
            return

        members_to_notify = set()

        for member in guild.members:
            if not member.bot and member.guild_permissions.administrator:
                members_to_notify.add(member)

        notification_role_ids = self.config.get('notification_roles_dm', [])
        if notification_role_ids:
            for role_id in notification_role_ids:
                role = guild.get_role(role_id)
                if role:
                    for member in role.members:
                        if not member.bot:
                            members_to_notify.add(member)

        if not members_to_notify:
            logger.info(f"Nenhum administrador ou cargo de notificação configurado na guilda {guild.name} para notificar via DM.")
            return

        logger.info(f"Notificando {len(members_to_notify)} membros privilegiados em {guild.name} via DM.")

        for member in members_to_notify:
            try:
                await self.send_dm(member, message_content=None, embed=embed)
                logger.debug(f"DM de notificação de administrador/cargo enfileirada para {member.display_name} ({member.id}).")
            except Exception as e:
                logger.error(f"Falha ao enfileirar DM de notificação para {member.display_name} na guilda {guild.name}: {e}")

    async def send_dm(self, member: discord.Member, message_content: str, embed: discord.Embed):
        try:
            priority = 'high' if embed and (embed.title.startswith("🚨") or embed.title.startswith("👢")) else 'low'
            await self.message_queue.put((
                member,
                message_content,
                embed,
                None
            ), priority=priority)
        except discord.Forbidden:
            logger.warning(f"Não foi possível enviar DM para {member.display_name}. (DMs desabilitadas)")
            await self.log_action(
                "Falha ao Enviar DM", 
                member, 
                "O usuário provavelmente desabilitou DMs de membros do servidor."
            )
        except discord.HTTPException as e:
            if e.code == 50007:
                logger.warning(f"Não foi possível enviar DM para {member.display_name}. (DMs desabilitadas)")
            else:
                logger.error(f"Erro ao enviar DM para {member}: {e}")
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

            last_check = None
            try:
                if hasattr(self, 'db') and self.db:
                    last_check = await self.db.get_last_period_check(member.id, member.guild.id)
            except Exception:
                last_check = None
            
            format_args = {
                'days': warnings_config.get('first_warning', 'N/A'),
                'monitoring_period': self.config.get('monitoring_period', 'N/A'),
                'required_minutes': self.config.get('required_minutes', 'N/A'),
                'required_days': self.config.get('required_days', 'N/A'),
                'guild': member.guild.name,
                'days_remaining': 'N/A'
            }
            
            if last_check:
                period_end = None
                if isinstance(last_check, dict):
                    period_end = last_check.get('period_end')
                else:
                    period_end = getattr(last_check, 'period_end', None)
                
                if period_end:
                    now = datetime.now(pytz.UTC)
                    
                    if getattr(period_end, 'tzinfo', None) is None:
                        try:
                            period_end = period_end.replace(tzinfo=pytz.UTC)
                        except Exception:
                            pass
                    
                    try:
                        days_remaining = max(0, (period_end - now).days)
                    except Exception:
                        days_remaining = 0
                    
                    if days_remaining == 0:
                        format_args['days_remaining'] = "HOJE"
                    elif days_remaining == 1:
                        format_args['days_remaining'] = "AMANHÃ"
                    else:
                        format_args['days_remaining'] = f"{days_remaining} dias"
            
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
                timestamp=datetime.now(pytz.UTC))
            
            if member.guild.icon:
                embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url)
            
            await self.send_dm(member, message, embed)
            
            try:
                if hasattr(self, 'db') and self.db:
                    await self.db.log_warning(member.id, member.guild.id, warning_type)
            except Exception as e:
                logger.error(f"Erro ao registrar aviso no DB para {member}: {e}")
            
            await self.log_action(f"Aviso Enviado ({warning_type})", member)
            
            if warning_type in ['first', 'second']:
                admin_embed = discord.Embed(
                    title=f"🔔 Relatório de Aviso: {warning_type.capitalize()}",
                    description=f"Um aviso de inatividade foi enviado para {member.mention}.",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(pytz.UTC)
                )
                admin_embed.set_author(name=f"{member.display_name}", icon_url=member.display_avatar.url)
                admin_embed.add_field(name="Usuário", value=f"{member.mention} (`{member.id}`)", inline=False)
                admin_embed.add_field(name="Tipo de Aviso", value=warning_type.capitalize(), inline=True)
                admin_embed.set_footer(text=f"Servidor: {member.guild.name}")
                
                await self.notify_admins_dm(member.guild, embed=admin_embed)

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
async def on_member_update(before: discord.Member, after: discord.Member):
    if before.roles == after.roles:
        return

    if not hasattr(bot, 'config') or not bot.config.get('tracked_roles'):
        return

    tracked_roles = set(bot.config['tracked_roles'])

    added_roles = [role for role in after.roles if role not in before.roles and role.id in tracked_roles]

    if added_roles:
        if hasattr(bot, 'db') and bot.db:
            await bot.db.reset_user_tracking(after.id, after.guild.id)
            logger.info(f"Acompanhamento de inatividade resetado para {after.display_name} após receber um cargo monitorado.")

        try:
            for role in added_roles:
                try:
                    last_removal = None
                    if hasattr(bot, 'db') and bot.db:
                        last_removal = await bot.db.get_last_specific_role_removal(after.id, after.guild.id, role.id)
                except Exception as e:
                    logger.error(f"Erro ao obter histórico de remoções do DB: {e}")
                    last_removal = None

                if last_removal and last_removal.get('removal_date'):
                    removal_date = last_removal['removal_date']
                    try:
                        if isinstance(removal_date, str):
                            from dateutil import parser
                            removal_date = parser.parse(removal_date)
                    except Exception:
                        pass

                    if removal_date.tzinfo is None:
                        removal_date = removal_date.replace(tzinfo=pytz.UTC)

                    time_since_removal = datetime.now(pytz.UTC) - removal_date

                    if time_since_removal <= timedelta(days=30):
                        await send_forgiveness_message(after, [role])
                        break

            for role in added_roles:
                try:
                    if hasattr(bot, 'db') and bot.db:
                        await bot.db.log_role_assignment(after.id, after.guild.id, role.id)
                    logger.info(f"Registrada atribuição de cargo {role.name} para {after.display_name}")
                except Exception as e:
                    logger.error(f"Erro ao registrar atribuição de cargo: {e}")
        except Exception as e:
            logger.error(f"Erro ao processar atualização de membro: {e}")

async def send_forgiveness_message(member: discord.Member, roles: List[discord.Role]):
    """Envia UMA mensagem de perdão quando um ou mais cargos são devolvidos."""
    if not roles:
        return

    try:
        role_for_db_check = roles[0]
        last_message = None
        try:
            if hasattr(bot, 'db') and bot.db:
                last_message = await bot.db.get_last_forgiveness_message(member.id, member.guild.id, role_for_db_check.id)
        except Exception as e:
            logger.error(f"Erro ao obter último registro de mensagem de perdão do DB: {e}")

        if last_message:
            try:
                lm = last_message
                if isinstance(lm, str):
                    from dateutil import parser
                    lm = parser.parse(lm)
                if lm.tzinfo is None:
                    lm = lm.replace(tzinfo=pytz.UTC)
                
                if (datetime.now(pytz.UTC) - lm) < timedelta(days=7):
                    logger.info(f"Mensagem de perdão já enviada recentemente para {member.display_name}.")
                    return
            except Exception:
                pass

        required_minutes = bot.config.get('required_minutes', 15)
        required_days = bot.config.get('required_days', 2)
        monitoring_period = bot.config.get('monitoring_period', 14)

        roles_text = "\n".join(f"• **{role.name}**" for role in roles)
        title = "🎉 Cargo Devolvido!" if len(roles) == 1 else "🎉 Cargos Devolvidos!"
        
        message = (
            f"🎉 **Seu(s) cargo(s) foi(ram) devolvido(s)!** 🎉\n\n"
            f"O(s) seguinte(s) cargo(s) foi(ram) devolvido(s) e você foi perdoado pelo seu período de inatividade:\n{roles_text}\n\n"
            f"Para mantê-lo(s), volte a ficar ativo nos canais de voz do servidor **{member.guild.name}**.\n\n"
            f"**Requisitos Atuais:**\n"
            f"• Período de Análise: **{monitoring_period} dias**\n"
            f"• Tempo Mínimo por Dia: **{required_minutes} minutos**\n"
            f"• Total de Dias Ativos: Pelo menos **{required_days} dias diferentes** dentro do período."
        )

        embed = discord.Embed(
            title=title,
            description=message,
            color=discord.Color.green(),
            timestamp=datetime.now(pytz.UTC)
        )
        embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url if member.guild.icon else None)
        embed.set_footer(text="Sistema de Controle de Atividades")

        await bot.send_dm(member, None, embed)
        logger.info(f"Mensagem de perdão enviada para {member.display_name} pelos cargos: {', '.join(r.name for r in roles)}")

        try:
            if hasattr(bot, 'db') and bot.db:
                await bot.db.log_forgiveness_message(member.id, member.guild.id, role_for_db_check.id)
        except Exception as e:
            logger.error(f"Erro ao registrar mensagem de perdão no DB: {e}")

        await bot.log_action(
            "Mensagem de Perdão Enviada",
            member,
            f"Cargo(s) devolvido(s): {', '.join(r.name for r in roles)}"
        )

    except Exception as e:
        logger.error(f"Erro ao enviar mensagem de perdão para {member}: {e}")

@bot.event
async def on_ready():
    try:
        if hasattr(bot, '_ready_called') and bot._ready_called:
            logger.info("on_ready chamado novamente, mas já foi processado. Ignorando.")
            return
        bot._ready_called = True

        logger.info(f'Bot conectado como {bot.user}')
        logger.info(f"Latência: {round(bot.latency * 1000)}ms")

        try:
            if not hasattr(bot, 'db') or not bot.db or not getattr(bot.db, '_is_initialized', False):
                logger.error("Banco de dados não foi inicializado corretamente. Tentando novamente...")
                if not await bot.initialize_db():
                    logger.critical("Falha crítica na inicialização do banco de dados no on_ready.")
                    return

            logger.info("Carregando e validando configurações...")
            await bot.load_config()
            await bot.save_config()
            
            if hasattr(bot, 'db') and getattr(bot.db, 'sync_task_periods', None):
                monitoring_period = bot.config.get('monitoring_period')
                if monitoring_period:
                    await bot.db.sync_task_periods(monitoring_period)

        except Exception as critical_error:
            logger.critical(f"Erro crítico na inicialização: {critical_error}", exc_info=True)
            return

        logger.info("Inicialização crítica concluída.")

        if not bot._tasks_started:
            logger.info("Iniciando tarefas de fundo...")
            
            from tasks import (
                inactivity_check, cleanup_members,
                database_backup, cleanup_old_data, monitor_rate_limits,
                report_metrics, health_check,
                process_pending_voice_events,
                check_current_voice_members, detect_missing_voice_leaves,
                cleanup_ghost_sessions_wrapper, register_role_assignments_wrapper,
                cleanup_old_bot_messages
            )
            
            async def start_task_with_jitter(task_coro, name):
                delay = random.uniform(1, 30)
                logger.info(f"Agendando a task '{name}' para iniciar em {delay:.2f} segundos.")
                await asyncio.sleep(delay)
                bot.loop.create_task(task_coro, name=name)

            await start_task_with_jitter(register_role_assignments_wrapper(), 'register_role_assignments_wrapper')
            await start_task_with_jitter(inactivity_check(), 'inactivity_check_wrapper')
            await start_task_with_jitter(cleanup_members(), 'cleanup_members_wrapper')
            await start_task_with_jitter(database_backup(), 'database_backup_wrapper')
            await start_task_with_jitter(cleanup_old_data(), 'cleanup_old_data_wrapper')
            await start_task_with_jitter(monitor_rate_limits(), 'monitor_rate_limits_wrapper')
            await start_task_with_jitter(report_metrics(), 'report_metrics_wrapper')
            await start_task_with_jitter(health_check(), 'health_check_wrapper')
            
            bot.loop.create_task(process_pending_voice_events(), name='process_pending_voice_events')
            bot.loop.create_task(check_current_voice_members(), name='check_current_voice_members')
            bot.loop.create_task(detect_missing_voice_leaves(), name='detect_missing_voice_leaves')
            bot.loop.create_task(cleanup_ghost_sessions_wrapper(), name='cleanup_ghost_sessions_wrapper')
            
            bot.loop.create_task(cleanup_old_bot_messages(), name='cleanup_old_bot_messages_task')

            bot.queue_processor_task = bot.loop.create_task(bot.process_queues(), name='queue_processor')
            bot.pool_monitor_task = bot.loop.create_task(bot.monitor_db_pool(), name='db_pool_monitor')
            bot.health_check_task = bot.loop.create_task(bot.periodic_health_check(), name='periodic_health_check')
            bot.audio_check_task = bot.loop.create_task(bot.check_audio_states(), name='audio_state_checker')
            bot.voice_event_processor_task = bot.loop.create_task(bot.process_voice_events(), name='voice_event_processor')

            bot._tasks_started = True
            logger.info("Todas as tarefas de fundo foram agendadas com sucesso.")

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
        logger.critical(f"Erro crítico irrecuperável no on_ready: {e}", exc_info=True)
        await bot.close()

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    try:
        event_id = bot.generate_event_id()
        event_time = datetime.now(pytz.UTC)
        
        if bot.last_reconnect_time and (event_time - bot.last_reconnect_time) < timedelta(seconds=10):
            logger.debug(f"Ignorando evento pós-reconexão: {event_id}")
            return
            
        await bot.voice_event_queue.put((
            'voice_state_update',
            member,
            before,
            after,
            event_id,
            event_time
        ))
    except Exception as e:
        logger.error(f"Erro ao enfileirar evento de voz: {e}")

# --- CÓDIGO DO SERVIDOR WEB PARA UPTIMEROBOT ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot de Controle de Atividade está online."

def run():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def start_web_server():
    t = Thread(target=run)
    t.daemon = True
    t.start()

# Importar comandos
from bot_commands import *

async def main():
    load_dotenv()
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    
    logger.info("Aguardando 15 segundos antes da primeira tentativa de conexão para estabilização do ambiente...")
    await asyncio.sleep(15)
    
    try:
        if not hasattr(bot, 'initialize_db'):
            raise AttributeError("Método initialize_db não encontrado na classe InactivityBot")
            
        db_initialized = await bot.initialize_db()
        if not db_initialized:
            logger.critical("Falha ao inicializar o banco de dados - encerrando")
            return
            
    except Exception as e:
        logger.critical(f"Falha crítica ao inicializar o banco de dados: {e}")
        return

    start_web_server()
        
    async with bot:
        try:
            await bot.start(DISCORD_TOKEN)
        except Exception as e:
            logger.critical(f"Erro ao iniciar o bot: {e}")

# CORREÇÃO: A lógica injetada foi integrada diretamente nas classes/métodos acima.
# Apenas a função de monkey-patching para o listener HTTP é necessária aqui.
async def on_http_request_completed(self, route, response):
    """Evento chamado após cada requisição HTTP que o bot faz."""
    try:
        headers = {k: v for k, v in response.headers.items()}
        self.rate_limit_monitor.update_from_headers(headers)
    except Exception as e:
        logger.error(f"Erro ao processar headers de rate limit: {e}")

# Injeta o método na classe InactivityBot
InactivityBot.on_http_request_completed = on_http_request_completed


if __name__ == '__main__':
    asyncio.run(main())