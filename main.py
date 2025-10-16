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

# --- INÍCIO DA MODIFICAÇÃO: Exceção customizada para Rate Limits ---
# Criamos uma exceção específica para facilitar o tratamento de erros de rate limit.
class RateLimitException(Exception):
    def __init__(self, message, retry_after: float):
        super().__init__(message)
        self.retry_after = retry_after
# --- FIM DA MODIFICAÇÃO ---

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
        self.max_delay = 30.0  # Aumentado para 30 segundos
        self.cooldown_until = 0
        self.cloudflare_blocked_until = 0
    
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
    
    def handle_cloudflare_block(self):
        """Lida com bloqueio do Cloudflare (Error 1015)"""
        now = time.time()
        self.cloudflare_blocked_until = now + 60  # Bloqueio por 1 minuto
        self.adaptive_delay = min(self.max_delay, self.adaptive_delay * 2)
        logger.warning("Bloqueio do Cloudflare detectado. Entrando em modo de resfriamento por 60 segundos.")
    
    def should_delay(self):
        now = time.time()
        
        # Verificar se estamos bloqueados pelo Cloudflare
        if now < self.cloudflare_blocked_until:
            return True
            
        if now < self.cooldown_until:
            return True
        
        # Lógica proativa: se restarem poucas requisições, espere um pouco
        if self.global_limits['remaining'] < 5:
            # Calcula um pequeno delay para distribuir as requisições restantes
            time_to_reset = self.global_limits['reset_at'] - now
            if time_to_reset > 0:
                # Espera uma fração do tempo restante
                proactive_delay = time_to_reset / (self.global_limits['remaining'] + 1)
                logger.info(f"Rate limit global baixo ({self.global_limits['remaining']}). Ativando delay proativo de {proactive_delay:.2f}s.")
                time.sleep(proactive_delay)
        
        # Lógica reativa (original) - Mantenha para o caso de estourar o limite mesmo assim
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
    "notification_roles_dm": [],  # <-- NOVO CAMPO ADICIONADO
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
        # Configuração do cache de membros otimizada para reduzir o uso de RAM,
        # mas garantindo que o `members intent` seja totalmente aproveitado.
        # A flag `chunk_guilds_at_startup` já existente é crucial e será mantida.
        member_cache_flags = discord.MemberCacheFlags.from_intents(kwargs.get('intents'))
        member_cache_flags.voice = True  # Garante que estados de voz sejam sempre cacheados.
        member_cache_flags.joined = True # Garante que a data de entrada seja sempre cacheada.
        
        kwargs.update({
            'max_messages': 100,
            'chunk_guilds_at_startup': True, # Esta linha é VITAL para popular o cache na inicialização.
            'member_cache_flags': member_cache_flags, # Utiliza a flag otimizada
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
        
        # Configurações iniciais
        self.config = DEFAULT_CONFIG  # Inicializa com configuração padrão
        self.timezone = pytz.timezone('America/Sao_Paulo')
        
        # Adicione esta linha para inicializar o atributo _ready
        self._ready = asyncio.Event()
        
        # Configurações do bot
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
        self._is_initialized = False  # Nova flag para controle de inicialização
        
        # Monitor de rate limits
        self.rate_limit_monitor = RateLimitMonitor()
        self.last_rate_limit_report = 0
        self.rate_limit_report_interval = 300
        
        # Melhorias no tratamento de rate limit
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
        
        # Novos atributos para tratamento de conexão
        self._connection_attempts = 0
        self._max_connection_attempts = 5
        # --- INÍCIO DA CORREÇÃO ---
        # Aumentado o delay base para ser mais paciente.
        self._connection_delay = 30.0
        # --- FIM DA CORREÇÃO ---
        
        # Novos atributos para controle de eventos
        self.event_counter = 0
        self.last_reconnect_time = None

    def generate_event_id(self):
        """Gera um ID único para cada evento"""
        self.event_counter += 1
        return f"{int(time.time())}_{self.event_counter}"
        
    async def clear_queues(self):
        """Limpa todas as filas de eventos"""
        self.voice_event_queue = asyncio.Queue(maxsize=500)
        self.message_queue = SmartPriorityQueue()
        self.event_counter = 0
        self.last_reconnect_time = datetime.now(pytz.UTC)
        logger.info("Filas de eventos limpas")

    # --- INÍCIO DA CORREÇÃO ---
    # O método `start` foi reescrito para usar uma estratégia de backoff muito mais
    # paciente e robusta, especialmente contra bloqueios do Cloudflare (Error 1015).
    async def start(self, token: str, *, reconnect: bool = True) -> None:
        """
        Sobrescreve o método start para lidar com falhas de conexão de forma robusta,
        limpando recursos e esperando pacientemente antes de tentar novamente.
        """
        while not self.is_closed():
            self._connection_attempts += 1
            wait_duration = 0
            
            try:
                # Só tenta conectar se não estiver já conectado
                if not self.is_ready():
                    logger.info(f"Tentando conectar ao Discord (Tentativa {self._connection_attempts}/{self._max_connection_attempts})...")
                    await self.login(token)
                    await self.connect(reconnect=reconnect)
                
                # Se a conexão for bem-sucedida e depois cair, o loop de reconexão interno do discord.py assume.
                # Se o bot for encerrado corretamente, saímos do loop.
                break

            except discord.HTTPException as e:
                # A sessão de cliente já foi fechada pela biblioteca nesta exceção.
                # Limpa o estado interno para a próxima tentativa.
                if self.http:
                    await self.http.close()
                self.http = discord.http.HTTPClient(self.loop) # <-- CORREÇÃO APLICADA
                self._ready.clear()
                self._connection.clear()

                if "Cloudflare" in str(e) or e.status == 429 or "1015" in str(e):
                    # Para bloqueios do Cloudflare, o backoff precisa ser longo e com variação.
                    # Ex: 1ª falha: ~2.5 min, 2ª falha: ~5 min, etc.
                    wait_duration = 120 + random.uniform(30, 60) * self._connection_attempts
                    logger.warning(
                        f"Bloqueio do Cloudflare/Rate limit severo detectado. "
                        f"Tentativa {self._connection_attempts} falhou."
                    )
                else:
                    # Para outros erros HTTP, usa um backoff exponencial padrão.
                    wait_duration = (self._connection_delay * (2 ** (self._connection_attempts - 1))) + random.uniform(5, 15)
                    logger.error(
                        f"Erro HTTP {e.status} ao conectar. "
                        f"Tentativa {self._connection_attempts} falhou.", exc_info=False
                    )

            except Exception as e:
                # Limpa a conexão em caso de outros erros também.
                if self.http:
                    await self.http.close()
                self.http = discord.http.HTTPClient(self.loop) # <-- CORREÇÃO APLICADA
                self._ready.clear()
                self._connection.clear()
                
                # Backoff para erros genéricos (ex: problemas de rede).
                wait_duration = (self._connection_delay * (2 ** (self._connection_attempts - 1))) + random.uniform(5, 15)
                logger.error(f"Erro inesperado ao conectar: {e}", exc_info=True)

            finally:
                if self._connection_attempts >= self._max_connection_attempts:
                    logger.critical("Máximo de tentativas de conexão atingido. Desistindo.")
                    # Chama o close() final para garantir que tudo seja limpo.
                    await self.close()
                    break # Sai do loop while
                
                if wait_duration > 0 and not self.is_closed():
                    logger.info(f"Aguardando {wait_duration:.2f} segundos antes da próxima tentativa de conexão.")
                    await asyncio.sleep(wait_duration)
    # --- FIM DA CORREÇÃO ---

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

            # Inicializar o backup após o banco estar pronto
            from database import DatabaseBackup
            self.db_backup = DatabaseBackup(self.db)
            logger.info("Backup do banco de dados inicializado")

            # Verificar se a conexão está realmente funcionando
            try:
                async with self.db.pool.acquire() as conn:
                    await asyncio.wait_for(conn.execute("SELECT 1"), timeout=10)
            except Exception as e:
                logger.error(f"Falha ao verificar conexão com o banco: {e}")
                self.db_connection_failed = True
                return False

            self._is_initialized = True

            # Carregar configuração após inicializar o banco
            await self.load_config()

            return True

        except Exception as e:
            logger.critical(f"Falha crítica ao inicializar o banco de dados: {e}", exc_info=True)
            self.db_connection_failed = True
            # Criar instância vazia para evitar erros de NoneType
            try:
                self.db = Database()
                self.db.pool = None
            except Exception:
                self.db = None
            return False

    async def load_config(self, guild_id: int = None):
        """Carrega configuração de forma assíncrona com tratamento melhorado"""
        try:
            # Primeiro tentar carregar do arquivo local
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
                
            # Depois tentar carregar do banco de dados se estiver disponível
            if hasattr(self, 'db') and self.db and hasattr(self.db, 'load_config'):
                try:
                    # Se guild_id foi especificado, carregar apenas essa
                    if guild_id is not None:
                        try:
                            db_config = await self.db.load_config(guild_id)
                            if db_config:
                                self._update_config(db_config)
                                logger.info(f"Configuração carregada do banco para guild {guild_id}")
                                return True
                        except Exception as e:
                            logger.warning(f"Erro ao carregar configuração para guild {guild_id}: {e}")
                    
                    # Se não, carregar para todas as guilds
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
            
            # Fallback para padrão se nenhuma configuração for encontrada
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
        # Garantir que todas as chaves padrão existam
        for key, value in DEFAULT_CONFIG.items():
            if key not in new_config:
                new_config[key] = value
        
        # Atualizar timezone
        self.timezone = pytz.timezone(new_config.get('timezone', 'America/Sao_Paulo'))
        
        # Atualizar configuração
        self.config = new_config
        logger.info("Configuração atualizada com sucesso")

    async def save_config(self, guild_id: int = None):
        """Salva configuração com cache (modificado)"""
        if not hasattr(self, 'config') or not self.config:
            return
            
        try:
            # Salvar no arquivo local
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            # Salvar no banco de dados para cada guild relevante
            if hasattr(self, 'db') and self.db and self.db._is_initialized:
                # Se guild_id não foi especificado, salvar para todas as guilds do bot
                guilds_to_save = [guild_id] if guild_id is not None else [guild.id for guild in self.guilds]
                
                for gid in guilds_to_save:
                    await self.db.save_config(gid, self.config)
                    logger.info(f"Configuração salva no banco para guild {gid}")
                
                # Verificar se o método sync_task_periods existe antes de chamá-lo
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
        
        # Carregar configurações de forma assíncrona
        await self.load_config()
        
        # Inicializar banco de dados
        await self.initialize_db()
        
        # Prossiga apenas se a conexão com o DB for bem-sucedida
        if self.db and not self.db_connection_failed:
            try:
                synced = await self.tree.sync()
                logger.info(f"Comandos slash sincronizados: {len(synced)} comandos.")
            except Exception as e:
                logger.error(f"Erro ao sincronizar comandos slash: {e}")

            # Adicionar task de processamento de eventos de voz
            self.voice_event_processor_task = self.loop.create_task(self.process_voice_events(), name='voice_event_processor')

            self._setup_complete = True
            logger.info("Setup hook concluído.")
        else:
            logger.critical("Falha na inicialização do banco de dados. As tarefas não serão iniciadas.")
            self.db_connection_failed = True
    
    # --- INÍCIO DA MODIFICAÇÃO: Função de envio de mensagens refatorada ---
    async def _ensure_http_client(self):
        """Garante que o HTTP client do discord (aiohttp session) esteja operacional.
        Se a session estiver fechada, tenta fechar com segurança e recriar o HTTPClient.
        """
        try:
            session = getattr(self.http, 'session', None)
            if session is None or getattr(session, 'closed', True):
                logger.warning("HTTP session fechada. Reinicializando HTTP client...")
                try:
                    await self.http.close()
                except Exception:
                    pass
                # recria o HTTP client
                try:
                    self.http = discord.http.HTTPClient(self.loop)
                except Exception as e:
                    logger.error(f"Falha ao recriar HTTPClient: {e}", exc_info=True)
                await asyncio.sleep(0.3)
                logger.info("HTTP client reiniciado com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao garantir HTTP client: {e}", exc_info=True)

    async def send_with_fallback(self, destination, content=None, embed=None, file=None):
        """
        Envia mensagem com tratamento adicional para 'Session is closed'.
        Tenta recriar a session e re-enviar uma vez antes de propagar o erro.
        """
        # garante que o HTTP client está ok antes de tentar
        try:
            await self._ensure_http_client()
        except Exception:
            # se falhar aqui, ainda tentamos enviar e deixamos o erro ser tratado abaixo
            pass

        max_attempts = 2
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            try:
                if file:
                    if isinstance(file, BytesIO):
                        file.seek(0)
                        if not isinstance(file, discord.File):
                            file = discord.File(file, filename='image.png')
                    await destination.send(content=content, embed=embed, file=file)
                elif embed:
                    await destination.send(embed=embed)
                elif content:
                    await destination.send(content)
                return  # sucesso
            except discord.HTTPException as e:
                # Rate limit -> propaga como RateLimitException com retry_after
                retry_after = getattr(e, 'retry_after', None) or getattr(e, 'retry_after_seconds', None)
                if retry_after is None:
                    # tentar extrair de e.text se disponível (não garantido)
                    retry_after = getattr(e, 'retry_after', None)
                if retry_after is not None:
                    logger.warning(f"Rate limit ao enviar para {destination}. retry_after={retry_after}")
                    raise RateLimitException("Rate limited by Discord API", float(retry_after)) from e
                logger.error(f"Erro HTTP ao enviar para {destination}: {getattr(e, 'text', str(e))}", exc_info=True)
                raise
            except RuntimeError as e:
                if "Session is closed" in str(e):
                    logger.warning(f"Session fechada detectada ao enviar para {destination} (attempt {attempt}/{max_attempts}). Tentando recuperar HTTP client...")
                    try:
                        await self._ensure_http_client()
                        await asyncio.sleep(0.5)
                        continue
                    except Exception as rec_e:
                        logger.error(f"Falha ao recuperar HTTP client durante envio: {rec_e}", exc_info=True)
                        raise
                logger.error(f"RuntimeError inesperado ao enviar para {destination}: {e}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Erro inesperado ao enviar mensagem para {destination}: {e}", exc_info=True)
                raise
# --- FIM DA MODIFICAÇÃO ---


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
                            
                            # Se não há sessão ativa, criar uma
                            if audio_key not in self.active_sessions:
                                self.active_sessions[audio_key] = {
                                    'start_time': datetime.now(pytz.UTC),
                                    'last_audio_time': datetime.now(pytz.UTC),
                                    'audio_disabled': current_audio_state,
                                    'total_audio_off_time': 0,
                                    'estimated': False
                                }
                                continue
                                
                            # Verificar mudanças no estado de áudio
                            if current_audio_state and not self.active_sessions[audio_key]['audio_disabled']:
                                # Áudio foi desligado
                                self.active_sessions[audio_key]['audio_disabled'] = True
                                self.active_sessions[audio_key]['audio_off_time'] = datetime.now(pytz.UTC)
                                
                            elif not current_audio_state and self.active_sessions[audio_key]['audio_disabled']:
                                # Áudio foi ligado
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
                            
                            # Se o pool estiver sobrecarregado, aumentar o tamanho
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
    
    # --- INÍCIO DA MODIFICAÇÃO: Lógica do processador de filas refatorada ---
    async def process_queues(self):
        """
        Processador de filas redesenhado para lidar corretamente com rate limits,
        evitando o erro 'task_done() called too many times'.
        """
        await self.wait_until_ready()
        while True:
            try:
                # 1. Checagem global de rate limit antes de processar qualquer coisa
                if self.rate_limit_monitor.should_delay():
                    delay = self.rate_limit_monitor.adaptive_delay
                    logger.debug(f"Delay global de rate limit ativado. Esperando {delay:.2f} segundos")
                    await asyncio.sleep(delay)
                    continue

                # 2. Processamento da fila de eventos de voz (sem alterações)
                if not self.voice_event_queue.empty():
                    batch = []
                    for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                        batch.append(await self.voice_event_queue.get())
                    
                    await self._process_voice_batch(batch)
                    
                    for _ in batch:
                        self.voice_event_queue.task_done()

                # 3. Processamento da fila de mensagens (lógica principal corrigida)
                item, priority = await self.message_queue.get_next_message()
                if item is None:
                    await asyncio.sleep(0.2) # Sleep menor quando a fila está vazia
                    continue

                try:
                    # Tenta processar o item da fila
                    destination, content, embed, file = item
                    if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                        await self.send_with_fallback(destination, content, embed, file)
                    else:
                         logger.warning(f"Destino inválido para mensagem na fila: {type(destination)}")
                    
                    # Se o envio foi bem-sucedido, marca a tarefa como concluída
                    self.message_queue.task_done(priority)

                except RateLimitException as e:
                    # Se um rate limit ocorreu, o item NÃO é concluído.
                    # Ele é colocado de volta na fila para uma nova tentativa.
                    logger.warning(f"Rate limit detectado pelo processador. Devolvendo item para a fila. Aguardando {e.retry_after:.2f}s.")
                    await self.message_queue.put(item, priority)
                    # O 'get' anterior é balanceado por este 'put', então não chamamos task_done().
                    await asyncio.sleep(e.retry_after) # Espera o tempo solicitado pela API

                except Exception as e:
                    # Tratamento especial para 'Session is closed': re-enfileira e tenta recuperar a client.
                    if isinstance(e, RuntimeError) and "Session is closed" in str(e):
                        logger.warning("Session fechada detectada ao processar item. Tentando recuperar HTTP client e re-enfileirar item.")
                        try:
                            await self._ensure_http_client()
                            # Re-enfileira o item para tentar novamente
                            await self.message_queue.put(item, priority)
                            # Não marcamos como task_done para manter o balanceamento
                            await asyncio.sleep(1.0)
                            continue
                        except Exception as rec_e:
                            logger.error(f"Falha ao recuperar HTTP client: {rec_e}", exc_info=True)
                            # Se a recuperação falhar, descartamos para evitar loop infinito
                            self.message_queue.task_done(priority)
                            logger.error(f"Item descartado após falha de recuperação: {item}")
                    else:
                        # Se ocorreu um erro irrecuperável (ex: Forbidden, NotFound), o item é descartado.
                        logger.error(f"Erro irrecuperável ao processar item da fila: {e}. O item será descartado.", exc_info=True)
                        # Marcamos como concluído para que não seja tentado novamente.
                        self.message_queue.task_done(priority)

            except Exception as e:
                # Erro no próprio loop do processador (não relacionado a um item específico)
                logger.critical(f"Erro crítico no loop do processador de filas: {e}", exc_info=True)
                if "Cloudflare" in str(e) or "1015" in str(e):
                    self.rate_limit_monitor.handle_cloudflare_block()
                await asyncio.sleep(5)
    # --- FIM DA MODIFICAÇÃO ---

    async def _process_voice_batch(self, batch):
        processed = {}
        
        for event in batch:
            try:
                # CORREÇÃO: Unpack dos 4 primeiros elementos, ignorando os extras.
                # Isso resolve o erro "too many values to unpack" lidando com os dois formatos de evento (4 e 6 elementos).
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
                # Ignorar bots
                if member.bot:
                    continue

                if member.id in self.config.get('whitelist', {}).get('users', []) or \
                   any(role.id in self.config.get('whitelist', {}).get('roles', []) for role in member.roles):
                    continue
                
                audio_key = (member.id, member.guild.id)
                
                # Se for uma sessão estimada e o usuário realmente saiu, ajustar o tempo
                if audio_key in self.active_sessions and self.active_sessions[audio_key].get('estimated'):
                    if before.channel is not None and after.channel is None:
                        # Ajustar o tempo inicial para refletir melhor a realidade
                        estimated_start = self.active_sessions[audio_key]['start_time']
                        actual_start = max(estimated_start, datetime.now(pytz.UTC) - timedelta(hours=1))  # No máximo 1 hora
                        self.active_sessions[audio_key]['start_time'] = actual_start
                        self.active_sessions[audio_key]['estimated'] = False  # Não é mais estimada
                
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
            # Registrar entrada no banco de dados
            await self.db.log_voice_join(member.id, member.guild.id)
            
            self.active_sessions[(member.id, member.guild.id)] = {
                'start_time': datetime.now(pytz.UTC),
                'last_audio_time': datetime.now(pytz.UTC),
                'audio_disabled': after.self_deaf or after.deaf,
                'total_audio_off_time': 0,
                'estimated': False  # Nova flag para indicar sessões estimadas
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
            # Calcular tempo total e tempo sem áudio
            now = datetime.now(pytz.UTC)
            total_time = (now - session_data['start_time']).total_seconds()
            audio_off_time = session_data.get('total_audio_off_time', 0)
            
            # Verificar se o áudio estava desligado e calcular o tempo
            if 'audio_off_time' in session_data:
                audio_off_duration = (now - session_data['audio_off_time']).total_seconds()
                audio_off_time += audio_off_duration
            
            # Calcular tempo efetivo (total - tempo sem áudio)
            effective_time = max(0, total_time - audio_off_time)
            
            # Registrar saída no banco de dados
            try:
                await self.db.log_voice_leave(member.id, member.guild.id, int(effective_time))
            except Exception as e:
                logger.error(f"Erro ao registrar saída de voz: {e}")
                await self.log_action("Erro DB - Saída de voz", member, str(e))
            
            # Logar a saída
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
            # Garantir que a sessão seja removida
            self.active_sessions.pop((member.id, member.guild.id), None)

    async def _handle_voice_move(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState, absence_channel_id: int):
        audio_key = (member.id, member.guild.id)
        
        # Caso 1: Entrando no canal de ausência a partir de outro canal
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
                    'paused_channel_id': before.channel.id  # Armazena o canal original
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
        
        # Caso 2: Saindo completamente do canal de voz (incluindo da ausência)
        elif (before.channel is not None and 
              after.channel is None):
            
            # Se estava na ausência e tem sessão ativa
            if before.channel.id == absence_channel_id and audio_key in self.active_sessions:
                # Se a sessão estava pausada, tratar como saída normal do canal original
                if self.active_sessions[audio_key].get('paused'):
                    # Obter o canal original antes de pausar
                    original_channel_id = self.active_sessions[audio_key].get('paused_channel_id')
                    original_channel = member.guild.get_channel(original_channel_id) if original_channel_id else None
                    
                    # Se encontrou o canal original, criar estado fictício
                    if original_channel:
                        before_state_data = {
                            'channel_id': original_channel.id,
                            'self_deaf': before.self_deaf,
                            'deaf': before.deaf,
                            'self_mute': before.self_mute,
                            'mute': before.mute,
                            'self_stream': False,
                            'self_video': False,
                            'suppress': False,
                            'requested_to_speak_at': None
                        }
                        
                        before_state = discord.VoiceState(
                            data=before_state_data,
                            channel=original_channel
                        )
                        
                        await self._handle_voice_leave(member, before_state)
                    else:
                        # Se não encontrou o canal original, usar o canal de ausência
                        await self._handle_voice_leave(member, before)
                    
                    # Limpar estado pausado
                    for key in ['paused', 'paused_time', 'pre_pause_duration', 'paused_channel_id']:
                        self.active_sessions[audio_key].pop(key, None)
                else:
                    # Se não estava pausada, tratar como saída normal
                    await self._handle_voice_leave(member, before)
            else:
                # Saída normal (não estava na ausência)
                await self._handle_voice_leave(member, before)
        
        # Caso 3: Voltando da ausência para outro canal
        elif (before.channel is not None and 
              before.channel.id == absence_channel_id and 
              after.channel is not None and 
              after.channel.id != absence_channel_id):
            
            if audio_key in self.active_sessions and self.active_sessions[audio_key].get('paused'):
                pause_duration = (datetime.now(pytz.UTC) - self.active_sessions[audio_key]['paused_time']).total_seconds()
                
                # Restaurar tempo de sessão
                self.active_sessions[audio_key]['start_time'] = datetime.now(pytz.UTC) - timedelta(
                    seconds=self.active_sessions[audio_key]['pre_pause_duration'])
                
                # Limpar estado pausado
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
        
        # Caso 4: Movimento entre outros canais (não envolvendo ausência)
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
        
        # Caso 5: Mudança de estado no mesmo canal (ex: mute/deafen)
        elif (before.channel is not None and 
              after.channel is not None and 
              before.channel == after.channel):
            
            await self._handle_audio_change(member, before, after)

    async def _handle_audio_change(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        audio_key = (member.id, member.guild.id)
        
        # Se não há sessão ativa e o usuário está em um canal, criar uma
        if audio_key not in self.active_sessions and after.channel is not None:
            await self._handle_voice_join(member, after)
            return

        if audio_key not in self.active_sessions:
            return

        audio_was_off = before.self_deaf or before.deaf
        audio_is_off = after.self_deaf or after.deaf

        # Se o áudio foi desligado
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
        
        # Se o áudio foi reativado
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
                
                # Verificar se o evento é válido (tem todos os campos esperados)
                if len(event) < 6:  # Evento antigo não timestamp/ID
                    self.voice_event_queue.task_done()
                    continue
                    
                _, member, before, after, event_id, event_time = event
                
                # Ignorar eventos muito antigos
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

        # Usar um conjunto para evitar enviar DMs duplicadas
        members_to_notify = set()

        # 1. Adicionar todos os administradores
        for member in guild.members:
            if not member.bot and member.guild_permissions.administrator:
                members_to_notify.add(member)

        # 2. Adicionar membros com os cargos de notificação
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
                # Usa o método send_dm que enfileira a mensagem
                await self.send_dm(member, message_content=None, embed=embed)
                logger.debug(f"DM de notificação de administrador/cargo enfileirada para {member.display_name} ({member.id}).")
            except Exception as e:
                logger.error(f"Falha ao enfileirar DM de notificação para {member.display_name} na guilda {guild.name}: {e}")

    async def send_dm(self, member: discord.Member, message_content: str, embed: discord.Embed):
        try:
            # A prioridade para DMs de admin será alta
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
            if e.code == 50007:  # Não é possível enviar mensagens para este usuário
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

            # Obter informações do período atual
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
                'days_remaining': 'N/A'  # Valor padrão
            }
            
            # Calcular dias restantes para todos os avisos
            if last_check:
                # last_check pode ser um dict ou um objeto; tente acessar de forma segura
                period_end = None
                if isinstance(last_check, dict):
                    period_end = last_check.get('period_end')
                else:
                    period_end = getattr(last_check, 'period_end', None)
                
                if period_end:
                    now = datetime.now(pytz.UTC)
                    
                    # Garantir timezone
                    if getattr(period_end, 'tzinfo', None) is None:
                        try:
                            period_end = period_end.replace(tzinfo=pytz.UTC)
                        except Exception:
                            pass
                    
                    try:
                        days_remaining = max(0, (period_end - now).days)
                    except Exception:
                        days_remaining = 0
                    
                    # Formatar para mensagem amigável
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
            
            # Registrar aviso no banco de dados
            try:
                if hasattr(self, 'db') and self.db:
                    await self.db.log_warning(member.id, member.guild.id, warning_type)
            except Exception as e:
                logger.error(f"Erro ao registrar aviso no DB para {member}: {e}")
            
            await self.log_action(f"Aviso Enviado ({warning_type})", member)
            
            # Notificar administradores se for primeiro ou segundo aviso
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
    """Evento que detecta quando membros recebem cargos e verifica se são devoluções por inatividade"""
    if before.roles == after.roles:
        return

    # Verificar se há cargos monitorados na configuração
    if not hasattr(bot, 'config') or not bot.config.get('tracked_roles'):
        return

    tracked_roles = set(bot.config['tracked_roles'])

    # Encontrar cargos adicionados
    added_roles = [role for role in after.roles if role not in before.roles and role.id in tracked_roles]

    if added_roles:
        # Sempre que um cargo monitorado é adicionado, o histórico do usuário é resetado.
        # Isso cria um "novo começo" e previne que o bot remova o cargo imediatamente
        # com base em um período de inatividade anterior onde o usuário não possuía o cargo.
        if hasattr(bot, 'db') and bot.db:
            await bot.db.reset_user_tracking(after.id, after.guild.id)
            logger.info(f"Acompanhamento de inatividade resetado para {after.display_name} após receber um cargo monitorado.")

        try:
            for role in added_roles:
                try:
                    last_removal = None
                    if hasattr(bot, 'db') and bot.db:
                        # CORREÇÃO: Chama a nova função para verificar a remoção do cargo específico.
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
                        # *** INÍCIO DA CORREÇÃO ***
                        # Ajusta a chamada para a nova assinatura da função, passando o cargo como uma lista.
                        await send_forgiveness_message(after, [role])
                        # *** FIM DA CORREÇÃO ***
                        
                        # O 'break' aqui é mantido para enviar apenas uma mensagem de perdão,
                        # mesmo que múltiplos cargos sejam devolvidos de uma vez.
                        break

            # Registrar a atribuição de cada cargo novo, independentemente de ser uma devolução ou não.
            for role in added_roles:
                try:
                    if hasattr(bot, 'db') and bot.db:
                        await bot.db.log_role_assignment(after.id, after.guild.id, role.id)
                    logger.info(f"Registrada atribuição de cargo {role.name} para {after.display_name}")
                except Exception as e:
                    logger.error(f"Erro ao registrar atribuição de cargo: {e}")
        except Exception as e:
            logger.error(f"Erro ao processar atualização de membro: {e}")

# *** INÍCIO DA CORREÇÃO ***
# Função atualizada para aceitar uma lista de cargos e enviar uma única DM consolidada.
async def send_forgiveness_message(member: discord.Member, roles: List[discord.Role]):
    """Envia UMA mensagem de perdão quando um ou mais cargos são devolvidos."""
    if not roles:
        return

    try:
        # Pega o primeiro cargo da lista para a verificação no DB,
        # assumindo que uma mensagem por devolução é suficiente para o controle de spam.
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

        # Obter configurações atuais
        required_minutes = bot.config.get('required_minutes', 15)
        required_days = bot.config.get('required_days', 2)
        monitoring_period = bot.config.get('monitoring_period', 14)

        # Formata a lista de cargos para a mensagem
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

        # Registrar o envio da mensagem de perdão (usando o primeiro cargo como referência)
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
# *** FIM DA CORREÇÃO ***


@bot.event
async def on_ready():
    try:
        if hasattr(bot, '_ready_called') and bot._ready_called:
            logger.info("on_ready chamado novamente, mas já foi processado. Ignorando.")
            return
        bot._ready_called = True

        logger.info(f'Bot conectado como {bot.user}')
        logger.info(f"Latência: {round(bot.latency * 1000)}ms")


        # --- ETAPA 1: INICIALIZAÇÃO CRÍTICA ---
        try:
            # Garante que o banco de dados e a configuração estão prontos.
            if not hasattr(bot, 'db') or not bot.db or not getattr(bot.db, '_is_initialized', False):
                logger.error("Banco de dados não foi inicializado corretamente. Tentando novamente...")
                if not await bot.initialize_db():
                    logger.critical("Falha crítica na inicialização do banco de dados no on_ready.")
                    return

            logger.info("Carregando e validando configurações...")
            await bot.load_config()
            await bot.save_config()  # Garante que a configuração está sincronizada com o DB.
            
            if hasattr(bot, 'db') and getattr(bot.db, 'sync_task_periods', None):
                monitoring_period = bot.config.get('monitoring_period')
                if monitoring_period:
                    await bot.db.sync_task_periods(monitoring_period)

        except Exception as critical_error:
            logger.critical(f"Erro crítico na inicialização: {critical_error}", exc_info=True)
            return

        logger.info("Inicialização crítica concluída.")

        # --- ETAPA 2: INICIAR TAREFAS DE FUNDO E PROCESSOS NÃO CRÍTICOS ---
        # Estas tarefas podem levar tempo e rodarão em segundo plano.

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

            # Agendar tarefas principais com um pequeno atraso para não sobrecarregar a inicialização
            await start_task_with_jitter(register_role_assignments_wrapper(), 'register_role_assignments_wrapper')
            await start_task_with_jitter(inactivity_check(), 'inactivity_check_wrapper')
            await start_task_with_jitter(cleanup_members(), 'cleanup_members_wrapper')
            await start_task_with_jitter(database_backup(), 'database_backup_wrapper')
            await start_task_with_jitter(cleanup_old_data(), 'cleanup_old_data_wrapper')
            await start_task_with_jitter(monitor_rate_limits(), 'monitor_rate_limits_wrapper')
            await start_task_with_jitter(report_metrics(), 'report_metrics_wrapper')
            await start_task_with_jitter(health_check(), 'health_check_wrapper')
            
            # Tarefas de processamento contínuo
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

        # Enviar log de inicialização
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
        
        # Se acabamos de reconectar, ignorar eventos muito antigos
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
    # Retorna uma resposta simples para o UptimeRobot
    return "Bot de Controle de Atividade está online."

def run():
    # O Render define a porta através da variável de ambiente PORT
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def start_web_server():
    # Inicia o servidor Flask em um thread separado
    t = Thread(target=run)
    t.daemon = True
    t.start()
# --- FIM DO CÓDIGO DO SERVIDOR WEB ---

# Importar comandos
from bot_commands import *

async def main():
    load_dotenv()
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    
    # --- INÍCIO DA CORREÇÃO ---
    # Aumentado o delay inicial e adicionado um fator aleatório para evitar
    # bloqueios do Cloudflare em ambientes de contêiner (como o Render).
    initial_wait = 120 + random.uniform(15, 45)
    logger.info(f"Aguardando {initial_wait:.2f} segundos antes da primeira tentativa de conexão para estabilização do ambiente...")
    await asyncio.sleep(initial_wait)
    # --- FIM DA CORREÇÃO ---
    
    # Tentar inicializar o banco de dados antes de iniciar o bot
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

    # Inicia o servidor web ANTES de iniciar o bot
    start_web_server()
        
    async with bot:
        try:
            await bot.start(DISCORD_TOKEN)
        except Exception as e:
            logger.critical(f"Erro ao iniciar o bot: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot desligado manualmente.")