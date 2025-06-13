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

class PriorityQueue:
    def __init__(self):
        self.queues = {
            'high': asyncio.Queue(maxsize=100),
            'normal': asyncio.Queue(maxsize=500),
            'low': asyncio.Queue(maxsize=200)
        }
    
    async def put(self, item, priority='normal'):
        await self.queues[priority].put(item)
    
    async def get(self):
        # Processa em ordem de prioridade
        for priority in ['high', 'normal', 'low']:
            if not self.queues[priority].empty():
                return await self.queues[priority].get(), priority
        return None, None
    
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
        self.message_queue = PriorityQueue()
        self.voice_event_processor_task = None
        self.queue_processor_task = None
        self.command_processor_task = None
        self.rate_limited = False
        self.last_rate_limit = None
        self.rate_limit_delay = 2.0
        self.max_rate_limit_delay = 10.0
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
        self.health_check_task = None

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
                                    
                                    await self.message_queue.put((
                                        (self.get_channel(self.config['log_channel']), None, embed, None),
                                        "high"
                                    ))
                                
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
                                        
                                        await self.message_queue.put((
                                            (self.get_channel(self.config['log_channel']), None, embed, None),
                                            "high"
                                        ))
                
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
        if self.rate_limited:
            if self.last_rate_limit_time and (datetime.now() - self.last_rate_limit_time).total_seconds() < 60:
                logger.warning("Ignorando envio devido a rate limit recente")
                return False
            self.rate_limited = False

        try:
            await channel.send(content=content, embed=embed, file=file)
            self.rate_limit_count = 0
            return True
        except discord.errors.HTTPException as e:
            if e.status == 429:
                self.rate_limited = True
                self.last_rate_limit_time = datetime.now()
                retry_after = float(e.response.headers.get('Retry-After', 5))
                
                if retry_count < self.max_rate_limit_retries:
                    logger.warning(f"Rate limit atingido. Tentando novamente em {retry_after} segundos (tentativa {retry_count + 1})")
                    await asyncio.sleep(retry_after)
                    return await self.safe_send(channel, content, embed, file, retry_count + 1)
                
                logger.error(f"Rate limit persistente após {self.max_rate_limit_retries} tentativas")
                return False
            raise

    async def process_queues(self):
        """Processa todas as filas de mensagens com prioridades"""
        while True:
            try:
                # Processar eventos de voz primeiro
                if not self.voice_event_queue.empty():
                    batch = []
                    for _ in range(min(self._batch_processing_size, self.voice_event_queue.qsize())):
                        batch.append(await self.voice_event_queue.get())
                    
                    await self._process_voice_batch(batch)
                    
                    for _ in batch:
                        self.voice_event_queue.task_done()
                
                # Processar mensagens da fila prioritária
                item, priority = await self.message_queue.get()
                if item is None:
                    await asyncio.sleep(0.1)
                    continue
                    
                # Verificar o tipo de item
                if isinstance(item, tuple):
                    # Formato: (destination, content, embed, file)
                    if len(item) == 4:
                        destination, content, embed, file = item
                        if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                            await self.safe_send(destination, content, embed, file)
                        else:
                            logger.warning(f"Destino inválido para mensagem: {type(destination)}")
                    # Formato simplificado: (destination, embed)
                    elif len(item) == 2:
                        destination, embed = item
                        if isinstance(destination, (discord.TextChannel, discord.User, discord.Member)):
                            await self.safe_send(destination, embed=embed)
                        else:
                            logger.warning(f"Destino inválido para mensagem: {type(destination)}")
                    else:
                        logger.warning(f"Item da fila em formato desconhecido: {item}")
                else:
                    logger.warning(f"Item da fila não é uma tupla: {type(item)}")
                    
                self.message_queue.task_done(priority)
                    
            except Exception as e:
                logger.error(f"Erro no processador de filas: {e}")
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
                            
                            await self.message_queue.put((
                                (self.get_channel(self.config['log_channel']), None, embed, None),
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
                                
                                await self.message_queue.put((
                                    (self.get_channel(self.config['log_channel']), None, embed, None),
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
                    
                    await self.message_queue.put((
                        (self.get_channel(self.config['log_channel']), None, embed, None),
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
                    
                    await self.message_queue.put((
                        (self.get_channel(self.config['log_channel']), None, embed, None),
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
                        
                        await self.message_queue.put((
                            (self.get_channel(self.config['log_channel']), None, embed, None),
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
                        
                        await self.message_queue.put((
                            (self.get_channel(self.config['log_channel']), None, embed, None),
                            "high"
                        ))
                    except Exception as e:
                        logger.error(f"Erro ao registrar retorno de ausência: {e}")

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
                    
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)

    async def log_action(self, action: str, member: Optional[discord.Member] = None, details: str = None, file: discord.File = None, embed: discord.Embed = None):
        """Registra uma ação no canal de logs com tratamento de rate limit"""
        try:
            channel = self.get_channel(self.config.get('log_channel', 1376013013206827161))
            if not channel:
                logger.warning("Canal de logs não encontrado")
                return
                
            # Se um embed foi fornecido, usá-lo diretamente
            if embed is not None:
                await self.message_queue.put((
                    (channel, None, embed, file),
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
            await self.message_queue.put((
                (channel, None, embed, file),
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
            
            await self.message_queue.put((
                (channel, None, embed, None),
                priority
            ))
            
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
            await self.message_queue.put((
                (member, None, embed, None),
                "low"
            ))
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
    intents=intents
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
    # Adicionar delay aleatório entre 1-10 segundos para evitar rate limit
    delay = random.uniform(1, 10)
    logger.info(f"Aguardando {delay:.2f} segundos antes de iniciar para evitar rate limit...")
    time.sleep(delay)
    
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
    
    max_retries = 5
    initial_delay = 2
    
    async def run_bot():
        for attempt in range(max_retries):
            try:
                await bot.start(os.getenv('DISCORD_TOKEN'))
                break
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = float(e.response.headers.get('Retry-After', 60))
                    logger.error(f"Rate limit atingido (tentativa {attempt + 1}/{max_retries}). Tentando novamente em {retry_after} segundos...")
                    await asyncio.sleep(retry_after)
                else:
                    raise
            except Exception as e:
                logger.error(f"Erro ao iniciar o bot (tentativa {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                sleep_time = initial_delay * (2 ** attempt)
                logger.info(f"Tentando novamente em {sleep_time} segundos...")
                await asyncio.sleep(sleep_time)
    
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.critical(f"Erro ao iniciar o bot após {max_retries} tentativas: {e}")
        raise
    finally:
        if hasattr(bot, 'db') and bot.db:
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                     bot.db.close()
            except Exception as e:
                logger.error(f"Erro ao fechar pool de conexões: {e}")