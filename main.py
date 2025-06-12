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

# Configuração básica do logger
logger = logging.getLogger('inactivity_bot')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = RotatingFileHandler('bot.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configurações padrão
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
    "whitelist": {"users": [], "roles": []},
    "warnings": {
        "first_warning": 3,
        "second_warning": 1,
        "messages": {
            "first": "⚠️ Aviso de Inatividade ⚠️\nVocê está prestes a perder seus cargos por inatividade.",
            "second": "🔴 Último Aviso 🔴\nVocê perderá seus cargos AMANHÃ por inatividade.",
            "final": "❌ Cargos Removidos ❌\nVocê perdeu seus cargos por inatividade."
        }
    }
}

class InactivityBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.voice_states = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            chunk_guilds_at_startup=False
        )
        
        self.config = DEFAULT_CONFIG.copy()
        self.timezone = pytz.timezone(self.config['timezone'])
        self.db = None
        self.active_sessions = {}
        self.queues = {
            'voice': asyncio.Queue(maxsize=1000),
            'commands': asyncio.Queue(maxsize=500),
            'high': asyncio.Queue(maxsize=100),
            'normal': asyncio.Queue(maxsize=500),
            'low': asyncio.Queue(maxsize=200)
        }
        self.rate_limits = {}
        # self._setup_tasks() # Remova esta linha
        
    async def setup_hook(self):
        """Configuração inicial do bot"""
        self.load_config()
        await self._initialize_db()
        await self.tree.sync()
        self._setup_tasks() # Chame _setup_tasks aqui
        logger.info("Bot inicializado com sucesso")

    def _setup_tasks(self):
        """Configura as tarefas em segundo plano"""
        self.tasks = {
            'voice_processor': asyncio.create_task(self._process_voice_events()),
            'command_processor': asyncio.create_task(self._process_commands()),
            'queue_processor': asyncio.create_task(self._process_queues()),
            'audio_checker': asyncio.create_task(self._check_audio_states())
        }

    # Métodos principais simplificados
    async def _initialize_db(self):
        """Inicializa a conexão com o banco de dados"""
        from database import Database
        self.db = Database()
        await self.db.initialize()

    def load_config(self):
        """Carrega a configuração do arquivo JSON"""
        try:
            if os.path.exists('config.json'):
                with open('config.json') as f:
                    self.config.update(json.load(f))
                logger.info("Configuração carregada")
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")

    async def save_config(self):
        """Salva a configuração atual"""
        try:
            with open('config.json', 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")

    # Processadores de fila simplificados
    async def _process_voice_events(self):
        """Processa eventos de voz"""
        while True:
            try:
                event = await self.queues['voice'].get()
                await self._handle_voice_event(*event)
                self.queues['voice'].task_done()
            except Exception as e:
                logger.error(f"Erro ao processar evento de voz: {e}")
                await asyncio.sleep(1)

    async def _process_commands(self):
        """Processa comandos na fila"""
        while True:
            try:
                interaction, command, args, kwargs = await self.queues['commands'].get()
                await command(interaction, *args, **kwargs)
                self.queues['commands'].task_done()
            except Exception as e:
                logger.error(f"Erro ao processar comando: {e}")
                await asyncio.sleep(1)

    async def _process_queues(self):
        """Processa mensagens nas filas de prioridade"""
        while True:
            for priority in ['high', 'normal', 'low']:
                if not self.queues[priority].empty():
                    channel, content, embed, file = await self.queues[priority].get()
                    try:
                        await channel.send(content=content, embed=embed, file=file)
                    except Exception as e:
                        logger.error(f"Erro ao enviar mensagem: {e}")
                    finally:
                        self.queues[priority].task_done()
            await asyncio.sleep(0.1)

    async def _check_audio_states(self):
        """Verifica periodicamente o estado de áudio dos membros"""
        while True:
            try:
                for guild in self.guilds:
                    for vc in guild.voice_channels:
                        for member in vc.members:
                            await self._update_audio_state(member)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Erro ao verificar estados de áudio: {e}")
                await asyncio.sleep(60)

    # Handlers simplificados
    async def _handle_voice_event(self, member, before, after):
        """Lida com mudanças no estado de voz"""
        if after.channel and after.channel.id != self.config.get('absence_channel'):
            await self._handle_voice_join(member, after)
        elif before.channel and before.channel.id != self.config.get('absence_channel'):
            await self._handle_voice_leave(member, before)

    async def _handle_voice_join(self, member, after):
        """Lida com entrada em canal de voz"""
        key = (member.id, member.guild.id)
        self.active_sessions[key] = {
            'start_time': datetime.utcnow(),
            'audio_disabled': after.self_deaf
        }
        await self.db.log_voice_join(member.id, member.guild.id)
        await self._log_action(f"{member} entrou em {after.channel.name}")

    async def _handle_voice_leave(self, member, before):
        """Lida com saída de canal de voz"""
        key = (member.id, member.guild.id)
        if key in self.active_sessions:
            session = self.active_sessions.pop(key)
            duration = (datetime.utcnow() - session['start_time']).total_seconds()
            await self.db.log_voice_leave(member.id, member.guild.id, int(duration))
            await self._log_action(f"{member} saiu de {before.channel.name}")

    async def _update_audio_state(self, member):
        """Atualiza o estado de áudio de um membro"""
        key = (member.id, member.guild.id)
        if key in self.active_sessions:
            current_state = member.voice.self_deaf
            session = self.active_sessions[key]
            
            if current_state != session.get('audio_disabled'):
                session['audio_disabled'] = current_state
                state = "mudo" if current_state else "com áudio"
                await self._log_action(f"{member} alterou estado para {state}")

    async def _log_action(self, message, member=None, embed=None, file=None):
        """Registra uma ação no log"""
        channel = self.get_channel(self.config['log_channel'])
        if channel:
            await self.queues['high'].put((channel, message, embed, file))

    async def send_warning(self, member, warning_type):
        """Envia um aviso de inatividade ao membro."""
        warning_message = self.config['warnings']['messages'].get(warning_type)
        if warning_message:
            try:
                await member.send(warning_message)
                await self.db.log_warning(member.id, member.guild.id, warning_type)
                logger.info(f"Aviso '{warning_type}' enviado para {member} no servidor {member.guild.name}")
            except discord.Forbidden:
                logger.warning(f"Não foi possível enviar DM para {member} (DMs desativadas).")
            except Exception as e:
                logger.error(f"Erro ao enviar aviso para {member}: {e}")

    async def log_action(self, action_type: str, member: Optional[discord.Member], message: str, embed: Optional[discord.Embed] = None, file: Optional[discord.File] = None):
        """Registra uma ação em um canal de log configurado."""
        log_channel_id = self.config.get('log_channel')
        if log_channel_id:
            log_channel = self.get_channel(log_channel_id)
            if log_channel:
                full_message = f"**{action_type}**\nUsuário: {member.mention if member else 'N/A'}\n{message}"
                try:
                    await self.queues['normal'].put((log_channel, full_message, embed, file))
                except asyncio.QueueFull:
                    logger.warning("Fila de log cheia, descartando mensagem.")
            else:
                logger.warning(f"Canal de log com ID {log_channel_id} não encontrado.")
        else:
            logger.info(f"Ação: {action_type}, Usuário: {member}, Mensagem: {message}")

    async def notify_roles(self, message: str):
        """Notifica os cargos configurados no canal de notificação."""
        notification_channel_id = self.config.get('notification_channel')
        if notification_channel_id:
            notification_channel = self.get_channel(notification_channel_id)
            if notification_channel:
                try:
                    await self.queues['normal'].put((notification_channel, message, None, None))
                except asyncio.QueueFull:
                    logger.warning("Fila de notificação cheia, descartando mensagem.")
            else:
                logger.warning(f"Canal de notificação com ID {notification_channel_id} não encontrado.")

    async def get_cached_user_data(self, user_id: int, guild_id: int) -> Optional[dict]:
        """Obtém dados do usuário do cache."""
        # Implementação de cache simples (pode ser aprimorada com Redis/memcached)
        return self._user_cache.get((user_id, guild_id))

    async def set_cached_user_data(self, user_id: int, guild_id: int, data: dict):
        """Define dados do usuário no cache."""
        # Implementação de cache simples
        if not hasattr(self, '_user_cache'):
            self._user_cache = {}
        self._user_cache[(user_id, guild_id)] = data

    async def invalidate_cache(self, cache_type: str):
        """Invalida o cache de um tipo específico."""
        if cache_type == 'user_data':
            if hasattr(self, '_user_cache'):
                self._user_cache = {}
                logger.info("Cache de dados de usuário invalidado.")

    async def check_rate_limit(self, action: str) -> int:
        """Verifica e gerencia o rate limit para uma ação específica."""
        # Implementação de rate limit simples
        current_time = time.time()
        
        if action not in self.rate_limits:
            self.rate_limits[action] = {'last_reset': current_time, 'count': 0, 'limit': 5, 'period': 10, 'retries': 0}
        
        stats = self.rate_limits[action]

        # Resetar o contador se o período de tempo passou
        if current_time - stats['last_reset'] > stats['period']:
            stats['count'] = 0
            stats['last_reset'] = current_time
            stats['retries'] = 0

        # Incrementar o contador
        stats['count'] += 1

        # Verificar se o limite foi excedido
        if stats['count'] > stats['limit']:
            stats['retries'] += 1
            if stats['retries'] > 3: # Limite de retries antes de parar
                logger.warning(f"Rate limit para '{action}' excedido e limite de retries atingido. Parando.")
                return -1 # Sinaliza para parar a operação
            
            wait_time = stats['period'] - (current_time - stats['last_reset']) + 1 # Espera até o próximo período
            logger.warning(f"Rate limit para '{action}' excedido. Esperando {wait_time:.2f} segundos.")
            return int(wait_time)
        return 0 # Não há rate limit ou não excedido

# Criar instância do bot antes de definir os eventos
bot = InactivityBot()

# Importar e configurar as tarefas agendadas
from tasks import setup_tasks
setup_tasks()

# Eventos do bot
@bot.event
async def on_ready():
    logger.info(f'Bot conectado como {bot.user}')
    await bot.log_action(f"Bot iniciado em {len(bot.guilds)} servidores")

@bot.event
async def on_voice_state_update(member, before, after):
    try:
        # Adiciona o evento à fila de voz para processamento assíncrono
        await bot.queues['voice'].put((member, before, after))
    except asyncio.QueueFull:
        logger.warning("Fila de eventos de voz cheia. Descartando evento para evitar sobrecarga.")
    except Exception as e:
        logger.error(f"Erro ao enfileirar evento de voz: {e}")

# Inicialização do bot
async def main():
    load_dotenv()
    try:
        await bot.start(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.error(f"Erro ao iniciar bot: {e}")
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())