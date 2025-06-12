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
        self._setup_tasks()

    def _setup_tasks(self):
        """Configura as tarefas em segundo plano"""
        self.tasks = {
            'voice_processor': asyncio.create_task(self._process_voice_events()),
            'command_processor': asyncio.create_task(self._process_commands()),
            'queue_processor': asyncio.create_task(self._process_queues()),
            'audio_checker': asyncio.create_task(self._check_audio_states())
        }

    async def setup_hook(self):
        """Configuração inicial do bot"""
        self.load_config()
        await self._initialize_db()
        await self.tree.sync()
        logger.info("Bot inicializado com sucesso")

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

    async def _log_action(self, message):
        """Registra uma ação no log"""
        channel = self.get_channel(self.config['log_channel'])
        if channel:
            await self.queues['high'].put((channel, None, discord.Embed(description=message), None))

# Criar instância do bot antes de definir os eventos
bot = InactivityBot()

# Eventos do bot
@bot.event
async def on_ready():
    logger.info(f'Bot conectado como {bot.user}')
    await bot.log_action(f"Bot iniciado em {len(bot.guilds)} servidores")

@bot.event
async def on_voice_state_update(member, before, after):
    try:
        await bot.queues['voice'].put((member, before, after))
    except asyncio.QueueFull:
        logger.warning("Fila de eventos de voz cheia")

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