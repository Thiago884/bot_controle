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
import mysql.connector
from discord.ext import tasks

# Configuração do logger
def setup_logger():
    logger = logging.getLogger('inactivity_bot')
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
    "notification_channel": None,
    "timezone": "America/Sao_Paulo",
    "absence_channel": 1187657181194113045,
    "allowed_roles": [],  # IDs dos cargos que podem usar os comandos
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
        self.load_config()
        self.timezone = pytz.timezone(self.config.get('timezone', 'America/Sao_Paulo'))
        self.db = None
        self.initialize_db()
        self.active_sessions = {}
        self.voice_event_queue = asyncio.Queue()
        self.voice_event_processor_task = None

    def initialize_db(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                from database import Database
                self.db = Database()
                break
            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = DEFAULT_CONFIG
                self.save_config()
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            self.config = DEFAULT_CONFIG

    def save_config(self):
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")

    async def log_action(self, action: str, member: Optional[discord.Member] = None, details: str = None):
        try:
            channel = self.get_channel(self.config.get('log_channel', 1376013013206827161))
            if channel:
                embed = discord.Embed(
                    title=f"Ação: {action}",
                    color=discord.Color.orange(),
                    timestamp=datetime.now(self.timezone)
                )
                
                if member is not None:
                    embed.description = f"Usuário: {member.mention}"
                else:
                    embed.description = "Ação do sistema"
                
                if details:
                    embed.add_field(name="Detalhes", value=details, inline=False)
                
                await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Erro ao registrar ação no log: {e}")

    async def notify_roles(self, message: str):
        try:
            channel = self.get_channel(self.config.get('notification_channel'))
            if channel:
                await channel.send(message)
        except Exception as e:
            logger.error(f"Erro ao enviar notificação de cargos: {e}")

    async def send_dm(self, member: discord.Member, message: str):
        try:
            await member.send(message)
        except discord.Forbidden:
            await self.log_action("DM Falhou", member, "Não foi possível enviar mensagem direta para o usuário.")
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
            
            await self.send_dm(member, message)
            await self.db.log_warning(member.id, member.guild.id, warning_type)
            await self.log_action(f"Aviso Enviado ({warning_type})", member)
        except Exception as e:
            logger.error(f"Erro ao enviar aviso para {member}: {e}")

    async def process_voice_events(self):
        while True:
            try:
                event = await self.voice_event_queue.get()
                event_type, member, before, after = event
                
                if member.id in self.config['whitelist']['users'] or \
                   any(role.id in self.config['whitelist']['roles'] for role in member.roles):
                    self.voice_event_queue.task_done()
                    continue
                    
                absence_channel_id = self.config.get('absence_channel')
                
                # Entrou em um canal de voz (não estava em nenhum antes)
                if before.channel is None and after.channel is not None:
                    # Se entrou no canal de ausência, não registramos
                    if after.channel.id == absence_channel_id:
                        self.voice_event_queue.task_done()
                        continue
                        
                    try:
                        await self.db.log_voice_join(member.id, member.guild.id)
                        self.active_sessions[(member.id, member.guild.id)] = datetime.utcnow()
                        await self.log_action("Entrou em voz", member, f"Canal: {after.channel.name}")
                    except mysql.connector.errors.PoolError:
                        logger.warning("Pool esgotado, criando conexão temporária")
                        temp_conn = None
                        try:
                            temp_conn = mysql.connector.connect(
                                host=os.getenv('DB_HOST'),
                                database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                port=int(os.getenv('DB_PORT', 3306))
                            )
                            cursor = temp_conn.cursor()
                            cursor.execute('''
                            INSERT INTO user_activity 
                            (user_id, guild_id, last_voice_join, voice_sessions) 
                            VALUES (%s, %s, %s, 1)
                            ON DUPLICATE KEY UPDATE 
                                last_voice_join = VALUES(last_voice_join),
                                voice_sessions = voice_sessions + 1
                            ''', (member.id, member.guild.id, datetime.utcnow()))
                            temp_conn.commit()
                            self.active_sessions[(member.id, member.guild.id)] = datetime.utcnow()
                            await self.log_action("Entrou em voz", member, f"Canal: {after.channel.name}")
                        except Exception as e:
                            logger.error(f"Erro ao registrar entrada em voz (temp connection): {e}")
                            await self.log_action("Erro DB - Entrada em voz", member, str(e))
                        finally:
                            if temp_conn and temp_conn.is_connected():
                                temp_conn.close()
                    except Exception as e:
                        logger.error(f"Erro ao registrar entrada em voz: {e}")
                        await self.log_action("Erro DB - Entrada em voz", member, str(e))
                
                # Saiu de um canal de voz (não entrou em outro)
                elif before.channel is not None and after.channel is None:
                    session_start = self.active_sessions.get((member.id, member.guild.id))
                    if session_start:
                        # Se estava no canal de ausência, apenas remove da sessão sem contar o tempo
                        if before.channel.id == absence_channel_id:
                            del self.active_sessions[(member.id, member.guild.id)]
                            self.voice_event_queue.task_done()
                            continue
                            
                        duration = (datetime.utcnow() - session_start).total_seconds()
                        if duration >= self.config['required_minutes'] * 60:
                            try:
                                await self.db.log_voice_leave(member.id, member.guild.id, int(duration))
                            except mysql.connector.errors.PoolError:
                                logger.warning("Pool esgotado, criando conexão temporária")
                                temp_conn = None
                                try:
                                    temp_conn = mysql.connector.connect(
                                        host=os.getenv('DB_HOST'),
                                        database=os.getenv('DB_NAME'),
                                        user=os.getenv('DB_USER'),
                                        password=os.getenv('DB_PASS'),
                                        port=int(os.getenv('DB_PORT', 3306))
                                    )
                                    cursor = temp_conn.cursor()
                                    
                                    # Atualizar user_activity
                                    cursor.execute('''
                                    UPDATE user_activity 
                                    SET last_voice_leave = %s,
                                        total_voice_time = total_voice_time + %s
                                    WHERE user_id = %s AND guild_id = %s
                                    ''', (datetime.utcnow(), int(duration), member.id, member.guild.id))
                                    
                                    # Inserir voice_sessions
                                    join_time = datetime.utcnow() - timedelta(seconds=duration)
                                    cursor.execute('''
                                    INSERT INTO voice_sessions
                                    (user_id, guild_id, join_time, leave_time, duration)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ''', (member.id, member.guild.id, join_time, datetime.utcnow(), int(duration)))
                                    
                                    temp_conn.commit()
                                except Exception as e:
                                    logger.error(f"Erro ao registrar saída de voz (temp connection): {e}")
                                    await self.log_action("Erro DB - Saída de voz", member, str(e))
                                finally:
                                    if temp_conn and temp_conn.is_connected():
                                        temp_conn.close()
                            except Exception as e:
                                logger.error(f"Erro ao registrar saída de voz: {e}")
                                await self.log_action("Erro DB - Saída de voz", member, str(e))
                        del self.active_sessions[(member.id, member.guild.id)]
                        await self.log_action("Saiu de voz", member, 
                                           f"Canal: {before.channel.name} | Duração: {int(duration//60)} minutos")
                
                # Movido de um canal para outro
                elif before.channel is not None and after.channel is not None:
                    # Saiu do canal normal para o canal de ausência
                    if after.channel.id == absence_channel_id:
                        session_start = self.active_sessions.get((member.id, member.guild.id))
                        if session_start:
                            duration = (datetime.utcnow() - session_start).total_seconds()
                            if duration >= self.config['required_minutes'] * 60:
                                try:
                                    await self.db.log_voice_leave(member.id, member.guild.id, int(duration))
                                except Exception as e:
                                    logger.error(f"Erro ao registrar saída de voz (movido para ausência): {e}")
                            del self.active_sessions[(member.id, member.guild.id)]
                            await self.log_action("Movido para ausência", member, 
                                               f"De: {before.channel.name} | Duração: {int(duration//60)} minutos")
                    
                    # Saiu do canal de ausência para um canal normal
                    elif before.channel.id == absence_channel_id:
                        try:
                            await self.db.log_voice_join(member.id, member.guild.id)
                            self.active_sessions[(member.id, member.guild.id)] = datetime.utcnow()
                            await self.log_action("Retornou de ausência", member, f"Para: {after.channel.name}")
                        except Exception as e:
                            logger.error(f"Erro ao registrar retorno de ausência: {e}")
                
                self.voice_event_queue.task_done()
            except Exception as e:
                logger.error(f"Erro no processador de eventos de voz: {e}")
                await asyncio.sleep(1)  # Previne loop rápido em caso de erro

# Decorador para verificar cargos permitidos
def allowed_roles_only():
    async def predicate(interaction: discord.Interaction):
        # Se não houver cargos definidos, qualquer um pode usar
        if not bot.config['allowed_roles']:
            return True
            
        # Administradores podem sempre usar
        if interaction.user.guild_permissions.administrator:
            return True
            
        # Verifica se o usuário tem algum dos cargos permitidos
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

bot = InactivityBot(command_prefix='!', intents=intents)

@bot.event
async def on_ready():
    logger.info(f'Bot conectado como {bot.user}')
    try:
        synced = await bot.tree.sync()
        logger.info(f"Comandos slash sincronizados: {len(synced)} comandos")
    except Exception as e:
        logger.error(f"Erro ao sincronizar comandos slash: {e}")
    
    # Importar e iniciar tarefas
    from tasks import inactivity_check, cleanup_members, check_warnings, database_backup, cleanup_old_data
    inactivity_check.start()
    cleanup_members.start()
    check_warnings.start()
    database_backup.start()
    cleanup_old_data.start()
    
    # Iniciar processador de eventos de voz
    bot.voice_event_processor_task = asyncio.create_task(bot.process_voice_events())
    
    await bot.notify_roles("🤖 Bot de Inatividade iniciado com sucesso!")

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    try:
        # Adiciona o evento à fila para processamento assíncrono
        await bot.voice_event_queue.put(('voice_state_update', member, before, after))
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
    
    # Configuração para Render
    if os.getenv('RENDER', 'false').lower() == 'true':
        logger.info("Executando no Render - Configurações especiais aplicadas")
        from web_panel import keep_alive
        keep_alive()
    
    try:
        bot.run(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.critical(f"Erro ao iniciar o bot: {e}")
        raise