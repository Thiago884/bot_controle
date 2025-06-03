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
        self.timezone = pytz.timezone('America/Sao_Paulo')  # Valor padrão inicial
        self.db = None
        self.active_sessions = {}
        self.voice_event_queue = asyncio.Queue()
        self.command_queue = asyncio.Queue()
        self.voice_event_processor_task = None
        self.command_processor_task = None
        self.rate_limited = False
        self.last_rate_limit = None
        self.rate_limit_delay = 1.0
        self.max_rate_limit_delay = 5.0
        self.db_backup = None
        self.pool_monitor_task = None

    async def setup_hook(self):
        """Configuração assíncrona que é executada após o login mas antes de conectar ao websocket"""
        # Carrega a configuração primeiro
        self.load_config()
        
        # Inicializa o banco de dados após carregar a configuração
        await self.initialize_db()
        
        # Inicializa o backup do banco de dados
        from database import DatabaseBackup
        self.db_backup = DatabaseBackup(self.db)
        
        # Sincroniza os comandos slash
        try:
            synced = await self.tree.sync()
            logger.info(f"Comandos slash sincronizados: {len(synced)} comandos")
        except Exception as e:
            logger.error(f"Erro ao sincronizar comandos slash: {e}")
        
        # Importa e inicia as tarefas
        from tasks import inactivity_check, cleanup_members, check_warnings, cleanup_old_data
        inactivity_check.start()
        cleanup_members.start()
        check_warnings.start()
        cleanup_old_data.start()
        
        # Inicia a tarefa de backup do banco de dados
        self.start_backup_task()
        
        # Inicia os processadores
        self.voice_event_processor_task = asyncio.create_task(self.process_voice_events())
        self.command_processor_task = asyncio.create_task(self.process_commands_queue())
        
        # Inicia o monitoramento do pool
        self.pool_monitor_task = asyncio.create_task(self.monitor_db_pool())

    async def initialize_db(self):
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                from database import Database
                self.db = Database()
                await self.db.initialize()
                logger.info("Banco de dados inicializado com sucesso")
                break
            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if attempt == max_retries - 1:
                    raise
                sleep_time = initial_delay * (2 ** attempt)
                logger.info(f"Tentando novamente em {sleep_time} segundos...")
                await asyncio.sleep(sleep_time)

    def load_config(self):
        try:
            # Tenta carregar do arquivo local primeiro
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    self.config = json.load(f)
                logger.info("Configuração carregada do arquivo local")
            else:
                self.config = DEFAULT_CONFIG
                logger.info("Usando configuração padrão")
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(self.config, f, indent=4)
            
            # Atualiza o timezone com a configuração carregada
            self.timezone = pytz.timezone(self.config.get('timezone', 'America/Sao_Paulo'))
            
            # Garante que o canal de notificações padrão está definido
            if 'notification_channel' not in self.config or not self.config['notification_channel']:
                self.config['notification_channel'] = 1224897652060196996
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(self.config, f, indent=4)
                
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            self.config = DEFAULT_CONFIG
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)

    async def save_config(self):
        try:
            # Salva no arquivo local
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            # Tenta salvar no banco de dados se estiver disponível
            if hasattr(self, 'db') and self.db:
                try:
                    await self.db.save_config(0, self.config)  # Usamos 0 para configuração global
                    logger.info("Configuração salva no banco de dados com sucesso")
                except Exception as db_error:
                    logger.error(f"Erro ao salvar configuração no banco de dados: {db_error}")
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            raise

    def start_backup_task(self):
        @tasks.loop(hours=24)
        async def database_backup():
            await self.wait_until_ready()
            if hasattr(self, 'db_backup'):
                success = await self.db_backup.create_backup()
                if success:
                    await self.log_action("Backup do Banco de Dados", None, "Backup diário realizado com sucesso")
        
        database_backup.start()

    async def monitor_db_pool(self):
        while True:
            try:
                if hasattr(self, 'db') and self.db:
                    connected, running = await self.db.check_pool_status()
                    if connected is not None:
                        log_with_context(
                            f"Status do pool de conexões: {connected} conectadas, {running} rodando",
                            logging.INFO
                        )
                
                await asyncio.sleep(3600)  # Verifica a cada hora
            except Exception as e:
                log_with_context(f"Erro no monitoramento do pool: {e}", logging.ERROR)
                await asyncio.sleep(60)  # Espera 1 minuto antes de tentar novamente em caso de erro

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
                
                try:
                    await channel.send(embed=embed)
                except discord.errors.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        retry_after = e.response.headers.get('Retry-After', 60)
                        logger.warning(f"Rate limit ao enviar log. Tentando novamente em {retry_after} segundos")
                        await asyncio.sleep(float(retry_after))
                        await channel.send(embed=embed)
                    else:
                        raise
        except Exception as e:
            logger.error(f"Erro ao registrar ação no log: {e}")

    async def notify_roles(self, message: str, is_warning: bool = False):
        try:
            if is_warning:
                # Para avisos de risco, usa o canal de notificações
                channel = self.get_channel(self.config.get('notification_channel'))
                if channel:
                    try:
                        await channel.send(message)
                    except discord.errors.HTTPException as e:
                        if e.status == 429:  # Rate limited
                            retry_after = e.response.headers.get('Retry-After', 60)
                            logger.warning(f"Rate limit ao enviar notificação. Tentando novamente em {retry_after} segundos")
                            await asyncio.sleep(float(retry_after))
                            await channel.send(message)
                        else:
                            raise
            else:
                # Para outras notificações, usa o canal de logs
                await self.log_action("Notificação", None, message)
        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")

    async def send_dm(self, member: discord.Member, message: str):
        try:
            await member.send(message)
        except discord.Forbidden:
            await self.log_action("DM Falhou", member, "Não foi possível enviar mensagem direta para o usuário.")
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = e.response.headers.get('Retry-After', 60)
                logger.warning(f"Rate limit ao enviar DM. Tentando novamente em {retry_after} segundos")
                await asyncio.sleep(float(retry_after))
                await member.send(message)
            else:
                raise
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

    async def process_commands_queue(self):
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
                
                # Adiciona delay antes de processar cada comando
                await asyncio.sleep(self.rate_limit_delay)
                
                try:
                    await command(interaction, *args, **kwargs)
                except discord.errors.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        self.rate_limited = True
                        self.last_rate_limit = datetime.now()
                        retry_after = e.response.headers.get('Retry-After', 60)
                        logger.error(f"Rate limit atingido. Tentar novamente após {retry_after} segundos")
                        
                        # Aumenta o delay para evitar novos rate limits (backoff exponencial)
                        self.rate_limit_delay = min(self.rate_limit_delay * 2, self.max_rate_limit_delay)
                        
                        await asyncio.sleep(float(retry_after))
                        self.rate_limited = False
                        
                        # Reenfileira o comando com um delay maior
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
                else:
                    # Se não houve erro, reduz gradualmente o delay (backoff decrescente)
                    self.rate_limit_delay = max(self.rate_limit_delay * 0.9, 0.5)  # Mínimo de 0.5 segundos
                
                self.command_queue.task_done()
            except Exception as e:
                logger.error(f"Erro no processador de comandos: {e}")
                await asyncio.sleep(1)

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

bot = InactivityBot(
    command_prefix='!', 
    intents=intents,
    max_messages=None,  # Reduzir cache de mensagens
    chunk_guilds_at_startup=False,  # Não carregar todos os membros no startup
    member_cache_flags=discord.MemberCacheFlags.none(),  # Minimizar cache de membros
    enable_debug_events=False,
    heartbeat_timeout=60.0
)

@bot.event
async def on_ready():
    logger.info(f'Bot conectado como {bot.user}')
    await bot.log_action("Inicialização", None, "🤖 Bot de Controle de Atividades iniciado com sucesso!")

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
    finally:
        # Garante que o pool de conexões é fechado corretamente
        if hasattr(bot, 'db') and bot.db:
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                     bot.db.close()
            except Exception as e:
                logger.error(f"Erro ao fechar pool de conexões: {e}")