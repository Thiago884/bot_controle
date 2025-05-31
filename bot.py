import discord
from discord.ext import commands, tasks
from datetime import datetime, timedelta
import pytz
import asyncio
import json
import os
import time
import mysql.connector
from mysql.connector import Error
from mysql.connector.errors import OperationalError, InterfaceError
from typing import Dict, List, Optional, Tuple, Union
from flask import Flask, jsonify, render_template, request
from threading import Thread
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import glob
import zipfile

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
    "absence_channel": 1187657181194113045,  # ID do canal de ausência
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

# Configuração do Flask para painel web
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"status": "ok", "message": "Bot is running"})

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html', 
                         bot_name=bot.user.name if bot.user else "Inactivity Bot",
                         guild_count=len(bot.guilds))

@app.route('/api/guilds')
def get_guilds():
    guilds = []
    for guild in bot.guilds:
        guilds.append({
            'id': guild.id,
            'name': guild.name,
            'member_count': guild.member_count,
            'icon': guild.icon.url if guild.icon else None
        })
    return jsonify(guilds)

@app.route('/api/guild/<int:guild_id>')
def get_guild_info(guild_id):
    guild = bot.get_guild(guild_id)
    if not guild:
        return jsonify({'error': 'Guild not found'}), 404
    
    tracked_roles = []
    for role_id in bot.config['tracked_roles']:
        role = guild.get_role(role_id)
        if role:
            tracked_roles.append({
                'id': role.id,
                'name': role.name,
                'color': str(role.color),
                'member_count': len(role.members)
            })
    
    return jsonify({
        'id': guild.id,
        'name': guild.name,
        'tracked_roles': tracked_roles,
        'config': {
            'required_minutes': bot.config['required_minutes'],
            'required_days': bot.config['required_days'],
            'monitoring_period': bot.config['monitoring_period'],
            'kick_after_days': bot.config['kick_after_days']
        }
    })

@app.route('/api/update_config', methods=['POST'])
def update_config():
    try:
        data = request.json
        if 'required_minutes' in data:
            bot.config['required_minutes'] = int(data['required_minutes'])
        if 'required_days' in data:
            bot.config['required_days'] = int(data['required_days'])
        if 'monitoring_period' in data:
            bot.config['monitoring_period'] = int(data['monitoring_period'])
        if 'kick_after_days' in data:
            bot.config['kick_after_days'] = int(data['kick_after_days'])
        
        bot.save_config()
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

def run_flask():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

class DatabaseBackup:
    def __init__(self, db):
        self.db = db
        self.backup_dir = 'backups'
        os.makedirs(self.backup_dir, exist_ok=True)

    async def create_backup(self):
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self.backup_dir, f'backup_{timestamp}.sql')
            
            cursor = None
            try:
                cursor = self.db.connection.cursor()
                
                # Get all tables
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
                
                with open(backup_file, 'w') as f:
                    for table in tables:
                        # Write table structure
                        cursor.execute(f"SHOW CREATE TABLE {table}")
                        create_table = cursor.fetchone()[1]
                        f.write(f"{create_table};\n\n")
                        
                        # Write table data
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        if rows:
                            columns = [col[0] for col in cursor.description]
                            f.write(f"INSERT INTO `{table}` (`{'`,`'.join(columns)}`) VALUES\n")
                            
                            for i, row in enumerate(rows):
                                values = []
                                for value in row:
                                    if value is None:
                                        values.append("NULL")
                                    elif isinstance(value, (int, float)):
                                        values.append(str(value))
                                    else:
                                        values.append("'" + str(value).replace("'", "''") + "'")
                                
                                f.write(f"({','.join(values)})")
                                if i < len(rows) - 1:
                                    f.write(",\n")
                                else:
                                    f.write(";\n\n")
                
                # Compress the backup
                with zipfile.ZipFile(f'{backup_file}.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(backup_file, os.path.basename(backup_file))
                
                os.remove(backup_file)
                
                # Clean up old backups (keep last 7)
                backups = sorted(glob.glob(os.path.join(self.backup_dir, '*.zip')))
                for old_backup in backups[:-7]:
                    os.remove(old_backup)
                
                logger.info(f"Backup criado com sucesso: {backup_file}.zip")
                return True
            finally:
                if cursor:
                    cursor.close()
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}")
            return False

class Database:
    def __init__(self):
        self.connection = None
        self.connect(retries=3, initial_delay=5)
        self.create_tables()

    def connect(self, retries=3, initial_delay=5):
        for attempt in range(retries):
            try:
                self.connection = mysql.connector.connect(
                    host=os.getenv('DB_HOST'),
                    database=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASS'),
                    port=int(os.getenv('DB_PORT', 3306)),
                    pool_name="bot_pool",
                    pool_size=20,  # Aumentado de 10 para 20
                    pool_reset_session=True,
                    connect_timeout=30,
                    connection_timeout=30
                )
                if self.connection.is_connected():
                    logger.info("Conexão ao MySQL estabelecida com sucesso")
                    return
            except Error as e:
                logger.error(f"Erro ao conectar ao MySQL (tentativa {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    sleep_time = initial_delay * (2 ** attempt)
                    logger.info(f"Tentando novamente em {sleep_time} segundos...")
                    time.sleep(sleep_time)
                else:
                    raise

    @staticmethod
    def ensure_connection(func):
        async def wrapper(self, *args, **kwargs):
            max_retries = 3
            cursor = None
            for attempt in range(max_retries):
                try:
                    if not self.connection or not self.connection.is_connected():
                        self.connect()
                    cursor = self.connection.cursor(dictionary=True)
                    result = await func(self, *args, **kwargs, cursor=cursor)
                    return result
                except (OperationalError, InterfaceError) as e:
                    logger.error(f"Erro de conexão (tentativa {attempt + 1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)
                    self.connect()
                except Exception as e:
                    logger.error(f"Erro inesperado: {e}")
                    raise
                finally:
                    if cursor:
                        cursor.close()
        return wrapper

    def reconnect(self):
        try:
            self.connection.reconnect(attempts=3, delay=1)
        except Exception as e:
            logger.error(f"Falha ao reconectar: {e}")
            self.connect()

    def create_tables(self):
        cursor = None
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_activity (
                user_id BIGINT,
                guild_id BIGINT,
                last_voice_join DATETIME,
                last_voice_leave DATETIME,
                voice_sessions INT DEFAULT 0,
                total_voice_time INT DEFAULT 0,
                PRIMARY KEY (user_id, guild_id),
                INDEX idx_guild_user (guild_id, user_id),
                INDEX idx_last_join (last_voice_join),
                INDEX idx_last_leave (last_voice_leave)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS voice_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                guild_id BIGINT,
                join_time DATETIME,
                leave_time DATETIME,
                duration INT,
                INDEX idx_user_guild (user_id, guild_id),
                INDEX idx_join_time (join_time),
                INDEX idx_leave_time (leave_time),
                INDEX idx_user_guild_time (user_id, guild_id, join_time, leave_time)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_warnings (
                user_id BIGINT,
                guild_id BIGINT,
                warning_type VARCHAR(20),
                warning_date DATETIME,
                PRIMARY KEY (user_id, guild_id, warning_type),
                INDEX idx_warning_date (warning_date),
                INDEX idx_user_guild_warning (user_id, guild_id, warning_type, warning_date)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS removed_roles (
                user_id BIGINT,
                guild_id BIGINT,
                role_id BIGINT,
                removal_date DATETIME,
                PRIMARY KEY (user_id, guild_id, role_id),
                INDEX idx_removal_date (removal_date)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS kicked_members (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                guild_id BIGINT,
                kick_date DATETIME,
                reason TEXT,
                INDEX idx_user_guild (user_id, guild_id),
                INDEX idx_kick_date (kick_date)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS checked_periods (
                user_id BIGINT,
                guild_id BIGINT,
                period_start DATETIME,
                period_end DATETIME,
                meets_requirements BOOLEAN,
                PRIMARY KEY (user_id, guild_id, period_start),
                INDEX idx_period_end (period_end),
                INDEX idx_requirements (meets_requirements),
                INDEX idx_user_guild_period (user_id, guild_id, period_start, period_end)
            )''')
            
            self.connection.commit()
            logger.info("Tabelas criadas/verificadas com sucesso")
        except Error as e:
            logger.error(f"Erro ao criar tabelas: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    async def log_voice_join(self, user_id: int, guild_id: int, cursor=None):
        now = datetime.utcnow()
        
        try:
            cursor.execute('''
            INSERT INTO user_activity 
            (user_id, guild_id, last_voice_join, voice_sessions) 
            VALUES (%s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                last_voice_join = VALUES(last_voice_join),
                voice_sessions = voice_sessions + 1
            ''', (user_id, guild_id, now))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar entrada em voz: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def log_voice_leave(self, user_id: int, guild_id: int, duration: int, cursor=None):
        now = datetime.utcnow()
        
        try:
            cursor.execute('''
            UPDATE user_activity 
            SET last_voice_leave = %s,
                total_voice_time = total_voice_time + %s
            WHERE user_id = %s AND guild_id = %s
            ''', (now, duration, user_id, guild_id))
            
            join_time = now - timedelta(seconds=duration)
            cursor.execute('''
            INSERT INTO voice_sessions
            (user_id, guild_id, join_time, leave_time, duration)
            VALUES (%s, %s, %s, %s, %s)
            ''', (user_id, guild_id, join_time, now, duration))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar saída de voz: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def get_user_activity(self, user_id: int, guild_id: int, cursor=None) -> Dict:
        try:
            cursor.execute('''
            SELECT last_voice_join, last_voice_leave, voice_sessions, total_voice_time 
            FROM user_activity 
            WHERE user_id = %s AND guild_id = %s
            ''', (user_id, guild_id))
            
            result = cursor.fetchone()
            return result if result else {}
        except Error as e:
            logger.error(f"Erro ao obter atividade do usuário: {e}")
            return {}

    @ensure_connection
    async def get_voice_sessions(self, user_id: int, guild_id: int, start_date: datetime, end_date: datetime, cursor=None) -> List[Dict]:
        try:
            cursor.execute('''
            SELECT join_time, leave_time, duration 
            FROM voice_sessions
            WHERE user_id = %s AND guild_id = %s
            AND join_time >= %s AND leave_time <= %s
            ORDER BY join_time
            ''', (user_id, guild_id, start_date, end_date))
            
            return cursor.fetchall()
        except Error as e:
            logger.error(f"Erro ao obter sessões de voz: {e}")
            return []

    @ensure_connection
    async def log_period_check(self, user_id: int, guild_id: int, start_date: datetime, end_date: datetime, meets_requirements: bool, cursor=None):
        try:
            cursor.execute('''
            INSERT INTO checked_periods
            (user_id, guild_id, period_start, period_end, meets_requirements)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                meets_requirements = VALUES(meets_requirements)
            ''', (user_id, guild_id, start_date, end_date, meets_requirements))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar verificação de período: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def get_last_period_check(self, user_id: int, guild_id: int, cursor=None) -> Optional[Dict]:
        try:
            cursor.execute('''
            SELECT period_start, period_end, meets_requirements
            FROM checked_periods
            WHERE user_id = %s AND guild_id = %s
            ORDER BY period_start DESC
            LIMIT 1
            ''', (user_id, guild_id))
            
            return cursor.fetchone()
        except Error as e:
            logger.error(f"Erro ao obter última verificação de período: {e}")
            return None

    @ensure_connection
    async def log_warning(self, user_id: int, guild_id: int, warning_type: str, cursor=None):
        now = datetime.utcnow()
        
        try:
            cursor.execute('''
            INSERT INTO user_warnings 
            (user_id, guild_id, warning_type, warning_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                warning_date = VALUES(warning_date)
            ''', (user_id, guild_id, warning_type, now))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar aviso: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def get_last_warning(self, user_id: int, guild_id: int, cursor=None) -> Optional[Tuple[str, datetime]]:
        try:
            cursor.execute('''
            SELECT warning_type, warning_date 
            FROM user_warnings 
            WHERE user_id = %s AND guild_id = %s
            ORDER BY warning_date DESC
            LIMIT 1
            ''', (user_id, guild_id))
            
            result = cursor.fetchone()
            if result:
                return result['warning_type'], result['warning_date']
            return None
        except Error as e:
            logger.error(f"Erro ao obter último aviso: {e}")
            return None

    @ensure_connection
    async def log_removed_roles(self, user_id: int, guild_id: int, role_ids: List[int], cursor=None):
        now = datetime.utcnow()
        
        try:
            for role_id in role_ids:
                cursor.execute('''
                INSERT INTO removed_roles 
                (user_id, guild_id, role_id, removal_date) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    removal_date = VALUES(removal_date)
                ''', (user_id, guild_id, role_id, now))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar cargos removidos: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def log_kicked_member(self, user_id: int, guild_id: int, reason: str, cursor=None):
        now = datetime.utcnow()
        
        try:
            cursor.execute('''
            INSERT INTO kicked_members 
            (user_id, guild_id, kick_date, reason) 
            VALUES (%s, %s, %s, %s)
            ''', (user_id, guild_id, now, reason))
            
            self.connection.commit()
        except Error as e:
            logger.error(f"Erro ao registrar membro expulso: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    @ensure_connection
    async def log_pool_status(self, cursor=None):
        try:
            logger.info(f"Pool status: {self.connection.pool_size} connections, {self.connection.pool_name}")
        except Exception as e:
            logger.error(f"Erro ao verificar status do pool: {e}")

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
                self.db = Database()
                break
            except Error as e:
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
    
    # Iniciar todas as tarefas
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

@tasks.loop(hours=24)
async def inactivity_check():
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    
    if not tracked_roles:
        return
    
    for guild in bot.guilds:
        for member in guild.members:
            if member.id in bot.config['whitelist']['users'] or \
               any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                continue
                
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = await bot.db.get_last_period_check(member.id, guild.id)
                    now = datetime.now(bot.timezone)
                    
                    if last_check:
                        last_period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                        if now < last_period_end:
                            continue
                    
                    period_end = now
                    period_start = period_end - timedelta(days=monitoring_period)
                    
                    sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
                    
                    meets_requirements = False
                    if sessions:
                        valid_days = set()
                        for session in sessions:
                            if session['duration'] >= required_minutes * 60:
                                day = session['join_time'].replace(tzinfo=bot.timezone).date()
                                valid_days.add(day)
                        
                        meets_requirements = len(valid_days) >= required_days
                    
                    await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
                    
                    if not meets_requirements:
                        roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                        if roles_to_remove:
                            try:
                                await member.remove_roles(*roles_to_remove)
                                await bot.send_warning(member, 'final')
                                await bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
                                await bot.log_action(
                                    "Cargo Removido",
                                    member,
                                    f"Cargos removidos: {', '.join([r.name for r in roles_to_remove])}")
                                await bot.notify_roles(
                                    f"🚨 Cargos removidos de {member.mention} por inatividade: " +
                                    ", ".join([f"`{r.name}`" for r in roles_to_remove]))
                            except discord.Forbidden:
                                await bot.log_action("Erro ao Remover Cargo", member, "Permissões insuficientes")
                            except Exception as e:
                                logger.error(f"Erro ao remover cargos de {member}: {e}")
                except Exception as e:
                    logger.error(f"Erro ao verificar inatividade para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def check_warnings():
    await bot.wait_until_ready()
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    monitoring_period = bot.config['monitoring_period']
    tracked_roles = bot.config['tracked_roles']
    warnings_config = bot.config.get('warnings', {})
    
    if not tracked_roles or not warnings_config:
        return
    
    first_warning_days = warnings_config.get('first_warning', 3)
    second_warning_days = warnings_config.get('second_warning', 1)
    
    for guild in bot.guilds:
        for member in guild.members:
            if member.id in bot.config['whitelist']['users'] or \
               any(role.id in bot.config['whitelist']['roles'] for role in member.roles):
                continue
                
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = await bot.db.get_last_period_check(member.id, guild.id)
                    if not last_check:
                        continue
                    
                    period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                    days_remaining = (period_end - datetime.now(bot.timezone)).days
                    
                    last_warning = await bot.db.get_last_warning(member.id, guild.id)
                    
                    if days_remaining <= first_warning_days and (
                        not last_warning or last_warning[0] != 'first'):
                        await bot.send_warning(member, 'first')
                    
                    elif days_remaining <= second_warning_days and (
                        not last_warning or last_warning[0] != 'second'):
                        await bot.send_warning(member, 'second')
                except Exception as e:
                    logger.error(f"Erro ao verificar avisos para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def cleanup_members():
    await bot.wait_until_ready()
    
    kick_after_days = bot.config['kick_after_days']
    if kick_after_days <= 0:
        return
    
    cutoff_date = datetime.now(bot.timezone) - timedelta(days=kick_after_days)
    
    for guild in bot.guilds:
        for member in guild.members:
            try:
                if member.id in bot.config['whitelist']['users']:
                    continue
                    
                if len(member.roles) == 1:
                    joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
                    if joined_at and joined_at < cutoff_date:
                        try:
                            await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                            await bot.db.log_kicked_member(member.id, guild.id, f"Sem cargos há mais de {kick_after_days} dias")
                            await bot.log_action(
                                "Membro Expulso",
                                member,
                                f"Motivo: Sem cargos há mais de {kick_after_days} dias")
                            await bot.notify_roles(
                                f"👢 {member.mention} foi expulso por estar sem cargos há mais de {kick_after_days} dias")
                        except discord.Forbidden:
                            await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
                        except Exception as e:
                            logger.error(f"Erro ao expulsar membro {member}: {e}")
            except Exception as e:
                logger.error(f"Erro ao verificar membro para expulsão {member}: {e}")
                try:
                    bot.db.reconnect()
                except Exception as db_error:
                    logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@tasks.loop(hours=24)
async def database_backup():
    await bot.wait_until_ready()
    if not hasattr(bot, 'db_backup'):
        bot.db_backup = DatabaseBackup(bot.db)
    
    success = await bot.db_backup.create_backup()
    if success:
        await bot.log_action("Backup do Banco de Dados", None, "Backup diário realizado com sucesso")

@tasks.loop(hours=24)
async def cleanup_old_data():
    await bot.wait_until_ready()
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=60)  # 2 meses
        
        cursor = None
        try:
            cursor = bot.db.connection.cursor()
            
            # Limpar sessões de voz antigas
            cursor.execute("DELETE FROM voice_sessions WHERE leave_time < %s", (cutoff_date,))
            voice_deleted = cursor.rowcount
            
            # Limpar avisos antigos
            cursor.execute("DELETE FROM user_warnings WHERE warning_date < %s", (cutoff_date,))
            warnings_deleted = cursor.rowcount
            
            # Limpar registros de cargos removidos antigos
            cursor.execute("DELETE FROM removed_roles WHERE removal_date < %s", (cutoff_date,))
            roles_deleted = cursor.rowcount
            
            # Limpar membros expulsos antigos
            cursor.execute("DELETE FROM kicked_members WHERE kick_date < %s", (cutoff_date,))
            kicks_deleted = cursor.rowcount
            
            bot.db.connection.commit()
            
            log_message = (
                f"Limpeza de dados antigos concluída: "
                f"Sessões de voz: {voice_deleted}, "
                f"Avisos: {warnings_deleted}, "
                f"Cargos removidos: {roles_deleted}, "
                f"Expulsões: {kicks_deleted}"
            )
            logger.info(log_message)
            await bot.log_action("Limpeza de Dados", None, log_message)
        finally:
            if cursor:
                cursor.close()
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")

# Comandos Slash
@bot.tree.command(name="set_inactivity", description="Define o número de dias do período de monitoramento")
@commands.has_permissions(administrator=True)
async def set_inactivity(interaction: discord.Interaction, days: int):
    try:
        bot.config['monitoring_period'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Período de monitoramento definido para {days} dias.")
    except Exception as e:
        logger.error(f"Erro ao definir período de inatividade: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_requirements", description="Define os requisitos de atividade (minutos e dias)")
@commands.has_permissions(administrator=True)
async def set_requirements(interaction: discord.Interaction, minutes: int, days: int):
    try:
        bot.config['required_minutes'] = minutes
        bot.config['required_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Requisitos definidos para {minutes} minutos em {days} dias diferentes "
            f"dentro de {bot.config['monitoring_period']} dias.")
    except Exception as e:
        logger.error(f"Erro ao definir requisitos: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_kick_days", description="Define após quantos dias sem cargo o membro será expulso")
@commands.has_permissions(administrator=True)
async def set_kick_days(interaction: discord.Interaction, days: int):
    try:
        bot.config['kick_after_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Membros sem cargo serão expulsos após {days} dias.")
    except Exception as e:
        logger.error(f"Erro ao definir dias para expulsão: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="add_tracked_role", description="Adiciona um cargo à lista de cargos monitorados")
@commands.has_permissions(administrator=True)
async def add_tracked_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id not in bot.config['tracked_roles']:
            bot.config['tracked_roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} adicionado à lista de monitorados.")
            await bot.notify_roles(f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.")
        else:
            await interaction.response.send_message("Este cargo já está sendo monitorado.")
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo monitorado: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="remove_tracked_role", description="Remove um cargo da lista de cargos monitorados")
@commands.has_permissions(administrator=True)
async def remove_tracked_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id in bot.config['tracked_roles']:
            bot.config['tracked_roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} removido da lista de monitorados.")
            await bot.notify_roles(f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.")
        else:
            await interaction.response.send_message("Este cargo não estava sendo monitorado.")
    except Exception as e:
        logger.error(f"Erro ao remover cargo monitorado: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_notification_channel", description="Define o canal para notificações de cargos")
@commands.has_permissions(administrator=True)
async def set_notification_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    try:
        bot.config['notification_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de notificações definido para {channel.mention}")
        await channel.send("✅ Este canal foi definido como o canal de notificações de cargos!")
    except Exception as e:
        logger.error(f"Erro ao definir canal de notificações: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao definir o canal. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_warning_days", description="Define os dias para os avisos de inatividade")
@commands.has_permissions(administrator=True)
async def set_warning_days(interaction: discord.Interaction, first: int, second: int):
    try:
        if first <= second:
            return await interaction.response.send_message(
                "O primeiro aviso deve ser enviado antes do segundo aviso.")
        
        bot.config['warnings']['first_warning'] = first
        bot.config['warnings']['second_warning'] = second
        bot.save_config()
        await interaction.response.send_message(
            f"Avisos configurados: primeiro aviso {first} dias antes, segundo aviso {second} dia(s) antes.")
    except Exception as e:
        logger.error(f"Erro ao configurar dias de aviso: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao configurar os avisos. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_warning_message", description="Define a mensagem para um tipo de aviso")
@commands.has_permissions(administrator=True)
async def set_warning_message(interaction: discord.Interaction, warning_type: str, message: str):
    try:
        if warning_type not in ['first', 'second', 'final']:
            return await interaction.response.send_message(
                "Tipo de aviso inválido. Use 'first', 'second' ou 'final'.")
        
        bot.config['warnings']['messages'][warning_type] = message
        bot.save_config()
        await interaction.response.send_message(f"Mensagem de {warning_type} atualizada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao definir mensagem de aviso: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a mensagem. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_add_user", description="Adiciona um usuário à whitelist")
@commands.has_permissions(administrator=True)
async def whitelist_add_user(interaction: discord.Interaction, user: discord.User):
    try:
        if user.id not in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].append(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} adicionado à whitelist.")
        else:
            await interaction.response.send_message("Este usuário já está na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao adicionar usuário à whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o usuário. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_add_role", description="Adiciona um cargo à whitelist")
@commands.has_permissions(administrator=True)
async def whitelist_add_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id not in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} adicionado à whitelist.")
        else:
            await interaction.response.send_message("Este cargo já está na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo à whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_remove_user", description="Remove um usuário da whitelist")
@commands.has_permissions(administrator=True)
async def whitelist_remove_user(interaction: discord.Interaction, user: discord.User):
    try:
        if user.id in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].remove(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} removido da whitelist.")
        else:
            await interaction.response.send_message("Este usuário não estava na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao remover usuário da whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o usuário. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_remove_role", description="Remove um cargo da whitelist")
@commands.has_permissions(administrator=True)
async def whitelist_remove_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} removido da whitelist.")
        else:
            await interaction.response.send_message("Este cargo não estava na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao remover cargo da whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_absence_channel", description="Define o canal de voz de ausência")
@commands.has_permissions(administrator=True)
async def set_absence_channel(interaction: discord.Interaction, channel: discord.VoiceChannel):
    try:
        bot.config['absence_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de ausência definido para {channel.mention}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de ausência: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao definir o canal de ausência. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="show_config", description="Mostra a configuração atual do bot")
@commands.has_permissions(administrator=True)
async def show_config(interaction: discord.Interaction):
    try:
        config = bot.config
        tracked_roles = []
        for role_id in config['tracked_roles']:
            role = interaction.guild.get_role(role_id)
            if role:
                tracked_roles.append(role.name)
        
        whitelist_users = []
        for user_id in config['whitelist']['users']:
            user = interaction.guild.get_member(user_id)
            if user:
                whitelist_users.append(user.display_name)
        
        whitelist_roles = []
        for role_id in config['whitelist']['roles']:
            role = interaction.guild.get_role(role_id)
            if role:
                whitelist_roles.append(role.name)
        
        warnings_config = config.get('warnings', {})
        
        embed = discord.Embed(
            title="Configuração do Bot",
            color=discord.Color.blue())
        embed.add_field(
            name="Requisitos de Atividade",
            value=f"{config['required_minutes']} minutos em {config['required_days']} dias diferentes",
            inline=True)
        embed.add_field(
            name="Período de Monitoramento",
            value=f"{config['monitoring_period']} dias",
            inline=True)
        embed.add_field(
            name="Expulsão sem Cargo",
            value=f"{config['kick_after_days']} dias",
            inline=True)
        embed.add_field(
            name="Canal de Ausência",
            value=f"<#{config['absence_channel']}>" if config.get('absence_channel') else "Não definido",
            inline=True)
        embed.add_field(
            name="Cargos Monitorados",
            value="\n".join(tracked_roles) if tracked_roles else "Nenhum",
            inline=False)
        embed.add_field(
            name="Whitelist - Usuários",
            value="\n".join(whitelist_users) if whitelist_users else "Nenhum",
            inline=True)
        embed.add_field(
            name="Whitelist - Cargos",
            value="\n".join(whitelist_roles) if whitelist_roles else "Nenhum",
            inline=True)
        embed.add_field(
            name="Canal de Logs",
            value=f"<#{config['log_channel']}>",
            inline=True)
        embed.add_field(
            name="Canal de Notificações",
            value=f"<#{config['notification_channel']}>" if config['notification_channel'] else "Não definido",
            inline=True)
        embed.add_field(
            name="Fuso Horário",
            value=config['timezone'],
            inline=True)
        
        if warnings_config:
            embed.add_field(
                name="Configurações de Avisos",
                value=f"Primeiro aviso: {warnings_config.get('first_warning', 'N/A')} dias antes\n"
                      f"Segundo aviso: {warnings_config.get('second_warning', 'N/A')} dia(s) antes",
                inline=False)
        
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao mostrar configuração: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao mostrar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="check_user", description="Verifica a atividade de um usuário")
@commands.has_permissions(administrator=True)
async def check_user(interaction: discord.Interaction, member: discord.Member):
    try:
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        last_join = user_data.get('last_voice_join')
        sessions = user_data.get('voice_sessions', 0)
        total_time = user_data.get('total_voice_time', 0)
        last_warning = await bot.db.get_last_warning(member.id, member.guild.id)
        last_check = await bot.db.get_last_period_check(member.id, member.guild.id)
        
        embed = discord.Embed(
            title=f"Atividade de {member.display_name}",
            color=discord.Color.green())
        embed.add_field(
            name="Última entrada em voz",
            value=last_join.strftime("%d/%m/%Y %H:%M") if last_join else "Nunca",
            inline=True)
        embed.add_field(
            name="Sessões de voz",
            value=str(sessions),
            inline=True)
        embed.add_field(
            name="Tempo total em voz",
            value=f"{int(total_time//3600)}h {int((total_time%3600)//60)}m",
            inline=True)
        
        if last_check:
            period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
            days_remaining = (period_end - datetime.now(bot.timezone)).days
            embed.add_field(
                name="Período Atual",
                value=f"Termina em {days_remaining} dias",
                inline=False)
        
        if last_warning:
            warning_type, warning_date = last_warning
            embed.add_field(
                name="Último aviso recebido",
                value=f"{warning_type} em {warning_date.strftime('%d/%m/%Y %H:%M')}",
                inline=False)
        
        if last_join and last_check:
            meets_requirements = last_check['meets_requirements']
            status = "✅ Ativo (requisitos cumpridos)" if meets_requirements else "❌ Inativo (requisitos não cumpridos)"
            embed.add_field(name="Status", value=status, inline=False)
        
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao verificar usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@bot.tree.command(name="check_user_history", description="Verifica o histórico completo de um usuário")
@commands.has_permissions(administrator=True)
async def check_user_history(interaction: discord.Interaction, member: discord.Member):
    try:
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        last_warning = await bot.db.get_last_warning(member.id, member.guild.id)
        last_check = await bot.db.get_last_period_check(member.id, member.guild.id)
        
        embed = discord.Embed(
            title=f"Histórico de {member.display_name}",
            color=discord.Color.blue())
        
        if user_data:
            last_join = user_data.get('last_voice_join')
            last_leave = user_data.get('last_voice_leave')
            sessions = user_data.get('voice_sessions', 0)
            total_time = user_data.get('total_voice_time', 0)
            
            embed.add_field(
                name="Atividade de Voz",
                value=f"Última entrada: {last_join.strftime('%d/%m/%Y %H:%M') if last_join else 'Nunca'}\n"
                      f"Última saída: {last_leave.strftime('%d/%m/%Y %H:%M') if last_leave else 'N/A'}\n"
                      f"Sessões: {sessions}\n"
                      f"Tempo total: {int(total_time//3600)}h {int((total_time%3600)//60)}m",
                inline=False)
        
        if last_check:
            period_start = last_check['period_start'].replace(tzinfo=bot.timezone)
            period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
            meets_requirements = last_check['meets_requirements']
            
            embed.add_field(
                name="Último Período Verificado",
                value=f"De {period_start.strftime('%d/%m/%Y')} a {period_end.strftime('%d/%m/%Y')}\n"
                      f"Status: {'✅ Cumpriu' if meets_requirements else '❌ Não cumpriu'} os requisitos",
                inline=False)
        
        if last_warning:
            warning_type, warning_date = last_warning
            embed.add_field(
                name="Último Aviso",
                value=f"Tipo: {warning_type}\n"
                      f"Data: {warning_date.strftime('%d/%m/%Y %H:%M')}",
                inline=False)
        
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao verificar histórico do usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o histórico do usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

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
        # Garante que o Flask roda em uma thread separada
        keep_alive()
    
    try:
        bot.run(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.critical(f"Erro ao iniciar o bot: {e}")
        raise