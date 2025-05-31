import os
import time
import mysql.connector
from mysql.connector import Error
from mysql.connector.errors import OperationalError, InterfaceError
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import logging
import glob
import zipfile

logger = logging.getLogger('inactivity_bot')

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
                    pool_size=20,
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