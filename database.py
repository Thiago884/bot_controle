import os
import time
import asyncio
import logging
import glob
import zipfile
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import aiomysql
from aiomysql import Pool, Connection, DictCursor

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
            
            async with self.db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Get all tables
                    await cursor.execute("SHOW TABLES")
                    tables = [table[0] for table in await cursor.fetchall()]
                    
                    with open(backup_file, 'w') as f:
                        for table in tables:
                            # Write table structure
                            await cursor.execute(f"SHOW CREATE TABLE {table}")
                            create_table = (await cursor.fetchone())[1]
                            f.write(f"{create_table};\n\n")
                            
                            # Write table data
                            await cursor.execute(f"SELECT * FROM {table}")
                            rows = await cursor.fetchall()
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
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}")
            return False

class Database:
    def __init__(self):
        self.pool = None
        self.semaphore = asyncio.Semaphore(20)  # Limite de operações concorrentes
        self._is_initialized = False

    async def initialize(self):
        if self._is_initialized:
            return
            
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                self.pool = await aiomysql.create_pool(
                    host=os.getenv('DB_HOST'),
                    port=int(os.getenv('DB_PORT', 3306)),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASS'),
                    db=os.getenv('DB_NAME'),
                    minsize=5,       # Número mínimo de conexões no pool
                    maxsize=30,       # Número máximo de conexões no pool
                    connect_timeout=30,
                    autocommit=True,
                    cursorclass=DictCursor,
                    pool_recycle=3600,  # Recicla conexões após 1 hora
                )
                
                # Testar conexão
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                
                await self.create_tables()
                self._is_initialized = True
                logger.info("Banco de dados inicializado com sucesso")
                return
                
            except Exception as e:
                logger.error(f"Erro ao conectar ao MySQL (tentativa {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    sleep_time = initial_delay * (2 ** attempt)
                    logger.info(f"Tentando novamente em {sleep_time} segundos...")
                    await asyncio.sleep(sleep_time)
                else:
                    raise

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Pool de conexões fechado")

    async def check_pool_status(self):
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                        threads = (await cursor.fetchone())['Value']
                        await cursor.execute("SHOW STATUS LIKE 'Threads_running'")
                        running = (await cursor.fetchone())['Value']
                        logger.info(f"Status do pool: Threads connected={threads}, running={running}")
                        return int(threads), int(running)
            except Exception as e:
                logger.error(f"Erro ao verificar status do pool: {e}")
                return None, None

    async def create_tables(self):
        async with self.semaphore:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    try:
                        await cursor.execute('''
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
                        
                        await cursor.execute('''
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
                        
                        await cursor.execute('''
                        CREATE TABLE IF NOT EXISTS user_warnings (
                            user_id BIGINT,
                            guild_id BIGINT,
                            warning_type VARCHAR(20),
                            warning_date DATETIME,
                            PRIMARY KEY (user_id, guild_id, warning_type),
                            INDEX idx_warning_date (warning_date),
                            INDEX idx_user_guild_warning (user_id, guild_id, warning_type, warning_date)
                        )''')
                        
                        await cursor.execute('''
                        CREATE TABLE IF NOT EXISTS removed_roles (
                            user_id BIGINT,
                            guild_id BIGINT,
                            role_id BIGINT,
                            removal_date DATETIME,
                            PRIMARY KEY (user_id, guild_id, role_id),
                            INDEX idx_removal_date (removal_date)
                        )''')
                        
                        await cursor.execute('''
                        CREATE TABLE IF NOT EXISTS kicked_members (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            user_id BIGINT,
                            guild_id BIGINT,
                            kick_date DATETIME,
                            reason TEXT,
                            INDEX idx_user_guild (user_id, guild_id),
                            INDEX idx_kick_date (kick_date)
                        )''')
                        
                        await cursor.execute('''
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
                        
                        await cursor.execute('''
                        CREATE TABLE IF NOT EXISTS bot_config (
                            guild_id BIGINT PRIMARY KEY,
                            config_json TEXT,
                            last_updated DATETIME,
                            INDEX idx_last_updated (last_updated)
                        )''')
                        
                        await conn.commit()
                        logger.info("Tabelas criadas/verificadas com sucesso")
                    except Exception as e:
                        logger.error(f"Erro ao criar tabelas: {e}")
                        raise

    async def execute_query(self, query: str, params: tuple = None):
        async with self.semaphore:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(query, params or ())
                        return cursor
            except aiomysql.OperationalError as e:
                logger.error(f"Erro operacional no banco de dados: {e}")
                await self.check_pool_status()
                raise
            except Exception as e:
                logger.error(f"Erro ao executar query: {e}")
                raise

    async def save_config(self, guild_id: int, config: dict):
        try:
            async with self.execute_query('''
                INSERT INTO bot_config (guild_id, config_json, last_updated)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    config_json = VALUES(config_json),
                    last_updated = VALUES(last_updated)
            ''', (guild_id, json.dumps(config), datetime.utcnow())) as cursor:
                await cursor.connection.commit()
                logger.info(f"Configuração salva no banco de dados para a guild {guild_id}")
                return True
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            return False

    async def load_config(self, guild_id: int) -> Optional[dict]:
        try:
            async with self.execute_query('''
                SELECT config_json FROM bot_config
                WHERE guild_id = %s
            ''', (guild_id,)) as cursor:
                result = await cursor.fetchone()
                if result:
                    logger.info(f"Configuração carregada do banco de dados para a guild {guild_id}")
                    return json.loads(result['config_json'])
                return None
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            return None

    async def log_voice_join(self, user_id: int, guild_id: int):
        now = datetime.utcnow()
        try:
            async with self.execute_query('''
                INSERT INTO user_activity 
                (user_id, guild_id, last_voice_join, voice_sessions) 
                VALUES (%s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE 
                    last_voice_join = VALUES(last_voice_join),
                    voice_sessions = voice_sessions + 1
            ''', (user_id, guild_id, now)) as cursor:
                await cursor.connection.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar entrada em voz: {e}")
            raise

    async def log_voice_leave(self, user_id: int, guild_id: int, duration: int):
        now = datetime.utcnow()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('''
                        UPDATE user_activity 
                        SET last_voice_leave = %s,
                            total_voice_time = total_voice_time + %s
                        WHERE user_id = %s AND guild_id = %s
                    ''', (now, duration, user_id, guild_id))
                    
                    join_time = now - timedelta(seconds=duration)
                    await cursor.execute('''
                        INSERT INTO voice_sessions
                        (user_id, guild_id, join_time, leave_time, duration)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (user_id, guild_id, join_time, now, duration))
                    
                    await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar saída de voz: {e}")
            raise

    async def get_user_activity(self, user_id: int, guild_id: int) -> Dict:
        try:
            async with self.execute_query('''
                SELECT last_voice_join, last_voice_leave, voice_sessions, total_voice_time 
                FROM user_activity 
                WHERE user_id = %s AND guild_id = %s
            ''', (user_id, guild_id)) as cursor:
                result = await cursor.fetchone()
                return result if result else {}
        except Exception as e:
            logger.error(f"Erro ao obter atividade do usuário: {e}")
            return {}

    async def get_voice_sessions(self, user_id: int, guild_id: int, 
                               start_date: datetime, end_date: datetime) -> List[Dict]:
        try:
            async with self.execute_query('''
                SELECT join_time, leave_time, duration 
                FROM voice_sessions
                WHERE user_id = %s AND guild_id = %s
                AND join_time >= %s AND leave_time <= %s
                ORDER BY join_time
            ''', (user_id, guild_id, start_date, end_date)) as cursor:
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Erro ao obter sessões de voz: {e}")
            return []

    async def log_period_check(self, user_id: int, guild_id: int, 
                             start_date: datetime, end_date: datetime, 
                             meets_requirements: bool):
        try:
            async with self.execute_query('''
                INSERT INTO checked_periods
                (user_id, guild_id, period_start, period_end, meets_requirements)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    meets_requirements = VALUES(meets_requirements)
            ''', (user_id, guild_id, start_date, end_date, meets_requirements)) as cursor:
                await cursor.connection.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar verificação de período: {e}")
            raise

    async def get_last_period_check(self, user_id: int, guild_id: int) -> Optional[Dict]:
        try:
            async with self.execute_query('''
                SELECT period_start, period_end, meets_requirements
                FROM checked_periods
                WHERE user_id = %s AND guild_id = %s
                ORDER BY period_start DESC
                LIMIT 1
            ''', (user_id, guild_id)) as cursor:
                return await cursor.fetchone()
        except Exception as e:
            logger.error(f"Erro ao obter última verificação de período: {e}")
            return None

    async def log_warning(self, user_id: int, guild_id: int, warning_type: str):
        now = datetime.utcnow()
        try:
            async with self.execute_query('''
                INSERT INTO user_warnings 
                (user_id, guild_id, warning_type, warning_date) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    warning_date = VALUES(warning_date)
            ''', (user_id, guild_id, warning_type, now)) as cursor:
                await cursor.connection.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar aviso: {e}")
            raise

    async def get_last_warning(self, user_id: int, guild_id: int) -> Optional[Tuple[str, datetime]]:
        try:
            async with self.execute_query('''
                SELECT warning_type, warning_date 
                FROM user_warnings 
                WHERE user_id = %s AND guild_id = %s
                ORDER BY warning_date DESC
                LIMIT 1
            ''', (user_id, guild_id)) as cursor:
                result = await cursor.fetchone()
                if result:
                    return result['warning_type'], result['warning_date']
                return None
        except Exception as e:
            logger.error(f"Erro ao obter último aviso: {e}")
            return None

    async def log_removed_roles(self, user_id: int, guild_id: int, role_ids: List[int]):
        now = datetime.utcnow()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for role_id in role_ids:
                        await cursor.execute('''
                            INSERT INTO removed_roles 
                            (user_id, guild_id, role_id, removal_date) 
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                                removal_date = VALUES(removal_date)
                        ''', (user_id, guild_id, role_id, now))
                    await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar cargos removidos: {e}")
            raise

    async def log_kicked_member(self, user_id: int, guild_id: int, reason: str):
        now = datetime.utcnow()
        try:
            async with self.execute_query('''
                INSERT INTO kicked_members 
                (user_id, guild_id, kick_date, reason) 
                VALUES (%s, %s, %s, %s)
            ''', (user_id, guild_id, now, reason)) as cursor:
                await cursor.connection.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar membro expulso: {e}")
            raise

    async def cleanup_old_data(self, days: int = 60):
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Limpar sessões de voz antigas
                    await cursor.execute("DELETE FROM voice_sessions WHERE leave_time < %s", (cutoff_date,))
                    voice_deleted = cursor.rowcount
                    
                    # Limpar avisos antigos
                    await cursor.execute("DELETE FROM user_warnings WHERE warning_date < %s", (cutoff_date,))
                    warnings_deleted = cursor.rowcount
                    
                    # Limpar registros de cargos removidos antigos
                    await cursor.execute("DELETE FROM removed_roles WHERE removal_date < %s", (cutoff_date,))
                    roles_deleted = cursor.rowcount
                    
                    # Limpar membros expulsos antigos
                    await cursor.execute("DELETE FROM kicked_members WHERE kick_date < %s", (cutoff_date,))
                    kicks_deleted = cursor.rowcount
                    
                    await conn.commit()
                    
                    log_message = (
                        f"Limpeza de dados antigos concluída: "
                        f"Sessões de voz: {voice_deleted}, "
                        f"Avisos: {warnings_deleted}, "
                        f"Cargos removidos: {roles_deleted}, "
                        f"Expulsões: {kicks_deleted}"
                    )
                    logger.info(log_message)
                    return log_message
        except Exception as e:
            logger.error(f"Erro ao limpar dados antigos: {e}")
            raise