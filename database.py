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
        try:
            os.makedirs(self.backup_dir, exist_ok=True)
            # Testar se o diretório é gravável
            test_file = os.path.join(self.backup_dir, 'test.txt')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            self.backup_enabled = True
        except Exception as e:
            logger.warning(f"Backups locais desabilitados - não foi possível acessar o diretório de backups: {e}")
            self.backup_enabled = False

    async def create_backup(self):
        """Cria backup do banco de dados usando método manual"""
        if not self.backup_enabled:
            logger.info("Backups locais estão desabilitados - pulando criação de backup")
            return False
            
        try:
            return await self._create_backup_manual()
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}", exc_info=True)
            return False

    async def _create_backup_manual(self):
        """Método manual de backup"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self.backup_dir, f'backup_{timestamp}.sql')
            zip_file = f'{backup_file}.zip'
            
            async with self.db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SHOW TABLES")
                    tables = [table['Tables_in_' + os.getenv('DB_NAME')] for table in await cursor.fetchall()]
                    
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        for table in tables:
                            # Escrever estrutura da tabela
                            await cursor.execute(f"SHOW CREATE TABLE `{table}`")
                            create_table = (await cursor.fetchone())['Create Table']
                            f.write(f"{create_table};\n\n")
                            
                            # Escrever dados da tabela
                            await cursor.execute(f"SELECT * FROM `{table}`")
                            rows = await cursor.fetchall()
                            if rows:
                                columns = [col[0] for col in cursor.description]
                                f.write(f"INSERT INTO `{table}` (`{'`,`'.join(columns)}`) VALUES\n")
                                
                                for i, row in enumerate(rows):
                                    values = []
                                    for value in row.values():
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
            
            # Compactar
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(backup_file, os.path.basename(backup_file))
            
            # Remover arquivo SQL temporário
            try:
                os.remove(backup_file)
            except Exception as e:
                logger.warning(f"Erro ao remover arquivo SQL temporário: {e}")
            
            # Limpar backups antigos
            self._cleanup_old_backups(keep=5)
            
            logger.info(f"Backup criado com sucesso: {zip_file}")
            return True
        except Exception as e:
            logger.error(f"Erro no método manual de backup: {e}")
            return False

    def _cleanup_old_backups(self, keep=5):
        """Remove backups antigos, mantendo apenas os 'keep' mais recentes"""
        try:
            backups = sorted(glob.glob(os.path.join(self.backup_dir, '*.zip')))
            for old_backup in backups[:-keep]:
                try:
                    os.remove(old_backup)
                    logger.info(f"Backup antigo removido: {old_backup}")
                except Exception as e:
                    logger.warning(f"Erro ao remover backup antigo {old_backup}: {e}")
        except Exception as e:
            logger.warning(f"Erro ao limpar backups antigos: {e}")

class Database:
    def __init__(self):
        self.pool = None
        self.semaphore = asyncio.Semaphore(25)  # Aumentado para 25 conexões simultâneas
        self._is_initialized = False
        self.heartbeat_task = None
        self._config_cache = {}  # Cache para configurações
        self._last_config_update = None

    async def initialize(self):
        """Inicializa o pool de conexões com configurações otimizadas para HostGator"""
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
                    minsize=5,
                    maxsize=25,
                    connect_timeout=30,
                    autocommit=True,
                    cursorclass=DictCursor,
                    pool_recycle=300,
                    echo=False
                )
                
                # Testar conexão com timeout
                try:
                    async with self.pool.acquire() as conn:
                        # Resetar a conexão para evitar "Command Out of Sync"
                        await conn.ping(reconnect=True)
                        async with conn.cursor() as cursor:
                            await cursor.execute("SELECT 1")
                            await cursor.fetchone()
                except asyncio.TimeoutError:
                    raise Exception("Timeout ao testar conexão com o banco de dados")
                
                # Verificar e criar tabelas se não estiverem inicializadas
                if not self._is_initialized:
                    try:
                        await self.create_tables()
                        self._is_initialized = True
                        logger.info("Banco de dados inicializado com sucesso")
                    except Exception as e:
                        logger.error(f"Erro ao criar tabelas: {e}")
                        # Tentar novamente mesmo que a criação de tabelas falhe
                        continue
                    
                # Iniciar task de heartbeat
                self.heartbeat_task = asyncio.create_task(self._db_heartbeat(interval=300))  # 5 minutos
                logger.info("Task de heartbeat do banco de dados iniciada")
                
                # Aquecer o pool (criar algumas conexões iniciais)
                await self._warmup_pool()
                
                return
                
            except Exception as e:
                logger.error(f"Erro ao conectar ao MySQL (tentativa {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    sleep_time = initial_delay * (2 ** attempt)
                    logger.info(f"Tentando novamente em {sleep_time} segundos...")
                    await asyncio.sleep(sleep_time)
                else:
                    raise

    async def _warmup_pool(self):
        """Cria algumas conexões iniciais para aquecer o pool"""
        try:
            warmup_conns = []
            for _ in range(min(5, self.pool.minsize)):  # Ajustado para o novo minsize
                conn = await self.pool.acquire()
                warmup_conns.append(conn)
            
            # Liberar conexões após aquecimento
            for conn in warmup_conns:
                self.pool.release(conn)
                
            logger.info(f"Pool aquecido com {len(warmup_conns)} conexões iniciais")
        except Exception as e:
            logger.warning(f"Erro ao aquecer pool: {e}")

    async def _db_heartbeat(self, interval: int = 300):
        """Envia um ping periódico para manter conexões ativas"""
        while True:
            try:
                # Verificar e reciclar conexões inativas
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await asyncio.wait_for(cursor.execute("SELECT 1"), timeout=10)
                        await cursor.fetchone()
                
                logger.debug("Heartbeat do banco de dados executado com sucesso")
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("Heartbeat do banco de dados cancelado.")
                break
            except Exception as e:
                logger.error(f"Erro no heartbeat do banco de dados: {e}")
                await asyncio.sleep(60)  # Espera 1 minuto antes de tentar novamente

    async def close(self):
        """Fecha o pool de conexões de forma segura"""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            logger.info("Task de heartbeat do banco de dados encerrada")
        
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Pool de conexões fechado")

    async def check_pool_status(self):
        """Retorna estatísticas do pool de conexões"""
        if self.pool:
            return {
                'size': self.pool.size,
                'freesize': self.pool.freesize,
                'used': self.pool.size - self.pool.freesize,
                'maxsize': self.pool.maxsize
            }
        return None

    async def create_tables(self):
        """Cria tabelas necessárias com índices otimizados"""
        async with self.semaphore:
            conn = None
            try:
                conn = await self.pool.acquire()
                async with conn.cursor() as cursor:
                    # Suprimir warnings de tabelas existentes
                    await cursor.execute("SET sql_notes = 0;")
                    
                    # Tabela de atividade do usuário
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
                        INDEX idx_last_leave (last_voice_leave),
                        INDEX idx_activity_composite (guild_id, user_id, last_voice_join, last_voice_leave)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de sessões de voz
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
                        INDEX idx_user_guild_time (user_id, guild_id, join_time, leave_time),
                        INDEX idx_duration (duration),
                        INDEX idx_session_composite (user_id, guild_id, join_time, leave_time, duration)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de avisos
                    await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_warnings (
                        user_id BIGINT,
                        guild_id BIGINT,
                        warning_type VARCHAR(20),
                        warning_date DATETIME,
                        PRIMARY KEY (user_id, guild_id, warning_type),
                        INDEX idx_warning_date (warning_date),
                        INDEX idx_user_guild_warning (user_id, guild_id, warning_type, warning_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de cargos removidos
                    await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS removed_roles (
                        user_id BIGINT,
                        guild_id BIGINT,
                        role_id BIGINT,
                        removal_date DATETIME,
                        PRIMARY KEY (user_id, guild_id, role_id),
                        INDEX idx_removal_date (removal_date),
                        INDEX idx_removal_composite (user_id, guild_id, role_id, removal_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de membros expulsos
                    await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS kicked_members (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT,
                        guild_id BIGINT,
                        kick_date DATETIME,
                        reason TEXT,
                        INDEX idx_user_guild (user_id, guild_id),
                        INDEX idx_kick_date (kick_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de períodos verificados
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
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de configuração do bot
                    await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS bot_config (
                        guild_id BIGINT PRIMARY KEY,
                        config_json TEXT,
                        last_updated DATETIME,
                        INDEX idx_last_updated (last_updated)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Tabela de logs de rate limit
                    await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rate_limit_logs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        guild_id BIGINT,
                        bucket VARCHAR(100),
                        limit_count INT,
                        remaining INT,
                        reset_at DATETIME,
                        scope VARCHAR(50),
                        endpoint VARCHAR(255),
                        retry_after FLOAT,
                        log_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_guild (guild_id),
                        INDEX idx_bucket (bucket),
                        INDEX idx_reset (reset_at),
                        INDEX idx_endpoint (endpoint),
                        INDEX idx_date (log_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''')
                    
                    # Reativar warnings
                    await cursor.execute("SET sql_notes = 1;")
                    await conn.commit()
                    logger.info("Tabelas criadas/verificadas com sucesso")
            except Exception as e:
                logger.error(f"Erro ao criar tabelas: {e}")
                raise
            finally:
                if conn:
                    self.pool.release(conn)

    async def execute_query(self, query: str, params: tuple = None, timeout: int = 30):
        """Executa uma query com tratamento de timeout e retry"""
        async with self.semaphore:
            max_retries = 3
            conn = None
            for attempt in range(max_retries):
                try:
                    # Adquirir conexão com timeout
                    conn = await asyncio.wait_for(self.pool.acquire(), timeout=timeout)
                    
                    # Executar query com timeout
                    cursor = await conn.cursor()
                    await asyncio.wait_for(cursor.execute(query, params or ()), timeout=timeout)
                    
                    return cursor, conn
                    
                except (aiomysql.OperationalError, aiomysql.InterfaceError) as e:
                    if "max_user_connections" in str(e):
                        logger.error(f"Limite de conexões excedido (tentativa {attempt + 1}/{max_retries})")
                        await asyncio.sleep(5 * (attempt + 1))  # Backoff exponencial
                        continue
                        
                    logger.error(f"Erro de conexão (tentativa {attempt + 1}/{max_retries}): {e}")
                    if conn:
                        self.pool.release(conn)
                        conn = None
                        
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3 * (attempt + 1))  # Backoff exponencial
                        continue
                        
                    raise
                    
                except asyncio.TimeoutError:
                    logger.error(f"Timeout ao executar query (tentativa {attempt + 1})")
                    if conn:
                        self.pool.release(conn)
                        conn = None
                        
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3 * (attempt + 1))  # Backoff exponencial
                        continue
                        
                    raise TimeoutError("Timeout ao executar query no banco de dados")
                    
                except Exception as e:
                    logger.error(f"Erro ao executar query: {e}")
                    if conn:
                        self.pool.release(conn)
                    raise

    async def save_config(self, guild_id: int, config: dict):
        """Salva configuração com cache"""
        cursor = None
        conn = None
        try:
            # Atualizar cache
            self._config_cache[guild_id] = config
            self._last_config_update = datetime.utcnow()
            
            # Serializar para JSON
            config_json = json.dumps(config)
            
            cursor, conn = await self.execute_query('''
                INSERT INTO bot_config (guild_id, config_json, last_updated)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    config_json = VALUES(config_json),
                    last_updated = VALUES(last_updated)
            ''', (guild_id, config_json, datetime.utcnow()))
            
            await conn.commit()
            logger.info(f"Configuração salva no banco de dados para a guild {guild_id}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            # Remover do cache em caso de erro
            if guild_id in self._config_cache:
                del self._config_cache[guild_id]
            return False
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def load_config(self, guild_id: int) -> Optional[dict]:
        """Carrega configuração com cache"""
        # Verificar cache primeiro
        if guild_id in self._config_cache:
            # Se a configuração foi atualizada recentemente, retornar do cache
            if self._last_config_update and (datetime.utcnow() - self._last_config_update).total_seconds() < 300:
                logger.debug(f"Retornando configuração do cache para guild {guild_id}")
                return self._config_cache[guild_id]
        
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                SELECT config_json FROM bot_config
                WHERE guild_id = %s
            ''', (guild_id,))
            
            result = await cursor.fetchone()
            if result:
                config = json.loads(result['config_json'])
                # Atualizar cache
                self._config_cache[guild_id] = config
                self._last_config_update = datetime.utcnow()
                
                logger.info(f"Configuração carregada do banco de dados para a guild {guild_id}")
                return config
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            return None
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def log_voice_join(self, user_id: int, guild_id: int):
        """Registra entrada em canal de voz"""
        now = datetime.utcnow()
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                INSERT INTO user_activity 
                (user_id, guild_id, last_voice_join, voice_sessions) 
                VALUES (%s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE 
                    last_voice_join = VALUES(last_voice_join),
                    voice_sessions = voice_sessions + 1
            ''', (user_id, guild_id, now))
            await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar entrada em voz: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def log_voice_leave(self, user_id: int, guild_id: int, duration: int):
        """Registra saída de canal de voz"""
        now = datetime.utcnow()
        conn = None
        try:
            conn = await self.pool.acquire()
            async with conn.cursor() as cursor:
                # Atualizar atividade do usuário
                await cursor.execute('''
                    UPDATE user_activity 
                    SET last_voice_leave = %s,
                        total_voice_time = total_voice_time + %s
                    WHERE user_id = %s AND guild_id = %s
                ''', (now, duration, user_id, guild_id))
                
                # Registrar sessão de voz
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
        finally:
            if conn:
                self.pool.release(conn)

    async def get_user_activity(self, user_id: int, guild_id: int) -> Dict:
        """Obtém dados de atividade do usuário"""
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                SELECT last_voice_join, last_voice_leave, voice_sessions, total_voice_time 
                FROM user_activity 
                WHERE user_id = %s AND guild_id = %s
            ''', (user_id, guild_id))
            
            result = await cursor.fetchone()
            return result if result else {}
        except Exception as e:
            logger.error(f"Erro ao obter atividade do usuário: {e}")
            return {}
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def get_voice_sessions(self, user_id: int, guild_id: int, 
                               start_date: datetime, end_date: datetime) -> List[Dict]:
        """Obtém sessões de voz do usuário em um período"""
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                SELECT join_time, leave_time, duration 
                FROM voice_sessions
                WHERE user_id = %s AND guild_id = %s
                AND join_time >= %s AND leave_time <= %s
                ORDER BY join_time
            ''', (user_id, guild_id, start_date, end_date))
            
            result = await cursor.fetchall()
            return result
        except Exception as e:
            logger.error(f"Erro ao obter sessões de voz: {e}")
            return []
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def log_period_check(self, user_id: int, guild_id: int, 
                             start_date: datetime, end_date: datetime, 
                             meets_requirements: bool):
        """Registra verificação de período"""
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                INSERT INTO checked_periods
                (user_id, guild_id, period_start, period_end, meets_requirements)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    meets_requirements = VALUES(meets_requirements)
            ''', (user_id, guild_id, start_date, end_date, meets_requirements))
            
            await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar verificação de período: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def get_last_period_check(self, user_id: int, guild_id: int) -> Optional[Dict]:
        """Obtém última verificação de período"""
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                SELECT period_start, period_end, meets_requirements
                FROM checked_periods
                WHERE user_id = %s AND guild_id = %s
                ORDER BY period_start DESC
                LIMIT 1
            ''', (user_id, guild_id))
            
            result = await cursor.fetchone()
            return result
        except Exception as e:
            logger.error(f"Erro ao obter última verificação de período: {e}")
            return None
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def log_warning(self, user_id: int, guild_id: int, warning_type: str):
        """Registra aviso enviado ao usuário"""
        now = datetime.utcnow()
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                INSERT INTO user_warnings 
                (user_id, guild_id, warning_type, warning_date) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    warning_date = VALUES(warning_date)
            ''', (user_id, guild_id, warning_type, now))
            
            await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar aviso: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def get_last_warning(self, user_id: int, guild_id: int) -> Optional[Tuple[str, datetime]]:
        """Obtém último aviso enviado ao usuário"""
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                SELECT warning_type, warning_date 
                FROM user_warnings 
                WHERE user_id = %s AND guild_id = %s
                ORDER BY warning_date DESC
                LIMIT 1
            ''', (user_id, guild_id))
            
            result = await cursor.fetchone()
            if result:
                return result['warning_type'], result['warning_date']
            return None
        except Exception as e:
            logger.error(f"Erro ao obter último aviso: {e}")
            return None
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def log_removed_roles(self, user_id: int, guild_id: int, role_ids: List[int]):
        """Registra cargos removidos por inatividade"""
        now = datetime.utcnow()
        conn = None
        try:
            conn = await self.pool.acquire()
            async with conn.cursor() as cursor:
                # Usar uma única transação para múltiplos inserts
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
        finally:
            if conn:
                self.pool.release(conn)

    async def log_kicked_member(self, user_id: int, guild_id: int, reason: str):
        """Registra membro expulso por inatividade"""
        now = datetime.utcnow()
        cursor = None
        conn = None
        try:
            cursor, conn = await self.execute_query('''
                INSERT INTO kicked_members 
                (user_id, guild_id, kick_date, reason) 
                VALUES (%s, %s, %s, %s)
            ''', (user_id, guild_id, now, reason))
            
            await conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar membro expulso: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def get_members_with_tracked_roles(self, guild_id: int, role_ids: List[int]) -> List[Dict]:
        """Obtém todos os membros que possuem pelo menos um dos cargos monitorados"""
        cursor = None
        conn = None
        try:
            # Precisamos usar uma query mais complexa para verificar múltiplos cargos
            placeholders = ','.join(['%s'] * len(role_ids))
            cursor, conn = await self.execute_query(f'''
                SELECT DISTINCT u.user_id 
                FROM user_activity u
                JOIN voice_sessions v ON u.user_id = v.user_id AND u.guild_id = v.guild_id
                WHERE u.guild_id = %s
                AND EXISTS (
                    SELECT 1 FROM voice_sessions 
                    WHERE user_id = u.user_id 
                    AND guild_id = u.guild_id
                )
                UNION
                SELECT user_id FROM user_activity
                WHERE guild_id = %s
                AND last_voice_join IS NOT NULL
            ''', (guild_id, guild_id))
            
            results = await cursor.fetchall()
            return [r['user_id'] for r in results] if results else []
        except Exception as e:
            logger.error(f"Erro ao buscar membros com cargos monitorados: {e}")
            return []
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def cleanup_old_data(self, days: int = 60):
        """Limpa dados antigos do banco de dados"""
        conn = None
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            conn = await self.pool.acquire()
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
                
                # Limpar logs de rate limit antigos
                await cursor.execute("DELETE FROM rate_limit_logs WHERE log_date < %s", (cutoff_date,))
                rate_limits_deleted = cursor.rowcount
                
                await conn.commit()
                
                log_message = (
                    f"Limpeza de dados antigos concluída: "
                    f"Sessões de voz: {voice_deleted}, "
                    f"Avisos: {warnings_deleted}, "
                    f"Cargos removidos: {roles_deleted}, "
                    f"Expulsões: {kicks_deleted}, "
                    f"Rate limits: {rate_limits_deleted}"
                )
                logger.info(log_message)
                return log_message
        except Exception as e:
            logger.error(f"Erro ao limpar dados antigos: {e}")
            raise
        finally:
            if conn:
                self.pool.release(conn)

    async def log_rate_limit(self, guild_id: int, data: dict):
        """Registra ocorrência de rate limit no banco de dados"""
        cursor = None
        conn = None
        try:
            # Converter reset timestamp para datetime se existir
            reset_at = datetime.utcfromtimestamp(data['reset']) if data.get('reset') else None
            
            cursor, conn = await self.execute_query('''
                INSERT INTO rate_limit_logs 
                (guild_id, bucket, limit_count, remaining, reset_at, scope, endpoint, retry_after)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                guild_id,
                data.get('bucket'),
                data.get('limit'),
                data.get('remaining'),
                reset_at,
                data.get('scope'),
                data.get('endpoint'),
                data.get('retry_after')
            ))
            await conn.commit()
            logger.info(f"Rate limit registrado para guild {guild_id} no endpoint {data.get('endpoint')}")
            return True
        except Exception as e:
            logger.error(f"Erro ao registrar rate limit: {e}")
            return False
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def get_rate_limit_history(self, guild_id: int, hours: int = 24) -> List[Dict]:
        """Obtém histórico de rate limits para uma guild"""
        cursor = None
        conn = None
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            cursor, conn = await self.execute_query('''
                SELECT bucket, limit_count, remaining, reset_at, scope, endpoint, retry_after, log_date
                FROM rate_limit_logs
                WHERE guild_id = %s AND log_date >= %s
                ORDER BY log_date DESC
            ''', (guild_id, since))
            
            return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Erro ao obter histórico de rate limits: {e}")
            return []
        finally:
            if cursor:
                await cursor.close()
            if conn:
                self.pool.release(conn)

    async def cleanup_rate_limit_logs(self, days: int = 7):
        """Limpa logs de rate limit antigos"""
        conn = None
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            conn = await self.pool.acquire()
            async with conn.cursor() as cursor:
                await cursor.execute("DELETE FROM rate_limit_logs WHERE log_date < %s", (cutoff_date,))
                deleted_count = cursor.rowcount
                await conn.commit()
                logger.info(f"Removidos {deleted_count} logs de rate limit antigos")
                return deleted_count
        except Exception as e:
            logger.error(f"Erro ao limpar logs de rate limit: {e}")
            return 0
        finally:
            if conn:
                self.pool.release(conn)