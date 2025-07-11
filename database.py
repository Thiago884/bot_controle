import os
import time
import asyncio
import logging
import glob
import zipfile
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import asyncpg
from asyncpg import Pool, Connection
from asyncpg.pool import create_pool
import pytz  # Added import

logger = logging.getLogger('inactivity_bot')

# Configuração padrão para fallback
DEFAULT_CONFIG = {
    # Adicione configurações padrão para guilds específicas se necessário
}

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
            timestamp = datetime.now(pytz.utc).strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self.backup_dir, f'backup_{timestamp}.sql')
            zip_file = f'{backup_file}.zip'
            
            # Garantir que o diretório existe
            os.makedirs(self.backup_dir, exist_ok=True)
            
            async with self.db.pool.acquire() as conn:
                tables = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                
                with open(backup_file, 'w', encoding='utf-8') as f:
                    for table in tables:
                        table_name = table['table_name']
                        try:
                            # Usar apenas a query alternativa, removendo a tentativa com pg_get_tabledef
                            create_table = await conn.fetchval('''
                                SELECT 'CREATE TABLE ' || quote_ident(table_name) || ' (' || 
                                       string_agg(column_definition, ', ') || ');'
                                FROM (
                                    SELECT 
                                        table_name,
                                        quote_ident(column_name) || ' ' || 
                                        data_type || 
                                        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END || 
                                        CASE WHEN column_default IS NOT NULL THEN ' DEFAULT ' || column_default ELSE '' END AS column_definition
                                    FROM information_schema.columns
                                    WHERE table_name = $1 AND table_schema = 'public'
                                    ORDER by ordinal_position
                                ) AS cols
                                GROUP BY table_name;
                            ''', table_name)
                            
                            if create_table:
                                f.write(f"{create_table};\n\n")
                            else:
                                logger.warning(f"Não foi possível obter definição da tabela {table_name}")
                                continue
                                
                        except Exception as e:
                            logger.warning(f"Erro ao obter definição da tabela {table_name}: {e}")
                            continue
                        
                        # Escrever dados da tabela
                        rows = await conn.fetch(f"SELECT * FROM {table_name}")
                        if rows:
                            columns = list(rows[0].keys())
                            f.write(f"INSERT INTO {table_name} ({','.join(columns)}) VALUES\n")
                            
                            for i, row in enumerate(rows):
                                values = []
                                for value in row.values():
                                    if value is None:
                                        values.append("NULL")
                                    elif isinstance(value, (int, float)):
                                        values.append(str(value))
                                    elif isinstance(value, datetime):
                                        values.append(f"'{value.isoformat()}'")
                                    elif isinstance(value, str):
                                        values.append("'" + value.replace("'", "''") + "'")
                                    else:
                                        values.append("'" + str(value).replace("'", "''") + "'")
                                
                                f.write(f"({','.join(values)})")
                                if i < len(rows) - 1:
                                    f.write(",\n")
                                else:
                                    f.write(";\n\n")
            
            # Criar arquivo ZIP antes de remover o SQL
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(backup_file, os.path.basename(backup_file))
            
            # Verificar se o ZIP foi criado antes de remover o SQL
            if os.path.exists(zip_file):
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
            # Remover arquivos temporários em caso de erro
            if 'backup_file' in locals() and os.path.exists(backup_file):
                try:
                    os.remove(backup_file)
                except:
                    pass
            if 'zip_file' in locals() and os.path.exists(zip_file):
                try:
                    os.remove(zip_file)
                except:
                    pass
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
        self.semaphore = asyncio.Semaphore(25)
        self._is_initialized = False
        self.heartbeat_task = None
        self._config_cache = {}
        self._last_config_update = None
        self._active_tasks = set()

    async def initialize(self):
        if self._is_initialized:
            return
                
        max_retries = 5
        initial_delay = 2
        for attempt in range(max_retries):
            try:
                db_url = os.getenv('DATABASE_URL')
                
                if not db_url:
                    raise ValueError("Variável de ambiente DATABASE_URL não definida")
                    
                logger.info("Tentando conectar ao banco de dados usando DATABASE_URL")
                
                # Adicionar parâmetro sslmode='require' se não estiver presente na URL
                if 'sslmode=' not in db_url:
                    db_url += '?sslmode=require'
                
                self.pool = await create_pool(
                    dsn=db_url,
                    min_size=5,
                    max_size=50,
                    command_timeout=60,
                    max_inactive_connection_lifetime=300,
                    ssl='require'
                )
                
                async with self.pool.acquire() as conn:
                    await asyncio.wait_for(conn.execute("SELECT 1"), timeout=10)
                
                await self.create_tables()
                self._is_initialized = True
                logger.info("Banco de dados inicializado com sucesso")
                    
                self.heartbeat_task = asyncio.create_task(self._db_heartbeat(interval=300))
                self.heartbeat_task._name = 'database_heartbeat'
                self._active_tasks.add(self.heartbeat_task)
                logger.info("Task de heartbeat do banco de dados iniciada")
                
                return  # Retorna None em caso de sucesso
                
            except OSError as e:
                if e.errno == 101: # Network is unreachable
                    logger.error(
                        f"Tentativa {attempt + 1} de conexão falhou: Rede inacessível (Network Unreachable). "
                        f"Isso geralmente é um problema de firewall. Verifique se o IP da sua aplicação na Render "
                        f"está na lista de permissões (Network Restrictions) do seu banco de dados Neon."
                    )
                else:
                    logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou com erro de OS: {e}")

            except Exception as e:
                logger.error(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
            
            if attempt == max_retries - 1:
                logger.critical("Falha ao conectar ao banco de dados após várias tentativas.")
                self.pool = None
                raise  # Lança a exceção para ser tratada pelo chamador
            
            sleep_time = initial_delay * (2 ** attempt)
            logger.info(f"Tentando novamente em {sleep_time} segundos...")
            await asyncio.sleep(sleep_time)

    async def _db_heartbeat(self, interval: int = 300):
        """Envia um ping periódico para manter conexões ativas"""
        while True:
            try:
                if self.pool:
                    async with self.pool.acquire() as conn:
                        await asyncio.wait_for(conn.execute("SELECT 1"), timeout=10)
                    logger.debug("Heartbeat do banco de dados executado com sucesso")
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("Heartbeat do banco de dados cancelado.")
                break
            except Exception as e:
                logger.error(f"Erro no heartbeat do banco de dados: {e}")
                await asyncio.sleep(60)

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
            await self.pool.close()
            logger.info("Pool de conexões fechado")

    async def check_pool_status(self):
        """Retorna estatísticas do pool de conexões"""
        if self.pool:
            return {
                'size': self.pool.get_size(),
                'freesize': self.pool.get_idle_size(),
                'used': self.pool.get_size() - self.pool.get_idle_size(),
                'maxsize': self.pool.get_max_size()
            }
        return None

    async def create_tables(self):
        """Cria tabelas necessárias com índices otimizados e TIMESTAMPTZ"""
        async with self.semaphore:
            conn = None
            try:
                conn = await self.pool.acquire()
                
                # Tabela de atividade do usuário
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_activity (
                    user_id BIGINT,
                    guild_id BIGINT,
                    last_voice_join TIMESTAMPTZ,
                    last_voice_leave TIMESTAMPTZ,
                    voice_sessions INT DEFAULT 0,
                    total_voice_time INT DEFAULT 0,
                    PRIMARY KEY (user_id, guild_id)
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_guild_user ON user_activity (guild_id, user_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_last_join ON user_activity (last_voice_join)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_last_leave ON user_activity (last_voice_leave)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_activity_composite ON user_activity (guild_id, user_id, last_voice_join, last_voice_leave)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_voice_time_composite ON user_activity (guild_id, user_id, total_voice_time)')
                
                # Tabela de sessões de voz
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS voice_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    guild_id BIGINT,
                    join_time TIMESTAMPTZ,
                    leave_time TIMESTAMPTZ,
                    duration INT
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild ON voice_sessions (user_id, guild_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_join_time ON voice_sessions (join_time)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_leave_time ON voice_sessions (leave_time)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild_time ON voice_sessions (user_id, guild_id, join_time, leave_time)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_duration ON voice_sessions (duration)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_session_composite ON voice_sessions (user_id, guild_id, join_time, leave_time, duration)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild_duration ON voice_sessions (user_id, guild_id, duration)')
                
                # Tabela de avisos
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_warnings (
                    user_id BIGINT,
                    guild_id BIGINT,
                    warning_type VARCHAR(20),
                    warning_date TIMESTAMPTZ,
                    PRIMARY KEY (user_id, guild_id, warning_type)
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_warning_date ON user_warnings (warning_date)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild_warning ON user_warnings (user_id, guild_id, warning_type, warning_date)')
                
                # Tabela de cargos removidos
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS removed_roles (
                    user_id BIGINT,
                    guild_id BIGINT,
                    role_id BIGINT,
                    removal_date TIMESTAMPTZ,
                    PRIMARY KEY (user_id, guild_id, role_id)
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_removal_date ON removed_roles (removal_date)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_removal_composite ON removed_roles (user_id, guild_id, role_id, removal_date)')
                
                # Tabela de membros expulsos
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS kicked_members (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    guild_id BIGINT,
                    kick_date TIMESTAMPTZ,
                    reason TEXT
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild ON kicked_members (user_id, guild_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_kick_date ON kicked_members (kick_date)')
                
                # Tabela de períodos verificados
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS checked_periods (
                    user_id BIGINT,
                    guild_id BIGINT,
                    period_start TIMESTAMPTZ,
                    period_end TIMESTAMPTZ,
                    meets_requirements BOOLEAN,
                    PRIMARY KEY (user_id, guild_id, period_start)
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_period_end ON checked_periods (period_end)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_requirements ON checked_periods (meets_requirements)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_guild_period ON checked_periods (user_id, guild_id, period_start, period_end)')
                
                # Tabela de configuração do bot
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_config (
                    guild_id BIGINT PRIMARY KEY,
                    config_json TEXT,
                    last_updated TIMESTAMPTZ
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_last_updated ON bot_config (last_updated)')
                
                # Tabela de logs de rate limit
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS rate_limit_logs (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    bucket VARCHAR(100),
                    limit_count INT,
                    remaining INT,
                    reset_at TIMESTAMPTZ,
                    scope VARCHAR(50),
                    endpoint VARCHAR(255),
                    retry_after FLOAT,
                    log_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_guild ON rate_limit_logs (guild_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_bucket ON rate_limit_logs (bucket)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_reset ON rate_limit_logs (reset_at)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_endpoint ON rate_limit_logs (endpoint)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON rate_limit_logs (log_date)')
                
                # Tabela de execuções de tasks
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS task_executions (
                    task_name VARCHAR(50) PRIMARY KEY,
                    last_execution TIMESTAMPTZ,
                    monitoring_period INT
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_last_execution ON task_executions (last_execution)')
                
                # Tabela de eventos de voz pendentes (nova tabela adicionada)
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS pending_voice_events (
                    id SERIAL PRIMARY KEY,
                    event_type VARCHAR(20),
                    user_id BIGINT,
                    guild_id BIGINT,
                    before_channel_id BIGINT,
                    after_channel_id BIGINT,
                    before_self_deaf BOOLEAN,
                    before_deaf BOOLEAN,
                    after_self_deaf BOOLEAN,
                    after_deaf BOOLEAN,
                    event_time TIMESTAMPTZ,
                    processed BOOLEAN DEFAULT FALSE
                )''')
                
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_pending_events ON pending_voice_events (user_id, guild_id, processed)')
                
                logger.info("Tabelas criadas/verificadas com sucesso")
            except Exception as e:
                logger.error(f"Erro ao criar tabelas: {e}")
                raise
            finally:
                if conn:
                    await self.pool.release(conn)

    async def execute_query(self, query: str, params: tuple = None, timeout: int = 30):
        """Executa uma query com tratamento de timeout e retry"""
        if not self.pool:
            raise RuntimeError("Pool de conexões não está disponível")
            
        async with self.semaphore:
            max_retries = 3
            conn = None
            for attempt in range(max_retries):
                try:
                    # Adquirir conexão com timeout
                    conn = await asyncio.wait_for(self.pool.acquire(), timeout=timeout)
                    
                    # Executar query com timeout
                    result = await asyncio.wait_for(conn.execute(query, *(params or ())), timeout=timeout)
                    
                    return conn, result
                    
                except (asyncpg.PostgresError, asyncpg.InterfaceError) as e:
                    logger.error(f"Erro de conexão (tentativa {attempt + 1}/{max_retries}): {e}")
                    if conn:
                        await self.pool.release(conn)
                        conn = None
                        
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3 * (attempt + 1))  # Backoff exponencial
                        continue
                        
                    raise
                    
                except asyncio.TimeoutError:
                    logger.error(f"Timeout ao executar query (tentativa {attempt + 1})")
                    if conn:
                        await self.pool.release(conn)
                        conn = None
                        
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3 * (attempt + 1))  # Backoff exponencial
                        continue
                        
                    raise TimeoutError("Timeout ao executar query no banco de dados")
                    
                except Exception as e:
                    logger.error(f"Erro ao executar query: {e}")
                    if conn:
                        await self.pool.release(conn)
                    raise

    async def save_pending_voice_event(self, event_type: str, user_id: int, guild_id: int,
                                     before_channel_id: Optional[int], after_channel_id: Optional[int],
                                     before_self_deaf: Optional[bool], before_deaf: Optional[bool],
                                     after_self_deaf: Optional[bool], after_deaf: Optional[bool]):
        """Salva um evento de voz pendente no banco de dados"""
        conn = None
        try:
            conn = await self.pool.acquire()
            event_time = datetime.now(pytz.utc)  # Garantir UTC timezone
            await conn.execute('''
                INSERT INTO pending_voice_events 
                (event_type, user_id, guild_id, before_channel_id, after_channel_id,
                 before_self_deaf, before_deaf, after_self_deaf, after_deaf, event_time)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', 
            event_type,
            user_id,
            guild_id,
            before_channel_id,
            after_channel_id,
            before_self_deaf,
            before_deaf,
            after_self_deaf,
            after_deaf,
            event_time)
        except Exception as e:
            logger.error(f"Erro ao salvar evento pendente: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_pending_voice_events(self, limit: int = 100) -> List[Dict]:
        """Obtém eventos de voz pendentes para processamento"""
        conn = None
        try:
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT * FROM pending_voice_events
                WHERE processed = FALSE
                ORDER BY event_time ASC
                LIMIT $1
            ''', limit)
            
            # Converter os resultados para dicionários
            events = []
            for row in results:
                event = dict(row)
                # Remover campos desnecessários e garantir tipos corretos
                event['before_channel_id'] = event.get('before_channel_id')
                event['after_channel_id'] = event.get('after_channel_id')
                event['before_self_deaf'] = bool(event.get('before_self_deaf', False))
                event['before_deaf'] = bool(event.get('before_deaf', False))
                event['after_self_deaf'] = bool(event.get('after_self_deaf', False))
                event['after_deaf'] = bool(event.get('after_deaf', False))
                events.append(event)
                
            return events
        except Exception as e:
            logger.error(f"Erro ao obter eventos pendentes: {e}")
            return []
        finally:
            if conn:
                await self.pool.release(conn)

    async def mark_events_as_processed(self, event_ids: List[int]):
        """Marca eventos como processados"""
        if not event_ids:
            return
            
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                UPDATE pending_voice_events
                SET processed = TRUE
                WHERE id = ANY($1)
            ''', event_ids)
        except Exception as e:
            logger.error(f"Erro ao marcar eventos como processados: {e}")
        finally:
            if conn:
                await self.pool.release(conn)

    async def save_config(self, guild_id: int, config: dict):
        """Salva configuração com cache"""
        conn = None
        try:
            # Atualizar cache
            self._config_cache[guild_id] = config
            self._last_config_update = datetime.now(pytz.utc)
            
            # Serializar para JSON
            config_json = json.dumps(config)
            
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO bot_config (guild_id, config_json, last_updated)
                VALUES ($1, $2, NOW())
                ON CONFLICT (guild_id) DO UPDATE
                SET config_json = EXCLUDED.config_json,
                    last_updated = EXCLUDED.last_updated
            ''', guild_id, config_json)
            
            logger.info(f"Configuração salva no banco de dados para a guild {guild_id}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
            # Remover do cache em caso de erro
            if guild_id in self._config_cache:
                del self._config_cache[guild_id]
            return False
        finally:
            if conn:
                await self.pool.release(conn)

    async def load_config(self, guild_id: int) -> Optional[dict]:
        """Carrega configuração com cache"""
        # Se o banco não estiver disponível, retorne a configuração padrão
        if not self.pool:
            logger.warning("Pool de conexões não disponível - retornando configuração padrão")
            return DEFAULT_CONFIG.get(guild_id, None)
        
        # Forçar atualização do cache a cada hora
        if self._last_config_update and (datetime.now(pytz.utc) - self._last_config_update).total_seconds() > 3600:
            if guild_id in self._config_cache:
                del self._config_cache[guild_id]
        
        # Verificar cache primeiro
        if guild_id in self._config_cache:
            logger.debug(f"Retornando configuração do cache para guild {guild_id}")
            return self._config_cache[guild_id]
        
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT config_json FROM bot_config
                WHERE guild_id = $1
            ''', guild_id)
            
            if result:
                config = json.loads(result['config_json'])
                # Atualizar cache
                self._config_cache[guild_id] = config
                self._last_config_update = datetime.now(pytz.utc)
                
                logger.info(f"Configuração carregada do banco de dados para a guild {guild_id}")
                return config
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            return None
        finally:
            if conn:
                await self.pool.release(conn)

    async def load_configs(self, guild_ids: List[int]) -> Dict[int, dict]:
        """Carrega configurações para múltiplas guilds de uma vez"""
        if not guild_ids:
            return {}

        conn = None
        try:
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT guild_id, config_json FROM bot_config
                WHERE guild_id = ANY($1)
            ''', guild_ids)
            
            configs = {}
            for row in results:
                try:
                    configs[row['guild_id']] = json.loads(row['config_json'])
                    # Atualizar cache
                    self._config_cache[row['guild_id']] = configs[row['guild_id']]
                except json.JSONDecodeError as e:
                    logger.error(f"Erro ao decodificar JSON para guild {row['guild_id']}: {e}")
            
            self._last_config_update = datetime.now(pytz.utc)
            return configs
        except Exception as e:
            logger.error(f"Erro ao carregar configurações múltiplas: {e}")
            return {}
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_voice_join(self, user_id: int, guild_id: int):
        """Registra entrada em canal de voz"""
        now = datetime.now(pytz.utc)
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO user_activity 
                (user_id, guild_id, last_voice_join, voice_sessions) 
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (user_id, guild_id) DO UPDATE 
                SET last_voice_join = EXCLUDED.last_voice_join,
                    voice_sessions = user_activity.voice_sessions + 1
            ''', user_id, guild_id, now)
        except Exception as e:
            logger.error(f"Erro ao registrar entrada em voz: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_voice_leave(self, user_id: int, guild_id: int, duration: int):
        """Registra saída de canal de voz"""
        now = datetime.now(pytz.utc)
        conn = None
        try:
            conn = await self.pool.acquire()
            async with conn.transaction():
                # Atualizar atividade do usuário
                await conn.execute('''
                    UPDATE user_activity 
                    SET last_voice_leave = $1,
                        total_voice_time = total_voice_time + $2
                    WHERE user_id = $3 AND guild_id = $4
                ''', now, duration, user_id, guild_id)
                
                # Registrar sessão de voz
                join_time = now - timedelta(seconds=duration)
                await conn.execute('''
                    INSERT INTO voice_sessions
                    (user_id, guild_id, join_time, leave_time, duration)
                    VALUES ($1, $2, $3, $4, $5)
                ''', user_id, guild_id, join_time, now, duration)
                
        except Exception as e:
            logger.error(f"Erro ao registrar saída de voz: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_user_activity(self, user_id: int, guild_id: int) -> Dict:
        """Obtém dados de atividade do usuário"""
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT last_voice_join, last_voice_leave, voice_sessions, total_voice_time 
                FROM user_activity 
                WHERE user_id = $1 AND guild_id = $2
            ''', user_id, guild_id)
            
            return dict(result) if result else {}
        except Exception as e:
            logger.error(f"Erro ao obter atividade do usuário: {e}")
            return {}
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_voice_sessions(self, user_id: int, guild_id: int, 
                               start_date: datetime, end_date: datetime) -> List[Dict]:
        """Obtém sessões de voz do usuário em um período"""
        conn = None
        try:
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT join_time, leave_time, duration 
                FROM voice_sessions
                WHERE user_id = $1 AND guild_id = $2
                AND join_time >= $3 AND leave_time <= $4
                ORDER BY join_time
            ''', user_id, guild_id, start_date, end_date)
            
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Erro ao obter sessões de voz: {e}")
            return []
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_period_check(self, user_id: int, guild_id: int, 
                             start_date: datetime, end_date: datetime, 
                             meets_requirements: bool):
        """Registra verificação de período"""
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO checked_periods
                (user_id, guild_id, period_start, period_end, meets_requirements)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, guild_id, period_start) DO UPDATE
                SET meets_requirements = EXCLUDED.meets_requirements
            ''', user_id, guild_id, start_date, end_date, meets_requirements)
        except Exception as e:
            logger.error(f"Erro ao registrar verificação de período: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_last_period_check(self, user_id: int, guild_id: int) -> Optional[Dict]:
        """Obtém última verificação de período"""
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT period_start, period_end, meets_requirements
                FROM checked_periods
                WHERE user_id = $1 AND guild_id = $2
                ORDER BY period_start DESC
                LIMIT 1
            ''', user_id, guild_id)
            
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Erro ao obter última verificação de período: {e}")
            return None
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_warning(self, user_id: int, guild_id: int, warning_type: str):
        """Registra aviso enviado ao usuário"""
        now = datetime.now(pytz.utc)
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO user_warnings 
                (user_id, guild_id, warning_type, warning_date) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id, guild_id, warning_type) DO UPDATE 
                SET warning_date = EXCLUDED.warning_date
            ''', user_id, guild_id, warning_type, now)
        except Exception as e:
            logger.error(f"Erro ao registrar aviso: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_last_warning(self, user_id: int, guild_id: int) -> Optional[Tuple[str, datetime]]:
        """Obtém último aviso enviado ao usuário"""
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT warning_type, warning_date 
                FROM user_warnings 
                WHERE user_id = $1 AND guild_id = $2
                ORDER BY warning_date DESC
                LIMIT 1
            ''', user_id, guild_id)
            
            if result:
                return result['warning_type'], result['warning_date']
            return None
        except Exception as e:
            logger.error(f"Erro ao obter último aviso: {e}")
            return None
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_removed_roles(self, user_id: int, guild_id: int, role_ids: List[int]):
        """Registra cargos removidos por inatividade (COM transação)."""
        conn = None
        try:
            conn = await self.pool.acquire()
            async with conn.transaction():
                for role_id in role_ids:
                    await conn.execute('''
                        INSERT INTO removed_roles 
                        (user_id, guild_id, role_id, removal_date) 
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id, guild_id, role_id) DO UPDATE 
                        SET removal_date = EXCLUDED.removal_date
                    ''', user_id, guild_id, role_id, datetime.now(pytz.utc))
        except Exception as e:
            logger.error(f"Erro ao registrar cargos removidos: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_kicked_member(self, user_id: int, guild_id: int, reason: str):
        """Registra membro expulso por inatividade"""
        now = datetime.now(pytz.utc)
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO kicked_members 
                (user_id, guild_id, kick_date, reason) 
                VALUES ($1, $2, $3, $4)
            ''', user_id, guild_id, now, reason)
        except Exception as e:
            logger.error(f"Erro ao registrar membro expulso: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_last_kick(self, user_id: int, guild_id: int) -> Optional[Dict]:
        """Obtém última expulsão do usuário"""
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT kick_date 
                FROM kicked_members
                WHERE user_id = $1 AND guild_id = $2
                ORDER BY kick_date DESC
                LIMIT 1
            ''', user_id, guild_id)
            
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Erro ao obter última expulsão: {e}")
            return None
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_members_with_tracked_roles(self, guild_id: int, role_ids: List[int]) -> List[int]:
        """Obtém todos os membros que possuem pelo menos um dos cargos monitorados"""
        if not role_ids:
            return []

        conn = None
        try:
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT DISTINCT user_id 
                FROM user_activity
                WHERE guild_id = $1
                AND EXISTS (
                    SELECT 1 FROM removed_roles 
                    WHERE removed_roles.user_id = user_activity.user_id 
                    AND removed_roles.guild_id = user_activity.guild_id
                    AND removed_roles.role_id = ANY($2)
                )
            ''', guild_id, role_ids)
            
            return [r['user_id'] for r in results] if results else []
        except Exception as e:
            logger.error(f"Erro ao buscar membros com cargos monitorados: {e}")
            return []
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_last_periods_batch(self, user_ids: List[int], guild_id: int) -> Dict[int, Dict]:
        """Obtém os últimos períodos verificados para um lote de usuários"""
        if not user_ids:
            return {}

        conn = None
        try:
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT DISTINCT ON (user_id) 
                    user_id, period_start, period_end, meets_requirements
                FROM checked_periods
                WHERE user_id = ANY($1) AND guild_id = $2
                ORDER BY user_id, period_start DESC
            ''', user_ids, guild_id)
            
            last_periods = {}
            for row in results:
                last_periods[row['user_id']] = {
                    'period_start': row['period_start'],
                    'period_end': row['period_end'],
                    'meets_requirements': row['meets_requirements']
                }
            
            return last_periods
        except Exception as e:
            logger.error(f"Erro ao obter últimos períodos em lote: {e}")
            return {}
        finally:
            if conn:
                await self.pool.release(conn)

    async def cleanup_old_data(self, days: int = 60):
        """Limpa dados antigos do banco de dados"""
        conn = None
        try:
            conn = await self.pool.acquire()
            async with conn.transaction():
                # Limpar sessões de voz antigas
                voice_deleted = await conn.execute("DELETE FROM voice_sessions WHERE leave_time < NOW() - $1 * INTERVAL '1 day'", days)
                
                # Limpar avisos antigos
                warnings_deleted = await conn.execute("DELETE FROM user_warnings WHERE warning_date < NOW() - $1 * INTERVAL '1 day'", days)
                
                # Limpar registros de cargos removidos antigos
                roles_deleted = await conn.execute("DELETE FROM removed_roles WHERE removal_date < NOW() - $1 * INTERVAL '1 day'", days)
                
                # Limpar membros expulsos antigos
                kicks_deleted = await conn.execute("DELETE FROM kicked_members WHERE kick_date < NOW() - $1 * INTERVAL '1 day'", days)
                
                # Limpar logs de rate limit antigos
                rate_limits_deleted = await conn.execute("DELETE FROM rate_limit_logs WHERE log_date < NOW() - $1 * INTERVAL '1 day'", days)
                
                # Limpar eventos de voz pendentes antigos (nova funcionalidade)
                pending_events_deleted = await conn.execute("DELETE FROM pending_voice_events WHERE event_time < NOW() - $1 * INTERVAL '1 day'", days)
                
                log_message = (
                    f"Limpeza de dados antigos concluída: "
                    f"Sessões de voz: {voice_deleted.split()[1]}, "
                    f"Avisos: {warnings_deleted.split()[1]}, "
                    f"Cargos removidos: {roles_deleted.split()[1]}, "
                    f"Expulsões: {kicks_deleted.split()[1]}, "
                    f"Rate limits: {rate_limits_deleted.split()[1]}, "
                    f"Eventos pendentes: {pending_events_deleted.split()[1]}"
                )
                logger.info(log_message)
                return log_message
        except Exception as e:
            logger.error(f"Erro ao limpar dados antigos: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_rate_limit_history(self, guild_id: int, hours: int = 24) -> List[Dict]:
        """Obtém histórico de rate limits para uma guild"""
        conn = None
        try:
            since = datetime.now(pytz.utc) - timedelta(hours=hours)
            conn = await self.pool.acquire()
            results = await conn.fetch('''
                SELECT bucket, limit_count, remaining, reset_at, scope, endpoint, retry_after, log_date
                FROM rate_limit_logs
                WHERE guild_id = $1 AND log_date >= $2
                ORDER BY log_date DESC
            ''', guild_id, since)
            
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Erro ao obter histórico de rate limits: {e}")
            return []
        finally:
            if conn:
                await self.pool.release(conn)

    async def cleanup_rate_limit_logs(self, days: int = 7):
        """Limpa logs de rate limit antigos"""
        conn = None
        try:
            cutoff_date = datetime.now(pytz.utc) - timedelta(days=days)
            conn = await self.pool.acquire()
            result = await conn.execute("DELETE FROM rate_limit_logs WHERE log_date < $1", cutoff_date)
            deleted_count = int(result.split()[1])
            logger.info(f"Removidos {deleted_count} logs de rate limit antigos")
            return deleted_count
        except Exception as e:
            logger.error(f"Erro ao limpar logs de rate limit: {e}")
            return 0
        finally:
            if conn:
                await self.pool.release(conn)

    async def get_last_task_execution(self, task_name: str) -> Optional[Dict]:
        """Obtém a última execução de uma task"""
        conn = None
        try:
            conn = await self.pool.acquire()
            result = await conn.fetchrow('''
                SELECT last_execution, monitoring_period 
                FROM task_executions 
                WHERE task_name = $1
            ''', task_name)
            
            if result:
                # Garantir que o datetime retornado está com timezone
                last_execution = result['last_execution']
                if last_execution and last_execution.tzinfo is None:
                    last_execution = last_execution.replace(tzinfo=pytz.utc)
                    result = dict(result)  # Criar uma cópia mutável
                    result['last_execution'] = last_execution
                
                return result
            return None
        except Exception as e:
            logger.error(f"Erro ao obter última execução da task: {e}")
            return None
        finally:
            if conn:
                await self.pool.release(conn)

    async def log_task_execution(self, task_name: str, monitoring_period: int):
        """Registra execução de uma task"""
        conn = None
        try:
            conn = await self.pool.acquire()
            await conn.execute('''
                INSERT INTO task_executions 
                (task_name, last_execution, monitoring_period) 
                VALUES ($1, $2, $3)
                ON CONFLICT (task_name) DO UPDATE 
                SET last_execution = EXCLUDED.last_execution,
                    monitoring_period = EXCLUDED.monitoring_period
            ''', task_name, datetime.now(pytz.utc), monitoring_period)
        except Exception as e:
            logger.error(f"Erro ao registrar execução da task: {e}")
            raise
        finally:
            if conn:
                await self.pool.release(conn)

    async def health_check(self):
        """Verifica a saúde do banco de dados e reinicia tasks se necessário"""
        # Verificação básica de conexão
        if not self.pool:
            return False
            
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            
            # Verifica se as tasks estão rodando
            active_tasks = {t._name for t in asyncio.all_tasks() if hasattr(t, '_name') and t._name}
            expected_tasks = {'database_heartbeat'}
            
            for task_name in expected_tasks:
                if task_name not in active_tasks:
                    logger.warning(f"Task {task_name} não está ativa - reiniciando...")
                    if task_name == 'database_heartbeat':
                        self.heartbeat_task = asyncio.create_task(self._db_heartbeat(interval=300))
                        self.heartbeat_task._name = 'database_heartbeat'
                        self._active_tasks.add(self.heartbeat_task)
            
            # Verifica o status do pool de conexões
            pool_status = await self.check_pool_status()
            if pool_status:
                logger.info(f"Status do pool de conexões: {pool_status}")
            
            return True
        except Exception as e:
            logger.error(f"Erro na verificação de saúde do banco de dados: {e}")
            return False