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
from typing import Dict, List, Optional, Tuple
from flask import Flask, jsonify
from threading import Thread

# Configurações iniciais
CONFIG_FILE = 'config.json'
DEFAULT_CONFIG = {
    "required_minutes": 15,  # Minutos necessários por sessão
    "required_days": 2,      # Dias diferentes com atividade necessária
    "monitoring_period": 14, # Período de monitoramento em dias
    "kick_after_days": 30,
    "tracked_roles": [],
    "log_channel": 1376013013206827161,  # Canal principal do bot
    "notification_channel": None,  # Canal para notificar cargos
    "timezone": "America/Sao_Paulo",
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

# Configuração do Flask para health checks
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"status": "ok", "message": "Bot is running"})

def run_flask():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

class Database:
    def __init__(self):
        self.connection = None
        self.connect(retries=3, delay=5)
        self.create_tables()

    def connect(self, retries=3, delay=5):
        for attempt in range(retries):
            try:
                self.connection = mysql.connector.connect(
                    host=os.getenv('DB_HOST'),
                    database=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASS'),
                    port=int(os.getenv('DB_PORT', 3306)),
                    pool_name="bot_pool",
                    pool_size=3,
                    pool_reset_session=True
                )
                if self.connection.is_connected():
                    print("Conexão ao MySQL estabelecida com sucesso")
                    return
            except Error as e:
                print(f"Erro ao conectar ao MySQL (tentativa {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise

    @staticmethod
    def ensure_connection(func):
        def wrapper(self, *args, **kwargs):
            try:
                if not self.connection or not self.connection.is_connected():
                    self.connect()
                return func(self, *args, **kwargs)
            except (OperationalError, InterfaceError) as e:
                print(f"Erro de conexão, tentando reconectar: {e}")
                self.connect()
                return func(self, *args, **kwargs)
        return wrapper

    def reconnect(self):
        try:
            self.connection.reconnect(attempts=3, delay=1)
        except:
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
                PRIMARY KEY (user_id, guild_id)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS voice_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                guild_id BIGINT,
                join_time DATETIME,
                leave_time DATETIME,
                duration INT,
                INDEX (user_id, guild_id, join_time)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_warnings (
                user_id BIGINT,
                guild_id BIGINT,
                warning_type VARCHAR(20),
                warning_date DATETIME,
                PRIMARY KEY (user_id, guild_id, warning_type)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS removed_roles (
                user_id BIGINT,
                guild_id BIGINT,
                role_id BIGINT,
                removal_date DATETIME,
                PRIMARY KEY (user_id, guild_id, role_id)
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS kicked_members (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                guild_id BIGINT,
                kick_date DATETIME,
                reason TEXT
            )''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS checked_periods (
                user_id BIGINT,
                guild_id BIGINT,
                period_start DATETIME,
                period_end DATETIME,
                meets_requirements BOOLEAN,
                PRIMARY KEY (user_id, guild_id, period_start)
            )''')
            
            self.connection.commit()
            print("Tabelas criadas/verificadas com sucesso")
        except Error as e:
            print(f"Erro ao criar tabelas: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_voice_join(self, user_id: int, guild_id: int):
        cursor = None
        now = datetime.utcnow()
        
        try:
            cursor = self.connection.cursor()
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
            print(f"Erro ao registrar entrada em voz: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_voice_leave(self, user_id: int, guild_id: int, duration: int):
        cursor = None
        now = datetime.utcnow()
        
        try:
            cursor = self.connection.cursor()
            
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
            print(f"Erro ao registrar saída de voz: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def get_user_activity(self, user_id: int, guild_id: int) -> Dict:
        cursor = None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute('''
            SELECT last_voice_join, last_voice_leave, voice_sessions, total_voice_time 
            FROM user_activity 
            WHERE user_id = %s AND guild_id = %s
            ''', (user_id, guild_id))
            
            result = cursor.fetchone()
            return result if result else {}
        except Error as e:
            print(f"Erro ao obter atividade do usuário: {e}")
            return {}
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def get_voice_sessions(self, user_id: int, guild_id: int, start_date: datetime, end_date: datetime) -> List[Dict]:
        cursor = None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute('''
            SELECT join_time, leave_time, duration 
            FROM voice_sessions
            WHERE user_id = %s AND guild_id = %s
            AND join_time >= %s AND leave_time <= %s
            ORDER BY join_time
            ''', (user_id, guild_id, start_date, end_date))
            
            return cursor.fetchall()
        except Error as e:
            print(f"Erro ao obter sessões de voz: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_period_check(self, user_id: int, guild_id: int, start_date: datetime, end_date: datetime, meets_requirements: bool):
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
            INSERT INTO checked_periods
            (user_id, guild_id, period_start, period_end, meets_requirements)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                meets_requirements = VALUES(meets_requirements)
            ''', (user_id, guild_id, start_date, end_date, meets_requirements))
            
            self.connection.commit()
        except Error as e:
            print(f"Erro ao registrar verificação de período: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def get_last_period_check(self, user_id: int, guild_id: int) -> Optional[Dict]:
        cursor = None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute('''
            SELECT period_start, period_end, meets_requirements
            FROM checked_periods
            WHERE user_id = %s AND guild_id = %s
            ORDER BY period_start DESC
            LIMIT 1
            ''', (user_id, guild_id))
            
            return cursor.fetchone()
        except Error as e:
            print(f"Erro ao obter última verificação de período: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_warning(self, user_id: int, guild_id: int, warning_type: str):
        cursor = None
        now = datetime.utcnow()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
            INSERT INTO user_warnings 
            (user_id, guild_id, warning_type, warning_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                warning_date = VALUES(warning_date)
            ''', (user_id, guild_id, warning_type, now))
            
            self.connection.commit()
        except Error as e:
            print(f"Erro ao registrar aviso: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def get_last_warning(self, user_id: int, guild_id: int) -> Optional[Tuple[str, datetime]]:
        cursor = None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
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
            print(f"Erro ao obter último aviso: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_removed_roles(self, user_id: int, guild_id: int, role_ids: List[int]):
        cursor = None
        now = datetime.utcnow()
        
        try:
            cursor = self.connection.cursor()
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
            print(f"Erro ao registrar cargos removidos: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    @ensure_connection
    def log_kicked_member(self, user_id: int, guild_id: int, reason: str):
        cursor = None
        now = datetime.utcnow()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
            INSERT INTO kicked_members 
            (user_id, guild_id, kick_date, reason) 
            VALUES (%s, %s, %s, %s)
            ''', (user_id, guild_id, now, reason))
            
            self.connection.commit()
        except Error as e:
            print(f"Erro ao registrar membro expulso: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

class InactivityBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = {}
        self.load_config()
        self.timezone = pytz.timezone(self.config.get('timezone', 'America/Sao_Paulo'))
        self.db = None
        self.initialize_db()
        self.active_sessions = {}

    def initialize_db(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.db = Database()
                break
            except Error as e:
                print(f"Tentativa {attempt + 1} de conexão ao banco de dados falhou: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = DEFAULT_CONFIG
            self.save_config()

    def save_config(self):
        with open(CONFIG_FILE, 'w') as f:
            json.dump(self.config, f, indent=4)

    async def log_action(self, action: str, member: discord.Member, details: str = None):
        channel = self.get_channel(self.config.get('log_channel', 1376013013206827161))
        if channel:
            embed = discord.Embed(
                title=f"Ação: {action}",
                description=f"Usuário: {member.mention}",
                color=discord.Color.orange(),
                timestamp=datetime.now(self.timezone)
            )
            if details:
                embed.add_field(name="Detalhes", value=details, inline=False)
            await channel.send(embed=embed)

    async def notify_roles(self, message: str):
        channel = self.get_channel(self.config.get('notification_channel'))
        if channel:
            await channel.send(message)

    async def send_dm(self, member: discord.Member, message: str):
        try:
            await member.send(message)
        except discord.Forbidden:
            await self.log_action("DM Falhou", member, "Não foi possível enviar mensagem direta para o usuário.")

    async def send_warning(self, member: discord.Member, warning_type: str):
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
        self.db.log_warning(member.id, member.guild.id, warning_type)
        await self.log_action(f"Aviso Enviado ({warning_type})", member)

intents = discord.Intents.default()
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = InactivityBot(command_prefix='!', intents=intents)

@bot.event
async def on_ready():
    print(f'Bot conectado como {bot.user}')
    try:
        synced = await bot.tree.sync()
        print(f"Comandos slash sincronizados: {len(synced)} comandos")
    except Exception as e:
        print(f"Erro ao sincronizar comandos slash: {e}")
    
    inactivity_check.start()
    cleanup_members.start()
    check_warnings.start()
    await bot.notify_roles("🤖 Bot de Inatividade iniciado com sucesso!")

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    try:
        if before.channel is None and after.channel is not None:
            try:
                bot.db.log_voice_join(member.id, member.guild.id)
                bot.active_sessions[(member.id, member.guild.id)] = datetime.utcnow()
                await bot.log_action("Entrou em voz", member, f"Canal: {after.channel.name}")
            except Exception as e:
                print(f"Erro ao registrar entrada em voz: {e}")
                await bot.log_action("Erro DB - Entrada em voz", member, str(e))
        
        elif before.channel is not None and after.channel is None:
            session_start = bot.active_sessions.get((member.id, member.guild.id))
            if session_start:
                duration = (datetime.utcnow() - session_start).total_seconds()
                if duration >= bot.config['required_minutes'] * 60:
                    try:
                        bot.db.log_voice_leave(member.id, member.guild.id, int(duration))
                    except Exception as e:
                        print(f"Erro ao registrar saída de voz: {e}")
                        await bot.log_action("Erro DB - Saída de voz", member, str(e))
                del bot.active_sessions[(member.id, member.guild.id)]
                await bot.log_action("Saiu de voz", member, 
                                   f"Canal: {before.channel.name} | Duração: {int(duration//60)} minutos")
    except Exception as e:
        print(f"Erro ao processar atualização de estado de voz: {e}")

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
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = bot.db.get_last_period_check(member.id, guild.id)
                    now = datetime.now(bot.timezone)
                    
                    if last_check:
                        last_period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                        if now < last_period_end:
                            continue
                    
                    period_end = now
                    period_start = period_end - timedelta(days=monitoring_period)
                    
                    sessions = bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
                    
                    meets_requirements = False
                    if sessions:
                        valid_days = set()
                        for session in sessions:
                            if session['duration'] >= required_minutes * 60:
                                day = session['join_time'].replace(tzinfo=bot.timezone).date()
                                valid_days.add(day)
                        
                        meets_requirements = len(valid_days) >= required_days
                    
                    bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
                    
                    if not meets_requirements:
                        roles_to_remove = [role for role in member.roles if role.id in tracked_roles]
                        if roles_to_remove:
                            try:
                                await member.remove_roles(*roles_to_remove)
                                await bot.send_warning(member, 'final')
                                bot.db.log_removed_roles(member.id, guild.id, [r.id for r in roles_to_remove])
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
                    print(f"Erro ao verificar inatividade para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        print(f"Falha ao reconectar ao banco de dados: {db_error}")

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
            if any(role.id in tracked_roles for role in member.roles):
                try:
                    last_check = bot.db.get_last_period_check(member.id, guild.id)
                    if not last_check:
                        continue
                    
                    period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
                    days_remaining = (period_end - datetime.now(bot.timezone)).days
                    
                    last_warning = bot.db.get_last_warning(member.id, guild.id)
                    
                    if days_remaining <= first_warning_days and (
                        not last_warning or last_warning[0] != 'first'):
                        await bot.send_warning(member, 'first')
                    
                    elif days_remaining <= second_warning_days and (
                        not last_warning or last_warning[0] != 'second'):
                        await bot.send_warning(member, 'second')
                except Exception as e:
                    print(f"Erro ao verificar avisos para {member}: {e}")
                    try:
                        bot.db.reconnect()
                    except Exception as db_error:
                        print(f"Falha ao reconectar ao banco de dados: {db_error}")

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
                if len(member.roles) == 1:
                    joined_at = member.joined_at.replace(tzinfo=bot.timezone) if member.joined_at else None
                    if joined_at and joined_at < cutoff_date:
                        try:
                            await member.kick(reason=f"Sem cargos há mais de {kick_after_days} dias")
                            bot.db.log_kicked_member(member.id, guild.id, f"Sem cargos há mais de {kick_after_days} dias")
                            await bot.log_action(
                                "Membro Expulso",
                                member,
                                f"Motivo: Sem cargos há mais de {kick_after_days} dias")
                            await bot.notify_roles(
                                f"👢 {member.mention} foi expulso por estar sem cargos há mais de {kick_after_days} dias")
                        except discord.Forbidden:
                            await bot.log_action("Erro ao Expulsar", member, "Permissões insuficientes")
            except Exception as e:
                print(f"Erro ao verificar membro para expulsão {member}: {e}")
                try:
                    bot.db.reconnect()
                except Exception as db_error:
                    print(f"Falha ao reconectar ao banco de dados: {db_error}")

@bot.tree.command(name="set_inactivity", description="Define o número de dias do período de monitoramento")
@commands.has_permissions(administrator=True)
async def set_inactivity(interaction: discord.Interaction, days: int):
    bot.config['monitoring_period'] = days
    bot.save_config()
    await interaction.response.send_message(
        f"Configuração atualizada: Período de monitoramento definido para {days} dias.")

@bot.tree.command(name="set_requirements", description="Define os requisitos de atividade (minutos e dias)")
@commands.has_permissions(administrator=True)
async def set_requirements(interaction: discord.Interaction, minutes: int, days: int):
    bot.config['required_minutes'] = minutes
    bot.config['required_days'] = days
    bot.save_config()
    await interaction.response.send_message(
        f"Configuração atualizada: Requisitos definidos para {minutes} minutos em {days} dias diferentes "
        f"dentro de {bot.config['monitoring_period']} dias.")

@bot.tree.command(name="set_kick_days", description="Define após quantos dias sem cargo o membro será expulso")
@commands.has_permissions(administrator=True)
async def set_kick_days(interaction: discord.Interaction, days: int):
    bot.config['kick_after_days'] = days
    bot.save_config()
    await interaction.response.send_message(
        f"Configuração atualizada: Membros sem cargo serão expulsos após {days} dias.")

@bot.tree.command(name="add_tracked_role", description="Adiciona um cargo à lista de cargos monitorados")
@commands.has_permissions(administrator=True)
async def add_tracked_role(interaction: discord.Interaction, role: discord.Role):
    if role.id not in bot.config['tracked_roles']:
        bot.config['tracked_roles'].append(role.id)
        bot.save_config()
        await interaction.response.send_message(f"Cargo {role.name} adicionado à lista de monitorados.")
        await bot.notify_roles(f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.")
    else:
        await interaction.response.send_message("Este cargo já está sendo monitorado.")

@bot.tree.command(name="remove_tracked_role", description="Remove um cargo da lista de cargos monitorados")
@commands.has_permissions(administrator=True)
async def remove_tracked_role(interaction: discord.Interaction, role: discord.Role):
    if role.id in bot.config['tracked_roles']:
        bot.config['tracked_roles'].remove(role.id)
        bot.save_config()
        await interaction.response.send_message(f"Cargo {role.name} removido da lista de monitorados.")
        await bot.notify_roles(f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.")
    else:
        await interaction.response.send_message("Este cargo não estava sendo monitorado.")

@bot.tree.command(name="set_notification_channel", description="Define o canal para notificações de cargos")
@commands.has_permissions(administrator=True)
async def set_notification_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    bot.config['notification_channel'] = channel.id
    bot.save_config()
    await interaction.response.send_message(f"Canal de notificações definido para {channel.mention}")
    await channel.send("✅ Este canal foi definido como o canal de notificações de cargos!")

@bot.tree.command(name="set_warning_days", description="Define os dias para os avisos de inatividade")
@commands.has_permissions(administrator=True)
async def set_warning_days(interaction: discord.Interaction, first: int, second: int):
    if first <= second:
        return await interaction.response.send_message(
            "O primeiro aviso deve ser enviado antes do segundo aviso.")
    
    bot.config['warnings']['first_warning'] = first
    bot.config['warnings']['second_warning'] = second
    bot.save_config()
    await interaction.response.send_message(
        f"Avisos configurados: primeiro aviso {first} dias antes, segundo aviso {second} dia(s) antes.")

@bot.tree.command(name="set_warning_message", description="Define a mensagem para um tipo de aviso")
@commands.has_permissions(administrator=True)
async def set_warning_message(interaction: discord.Interaction, warning_type: str, message: str):
    if warning_type not in ['first', 'second', 'final']:
        return await interaction.response.send_message(
            "Tipo de aviso inválido. Use 'first', 'second' ou 'final'.")
    
    bot.config['warnings']['messages'][warning_type] = message
    bot.save_config()
    await interaction.response.send_message(f"Mensagem de {warning_type} atualizada com sucesso.")

@bot.tree.command(name="show_config", description="Mostra a configuração atual do bot")
@commands.has_permissions(administrator=True)
async def show_config(interaction: discord.Interaction):
    config = bot.config
    tracked_roles = []
    for role_id in config['tracked_roles']:
        role = interaction.guild.get_role(role_id)
        if role:
            tracked_roles.append(role.name)
    
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
        name="Cargos Monitorados",
        value="\n".join(tracked_roles) if tracked_roles else "Nenhum",
        inline=False)
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

@bot.tree.command(name="check_user", description="Verifica a atividade de um usuário")
@commands.has_permissions(administrator=True)
async def check_user(interaction: discord.Interaction, member: discord.Member):
    try:
        user_data = bot.db.get_user_activity(member.id, member.guild.id)
        last_join = user_data.get('last_voice_join')
        sessions = user_data.get('voice_sessions', 0)
        total_time = user_data.get('total_voice_time', 0)
        last_warning = bot.db.get_last_warning(member.id, member.guild.id)
        last_check = bot.db.get_last_period_check(member.id, member.guild.id)
        
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
        print(f"Erro ao verificar usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            print(f"Falha ao reconectar ao banco de dados: {db_error}")

@bot.tree.command(name="check_user_history", description="Verifica o histórico completo de um usuário")
@commands.has_permissions(administrator=True)
async def check_user_history(interaction: discord.Interaction, member: discord.Member):
    try:
        user_data = bot.db.get_user_activity(member.id, member.guild.id)
        last_warning = bot.db.get_last_warning(member.id, member.guild.id)
        last_check = bot.db.get_last_period_check(member.id, member.guild.id)
        
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
        print(f"Erro ao verificar histórico do usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o histórico do usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            print(f"Falha ao reconectar ao banco de dados: {db_error}")

# Iniciar o bot
if __name__ == "__main__":
    from dotenv import load_dotenv
    
    load_dotenv()
    
    required_env_vars = ['DISCORD_TOKEN', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}")
    
    keep_alive()
    
    bot.run(os.getenv('DISCORD_TOKEN'))