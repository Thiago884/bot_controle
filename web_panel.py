from flask import Flask, jsonify, render_template, request, redirect, url_for, Response, send_from_directory
from threading import Thread
import threading
from main import bot
import asyncio
import datetime
from datetime import timedelta
import pytz
import os
import json
from collections import deque
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps
import discord
import aiomysql
import psutil
from werkzeug.middleware.proxy_fix import ProxyFix
import time
import sys
import encodings.idna

# Adicione no início do arquivo, após os imports
bot._ready_event = asyncio.Event()
# Configuração básica do logger para o web panel
web_logger = logging.getLogger('web_panel')
web_logger.setLevel(logging.INFO)

# Configura o handler de arquivo com rotação
handler = RotatingFileHandler('web_panel.log', maxBytes=5 * 1024 * 1024, backupCount=3)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
web_logger.addHandler(handler)

# Adiciona um handler para console para depuração mais fácil durante o desenvolvimento
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
web_logger.addHandler(console_handler)

# Configure o diretório de templates
app = Flask(__name__, template_folder='templates')
app.config['SERVER_NAME'] = None
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Configuração de autenticação
WEB_AUTH_USER = os.getenv('WEB_AUTH_USER', 'admin')
WEB_AUTH_PASS = os.getenv('WEB_AUTH_PASS', 'admin123')

# Variáveis globais para controlar o estado do bot
bot_running = False
bot_initialized = False

# Verificar token do Discord antes de iniciar
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN:
    web_logger.critical("Token do Discord não encontrado! Verifique a variável de ambiente DISCORD_TOKEN")
    sys.exit(1)

# --- Lógica de Inicialização do Bot ---

# Em web_panel.py, modificar a função run_bot_in_thread:
def run_bot_in_thread():
    """Configura um loop de eventos asyncio dedicado e executa o bot nele."""
    def bot_runner():
        global bot_initialized
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            web_logger.info("Iniciando o loop de eventos do bot no thread de background.")
            global bot_running
            bot_running = True
            
            # Configurar o loop no bot antes de iniciar
            bot.loop = loop
            bot._ready_event = asyncio.Event()  # Criar o evento aqui
            
            # Esperar o bot ficar pronto
            loop.run_until_complete(bot.start(DISCORD_TOKEN))
            bot_initialized = True
        except discord.LoginFailure:
            web_logger.critical("Falha no login: Token do Discord inválido.")
        except Exception as e:
            web_logger.critical(f"Erro fatal ao executar o bot Discord no thread: {e}", exc_info=True)
        finally:
            web_logger.warning("O loop do bot foi finalizado. Fechando o bot.")
            bot_running = False
            bot_initialized = False
            if not bot.is_closed():
                loop.run_until_complete(bot.close())
            loop.close()

    # Inicia o bot em uma thread separada
    bot_thread = threading.Thread(target=bot_runner, daemon=True)
    bot_thread.start()

# --- Funções Auxiliares ---

def check_bot_initialized():
    if not hasattr(bot, 'loop') or not bot.loop or not bot_running or not bot_initialized:
        web_logger.error("Bot não inicializado corretamente")
        return False
    return True

# Decorator para autenticação
def basic_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            auth = request.authorization
            if not auth or auth.username != WEB_AUTH_USER or auth.password != WEB_AUTH_PASS:
                web_logger.warning(f"Tentativa de acesso não autorizado ao painel web. IP: {request.remote_addr}")
                return Response(
                    'Acesso não autorizado', 401, 
                    {'WWW-Authenticate': 'Basic realm="Login Required"'}
                )
            return f(*args, **kwargs)
        except Exception as e:
            web_logger.error(f"Erro na autenticação: {e}", exc_info=True)
            return Response(
                'Erro interno de autenticação', 500
            )
    return decorated

def run_coroutine_in_bot_loop(coro):
    """Executa uma corrotina no loop de eventos do bot e espera pelo resultado."""
    if not check_bot_initialized():
        web_logger.error("Loop do bot não está rodando. Não é possível executar corrotina.")
        raise RuntimeError("Bot loop is not running.")
    
    future = asyncio.run_coroutine_threadsafe(coro, bot.loop)
    try:
        return future.result(timeout=30)  # Aumentado timeout para 30 segundos
    except asyncio.TimeoutError:
        web_logger.error("Timeout ao executar corrotina no loop do bot.")
        raise TimeoutError("Coroutine execution timed out.")
    except Exception as e:
        web_logger.error(f"Erro ao obter resultado da corrotina: {e}", exc_info=True)
        raise

async def safe_db_operation(coro):
    """Executa operações no banco de dados com tratamento de erros"""
    try:
        return await run_coroutine_in_bot_loop(coro)
    except aiomysql.Error as e:
        web_logger.error(f"Erro de banco de dados: {e}", exc_info=True)
        return None
    except Exception as e:
        web_logger.error(f"Erro inesperado: {e}", exc_info=True)
        return None

def get_main_guild():
    """Obtém a guilda principal do bot"""
    if not hasattr(bot, 'guilds') or not bot.guilds:
        web_logger.warning("Nenhuma guilda disponível no cache do bot.")
        return None
    return bot.guilds[0]  # Assumindo que o bot está em apenas uma guilda

# Middleware para verificar se o bot está pronto
@app.before_request
def check_bot_ready():
    if request.path.startswith('/static') or request.path == '/keepalive' or request.path == '/health':
        return
        
    if not bot_running or not bot_initialized:
        return jsonify({'status': 'error', 'message': 'Bot não está rodando ou não foi inicializado'}), 503
        
    if not hasattr(bot, 'is_ready') or not bot.is_ready():
        return jsonify({'status': 'error', 'message': 'Bot não está completamente pronto'}), 503

# --- Rotas Web do Flask ---

# Rota para servir arquivos estáticos
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

# Rota keepalive para evitar timeout no Render
@app.route('/keepalive')
def keepalive():
    return jsonify({'status': 'alive'})

# Tratamento de erro para rotas não encontradas
@app.errorhandler(404)
def not_found(error):
    web_logger.warning(f"Página não encontrada: {request.path}")
    return render_template('error.html', error_message="Página não encontrada"), 404

# Novo endpoint de health check
@app.route('/health')
def health_check():
    try:
        bot_status = "running" if bot_running and bot_initialized and hasattr(bot, 'is_ready') and bot.is_ready() else "error"
        return jsonify({
            'status': 'ok' if bot_status == "running" else 'error',
            'bot_status': bot_status,
            'bot_ready': bot.is_ready() if hasattr(bot, 'is_ready') else False,
            'web_panel': 'running',
            'database': 'connected' if hasattr(bot, 'db') and bot.db else 'disconnected'
        }), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/')
@basic_auth_required
def home():
    try:
        if not bot_running or not bot_initialized:
            return render_template('error.html', error_message="Bot não está rodando ou não foi inicializado"), 503
            
        web_logger.info("Redirecionando para /dashboard")
        return redirect(url_for('dashboard'))
    except Exception as e:
        web_logger.error(f"Erro na rota principal: {e}", exc_info=True)
        return render_template('error.html', error_message="Erro interno do servidor"), 500

@app.route('/dashboard')
@basic_auth_required
def dashboard():
    try:
        web_logger.info("Acessando a rota dashboard")
        
        bot_user_name = bot.user.name if hasattr(bot, 'user') and bot.user else "Inactivity Bot"
        uptime = "N/A"
        if hasattr(bot, 'start_time'):
            delta = datetime.datetime.now() - bot.start_time
            uptime = str(delta).split('.')[0]
        
        guild_stats = []
        guild_count = 0
        if hasattr(bot, 'guilds') and bot.guilds:
            guild_count = len(bot.guilds)
            for guild in bot.guilds:
                voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
                guild_stats.append({
                    'id': guild.id,
                    'name': guild.name,
                    'members': guild.member_count,
                    'voice_channels': voice_channels,
                    'icon': guild.icon.url if guild.icon else None
                })
        
        db_status = "Desconhecido"
        pool_status = {
            'freesize': 0,
            'maxsize': 0,
            'size': 0,
            'used': 0
        }
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status_data = run_coroutine_in_bot_loop(bot.db.check_pool_status())
                if pool_status_data:
                    pool_status = pool_status_data
                    db_status = "Operacional" if pool_status.get('size', 0) > 0 else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
                web_logger.error(f"Erro ao verificar status do banco no dashboard: {e}", exc_info=True)
        
        queue_status = {
            'voice_events': 0,
            'messages': {'critical': 0, 'high': 0, 'normal': 0, 'low': 0}
        }
        if hasattr(bot, 'voice_event_queue') and bot.voice_event_queue:
            queue_status['voice_events'] = bot.voice_event_queue.qsize()
        if hasattr(bot, 'message_queue') and bot.message_queue:
            if isinstance(bot.message_queue, dict):
                queue_status['messages'] = {k: v.qsize() for k, v in bot.message_queue.items()}
            else:
                queue_status['messages'] = bot.message_queue.qsize()
        
        current_config = {
            'required_minutes': bot.config.get('required_minutes', 15) if hasattr(bot, 'config') else 15,
            'required_days': bot.config.get('required_days', 2) if hasattr(bot, 'config') else 2,
            'monitoring_period': bot.config.get('monitoring_period', 14) if hasattr(bot, 'config') else 14,
            'kick_after_days': bot.config.get('kick_after_days', 30) if hasattr(bot, 'config') else 30
        }
        
        # Obter informações do sistema
        system_info = {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'process_memory': psutil.Process().memory_info().rss / 1024 / 1024  # em MB
        }
        
        return render_template('dashboard.html', 
                            bot_name=bot_user_name,
                            guild_count=guild_count,
                            guild_stats=guild_stats,
                            uptime=uptime,
                            db_status=db_status,
                            pool_status=pool_status,
                            queue_status=queue_status,
                            current_config=current_config,
                            system_info=system_info)
    
    except Exception as e:
        web_logger.error(f"Erro fatal na rota dashboard: {e}", exc_info=True)
        return render_template('error.html', error_message=f"Erro ao carregar o dashboard: {e}"), 500

@app.route('/monitor')
@basic_auth_required
def monitor():
    try:
        web_logger.info("Acessando a rota monitor")
        
        system_status_data = get_system_status().json
        rate_limits_data = get_rate_limits().json
        recent_events_data = get_recent_events().json
        
        log_lines = []
        try:
            with open('bot.log', 'r', encoding='utf-8', errors='ignore') as log_file:
                log_lines = list(deque(log_file, 100))
        except FileNotFoundError:
            web_logger.warning("Arquivo de log 'bot.log' não encontrado para o monitor.")
            log_lines = ["Arquivo de log 'bot.log' não encontrado."]
        except Exception as e:
            web_logger.error(f"Erro ao ler arquivo de log para o monitor: {e}", exc_info=True)
            log_lines = [f"Erro ao carregar logs: {str(e)}"]
        
        bot_user_name = bot.user.name if hasattr(bot, 'user') and bot.user else "Inactivity Bot"
        
        return render_template('monitor.html',
                            bot_name=bot_user_name,
                            system_status=system_status_data,
                            rate_limits=rate_limits_data,
                            recent_events=recent_events_data,
                            log_lines=log_lines)
    
    except Exception as e:
        web_logger.error(f"Erro fatal na rota monitor: {e}", exc_info=True)
        return render_template('error.html', error_message=f"Erro ao carregar o monitor: {e}"), 500

@app.route('/api/panel_status')
@basic_auth_required
def panel_status():
    """Endpoint para verificar o status do painel e do bot"""
    try:
        return jsonify({
            'status': 'running',
            'bot_ready': bot.is_ready() if hasattr(bot, 'is_ready') else False,
            'guild_count': len(bot.guilds) if hasattr(bot, 'guilds') else 0,
            'last_heartbeat': datetime.datetime.now().isoformat(),
            'panel_version': '1.0.0'
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/panel_status: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system_info')
@basic_auth_required
def system_info():
    """Endpoint para obter informações do sistema"""
    try:
        return jsonify({
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'process_memory': psutil.Process().memory_info().rss / 1024 / 1024,  # em MB
            'uptime': str(datetime.datetime.now() - bot.start_time).split('.')[0] if hasattr(bot, 'start_time') else 'N/A'
        })
    except Exception as e:
        web_logger.error(f"Erro ao obter informações do sistema: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/status')
@basic_auth_required
def get_system_status():
    try:
        bot_status = "Operacional" if hasattr(bot, 'is_ready') and bot.is_ready() else "Erro"
        
        db_status = "Operacional"
        pool_status = {
            'freesize': 0,
            'maxsize': 0,
            'size': 0,
            'used': 0
        }
        
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status_data = run_coroutine_in_bot_loop(bot.db.check_pool_status())
                if pool_status_data:
                    pool_status = pool_status_data
                    db_status = "Operacional" if pool_status.get('size', 0) > 0 else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
                web_logger.error(f"Erro ao verificar status do banco em /api/status: {e}", exc_info=True)
        
        uptime = "N/A"
        if hasattr(bot, 'start_time'):
            delta = datetime.datetime.now() - bot.start_time
            uptime = str(delta).split('.')[0]
        
        guild_count = len(bot.guilds) if hasattr(bot, 'guilds') else 0
        
        queue_status = {
            'voice_events': 0,
            'messages': {'critical': 0, 'high': 0, 'normal': 0, 'low': 0}
        }
        if hasattr(bot, 'voice_event_queue') and bot.voice_event_queue:
            queue_status['voice_events'] = bot.voice_event_queue.qsize()
        if hasattr(bot, 'message_queue') and bot.message_queue:
            if isinstance(bot.message_queue, dict):
                queue_status['messages'] = {k: v.qsize() for k, v in bot.message_queue.items()}
            else:
                queue_status['messages'] = bot.message_queue.qsize()
        
        return jsonify({
            'bot_status': bot_status,
            'db_status': db_status,
            'pool_status': pool_status,
            'uptime': uptime,
            'guild_count': guild_count,
            'queue_status': queue_status
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/status: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/guilds')
@basic_auth_required
def get_guilds():
    try:
        guilds_data = []
        if hasattr(bot, 'guilds') and bot.guilds:
            for guild in bot.guilds:
                voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
                guilds_data.append({
                    'id': str(guild.id),
                    'name': guild.name,
                    'member_count': guild.member_count,
                    'icon': guild.icon.url if guild.icon else None,
                    'voice_channels': voice_channels
                })
        return jsonify(guilds_data)
    except Exception as e:
        web_logger.error(f"Erro em /api/guilds: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/guild/<int:guild_id>')
@basic_auth_required
def get_guild_info(guild_id):
    try:
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning(f"Bot não conectado a nenhuma guilda ao tentar obter informações da guilda {guild_id}.")
            return jsonify({'status': 'error', 'message': 'Bot não conectado a nenhuma guilda.'}), 404

        guild = discord.utils.get(bot.guilds, id=guild_id)
        
        if not guild:
            web_logger.warning(f"Guild {guild_id} não encontrada no cache do bot. Tentando buscar diretamente...")
            try:
                guild = run_coroutine_in_bot_loop(bot.fetch_guild(guild_id))
                web_logger.info(f"Guild {guild_id} encontrada via fetch_guild.")
            except discord.NotFound:
                web_logger.error(f"Guild {guild_id} não encontrada, mesmo via fetch_guild. Verificar ID ou bot não está nela.")
                return jsonify({
                    'status': 'error',
                    'message': 'Servidor Discord não encontrado ou bot não faz parte dele.',
                    'solutions': [
                        'Verifique se o bot está no servidor com o ID fornecido.',
                        'Certifique-se de que o ID do servidor está correto.',
                        'Verifique as permissões do bot no servidor (Intents de Gateway no Portal do Desenvolvedor Discord).'
                    ]
                }), 404
            except discord.Forbidden:
                web_logger.error(f"Bot não tem permissões para acessar a guilda {guild_id}.")
                return jsonify({
                    'status': 'error',
                    'message': 'Permissões insuficientes do bot para acessar este servidor.',
                    'solutions': [
                        'Verifique as permissões do bot no servidor, especialmente a permissão VIEW_CHANNEL.',
                        'Certifique-se de que os Intents de Membros e Guilds estão habilitados no Portal do Desenvolvedor Discord.'
                    ]
                }), 403
            except Exception as fetch_e:
                web_logger.error(f"Erro inesperado ao tentar buscar a guilda {guild_id}: {fetch_e}", exc_info=True)
                return jsonify({'status': 'error', 'message': f"Erro ao buscar informações do servidor: {str(fetch_e)}"}), 500

        try:
            run_coroutine_in_bot_loop(guild.chunk())
            web_logger.debug(f"Membros da guilda {guild.id} chunked com sucesso.")
        except Exception as e:
            web_logger.warning(f"Erro ao fazer chunking de membros da guilda {guild.id}: {e}", exc_info=True)

        tracked_roles_data = []
        if hasattr(bot, 'config') and 'tracked_roles' in bot.config:
            for role_id in bot.config['tracked_roles']:
                role = guild.get_role(role_id)
                if role:
                    tracked_roles_data.append({
                        'id': str(role.id),
                        'name': role.name,
                        'color': str(role.color),
                        'member_count': len(role.members)
                    })
        
        activity_stats = {
            'total_users': guild.member_count,
            'active_users': 0,
            'inactive_users': 0,
            'warned_users': 0,
            'kicked_users': 0
        }

        voice_channels_data = []
        for channel in guild.channels:
            if isinstance(channel, discord.VoiceChannel):
                voice_channels_data.append({
                    'id': str(channel.id),
                    'name': channel.name,
                    'user_count': len(channel.members)
                })
        
        guild_config = {
            'required_minutes': bot.config.get('required_minutes', 15) if hasattr(bot, 'config') else 15,
            'required_days': bot.config.get('required_days', 2) if hasattr(bot, 'config') else 2,
            'monitoring_period': bot.config.get('monitoring_period', 14) if hasattr(bot, 'config') else 14,
            'kick_after_days': bot.config.get('kick_after_days', 30) if hasattr(bot, 'config') else 30,
            'timezone': bot.config.get('timezone', 'America/Sao_Paulo') if hasattr(bot, 'config') else 'America/Sao_Paulo'
        }

        return jsonify({
            'id': str(guild.id),
            'name': guild.name,
            'member_count': guild.member_count,
            'icon': guild.icon.url if guild.icon else None,
            'tracked_roles': tracked_roles_data,
            'activity_stats': activity_stats,
            'voice_channels': voice_channels_data,
            'config': guild_config
        })
    except Exception as e:
        web_logger.error(f"Erro geral em /api/guild/{guild_id}: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro interno do servidor ao carregar informações da guilda: {str(e)}"}), 500

@app.route('/api/rate_limits')
@basic_auth_required
def get_rate_limits():
    try:
        if not hasattr(bot, 'rate_limit_monitor') or not bot.rate_limit_monitor:
            web_logger.warning("Rate limit monitor não inicializado.")
            return jsonify({'status': 'error', 'message': 'Monitor de Rate Limit não inicializado.'}), 500
        
        report = bot.rate_limit_monitor.get_status_report()
        return jsonify(report)
    except Exception as e:
        web_logger.error(f"Erro em /api/rate_limits: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/events')
@basic_auth_required
def get_recent_events():
    try:
        recent_events = []
        if hasattr(bot, 'rate_limit_monitor') and hasattr(bot.rate_limit_monitor, 'history') and bot.rate_limit_monitor.history:
            for event in list(bot.rate_limit_monitor.history)[-20:]:
                recent_events.append({
                    'time': datetime.datetime.fromtimestamp(event['time']).strftime('%Y-%m-%d %H:%M:%S'),
                    'bucket': event.get('bucket', 'N/A'),
                    'remaining': event.get('remaining', 'N/A'),
                    'endpoint': event.get('endpoint', 'N/A'),
                    'method': event.get('method', 'N/A')
                })
        return jsonify({'recent_events': recent_events})
    except Exception as e:
        web_logger.error(f"Erro em /api/events: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/update_config', methods=['POST'])
@basic_auth_required
def update_config():
    try:
        data = request.json
        if not data:
            web_logger.warning("Tentativa de atualização de configuração sem dados.")
            return jsonify({'status': 'error', 'message': 'Nenhum dado fornecido'}), 400
        
        if not hasattr(bot, 'config') or not bot.config:
            web_logger.warning("Objeto de configuração do bot não inicializado ao tentar atualizar.")
            return jsonify({'status': 'error', 'message': 'Configuração do bot não inicializada'}), 500

        updated = False
        if 'required_minutes' in data and isinstance(data['required_minutes'], (int, str)):
            bot.config['required_minutes'] = int(data['required_minutes'])
            updated = True
        if 'required_days' in data and isinstance(data['required_days'], (int, str)):
            bot.config['required_days'] = int(data['required_days'])
            updated = True
        if 'monitoring_period' in data and isinstance(data['monitoring_period'], (int, str)):
            bot.config['monitoring_period'] = int(data['monitoring_period'])
            updated = True
        if 'kick_after_days' in data and isinstance(data['kick_after_days'], (int, str)):
            bot.config['kick_after_days'] = int(data['kick_after_days'])
            updated = True
        if 'timezone' in data and isinstance(data['timezone'], str):
            bot.config['timezone'] = data['timezone']
            updated = True
        
        if updated:
            run_coroutine_in_bot_loop(bot.save_config())
            web_logger.info("Configurações atualizadas e salvas.")
            return jsonify({'status': 'success', 'message': 'Configurações atualizadas com sucesso.'})
        else:
            web_logger.info("Nenhuma configuração válida para atualizar.")
            return jsonify({'status': 'info', 'message': 'Nenhuma configuração válida para atualizar.'}), 200
    except ValueError:
        web_logger.warning("Valor de configuração inválido fornecido.")
        return jsonify({'status': 'error', 'message': 'Um ou mais valores de configuração são inválidos (esperado número).'}), 400
    except Exception as e:
        web_logger.error(f"Erro em /api/update_config: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao atualizar configuração: {str(e)}"}), 400

@app.route('/api/restart', methods=['POST'])
@basic_auth_required
def restart_bot():
    try:
        web_logger.info("Reinicialização do bot solicitada via painel web.")
        
        def initiate_restart():
            import time
            time.sleep(1)
            import sys
            python = sys.executable
            os.execl(python, python, *sys.argv)
        
        Thread(target=initiate_restart).start()
        
        return jsonify({
            'status': 'success', 
            'message': 'Reinicialização solicitada. O bot e o painel web devem reiniciar em breve.'
        })
    except Exception as e:
        web_logger.critical(f"Erro ao iniciar processo de reinicialização: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao reiniciar o bot: {str(e)}"}), 500

@app.route('/api/whitelist', methods=['GET', 'POST'])
@basic_auth_required
def manage_whitelist():
    try:
        if not hasattr(bot, 'config') or 'whitelist' not in bot.config:
            web_logger.warning("Config ou whitelist do bot não inicializadas ao gerenciar whitelist.")
            return jsonify({'status': 'error', 'message': 'Configuração do bot ou whitelist não inicializada'}), 500

        if request.method == 'GET':
            whitelisted_users = [str(id) for id in bot.config['whitelist'].get('users', [])]
            whitelisted_roles = [str(id) for id in bot.config['whitelist'].get('roles', [])]
            return jsonify({
                'users': whitelisted_users,
                'roles': whitelisted_roles
            })
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')
            target_type = data.get('type')
            target_id_str = data.get('id')
            
            if not all([action, target_type, target_id_str]):
                web_logger.warning("Parâmetros ausentes na requisição de whitelist.")
                return jsonify({'status': 'error', 'message': 'Parâmetros ausentes (action, type, id)'}), 400
            
            try:
                target_id = int(target_id_str)
            except ValueError:
                web_logger.warning(f"ID inválido fornecido para whitelist: {target_id_str}")
                return jsonify({'status': 'error', 'message': 'Formato de ID inválido. Deve ser um número inteiro.'}), 400
            
            config_key = None
            if target_type == 'user':
                config_key = 'users'
            elif target_type == 'role':
                config_key = 'roles'
            else:
                web_logger.warning(f"Tipo de alvo inválido para whitelist: {target_type}")
                return jsonify({'status': 'error', 'message': 'Tipo de alvo inválido. Deve ser "user" ou "role".'}), 400

            if config_key not in bot.config['whitelist']:
                bot.config['whitelist'][config_key] = []
            
            if action == 'add':
                if target_id not in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].append(target_id)
                    web_logger.info(f"Adicionado {target_type} {target_id} à whitelist.")
                else:
                    web_logger.info(f"Tentativa de adicionar {target_type} {target_id} já existente na whitelist.")
                    return jsonify({'status': 'info', 'message': f'ID {target_id} já está na whitelist.'}), 200
            elif action == 'remove':
                if target_id in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].remove(target_id)
                    web_logger.info(f"Removido {target_type} {target_id} da whitelist.")
                else:
                    web_logger.info(f"Tentativa de remover {target_type} {target_id} não existente na whitelist.")
                    return jsonify({'status': 'info', 'message': f'ID {target_id} não encontrado na whitelist.'}), 200
            else:
                web_logger.warning(f"Ação inválida para whitelist: {action}")
                return jsonify({'status': 'error', 'message': 'Ação inválida. Deve ser "add" ou "remove".'}), 400
            
            run_coroutine_in_bot_loop(bot.save_config())
            return jsonify({'status': 'success', 'message': 'Whitelist atualizada com sucesso.'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/whitelist: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao gerenciar whitelist: {str(e)}"}), 500

@app.route('/api/logs')
@basic_auth_required
def get_logs():
    try:
        lines = request.args.get('lines', default=200, type=int)
        
        log_lines = []
        log_file_path = 'bot.log'
        
        if not os.path.exists(log_file_path):
            web_logger.warning(f"Arquivo de log '{log_file_path}' não encontrado.")
            return jsonify({'logs': [f"Arquivo de log '{log_file_path}' não encontrado. Verifique se o bot está gerando logs."]}), 200
        
        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                log_lines = list(deque(f, lines))
        except Exception as e:
            web_logger.error(f"Erro ao ler logs de {log_file_path}: {e}", exc_info=True)
            return jsonify({'status': 'error', 'message': f"Erro ao ler logs: {str(e)}"}), 500
        
        return jsonify({
            'logs': log_lines
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/logs: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro interno do servidor ao carregar logs: {str(e)}"}), 500

@app.route('/api/activity_stats')
@basic_auth_required
def get_activity_stats():
    try:
        stats = {
            'total_users': 0,
            'active_users': 0,
            'inactive_users': 0,
            'warned_users': 0,
            'kicked_users': 0
        }

        if hasattr(bot, 'db') and bot.db and hasattr(bot, 'guilds') and bot.guilds:
            guild_id = bot.guilds[0].id
            
            async def _fetch_activity_stats():
                try:
                    stats['total_users'] = bot.guilds[0].member_count if bot.guilds else 0
                except Exception as e:
                    web_logger.error(f"Erro ao buscar estatísticas de atividade do DB: {e}")
                return stats
            
            fetched_stats = run_coroutine_in_bot_loop(_fetch_activity_stats())
            stats.update(fetched_stats)

        return jsonify(stats)
    except Exception as e:
        web_logger.error(f"Erro em /api/activity_stats: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao carregar estatísticas de atividade: {str(e)}"}), 500

@app.route('/api/backup', methods=['POST'])
@basic_auth_required
def create_backup():
    try:
        if not hasattr(bot, 'db_backup') or not bot.db_backup:
            web_logger.warning("Sistema de backup não inicializado.")
            return jsonify({'status': 'error', 'message': 'Sistema de backup não inicializado'}), 500
        
        web_logger.info("Solicitando criação de backup do banco de dados.")
        success = run_coroutine_in_bot_loop(bot.db_backup.create_backup())
        
        if success:
            web_logger.info("Backup criado com sucesso.")
            return jsonify({'status': 'success', 'message': 'Backup criado com sucesso'})
        else:
            web_logger.error("Falha ao criar backup.")
            return jsonify({'status': 'error', 'message': 'Falha ao criar backup'}), 500
    except Exception as e:
        web_logger.error(f"Erro em /api/backup: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao criar backup: {str(e)}"}), 500

@app.route('/api/allowed_roles', methods=['GET', 'POST'])
@basic_auth_required
def manage_allowed_roles():
    try:
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Bot não conectado a nenhuma guilda ao gerenciar allowed_roles.")
            return jsonify({'status': 'error', 'message': 'Nenhuma guilda disponível. O bot precisa estar conectado a pelo menos uma guilda.'}), 400
        
        guild = bot.guilds[0]
        
        if request.method == 'GET':
            allowed_roles_data = []
            if hasattr(bot, 'config') and 'allowed_roles' in bot.config:
                for role_id in bot.config['allowed_roles']:
                    role = guild.get_role(role_id)
                    if role:
                        allowed_roles_data.append({
                            'id': str(role.id),
                            'name': role.name,
                            'color': str(role.color)
                        })
            return jsonify(allowed_roles_data)
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')
            role_id_str = data.get('id')
            
            if not all([action, role_id_str]):
                web_logger.warning("Parâmetros ausentes na requisição de allowed_roles.")
                return jsonify({'status': 'error', 'message': 'Parâmetros ausentes (action, id)'}), 400
            
            try:
                role_id = int(role_id_str)
            except ValueError:
                web_logger.warning(f"ID de role inválido fornecido: {role_id_str}")
                return jsonify({'status': 'error', 'message': 'Formato de ID inválido. Deve ser um número inteiro.'}), 400
            
            if not hasattr(bot, 'config'):
                bot.config = {}

            if 'allowed_roles' not in bot.config:
                bot.config['allowed_roles'] = []
            
            if action == 'add':
                if role_id not in bot.config['allowed_roles']:
                    bot.config['allowed_roles'].append(role_id)
                    web_logger.info(f"Adicionado role {role_id} aos allowed_roles.")
                else:
                    web_logger.info(f"Tentativa de adicionar role {role_id} já existente nos allowed_roles.")
                    return jsonify({'status': 'info', 'message': f'Role {role_id} já está nos allowed_roles.'}), 200
            elif action == 'remove':
                if role_id in bot.config['allowed_roles']:
                    bot.config['allowed_roles'].remove(role_id)
                    web_logger.info(f"Removido role {role_id} dos allowed_roles.")
                else:
                    web_logger.info(f"Tentativa de remover role {role_id} não existente nos allowed_roles.")
                    return jsonify({'status': 'info', 'message': f'Role {role_id} não encontrado nos allowed_roles.'}), 200
            else:
                web_logger.warning(f"Ação inválida para allowed_roles: {action}")
                return jsonify({'status': 'error', 'message': 'Ação inválida. Deve ser "add" ou "remove".'}), 400
            
            run_coroutine_in_bot_loop(bot.save_config())
            return jsonify({'status': 'success', 'message': 'Allowed roles atualizados com sucesso.'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/allowed_roles: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao gerenciar allowed roles: {str(e)}"}), 500

@app.route('/api/whitelist_users', methods=['GET'])
@basic_auth_required
def get_whitelist_users():
    try:
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Bot não conectado a nenhuma guilda ao tentar obter usuários da whitelist.")
            return jsonify({'status': 'error', 'message': 'Nenhuma guilda disponível. O bot precisa estar conectado a pelo menos uma guilda.'}), 400
        
        guild = bot.guilds[0]
        users_data = []
        if hasattr(bot, 'config') and 'whitelist' in bot.config and 'users' in bot.config['whitelist']:
            for user_id in bot.config['whitelist']['users']:
                member = guild.get_member(user_id)
                if member:
                    users_data.append({
                        'id': str(member.id),
                        'name': member.display_name,
                        'avatar': str(member.avatar.url) if member.avatar else None
                    })
                else:
                    web_logger.warning(f"Usuário {user_id} da whitelist não encontrado no cache da guilda {guild.id}.")
                    users_data.append({
                        'id': str(user_id),
                        'name': f"ID: {user_id} (Não encontrado)",
                        'avatar': None
                    })
        return jsonify(users_data)
    except Exception as e:
        web_logger.error(f"Erro em /api/whitelist_users: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao carregar usuários da whitelist: {str(e)}"}), 500

@app.route('/api/run_command', methods=['POST'])
@basic_auth_required
def run_command():
    try:
        data = request.json
        command = data.get('command')
        
        if not command:
            web_logger.warning("Tentativa de executar comando sem especificar o comando.")
            return jsonify({'status': 'error', 'message': 'Nenhum comando especificado'}), 400
        
        async def _run_bot_command():
            try:
                if command == 'force_check':
                    member_id = data.get('member_id')
                    if not member_id:
                        return {'status': 'error', 'message': 'ID do membro é obrigatório para "force_check"'}
                    
                    if not hasattr(bot, 'guilds') or not bot.guilds:
                        return {'status': 'error', 'message': 'Nenhuma guilda disponível para executar "force_check"'}
                    
                    guild = bot.guilds[0]
                    member = guild.get_member(int(member_id))
                    if not member:
                        try:
                            member = await guild.fetch_member(int(member_id))
                        except discord.NotFound:
                            return {'status': 'error', 'message': f'Membro com ID {member_id} não encontrado neste servidor.'}
                        except discord.Forbidden:
                            return {'status': 'error', 'message': 'Bot não tem permissão para buscar este membro.'}

                    from tasks import _execute_force_check
                    result = await _execute_force_check(member)
                    return {'status': 'success', 'result': result}
                
                elif command == 'cleanup_data':
                    days = data.get('days', 60)
                    if not hasattr(bot, 'db') or not bot.db:
                        return {'status': 'error', 'message': 'Sistema de banco de dados não inicializado para "cleanup_data"'}
                    result = await bot.db.cleanup_old_data(days)
                    return {'status': 'success', 'result': result}
                
                elif command == 'sync_commands':
                    if not hasattr(bot, 'tree'):
                        return {'status': 'error', 'message': 'Árvore de comandos do bot não inicializada'}
                    await bot.tree.sync()
                    return {'status': 'success', 'message': 'Comandos sincronizados com sucesso!'}
                
                else:
                    return {'status': 'error', 'message': 'Comando inválido'}
            
            except Exception as e:
                web_logger.error(f"Erro ao executar comando '{command}' no loop do bot: {e}", exc_info=True)
                return {'status': 'error', 'message': f"Erro ao executar comando: {str(e)}"}
        
        result = run_coroutine_in_bot_loop(_run_bot_command())
        return jsonify(result)
    
    except Exception as e:
        web_logger.error(f"Erro em /api/run_command: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro interno do servidor ao tentar executar comando: {str(e)}"}), 500

@app.route('/api/warnings_history', methods=['GET'])
@basic_auth_required
def get_warnings_history():
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=50, type=int)
        days = request.args.get('days', default=30, type=int)
        
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Nenhuma guilda disponível para buscar histórico de avisos.")
            return jsonify({'status': 'error', 'message': 'Nenhuma guilda disponível', 'warnings': []}), 400
        
        guild_id = bot.guilds[0].id
        
        async def _get_warnings_from_db():
            if not hasattr(bot, 'db') or not bot.db:
                web_logger.warning("Objeto de banco de dados não disponível para warnings_history.")
                return {'total': 0, 'warnings': []}
                
            try:
                async with bot.db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        # Query para contar o total
                        count_query = '''
                            SELECT COUNT(*) as total
                            FROM user_warnings
                            WHERE guild_id = %s
                            AND warning_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        '''
                        await cursor.execute(count_query, (guild_id, days))
                        total = (await cursor.fetchone())['total']
                        
                        # Query para obter os dados paginados
                        query = '''
                            SELECT 
                                user_id, 
                                warning_type, 
                                warning_date
                            FROM user_warnings
                            WHERE guild_id = %s
                            AND warning_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            ORDER BY warning_date DESC
                            LIMIT %s OFFSET %s
                        '''
                        offset = (page - 1) * per_page
                        web_logger.debug(f"Executando query de warnings: {query} com params: {guild_id}, {days}, {per_page}, {offset}")
                        await cursor.execute(query, (guild_id, days, per_page, offset))
                        
                        return {
                            'total': total,
                            'warnings': await cursor.fetchall()
                        }
            except Exception as e:
                web_logger.error(f"Erro de banco de dados em _get_warnings_from_db: {e}", exc_info=True)
                return {'total': 0, 'warnings': []}
        
        warnings_data = run_coroutine_in_bot_loop(_get_warnings_from_db())
        
        warnings_with_names = []
        guild = bot.guilds[0] if bot.guilds else None
        
        for warning in warnings_data['warnings']:
            member = guild.get_member(warning['user_id']) if guild else None
            user_name = member.display_name if member else f"ID: {warning['user_id']}"
            if not member:
                web_logger.debug(f"Membro {warning['user_id']} não encontrado no cache da guilda {guild_id} para warnings_history.")

            warnings_with_names.append({
                'user_id': str(warning['user_id']),
                'user_name': user_name,
                'warning_type': warning['warning_type'],
                'warning_date': warning['warning_date'].strftime('%Y-%m-%d %H:%M:%S') if warning['warning_date'] else None
            })
        
        return jsonify({
            'status': 'success',
            'warnings': warnings_with_names,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': warnings_data['total'],
                'total_pages': (warnings_data['total'] + per_page - 1) // per_page
            }
        })
    
    except Exception as e:
        web_logger.error(f"Erro em /api/warnings_history: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f"Erro ao carregar histórico de avisos: {str(e)}",
            'warnings': []
        }), 500

@app.route('/api/kicks_history', methods=['GET'])
@basic_auth_required
def get_kicks_history():
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=50, type=int)
        days = request.args.get('days', default=30, type=int)
        
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Nenhuma guilda disponível para buscar histórico de expulsões.")
            return jsonify({'status': 'error', 'message': 'Nenhuma guilda disponível', 'kicks': []}), 400
        
        guild_id = bot.guilds[0].id
        
        async def _get_kicks_from_db():
            if not hasattr(bot, 'db') or not bot.db:
                web_logger.warning("Objeto de banco de dados não disponível para kicks_history.")
                return {'total': 0, 'kicks': []}
            try:
                async with bot.db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        # Query para contar o total
                        count_query = '''
                            SELECT COUNT(*) as total
                            FROM kicked_members
                            WHERE guild_id = %s
                            AND kick_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        '''
                        await cursor.execute(count_query, (guild_id, days))
                        total = (await cursor.fetchone())['total']
                        
                        # Query para obter os dados paginados
                        query = '''
                            SELECT 
                                user_id, 
                                kick_date, 
                                reason
                            FROM kicked_members
                            WHERE guild_id = %s
                            AND kick_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            ORDER BY kick_date DESC
                            LIMIT %s OFFSET %s
                        '''
                        offset = (page - 1) * per_page
                        web_logger.debug(f"Executando query de kicks: {query} com params: {guild_id}, {days}, {per_page}, {offset}")
                        await cursor.execute(query, (guild_id, days, per_page, offset))
                        
                        return {
                            'total': total,
                            'kicks': await cursor.fetchall()
                        }
            except Exception as e:
                web_logger.error(f"Erro de banco de dados em _get_kicks_from_db: {e}", exc_info=True)
                return {'total': 0, 'kicks': []}
        
        kicks_data = run_coroutine_in_bot_loop(_get_kicks_from_db())
        
        kicks_with_names = []
        guild = bot.guilds[0] if bot.guilds else None
        
        for kick in kicks_data['kicks']:
            member = guild.get_member(kick['user_id']) if guild else None
            user_name = member.display_name if member else f"ID: {kick['user_id']}"
            if not member:
                web_logger.debug(f"Membro {kick['user_id']} não encontrado no cache da guilda {guild_id} para kicks_history.")

            kicks_with_names.append({
                'user_id': str(kick['user_id']),
                'user_name': user_name,
                'kick_date': kick['kick_date'].strftime('%Y-%m-%d %H:%M:%S') if kick['kick_date'] else None,
                'reason': kick['reason']
            })
        
        return jsonify({
            'status': 'success',
            'kicks': kicks_with_names,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': kicks_data['total'],
                'total_pages': (kicks_data['total'] + per_page - 1) // per_page
            }
        })
    
    except Exception as e:
        web_logger.error(f"Erro em /api/kicks_history: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f"Erro ao carregar histórico de expulsões: {str(e)}",
            'kicks': []
        }), 500

@app.route('/api/get_role_removals', methods=['GET'])
@basic_auth_required
def get_role_removals():
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=50, type=int)
        days = request.args.get('days', default=30, type=int)
        
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Nenhuma guilda disponível para obter remoções de cargos")
            return jsonify({
                'status': 'error',
                'message': 'Nenhuma guilda disponível'
            }), 400
            
        guild_id = bot.guilds[0].id
        
        async def _get_role_removals_from_db():
            if not hasattr(bot, 'db') or not bot.db:
                web_logger.warning("Banco de dados não disponível para remoções de cargos")
                return {'total': 0, 'removals': []}
                
            try:
                async with bot.db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        # Query para contar o total
                        count_query = '''
                            SELECT COUNT(*) as total
                            FROM removed_roles
                            WHERE guild_id = %s
                            AND removal_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        '''
                        await cursor.execute(count_query, (guild_id, days))
                        total = (await cursor.fetchone())['total']
                        
                        # Query para obter os dados paginados
                        query = '''
                            SELECT 
                                user_id,
                                role_id as removed_roles,
                                removal_date,
                                NULL as executed_by
                            FROM removed_roles
                            WHERE guild_id = %s
                            AND removal_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            ORDER BY removal_date DESC
                            LIMIT %s OFFSET %s
                        '''
                        offset = (page - 1) * per_page
                        await cursor.execute(query, (guild_id, days, per_page, offset))
                        
                        return {
                            'total': total,
                            'removals': await cursor.fetchall()
                        }
            except Exception as e:
                web_logger.error(f"Erro ao buscar remoções de cargo: {e}", exc_info=True)
                return {'total': 0, 'removals': []}
        
        removals_data = run_coroutine_in_bot_loop(_get_role_removals_from_db())
        
        # Adicionar nomes de usuário
        guild = bot.guilds[0]
        removals_with_names = []
        for item in removals_data['removals']:
            member = guild.get_member(item['user_id'])
            role = guild.get_role(item['removed_roles']) if item['removed_roles'] else None
            
            removals_with_names.append({
                'user_id': str(item['user_id']),
                'user_name': member.display_name if member else f"ID: {item['user_id']}",
                'removed_roles': role.name if role else f"ID: {item['removed_roles']}",
                'removal_date': item['removal_date'].strftime('%Y-%m-%d %H:%M:%S') if item['removal_date'] else None,
                'executed_by': "Sistema"  # Como não temos executor no banco, marcamos como sistema
            })
        
        return jsonify({
            'status': 'success',
            'removals': removals_with_names,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': removals_data['total'],
                'total_pages': (removals_data['total'] + per_page - 1) // per_page
            }
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/get_role_removals: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/get_ranking', methods=['GET'])
@basic_auth_required
def get_ranking():
    try:
        days = request.args.get('days', default=7, type=int)
        limit = request.args.get('limit', default=5, type=int)
        
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Nenhuma guilda disponível para obter ranking")
            return jsonify({
                'status': 'error',
                'message': 'Nenhuma guilda disponível'
            }), 400
            
        guild_id = bot.guilds[0].id
        
        async def _get_ranking_from_db():
            if not hasattr(bot, 'db') or not bot.db:
                web_logger.warning("Banco de dados não disponível para ranking")
                return []
                
            try:
                async with bot.db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        query = '''
                            SELECT 
                                user_id,
                                SUM(duration)/60 as total_minutes,
                                COUNT(DISTINCT DATE(join_time)) as active_days
                            FROM voice_sessions
                            WHERE guild_id = %s
                            AND join_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            GROUP BY user_id
                            ORDER BY total_minutes DESC, active_days DESC
                            LIMIT %s
                        '''
                        await cursor.execute(query, (guild_id, days, limit))
                        return await cursor.fetchall()
            except Exception as e:
                web_logger.error(f"Erro ao buscar ranking: {e}", exc_info=True)
                return []
        
        ranking = run_coroutine_in_bot_loop(_get_ranking_from_db())
        
        # Adicionar nomes de usuário
        guild = bot.guilds[0]
        ranking_with_names = []
        for item in ranking:
            member = guild.get_member(item['user_id'])
            ranking_with_names.append({
                'user_id': str(item['user_id']),
                'user_name': member.display_name if member else f"ID: {item['user_id']}",
                'total_minutes': item['total_minutes'],
                'active_days': item['active_days']
            })
        
        return jsonify({
            'status': 'success',
            'ranking': ranking_with_names
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/get_ranking: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Nova rota para cargos monitorados
@app.route('/api/tracked_roles', methods=['GET', 'POST'])
@basic_auth_required
def manage_tracked_roles():
    try:
        if not hasattr(bot, 'guilds') or not bot.guilds:
            web_logger.warning("Bot não conectado a nenhuma guilda ao gerenciar tracked_roles.")
            return jsonify({'status': 'error', 'message': 'Nenhuma guilda disponível. O bot precisa estar conectado a pelo menos uma guilda.'}), 400
        
        guild = bot.guilds[0]
        
        if request.method == 'GET':
            # Lista de cargos disponíveis
            available_roles = []
            for role in guild.roles:
                if role.name != "@everyone":
                    available_roles.append({
                        'id': str(role.id),
                        'name': role.name,
                        'color': str(role.color),
                        'position': role.position,
                        'member_count': len(role.members)
                    })
            
            # Lista de cargos monitorados
            tracked_roles = []
            if hasattr(bot, 'config') and 'tracked_roles' in bot.config:
                for role_id in bot.config['tracked_roles']:
                    role = guild.get_role(role_id)
                    if role:
                        tracked_roles.append({
                            'id': str(role.id),
                            'name': role.name,
                            'color': str(role.color),
                            'position': role.position,
                            'member_count': len(role.members)
                        })
            
            return jsonify({
                'available_roles': available_roles,
                'tracked_roles': tracked_roles
            })
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')
            role_id_str = data.get('role_id')
            
            if not all([action, role_id_str]):
                web_logger.warning("Parâmetros ausentes na requisição de tracked_roles.")
                return jsonify({'status': 'error', 'message': 'Parâmetros ausentes (action, role_id)'}), 400
            
            try:
                role_id = int(role_id_str)
            except ValueError:
                web_logger.warning(f"ID de role inválido fornecido: {role_id_str}")
                return jsonify({'status': 'error', 'message': 'Formato de ID inválido. Deve ser um número inteiro.'}), 400
            
            if not hasattr(bot, 'config'):
                bot.config = {}

            if 'tracked_roles' not in bot.config:
                bot.config['tracked_roles'] = []

            if action == 'add':
                if role_id not in bot.config['tracked_roles']:
                    bot.config['tracked_roles'].append(role_id)
                    web_logger.info(f"Adicionado role {role_id} aos tracked_roles.")
                else:
                    web_logger.info(f"Tentativa de adicionar role {role_id} já existente nos tracked_roles.")
                    return jsonify({'status': 'info', 'message': f'Role {role_id} já está nos tracked_roles.'}), 200
            elif action == 'remove':
                if role_id in bot.config['tracked_roles']:
                    bot.config['tracked_roles'].remove(role_id)
                    web_logger.info(f"Removido role {role_id} dos tracked_roles.")
                else:
                    web_logger.info(f"Tentativa de remover role {role_id} não existente nos tracked_roles.")
                    return jsonify({'status': 'info', 'message': f'Role {role_id} não encontrado nos tracked_roles.'}), 200
            else:
                web_logger.warning(f"Ação inválida para tracked_roles: {action}")
                return jsonify({'status': 'error', 'message': 'Ação inválida. Deve ser "add" ou "remove".'}), 400
            
            run_coroutine_in_bot_loop(bot.save_config())
            return jsonify({'status': 'success', 'message': 'Tracked roles atualizados com sucesso.'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/tracked_roles: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao gerenciar tracked roles: {str(e)}"}), 500

# Nova rota para configurações de aviso
@app.route('/api/warning_settings', methods=['GET', 'POST'])
@basic_auth_required
def manage_warning_settings():
    try:
        if not hasattr(bot, 'config'):
            bot.config = {}

        if 'warning_settings' not in bot.config:
            bot.config['warning_settings'] = {
                'first_warning': 7,
                'second_warning': 14,
                'messages': {
                    'first_warning': 'Você está sendo avisado por inatividade. Por favor, seja mais ativo para evitar ações futuras.',
                    'second_warning': 'Este é seu segundo aviso por inatividade. Sua situação está se tornando crítica.',
                    'final_warning': 'Este é seu aviso final. Se não melhorar sua atividade, você será removido dos cargos.'
                }
            }

        if request.method == 'GET':
            return jsonify(bot.config['warning_settings'])
        
        elif request.method == 'POST':
            data = request.json
            updated = False

            if 'first_warning' in data and isinstance(data['first_warning'], (int, str)):
                bot.config['warning_settings']['first_warning'] = int(data['first_warning'])
                updated = True
            
            if 'second_warning' in data and isinstance(data['second_warning'], (int, str)):
                bot.config['warning_settings']['second_warning'] = int(data['second_warning'])
                updated = True
            
            if 'message_type' in data and 'message_content' in data:
                message_type = data['message_type']
                if message_type in ['first_warning', 'second_warning', 'final_warning']:
                    bot.config['warning_settings']['messages'][message_type] = data['message_content']
                    updated = True
            
            if updated:
                run_coroutine_in_bot_loop(bot.save_config())
                return jsonify({'status': 'success', 'message': 'Configurações de aviso atualizadas com sucesso.'})
            else:
                return jsonify({'status': 'info', 'message': 'Nenhuma configuração válida para atualizar.'}), 200
    
    except Exception as e:
        web_logger.error(f"Erro em /api/warning_settings: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f"Erro ao gerenciar configurações de aviso: {str(e)}"}), 500

# Tratamento de erro global
@app.errorhandler(Exception)
def handle_exception(e):
    web_logger.error(f"Erro não tratado: {e}", exc_info=True)
    return jsonify({
        'status': 'error',
        'message': 'Ocorreu um erro interno no servidor'
    }), 500

# --- Inicialização do Bot e Servidor ---

# Inicia o bot em uma thread separada assim que o módulo é carregado
web_logger.info("Iniciando a thread do bot Discord...")
run_bot_in_thread()

# Adiciona um pequeno atraso para garantir que o bot tenha tempo de inicializar
time.sleep(5)

# Este bloco só será usado para testes locais, não quando executado pelo Gunicorn
if __name__ == '__main__':
    web_logger.info("Iniciando o servidor Flask em modo de desenvolvimento local.")
    app.run(host='0.0.0.0', port=8080)