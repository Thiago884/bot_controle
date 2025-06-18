from flask import Flask, jsonify, render_template, request, redirect, url_for, Response
from threading import Thread
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

# Configuração básica do logger para o web panel
web_logger = logging.getLogger('web_panel')
web_logger.setLevel(logging.INFO)

handler = RotatingFileHandler('web_panel.log', maxBytes=5*1024*1024, backupCount=3)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
web_logger.addHandler(handler)

app = Flask(__name__)

# Configuração de autenticação
WEB_AUTH_USER = os.getenv('WEB_AUTH_USER', 'admin')
WEB_AUTH_PASS = os.getenv('WEB_AUTH_PASS', 'admin123')

def basic_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != WEB_AUTH_USER or auth.password != WEB_AUTH_PASS:
            return Response(
                'Acesso não autorizado',
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )
        return f(*args, **kwargs)
    return decorated

# Função para executar corrotinas no loop do bot
def run_coroutine_in_bot_loop(coro):
    future = asyncio.run_coroutine_threadsafe(coro, bot.loop)
    return future.result()

@app.route('/')
@basic_auth_required
def home():
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
@basic_auth_required
def dashboard():
    try:
        web_logger.info("Acessando a rota dashboard")
        
        # Estatísticas básicas do bot
        uptime = (datetime.datetime.now() - bot.start_time) if hasattr(bot, 'start_time') else "N/A"
        web_logger.info(f"Tempo de atividade: {uptime}")

        # Processar informações das guildas
        guild_stats = []
        if hasattr(bot, 'guilds') and bot.guilds:
            for guild in bot.guilds:
                web_logger.info(f"Processando guilda: {guild.name}")
                voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
                guild_stats.append({
                    'id': guild.id,
                    'name': guild.name,
                    'members': guild.member_count,
                    'voice_channels': voice_channels,
                    'icon': guild.icon.url if guild.icon else None
                })
        else:
            web_logger.warning("Bot.guilds não está disponível ou está vazio.")
        
        # Status do banco de dados
        db_status = "Desconhecido"
        pool_status = {}
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status = run_coroutine_in_bot_loop(bot.db.check_pool_status()) or {}
                db_status = "Operacional" if pool_status else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
                web_logger.error(f"Erro ao verificar status do banco: {e}", exc_info=True)
        else:
            web_logger.warning("Objeto do banco de dados (bot.db) não está disponível.")
        
        # Status das filas
        queue_status = {
            'voice_events': 0,
            'messages': {'critical': 0, 'high': 0, 'normal': 0, 'low': 0}
        }
        if hasattr(bot, 'voice_event_queue'):
            queue_status['voice_events'] = bot.voice_event_queue.qsize()
        if hasattr(bot, 'message_queue'):
            if isinstance(bot.message_queue, dict):
                queue_status['messages'] = {k: v.qsize() for k, v in bot.message_queue.items()}
            else:
                queue_status['messages'] = bot.message_queue.qsize()
        
        # Configurações atuais
        current_config = {
            'required_minutes': bot.config.get('required_minutes', 15),
            'required_days': bot.config.get('required_days', 2),
            'monitoring_period': bot.config.get('monitoring_period', 14),
            'kick_after_days': bot.config.get('kick_after_days', 30)
        }
        
        return render_template('dashboard.html', 
                            bot_name=bot.user.name if hasattr(bot, 'user') and bot.user else "Inactivity Bot",
                            guild_count=len(bot.guilds) if hasattr(bot, 'guilds') else 0,
                            guild_stats=guild_stats,
                            uptime=str(uptime).split('.')[0] if uptime != "N/A" else uptime,
                            db_status=db_status,
                            pool_status=pool_status,
                            queue_status=queue_status,
                            current_config=current_config)
    
    except Exception as e:
        web_logger.error(f"Erro na rota dashboard: {e}", exc_info=True)
        return render_template('error.html', error_message="Erro ao carregar o dashboard"), 500

@app.route('/monitor')
@basic_auth_required
def monitor():
    try:
        web_logger.info("Acessando a rota monitor")
        
        # Obter status do sistema
        system_status = get_system_status().json
        
        # Obter rate limits
        rate_limits = get_rate_limits().json
        
        # Obter eventos recentes
        recent_events = get_recent_events().json
        
        # Obter logs recentes
        log_lines = []
        try:
            with open('bot.log', 'r') as log_file:
                log_lines = list(deque(log_file, 100))  # Últimas 100 linhas
        except Exception as e:
            web_logger.warning(f"Erro ao ler arquivo de log: {e}")
        
        return render_template('monitor.html',
                            bot_name=bot.user.name if hasattr(bot, 'user') and bot.user else "Inactivity Bot",
                            system_status=system_status,
                            rate_limits=rate_limits,
                            recent_events=recent_events,
                            log_lines=log_lines)
    
    except Exception as e:
        web_logger.error(f"Erro na rota monitor: {e}", exc_info=True)
        return render_template('error.html', error_message="Erro ao carregar o monitor"), 500

@app.route('/api/status')
@basic_auth_required
def get_system_status():
    try:
        # Obter status do bot
        bot_status = "Operacional"
        
        # Obter status do banco de dados
        db_status = "Desconhecido"
        pool_status = {}
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status = run_coroutine_in_bot_loop(bot.db.check_pool_status()) or {}
                db_status = "Operacional" if pool_status else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
        
        # Obter uptime
        uptime = str(datetime.datetime.now() - bot.start_time).split('.')[0] if hasattr(bot, 'start_time') else "N/A"
        
        # Obter contagem de servidores
        guild_count = len(bot.guilds) if hasattr(bot, 'guilds') else 0
        
        # Obter status das filas
        queue_status = {
            'voice_events': 0,
            'messages_critical': 0,
            'messages_high': 0,
            'messages_normal': 0,
            'messages_low': 0
        }
        if hasattr(bot, 'voice_event_queue'):
            queue_status['voice_events'] = bot.voice_event_queue.qsize()
        if hasattr(bot, 'message_queue'):
            if isinstance(bot.message_queue, dict):
                queue_status.update({
                    'messages_critical': bot.message_queue['critical'].qsize(),
                    'messages_high': bot.message_queue['high'].qsize(),
                    'messages_normal': bot.message_queue['normal'].qsize(),
                    'messages_low': bot.message_queue['low'].qsize()
                })
            else:
                queue_status['messages_normal'] = bot.message_queue.qsize()
        
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
        return jsonify({'error': str(e)}), 500

@app.route('/api/guilds')
@basic_auth_required
def get_guilds():
    try:
        guilds = []
        if hasattr(bot, 'guilds') and bot.guilds:
            for guild in bot.guilds:
                voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
                guilds.append({
                    'id': guild.id,
                    'name': guild.name,
                    'member_count': guild.member_count,
                    'icon': guild.icon.url if guild.icon else None,
                    'voice_channels': voice_channels
                })
        return jsonify(guilds)
    except Exception as e:
        web_logger.error(f"Erro em /api/guilds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/guild/<int:guild_id>')
@basic_auth_required
def get_guild_info(guild_id):
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            return jsonify({'error': 'Guild not found'}), 404
        
        # Obter membros com cargos monitorados
        tracked_members = []
        if hasattr(bot, 'config') and 'tracked_roles' in bot.config:
            for role_id in bot.config['tracked_roles']:
                role = guild.get_role(role_id)
                if role:
                    for member in role.members:
                        tracked_members.append(member.id)
        
        # Estatísticas básicas (simuladas)
        active_users = len(set(tracked_members))  # Simulação
        inactive_users = 0  # Será calculado depois
        
        return jsonify({
            'id': guild.id,
            'name': guild.name,
            'icon': guild.icon.url if guild.icon else None,
            'tracked_roles': [{
                'id': role.id,
                'name': role.name,
                'color': str(role.color),
                'member_count': len(role.members)
            } for role in guild.roles if hasattr(bot, 'config') and 'tracked_roles' in bot.config and role.id in bot.config['tracked_roles']],
            'activity_stats': {
                'active_users': active_users,
                'inactive_users': inactive_users,
                'warned_users': 0  # Será calculado depois
            },
            'voice_channels': [{
                'id': channel.id,
                'name': channel.name,
                'user_count': len(channel.members)
            } for channel in guild.channels if isinstance(channel, discord.VoiceChannel)],
            'config': {
                'required_minutes': bot.config.get('required_minutes', 15),
                'required_days': bot.config.get('required_days', 2),
                'monitoring_period': bot.config.get('monitoring_period', 14),
                'kick_after_days': bot.config.get('kick_after_days', 30)
            }
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/guild/{guild_id}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/rate_limits')
@basic_auth_required
def get_rate_limits():
    try:
        if not hasattr(bot, 'rate_limit_monitor'):
            return jsonify({'error': 'Rate limit monitor not initialized'}), 500
        
        report = bot.rate_limit_monitor.get_status_report()
        return jsonify(report)
    except Exception as e:
        web_logger.error(f"Erro em /api/rate_limits: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/events')
@basic_auth_required
def get_recent_events():
    try:
        recent_events = []
        if hasattr(bot, 'rate_limit_monitor') and hasattr(bot.rate_limit_monitor, 'history'):
            for event in list(bot.rate_limit_monitor.history)[-20:]:
                recent_events.append({
                    'time': datetime.datetime.fromtimestamp(event['time']).strftime('%H:%M:%S'),
                    'bucket': event['bucket'],
                    'remaining': event['remaining'],
                    'endpoint': event.get('endpoint', 'unknown'),
                    'method': event.get('method', 'GET')
                })
        return jsonify({'recent_events': recent_events})
    except Exception as e:
        web_logger.error(f"Erro em /api/events: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/update_config', methods=['POST'])
@basic_auth_required
def update_config():
    try:
        data = request.json
        if not data:
            return jsonify({'status': 'error', 'message': 'No data provided'}), 400
        
        if not hasattr(bot, 'config'):
            return jsonify({'status': 'error', 'message': 'Bot config not initialized'}), 500

        if 'required_minutes' in data:
            bot.config['required_minutes'] = int(data['required_minutes'])
        if 'required_days' in data:
            bot.config['required_days'] = int(data['required_days'])
        if 'monitoring_period' in data:
            bot.config['monitoring_period'] = int(data['monitoring_period'])
        if 'kick_after_days' in data:
            bot.config['kick_after_days'] = int(data['kick_after_days'])
        
        run_coroutine_in_bot_loop(bot.save_config())
        return jsonify({'status': 'success'})
    except Exception as e:
        web_logger.error(f"Erro em /api/update_config: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/restart', methods=['POST'])
@basic_auth_required
def restart_bot():
    try:
        web_logger.info("Reinicialização do bot solicitada")
        
        def restart():
            import sys
            python = sys.executable
            os.execl(python, python, *sys.argv)
        
        Thread(target=restart).start()
        
        return jsonify({
            'status': 'success', 
            'message': 'Reinicialização solicitada. O bot deve reiniciar em breve.'
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/restart: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/whitelist', methods=['GET', 'POST'])
@basic_auth_required
def manage_whitelist():
    try:
        if not hasattr(bot, 'config') or 'whitelist' not in bot.config:
            return jsonify({'status': 'error', 'message': 'Bot config or whitelist not initialized'}), 500

        if request.method == 'GET':
            return jsonify({
                'users': [str(id) for id in bot.config['whitelist']['users']],
                'roles': [str(id) for id in bot.config['whitelist']['roles']]
            })
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')  # 'add' or 'remove'
            target_type = data.get('type')  # 'user' or 'role'
            target_id = data.get('id')
            
            if not all([action, target_type, target_id]):
                return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
            
            try:
                target_id = int(target_id)
            except ValueError:
                return jsonify({'status': 'error', 'message': 'Invalid ID format'}), 400
            
            config_key = 'users' if target_type == 'user' else 'roles'
            
            if action == 'add':
                if target_id not in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].append(target_id)
            elif action == 'remove':
                if target_id in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].remove(target_id)
            else:
                return jsonify({'status': 'error', 'message': 'Invalid action'}), 400
            
            run_coroutine_in_bot_loop(bot.save_config())
            return jsonify({'status': 'success'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/whitelist: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/logs')
@basic_auth_required
def get_logs():
    try:
        lines = request.args.get('lines', default=100, type=int)
        
        log_lines = []
        try:
            with open('bot.log', 'r') as log_file:
                log_lines = list(deque(log_file, lines))
        except FileNotFoundError:
            web_logger.warning("Arquivo de log 'bot.log' não encontrado.")
            return jsonify({'logs': ['Arquivo de log "bot.log" não encontrado.']})
        except Exception as e:
            web_logger.error(f"Erro ao ler logs de bot.log: {e}", exc_info=True)
            return jsonify({'error': f"Erro ao ler logs: {str(e)}"}), 500
        
        return jsonify({
            'logs': log_lines
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/logs: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

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
        
        return jsonify(stats)
    except Exception as e:
        web_logger.error(f"Erro em /api/activity_stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup', methods=['POST'])
@basic_auth_required
def create_backup():
    try:
        if not hasattr(bot, 'db_backup'):
            return jsonify({'status': 'error', 'message': 'Backup system not initialized'}), 500
        
        success = run_coroutine_in_bot_loop(bot.db_backup.create_backup())
        
        if success:
            return jsonify({'status': 'success', 'message': 'Backup created successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to create backup'}), 500
    except Exception as e:
        web_logger.error(f"Erro em /api/backup: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/allowed_roles', methods=['GET', 'POST'])
@basic_auth_required
def manage_allowed_roles():
    try:
        if request.method == 'GET':
            allowed_roles = []
            for role_id in bot.config.get('allowed_roles', []):
                role = bot.get_guild(bot.config['guild_id']).get_role(role_id)
                if role:
                    allowed_roles.append({
                        'id': role.id,
                        'name': role.name,
                        'color': str(role.color)
                    })
            return jsonify(allowed_roles)
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')  # 'add' or 'remove'
            role_id = data.get('id')
            
            if not all([action, role_id]):
                return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
            
            try:
                role_id = int(role_id)
            except ValueError:
                return jsonify({'status': 'error', 'message': 'Invalid ID format'}), 400
            
            if action == 'add':
                if role_id not in bot.config['allowed_roles']:
                    bot.config['allowed_roles'].append(role_id)
            elif action == 'remove':
                if role_id in bot.config['allowed_roles']:
                    bot.config['allowed_roles'].remove(role_id)
            else:
                return jsonify({'status': 'error', 'message': 'Invalid action'}), 400
            
            run_coroutine_in_bot_loop(bot.save_config())
            return jsonify({'status': 'success'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/allowed_roles: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/whitelist_users', methods=['GET'])
@basic_auth_required
def get_whitelist_users():
    try:
        guild = bot.get_guild(bot.config['guild_id'])
        users = []
        for user_id in bot.config['whitelist']['users']:
            user = guild.get_member(user_id)
            if user:
                users.append({
                    'id': user.id,
                    'name': user.display_name,
                    'avatar': str(user.avatar.url) if user.avatar else None
                })
        return jsonify(users)
    except Exception as e:
        web_logger.error(f"Erro em /api/whitelist_users: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/run_command', methods=['POST'])
@basic_auth_required
def run_command():
    try:
        data = request.json
        command = data.get('command')
        
        if not command:
            return jsonify({'status': 'error', 'message': 'No command specified'}), 400
        
        async def _run_command():
            try:
                if command == 'force_check':
                    member_id = data.get('member_id')
                    if not member_id:
                        return {'status': 'error', 'message': 'Member ID required'}
                    
                    member = bot.get_guild(bot.config['guild_id']).get_member(int(member_id))
                    if not member:
                        return {'status': 'error', 'message': 'Member not found'}
                    
                    from tasks import _execute_force_check
                    result = await _execute_force_check(member)
                    return {'status': 'success', 'result': result}
                
                elif command == 'cleanup_data':
                    days = data.get('days', 60)
                    result = await bot.db.cleanup_old_data(days)
                    return {'status': 'success', 'result': result}
                
                elif command == 'sync_commands':
                    await bot.tree.sync()
                    return {'status': 'success', 'message': 'Commands synced'}
                
                else:
                    return {'status': 'error', 'message': 'Invalid command'}
            
            except Exception as e:
                return {'status': 'error', 'message': str(e)}
        
        result = run_coroutine_in_bot_loop(_run_command())
        return jsonify(result)
    
    except Exception as e:
        web_logger.error(f"Erro em /api/run_command: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/warnings_history', methods=['GET'])
@basic_auth_required
def get_warnings_history():
    try:
        days = request.args.get('days', default=30, type=int)
        limit = request.args.get('limit', default=100, type=int)
        
        async def _get_warnings():
            async with bot.db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('''
                        SELECT 
                            user_id, 
                            warning_type, 
                            warning_date
                        FROM user_warnings
                        WHERE guild_id = %s
                        AND warning_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        ORDER BY warning_date DESC
                        LIMIT %s
                    ''', (bot.config['guild_id'], days, limit))
                    
                    return await cursor.fetchall()
        
        warnings = run_coroutine_in_bot_loop(_get_warnings())
        
        # Adicionar nomes de usuários se possível
        warnings_with_names = []
        guild = bot.get_guild(bot.config['guild_id'])
        
        for warning in warnings:
            member = guild.get_member(warning['user_id']) if guild else None
            warnings_with_names.append({
                'user_id': warning['user_id'],
                'user_name': member.display_name if member else str(warning['user_id']),
                'warning_type': warning['warning_type'],
                'warning_date': warning['warning_date']
            })
        
        return jsonify(warnings_with_names)
    
    except Exception as e:
        web_logger.error(f"Erro em /api/warnings_history: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/kicks_history', methods=['GET'])
@basic_auth_required
def get_kicks_history():
    try:
        days = request.args.get('days', default=30, type=int)
        limit = request.args.get('limit', default=100, type=int)
        
        async def _get_kicks():
            async with bot.db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('''
                        SELECT k.user_id, k.kick_date, k.reason
                        FROM kicked_members k
                        WHERE k.guild_id = %s
                        AND k.kick_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        ORDER BY k.kick_date DESC
                        LIMIT %s
                    ''', (bot.config['guild_id'], days, limit))
                    
                    return await cursor.fetchall()
        
        kicks = run_coroutine_in_bot_loop(_get_kicks())
        
        # Adicionar nomes de usuários se possível
        kicks_with_names = []
        guild = bot.get_guild(bot.config['guild_id'])
        
        for kick in kicks:
            member = guild.get_member(kick['user_id']) if guild else None
            kicks_with_names.append({
                'user_id': kick['user_id'],
                'user_name': member.display_name if member else str(kick['user_id']),
                'kick_date': kick['kick_date'],
                'reason': kick['reason']
            })
        
        return jsonify(kicks_with_names)
    
    except Exception as e:
        web_logger.error(f"Erro em /api/kicks_history: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

def run_flask():
    try:
        app.run(host='0.0.0.0', port=8080, threaded=True)
    except Exception as e:
        web_logger.critical(f"Erro ao iniciar servidor Flask: {e}", exc_info=True)
        raise

def keep_alive():
    try:
        t = Thread(target=run_flask)
        t.daemon = True
        t.start()
        web_logger.info("Web panel iniciado na porta 8080")
    except Exception as e:
        web_logger.critical(f"Erro ao iniciar thread do web panel: {e}", exc_info=True)
        raise