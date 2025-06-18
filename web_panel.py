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

@app.route('/')
@basic_auth_required
def home():
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
@basic_auth_required
def dashboard():
    try:
        # Estatísticas básicas do bot
        uptime = (datetime.datetime.now() - bot.start_time) if hasattr(bot, 'start_time') else "N/A"
        
        # Processar informações das guildas
        guild_stats = []
        for guild in bot.guilds:
            voice_channels = len([c for c in guild.channels if isinstance(c, discord.VoiceChannel)])
            guild_stats.append({
                'id': guild.id,
                'name': guild.name,
                'members': guild.member_count,
                'voice_channels': voice_channels,
                'icon': guild.icon.url if guild.icon else None
            })
        
        # Status do banco de dados
        db_status = "Desconhecido"
        pool_status = {}
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status = asyncio.run(bot.db.check_pool_status()) or {}
                db_status = "Operacional" if pool_status else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
                web_logger.error(f"Erro ao verificar status do banco: {e}")
        
        # Status das filas
        queue_status = {
            'voice_events': 0,
            'messages': {'critical': 0, 'high': 0, 'normal': 0, 'low': 0}
        }
        if hasattr(bot, 'voice_event_queue'):
            queue_status['voice_events'] = bot.voice_event_queue.qsize()
        if hasattr(bot, 'message_queue'):
            queue_status['messages'] = bot.message_queue.qsize()
        
        # Configurações atuais
        current_config = {
            'required_minutes': bot.config.get('required_minutes', 15),
            'required_days': bot.config.get('required_days', 2),
            'monitoring_period': bot.config.get('monitoring_period', 14),
            'kick_after_days': bot.config.get('kick_after_days', 30)
        }
        
        return render_template('dashboard.html', 
                            bot_name=bot.user.name if bot.user else "Inactivity Bot",
                            guild_count=len(bot.guilds),
                            guild_stats=guild_stats,
                            uptime=str(uptime).split('.')[0] if uptime != "N/A" else uptime,
                            db_status=db_status,
                            pool_status=pool_status,
                            queue_status=queue_status,
                            current_config=current_config)
    
    except Exception as e:
        web_logger.error(f"Erro na rota dashboard: {e}")
        return render_template('error.html', error_message="Erro ao carregar o dashboard"), 500

@app.route('/monitor')
@basic_auth_required
def monitor():
    try:
        # Obter status do banco de dados
        db_status = "Desconhecido"
        pool_status = {}
        if hasattr(bot, 'db') and bot.db:
            try:
                pool_status = asyncio.run(bot.db.check_pool_status()) or {}
                db_status = "Operacional" if pool_status else "Erro"
            except Exception as e:
                db_status = f"Erro: {str(e)}"
        
        # Obter status de rate limits
        rate_limits = {}
        if hasattr(bot, 'rate_limit_monitor'):
            rate_limits = bot.rate_limit_monitor.get_status_report()
        
        # Obter estatísticas de uso
        queue_status = {}
        if hasattr(bot, 'message_queue'):
            queue_status = bot.message_queue.qsize()
        
        # Processar eventos recentes
        recent_events = []
        if hasattr(bot, 'rate_limit_monitor') and hasattr(bot.rate_limit_monitor, 'history'):
            for event in list(bot.rate_limit_monitor.history)[-10:]:
                recent_events.append({
                    'time': datetime.datetime.fromtimestamp(event['time']).strftime('%H:%M:%S'),
                    'bucket': event['bucket'],
                    'remaining': event['remaining'],
                    'endpoint': event.get('endpoint', 'unknown')
                })
        
        # Obter logs recentes
        log_lines = []
        try:
            with open('bot.log', 'r') as log_file:
                log_lines = deque(log_file, 100)  # Últimas 100 linhas
        except Exception as e:
            web_logger.warning(f"Erro ao ler arquivo de log: {e}")
        
        return render_template('monitor.html',
                            db_status=db_status,
                            pool_status=pool_status,
                            rate_limits=rate_limits,
                            queue_status=queue_status,
                            recent_events=recent_events,
                            log_lines=log_lines,
                            bot_name=bot.user.name if bot.user else "Inactivity Bot")
    
    except Exception as e:
        web_logger.error(f"Erro na rota monitor: {e}")
        return render_template('error.html', error_message="Erro ao carregar o monitor"), 500

@app.route('/api/guilds')
@basic_auth_required
def get_guilds():
    try:
        guilds = []
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
        web_logger.error(f"Erro em /api/guilds: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/guild/<int:guild_id>')
@basic_auth_required
def get_guild_info(guild_id):
    try:
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
        
        # Adicionar estatísticas de atividade (simuladas - implementação real depende do seu banco de dados)
        activity_stats = {
            'active_users': 0,
            'inactive_users': 0,
            'warned_users': 0
        }
        
        # Obter canais de voz
        voice_channels = []
        for channel in guild.channels:
            if isinstance(channel, discord.VoiceChannel):
                voice_channels.append({
                    'id': channel.id,
                    'name': channel.name,
                    'user_count': len(channel.members)
                })
        
        # Obter configurações de notificação
        notification_settings = {
            'log_channel': bot.config.get('log_channel'),
            'notification_channel': bot.config.get('notification_channel'),
            'absence_channel': bot.config.get('absence_channel')
        }
        
        return jsonify({
            'id': guild.id,
            'name': guild.name,
            'icon': guild.icon.url if guild.icon else None,
            'tracked_roles': tracked_roles,
            'activity_stats': activity_stats,
            'voice_channels': voice_channels,
            'notification_settings': notification_settings,
            'config': {
                'required_minutes': bot.config['required_minutes'],
                'required_days': bot.config['required_days'],
                'monitoring_period': bot.config['monitoring_period'],
                'kick_after_days': bot.config['kick_after_days']
            }
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/guild/{guild_id}: {e}")
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
        web_logger.error(f"Erro em /api/rate_limits: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/update_config', methods=['POST'])
@basic_auth_required
def update_config():
    try:
        data = request.json
        if not data:
            return jsonify({'status': 'error', 'message': 'No data provided'}), 400
        
        if 'required_minutes' in data:
            bot.config['required_minutes'] = int(data['required_minutes'])
        if 'required_days' in data:
            bot.config['required_days'] = int(data['required_days'])
        if 'monitoring_period' in data:
            bot.config['monitoring_period'] = int(data['monitoring_period'])
        if 'kick_after_days' in data:
            bot.config['kick_after_days'] = int(data['kick_after_days'])
        
        asyncio.run(bot.save_config())
        return jsonify({'status': 'success'})
    except Exception as e:
        web_logger.error(f"Erro em /api/update_config: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/restart', methods=['POST'])
@basic_auth_required
def restart_bot():
    try:
        # Em produção, você deve usar um sistema de gerenciamento de processos como systemd ou supervisor
        # Esta é apenas uma implementação de exemplo
        web_logger.info("Reinicialização do bot solicitada")
        
        # Simular reinicialização
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
        web_logger.error(f"Erro em /api/restart: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/whitelist', methods=['GET', 'POST'])
@basic_auth_required
def manage_whitelist():
    try:
        if request.method == 'GET':
            return jsonify({
                'users': bot.config['whitelist']['users'],
                'roles': bot.config['whitelist']['roles']
            })
        
        elif request.method == 'POST':
            data = request.json
            action = data.get('action')  # 'add' or 'remove'
            target_type = data.get('type')  # 'user' or 'role'
            target_id = data.get('id')
            
            if not all([action, target_type, target_id]):
                return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
            
            target_id = int(target_id)
            config_key = 'users' if target_type == 'user' else 'roles'
            
            if action == 'add':
                if target_id not in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].append(target_id)
            elif action == 'remove':
                if target_id in bot.config['whitelist'][config_key]:
                    bot.config['whitelist'][config_key].remove(target_id)
            else:
                return jsonify({'status': 'error', 'message': 'Invalid action'}), 400
            
            asyncio.run(bot.save_config())
            return jsonify({'status': 'success'})
    
    except Exception as e:
        web_logger.error(f"Erro em /api/whitelist: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/logs')
@basic_auth_required
def get_logs():
    try:
        lines = request.args.get('lines', default=100, type=int)
        
        with open('bot.log', 'r') as log_file:
            log_lines = deque(log_file, lines)
        
        return jsonify({
            'logs': list(log_lines)
        })
    except Exception as e:
        web_logger.error(f"Erro em /api/logs: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/activity_stats')
@basic_auth_required
def get_activity_stats():
    try:
        # Esta é uma implementação simulada - você precisará adaptar para seu banco de dados
        stats = {
            'total_users': 0,
            'active_users': 0,
            'inactive_users': 0,
            'warned_users': 0,
            'kicked_users': 0
        }
        
        return jsonify(stats)
    except Exception as e:
        web_logger.error(f"Erro em /api/activity_stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup', methods=['POST'])
@basic_auth_required
def create_backup():
    try:
        if not hasattr(bot, 'db_backup'):
            return jsonify({'status': 'error', 'message': 'Backup system not initialized'}), 500
        
        success = asyncio.run(bot.db_backup.create_backup())
        
        if success:
            return jsonify({'status': 'success', 'message': 'Backup created successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to create backup'}), 500
    except Exception as e:
        web_logger.error(f"Erro em /api/backup: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

def run_flask():
    try:
        app.run(host='0.0.0.0', port=8080, threaded=True)
    except Exception as e:
        web_logger.critical(f"Erro ao iniciar servidor Flask: {e}")
        raise

def keep_alive():
    try:
        t = Thread(target=run_flask)
        t.daemon = True
        t.start()
        web_logger.info("Web panel iniciado na porta 8080")
    except Exception as e:
        web_logger.critical(f"Erro ao iniciar thread do web panel: {e}")
        raise