from flask import Flask, jsonify, render_template, request
from threading import Thread
from main import bot

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