import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional, List
from datetime import datetime, timedelta
import logging
from main import bot, allowed_roles_only
import asyncio

logger = logging.getLogger('inactivity_bot')

# Comandos de administração para gerenciar cargos permitidos
@bot.tree.command(name="allow_role", description="Adiciona um cargo à lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def allow_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando allow_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id not in bot.config['allowed_roles']:
            bot.config['allowed_roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(
                f"✅ Cargo {role.mention} adicionado à lista de permissões.")
            logger.info(f"Cargo {role.name} adicionado com sucesso à lista de permissões")
        else:
            await interaction.response.send_message(
                "ℹ️ Este cargo já está na lista de permissões.",
                ephemeral=True)
            logger.info(f"Cargo {role.name} já estava na lista de permissões")
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo permitido: {e}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="disallow_role", description="Remove um cargo da lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def disallow_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando disallow_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id in bot.config['allowed_roles']:
            bot.config['allowed_roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(
                f"✅ Cargo {role.mention} removido da lista de permissões.")
            logger.info(f"Cargo {role.name} removido com sucesso da lista de permissões")
        else:
            await interaction.response.send_message(
                "ℹ️ Este cargo não estava na lista de permissões.",
                ephemeral=True)
            logger.info(f"Cargo {role.name} não estava na lista de permissões")
    except Exception as e:
        logger.error(f"Erro ao remover cargo permitido: {e}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="list_allowed_roles", description="Lista os cargos com permissão para usar comandos")
async def list_allowed_roles(interaction: discord.Interaction):
    try:
        logger.info(f"Comando list_allowed_roles acionado por {interaction.user}")
        if not bot.config['allowed_roles']:
            await interaction.response.send_message(
                "ℹ️ Nenhum cargo foi definido como permitido. Todos os membros podem usar comandos.",
                ephemeral=True)
            return

        roles = []
        for role_id in bot.config['allowed_roles']:
            role = interaction.guild.get_role(role_id)
            if role:
                roles.append(role.mention)

        embed = discord.Embed(
            title="Cargos com Permissão",
            description="\n".join(roles) if roles else "Nenhum cargo definido",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info("Lista de cargos permitidos exibida com sucesso")
    except Exception as e:
        logger.error(f"Erro ao listar cargos permitidos: {e}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao listar os cargos. Por favor, tente novamente.",
            ephemeral=True)

# Comandos Slash com restrição de cargos permitidos
@bot.tree.command(name="set_inactivity", description="Define o número de dias do período de monitoramento")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_inactivity(interaction: discord.Interaction, days: int):
    try:
        logger.info(f"Comando set_inactivity acionado por {interaction.user} com {days} dias")
        bot.config['monitoring_period'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Período de monitoramento definido para {days} dias.")
        logger.info(f"Período de monitoramento atualizado para {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir período de inatividade: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_requirements", description="Define os requisitos de atividade (minutos e dias)")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_requirements(interaction: discord.Interaction, minutes: int, days: int):
    try:
        logger.info(f"Comando set_requirements acionado por {interaction.user} com {minutes} minutos e {days} dias")
        bot.config['required_minutes'] = minutes
        bot.config['required_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Requisitos definidos para {minutes} minutos em {days} dias diferentes "
            f"dentro de {bot.config['monitoring_period']} dias.")
        logger.info(f"Requisitos atualizados para {minutes} minutos em {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir requisitos: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_kick_days", description="Define após quantos dias sem cargo o membro será expulso")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_kick_days(interaction: discord.Interaction, days: int):
    try:
        logger.info(f"Comando set_kick_days acionado por {interaction.user} com {days} dias")
        bot.config['kick_after_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Membros sem cargo serão expulsos após {days} dias.")
        logger.info(f"Dias para expulsão atualizados para {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir dias para expulsão: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="add_tracked_role", description="Adiciona um cargo à lista de cargos monitorados")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def add_tracked_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando add_tracked_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id not in bot.config['tracked_roles']:
            bot.config['tracked_roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} adicionado à lista de monitorados.")
            await bot.notify_roles(f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.")
            logger.info(f"Cargo {role.name} adicionado à lista de monitorados")
        else:
            await interaction.response.send_message("Este cargo já está sendo monitorado.")
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo monitorado: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="remove_tracked_role", description="Remove um cargo da lista de cargos monitorados")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def remove_tracked_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando remove_tracked_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id in bot.config['tracked_roles']:
            bot.config['tracked_roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} removido da lista de monitorados.")
            await bot.notify_roles(f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.")
            logger.info(f"Cargo {role.name} removido da lista de monitorados")
        else:
            await interaction.response.send_message("Este cargo não estava sendo monitorado.")
    except Exception as e:
        logger.error(f"Erro ao remover cargo monitorado: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_notification_channel", description="Define o canal para notificações de cargos")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_notification_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    try:
        logger.info(f"Comando set_notification_channel acionado por {interaction.user} para o canal {channel.name}")
        bot.config['notification_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de notificações definido para {channel.mention}")
        await channel.send("✅ Este canal foi definido como o canal de notificações de cargos!")
        logger.info(f"Canal de notificações definido para {channel.name}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de notificações: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao definir o canal. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_warning_days", description="Define os dias para os avisos de inatividade")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_warning_days(interaction: discord.Interaction, first: int, second: int):
    try:
        logger.info(f"Comando set_warning_days acionado por {interaction.user} com {first} e {second} dias")
        if first <= second:
            return await interaction.response.send_message(
                "O primeiro aviso deve ser enviado antes do segundo aviso.")
        
        bot.config['warnings']['first_warning'] = first
        bot.config['warnings']['second_warning'] = second
        bot.save_config()
        await interaction.response.send_message(
            f"Avisos configurados: primeiro aviso {first} dias antes, segundo aviso {second} dia(s) antes.")
        logger.info(f"Dias de aviso atualizados para {first} e {second} dias")
    except Exception as e:
        logger.error(f"Erro ao configurar dias de aviso: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao configurar os avisos. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_warning_message", description="Define a mensagem para um tipo de aviso")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_warning_message(interaction: discord.Interaction, warning_type: str, message: str):
    try:
        logger.info(f"Comando set_warning_message acionado por {interaction.user} para o tipo {warning_type}")
        if warning_type not in ['first', 'second', 'final']:
            return await interaction.response.send_message(
                "Tipo de aviso inválido. Use 'first', 'second' ou 'final'.")
        
        bot.config['warnings']['messages'][warning_type] = message
        bot.save_config()
        await interaction.response.send_message(f"Mensagem de {warning_type} atualizada com sucesso.")
        logger.info(f"Mensagem de aviso {warning_type} atualizada")
    except Exception as e:
        logger.error(f"Erro ao definir mensagem de aviso: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao atualizar a mensagem. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_add_user", description="Adiciona um usuário à whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_add_user(interaction: discord.Interaction, user: discord.User):
    try:
        logger.info(f"Comando whitelist_add_user acionado por {interaction.user} para o usuário {user.name}")
        if user.id not in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].append(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} adicionado à whitelist.")
            logger.info(f"Usuário {user.name} adicionado à whitelist")
        else:
            await interaction.response.send_message("Este usuário já está na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao adicionar usuário à whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o usuário. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_add_role", description="Adiciona um cargo à whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_add_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando whitelist_add_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id not in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} adicionado à whitelist.")
            logger.info(f"Cargo {role.name} adicionado à whitelist")
        else:
            await interaction.response.send_message("Este cargo já está na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo à whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_remove_user", description="Remove um usuário da whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_remove_user(interaction: discord.Interaction, user: discord.User):
    try:
        logger.info(f"Comando whitelist_remove_user acionado por {interaction.user} para o usuário {user.name}")
        if user.id in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].remove(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} removido da whitelist.")
            logger.info(f"Usuário {user.name} removido da whitelist")
        else:
            await interaction.response.send_message("Este usuário não estava na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao remover usuário da whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o usuário. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="whitelist_remove_role", description="Remove um cargo da whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_remove_role(interaction: discord.Interaction, role: discord.Role):
    try:
        logger.info(f"Comando whitelist_remove_role acionado por {interaction.user} para o cargo {role.name}")
        if role.id in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} removido da whitelist.")
            logger.info(f"Cargo {role.name} removido da whitelist")
        else:
            await interaction.response.send_message("Este cargo não estava na whitelist.")
    except Exception as e:
        logger.error(f"Erro ao remover cargo da whitelist: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="set_absence_channel", description="Define o canal de voz de ausência")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_absence_channel(interaction: discord.Interaction, channel: discord.VoiceChannel):
    try:
        logger.info(f"Comando set_absence_channel acionado por {interaction.user} para o canal {channel.name}")
        bot.config['absence_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de ausência definido para {channel.mention}")
        logger.info(f"Canal de ausência definido para {channel.name}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de ausência: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao definir o canal de ausência. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="show_config", description="Mostra a configuração atual do bot")
@allowed_roles_only()
async def show_config(interaction: discord.Interaction):
    try:
        logger.info(f"Comando show_config acionado por {interaction.user}")
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
        
        allowed_roles = []
        for role_id in config['allowed_roles']:
            role = interaction.guild.get_role(role_id)
            if role:
                allowed_roles.append(role.mention)
        
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
            name="Cargos Permitidos",
            value="\n".join(allowed_roles) if allowed_roles else "Todos podem usar comandos",
            inline=False)
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
        logger.info("Configuração exibida com sucesso")
    except Exception as e:
        logger.error(f"Erro ao mostrar configuração: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao mostrar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="check_user", description="Verifica a atividade de um usuário")
@allowed_roles_only()
async def check_user(interaction: discord.Interaction, member: discord.Member):
    try:
        logger.info(f"Comando check_user acionado por {interaction.user} para o membro {member.name}")
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
        logger.info(f"Informações de atividade de {member.name} exibidas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao verificar usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
            logger.info("Tentativa de reconexão ao banco de dados realizada")
        except Exception as db_error:
            logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

async def _execute_check_user_history(interaction: discord.Interaction, member: discord.Member):
    try:
        logger.info(f"Iniciando check_user_history para {member.name} solicitado por {interaction.user}")
        
        # Adiciona um pequeno delay antes de cada operação que pode causar rate limit
        await asyncio.sleep(1)
        
        # 1. Coletar todos os dados necessários
        logger.info("Coletando dados básicos do usuário...")
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        
        # Adiciona delay entre consultas
        await asyncio.sleep(0.5)
        
        logger.info("Coletando sessões de voz (últimos 90 dias)...")
        end_date = datetime.now(bot.timezone)
        start_date = end_date - timedelta(days=90)
        voice_sessions = await bot.db.get_voice_sessions(member.id, member.guild.id, start_date, end_date)
        
        # Adiciona delay entre consultas
        await asyncio.sleep(0.5)
        
        logger.info("Coletando avisos, cargos removidos e verificações de período...")
        cursor = None
        try:
            cursor = bot.db.connection.cursor(dictionary=True)
            
            # Avisos
            cursor.execute('''
            SELECT warning_type, warning_date 
            FROM user_warnings 
            WHERE user_id = %s AND guild_id = %s
            ORDER BY warning_date DESC
            ''', (member.id, member.guild.id))
            all_warnings = cursor.fetchall()
            
            await asyncio.sleep(0.5)
            
            # Cargos removidos
            cursor.execute('''
            SELECT role_id, removal_date 
            FROM removed_roles 
            WHERE user_id = %s AND guild_id = %s
            ORDER BY removal_date DESC
            ''', (member.id, member.guild.id))
            removed_roles = cursor.fetchall()
            
            await asyncio.sleep(0.5)
            
            # Períodos verificados
            cursor.execute('''
            SELECT period_start, period_end, meets_requirements
            FROM checked_periods
            WHERE user_id = %s AND guild_id = %s
            ORDER BY period_start DESC
            LIMIT 5
            ''', (member.id, member.guild.id))
            period_checks = cursor.fetchall()
            
        finally:
            if cursor:
                cursor.close()
        
        logger.info("Processando estatísticas...")
        # Estatísticas gerais
        total_sessions = user_data.get('voice_sessions', 0) if user_data else 0
        total_time = user_data.get('total_voice_time', 0) if user_data else 0
        avg_session = total_time / total_sessions if total_sessions > 0 else 0
        
        # Encontrar sessão mais longa
        longest_session = max(voice_sessions, key=lambda x: x['duration'], default=None)
        
        # Padrões de horário (agora como texto)
        hour_counts = [0] * 24
        for session in voice_sessions:
            hour = session['join_time'].hour
            hour_counts[hour] += session['duration'] / 3600  # em horas
        
        hour_stats = "\n".join(
            f"{h:02d}h-{(h+1)%24:02d}h: {int(count)}h {int((count%1)*60)}m" 
            for h, count in enumerate(hour_counts) if count > 0
        )
        
        # Dias da semana
        weekday_names = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        weekday_counts = [0] * 7
        for session in voice_sessions:
            weekday = session['join_time'].weekday()
            weekday_counts[weekday] += session['duration'] / 3600  # em horas
        
        weekday_stats = "\n".join(
            f"{weekday_names[i]}: {int(count)}h {int((count%1)*60)}m" 
            for i, count in enumerate(weekday_counts) if count > 0
        )
        
        # 4. Criar o embed principal
        logger.info("Criando embed de relatório...")
        embed = discord.Embed(
            title=f"📊 Relatório Completo de {member.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(bot.timezone))
        
        embed.set_thumbnail(url=member.display_avatar.url)
        
        # 5. Seção de Estatísticas Gerais
        stats_value = (
            f"🎙️ **Total de Sessões:** {total_sessions}\n"
            f"⏱️ **Tempo Total:** {int(total_time//3600)}h {int((total_time%3600)//60)}m\n"
            f"📅 **Média por Sessão:** {int(avg_session//60)}m\n"
        )
        
        if longest_session:
            stats_value += (
                f"🏆 **Sessão Mais Longa:** {int(longest_session['duration']//3600)}h "
                f"{int((longest_session['duration']%3600)//60)}m "
                f"(em {longest_session['join_time'].strftime('%d/%m/%Y')}\n"
            )
        
        embed.add_field(name="📈 Estatísticas Gerais", value=stats_value, inline=False)
        
        # 6. Seção de Status Atual
        last_check = period_checks[0] if period_checks else None
        if last_check:
            period_start = last_check['period_start'].replace(tzinfo=bot.timezone)
            period_end = last_check['period_end'].replace(tzinfo=bot.timezone)
            meets_req = last_check['meets_requirements']
            
            # Calcular dias restantes
            days_left = (period_end - datetime.now(bot.timezone)).days
            
            status_value = (
                f"📅 **Período:** {period_start.strftime('%d/%m/%Y')} - {period_end.strftime('%d/%m/%Y')}\n"
                f"⏳ **Dias Restantes:** {days_left}\n"
                f"✅ **Status:** {'Cumprindo' if meets_req else 'Não cumprindo'}\n"
                f"🎯 **Requisitos:** {bot.config['required_minutes']}min em {bot.config['required_days']} dias"
            )
            
            embed.add_field(name="🔄 Status Atual", value=status_value, inline=False)
        
        # 7. Seção de Histórico Recente
        if voice_sessions:
            recent_sessions = voice_sessions[:5]  # Últimas 5 sessões
            sessions_value = "\n".join(
                f"▸ {s['join_time'].strftime('%d/%m %H:%M')} - "
                f"{int(s['duration']//60)}min"
                for s in recent_sessions
            )
            embed.add_field(name="🕒 Últimas Sessões", value=sessions_value, inline=True)
        
        # 8. Seção de Padrões de Atividade
        activity_patterns = ""
        if hour_stats:
            activity_patterns += f"**Horários mais ativos:**\n{hour_stats}\n\n"
        if weekday_stats:
            activity_patterns += f"**Dias mais ativos:**\n{weekday_stats}"
        
        if activity_patterns:
            embed.add_field(name="⏰ Padrões de Atividade", value=activity_patterns, inline=True)
        
        # 9. Seção de Cargos
        current_roles = [role for role in member.roles if role.id in bot.config['tracked_roles']]
        roles_value = "Nenhum cargo monitorado"
        if current_roles:
            roles_value = "\n".join(f"▸ {role.mention}" for role in current_roles)
        
        embed.add_field(name="🎖️ Cargos Atuais", value=roles_value, inline=True)
        
        # 10. Seção de Avisos
        if all_warnings:
            warnings_value = "\n".join(
                f"▸ {w['warning_type']} - {w['warning_date'].strftime('%d/%m/%Y')}"
                for w in all_warnings[:3]  # Mostrar apenas 3 avisos recentes
            )
            embed.add_field(name="⚠️ Avisos Recentes", value=warnings_value, inline=True)
        
        embed.set_footer(text=f"ID do usuário: {member.id} | Período: 90 dias")
        
        logger.info("Enviando relatório...")
        try:
            await interaction.followup.send(embed=embed)
            logger.info("Relatório enviado com sucesso")
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = e.response.headers.get('Retry-After', 60)
                logger.error(f"Rate limit ao enviar embed. Tentando novamente em {retry_after} segundos")
                await asyncio.sleep(float(retry_after))
                await interaction.followup.send(embed=embed)  # Tentar novamente
            else:
                raise
        
    except Exception as e:
        logger.error(f"Erro ao gerar relatório completo: {e}", exc_info=True)
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro ao gerar o relatório completo. Por favor, tente novamente mais tarde.",
                ephemeral=True)
        except Exception as followup_error:
            logger.error(f"Erro ao enviar mensagem de erro: {followup_error}")

@bot.tree.command(name="check_user_history", description="Relatório completo de atividade do usuário")
@app_commands.checks.cooldown(1, 120.0, key=lambda i: (i.guild_id, i.user.id))  # Aumentado para 120 segundos
@allowed_roles_only()
async def check_user_history(interaction: discord.Interaction, member: discord.Member):
    try:
        await interaction.response.defer(ephemeral=True)
        # Adiciona um pequeno delay antes de processar para evitar rate limits
        await asyncio.sleep(1)
        
        # Adiciona o comando à fila para processamento
        await bot.command_queue.put((
            interaction,
            _execute_check_user_history,
            [member],
            {}
        ))
    except Exception as e:
        logger.error(f"Erro ao enfileirar check_user_history: {e}")
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro ao processar sua requisição.",
                ephemeral=True)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de erro: {e}")

@check_user_history.error
async def check_user_history_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        logger.warning(f"Comando check_user_history em cooldown para {interaction.user}: {error}")
        try:
            await interaction.response.send_message(
                f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
                ephemeral=True)
        except discord.errors.HTTPException as http_error:
            logger.error(f"Erro ao enviar mensagem de cooldown: {http_error}")
    
    elif isinstance(error, discord.errors.HTTPException) and error.status == 429:
        retry_after = error.response.headers.get('Retry-After', 60)
        logger.error(f"Rate limit atingido no comando check_user_history. Tentar novamente após {retry_after} segundos")
        try:
            await asyncio.sleep(float(retry_after))
            await interaction.followup.send(
                f"⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente em {retry_after} segundos.",
                ephemeral=True)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de rate limit: {e}")
    
    elif isinstance(error, Exception):
        logger.error(f"Erro não tratado em check_user_history: {error}", exc_info=True)
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro inesperado. Por favor, tente novamente mais tarde.",
                ephemeral=True)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de erro: {e}")