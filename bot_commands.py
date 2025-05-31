import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional, List
from datetime import datetime, timedelta
import logging
from main import bot, allowed_roles_only

logger = logging.getLogger('inactivity_bot')

# Comandos de administração para gerenciar cargos permitidos
@bot.tree.command(name="allow_role", description="Adiciona um cargo à lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def allow_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id not in bot.config['allowed_roles']:
            bot.config['allowed_roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(
                f"✅ Cargo {role.mention} adicionado à lista de permissões.")
        else:
            await interaction.response.send_message(
                "ℹ️ Este cargo já está na lista de permissões.",
                ephemeral=True)
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo permitido: {e}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="disallow_role", description="Remove um cargo da lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def disallow_role(interaction: discord.Interaction, role: discord.Role):
    try:
        if role.id in bot.config['allowed_roles']:
            bot.config['allowed_roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(
                f"✅ Cargo {role.mention} removido da lista de permissões.")
        else:
            await interaction.response.send_message(
                "ℹ️ Este cargo não estava na lista de permissões.",
                ephemeral=True)
    except Exception as e:
        logger.error(f"Erro ao remover cargo permitido: {e}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="list_allowed_roles", description="Lista os cargos com permissão para usar comandos")
async def list_allowed_roles(interaction: discord.Interaction):
    try:
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
        bot.config['monitoring_period'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Período de monitoramento definido para {days} dias.")
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
        bot.config['required_minutes'] = minutes
        bot.config['required_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Requisitos definidos para {minutes} minutos em {days} dias diferentes "
            f"dentro de {bot.config['monitoring_period']} dias.")
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
        bot.config['kick_after_days'] = days
        bot.save_config()
        await interaction.response.send_message(
            f"Configuração atualizada: Membros sem cargo serão expulsos após {days} dias.")
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
        if role.id not in bot.config['tracked_roles']:
            bot.config['tracked_roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} adicionado à lista de monitorados.")
            await bot.notify_roles(f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.")
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
        if role.id in bot.config['tracked_roles']:
            bot.config['tracked_roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.name} removido da lista de monitorados.")
            await bot.notify_roles(f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.")
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
        bot.config['notification_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de notificações definido para {channel.mention}")
        await channel.send("✅ Este canal foi definido como o canal de notificações de cargos!")
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
        if first <= second:
            return await interaction.response.send_message(
                "O primeiro aviso deve ser enviado antes do segundo aviso.")
        
        bot.config['warnings']['first_warning'] = first
        bot.config['warnings']['second_warning'] = second
        bot.save_config()
        await interaction.response.send_message(
            f"Avisos configurados: primeiro aviso {first} dias antes, segundo aviso {second} dia(s) antes.")
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
        if warning_type not in ['first', 'second', 'final']:
            return await interaction.response.send_message(
                "Tipo de aviso inválido. Use 'first', 'second' ou 'final'.")
        
        bot.config['warnings']['messages'][warning_type] = message
        bot.save_config()
        await interaction.response.send_message(f"Mensagem de {warning_type} atualizada com sucesso.")
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
        if user.id not in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].append(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} adicionado à whitelist.")
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
        if role.id not in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].append(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} adicionado à whitelist.")
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
        if user.id in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].remove(user.id)
            bot.save_config()
            await interaction.response.send_message(f"Usuário {user.mention} removido da whitelist.")
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
        if role.id in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].remove(role.id)
            bot.save_config()
            await interaction.response.send_message(f"Cargo {role.mention} removido da whitelist.")
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
        bot.config['absence_channel'] = channel.id
        bot.save_config()
        await interaction.response.send_message(f"Canal de ausência definido para {channel.mention}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de ausência: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao definir o canal de ausência. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="show_config", description="Mostra a configuração atual do bot")
@allowed_roles_only()
async def show_config(interaction: discord.Interaction):
    try:
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
    except Exception as e:
        logger.error(f"Erro ao mostrar configuração: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao mostrar a configuração. Por favor, tente novamente.",
            ephemeral=True)

@bot.tree.command(name="check_user", description="Verifica a atividade de um usuário")
@allowed_roles_only()
async def check_user(interaction: discord.Interaction, member: discord.Member):
    try:
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
    except Exception as e:
        logger.error(f"Erro ao verificar usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")

@bot.tree.command(name="check_user_history", description="Verifica o histórico completo de um usuário")
@allowed_roles_only()
async def check_user_history(interaction: discord.Interaction, member: discord.Member):
    try:
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        last_warning = await bot.db.get_last_warning(member.id, member.guild.id)
        last_check = await bot.db.get_last_period_check(member.id, member.guild.id)
        
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
        logger.error(f"Erro ao verificar histórico do usuário: {e}")
        await interaction.response.send_message(
            "Ocorreu um erro ao verificar o histórico do usuário. Por favor, tente novamente mais tarde.",
            ephemeral=True)
        try:
            bot.db.reconnect()
        except Exception as db_error:
            logger.error(f"Falha ao reconectar ao banco de dados: {db_error}")