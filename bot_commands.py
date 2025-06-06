import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional, List
from datetime import datetime, timedelta
import logging
from main import bot, allowed_roles_only
import asyncio
from io import BytesIO
from utils import generate_activity_graph

logger = logging.getLogger('inactivity_bot')

# Comandos de administração para gerenciar cargos permitidos
@bot.tree.command(name="allow_role", description="Adiciona um cargo à lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def allow_role(interaction: discord.Interaction, role: discord.Role):
    """Adiciona um cargo à lista de cargos permitidos para usar comandos do bot"""
    try:
        logger.info(f"Comando allow_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id not in bot.config['allowed_roles']:
            bot.config['allowed_roles'].append(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Permitido Adicionado",
                description=f"O cargo {role.mention} foi adicionado à lista de permissões.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Permitido Adicionado",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            logger.info(f"Cargo {role.name} adicionado à lista de permissões")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo já está na lista de permissões.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo permitido: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="disallow_role", description="Remove um cargo da lista de cargos permitidos")
@commands.has_permissions(administrator=True)
async def disallow_role(interaction: discord.Interaction, role: discord.Role):
    """Remove um cargo da lista de cargos permitidos para usar comandos do bot"""
    try:
        logger.info(f"Comando disallow_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id in bot.config['allowed_roles']:
            bot.config['allowed_roles'].remove(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Permitido Removido",
                description=f"O cargo {role.mention} foi removido da lista de permissões.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Permitido Removido",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            logger.info(f"Cargo {role.name} removido da lista de permissões")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo não estava na lista de permissões.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao remover cargo permitido: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="list_allowed_roles", description="Lista os cargos com permissão para usar comandos")
async def list_allowed_roles(interaction: discord.Interaction):
    """Lista todos os cargos que têm permissão para usar comandos do bot"""
    try:
        logger.info(f"Comando list_allowed_roles acionado por {interaction.user}")
        
        if not bot.config['allowed_roles']:
            embed = discord.Embed(
                title="ℹ️ Cargos Permitidos",
                description="Nenhum cargo foi definido como permitido. Todos os membros podem usar comandos.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
            return

        roles = []
        for role_id in bot.config['allowed_roles']:
            role = interaction.guild.get_role(role_id)
            if role:
                roles.append(role.mention)

        embed = discord.Embed(
            title="📋 Cargos com Permissão",
            description="\n".join(roles) if roles else "Nenhum cargo definido",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed)
        logger.info("Lista de cargos permitidos exibida com sucesso")
    except Exception as e:
        logger.error(f"Erro ao listar cargos permitidos: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao listar os cargos. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

# Comandos Slash com restrição de cargos permitidos
@bot.tree.command(name="set_inactivity", description="Define o número de dias do período de monitoramento")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_inactivity(interaction: discord.Interaction, days: int):
    """Define o período de monitoramento de inatividade em dias"""
    try:
        logger.info(f"Comando set_inactivity acionado por {interaction.user} com {days} dias")
        
        if days <= 0:
            embed = discord.Embed(
                title="❌ Valor Inválido",
                description="O período de monitoramento deve ser maior que zero.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return
            
        bot.config['monitoring_period'] = days
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Configuração Atualizada",
            description=f"Período de monitoramento definido para {days} dias.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Período de Monitoramento Alterado",
            interaction.user,
            f"Novo período: {days} dias"
        )
        logger.info(f"Período de monitoramento atualizado para {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir período de inatividade: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_requirements", description="Define os requisitos de atividade (minutos e dias)")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_requirements(interaction: discord.Interaction, minutes: int, days: int):
    """Define os requisitos mínimos de atividade em minutos e dias"""
    try:
        logger.info(f"Comando set_requirements acionado por {interaction.user} com {minutes} minutos e {days} dias")
        
        if minutes <= 0 or days <= 0:
            embed = discord.Embed(
                title="❌ Valores Inválidos",
                description="Os minutos e dias devem ser maiores que zero.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return
            
        bot.config['required_minutes'] = minutes
        bot.config['required_days'] = days
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Configuração Atualizada",
            description=(
                f"Requisitos definidos para {minutes} minutos em {days} dias diferentes "
                f"dentro de {bot.config['monitoring_period']} dias."
            ),
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Requisitos de Atividade Alterados",
            interaction.user,
            f"{minutes} minutos em {days} dias"
        )
        logger.info(f"Requisitos atualizados para {minutes} minutos em {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir requisitos: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_kick_days", description="Define após quantos dias sem cargo o membro será expulso")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_kick_days(interaction: discord.Interaction, days: int):
    """Define após quantos dias sem cargos um membro será expulso do servidor"""
    try:
        logger.info(f"Comando set_kick_days acionado por {interaction.user} com {days} dias")
        
        if days <= 0:
            embed = discord.Embed(
                title="❌ Valor Inválido",
                description="O número de dias deve ser maior que zero.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return
            
        bot.config['kick_after_days'] = days
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Configuração Atualizada",
            description=f"Membros sem cargo serão expulsos após {days} dias.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Dias para Expulsão Alterados",
            interaction.user,
            f"Novo valor: {days} dias"
        )
        logger.info(f"Dias para expulsão atualizados para {days} dias")
    except Exception as e:
        logger.error(f"Erro ao definir dias para expulsão: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="add_tracked_role", description="Adiciona um cargo à lista de cargos monitorados")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def add_tracked_role(interaction: discord.Interaction, role: discord.Role):
    """Adiciona um cargo à lista de cargos monitorados para inatividade"""
    try:
        logger.info(f"Comando add_tracked_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id not in bot.config['tracked_roles']:
            bot.config['tracked_roles'].append(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Monitorado Adicionado",
                description=f"O cargo {role.mention} foi adicionado à lista de monitorados.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Monitorado Adicionado",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            
            await bot.notify_roles(
                f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.",
                is_warning=False
            )
            logger.info(f"Cargo {role.name} adicionado à lista de monitorados")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo já está sendo monitorado.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo monitorado: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove_tracked_role", description="Remove um cargo da lista de cargos monitorados")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def remove_tracked_role(interaction: discord.Interaction, role: discord.Role):
    """Remove um cargo da lista de cargos monitorados para inatividade"""
    try:
        logger.info(f"Comando remove_tracked_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id in bot.config['tracked_roles']:
            bot.config['tracked_roles'].remove(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Monitorado Removido",
                description=f"O cargo {role.mention} foi removido da lista de monitorados.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Monitorado Removido",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            
            await bot.notify_roles(
                f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.",
                is_warning=False
            )
            logger.info(f"Cargo {role.name} removido da lista de monitorados")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo não estava sendo monitorado.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao remover cargo monitorado: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_notification_channel", description="Define o canal para notificações de cargos")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_notification_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Define o canal onde serão enviadas as notificações de cargos"""
    try:
        logger.info(f"Comando set_notification_channel acionado por {interaction.user} para o canal {channel.name}")
        
        bot.config['notification_channel'] = channel.id
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Canal de Notificações Definido",
            description=f"Canal de notificações definido para {channel.mention}",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Canal de Notificações Alterado",
            interaction.user,
            f"Novo canal: {channel.name} (ID: {channel.id})"
        )
        
        await channel.send("✅ Este canal foi definido como o canal de notificações de cargos!")
        logger.info(f"Canal de notificações definido para {channel.name}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de notificações: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao definir o canal. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_warning_days", description="Define os dias para os avisos de inatividade")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_warning_days(interaction: discord.Interaction, first: int, second: int):
    """Define os dias de antecedência para os avisos de inatividade"""
    try:
        logger.info(f"Comando set_warning_days acionado por {interaction.user} com {first} e {second} dias")
        
        if first <= second:
            embed = discord.Embed(
                title="❌ Valores Inválidos",
                description="O primeiro aviso deve ser enviado antes do segundo aviso.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return
        
        bot.config['warnings']['first_warning'] = first
        bot.config['warnings']['second_warning'] = second
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Configuração de Avisos Atualizada",
            description=(
                f"Avisos configurados:\n"
                f"- Primeiro aviso: {first} dias antes\n"
                f"- Segundo aviso: {second} dia(s) antes"
            ),
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Dias de Aviso Alterados",
            interaction.user,
            f"Primeiro aviso: {first} dias, Segundo aviso: {second} dias"
        )
        logger.info(f"Dias de aviso atualizados para {first} e {second} dias")
    except Exception as e:
        logger.error(f"Erro ao configurar dias de aviso: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao configurar os avisos. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_warning_message", description="Define a mensagem para um tipo de aviso")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_warning_message(interaction: discord.Interaction, warning_type: str, message: str):
    """Define a mensagem personalizada para um tipo específico de aviso"""
    try:
        logger.info(f"Comando set_warning_message acionado por {interaction.user} para o tipo {warning_type}")
        
        if warning_type not in ['first', 'second', 'final']:
            embed = discord.Embed(
                title="❌ Tipo Inválido",
                description="Tipo de aviso inválido. Use 'first', 'second' ou 'final'.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return
        
        bot.config['warnings']['messages'][warning_type] = message
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Mensagem de Aviso Atualizada",
            description=f"Mensagem de {warning_type} atualizada com sucesso.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Mensagem de Aviso Atualizada",
            interaction.user,
            f"Tipo: {warning_type}\nNova mensagem: {message}"
        )
        logger.info(f"Mensagem de aviso {warning_type} atualizada")
    except Exception as e:
        logger.error(f"Erro ao definir mensagem de aviso: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar a mensagem. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="whitelist_add_user", description="Adiciona um usuário à whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_add_user(interaction: discord.Interaction, user: discord.User):
    """Adiciona um usuário à whitelist (não será verificado por inatividade)"""
    try:
        logger.info(f"Comando whitelist_add_user acionado por {interaction.user} para o usuário {user.name}")
        
        if user.id not in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].append(user.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Usuário Whitelistado",
                description=f"O usuário {user.mention} foi adicionado à whitelist.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Usuário Adicionado à Whitelist",
                interaction.user,
                f"Usuário: {user.name} (ID: {user.id})"
            )
            logger.info(f"Usuário {user.name} adicionado à whitelist")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este usuário já está na whitelist.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao adicionar usuário à whitelist: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao adicionar o usuário. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="whitelist_add_role", description="Adiciona um cargo à whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_add_role(interaction: discord.Interaction, role: discord.Role):
    """Adiciona um cargo à whitelist (membros com este cargo não serão verificados)"""
    try:
        logger.info(f"Comando whitelist_add_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id not in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].append(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Whitelistado",
                description=f"O cargo {role.mention} foi adicionado à whitelist.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Adicionado à Whitelist",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            logger.info(f"Cargo {role.name} adicionado à whitelist")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo já está na whitelist.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao adicionar cargo à whitelist: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao adicionar o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="whitelist_remove_user", description="Remove um usuário da whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_remove_user(interaction: discord.Interaction, user: discord.User):
    """Remove um usuário da whitelist (voltará a ser verificado por inatividade)"""
    try:
        logger.info(f"Comando whitelist_remove_user acionado por {interaction.user} para o usuário {user.name}")
        
        if user.id in bot.config['whitelist']['users']:
            bot.config['whitelist']['users'].remove(user.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Usuário Removido da Whitelist",
                description=f"O usuário {user.mention} foi removido da whitelist.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Usuário Removido da Whitelist",
                interaction.user,
                f"Usuário: {user.name} (ID: {user.id})"
            )
            logger.info(f"Usuário {user.name} removido da whitelist")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este usuário não estava na whitelist.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao remover usuário da whitelist: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao remover o usuário. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="whitelist_remove_role", description="Remove um cargo da whitelist")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_remove_role(interaction: discord.Interaction, role: discord.Role):
    """Remove um cargo da whitelist (membros com este cargo voltarão a ser verificados)"""
    try:
        logger.info(f"Comando whitelist_remove_role acionado por {interaction.user} para o cargo {role.name}")
        
        if role.id in bot.config['whitelist']['roles']:
            bot.config['whitelist']['roles'].remove(role.id)
            await bot.save_config()
            
            embed = discord.Embed(
                title="✅ Cargo Removido da Whitelist",
                description=f"O cargo {role.mention} foi removido da whitelist.",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            
            await bot.log_action(
                "Cargo Removido da Whitelist",
                interaction.user,
                f"Cargo: {role.name} (ID: {role.id})"
            )
            logger.info(f"Cargo {role.name} removido da whitelist")
        else:
            embed = discord.Embed(
                title="ℹ️ Informação",
                description="Este cargo não estava na whitelist.",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao remover cargo da whitelist: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao remover o cargo. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="set_absence_channel", description="Define o canal de voz de ausência")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_absence_channel(interaction: discord.Interaction, channel: discord.VoiceChannel):
    """Define o canal de voz que será considerado como ausência (não contabiliza tempo)"""
    try:
        logger.info(f"Comando set_absence_channel acionado por {interaction.user} para o canal {channel.name}")
        
        bot.config['absence_channel'] = channel.id
        await bot.save_config()
        
        embed = discord.Embed(
            title="✅ Canal de Ausência Definido",
            description=f"Canal de ausência definido para {channel.mention}",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
        
        await bot.log_action(
            "Canal de Ausência Alterado",
            interaction.user,
            f"Novo canal: {channel.name} (ID: {channel.id})"
        )
        logger.info(f"Canal de ausência definido para {channel.name}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de ausência: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao definir o canal de ausência. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="show_config", description="Mostra a configuração atual do bot")
@allowed_roles_only()
async def show_config(interaction: discord.Interaction):
    """Mostra todas as configurações atuais do bot"""
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
            title="⚙️ Configuração do Bot",
            color=discord.Color.blue())
        
        # Seção de Requisitos
        embed.add_field(
            name="📊 Requisitos de Atividade",
            value=(
                f"**Minutos necessários:** {config['required_minutes']}\n"
                f"**Dias necessários:** {config['required_days']}\n"
                f"**Período de monitoramento:** {config['monitoring_period']} dias\n"
                f"**Expulsão sem cargo:** {config['kick_after_days']} dias"
            ),
            inline=False
        )
        
        # Seção de Canais
        embed.add_field(
            name="📌 Canais",
            value=(
                f"**Logs:** <#{config['log_channel']}>\n"
                f"**Notificações:** <#{config['notification_channel']}>\n"
                f"**Ausência:** <#{config['absence_channel']}>" if config.get('absence_channel') else "**Ausência:** Não definido"
            ),
            inline=True
        )
        
        # Seção de Whitelist
        embed.add_field(
            name="🛡️ Whitelist",
            value=(
                f"**Usuários:** {len(whitelist_users)}\n"
                f"**Cargos:** {len(whitelist_roles)}"
            ),
            inline=True
        )
        
        # Seção de Cargos
        embed.add_field(
            name="🎖️ Cargos",
            value=(
                f"**Monitorados:** {len(tracked_roles)}\n"
                f"**Permitidos:** {len(allowed_roles)}"
            ),
            inline=True
        )
        
        # Seção de Avisos
        if warnings_config:
            embed.add_field(
                name="⚠️ Configurações de Avisos",
                value=(
                    f"**Primeiro aviso:** {warnings_config.get('first_warning', 'N/A')} dias antes\n"
                    f"**Segundo aviso:** {warnings_config.get('second_warning', 'N/A')} dia(s) antes"
                ),
                inline=False
            )
        
        await interaction.response.send_message(embed=embed)
        logger.info("Configuração exibida com sucesso")
    except Exception as e:
        logger.error(f"Erro ao mostrar configuração: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao mostrar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="check_user", description="Verifica a atividade de um usuário")
@allowed_roles_only()
async def check_user(interaction: discord.Interaction, member: discord.Member):
    """Verifica as estatísticas de atividade de um usuário específico"""
    try:
        logger.info(f"Comando check_user acionado por {interaction.user} para o membro {member.name}")
        
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        last_join = user_data.get('last_voice_join')
        sessions = user_data.get('voice_sessions', 0)
        total_time = user_data.get('total_voice_time', 0)
        last_warning = await bot.db.get_last_warning(member.id, member.guild.id)
        last_check = await bot.db.get_last_period_check(member.id, member.guild.id)
        
        embed = discord.Embed(
            title=f"📊 Atividade de {member.display_name}",
            color=discord.Color.green(),
            timestamp=datetime.now(bot.timezone)
        )

        embed.set_thumbnail(url=member.display_avatar.url)

        embed.add_field(name="Último Join na Call", value=last_join or "N/A", inline=False)
        embed.add_field(name="Sessões em Call", value=str(sessions), inline=True)
        embed.add_field(name="Tempo Total em Call", value=f"{total_time} minutos", inline=True)
        embed.add_field(name="Último Aviso", value=last_warning or "Nenhum", inline=False)
        embed.add_field(name="Última Verificação", value=last_check or "Nenhuma", inline=False)

        await interaction.response.send_message(embed=embed)

    except Exception as e:
        logger.error(f"Erro ao executar check_user: {e}")
        await interaction.response.send_message("❌ Ocorreu um erro ao verificar a atividade do usuário.", ephemeral=True)

async def _execute_check_user_history(interaction: discord.Interaction, member: discord.Member):
    """Executa a verificação completa do histórico de um usuário"""
    try:
        logger.info(f"Iniciando check_user_history para {member.name} solicitado por {interaction.user}")
        
        # Delay inicial maior para evitar rate limits
        await asyncio.sleep(2)
        
        # 1. Coletar todos os dados necessários com delays entre consultas
        logger.info("Coletando dados básicos do usuário...")
        user_data = await bot.db.get_user_activity(member.id, member.guild.id)
        
        await asyncio.sleep(1)  # Delay entre consultas
        
        logger.info("Coletando sessões de voz (últimos 30 dias)...")
        end_date = datetime.now(bot.timezone)
        start_date = end_date - timedelta(days=30)  # Reduzido de 90 para 30 dias
        voice_sessions = await bot.db.get_voice_sessions(member.id, member.guild.id, start_date, end_date)
        
        await asyncio.sleep(1)  # Delay entre consultas
        
        logger.info("Coletando avisos, cargos removidos e verificações de período...")
        async with bot.db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Avisos
                await cursor.execute('''
                SELECT warning_type, warning_date 
                FROM user_warnings 
                WHERE user_id = %s AND guild_id = %s
                ORDER BY warning_date DESC
                LIMIT 5  # Limite adicionado
                ''', (member.id, member.guild.id))
                all_warnings = await cursor.fetchall()
                
                await asyncio.sleep(0.5)  # Delay entre consultas
                
                # Cargos removidos
                await cursor.execute('''
                SELECT role_id, removal_date 
                FROM removed_roles 
                WHERE user_id = %s AND guild_id = %s
                ORDER BY removal_date DESC
                LIMIT 5  # Limite adicionado
                ''', (member.id, member.guild.id))
                removed_roles = await cursor.fetchall()
                
                await asyncio.sleep(0.5)  # Delay entre consultas
                
                # Períodos verificados
                await cursor.execute('''
                SELECT period_start, period_end, meets_requirements
                FROM checked_periods
                WHERE user_id = %s AND guild_id = %s
                ORDER BY period_start DESC
                LIMIT 5
                ''', (member.id, member.guild.id))
                period_checks = await cursor.fetchall()
        
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
        
        embed.set_footer(text=f"ID do usuário: {member.id} | Período: 30 dias")
        
        # 11. Gerar e enviar gráfico de atividade
        graph_buffer = await generate_activity_graph(member, voice_sessions)
        
        logger.info("Enviando relatório...")
        
        # Sistema de tentativas com tratamento de rate limit
        max_retries = 3
        retry_delay = 5  # segundos
        
        for attempt in range(max_retries):
            try:
                if graph_buffer:
                    graph_file = discord.File(graph_buffer, filename='atividade.png')
                    embed.set_image(url="attachment://atividade.png")
                    await interaction.followup.send(embed=embed, file=graph_file)
                else:
                    await interaction.followup.send(embed=embed)
                break  # Se enviou com sucesso, sai do loop
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = int(e.response.headers.get('Retry-After', retry_delay))
                    logger.warning(f"Rate limit (tentativa {attempt + 1}/{max_retries}). Tentando novamente em {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                raise
        else:
            logger.error("Falha após várias tentativas de enviar o relatório")
            try:
                # Tenta enviar uma mensagem mais simples
                await interaction.followup.send(
                    "📊 Relatório disponível, mas não foi possível enviar o formato completo devido a limitações do Discord.")
            except:
                pass
                
    except Exception as e:
        logger.error(f"Erro ao gerar relatório completo: {e}", exc_info=True)
        try:
            # Mensagem de erro simplificada
            await interaction.followup.send(
                "❌ Ocorreu um erro ao gerar o relatório completo.")
        except:
            pass

@bot.tree.command(name="check_user_history", description="Relatório completo de atividade do usuário")
@app_commands.checks.cooldown(1, 180.0, key=lambda i: (i.guild_id, i.user.id))  # Aumentado para 180 segundos
@allowed_roles_only()
async def check_user_history(interaction: discord.Interaction, member: discord.Member):
    """Gera um relatório completo da atividade de um usuário"""
    try:
        await interaction.response.defer(thinking=True)
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
                "❌ Ocorreu um erro ao processar sua requisição.")
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de erro: {e}")
            try:
                await interaction.edit_original_response(content="❌ Ocorreu um erro ao processar sua requisição.")
            except Exception as e:
                logger.error(f"Erro ao editar resposta original: {e}")

@check_user_history.error
async def check_user_history_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros específicos do comando check_user_history"""
    if isinstance(error, app_commands.CommandOnCooldown):
        logger.warning(f"Comando check_user_history em cooldown para {interaction.user}: {error}")
        try:
            await interaction.response.send_message(
                f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.")
        except discord.errors.HTTPException as http_error:
            logger.error(f"Erro ao enviar mensagem de cooldown: {http_error}")
    
    elif isinstance(error, discord.errors.HTTPException) and error.status == 429:
        retry_after = error.response.headers.get('Retry-After', 60)
        logger.error(f"Rate limit atingido no comando check_user_history. Tentar novamente após {retry_after} segundos")
        try:
            await asyncio.sleep(float(retry_after))
            await interaction.followup.send(
                f"⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente em {retry_after} segundos.")
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de rate limit: {e}")
    
    elif isinstance(error, Exception):
        logger.error(f"Erro não tratado em check_user_history: {error}", exc_info=True)
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro inesperado. Por favor, tente novamente mais tarde.")
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem de erro: {e}")

@bot.tree.command(name="force_check", description="Força uma verificação imediata de inatividade para um usuário")
@app_commands.checks.cooldown(1, 30.0, key=lambda i: (i.guild_id, i.user.id))  # 30 segundos de cooldown
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def force_check(interaction: discord.Interaction, member: discord.Member):
    """Força uma verificação imediata de inatividade para um usuário específico"""
    try:
        await interaction.response.defer(thinking=True)
        
        # Adiciona um pequeno delay antes de processar para evitar rate limits
        await asyncio.sleep(1)
        
        from tasks import _execute_force_check
        result = await _execute_force_check(member)
        
        if result['meets_requirements']:
            message = f"✅ {member.mention} está cumprindo os requisitos de atividade."
        else:
            message = (
                f"⚠️ {member.mention} não está cumprindo os requisitos de atividade.\n"
                f"Dias válidos: {result['valid_days']}/{result['required_days']}\n"
                f"Sessões no período: {result['sessions_count']}"
            )
        
        # Tentar enviar a resposta com tratamento de rate limit
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await interaction.followup.send(message)
                break
            except discord.errors.HTTPException as e:
                if e.status == 429 and attempt < max_retries - 1:
                    retry_after = float(e.response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limit ao enviar resposta (tentativa {attempt + 1}). Tentando novamente em {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                raise
        
        await bot.log_action(
            "Verificação Forçada",
            interaction.user,
            f"Verificação manual executada para {member.mention}\n"
            f"Resultado: {'Cumpre' if result['meets_requirements'] else 'Não cumpre'} requisitos"
        )
    except Exception as e:
        logger.error(f"Erro ao forçar verificação: {e}")
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro ao forçar a verificação. Por favor, tente novamente.")
        except discord.errors.HTTPException as e:
            if e.status == 429:
                logger.error("Rate limit ao tentar enviar mensagem de erro")

@force_check.error
async def force_check_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros específicos do comando force_check"""
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(
            f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
            ephemeral=True)
    else:
        logger.error(f"Erro no comando force_check: {error}")
        await interaction.response.send_message(
            "❌ Ocorreu um erro ao executar este comando.",
            ephemeral=True)

@bot.tree.command(name="cleanup_data", description="Limpa dados antigos do banco de dados")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def cleanup_data(interaction: discord.Interaction, days: int = 60):
    """Limpa dados antigos do banco de dados (padrão: 60 dias)"""
    try:
        if days < 7:
            await interaction.response.send_message(
                "⚠️ O período mínimo para limpeza é de 7 dias.",
                ephemeral=True)
            return
            
        await interaction.response.defer(thinking=True)
        
        result = await bot.db.cleanup_old_data(days)
        await interaction.followup.send(
            f"✅ Limpeza de dados concluída: {result}")
        await bot.log_action(
            "Limpeza de Dados Manual",
            interaction.user,
            f"Dados antigos removidos (mais de {days} dias)"
        )
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro ao limpar os dados. Por favor, tente novamente.")