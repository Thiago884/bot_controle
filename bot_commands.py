# bot_commands.py

import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional, List, Literal, Union, Dict
from datetime import datetime, timedelta
import logging
from main import bot, allowed_roles_only, send_forgiveness_message
import asyncio
from utils import generate_activity_report, calculate_most_active_days
import numpy as np
import time
from tasks import perf_metrics
import pytz
import asyncpg
from collections import defaultdict

logger = logging.getLogger('inactivity_bot')

async def check_db_connection(interaction: discord.Interaction) -> bool:
    """Verifica se a conexão com o banco de dados está disponível"""
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        if interaction.response.is_done():
            await interaction.followup.send(
                "⚠️ Banco de dados não disponível no momento.",
                ephemeral=True)
        else:
            await interaction.response.send_message(
                "⚠️ Banco de dados não disponível no momento.",
                ephemeral=True)
        return False
    return True

@bot.tree.command(name="list_allowed_roles", description="Lista os cargos com permissão para usar comandos")
@app_commands.describe(role="Selecione um cargo para ver detalhes (opcional)")
async def list_allowed_roles(
    interaction: discord.Interaction,
    role: Optional[discord.Role] = None
):
    """Lista todos os cargos que têm permissão para usar comandos do bot ou detalhes de um cargo específico"""
    try:
        logger.info(f"Comando list_allowed_roles acionado por {interaction.user}")

        if role:
            # Mostrar informações sobre um cargo específico
            is_allowed = role.id in bot.config['allowed_roles']
            embed = discord.Embed(
                title=f"ℹ️ Informações do Cargo {role.name}",
                color=role.color
            )
            embed.add_field(name="Permissão no Bot", value="✅ Permitido" if is_allowed else "❌ Não permitido")
            embed.add_field(name="ID do Cargo", value=str(role.id))
            await interaction.response.send_message(embed=embed)
            return

        # Código original para listar todos os cargos
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
            description="Ocorreu um erro ao definir o canal. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="whitelist_manage", description="Gerencia la whitelist de usuários e cargos")
@app_commands.describe(
    action="Ação a ser realizada (adicionar ou remover)",
    target_type="Tipo de alvo (usuário ou cargo)",
    target="Usuário ou cargo a ser gerenciado"
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def whitelist_manage(
    interaction: discord.Interaction,
    action: Literal["add", "remove"],
    target_type: Literal["user", "role"],
    target: Union[discord.User, discord.Role]
):
    """Gerencia a whitelist de usuários e cargos"""
    try:
        logger.info(f"Comando whitelist_manage acionado por {interaction.user} - Ação: {action} Tipo: {target_type} Alvo: {target}")

        config_key = 'users' if target_type == 'user' else 'roles'
        target_id = target.id

        if action == "add":
            if target_id not in bot.config['whitelist'][config_key]:
                bot.config['whitelist'][config_key].append(target_id)
                await bot.save_config()

                embed = discord.Embed(
                    title="✅ Whitelist Atualizada",
                    description=f"O {'usuário' if target_type == 'user' else 'cargo'} {target.mention} foi adicionado à whitelist.",
                    color=discord.Color.green()
                )

                await bot.log_action(
                    "Whitelist Atualizada",
                    interaction.user,
                    f"{'Usuário' if target_type == 'user' else 'Cargo'} adicionado: {target.name} (ID: {target.id})"
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description=f"Este {'usuário' if target_type == 'user' else 'cargo'} já está na whitelist.",
                    color=discord.Color.blue()
                )
        else:  # remove
            if target_id in bot.config['whitelist'][config_key]:
                bot.config['whitelist'][config_key].remove(target_id)
                await bot.save_config()

                embed = discord.Embed(
                    title="✅ Whitelist Atualizada",
                    description=f"O {'usuário' if target_type == 'user' else 'cargo'} {target.mention} foi removido da whitelist.",
                    color=discord.Color.green()
                )

                await bot.log_action(
                    "Whitelist Atualizada",
                    interaction.user,
                    f"{'Usuário' if target_type == 'user' else 'Cargo'} removido: {target.name} (ID: {target.id})"
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description=f"Este {'usuário' if target_type == 'user' else 'cargo'} não estava na whitelist.",
                    color=discord.Color.blue()
                )

        await interaction.response.send_message(embed=embed)
        logger.info(f"Whitelist atualizada com sucesso - Ação: {action} Tipo: {target_type} Alvo: {target}")

    except Exception as e:
        logger.error(f"Erro ao gerenciar whitelist: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar la whitelist. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="manage_tracked_roles", description="Gerencia cargos monitorados por inatividade")
@app_commands.describe(
    action="Ação a ser realizada (adicionar ou remover)",
    role="Cargo a ser gerenciado"
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def manage_tracked_roles(
    interaction: discord.Interaction,
    action: Literal["add", "remove"],
    role: discord.Role
):
    """Gerencia cargos monitorados por inatividade"""
    try:
        logger.info(f"Comando manage_tracked_roles acionado por {interaction.user} - Ação: {action} Cargo: {role.name}")

        if action == "add":
            if role.id not in bot.config['tracked_roles']:
                bot.config['tracked_roles'].append(role.id)
                await bot.save_config()

                # Registrar atribuição do cargo para todos os membros que o possuem
                members_with_role = [member for member in interaction.guild.members if role in member.roles]
                for member in members_with_role:
                    try:
                        await bot.db.log_role_assignment(member.id, interaction.guild.id, role.id)
                        logger.debug(f"Registrada atribuição do cargo {role.name} para {member.display_name}")
                    except Exception as e:
                        logger.error(f"Erro ao registrar atribuição de cargo para {member.display_name}: {e}")

                embed = discord.Embed(
                    title="✅ Cargo Monitorado Adicionado",
                    description=f"O cargo {role.mention} foi adicionado à lista de monitorados.",
                    color=discord.Color.green()
                )

                await bot.log_action(
                    "Cargo Monitorado Adicionado",
                    interaction.user,
                    f"Cargo: {role.name} (ID: {role.id})"
                )

                await bot.notify_roles(
                    f"🔔 Cargo `{role.name}` adicionado à lista de monitorados de inatividade.",
                    is_warning=False
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description="Este cargo já está sendo monitorado.",
                    color=discord.Color.blue()
                )
        else:  # remove
            if role.id in bot.config['tracked_roles']:
                bot.config['tracked_roles'].remove(role.id)
                await bot.save_config()

                embed = discord.Embed(
                    title="✅ Cargo Monitorado Removido",
                    description=f"O cargo {role.mention} foi removido da lista de monitorados.",
                    color=discord.Color.green()
                )

                await bot.log_action(
                    "Cargo Monitorado Removido",
                    interaction.user,
                    f"Cargo: {role.name} (ID: {role.id})"
                )

                await bot.notify_roles(
                    f"🔕 Cargo `{role.name}` removido da lista de monitorados de inatividade.",
                    is_warning=False
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description="Este cargo não estava sendo monitorado.",
                    color=discord.Color.blue()
                )

        await interaction.response.send_message(embed=embed)
        logger.info(f"Cargo monitorado atualizado com sucesso - Ação: {action} Cargo: {role.name}")

    except Exception as e:
        logger.error(f"Erro ao gerenciar cargos monitorados: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar os cargos monitorados. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="manage_dm_notification_roles", description="Gerencia cargos que recebem notificações por DM")
@app_commands.describe(
    action="Ação a ser realizada (adicionar ou remover)",
    role="Cargo a ser gerenciado para receber DMs"
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def manage_dm_notification_roles(
    interaction: discord.Interaction,
    action: Literal["add", "remove"],
    role: discord.Role
):
    """Gerencia quais cargos, além de administradores, devem receber DMs de notificação."""
    try:
        logger.info(f"Comando manage_dm_notification_roles por {interaction.user} - Ação: {action} Cargo: {role.name}")
        config_key = 'notification_roles_dm'

        if action == "add":
            if role.id not in bot.config[config_key]:
                bot.config[config_key].append(role.id)
                await bot.save_config()

                embed = discord.Embed(
                    title="✅ Cargos de Notificação (DM) Atualizados",
                    description=f"O cargo {role.mention} agora receberá notificações por DM.",
                    color=discord.Color.green()
                )
                await bot.log_action(
                    "Cargos de Notificação (DM) Atualizados",
                    interaction.user,
                    f"Cargo adicionado: {role.name} (ID: {role.id})"
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description=f"O cargo {role.mention} já está configurado para receber notificações por DM.",
                    color=discord.Color.blue()
                )
        else:  # remove
            if role.id in bot.config[config_key]:
                bot.config[config_key].remove(role.id)
                await bot.save_config()

                embed = discord.Embed(
                    title="✅ Cargos de Notificação (DM) Atualizados",
                    description=f"O cargo {role.mention} não receberá mais notificações por DM.",
                    color=discord.Color.green()
                )
                await bot.log_action(
                    "Cargos de Notificação (DM) Atualizados",
                    interaction.user,
                    f"Cargo removido: {role.name} (ID: {role.id})"
                )
            else:
                embed = discord.Embed(
                    title="ℹ️ Informação",
                    description=f"O cargo {role.mention} não estava na lista de notificações por DM.",
                    color=discord.Color.blue()
                )

        await interaction.response.send_message(embed=embed)

    except Exception as e:
        logger.error(f"Erro ao gerenciar cargos de notificação por DM: {e}", exc_info=True)
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao atualizar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="force_role_assignment_log", description="Força o registro de atribuição para membros sem data registrada.")
@app_commands.describe(
    role="Cargo para registrar a atribuição"
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def force_role_assignment_log(interaction: discord.Interaction, role: discord.Role):
    """Força o registro de atribuição de cargos apenas para membros que ainda não têm um registro para o cargo especificado."""
    try:
        await interaction.response.defer(thinking=True)

        if not await check_db_connection(interaction):
            return

        members_with_role = [member for member in interaction.guild.members if role in member.roles]
        total_members = len(members_with_role)

        if total_members == 0:
            await interaction.followup.send(
                f"ℹ️ Nenhum membro encontrado com o cargo {role.mention}.",
                ephemeral=True
            )
            return

        processed = 0
        errors = 0
        already_logged = 0

        async with bot.db.pool.acquire() as conn:
            async with conn.transaction():
                for member in members_with_role:
                    try:
                        # Verificar se já existe uma data de atribuição
                        existing_time = await conn.fetchval(
                            "SELECT assigned_at FROM role_assignments WHERE user_id = $1 AND guild_id = $2 AND role_id = $3",
                            member.id, interaction.guild.id, role.id
                        )

                        # Se não existir, registrar agora
                        if existing_time is None:
                            await conn.execute(
                                "INSERT INTO role_assignments (user_id, guild_id, role_id, assigned_at) VALUES ($1, $2, $3, NOW())",
                                member.id, interaction.guild.id, role.id
                            )
                            processed += 1
                        else:
                            already_logged += 1
                    except asyncpg.PostgresError as e:
                        logger.error(f"Erro de banco de dados ao processar {member.display_name}: {e}")
                        errors += 1
                    except Exception as e:
                        logger.error(f"Erro ao processar registro de atribuição de cargo para {member.display_name}: {e}")
                        errors += 1

        embed = discord.Embed(
            title="✅ Registro de Atribuição de Cargos Concluído",
            description=(
                f"Verificação para o cargo {role.mention}:\n"
                f"- **Novos registros criados:** {processed}\n"
                f"- **Membros que já possuíam registro:** {already_logged}\n"
                f"- **Total de membros com o cargo:** {total_members}\n"
                f"- **Erros:** {errors}"
            ),
            color=discord.Color.green()
        )

        await interaction.followup.send(embed=embed)

        await bot.log_action(
            "Registro Forçado de Atribuição de Cargos",
            interaction.user,
            f"Cargo: {role.name} (ID: {role.id})\nNovos registros: {processed}\nJá registrados: {already_logged}\nErros: {errors}"
        )

    except asyncpg.PostgresError as e:
        logger.error(f"Erro de banco de dados no comando force_role_assignment_log: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro no banco de dados ao processar o registro de atribuição de cargos.",
            ephemeral=True
        )
    except Exception as e:
        logger.error(f"Erro no comando force_role_assignment_log: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro ao processar o registro de atribuição de cargos.",
            ephemeral=True
        )

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
    """Define la mensagem personalizada para um tipo específico de aviso"""
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

        # Verificar placeholders necessários
        required_placeholders = {
            'first': ['{days}', '{required_minutes}', '{required_days}'],
            'second': ['{required_minutes}', '{required_days}'],
            'final': ['{guild}', '{monitoring_period}', '{required_minutes}', '{required_days}']
        }

        missing = [ph for ph in required_placeholders[warning_type] if ph not in message]
        if missing:
            embed = discord.Embed(
                title="❌ Placeholders Faltando",
                description=f"La mensagem deve conter os seguintes placeholders: {', '.join(missing)}",
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
            description="Ocorreu um erro ao atualizar la mensagem. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="show_warning_messages", description="Mostra as mensagens de aviso atuais")
@allowed_roles_only()
async def show_warning_messages(interaction: discord.Interaction):
    """Mostra as mensagens de aviso configuradas"""
    try:
        warnings_config = bot.config.get('warnings', {})
        messages = warnings_config.get('messages', {})

        embed = discord.Embed(
            title="⚠️ Mensagens de Aviso Configuradas",
            color=discord.Color.blue()
        )

        for msg_type, message in messages.items():
            # Substituir placeholders com valores atuais para visualização
            preview = message.format(
                days=warnings_config.get('first_warning', 3),
                monitoring_period=bot.config.get('monitoring_period', 14),
                required_minutes=bot.config.get('required_minutes', 15),
                required_days=bot.config.get('required_days', 2),
                guild=interaction.guild.name
            )

            embed.add_field(
                name=f"Tipo: {msg_type.capitalize()}",
                value=f"```\n{preview}\n```",
                inline=False
            )

        await interaction.response.send_message(embed=embed)
        logger.info("Mensagens de aviso exibidas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao exibir mensagens de aviso: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao exibir as mensagens de aviso.",
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
        if not await check_db_connection(interaction):
            return

        logger.info(f"Comando show_config acionado por {interaction.user}")

        config = bot.config
        
        # Helper function to get role names/mentions
        def get_role_mentions(role_ids: list) -> str:
            mentions = []
            for role_id in role_ids:
                role = interaction.guild.get_role(role_id)
                if role:
                    mentions.append(role.mention)
            return "\n".join(mentions) if mentions else "Nenhum"

        tracked_roles_mentions = get_role_mentions(config.get('tracked_roles', []))
        allowed_roles_mentions = get_role_mentions(config.get('allowed_roles', []))
        dm_notification_roles_mentions = get_role_mentions(config.get('notification_roles_dm', []))

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

        warnings_config = config.get('warnings', {})

        embed = discord.Embed(
            title="⚙️ Configuração do Bot",
            color=discord.Color.blue()
        )

        # Seção de Requisitos
        embed.add_field(
            name="📊 Requisitos de Atividade",
            value=(
                f"**Período de monitoramento:** {config['monitoring_period']} dias\n"
                f"**Minutos necessários:** {config['required_minutes']} min\n"
                f"**Dias necessários:** {config['required_days']} dias\n"
                f"**Expulsão sem cargo:** {config['kick_after_days']} dias"
            ),
            inline=False
        )

        # Seção de Canais
        notification_channel_mention = f"<#{config['notification_channel']}>" if config.get('notification_channel') else "Não definido"
        log_channel_mention = f"<#{config['log_channel']}>" if config.get('log_channel') else "Não definido"
        absence_channel_mention = f"<#{config['absence_channel']}>" if config.get('absence_channel') else "Não definido"
        
        embed.add_field(
            name="📌 Canais",
            value=(
                f"**Logs:** {log_channel_mention}\n"
                f"**Notificações:** {notification_channel_mention}\n"
                f"**Ausência (Voz):** {absence_channel_mention}"
            ),
            inline=False
        )

        # Seção de Cargos
        embed.add_field(name="Monitorados por Inatividade", value=tracked_roles_mentions, inline=True)
        embed.add_field(name="Permissão de Comandos", value=allowed_roles_mentions, inline=True)
        embed.add_field(name="Notificações por DM", value=dm_notification_roles_mentions, inline=True)
        
        # Seção de Whitelist
        whitelist_users_text = "\n".join(whitelist_users) if whitelist_users else "Nenhum"
        whitelist_roles_text = "\n".join(whitelist_roles) if whitelist_roles else "Nenhum"
        embed.add_field(
            name="🛡️ Whitelist (Ignorados pelo bot)",
            value=(
                f"**Usuários:**\n{whitelist_users_text}\n"
                f"**Cargos:**\n{whitelist_roles_text}"
            ),
            inline=False
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
        logger.error(f"Erro ao mostrar configuração: {e}", exc_info=True)
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao mostrar a configuração. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)


@bot.tree.command(name="user_activity", description="Verifica as estatísticas de atividade de um usuário")
@app_commands.describe(
    member="Membro para verificar a atividade",
    days="Período em dias para análise (padrão: 14)"
)
@allowed_roles_only()
async def user_activity(interaction: discord.Interaction, member: discord.Member, days: int = 14):
    """Verifica as estatísticas completas de atividade de um usuário"""
    try:
        await interaction.response.defer(thinking=True)

        if days < 1 or days > 30:
            await interaction.followup.send("⚠️ O período deve ser entre 1 e 30 dias.", ephemeral=True)
            return

        if not await check_db_connection(interaction):
            return

        end_date_param = datetime.now(pytz.utc)
        start_date_param = end_date_param - timedelta(days=days)

        try:
            async with bot.db.pool.acquire() as conn:
                voice_sessions_param = await conn.fetch(
                    "SELECT * FROM voice_sessions WHERE user_id = $1 AND guild_id = $2 AND join_time < $4 AND leave_time > $3 ORDER BY join_time DESC",
                    member.id, member.guild.id, start_date_param, end_date_param
                )
                
                total_time = sum(session['duration'] for session in voice_sessions_param)
                total_minutes = total_time / 60
                sessions_count = len(voice_sessions_param)
                avg_session_duration = total_minutes / sessions_count if sessions_count else 0
                most_active_days = calculate_most_active_days(voice_sessions_param, days)

                active_days_text = "Nenhum dia com atividade"
                if most_active_days:
                    active_days_text = "\n".join(
                        f"• {day_name} ({date_str}): {total} min (⌀ {avg} min/sessão)"
                        for day_name, date_str, total, avg in most_active_days[:3]
                    )

                required_min = bot.config['required_minutes']
                required_days = bot.config['required_days']
                monitoring_period = bot.config['monitoring_period']

                embed = discord.Embed(
                    title=f"📊 Atividade de {member.display_name} (últimos {days} dias)",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(pytz.utc)
                )
                embed.set_thumbnail(url=member.display_avatar.url)

                embed.add_field(
                    name="📈 Estatísticas Gerais",
                    value=(
                        f"**Sessões:** {sessions_count}\n"
                        f"**Tempo Total:** {int(total_minutes)} min\n"
                        f"**Duração Média:** {int(avg_session_duration)} min/sessão\n"
                        f"**Dias Mais Ativos:**\n{active_days_text}\n"
                        f"**Última Atividade:** {voice_sessions_param[0]['join_time'].strftime('%d/%m %H:%M') if voice_sessions_param else 'N/D'}"
                    ),
                    inline=True
                )

                embed.add_field(
                    name="📋 Requisitos do Servidor",
                    value=(
                        f"**Minutos necessários:** {required_min} min\n"
                        f"**Dias necessários:** {required_days} dias\n"
                        f"**Período de monitoramento:** {monitoring_period} dias"
                    ),
                    inline=True
                )
                
                now = datetime.now(pytz.utc)
                period_duration_days = bot.config.get('monitoring_period', 14)
                period_start = None
                anchor_date = None

                # Tenta obter a data de atribuição do cargo como âncora principal
                tracked_roles_ids = bot.config.get('tracked_roles', [])
                member_tracked_roles = [role for role in member.roles if role.id in tracked_roles_ids]
                
                if member_tracked_roles:
                    assigned_times = await asyncio.gather(*[
                        bot.db.get_role_assigned_time(member.id, member.guild.id, role.id) for role in member_tracked_roles
                    ])
                    valid_times = [t for t in assigned_times if t is not None]
                    if valid_times:
                        # A âncora é a data de atribuição MAIS RECENTE
                        anchor_date = max(valid_times)
                        if anchor_date.tzinfo is None:
                             anchor_date = anchor_date.replace(tzinfo=pytz.utc)

                # Se não encontrou data de atribuição, usa a última verificação como fallback
                if not anchor_date:
                    last_check = await conn.fetchrow(
                        "SELECT period_start FROM checked_periods WHERE user_id = $1 AND guild_id = $2 ORDER BY period_start DESC LIMIT 1",
                        member.id, member.guild.id
                    )
                    if last_check and last_check['period_start']:
                        anchor_date = last_check['period_start']
                        if anchor_date.tzinfo is None:
                            anchor_date = anchor_date.replace(tzinfo=pytz.utc)

                if anchor_date:
                    # Cálculo direto para encontrar o período atual
                    time_since_anchor = now - anchor_date
                    periods_passed = time_since_anchor.days // period_duration_days
                    
                    period_start = anchor_date + timedelta(days=periods_passed * period_duration_days)
                    period_end = period_start + timedelta(days=period_duration_days)

                    # Obter sessões de voz para o período atual calculado
                    valid_days_current_period = set()
                    current_period_sessions = await conn.fetch(
                        "SELECT join_time, duration FROM voice_sessions WHERE user_id = $1 AND guild_id = $2 AND join_time >= $3 AND join_time < $4",
                        member.id, member.guild.id, period_start, period_end
                    )
                    
                    for session in current_period_sessions:
                        # O requisito de minutos deve ser aplicado por sessão, não por dia
                        if session['duration'] >= (required_min * 60):
                            valid_days_current_period.add(session['join_time'].date())

                    is_complying = len(valid_days_current_period) >= required_days
                    status_emoji = "✅" if is_complying else "⚠️"
                    status_text = "Cumprindo" if is_complying else "Não cumprindo"
                    days_remaining = max(0, (period_end - now).days)

                    embed.add_field(
                        name="🔄 Status Atual",
                        value=(
                            f"{status_emoji} **{status_text}** os requisitos\n"
                            f"**Período:** {period_start.strftime('%d/%m/%Y')} a {period_end.strftime('%d/%m/%Y')}\n"
                            f"**Dias Restantes:** {days_remaining}"
                        ),
                        inline=True
                    )

                    progress = min(1.0, len(valid_days_current_period) / required_days) if required_days > 0 else 1.0
                    progress_bar = "[" + "█" * int(progress * 10) + " " * (10 - int(progress * 10)) + "]"
                    progress_text = f"{progress*100:.0f}% ({len(valid_days_current_period)}/{required_days} dias)"

                    embed.add_field(
                        name="📊 Progresso no Período",
                        value=f"{progress_bar}\n{progress_text}",
                        inline=False
                    )
                else:
                    # Fallback caso o membro não tenha cargos rastreados ou histórico de verificação
                     embed.add_field(
                        name="🔄 Status Atual",
                        value="Não foi possível determinar o período (sem cargos monitorados ou histórico).",
                        inline=True
                    )

                all_warnings = await conn.fetch(
                    "SELECT warning_type, warning_date FROM user_warnings WHERE user_id = $1 AND guild_id = $2 ORDER BY warning_date DESC LIMIT 3",
                    member.id, member.guild.id
                )

                if all_warnings:
                    warnings_text = "\n".join(
                        f"• {warn['warning_type'].capitalize()} - {warn['warning_date'].strftime('%d/%m/%Y %H:%M')} (UTC)"
                        for warn in all_warnings
                    )
                    embed.add_field(
                        name="⚠️ Histórico de Avisos",
                        value=warnings_text,
                        inline=False
                    )

                tracked_roles = [role for role in member.roles if role.id in bot.config['tracked_roles']]
                if tracked_roles:
                    embed.add_field(
                        name="🎖️ Cargos Monitorados",
                        value="\n".join(role.mention for role in tracked_roles),
                        inline=True
                    )
                    assignment_info = []
                    for role in tracked_roles:
                        assigned_at = await conn.fetchval(
                            "SELECT assigned_at FROM role_assignments WHERE user_id = $1 AND guild_id = $2 AND role_id = $3",
                            member.id, member.guild.id, role.id
                        )
                        if assigned_at:
                            assignment_info.append(f"• {role.mention}: {assigned_at.strftime('%d/%m/%Y')}")
                        else:
                            assignment_info.append(f"• {role.mention}: Data desconhecida")

                    embed.add_field(
                        name="📅 Data de Atribuição",
                        value="\n".join(assignment_info),
                        inline=True
                    )

                if voice_sessions_param:
                    try:
                        report_file = await generate_activity_report(member, voice_sessions_param, days)
                        if report_file:
                            await interaction.followup.send(embed=embed, file=report_file)
                            return
                    except Exception as e:
                        logger.error(f"Erro ao gerar gráfico: {e}")

                await interaction.followup.send(embed=embed)

        except asyncpg.PostgresError as db_error:
            logger.error(f"Erro de banco de dados: {db_error}")
            await interaction.followup.send(
                "❌ Erro ao acessar o banco de dados. Tente novamente mais tarde.",
                ephemeral=True)
            return

    except Exception as e:
        logger.error(f"Erro no comando user_activity: {e}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ Ocorreu um erro inesperado.", ephemeral=True)
        else:
            try:
                await interaction.followup.send("❌ Ocorreu um erro inesperado.", ephemeral=True)
            except:
                pass

@bot.tree.command(name="activity_ranking", description="Mostra o ranking dos usuários mais ativos")
@app_commands.describe(
    days="Período em dias para análise (1-30)",
    limit="Número de usuários para mostrar (3-20)"
)
@allowed_roles_only()
@app_commands.checks.cooldown(1, 60.0, key=lambda i: (i.guild_id, i.user.id))
async def activity_ranking(interaction: discord.Interaction, days: int = 7, limit: int = 5):
    """Mostra os usuários mais ativos no servidor"""
    try:
        # Validar parâmetros
        if days < 1 or days > 30:
            await interaction.response.send_message(
                "⚠️ O período deve ser entre 1 e 30 dias.", ephemeral=True)
            return

        if limit < 3 or limit > 20:
            await interaction.response.send_message(
                "⚠️ O limite deve ser entre 3 e 20 usuários.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        # Verificar se o banco está disponível
        if not await check_db_connection(interaction):
            return

        # Definir período de análise em UTC
        end_date = datetime.now(pytz.utc)
        start_date = end_date - timedelta(days=days)

        async with bot.db.pool.acquire() as conn:
            # Consulta otimizada para buscar apenas o top N de usuários
            top_results = await conn.fetch('''
                SELECT
                    user_id,
                    SUM(duration) as total_time,
                    COUNT(DISTINCT DATE(join_time)) as active_days,
                    COUNT(*) as session_count,
                    AVG(duration) as avg_duration
                FROM voice_sessions
                WHERE guild_id = $1
                AND join_time >= $2
                AND leave_time <= $3
                GROUP BY user_id
                ORDER BY total_time DESC
                LIMIT $4
            ''', interaction.guild.id, start_date, end_date, limit)

            # Consulta para obter as estatísticas gerais de todos os usuários no período
            general_stats = await conn.fetchrow('''
                SELECT
                    COUNT(DISTINCT user_id) as total_users,
                    COUNT(*) as total_sessions,
                    SUM(duration) as total_time
                FROM voice_sessions
                WHERE guild_id = $1
                AND join_time >= $2
                AND leave_time <= $3
            ''', interaction.guild.id, start_date, end_date)

        # Processar resultados
        if not top_results:
            embed = discord.Embed(
                title=f"🏆 Ranking de Atividade (últimos {days} dias)",
                description=f"ℹ️ Nenhuma sessão de voz registrada nos últimos {days} dias.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed)
            return

        # Obter informações dos membros de forma mais eficiente
        member_ids = [row['user_id'] for row in top_results]
        members = {member.id: member for member in interaction.guild.members if member.id in member_ids}

        # Construir ranking
        ranking = []
        for idx, row in enumerate(top_results, 1):
            member = members.get(row['user_id'])
            if member:
                member_name = member.display_name
            else:
                # Se o membro não foi encontrado, tentar buscar do cache ou API
                try:
                    member = await interaction.guild.fetch_member(row['user_id'])
                    member_name = member.display_name
                except:
                    member_name = f"Usuário ID: {row['user_id']}"

            total_hours = row['total_time'] / 3600
            avg_session = row['avg_duration'] / 60

            ranking.append(
                f"**{idx}.** {member_name} - "
                f"**{total_hours:.1f}h** total "
                f"({row['active_days']} dias ativos, "
                f"{row['session_count']} sessões, "
                f"⌀ {avg_session:.1f} min/sessão)"
            )

        # Calcular estatísticas gerais a partir da segunda consulta
        total_users_active = general_stats['total_users'] if general_stats else 0
        total_sessions_all = general_stats['total_sessions'] if general_stats else 0
        total_time_all = general_stats['total_time'] if general_stats else 0
        avg_time_per_user = total_time_all / total_users_active if total_users_active > 0 else 0

        # Criar embed
        embed = discord.Embed(
            title=f"🏆 Ranking de Atividade (últimos {days} dias)",
            description="\n".join(ranking),
            color=discord.Color.gold(),
            timestamp=datetime.now(pytz.utc))

        # Adicionar estatísticas gerais
        embed.add_field(
            name="📊 Estatísticas Gerais",
            value=(
                f"**Total de usuários ativos:** {total_users_active}\n"
                f"**Total de sessões:** {total_sessions_all}\n"
                f"**Tempo total em voz:** {total_time_all/3600:.1f}h\n"
                f"**Média por usuário:** {avg_time_per_user/3600:.1f}h"
            ),
            inline=False
        )

        embed.set_footer(text=f"Top {limit} usuários mais ativos | Período: {days} dias")

        try:
            await interaction.followup.send(embed=embed)
        except discord.errors.HTTPException as e:
            if e.status == 429:
                retry_after = float(e.response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit ao enviar ranking. Tentando novamente em {retry_after} segundos")
                await asyncio.sleep(retry_after)
                await interaction.followup.send(embed=embed)
            else:
                raise

    except Exception as e:
        logger.error(f"Erro ao gerar ranking de atividade: {e}")
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro ao gerar o ranking. Por favor, tente novamente mais tarde.",
                ephemeral=True)
        except:
            pass

@activity_ranking.error
async def activity_ranking_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros específicos do comando activity_ranking"""
    try:
        if isinstance(error, app_commands.CommandOnCooldown):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
                    ephemeral=True)
            else:
                await interaction.followup.send(
                    f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
                    ephemeral=True)
        elif isinstance(error, discord.errors.HTTPException) and error.status == 429:
            retry_after = float(error.response.headers.get('Retry-After', 60))
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente em {retry_after:.1f} segundos.",
                    ephemeral=True)
            else:
                await interaction.followup.send(
                    f"⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente em {retry_after:.1f} segundos.",
                    ephemeral=True)
        else:
            logger.error(f"Erro no comando activity_ranking: {error}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ Ocorreu um erro ao executar este comando.",
                    ephemeral=True)
            else:
                await interaction.followup.send(
                    "❌ Ocorreu um erro ao executar este comando.",
                    ephemeral=True)
    except Exception as e:
        logger.error(f"Erro ao tratar erro do comando activity_ranking: {e}")

@bot.tree.command(name="force_check", description="Força uma verificação imediata de inatividade para um usuário")
@app_commands.checks.cooldown(1, 30.0, key=lambda i: (i.guild_id, i.user.id))
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def force_check(interaction: discord.Interaction, member: discord.Member):
    """Força uma verificação imediata de inatividade para um usuário específico"""
    try:
        await interaction.response.defer(thinking=True)

        # Verificar se o banco está disponível
        if not await check_db_connection(interaction):
            return

        # Usar UTC para todas as datas
        now = datetime.now(pytz.utc)
        period_end = now
        period_start = now - timedelta(days=bot.config['monitoring_period'])

        from tasks import _execute_force_check
        result = await _execute_force_check(member)

        # Verificar se o usuário tem cargos monitorados
        tracked_roles = bot.config['tracked_roles']
        member_tracked_roles = [role for role in member.roles if role.id in tracked_roles]

        embed = discord.Embed(
            title=f"Verificação de Atividade - {member.display_name}",
            color=discord.Color.blue()
        )

        if result['meets_requirements']:
            embed.description = f"✅ {member.mention} está cumprindo os requisitos de atividade."
            embed.color = discord.Color.green()
            await interaction.followup.send(embed=embed)
        else:
            embed.description = (
                f"⚠️ {member.mention} não está cumprindo os requisitos de atividade.\n"
                f"**Dias válidos:** {result['valid_days']}/{result['required_days']}\n"
                f"**Sessões no período:** {result['sessions_count']}"
            )
            embed.color = discord.Color.orange()

            # Adicionar informações sobre o período
            embed.add_field(
                name="Período Analisado",
                value=(
                    f"**Início:** {result['period_start'].strftime('%d/%m/%Y')}\n"
                    f"**Término:** {result['period_end'].strftime('%d/%m/%Y')}\n"
                    f"**Dias:** {bot.config['monitoring_period']}"
                ),
                inline=True
            )

            # Adicionar informações sobre os requisitos
            embed.add_field(
                name="Requisitos do Servidor",
                value=(
                    f"**Minutos necessários:** {bot.config['required_minutes']}\n"
                    f"**Dias necessários:** {bot.config['required_days']}"
                ),
                inline=True
            )

            # Criar view com botões se o usuário tiver cargos monitorados e não cumprir requisitos
            if member_tracked_roles:
                view = discord.ui.View()
                button = discord.ui.Button(
                    style=discord.ButtonStyle.danger,
                    label="Remover Cargos Monitorados",
                    custom_id=f"remove_roles_{member.id}"
                )

                async def button_callback(interaction: discord.Interaction):
                    if not any(role.id in bot.config['allowed_roles'] for role in interaction.user.roles):
                        await interaction.response.send_message(
                            "❌ Você não tem permissão para executar esta ação.",
                            ephemeral=True)
                        return

                    try:
                        start_time = time.time()
                        await member.remove_roles(*member_tracked_roles)
                        perf_metrics.record_api_call(time.time() - start_time)

                        start_time = time.time()
                        await bot.db.log_removed_roles(member.id, member.guild.id, [r.id for r in member_tracked_roles])
                        perf_metrics.record_db_query(time.time() - start_time)

                        removed_roles = ", ".join([f"`{role.name}`" for role in member_tracked_roles])
                        await interaction.response.send_message(
                            f"✅ Cargos removidos com sucesso: {removed_roles}",
                            ephemeral=True)

                        # Atualizar a mensagem original
                        embed.color = discord.Color.red()
                        embed.description = f"🚨 Cargos removidos de {member.mention} por inatividade."
                        await interaction.message.edit(embed=embed, view=None)

                        await bot.log_action(
                            "Cargo Removido (Forçado)",
                            interaction.user,
                            f"Cargos removidos de {member.mention}: {removed_roles}"
                        )

                    except Exception as e:
                        logger.error(f"Erro ao remover cargos: {e}")
                        await interaction.response.send_message(
                            "❌ Ocorreu um erro ao remover os cargos.",
                            ephemeral=True)

                button.callback = button_callback
                view.add_item(button)
                await interaction.followup.send(embed=embed, view=view)
            else:
                await interaction.followup.send(embed=embed)

        await bot.log_action(
            "Verificação Forçada",
            interaction.user,
            f"Verificação manual executada para {member.mention}\n"
            f"Resultado: {'Cumpre' if result['meets_requirements'] else 'Não cumpre'} requisitos"
        )
    except Exception as e:
        logger.error(f"Erro no force_check: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro ao executar la verificação.",
            ephemeral=True)

@force_check.error
async def force_check_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros específicos do comando force_check"""
    try:
        if isinstance(error, app_commands.CommandOnCooldown):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
                    ephemeral=True)
            else:
                await interaction.followup.send(
                    f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos.",
                    ephemeral=True)
        else:
            logger.error(f"Erro no comando force_check: {error}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ Ocorreu um erro ao executar este comando.",
                    ephemeral=True)
            else:
                await interaction.followup.send(
                    "❌ Ocorreu um erro ao executar este comando.",
                    ephemeral=True)
    except Exception as e:
        logger.error(f"Erro ao tratar erro do comando force_check: {e}")

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

        # Verificar se o banco está disponível
        if not await check_db_connection(interaction):
            return

        async with bot.db.pool.acquire() as conn:
            async with conn.transaction():
                # Limpar sessões de voz antigas
                voice_result = await conn.execute(
                    "DELETE FROM voice_sessions WHERE leave_time < NOW() - $1::interval",
                    f"{days} days"
                )

                # Limpar verificações de período antigas
                checks_result = await conn.execute(
                    "DELETE FROM checked_periods WHERE period_end < NOW() - $1::interval",
                    f"{days} days"
                )

                # Limpar avisos antigos
                warnings_result = await conn.execute(
                    "DELETE FROM user_warnings WHERE warning_date < NOW() - $1::interval",
                    f"{days} days"
                )

        await interaction.followup.send(
            f"✅ Limpeza de dados concluída:\n"
            f"- Sessões de voz removidas: {voice_result.split()[1]}\n"
            f"- Verificações de período removidas: {checks_result.split()[1]}\n"
            f"- Avisos removidos: {warnings_result.split()[1]}"
        )

        await bot.log_action(
            "Limpeza de Dados Manual",
            interaction.user,
            f"Dados antigos removidos (mais de {days} dias)"
        )
    except asyncpg.PostgresError as e:
        logger.error(f"Erro de banco de dados ao limpar dados antigos: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro no banco de dados ao limpar os dados.",
            ephemeral=True
        )
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        await interaction.followup.send(
            "❌ Ocorreu um erro ao limpar os dados. Por favor, tente novamente.",
            ephemeral=True
        )

@bot.tree.command(name="set_log_channel", description="Define o canal para logs do bot")
@allowed_roles_only()
@commands.has_permissions(administrator=True)
async def set_log_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Define o canal onde serão enviados os logs do bot"""
    try:
        logger.info(f"Comando set_log_channel acionado por {interaction.user} para o canal {channel.name}")

        bot.config['log_channel'] = channel.id
        await bot.save_config()

        embed = discord.Embed(
            title="✅ Canal de Logs Definido",
            description=f"Canal de logs definido para {channel.mention}",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)

        await bot.log_action(
            "Canal de Logs Alterado",
            interaction.user,
            f"Novo canal: {channel.name} (ID: {channel.id})"
        )

        await channel.send("✅ Este canal foi definido como o canal de logs do bot!")
        logger.info(f"Canal de logs definido para {channel.name}")
    except Exception as e:
        logger.error(f"Erro ao definir canal de logs: {e}")
        embed = discord.Embed(
            title="❌ Erro",
            description="Ocorreu um erro ao definir o canal de logs. Por favor, tente novamente.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

# --- INÍCIO DO NOVO COMANDO INTELIGENTE DE DEVOLUÇÃO DE CARGOS ---

class SmartRestoreView(discord.ui.View):
    """
    View com botões de confirmação para a devolução de cargos.
    Esta view inicia um processo em segundo plano que fornece feedback em tempo real.
    """
    def __init__(self, author: discord.User, roles_to_restore: Dict[discord.Member, List[discord.Role]], periodo_horas: int):
        super().__init__(timeout=300)  # 5 minutos para confirmar
        self.author = author
        self.roles_to_restore = roles_to_restore
        self.periodo_horas = periodo_horas
        self.message: Optional[discord.Message] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "❌ Apenas quem executou o comando pode usar estes botões.",
                ephemeral=True
            )
            return False
        return True

    async def disable_buttons(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.NotFound, discord.HTTPException):
                pass
        self.stop()

    async def _run_restore_process(self, interaction: discord.Interaction):
        """Função que executa a devolução em segundo plano com feedback."""
        total_members = len(self.roles_to_restore)
        processed_count = 0
        restored_count = 0
        dm_sent_count = 0
        error_count = 0
        failure_logs = []

        # Processar em lotes para evitar rate limits
        batch_size = 5
        delay_between_batches = 2.5  # Segundos

        member_items = list(self.roles_to_restore.items())

        for i in range(0, total_members, batch_size):
            batch = member_items[i:i + batch_size]
            
            # Atualizar feedback de progresso para o admin
            progress_embed = discord.Embed(
                title="⏳ Devolvendo Cargos...",
                description=f"Progresso: {processed_count}/{total_members} membros processados.",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=progress_embed, view=None)

            for member, roles in batch:
                try:
                    await member.add_roles(*roles, reason=f"Reversão via /devolver_cargos por {interaction.user.name}.")
                    restored_count += len(roles)
                    
                    # Enviar uma única mensagem de perdão consolidada
                    await send_forgiveness_message(member, roles)
                    dm_sent_count += 1
                except discord.Forbidden:
                    error_count += len(roles)
                    failure_logs.append(f"🔒 {member.display_name} (Permissão/Hierarquia)")
                except discord.HTTPException as http_err:
                    error_count += len(roles)
                    failure_logs.append(f"🌐 {member.display_name} (Erro HTTP: {http_err.status})")
                except Exception as e:
                    error_count += len(roles)
                    failure_logs.append(f"❓ {member.display_name} (Erro: {type(e).__name__})")
                
                processed_count += 1
                await asyncio.sleep(0.5) # Pequeno delay entre cada membro dentro do lote

            # Delay maior entre os lotes
            if i + batch_size < total_members:
                await asyncio.sleep(delay_between_batches)

        # Criar o embed de resumo final
        summary_embed = discord.Embed(
            title="✅ Operação Concluída: Devolução de Cargos",
            description=f"A devolução de cargos para as últimas {self.periodo_horas} horas foi finalizada.",
            color=discord.Color.green() if error_count == 0 else discord.Color.orange()
        )
        summary_embed.add_field(
            name="📊 Resumo da Operação",
            value=(
                f"**Membros Processados:** {processed_count}\n"
                f"**Total de Cargos Devolvidos:** {restored_count}\n"
                f"**Mensagens de Perdão Enviadas:** {dm_sent_count}\n"
                f"**Falhas (cargos não devolvidos):** {error_count}"
            ),
            inline=False
        )

        if failure_logs:
            summary_embed.add_field(
                name="⚠️ Detalhes das Falhas",
                value="\n".join(failure_logs[:15]), # Mostra até 15 falhas
                inline=False
            )

        await interaction.edit_original_response(embed=summary_embed)

        # Logar a ação completa no canal de logs do bot
        log_details = (
            f"Executou /devolver_cargos para as últimas {self.periodo_horas} horas.\n"
            f"**Membros:** {total_members} | **Cargos Devolvidos:** {restored_count} | **Falhas:** {error_count}"
        )
        await bot.log_action(
            "Reversão de Remoção de Cargos",
            interaction.user,
            log_details
        )

    @discord.ui.button(label="Confirmar Devolução", style=discord.ButtonStyle.green)
    async def confirm_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Desativa os botões na mensagem original
        await self.disable_buttons()
        # Confirma a interação e informa que o processo começou
        await interaction.response.send_message("✅ Confirmação recebida. Iniciando processo em segundo plano...", ephemeral=True)
        # Inicia o processo de restauração, que enviará seu próprio feedback
        asyncio.create_task(self._run_restore_process(interaction))

    @discord.ui.button(label="Cancelar", style=discord.ButtonStyle.red)
    async def cancel_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.disable_buttons()
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)


@bot.tree.command(name="devolver_cargos", description="Devolve cargos removidos por inatividade em um período recente.")
@app_commands.describe(
    periodo_horas="Período em horas para buscar remoções (1-168, padrão: 24)."
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
@app_commands.checks.cooldown(1, 300.0, key=lambda i: (i.guild_id, i.user.id))
async def devolver_cargos(interaction: discord.Interaction, periodo_horas: int = 24):
    """Devolve cargos removidos pelo bot por inatividade de forma segura e inteligente."""
    await interaction.response.defer(thinking=True, ephemeral=True)

    if not await check_db_connection(interaction):
        return

    if not (1 <= periodo_horas <= 168): # 1 hora a 7 dias
        await interaction.followup.send("⚠️ O período deve ser entre 1 e 168 horas.", ephemeral=True)
        return

    guild = interaction.guild
    start_date = datetime.now(pytz.utc) - timedelta(hours=periodo_horas)

    # 1. Obter dados do banco
    async with bot.db.pool.acquire() as conn:
        removals = await conn.fetch(
            'SELECT user_id, role_id FROM removed_roles WHERE guild_id = $1 AND removal_date >= $2',
            guild.id, start_date
        )

    if not removals:
        await interaction.followup.send(f"ℹ️ Nenhuma remoção de cargo encontrada nas últimas {periodo_horas} horas.", ephemeral=True)
        return

    # 2. Resolver membros e cargos, com prevenção de rate limit
    roles_to_restore = defaultdict(list)
    unique_user_ids = {r['user_id'] for r in removals}
    
    for uid in unique_user_ids:
        try:
            member = guild.get_member(uid) or await guild.fetch_member(uid)
            if member:
                for record in removals:
                    if record['user_id'] == uid:
                        role = guild.get_role(record['role_id'])
                        if role and role not in member.roles:
                            roles_to_restore[member].append(role)
            # Pausa estratégica para não sobrecarregar a API ao buscar múltiplos membros
            await asyncio.sleep(0.7) 
        except discord.NotFound:
            logger.info(f"Membro {uid} não encontrado no servidor durante a busca para devolução de cargos.")
        except discord.HTTPException as e:
            logger.warning(f"Erro HTTP ao buscar membro {uid}: {e.status}")
            await asyncio.sleep(1.5) # Pausa maior em caso de erro HTTP

    if not roles_to_restore:
        await interaction.followup.send("ℹ️ Todos os cargos removidos já foram devolvidos ou os membros/cargos não existem mais.", ephemeral=True)
        return

    # 3. Mostrar embed de confirmação
    total_roles = sum(len(roles) for roles in roles_to_restore.values())
    embed = discord.Embed(
        title="⚠️ Confirmação de Devolução de Cargos",
        description=(
            f"Encontrei **{total_roles} cargo(s)** para devolver a **{len(roles_to_restore)} membro(s)** "
            f"com base nas remoções das últimas **{periodo_horas} horas**.\n\n"
            "O processo será executado em segundo plano com feedback de progresso."
        ),
        color=discord.Color.orange()
    )
    embed.set_footer(text="Esta ação não pode ser desfeita. A confirmação expira em 5 minutos.")

    view = SmartRestoreView(author=interaction.user, roles_to_restore=roles_to_restore, periodo_horas=periodo_horas)
    
    message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = message if message is not None else await interaction.original_response()

@devolver_cargos.error
async def devolver_cargos_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros do comando devolver_cargos"""
    if isinstance(error, app_commands.CommandOnCooldown):
        msg = f"⏳ Este comando está em cooldown. Tente novamente em {error.retry_after:.1f} segundos."
    else:
        logger.error(f"Erro inesperado em /devolver_cargos: {error}", exc_info=True)
        msg = "❌ Ocorreu um erro inesperado ao executar este comando."
    
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)

# --- FIM DO NOVO COMANDO ---