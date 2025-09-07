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
        # Verificar banco de dados
        if not await check_db_connection(interaction):
            return

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

        # Validar período
        if days < 1 or days > 30:
            await interaction.followup.send("⚠️ O período deve ser entre 1 e 30 dias.", ephemeral=True)
            return

        # Verificar banco de dados
        if not await check_db_connection(interaction):
            return

        # Usar UTC consistentemente
        end_date_param = datetime.now(pytz.utc)
        start_date_param = end_date_param - timedelta(days=days)

        try:
            async with bot.db.pool.acquire() as conn:
                # Obter sessões de voz para o período do comando (/user_activity X dias)
                voice_sessions_param = await conn.fetch(
                    "SELECT * FROM voice_sessions WHERE user_id = $1 AND guild_id = $2 AND join_time >= $3 AND leave_time <= $4",
                    member.id, member.guild.id, start_date_param, end_date_param
                )

                # Calcular tempo total apenas para o período solicitado
                total_time = sum(session['duration'] for session in voice_sessions_param) if voice_sessions_param else 0
                total_minutes = total_time / 60
                sessions_count = len(voice_sessions_param)
                avg_session_duration = total_minutes / sessions_count if sessions_count else 0

                # Calcular dias mais ativos com datas
                most_active_days = calculate_most_active_days(voice_sessions_param, days)

                # Formatar a lista de dias mais ativos com datas
                active_days_text = "Nenhum dia com atividade"
                if most_active_days:
                    active_days_text = "\n".join(
                        f"• {day_name} ({date_str}): {total} min (⌀ {avg} min/sessão)"
                        for day_name, date_str, total, avg in most_active_days[:3]  # Mostrar top 3 dias
                    )

                # Configurações de requisitos
                required_min = bot.config['required_minutes']
                required_days = bot.config['required_days']
                monitoring_period = bot.config['monitoring_period']

                # Criar embed principal
                embed = discord.Embed(
                    title=f"📊 Atividade de {member.display_name} (últimos {days} dias)",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(pytz.utc)
                )
                embed.set_thumbnail(url=member.display_avatar.url)

                # Seção de estatísticas gerais
                embed.add_field(
                    name="📈 Estatísticas Gerais",
                    value=(
                        f"**Sessões:** {sessions_count}\n"
                        f"**Tempo Total:** {int(total_minutes)} min\n"
                        f"**Duração Média:** {int(avg_session_duration)} min/sessão\n"
                        f"**Dias Mais Ativos:**\n{active_days_text}\n"
                        f"**Última Atividade:** {max(s['join_time'] for s in voice_sessions_param).strftime('%d/%m %H:%M') if voice_sessions_param else 'N/D'}"
                    ),
                    inline=True
                )

                # Seção de requisitos do servidor
                embed.add_field(
                    name="📋 Requisitos do Servidor",
                    value=(
                        f"**Minutos necessários:** {required_min} min\n"
                        f"**Dias necessários:** {required_days} dias\n"
                        f"**Período de monitoramento:** {monitoring_period} dias"
                    ),
                    inline=True
                )
                
                # --- INÍCIO DA CORREÇÃO ---
                # Seção de status atual (lógica de cálculo de período corrigida)

                last_check = await conn.fetchrow(
                    "SELECT * FROM checked_periods WHERE user_id = $1 AND guild_id = $2 ORDER BY period_end DESC LIMIT 1",
                    member.id, member.guild.id
                )

                now = datetime.now(pytz.utc)
                period_start = None
                period_end = None

                # Lógica robusta para determinar o período de análise ATUAL.
                if last_check:
                    last_period_end = last_check['period_end'].replace(tzinfo=pytz.utc)
                    # Se a data atual for anterior ao fim do último período, estamos nele.
                    if now < last_period_end:
                        period_start = last_check['period_start'].replace(tzinfo=pytz.utc)
                        period_end = last_period_end
                    # Se o último período já acabou, o período atual é o ciclo seguinte.
                    else:
                        period_start = last_period_end
                        period_end = period_start + timedelta(days=monitoring_period)
                else:
                    # Se o usuário nunca foi verificado, o período atual é uma janela retroativa
                    # para fins de exibição, alinhado ao que a task de verificação faria.
                    period_end = now
                    period_start = now - timedelta(days=monitoring_period)

                # Calcular dias válidos para o período ATUAL determinado acima
                valid_days_current_period = set()
                current_period_sessions = await conn.fetch(
                    "SELECT join_time, duration FROM voice_sessions WHERE user_id = $1 AND guild_id = $2 AND join_time >= $3 AND leave_time <= $4",
                    member.id, member.guild.id, period_start, period_end
                )
                for session in current_period_sessions:
                    if session['duration'] >= required_min * 60:
                        day = session['join_time'].replace(tzinfo=pytz.utc).date()
                        valid_days_current_period.add(day)

                is_complying = len(valid_days_current_period) >= required_days
                status_emoji = "✅" if is_complying else "⚠️"
                status_text = "Cumprindo" if is_complying else "Não cumprindo"

                days_remaining = (period_end - now).days if period_end > now else 0

                embed.add_field(
                    name="🔄 Status Atual",
                    value=(
                        f"{status_emoji} **{status_text}** os requisitos\n"
                        f"**Período:** {period_start.strftime('%d/%m/%Y')} a {period_end.strftime('%d/%m/%Y')}\n"
                        f"**Dias Restantes:** {days_remaining if days_remaining >= 0 else 'Finalizado'}"
                    ),
                    inline=True
                )

                # A barra de progresso agora usa os dados do período correto
                progress = min(1.0, len(valid_days_current_period) / required_days)
                progress_bar = "[" + "█" * int(progress * 10) + " " * (10 - int(progress * 10)) + "]"
                progress_text = f"{progress*100:.0f}% ({len(valid_days_current_period)}/{required_days} dias)"

                embed.add_field(
                    name="📊 Progresso no Período",
                    value=f"{progress_bar}\n{progress_text}",
                    inline=False
                )
                # --- FIM DA CORREÇÃO ---

                # Seção de avisos
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

                # Seção de cargos monitorados
                tracked_roles = [
                    role for role in member.roles
                    if role.id in bot.config['tracked_roles']
                ]

                if tracked_roles:
                    embed.add_field(
                        name="🎖️ Cargos Monitorados",
                        value="\n".join(role.mention for role in tracked_roles),
                        inline=True
                    )

                # Seção de data de atribuição dos cargos
                if tracked_roles:
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
                        name="📅 Data de Atribuição dos Cargos",
                        value="\n".join(assignment_info),
                        inline=True
                    )

                # Enviar resposta com tratamento de rate limit
                try:
                    # Gerar gráfico com base nos dias do parâmetro do comando
                    if voice_sessions_param:
                        try:
                            report_file = await generate_activity_report(member, voice_sessions_param, days)
                            if report_file:
                                await interaction.followup.send(embed=embed, file=report_file)
                                return
                        except Exception as e:
                            logger.error(f"Erro ao gerar gráfico: {e}")

                    await interaction.followup.send(embed=embed)

                except discord.errors.HTTPException as e:
                    if e.status == 429:
                        retry_after = float(e.response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limit ao enviar resposta. Tentando novamente em {retry_after} segundos")
                        await asyncio.sleep(retry_after)
                        try:
                            await interaction.followup.send(embed=embed)
                        except Exception as e:
                            logger.error(f"Erro ao enviar resposta após rate limit: {e}")
                    else:
                        raise

        except asyncpg.PostgresError as db_error:
            logger.error(f"Erro de banco de dados: {db_error}")
            await interaction.followup.send(
                "❌ Erro ao acessar o banco de dados. Tente novamente mais tarde.",
                ephemeral=True)
            return

    except Exception as e:
        logger.error(f"Erro no comando user_activity: {e}", exc_info=True)
        try:
            await interaction.followup.send(
                "❌ Ocorreu um erro inesperado.",
                ephemeral=True)
        except:
            pass

@bot.tree.command(name="activity_ranking", description="Mostra o ranking dos usuários mais ativos")
@app_commands.describe(
    days="Período em dias para análise (1-30)",
    limit="Número de usuários para mostrar (3-10)"
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

        if limit < 3 or limit > 10:
            await interaction.response.send_message(
                "⚠️ O limite deve ser entre 3 e 10 usuários.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        # Verificar se o banco está disponível
        if not await check_db_connection(interaction):
            return

        # Definir período de análise em UTC
        end_date = datetime.now(pytz.utc)
        start_date = end_date - timedelta(days=days)

        # Obter ranking completo do banco de dados
        async with bot.db.pool.acquire() as conn:
            results = await conn.fetch('''
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
            ''', interaction.guild.id, start_date, end_date)

        # Processar resultados
        if not results:
            embed = discord.Embed(
                title=f"🏆 Ranking de Atividade (últimos {days} dias)",
                description=f"ℹ️ Nenhuma sessão de voz registrada nos últimos {days} dias.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed)
            return

        # Pegar apenas os top N resultados
        top_results = results[:limit]

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

        # Calcular estatísticas gerais
        total_sessions = sum(row['session_count'] for row in results)
        total_time = sum(row['total_time'] for row in results)
        avg_time_per_user = total_time / len(results) if results else 0

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
                f"**Total de usuários ativos:** {len(results)}\n"
                f"**Total de sessões:** {total_sessions}\n"
                f"**Tempo total em voz:** {total_time/3600:.1f}h\n"
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


# NOVA CLASSE VIEW PARA CONFIRMAÇÃO (ATUALIZADA)
class ConfirmRestoreView(discord.ui.View):
    """View com botões de confirmação e cancelamento para a devolução de cargos."""
    def __init__(self, author: discord.User, roles_to_restore: Dict[discord.Member, List[discord.Role]], periodo_horas: int):
        super().__init__(timeout=180)  # 3 minutos
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

    @discord.ui.button(label="Confirmar Devolução", style=discord.ButtonStyle.green, custom_id="confirm_restore")
    
    async def confirm_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True, ephemeral=True)
        restored_count = 0
        error_count = 0
        log_lines = []
        forgiveness_messages_sent = 0  # <-- Contador adicionado

        # iterate over a copy to avoid mutation issues
        for member, roles in list(self.roles_to_restore.items()):
            try:
                # Add roles (respecting hierarchy will raise Forbidden if not allowed)
                start = time.time()
                await member.add_roles(*roles, reason="Reversão de remoção por inatividade via comando /devolver_cargos.")
                perf_metrics.record_api_call(time.time() - start)

                restored_count += len(roles)
                log_lines.append(f"✅ Devolvido(s) {', '.join(r.name for r in roles)} para {member}")

                # Send forgiveness messages (best-effort)
                for role in roles:
                    try:
                        await send_forgiveness_message(member, role)
                        forgiveness_messages_sent += 1  # <-- Contador incrementado
                    except Exception as e:
                        logger.warning(f"Falha ao enviar mensagem de perdão para {member}: {e}", exc_info=True) # <-- Log melhorado

                # Try to log to DB (best-effort)
                try:
                    start = time.time()
                    await bot.db.log_restored_roles(member.id, member.guild.id, [r.id for r in roles])
                    perf_metrics.record_db_query(time.time() - start)
                except Exception:
                    logger.debug("Falha ao registrar devolução no DB, ignorando.")

            except discord.Forbidden:
                error_count += len(roles)
                log_lines.append(f"❌ Falha ao devolver cargos para {member} (Permissão/Hierarquia).")
            except Exception as e:
                error_count += len(roles)
                log_lines.append(f"❌ Falha ao devolver cargos para {member} (Erro: {e}).")

        embed = discord.Embed(
            title="✅ Operação Concluída: Devolução de Cargos",
            description="A devolução de cargos foi finalizada.",
            color=discord.Color.green()
        )
        embed.add_field(
            name="Resumo da Operação",
            value=(
                f"**Membros afetados:** {len(self.roles_to_restore)}\n"
                f"**Total de cargos devolvidos:** {restored_count}\n"
                f"**Falhas:** {error_count}"
            ),
            inline=False
        )

        if log_lines:
            embed.add_field(name="Detalhes (amostra)", value="\n".join(log_lines[:10]), inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

        # Log the action to bot logs (best-effort)
        try:
            # <-- Log final melhorado para incluir o contador
            await bot.log_action(
                "Reversão de Remoção de Cargos",
                interaction.user,
                f"Executou /devolver_cargos para as últimas {self.periodo_horas} horas. "
                f"Cargos devolvidos: {restored_count}. "
                f"Mensagens de perdão enviadas: {forgiveness_messages_sent}. "
                f"Falhas: {error_count}"
            )
        except Exception as e:
            logger.debug(f"Falha ao logar ação: {e}")

        await self.disable_buttons()


    @discord.ui.button(label="Cancelar", style=discord.ButtonStyle.red, custom_id="cancel_restore")
    async def cancel_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True, ephemeral=True)
        embed = discord.Embed(
            title="❌ Operação Cancelada",
            description="A devolução de cargos foi cancelada.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.disable_buttons()


# COMANDO DEVOLVER_CARGOS ATUALIZADO
@bot.tree.command(name="devolver_cargos", description="Devolve cargos removidos por inatividade em um período recente.")
@app_commands.describe(
    periodo_horas="Período em horas para buscar remoções (padrão: 24)."
)
@allowed_roles_only()
@commands.has_permissions(administrator=True)
@app_commands.checks.cooldown(1, 300.0, key=lambda i: (i.guild_id, i.user.id))  # 5 minutes cooldown
async def devolver_cargos(interaction: discord.Interaction, periodo_horas: int = 24):
    """Devolve cargos removidos pelo bot por inatividade dentro de um período especificado."""
    await interaction.response.defer(thinking=True, ephemeral=True)

    if not await check_db_connection(interaction):
        return

    try:
        # Validate period
        if periodo_horas < 1 or periodo_horas > 168:
            await interaction.followup.send("⚠️ O período deve ser entre 1 e 168 horas (1 semana).", ephemeral=True)
            return

        guild = interaction.guild
        now = datetime.now(pytz.utc)
        start_date = now - timedelta(hours=periodo_horas)

        async with bot.db.pool.acquire() as conn:
            removals = await conn.fetch('''
                SELECT user_id, role_id, removal_date
                FROM removed_roles
                WHERE guild_id = $1 AND removal_date >= $2
                ORDER BY removal_date DESC
            ''', guild.id, start_date)

        if not removals:
            await interaction.followup.send(f"ℹ️ Nenhuma remoção de cargo encontrada nas últimas {periodo_horas} horas.", ephemeral=True)
            return

        roles_to_restore: Dict[discord.Member, List[discord.Role]] = defaultdict(list)
        member_cache: Dict[int, Optional[discord.Member]] = {}

        for record in removals:
            uid = record['user_id']
            rid = record['role_id']

            if uid not in member_cache:
                try:
                    member = guild.get_member(uid)
                    if not member:
                        try:
                            member = await guild.fetch_member(uid)
                        except discord.NotFound:
                            member = None
                    member_cache[uid] = member
                except discord.HTTPException as e:
                    logger.warning(f"Erro ao buscar membro {uid}: {e}")
                    member_cache[uid] = None
                    continue

            member = member_cache[uid]
            if not member:
                continue

            role = guild.get_role(rid)
            if not role:
                continue

            # only restore if member doesn't already have role
            if role not in member.roles:
                roles_to_restore[member].append(role)

        if not roles_to_restore:
            await interaction.followup.send("ℹ️ Todos os membros cujos cargos foram removidos já os receberam de volta ou não foram encontrados no servidor.", ephemeral=True)
            return

        # Build confirmation embed (limit content)
        embed = discord.Embed(
            title="⚠️ Confirmação de Devolução de Cargos",
            description=(
                f"Encontrei **{len(roles_to_restore)} membro(s)** para ter cargos devolvidos com base nas remoções das últimas **{periodo_horas} horas**.\n\n"
                "**Você confirma a devolução dos seguintes cargos?**"
            ),
            color=discord.Color.orange()
        )

        member_list = []
        for member, roles in list(roles_to_restore.items())[:10]:
            role_mentions = ", ".join(r.mention for r in roles[:5])
            if len(roles) > 5:
                role_mentions += f" e mais {len(roles) - 5} cargos"
            member_list.append(f"**{member.display_name}** receberá: {role_mentions}")

        description_text = "\n".join(member_list)
        if len(roles_to_restore) > 10:
            description_text += f"\n\n...e mais {len(roles_to_restore) - 10} membros."

        embed.add_field(name="Membros e Cargos a Restaurar", value=description_text, inline=False)
        embed.set_footer(text="Esta ação não pode ser desfeita. A operação irá expirar em 3 minutos.")

        view = ConfirmRestoreView(author=interaction.user, roles_to_restore=roles_to_restore, periodo_horas=periodo_horas)

        try:
            # send via followup (ephemeral)
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            # followup.send returns a Message for non-ephemeral; some gateways return None for ephemeral in older libs.
            # We'll try to get the original response as fallback.
            view.message = message if message is not None else await interaction.original_response()
        except discord.HTTPException as e:
            if getattr(e, 'status', None) == 429:
                retry_after = getattr(e, 'retry_after', 60)
                logger.warning(f"Rate limit ao enviar confirmação. Tentando novamente em {retry_after}s")
                await asyncio.sleep(retry_after)
                message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                view.message = message if message is not None else await interaction.original_response()
            else:
                logger.error(f"Erro ao enviar confirmação: {e}")
                raise

    except Exception as e:
        logger.error(f"Erro no comando /devolver_cargos: {e}", exc_info=True)
        try:
            await interaction.followup.send("❌ Ocorreu um erro inesperado ao executar este comando.", ephemeral=True)
        except Exception:
            pass


@devolver_cargos.error
async def devolver_cargos_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Trata erros do comando devolver_cargos"""
    try:
        if isinstance(error, app_commands.CommandOnCooldown):
            retry = getattr(error, 'retry_after', None)
            msg = f"⏳ Este comando está em cooldown. Tente novamente em {retry:.1f} segundos." if retry else "⏳ Este comando está em cooldown. Tente novamente mais tarde."
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        elif isinstance(error, discord.errors.HTTPException) and getattr(error, 'status', None) == 429:
            retry_after = getattr(error, 'retry_after', 60)
            msg = f"⚠️ O bot está sendo limitado pelo Discord. Por favor, tente novamente em {retry_after:.1f} segundos."
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        else:
            logger.error(f"Erro no devolver_cargos: {error}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Ocorreu um erro ao executar este comando.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Ocorreu um erro ao executar este comando.", ephemeral=True)
    except Exception as e:
        logger.error(f"Erro ao tratar erro do comando devolver_cargos: {e}")