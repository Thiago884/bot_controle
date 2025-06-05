# utils.py
import matplotlib.pyplot as plt
from io import BytesIO
import discord
from typing import List, Dict, Optional
import logging

logger = logging.getLogger('inactivity_bot')

async def generate_activity_graph(member: discord.Member, sessions: List[Dict]) -> Optional[BytesIO]:
    """Gera um gráfico de atividade do usuário e retorna como BytesIO"""
    if not sessions:
        return None

    try:
        # Preparar dados para o gráfico
        dates = []
        durations = []
        for session in sessions:
            dates.append(session['join_time'].date())
            durations.append(session['duration'] / 60)  # Converter para minutos

        # Agrupar por data
        unique_dates = sorted(list(set(dates)))
        date_to_duration = {date: 0 for date in unique_dates}
        
        for date, duration in zip(dates, durations):
            date_to_duration[date] += duration

        # Configurar o gráfico
        plt.figure(figsize=(10, 5))
        bars = plt.bar(unique_dates, [date_to_duration[d] for d in unique_dates], color='skyblue')
        
        # Adicionar valores nas barras
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}m',
                        ha='center', va='bottom')

        plt.title(f'Atividade de Voz - {member.display_name}\nPeríodo: {unique_dates[0]} a {unique_dates[-1]}')
        plt.xlabel('Data')
        plt.ylabel('Minutos em Voz')
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Salvar em buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=100)
        buffer.seek(0)
        plt.close()
        
        return buffer
    except Exception as e:
        logger.error(f"Erro ao gerar gráfico de atividade: {e}")
        return None

async def generate_activity_report(member: discord.Member, sessions: list) -> Optional[discord.File]:
    """Gera um relatório gráfico de atividade e retorna como discord.File"""
    try:
        buffer = await generate_activity_graph(member, sessions)
        if buffer:
            return discord.File(buffer, filename='atividade.png')
        return None
    except Exception as e:
        logger.error(f"Erro ao gerar relatório gráfico: {e}")
        return None