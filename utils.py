# utils.py
import matplotlib.pyplot as plt
from io import BytesIO
import discord
from typing import List, Dict, Optional
import logging
import asyncio
import time
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger('inactivity_bot')

# Configurações de rate limit para geração de gráficos
GRAPH_GENERATION_LIMIT = 5  # Máximo de gráficos por minuto
last_graph_times = []

def calculate_most_active_days(sessions: List[Dict], days: int) -> List[Tuple[str, int, int]]:
    """Calcula os dias mais ativos com tempo total e média por sessão"""
    weekdays = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    day_stats = {day: {'total_min': 0, 'count': 0} for day in weekdays}
    
    for session in sessions:
        join_time = session['join_time'].replace(tzinfo=None)
        day_name = weekdays[join_time.weekday()]
        duration_min = session['duration'] // 60
        day_stats[day_name]['total_min'] += duration_min
        day_stats[day_name]['count'] += 1
    
    # Calcular média por dia e filtrar dias com atividade
    active_days = []
    for day, stats in day_stats.items():
        if stats['total_min'] > 0:
            avg = stats['total_min'] / stats['count'] if stats['count'] > 0 else 0
            active_days.append((day, stats['total_min'], int(avg)))
    
    # Ordenar por tempo total (decrescente)
    active_days.sort(key=lambda x: x[1], reverse=True)
    
    return active_days

async def generate_activity_graph(member: discord.Member, sessions: List[Dict]) -> Optional[BytesIO]:
    """Gera um gráfico de atividade do usuário e retorna como BytesIO com proteção contra rate limits"""
    if not sessions:
        return None

    # Verificar rate limit para geração de gráficos
    current_time = time.time()
    global last_graph_times
    
    # Remover registros antigos (último minuto)
    last_graph_times = [t for t in last_graph_times if current_time - t < 60]
    
    if len(last_graph_times) >= GRAPH_GENERATION_LIMIT:
        logger.warning(f"Rate limit de geração de gráficos atingido ({GRAPH_GENERATION_LIMIT}/minuto)")
        return None
    
    try:
        # Adicionar delay para evitar sobrecarga
        await asyncio.sleep(1)  # Delay base de 1 segundo
        
        # Registrar a tentativa de geração
        last_graph_times.append(current_time)
        
        # Limitar aos últimos 30 dias
        cutoff_date = datetime.now() - timedelta(days=30)
        sessions = [s for s in sessions if s['join_time'].replace(tzinfo=None) >= cutoff_date]
        
        if not sessions:
            return None

        # Preparar dados para o gráfico
        dates = []
        durations = []
        for session in sessions:
            join_time = session['join_time'].replace(tzinfo=None)
            dates.append(join_time)
            durations.append(session['duration'] / 60)  # Converter para minutos

        # Agrupar por dia da semana
        weekdays = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        weekday_durations = {day: 0 for day in weekdays}
        
        for date, duration in zip(dates, durations):
            weekday = date.weekday()
            weekday_durations[weekdays[weekday]] += duration

        # Configurar o gráfico
        plt.figure(figsize=(10, 5))
        plt.style.use('seaborn-v0_8')
        
        # Ordenar os dias da semana corretamente
        ordered_days = weekdays
        ordered_values = [weekday_durations[day] for day in ordered_days]
        
        # Gerar gráfico de barras
        bars = plt.bar(ordered_days, ordered_values, color='#5865F2', width=0.7)
        
        # Adicionar valores nas barras
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)} min',
                        ha='center', va='bottom', fontsize=9)

        # Destacar fins de semana
        for i, day in enumerate(ordered_days):
            if day in ['Sexta', 'Sábado', 'Domingo']:
                bars[i].set_color('#ED4245')  # Cor diferente para fins de semana

        # Configurar título e labels
        plt.title(f'Atividade de Voz - {member.display_name}\nÚltimos 30 dias', 
                 fontsize=12, pad=12)
        plt.xlabel('Dia da Semana', fontsize=10)
        plt.ylabel('Minutos em Voz', fontsize=10)
        
        # Ajustar eixos
        plt.xticks(fontsize=9)
        plt.yticks(fontsize=9)
        plt.ylim(0, max(ordered_values) * 1.2 if max(ordered_values) > 0 else 100)
        
        # Adicionar grid
        plt.grid(axis='y', alpha=0.4)
        
        # Ajustar layout
        plt.tight_layout()

        # Salvar em buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
        buffer.seek(0)
        
        # Fechar a figura
        plt.close()
        
        return buffer
        
    except Exception as e:
        logger.error(f"Erro ao gerar gráfico de atividade: {e}", exc_info=True)
        return None
    finally:
        if 'plt' in locals():
            plt.close('all')

async def generate_activity_report(member: discord.Member, sessions: list) -> Optional[discord.File]:
    """Gera um relatório gráfico de atividade e retorna como discord.File com tratamento de erros"""
    try:
        # Verificar rate limit antes de gerar o gráfico
        current_time = time.time()
        global last_graph_times
        last_graph_times = [t for t in last_graph_times if current_time - t < 60]
        
        if len(last_graph_times) >= GRAPH_GENERATION_LIMIT:
            logger.warning("Limite de geração de gráficos atingido, ignorando requisição")
            return None
            
        # Adicionar delay para evitar sobrecarga
        await asyncio.sleep(1.5)  # Delay maior para relatórios completos
        
        buffer = await generate_activity_graph(member, sessions)
        if buffer:
            return discord.File(buffer, filename='atividade.png')
        return None
    except Exception as e:
        logger.error(f"Erro ao gerar relatório gráfico: {e}", exc_info=True)
        return None

async def safe_send_message(destination, content=None, embed=None, file=None, max_retries=3):
    """Envia mensagem com tratamento de rate limits e retries"""
    from main import bot  # Importação local para evitar circular imports
    
    if bot.rate_limited:
        logger.warning("Ignorando envio de mensagem devido a rate limit global")
        return False
        
    for attempt in range(max_retries):
        try:
            await destination.send(content=content, embed=embed, file=file)
            return True
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = float(e.response.headers.get('Retry-After', 5))
                logger.warning(f"Rate limit ao enviar mensagem (tentativa {attempt + 1}). Tentando novamente em {retry_after} segundos")
                
                # Atualizar status global de rate limit
                bot.rate_limited = True
                bot.last_rate_limit = time.time()
                
                await asyncio.sleep(retry_after)
                bot.rate_limited = False
                continue
                
            logger.error(f"Erro HTTP ao enviar mensagem: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False
            
    logger.error(f"Falha ao enviar mensagem após {max_retries} tentativas")
    return False

def format_time_interval(seconds: int) -> str:
    """Formata um intervalo de tempo em segundos para uma string legível"""
    intervals = (
        ('dias', 86400),    # 60 * 60 * 24
        ('horas', 3600),    # 60 * 60
        ('minutos', 60),
        ('segundos', 1),
    )
    
    result = []
    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            result.append(f"{value} {name}")
    
    return ', '.join(result[:3])  # Retorna no máximo 3 unidades de tempo

async def batch_process(items, process_func, batch_size=10, delay=1.0):
    """
    Processa itens em lotes com delay entre eles para evitar rate limits
    
    Args:
        items: Lista de itens a serem processados
        process_func: Função async para processar cada item
        batch_size: Número de itens por lote
        delay: Tempo de espera entre lotes (em segundos)
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        # Processar o lote atual
        await asyncio.gather(*[process_func(item) for item in batch])
        
        # Esperar antes do próximo lote, se não for o último
        if i + batch_size < len(items):
            await asyncio.sleep(delay)

def setup_matplotlib():
    """Configura o matplotlib para uso eficiente em ambiente headless"""
    import matplotlib
    matplotlib.use('Agg')  # Usar backend que não requer display
    plt.style.use('seaborn-v0_8')  # Usar estilo mais leve
    plt.rcParams.update({
        'figure.autolayout': True,  # Ajustar layout automaticamente
        'figure.dpi': 100,          # Resolução padrão
        'savefig.dpi': 100,         # Resolução ao salvar
        'axes.titlesize': 10,       # Tamanho do título
        'axes.labelsize': 8,        # Tamanho dos labels
        'xtick.labelsize': 7,       # Tamanho dos ticks X
        'ytick.labelsize': 7,       # Tamanho dos ticks Y
        'font.size': 8,             # Tamanho geral da fonte
    })

# Configurar matplotlib na inicialização
setup_matplotlib()