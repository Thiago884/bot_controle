# utils.py
import matplotlib.pyplot as plt
from io import BytesIO
import discord
from typing import List, Dict, Optional, Tuple
import logging
import asyncio
import time
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import pytz

logger = logging.getLogger('inactivity_bot')

# Configurações de rate limit para geração de gráficos
GRAPH_RATE_LIMIT = {
    'last_request': 0,
    'queue': deque(maxlen=5),  # Máximo 5 gráficos por minuto
    'cooldown': 15  # Segundos mínimos entre requisições
}

async def check_graph_rate_limit():
    """Verifica e aplica rate limit para geração de gráficos"""
    current_time = time.time()
    elapsed = current_time - GRAPH_RATE_LIMIT['last_request']
    
    # Se ainda estiver no cooldown, esperar
    if elapsed < GRAPH_RATE_LIMIT['cooldown']:
        wait_time = GRAPH_RATE_LIMIT['cooldown'] - elapsed
        await asyncio.sleep(wait_time)
    
    # Verificar se atingiu o limite de requisições por minuto
    if len(GRAPH_RATE_LIMIT['queue']) >= GRAPH_RATE_LIMIT['queue'].maxlen:
        oldest_request = GRAPH_RATE_LIMIT['queue'][0]
        if current_time - oldest_request < 60:
            wait_time = 60 - (current_time - oldest_request)
            logger.warning(f"Rate limit de gráficos atingido. Aguardando {wait_time:.1f} segundos")
            await asyncio.sleep(wait_time)
    
    # Registrar a nova requisição
    GRAPH_RATE_LIMIT['queue'].append(current_time)
    GRAPH_RATE_LIMIT['last_request'] = current_time
    return True

def calculate_most_active_days(sessions: List[Dict], days: int) -> List[Tuple[str, str, int, int]]:
    """Calcula os dias mais ativos com tempo total e média por sessão, incluindo as datas"""
    weekdays = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    day_stats = {}
    
    for session in sessions:
        join_time = session['join_time'].astimezone(pytz.utc)
        date_str = join_time.strftime('%d/%m/%Y')
        day_name = weekdays[join_time.weekday()]
        duration_min = session['duration'] // 60
        
        # Usamos a data como chave para agrupar por dia
        if date_str not in day_stats:
            day_stats[date_str] = {
                'day_name': day_name,
                'total_min': 0,
                'count': 0,
                'date': join_time.date()
            }
        
        day_stats[date_str]['total_min'] += duration_min
        day_stats[date_str]['count'] += 1
    
    # Converter para lista e calcular média
    active_days = []
    for date_str, stats in day_stats.items():
        avg = stats['total_min'] / stats['count'] if stats['count'] > 0 else 0
        active_days.append((
            stats['day_name'],
            date_str,
            stats['total_min'],
            int(avg)
        ))
    
    # Ordenar por data (mais recente primeiro)
    active_days.sort(key=lambda x: x[3], reverse=True)
    
    return active_days

async def generate_activity_graph(member: discord.Member, sessions: List[Dict], days: int = 14) -> Optional[BytesIO]:
    """Gera um gráfico de atividade do usuário para o período específico"""
    try:
        # Verificar e respeitar rate limits
        await check_graph_rate_limit()
        
        # Filtrar sessões para o período solicitado
        cutoff_date = datetime.now(pytz.utc) - timedelta(days=days)
        filtered_sessions = [s for s in sessions if s['join_time'].astimezone(pytz.utc) >= cutoff_date]
        
        if not filtered_sessions:
            return None

        # Preparar dados para o gráfico
        dates = []
        durations = []
        for session in filtered_sessions:
            join_time = session['join_time'].astimezone(pytz.utc)
            dates.append(join_time)
            durations.append(session['duration'] / 60)  # Converter para minutos

        # Agrupar por data
        date_durations = {}
        for date, duration in zip(dates, durations):
            date_str = date.strftime('%d/%m')
            if date_str not in date_durations:
                date_durations[date_str] = 0
            date_durations[date_str] += duration

        # Ordenar datas
        sorted_dates = sorted(date_durations.keys(), 
                            key=lambda x: datetime.strptime(x, '%d/%m'))
        
        sorted_values = [date_durations[date] for date in sorted_dates]

        # Configurar o gráfico
        plt.figure(figsize=(12, 6))
        plt.style.use('seaborn-v0_8')
        
        # Gerar gráfico de barras
        bars = plt.bar(sorted_dates, sorted_values, color='#5865F2', width=0.7)
        
        # Adicionar valores nas barras
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)} min',
                        ha='center', va='bottom', fontsize=9)

        # Configurar título e labels com o período
        start_date = (datetime.now(pytz.utc) - timedelta(days=days)).strftime('%d/%m/%Y')
        end_date = datetime.now(pytz.utc).strftime('%d/%m/%Y')
        plt.title(f'Atividade de Voz - {member.display_name}\nPeríodo: {start_date} a {end_date} ({days} dias)', 
                 fontsize=12, pad=12)
        plt.xlabel('Data', fontsize=10)
        plt.ylabel('Minutos em Voz', fontsize=10)
        
        # Rotacionar labels do eixo X para melhor legibilidade
        plt.xticks(rotation=45, fontsize=8)
        plt.yticks(fontsize=9)
        plt.ylim(0, max(sorted_values) * 1.2 if max(sorted_values) > 0 else 100)
        
        # Adicionar grid
        plt.grid(axis='y', alpha=0.4)
        
        # Ajustar layout para evitar cortes
        plt.tight_layout()

        # Salvar em buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
        buffer.seek(0)
        
        return buffer
        
    except Exception as e:
        logger.error(f"Erro ao gerar gráfico de atividade: {e}", exc_info=True)
        return None
    finally:
        if 'plt' in locals():
            plt.close('all')

async def generate_activity_report(member: discord.Member, sessions: list, days: int = 14) -> Optional[discord.File]:
    """Gera um relatório gráfico de atividade com tratamento robusto de erros"""
    try:
        # Verificar rate limit antes de gerar o gráfico
        if not await check_graph_rate_limit():
            logger.warning("Limite de geração de gráficos atingido, ignorando requisição")
            return None
            
        buffer = await generate_activity_graph(member, sessions, days)
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