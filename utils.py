# utils.py
import matplotlib.pyplot as plt
from io import BytesIO
import discord
from typing import List, Dict, Optional
import logging
import asyncio
import time
from datetime import datetime
import numpy as np

logger = logging.getLogger('inactivity_bot')

# Configurações de rate limit para geração de gráficos
GRAPH_GENERATION_LIMIT = 5  # Máximo de gráficos por minuto
last_graph_times = []

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
        
        # Limitar a quantidade de dados processados para evitar sobrecarga
        max_sessions = 100  # Número máximo de sessões a serem plotadas
        if len(sessions) > max_sessions:
            sessions = sessions[-max_sessions:]  # Pegar as sessões mais recentes
            logger.info(f"Limitando gráfico às {max_sessions} sessões mais recentes para {member.display_name}")

        # Preparar dados para o gráfico
        dates = []
        durations = []
        for session in sessions:
            join_time = session['join_time'].replace(tzinfo=None)  # Remover timezone para evitar problemas
            dates.append(join_time)
            durations.append(session['duration'] / 60)  # Converter para minutos

        # Agrupar por data
        unique_dates = sorted(list({d.date() for d in dates}))
        date_to_duration = {date: 0 for date in unique_dates}
        
        for date, duration in zip([d.date() for d in dates], durations):
            date_to_duration[date] += duration

        # Configurar o gráfico com tamanho otimizado
        plt.figure(figsize=(10, 5))
        
        # Usar estilo mais leve
        plt.style.use('seaborn-v0_8')
        
        # Gerar o gráfico de barras
        bars = plt.bar(unique_dates, [date_to_duration[d] for d in unique_dates], 
                      color='skyblue', width=0.8)
        
        # Adicionar valores nas barras (apenas se houver espaço)
        for bar in bars:
            height = bar.get_height()
            if height > 0 and height < max(date_to_duration.values()) * 0.8:  # Só coloca texto se não sobrepor
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}m',
                        ha='center', va='bottom', fontsize=8)

        # Adicionar linhas para marcar os dias da semana
        for i, date in enumerate(unique_dates):
            if date.weekday() == 4:  # Sexta-feira
                plt.axvline(x=i, color='orange', linestyle='--', alpha=0.3, linewidth=1)
            elif date.weekday() == 5:  # Sábado
                plt.axvline(x=i, color='red', linestyle=':', alpha=0.3, linewidth=1)
            elif date.weekday() == 6:  # Domingo
                plt.axvline(x=i, color='red', linestyle='-.', alpha=0.3, linewidth=1)

        # Configurar título e labels
        plt.title(f'Atividade de Voz - {member.display_name}\nPeríodo: {unique_dates[0].strftime("%d/%m/%Y")} a {unique_dates[-1].strftime("%d/%m/%Y")}', 
                 fontsize=10, pad=10)
        plt.xlabel('Data', fontsize=9)
        plt.ylabel('Minutos em Voz', fontsize=9)
        
        # Rotacionar labels do eixo X para melhor legibilidade
        plt.xticks(rotation=45, ha='right', fontsize=8)
        plt.yticks(fontsize=8)
        
        # Adicionar legenda para os dias da semana
        weekend_patch = plt.Line2D([0], [0], color='orange', linestyle='--', label='Sexta')
        saturday_patch = plt.Line2D([0], [0], color='red', linestyle=':', label='Sábado')
        sunday_patch = plt.Line2D([0], [0], color='red', linestyle='-.', label='Domingo')
        plt.legend(handles=[weekend_patch, saturday_patch, sunday_patch], fontsize=8, loc='upper right')
        
        # Ajustar layout para evitar cortes
        plt.tight_layout()

        # Salvar em buffer com qualidade balanceada
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
        buffer.seek(0)
        
        # Fechar a figura para liberar memória
        plt.close()
        
        return buffer
        
    except Exception as e:
        logger.error(f"Erro ao gerar gráfico de atividade: {e}", exc_info=True)
        return None
    finally:
        # Garantir que a figura seja fechada mesmo em caso de erro
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