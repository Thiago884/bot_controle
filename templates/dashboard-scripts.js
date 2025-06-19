// Variáveis globais para os gráficos
let activityChart, usageChart;
let refreshIntervals = [];
let currentGuildId = null;

// Função para mostrar toast (notificação)
function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toast-container');
    const toastId = 'toast-' + Date.now();
    
    const typeClasses = {
        'success': 'bg-success text-white',
        'error': 'bg-danger text-white',
        'warning': 'bg-warning text-dark',
        'info': 'bg-info text-white'
    };
    
    const toast = document.createElement('div');
    toast.className = `toast show ${typeClasses[type] || ''}`;
    toast.id = toastId;
    toast.role = 'alert';
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    toast.style.marginBottom = '10px';
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;
    
    toastContainer.appendChild(toast);
    
    // Remover o toast após 5 segundos
    setTimeout(() => {
        const toastElement = document.getElementById(toastId);
        if (toastElement) {
            toastElement.style.opacity = '0';
            setTimeout(() => toastElement.remove(), 300);
        }
    }, 5000);
}

// Função para alternar o estado de carregamento de um botão
function toggleLoading(buttonId, isLoading) {
    const button = document.getElementById(buttonId);
    if (!button) return;
    
    const spinner = button.querySelector('.loading-spinner');
    const icon = button.querySelector('i');
    
    if (isLoading) {
        spinner.style.display = 'inline-block';
        if (icon) icon.style.display = 'none';
        button.disabled = true;
    } else {
        spinner.style.display = 'none';
        if (icon) icon.style.display = 'inline-block';
        button.disabled = false;
    }
}

// Carregar configurações atuais
async function loadConfig() {
    try {
        const response = await fetch('/api/guilds');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar guildas');
        }
        
        const guilds = await response.json();
        if (guilds.length > 0) {
            const guildResponse = await fetch(`/api/guild/${guilds[0].id}`);
            if (!guildResponse.ok) {
                const errorData = await guildResponse.json();
                throw new Error(errorData.error || 'Erro ao carregar configurações');
            }
            
            const config = await guildResponse.json();
            
            document.getElementById('required_minutes').value = config.config.required_minutes;
            document.getElementById('required_days').value = config.config.required_days;
            document.getElementById('monitoring_period').value = config.config.monitoring_period;
            document.getElementById('kick_after_days').value = config.config.kick_after_days;
            
            if (config.notification_settings) {
                document.getElementById('notification_channel').value = config.notification_settings.notification_channel || '';
                document.getElementById('log_channel').value = config.notification_settings.log_channel || '';
                document.getElementById('absence_channel').value = config.notification_settings.absence_channel || '';
            }
            
            if (config.config.timezone) {
                document.getElementById('timezone').value = config.config.timezone;
            }
        }
    } catch (error) {
        console.error('Erro ao carregar configurações:', error);
        showToast('Erro ao carregar configurações: ' + error.message, 'error');
    }
}

// Carregar lista de servidores
async function loadGuilds() {
    try {
        const response = await fetch('/api/guilds');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar servidores');
        }
        
        const guilds = await response.json();
        const tbody = document.getElementById('guilds-table-body');
        
        if (guilds.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center">Nenhum servidor encontrado</td></tr>';
            return;
        }
        
        let html = '';
        guilds.forEach(guild => {
            // Verificar se a guilda existe antes de adicionar à tabela
            if (guild && guild.id) {
                html += `
                    <tr>
                        <td>
                            ${guild.icon ? 
                                `<img src="${guild.icon}" class="guild-icon" alt="${guild.name}">` : 
                                `<div class="guild-icon guild-icon-placeholder"><i class="bi bi-server"></i></div>`}
                        </td>
                        <td>${guild.name || 'Nome desconhecido'}</td>
                        <td>${guild.member_count || 0}</td>
                        <td>${guild.voice_channels?.length || 0}</td>
                        <td>
                            <button class="btn btn-sm btn-outline-primary" onclick="loadGuildDetails('${guild.id}')">
                                <i class="bi bi-eye"></i> Detalhes
                            </button>
                        </td>
                    </tr>`;
            }
        });
        
        tbody.innerHTML = html || '<tr><td colspan="5" class="text-center">Nenhum servidor válido encontrado</td></tr>';
    } catch (error) {
        console.error('Erro ao carregar servidores:', error);
        showToast('Erro ao carregar servidores: ' + error.message, 'error');
    }
}

// Carregar detalhes da guilda
async function loadGuildDetails(guildId) {
    try {
        currentGuildId = guildId;
        const modal = new bootstrap.Modal(document.getElementById('guildModal'));
        document.getElementById('guildModalTitle').textContent = 'Carregando...';
        document.getElementById('guildModalBody').innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        modal.show();
        
        const response = await fetch(`/api/guild/${guildId}`);
        if (!response.ok) {
            const errorData = await response.json();
            let errorMessage = errorData.error || 'Erro desconhecido';
            
            if (errorData.error === 'Guild not found') {
                errorMessage = 'Servidor não encontrado. Verifique se:';
                errorMessage += '<ul>';
                errorMessage += '<li>O bot ainda está no servidor</li>';
                errorMessage += '<li>O servidor está disponível</li>';
                errorMessage += '<li>Você tem permissões para visualizar</li>';
                errorMessage += '</ul>';
            }
            
            document.getElementById('guildModalBody').innerHTML = `
                <div class="alert alert-danger">
                    ${errorMessage}
                    <button class="btn btn-sm btn-outline-primary mt-2" onclick="loadGuilds()">
                        <i class="bi bi-arrow-repeat"></i> Atualizar lista de servidores
                    </button>
                </div>`;
            return;
        }
        
        const guild = await response.json();
        
        if (!guild || !guild.id) {
            throw new Error('Dados do servidor inválidos');
        }
        
        // Cabeçalho com ícone e nome do servidor
        let html = `
            <div class="guild-header">
                ${guild.icon ? 
                    `<img src="${guild.icon}" class="guild-modal-icon" alt="${guild.name}">` : 
                    `<div class="guild-modal-icon guild-icon-placeholder"><i class="bi bi-server" style="font-size: 2rem;"></i></div>`}
                <div class="guild-info">
                    <h4>${guild.name}</h4>
                    <p class="text-muted">ID: ${guild.id}</p>
                </div>
            </div>`;
        
        // Estatísticas principais em cards
        html += `
            <div class="guild-stats-grid">
                <div class="guild-stat-card">
                    <div class="guild-stat-label">Membros</div>
                    <div class="guild-stat-value" id="guild-member-count">${guild.member_count || 0}</div>
                </div>
                <div class="guild-stat-card">
                    <div class="guild-stat-label">Canais de Voz</div>
                    <div class="guild-stat-value" id="guild-voice-channels">${guild.voice_channels?.length || 0}</div>
                </div>
                <div class="guild-stat-card">
                    <div class="guild-stat-label">Ativos</div>
                    <div class="guild-stat-value text-success">${guild.activity_stats?.active_users || 0}</div>
                </div>
                <div class="guild-stat-card">
                    <div class="guild-stat-label">Inativos</div>
                    <div class="guild-stat-value text-warning">${guild.activity_stats?.inactive_users || 0}</div>
                </div>
                <div class="guild-stat-card">
                    <div class="guild-stat-label">Avisados</div>
                    <div class="guild-stat-value text-danger">${guild.activity_stats?.warned_users || 0}</div>
                </div>
            </div>`;
        
        // Configurações em seções organizadas
        html += `
            <div class="config-section">
                <h5>Configurações do Bot</h5>
                <div class="row">
                    <div class="col-md-6">
                        <ul class="list-group">
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Minutos necessários em voz
                                <span class="badge bg-primary rounded-pill config-value">${guild.config?.required_minutes || 'N/A'}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Dias diferentes necessários
                                <span class="badge bg-primary rounded-pill config-value">${guild.config?.required_days || 'N/A'}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Período de monitoramento
                                <span class="badge bg-primary rounded-pill config-value">${guild.config?.monitoring_period || 'N/A'} dias</span>
                            </li>
                        </ul>
                    </div>
                    <div class="col-md-6">
                        <ul class="list-group">
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Expulsão após dias sem cargo
                                <span class="badge bg-primary rounded-pill config-value">${guild.config?.kick_after_days || 'N/A'}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Fuso horário
                                <span class="badge bg-primary rounded-pill config-value">${guild.config?.timezone || 'N/A'}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Última verificação
                                <span class="badge bg-primary rounded-pill config-value">${guild.last_check ? new Date(guild.last_check).toLocaleString() : 'N/A'}</span>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>`;
        
        // Canais configurados
        if (guild.notification_settings) {
            html += `
                <div class="config-section">
                    <h5>Canais Configurados</h5>
                    <div class="row">
                        <div class="col-md-4">
                            <div class="card">
                                <div class="card-body">
                                    <h6 class="card-subtitle mb-2 text-muted">Notificações</h6>
                                    <p class="card-text">${guild.notification_settings.notification_channel || 'Não configurado'}</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="card">
                                <div class="card-body">
                                    <h6 class="card-subtitle mb-2 text-muted">Logs</h6>
                                    <p class="card-text">${guild.notification_settings.log_channel || 'Não configurado'}</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="card">
                                <div class="card-body">
                                    <h6 class="card-subtitle mb-2 text-muted">Ausências</h6>
                                    <p class="card-text">${guild.notification_settings.absence_channel || 'Não configurado'}</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>`;
        }
        
        // Cargos monitorados
        if (guild.tracked_roles && guild.tracked_roles.length > 0) {
            html += `
                <div class="config-section">
                    <h5>Cargos Monitorados</h5>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Nome</th>
                                    <th>Membros</th>
                                    <th>Cor</th>
                                    <th>Prioridade</th>
                                </tr>
                            </thead>
                            <tbody>`;
            
            guild.tracked_roles.forEach(role => {
                html += `
                    <tr>
                        <td>${role.name}</td>
                        <td>${role.member_count}</td>
                        <td><span class="badge" style="background-color: ${role.color}">&nbsp;&nbsp;&nbsp;</span></td>
                        <td>${role.position}</td>
                    </tr>`;
            });
            
            html += `
                            </tbody>
                        </table>
                    </div>
                </div>`;
        }
        
        // Canais de voz
        if (guild.voice_channels && guild.voice_channels.length > 0) {
            html += `
                <div class="config-section">
                    <h5>Canais de Voz</h5>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Nome</th>
                                    <th>Usuários</th>
                                    <th>Tipo</th>
                                </tr>
                            </thead>
                            <tbody>`;
            
            guild.voice_channels.forEach(channel => {
                html += `
                    <tr>
                        <td>${channel.name}</td>
                        <td>${channel.user_count}</td>
                        <td>${channel.type || 'Normal'}</td>
                    </tr>`;
            });
            
            html += `
                            </tbody>
                        </table>
                    </div>
                </div>`;
        }
        
        document.getElementById('guildModalBody').innerHTML = html;
        document.getElementById('guildModalTitle').textContent = guild.name;
    } catch (error) {
        console.error('Erro ao carregar detalhes da guilda:', error);
        document.getElementById('guildModalBody').innerHTML = `
            <div class="alert alert-danger">
                Erro ao carregar detalhes: ${error.message}
                <button class="btn btn-sm btn-outline-primary mt-2" onclick="loadGuilds()">
                    <i class="bi bi-arrow-repeat"></i> Atualizar lista de servidores
                </button>
            </div>`;
    }
}

// Carregar whitelist
async function loadWhitelist() {
    try {
        const response = await fetch('/api/whitelist');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar whitelist');
        }
        
        const data = await response.json();
        
        // Usuários
        let usersHtml = '';
        if (data.users && data.users.length > 0) {
            data.users.forEach(userId => {
                usersHtml += `
                    <div class="whitelist-item">
                        <span>${userId}</span>
                        <button class="btn btn-sm btn-danger remove-whitelist" data-type="user" data-id="${userId}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            usersHtml = '<p class="text-muted">Nenhum usuário na whitelist</p>';
        }
        document.getElementById('whitelist-users').innerHTML = usersHtml;
        
        // Cargos
        let rolesHtml = '';
        if (data.roles && data.roles.length > 0) {
            data.roles.forEach(roleId => {
                rolesHtml += `
                    <div class="whitelist-item">
                        <span>${roleId}</span>
                        <button class="btn btn-sm btn-danger remove-whitelist" data-type="role" data-id="${roleId}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            rolesHtml = '<p class="text-muted">Nenhum cargo na whitelist</p>';
        }
        document.getElementById('whitelist-roles').innerHTML = rolesHtml;
        
        // Adicionar eventos aos botões de remoção
        document.querySelectorAll('.remove-whitelist').forEach(button => {
            button.addEventListener('click', function() {
                const type = this.getAttribute('data-type');
                const id = this.getAttribute('data-id');
                updateWhitelist('remove', type, id);
            });
        });
    } catch (error) {
        console.error('Erro ao carregar whitelist:', error);
        showToast('Erro ao carregar whitelist: ' + error.message, 'error');
    }
}

// Atualizar whitelist
async function updateWhitelist(action, targetType, targetId) {
    try {
        toggleLoading(targetType === 'user' ? 'add-user-whitelist' : 'add-role-whitelist', true);
        
        const response = await fetch('/api/whitelist', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                action: action,
                type: targetType,
                id: targetId
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro desconhecido');
        }
        
        await loadWhitelist();
        
        if (targetType === 'user') {
            document.getElementById('whitelist-user-id').value = '';
        } else {
            document.getElementById('whitelist-role-id').value = '';
        }
        
        showToast(`Whitelist atualizada com sucesso! (${action} ${targetType})`, 'success');
    } catch (error) {
        console.error('Erro ao atualizar whitelist:', error);
        showToast('Erro ao atualizar whitelist: ' + error.message, 'error');
    } finally {
        toggleLoading(targetType === 'user' ? 'add-user-whitelist' : 'add-role-whitelist', false);
    }
}

// Carregar eventos recentes
async function loadRecentEvents() {
    try {
        document.getElementById('recentEvents').innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        const response = await fetch('/api/events');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar eventos');
        }
        
        const data = await response.json();
        
        let html = '<h5>Eventos Recentes</h5>';
        
        if (data.recent_events && data.recent_events.length > 0) {
            data.recent_events.forEach(event => {
                html += `
                    <div class="event-item">
                        <div><strong>${event.time}</strong> - ${event.endpoint}</div>
                        <div>Bucket: ${event.bucket} - Restantes: ${event.remaining}</div>
                        <small class="text-muted">${event.method || 'GET'}</small>
                    </div>`;
            });
        } else {
            html += '<p class="text-muted">Nenhum evento recente registrado</p>';
        }
        
        document.getElementById('recentEvents').innerHTML = html;
    } catch (error) {
        console.error('Erro ao carregar eventos recentes:', error);
        document.getElementById('recentEvents').innerHTML = `
            <div class="alert alert-danger">
                Erro ao carregar eventos: ${error.message}
            </div>`;
    }
}

// Carregar logs
async function loadLogs() {
    try {
        toggleLoading('refreshLogs', true);
        
        const lines = document.getElementById('log-lines-count').value;
        document.getElementById('logEntries').innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        const response = await fetch(`/api/logs?lines=${lines}`);
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar logs');
        }
        
        const data = await response.json();
        
        let html = '';
        if (data.logs && data.logs.length > 0) {
            data.logs.forEach(log => {
                const logClass = log.includes('ERROR') ? 'log-error' : 
                                log.includes('WARNING') ? 'log-warning' : 'log-info';
                html += `<div class="log-entry ${logClass}">${log}</div>`;
            });
        } else {
            html = '<p class="text-muted">Nenhum log disponível</p>';
        }
        
        document.getElementById('logEntries').innerHTML = html;
        document.getElementById('logEntries').scrollTop = document.getElementById('logEntries').scrollHeight;
    } catch (error) {
        console.error('Erro ao carregar logs:', error);
        document.getElementById('logEntries').innerHTML = `
            <div class="alert alert-danger">
                Erro ao carregar logs: ${error.message}
            </div>`;
    } finally {
        toggleLoading('refreshLogs', false);
    }
}

// Inicializar gráficos
function initCharts() {
    const activityCtx = document.getElementById('activityChart').getContext('2d');
    activityChart = new Chart(activityCtx, {
        type: 'line',
        data: {
            labels: ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'],
            datasets: [{
                label: 'Atividade de Voz (minutos)',
                data: [120, 190, 170, 220, 180, 150, 200],
                backgroundColor: 'rgba(54, 162, 235, 0.2)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${context.raw} minutos`;
                        }
                    }
                }
            }
        }
    });
    
    const usageCtx = document.getElementById('usageChart').getContext('2d');
    usageChart = new Chart(usageCtx, {
        type: 'bar',
        data: {
            labels: ['Usuários Ativos', 'Usuários Inativos', 'Usuários Avisados'],
            datasets: [{
                label: 'Status de Usuários',
                data: [65, 12, 8],
                backgroundColor: [
                    'rgba(75, 192, 192, 0.2)',
                    'rgba(255, 206, 86, 0.2)',
                    'rgba(255, 99, 132, 0.2)'
                ],
                borderColor: [
                    'rgba(75, 192, 192, 1)',
                    'rgba(255, 206, 86, 1)',
                    'rgba(255, 99, 132, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${context.raw}`;
                        }
                    }
                }
            }
        }
    });
}

// Atualizar gráficos com dados reais
async function updateCharts() {
    try {
        const response = await fetch('/api/activity_stats');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar estatísticas');
        }
        
        const data = await response.json();
        
        if (activityChart && usageChart) {
            // Atualizar gráfico de atividade (dados simulados)
            const newData = Array(7).fill().map(() => Math.floor(Math.random() * 200) + 50);
            activityChart.data.datasets[0].data = newData;
            activityChart.update();
            
            // Atualizar gráfico de uso
            if (data.total_users > 0) {
                usageChart.data.datasets[0].data = [
                    data.active_users,
                    data.inactive_users,
                    data.warned_users
                ];
                usageChart.update();
            }
        }
    } catch (error) {
        console.error('Erro ao atualizar gráficos:', error);
        showToast('Erro ao atualizar gráficos: ' + error.message, 'error');
    }
}

// Carregar status do sistema
async function loadSystemStatus() {
    try {
        const response = await fetch('/api/status');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar status');
        }
        
        const data = await response.json();
        
        // Atualizar nome do bot
        document.getElementById('bot-name').textContent = data.bot_name || 'Inactivity Bot';
        
        // Atualizar status do bot
        const botStatusIcon = document.getElementById('bot-status-icon');
        const botStatusText = document.getElementById('bot-status-text');
        
        if (data.bot_status === 'Operacional') {
            botStatusIcon.className = 'status-indicator status-operational';
            botStatusText.textContent = 'Operacional';
        } else {
            botStatusIcon.className = 'status-indicator status-error';
            botStatusText.textContent = 'Erro';
        }
        
        // Atualizar contagem de servidores
        document.getElementById('guild-count').textContent = data.guild_count;
        
        // Atualizar uptime
        document.getElementById('uptime').textContent = data.uptime;
        
        // Atualizar status do banco de dados
        const dbStatusBadge = document.getElementById('db-status-badge');
        if (data.db_status.includes('Operacional')) {
            dbStatusBadge.className = 'badge bg-success';
            dbStatusBadge.textContent = 'Operacional';
        } else if (data.db_status.includes('Erro')) {
            dbStatusBadge.className = 'badge bg-danger';
            dbStatusBadge.textContent = 'Erro';
        } else {
            dbStatusBadge.className = 'badge bg-warning';
            dbStatusBadge.textContent = 'Desconhecido';
        }
        
        // Atualizar status das filas
        let queueHtml = '';
        for (const [queue, size] of Object.entries(data.queue_status)) {
            let badgeClass = 'bg-secondary';
            if (size > 100) badgeClass = 'bg-danger';
            else if (size > 50) badgeClass = 'bg-warning';
            else if (size > 0) badgeClass = 'bg-primary';
            
            queueHtml += `<span class="badge ${badgeClass} badge-queue">${queue}: ${size}</span>`;
        }
        document.getElementById('queue-status').innerHTML = queueHtml;
    } catch (error) {
        console.error('Erro ao carregar status do sistema:', error);
        showToast('Erro ao carregar status do sistema: ' + error.message, 'error');
    }
}

// Salvar configurações
async function saveConfig() {
    try {
        toggleLoading('save-config', true);
        
        const config = {
            required_minutes: document.getElementById('required_minutes').value,
            required_days: document.getElementById('required_days').value,
            monitoring_period: document.getElementById('monitoring_period').value,
            kick_after_days: document.getElementById('kick_after_days').value,
            notification_channel: document.getElementById('notification_channel').value,
            log_channel: document.getElementById('log_channel').value,
            absence_channel: document.getElementById('absence_channel').value,
            timezone: document.getElementById('timezone').value
        };

        const response = await fetch('/api/update_config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao salvar configurações');
        }
        
        showToast('Configurações salvas com sucesso!', 'success');
    } catch (error) {
        console.error('Erro ao salvar configurações:', error);
        showToast('Erro ao salvar configurações: ' + error.message, 'error');
    } finally {
        toggleLoading('save-config', false);
    }
}

// Criar backup
async function createBackup() {
    try {
        toggleLoading('backup-bot', true);
        
        if (!confirm('Deseja criar um backup do banco de dados agora?')) {
            return;
        }
        
        const response = await fetch('/api/backup', {
            method: 'POST'
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao criar backup');
        }
        
        const data = await response.json();
        showToast(data.message || 'Backup criado com sucesso!', 'success');
    } catch (error) {
        console.error('Erro ao criar backup:', error);
        showToast('Erro ao criar backup: ' + error.message, 'error');
    } finally {
        toggleLoading('backup-bot', false);
    }
}

// Reiniciar bot
async function restartBot() {
    try {
        if (!confirm('Tem certeza que deseja reiniciar o bot? Isso pode causar uma breve interrupção no serviço.')) {
            return;
        }
        
        const response = await fetch('/api/restart', {
            method: 'POST'
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao reiniciar o bot');
        }
        
        const data = await response.json();
        showToast(data.message, 'success');
        
        // Mostrar mensagem de reinicialização
        setTimeout(() => {
            showToast('O bot está reiniciando...', 'info');
        }, 2000);
    } catch (error) {
        console.error('Erro ao reiniciar o bot:', error);
        showToast('Erro ao reiniciar o bot: ' + error.message, 'error');
    }
}

// Exportar relatório
function exportReport() {
    showToast('Relatório exportado com sucesso! (Funcionalidade simulada)', 'info');
}

// Funções para cargos permitidos
async function loadAllowedRoles() {
    try {
        const response = await fetch('/api/allowed_roles');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar cargos permitidos');
        }
        
        const roles = await response.json();
        let html = '';
        
        if (roles.length > 0) {
            roles.forEach(role => {
                html += `
                    <div class="whitelist-item">
                        <span>
                            <span class="badge" style="background-color: ${role.color}">&nbsp;&nbsp;&nbsp;</span>
                            ${role.name} (${role.id})
                        </span>
                        <button class="btn btn-sm btn-danger remove-allowed-role" data-id="${role.id}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            html = '<p class="text-muted">Nenhum cargo permitido definido</p>';
        }
        
        document.getElementById('allowed-roles-list').innerHTML = html;
        
        // Adicionar eventos aos botões de remoção
        document.querySelectorAll('.remove-allowed-role').forEach(button => {
            button.addEventListener('click', function() {
                const roleId = this.getAttribute('data-id');
                updateAllowedRoles('remove', roleId);
            });
        });
    } catch (error) {
        console.error('Erro ao carregar cargos permitidos:', error);
        document.getElementById('allowed-roles-list').innerHTML = `
            <div class="alert alert-danger">
                Erro ao carregar cargos permitidos: ${error.message}
            </div>`;
    }
}

async function updateAllowedRoles(action, roleId) {
    try {
        const buttonId = action === 'add' ? 'add-allowed-role' : null;
        if (buttonId) toggleLoading(buttonId, true);
        
        const response = await fetch('/api/allowed_roles', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                action: action,
                id: roleId
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro desconhecido');
        }
        
        await loadAllowedRoles();
        showToast(`Cargo ${action === 'add' ? 'adicionado' : 'removido'} com sucesso!`, 'success');
        
        if (action === 'add') {
            document.getElementById('allowed-role-id').value = '';
        }
    } catch (error) {
        console.error('Erro ao atualizar cargos permitidos:', error);
        showToast('Erro ao atualizar cargos permitidos: ' + error.message, 'error');
    } finally {
        toggleLoading('add-allowed-role', false);
    }
}

// Funções para histórico
async function loadWarningsHistory() {
    try {
        const response = await fetch('/api/warnings_history?days=30&limit=50');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar histórico de avisos');
        }
        
        const warnings = await response.json();
        let html = '';
        
        if (warnings.length > 0) {
            warnings.forEach(warning => {
                html += `
                    <tr>
                        <td>${warning.user_name || warning.user_id}</td>
                        <td>${warning.warning_type}</td>
                        <td>${new Date(warning.warning_date).toLocaleString()}</td>
                    </tr>`;
            });
        } else {
            html = '<tr><td colspan="3" class="text-center">Nenhum aviso registrado</td></tr>';
        }
        
        document.getElementById('warnings-table').innerHTML = html;
    } catch (error) {
        console.error('Erro ao carregar histórico de avisos:', error);
        document.getElementById('warnings-table').innerHTML = `
            <tr><td colspan="3" class="text-center text-danger">Erro ao carregar avisos: ${error.message}</td></tr>`;
    }
}

async function loadKicksHistory() {
    try {
        const response = await fetch('/api/kicks_history?days=30&limit=50');
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao carregar histórico de expulsões');
        }
        
        const kicks = await response.json();
        let html = '';
        
        if (kicks.length > 0) {
            kicks.forEach(kick => {
                html += `
                    <tr>
                        <td>${kick.user_name || kick.user_id}</td>
                        <td>${new Date(kick.kick_date).toLocaleString()}</td>
                        <td>${kick.reason || 'N/A'}</td>
                    </tr>`;
            });
        } else {
            html = '<tr><td colspan="3" class="text-center">Nenhuma expulsão registrada</td></tr>';
        }
        
        document.getElementById('kicks-table').innerHTML = html;
    } catch (error) {
        console.error('Erro ao carregar histórico de expulsões:', error);
        document.getElementById('kicks-table').innerHTML = `
            <tr><td colspan="3" class="text-center text-danger">Erro ao carregar expulsões: ${error.message}</td></tr>`;
    }
}

// Funções para executar comandos
async function runBotCommand(command, data = {}) {
    try {
        const response = await fetch('/api/run_command', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                command: command,
                ...data
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Erro ao executar comando');
        }
        
        const result = await response.json();
        
        if (result.status === 'success') {
            showToast(`Comando ${command} executado com sucesso!`, 'success');
            return result;
        } else {
            throw new Error(result.message || 'Erro desconhecido');
        }
    } catch (error) {
        console.error(`Erro ao executar comando ${command}:`, error);
        showToast(`Erro ao executar comando ${command}: ${error.message}`, 'error');
        return null;
    }
}

// Configurar eventos dos botões
function setupEventListeners() {
    // Whitelist
    document.getElementById('add-user-whitelist').addEventListener('click', () => {
        const userId = document.getElementById('whitelist-user-id').value;
        if (userId) {
            updateWhitelist('add', 'user', userId);
        } else {
            showToast('Por favor, insira um ID de usuário válido', 'warning');
        }
    });
    
    document.getElementById('add-role-whitelist').addEventListener('click', () => {
        const roleId = document.getElementById('whitelist-role-id').value;
        if (roleId) {
            updateWhitelist('add', 'role', roleId);
        } else {
            showToast('Por favor, insira um ID de cargo válido', 'warning');
        }
    });
    
    // Cargos permitidos
    document.getElementById('add-allowed-role').addEventListener('click', () => {
        const roleId = document.getElementById('allowed-role-id').value;
        if (roleId) {
            updateAllowedRoles('add', roleId);
        } else {
            showToast('Por favor, insira um ID de cargo válido', 'warning');
        }
    });
    
    // Logs e eventos
    document.getElementById('refreshLogs').addEventListener('click', loadLogs);
    
    // Configurações
    document.getElementById('save-config').addEventListener('click', saveConfig);
    document.getElementById('restart-bot').addEventListener('click', restartBot);
    document.getElementById('backup-bot').addEventListener('click', createBackup);
    document.getElementById('export-report').addEventListener('click', exportReport);
    
    // Comandos do bot
    document.getElementById('sync-commands-btn').addEventListener('click', () => {
        if (confirm('Deseja sincronizar os comandos do bot agora?')) {
            runBotCommand('sync_commands');
        }
    });
    
    document.getElementById('cleanup-data-btn').addEventListener('click', () => {
        if (confirm('Deseja limpar dados antigos do banco de dados agora?')) {
            const days = prompt('Dias para manter (padrão: 60):', '60');
            runBotCommand('cleanup_data', { days: parseInt(days) || 60 });
        }
    });
    
    document.getElementById('force-check-btn').addEventListener('click', () => {
        const userId = document.getElementById('force-check-user').value;
        if (userId) {
            if (confirm(`Deseja forçar verificação de inatividade para o usuário ${userId}?`)) {
                runBotCommand('force_check', { member_id: userId });
            }
        } else {
            showToast('Por favor, insira um ID de usuário válido', 'warning');
        }
    });
    
    // Carregar históricos quando as abas são clicadas
    document.getElementById('warnings-tab').addEventListener('click', loadWarningsHistory);
    document.getElementById('kicks-tab').addEventListener('click', loadKicksHistory);
    
    // Botão de atualizar detalhes da guilda
    document.getElementById('refresh-guild-details').addEventListener('click', () => {
        if (currentGuildId) {
            loadGuildDetails(currentGuildId);
        }
    });
}

// Função de inicialização do dashboard
function initializeDashboard() {
    // Carregar dados iniciais
    loadSystemStatus();
    loadGuilds();
    loadWhitelist();
    loadAllowedRoles();
    loadRecentEvents();
    loadLogs();
    loadConfig();
    loadWarningsHistory();
    initCharts();
    setupEventListeners();
    
    // Configurar atualizações periódicas
    refreshIntervals.push(setInterval(loadSystemStatus, 10000)); // 10 segundos
    refreshIntervals.push(setInterval(updateCharts, 30000)); // 30 segundos
    refreshIntervals.push(setInterval(loadRecentEvents, 15000)); // 15 segundos
    refreshIntervals.push(setInterval(loadLogs, 20000)); // 20 segundos
}

// Limpar intervalos quando a página é fechada
window.addEventListener('beforeunload', () => {
    refreshIntervals.forEach(interval => clearInterval(interval));
});