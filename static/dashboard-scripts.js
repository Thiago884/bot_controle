// Variáveis globais para os gráficos
let activityChart, usageChart;
let refreshIntervals = [];
let currentGuildId = null;

// Verificar se Chart.js está carregado
if (typeof Chart === 'undefined') {
    console.error('Chart.js não foi carregado corretamente');
    document.addEventListener('DOMContentLoaded', function() {
        showToast('Erro ao carregar biblioteca de gráficos', 'error');
    });
}

// Função para mostrar toast (notificação)
function showToast(message, type = 'info') {
    try {
        let toastContainer = document.getElementById('toast-container');
        if (!toastContainer) {
            console.warn('Toast container não encontrado, criando novo container');
            toastContainer = document.createElement('div');
            toastContainer.id = 'toast-container';
            toastContainer.style.position = 'fixed';
            toastContainer.style.top = '20px';
            toastContainer.style.right = '20px';
            toastContainer.style.zIndex = '9999';
            document.body.appendChild(toastContainer);
        }
        
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
    } catch (error) {
        console.error('Erro ao mostrar toast:', error);
    }
}

// Função para alternar o estado de carregamento de um botão
function toggleLoading(buttonId, isLoading) {
    try {
        const button = document.getElementById(buttonId);
        if (!button) {
            console.error(`Botão com ID ${buttonId} não encontrado`);
            return;
        }
        
        const spinner = button.querySelector('.loading-spinner');
        const icon = button.querySelector('i');
        
        if (isLoading) {
            if (spinner) spinner.style.display = 'inline-block';
            if (icon) icon.style.display = 'none';
            button.disabled = true;
        } else {
            if (spinner) spinner.style.display = 'none';
            if (icon) icon.style.display = 'inline-block';
            button.disabled = false;
        }
    } catch (error) {
        console.error('Erro ao alternar estado de carregamento:', error);
    }
}

// Função auxiliar para chamadas API com tratamento de erros
async function fetchWithErrorHandling(url, options = {}) {
    try {
        const response = await fetch(url, options);
        
        if (!response.ok) {
            let errorData;
            try {
                errorData = await response.json();
            } catch {
                errorData = { error: `HTTP error! status: ${response.status}` };
            }
            throw new Error(errorData.error || `Erro na requisição: ${response.status}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('Erro na requisição:', error);
        showToast('Erro ao comunicar com o servidor', 'error');
        throw error;
    }
}

// Carregar configurações atuais
async function loadConfig() {
    try {
        const guilds = await fetchWithErrorHandling('/api/guilds');
        if (!Array.isArray(guilds)) {
            throw new Error('Resposta inválida do servidor: guilds não é um array');
        }

        if (guilds.length > 0) {
            const config = await fetchWithErrorHandling(`/api/guild/${guilds[0].id}`);
            if (!config || !config.config) {
                throw new Error('Configurações inválidas retornadas pelo servidor');
            }
            
            // Atualizar campos do formulário
            const setValueIfExists = (id, value) => {
                const element = document.getElementById(id);
                if (element) element.value = value || '';
            };
            
            setValueIfExists('required_minutes', config.config.required_minutes);
            setValueIfExists('required_days', config.config.required_days);
            setValueIfExists('monitoring_period', config.config.monitoring_period);
            setValueIfExists('kick_after_days', config.config.kick_after_days);
            
            if (config.notification_settings) {
                setValueIfExists('notification_channel', config.notification_settings.notification_channel);
                setValueIfExists('log_channel', config.notification_settings.log_channel);
                setValueIfExists('absence_channel', config.notification_settings.absence_channel);
            }
            
            if (config.config.timezone) {
                setValueIfExists('timezone', config.config.timezone);
            }
        }
    } catch (error) {
        console.error('Erro ao carregar configurações:', error);
        showToast('Erro ao carregar configurações: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Carregar lista de servidores
async function loadGuilds() {
    try {
        const guilds = await fetchWithErrorHandling('/api/guilds');
        if (!Array.isArray(guilds)) {
            throw new Error('Resposta inválida do servidor: guilds não é um array');
        }

        const tbody = document.getElementById('guilds-table-body');
        if (!tbody) {
            throw new Error('Elemento guilds-table-body não encontrado');
        }
        
        if (guilds.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center">Nenhum servidor encontrado</td></tr>';
            return;
        }
        
        let html = '';
        guilds.forEach(guild => {
            if (guild && guild.id) {
                html += `
                    <tr>
                        <td>
                            ${guild.icon ? 
                                `<img src="${guild.icon}" class="guild-icon" alt="${guild.name || 'Servidor sem nome'}">` : 
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
        showToast('Erro ao carregar servidores: ' + (error.message || 'Erro desconhecido'), 'error');
        
        const tbody = document.getElementById('guilds-table-body');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center text-danger">
                        Erro ao carregar servidores
                        <button class="btn btn-sm btn-outline-primary mt-2" onclick="loadGuilds()">
                            <i class="bi bi-arrow-repeat"></i> Tentar novamente
                        </button>
                    </td>
                </tr>`;
        }
    }
}

// Carregar detalhes da guilda
async function loadGuildDetails(guildId) {
    try {
        if (!guildId) {
            throw new Error('ID da guilda não fornecido');
        }
        
        currentGuildId = guildId;
        const modal = new bootstrap.Modal(document.getElementById('guildModal'));
        const modalTitle = document.getElementById('guildModalTitle');
        const modalBody = document.getElementById('guildModalBody');
        
        if (!modal || !modalTitle || !modalBody) {
            throw new Error('Elementos do modal não encontrados');
        }
        
        modalTitle.textContent = 'Carregando...';
        modalBody.innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        modal.show();
        
        const guild = await fetchWithErrorHandling(`/api/guild/${guildId}`);
        if (!guild || !guild.id) {
            throw new Error('Dados do servidor inválidos');
        }
        
        // Cabeçalho com ícone e nome do servidor
        let html = `
            <div class="guild-header">
                ${guild.icon ? 
                    `<img src="${guild.icon}" class="guild-modal-icon" alt="${guild.name || 'Servidor sem nome'}">` : 
                    `<div class="guild-modal-icon guild-icon-placeholder"><i class="bi bi-server" style="font-size: 2rem;"></i></div>`}
                <div class="guild-info">
                    <h4>${guild.name || 'Servidor sem nome'}</h4>
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
                        <td>${role.name || 'Sem nome'}</td>
                        <td>${role.member_count || 0}</td>
                        <td><span class="badge" style="background-color: ${role.color || '#99aab5'}">&nbsp;&nbsp;&nbsp;</span></td>
                        <td>${role.position || 0}</td>
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
                        <td>${channel.name || 'Sem nome'}</td>
                        <td>${channel.user_count || 0}</td>
                        <td>${channel.type || 'Normal'}</td>
                    </tr>`;
            });
            
            html += `
                            </tbody>
                        </table>
                    </div>
                </div>`;
        }
        
        modalBody.innerHTML = html;
        modalTitle.textContent = guild.name || 'Detalhes do Servidor';
    } catch (error) {
        console.error('Erro ao carregar detalhes da guilda:', error);
        
        const modalBody = document.getElementById('guildModalBody');
        if (modalBody) {
            let errorMessage = error.message || 'Erro desconhecido';
            
            if (error.message === 'Guild not found') {
                errorMessage = 'Servidor não encontrado. Verifique se:';
                errorMessage += '<ul>';
                errorMessage += '<li>O bot ainda está no servidor</li>';
                errorMessage += '<li>O servidor está disponível</li>';
                errorMessage += '<li>Você tem permissões para visualizar</li>';
                errorMessage += '</ul>';
            }
            
            modalBody.innerHTML = `
                <div class="alert alert-danger">
                    ${errorMessage}
                    <button class="btn btn-sm btn-outline-primary mt-2" onclick="loadGuilds()">
                        <i class="bi bi-arrow-repeat"></i> Atualizar lista de servidores
                    </button>
                </div>`;
        }
    }
}

// Carregar whitelist
async function loadWhitelist() {
    try {
        const data = await fetchWithErrorHandling('/api/whitelist');
        
        // Usuários
        let usersHtml = '';
        if (data.users && data.users.length > 0) {
            data.users.forEach(userId => {
                usersHtml += `
                    <div class="whitelist-item">
                        <span>${userId || 'ID desconhecido'}</span>
                        <button class="btn btn-sm btn-danger remove-whitelist" data-type="user" data-id="${userId}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            usersHtml = '<p class="text-muted">Nenhum usuário na whitelist</p>';
        }
        
        const usersContainer = document.getElementById('whitelist-users');
        if (usersContainer) {
            usersContainer.innerHTML = usersHtml;
        }
        
        // Cargos
        let rolesHtml = '';
        if (data.roles && data.roles.length > 0) {
            data.roles.forEach(roleId => {
                rolesHtml += `
                    <div class="whitelist-item">
                        <span>${roleId || 'ID desconhecido'}</span>
                        <button class="btn btn-sm btn-danger remove-whitelist" data-type="role" data-id="${roleId}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            rolesHtml = '<p class="text-muted">Nenhum cargo na whitelist</p>';
        }
        
        const rolesContainer = document.getElementById('whitelist-roles');
        if (rolesContainer) {
            rolesContainer.innerHTML = rolesHtml;
        }
        
        // Adicionar eventos aos botões de remoção
        document.querySelectorAll('.remove-whitelist').forEach(button => {
            button.addEventListener('click', function() {
                const type = this.getAttribute('data-type');
                const id = this.getAttribute('data-id');
                if (type && id) {
                    updateWhitelist('remove', type, id);
                }
            });
        });
    } catch (error) {
        console.error('Erro ao carregar whitelist:', error);
        
        const usersContainer = document.getElementById('whitelist-users');
        const rolesContainer = document.getElementById('whitelist-roles');
        
        if (usersContainer) {
            usersContainer.innerHTML = `
                <div class="alert alert-danger">
                    Erro ao carregar usuários da whitelist: ${error.message || 'Erro desconhecido'}
                </div>`;
        }
        
        if (rolesContainer) {
            rolesContainer.innerHTML = `
                <div class="alert alert-danger">
                    Erro ao carregar cargos da whitelist: ${error.message || 'Erro desconhecido'}
                </div>`;
        }
    }
}

// Atualizar whitelist
async function updateWhitelist(action, targetType, targetId) {
    try {
        if (!action || !targetType || !targetId) {
            throw new Error('Parâmetros inválidos para atualizar whitelist');
        }
        
        toggleLoading(targetType === 'user' ? 'add-user-whitelist' : 'add-role-whitelist', true);
        
        await fetchWithErrorHandling('/api/whitelist', {
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
        
        await loadWhitelist();
        
        if (targetType === 'user') {
            const userIdInput = document.getElementById('whitelist-user-id');
            if (userIdInput) userIdInput.value = '';
        } else {
            const roleIdInput = document.getElementById('whitelist-role-id');
            if (roleIdInput) roleIdInput.value = '';
        }
        
        showToast(`Whitelist atualizada com sucesso! (${action} ${targetType})`, 'success');
    } catch (error) {
        console.error('Erro ao atualizar whitelist:', error);
        showToast('Erro ao atualizar whitelist: ' + (error.message || 'Erro desconhecido'), 'error');
    } finally {
        toggleLoading(targetType === 'user' ? 'add-user-whitelist' : 'add-role-whitelist', false);
    }
}

// Carregar eventos recentes
async function loadRecentEvents() {
    try {
        const recentEventsContainer = document.getElementById('recentEvents');
        if (!recentEventsContainer) {
            throw new Error('Container de eventos recentes não encontrado');
        }
        
        recentEventsContainer.innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        const data = await fetchWithErrorHandling('/api/events');
        
        let html = '<h5>Eventos Recentes</h5>';
        
        if (data.recent_events && data.recent_events.length > 0) {
            data.recent_events.forEach(event => {
                html += `
                    <div class="event-item">
                        <div><strong>${event.time || 'N/A'}</strong> - ${event.endpoint || 'N/A'}</div>
                        <div>Bucket: ${event.bucket || 'N/A'} - Restantes: ${event.remaining || 'N/A'}</div>
                        <small class="text-muted">${event.method || 'GET'}</small>
                    </div>`;
            });
        } else {
            html += '<p class="text-muted">Nenhum evento recente registrado</p>';
        }
        
        recentEventsContainer.innerHTML = html;
    } catch (error) {
        console.error('Erro ao carregar eventos recentes:', error);
        
        const recentEventsContainer = document.getElementById('recentEvents');
        if (recentEventsContainer) {
            recentEventsContainer.innerHTML = `
                <div class="alert alert-danger">
                    Erro ao carregar eventos: ${error.message || 'Erro desconhecido'}
                </div>`;
        }
    }
}

// Carregar logs
async function loadLogs() {
    try {
        toggleLoading('refreshLogs', true);
        
        const linesInput = document.getElementById('log-lines-count');
        const lines = linesInput ? linesInput.value : 50;
        
        const logEntriesContainer = document.getElementById('logEntries');
        if (!logEntriesContainer) {
            throw new Error('Container de logs não encontrado');
        }
        
        logEntriesContainer.innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </div>`;
        
        const data = await fetchWithErrorHandling(`/api/logs?lines=${lines}`);
        
        let html = '';
        if (data.logs && data.logs.length > 0) {
            data.logs.forEach(log => {
                const logClass = log.includes('ERROR') ? 'log-error' : 
                                log.includes('WARNING') ? 'log-warning' : 'log-info';
                html += `<div class="log-entry ${logClass}">${log || 'Entrada de log vazia'}</div>`;
            });
        } else {
            html = '<p class="text-muted">Nenhum log disponível</p>';
        }
        
        logEntriesContainer.innerHTML = html;
        logEntriesContainer.scrollTop = logEntriesContainer.scrollHeight;
    } catch (error) {
        console.error('Erro ao carregar logs:', error);
        
        const logEntriesContainer = document.getElementById('logEntries');
        if (logEntriesContainer) {
            logEntriesContainer.innerHTML = `
                <div class="alert alert-danger">
                    Erro ao carregar logs: ${error.message || 'Erro desconhecido'}
                </div>`;
        }
    } finally {
        toggleLoading('refreshLogs', false);
    }
}

// Inicializar gráficos
function initCharts() {
    try {
        const activityCtx = document.getElementById('activityChart')?.getContext('2d');
        if (!activityCtx) {
            throw new Error('Elemento activityChart não encontrado');
        }
        
        // Destruir gráficos existentes antes de criar novos
        if (activityChart) {
            activityChart.destroy();
        }
        if (usageChart) {
            usageChart.destroy();
        }
        
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
        
        const usageCtx = document.getElementById('usageChart')?.getContext('2d');
        if (!usageCtx) {
            throw new Error('Elemento usageChart não encontrado');
        }
        
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
    } catch (error) {
        console.error('Erro ao inicializar gráficos:', error);
        showToast('Erro ao inicializar gráficos: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Atualizar gráficos com dados reais
async function updateCharts() {
    try {
        const data = await fetchWithErrorHandling('/api/activity_stats');
        
        if (!activityChart || !usageChart) {
            initCharts(); // Re-inicializa se os gráficos não existirem
            return;
        }
        
        // Atualizar gráfico de atividade (dados simulados)
        const newData = Array(7).fill().map(() => Math.floor(Math.random() * 200) + 50);
        activityChart.data.datasets[0].data = newData;
        activityChart.update();
        
        // Atualizar gráfico de uso
        if (data.total_users > 0) {
            usageChart.data.datasets[0].data = [
                data.active_users || 0,
                data.inactive_users || 0,
                data.warned_users || 0
            ];
            usageChart.update();
        }
    } catch (error) {
        console.error('Erro ao atualizar gráficos:', error);
        showToast('Erro ao atualizar gráficos: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Carregar status do sistema
async function loadSystemStatus() {
    try {
        const data = await fetchWithErrorHandling('/api/status');
        
        // Atualizar nome do bot
        const botNameElement = document.getElementById('bot-name');
        if (botNameElement) {
            botNameElement.textContent = data.bot_name || 'Inactivity Bot';
        }
        
        // Atualizar status do bot
        const botStatusIcon = document.getElementById('bot-status-icon');
        const botStatusText = document.getElementById('bot-status-text');
        
        if (botStatusIcon && botStatusText) {
            if (data.bot_status === 'Operacional') {
                botStatusIcon.className = 'status-indicator status-operational';
                botStatusText.textContent = 'Operacional';
            } else {
                botStatusIcon.className = 'status-indicator status-error';
                botStatusText.textContent = 'Erro';
            }
        }
        
        // Atualizar contagem de servidores
        const guildCountElement = document.getElementById('guild-count');
        if (guildCountElement) {
            guildCountElement.textContent = data.guild_count || 0;
        }
        
        // Atualizar uptime
        const uptimeElement = document.getElementById('uptime');
        if (uptimeElement) {
            uptimeElement.textContent = data.uptime || 'N/A';
        }
        
        // Atualizar status do banco de dados
        const dbStatusBadge = document.getElementById('db-status-badge');
        if (dbStatusBadge) {
            if (data.db_status?.includes('Operacional')) {
                dbStatusBadge.className = 'badge bg-success';
                dbStatusBadge.textContent = 'Operacional';
            } else if (data.db_status?.includes('Erro')) {
                dbStatusBadge.className = 'badge bg-danger';
                dbStatusBadge.textContent = 'Erro';
            } else {
                dbStatusBadge.className = 'badge bg-warning';
                dbStatusBadge.textContent = 'Desconhecido';
            }
        }
        
        // Atualizar status das filas
        const queueStatusContainer = document.getElementById('queue-status');
        if (queueStatusContainer && data.queue_status) {
            let queueHtml = '';
            for (const [queue, size] of Object.entries(data.queue_status)) {
                let badgeClass = 'bg-secondary';
                if (size > 100) badgeClass = 'bg-danger';
                else if (size > 50) badgeClass = 'bg-warning';
                else if (size > 0) badgeClass = 'bg-primary';
                
                queueHtml += `<span class="badge ${badgeClass} badge-queue">${queue}: ${size}</span>`;
            }
            queueStatusContainer.innerHTML = queueHtml;
        }
    } catch (error) {
        console.error('Erro ao carregar status do sistema:', error);
        
        const botStatusContainer = document.getElementById('bot-status');
        if (botStatusContainer) {
            botStatusContainer.innerHTML = `
                <span class="status-indicator status-error"></span>
                <span>Erro ao carregar</span>
            `;
        }
        
        showToast('Erro ao carregar status do sistema: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Salvar configurações
async function saveConfig() {
    try {
        toggleLoading('save-config', true);
        
        const config = {
            required_minutes: document.getElementById('required_minutes')?.value || '',
            required_days: document.getElementById('required_days')?.value || '',
            monitoring_period: document.getElementById('monitoring_period')?.value || '',
            kick_after_days: document.getElementById('kick_after_days')?.value || '',
            notification_channel: document.getElementById('notification_channel')?.value || '',
            log_channel: document.getElementById('log_channel')?.value || '',
            absence_channel: document.getElementById('absence_channel')?.value || '',
            timezone: document.getElementById('timezone')?.value || ''
        };

        await fetchWithErrorHandling('/api/update_config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(config)
        });
        
        showToast('Configurações salvas com sucesso!', 'success');
    } catch (error) {
        console.error('Erro ao salvar configurações:', error);
        showToast('Erro ao salvar configurações: ' + (error.message || 'Erro desconhecido'), 'error');
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
        
        const data = await fetchWithErrorHandling('/api/backup', {
            method: 'POST'
        });
        
        showToast(data.message || 'Backup criado com sucesso!', 'success');
    } catch (error) {
        console.error('Erro ao criar backup:', error);
        showToast('Erro ao criar backup: ' + (error.message || 'Erro desconhecido'), 'error');
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
        
        const data = await fetchWithErrorHandling('/api/restart', {
            method: 'POST'
        });
        
        showToast(data.message || 'Reinicialização iniciada', 'success');
        
        // Mostrar mensagem de reinicialização
        setTimeout(() => {
            showToast('O bot está reiniciando...', 'info');
        }, 2000);
    } catch (error) {
        console.error('Erro ao reiniciar o bot:', error);
        showToast('Erro ao reiniciar o bot: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Exportar relatório
function exportReport() {
    try {
        showToast('Relatório exportado com sucesso! (Funcionalidade simulada)', 'info');
    } catch (error) {
        console.error('Erro ao exportar relatório:', error);
        showToast('Erro ao exportar relatório: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Funções para cargos permitidos
async function loadAllowedRoles() {
    try {
        const roles = await fetchWithErrorHandling('/api/allowed_roles');
        
        let html = '';
        
        if (roles.length > 0) {
            roles.forEach(role => {
                html += `
                    <div class="whitelist-item">
                        <span>
                            <span class="badge" style="background-color: ${role.color || '#99aab5'}">&nbsp;&nbsp;&nbsp;</span>
                            ${role.name || 'Sem nome'} (${role.id || 'ID desconhecido'})
                        </span>
                        <button class="btn btn-sm btn-danger remove-allowed-role" data-id="${role.id}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>`;
            });
        } else {
            html = '<p class="text-muted">Nenhum cargo permitido definido</p>';
        }
        
        const allowedRolesContainer = document.getElementById('allowed-roles-list');
        if (allowedRolesContainer) {
            allowedRolesContainer.innerHTML = html;
        }
        
        // Adicionar eventos aos botões de remoção
        document.querySelectorAll('.remove-allowed-role').forEach(button => {
            button.addEventListener('click', function() {
                const roleId = this.getAttribute('data-id');
                if (roleId) {
                    updateAllowedRoles('remove', roleId);
                }
            });
        });
    } catch (error) {
        console.error('Erro ao carregar cargos permitidos:', error);
        
        const allowedRolesContainer = document.getElementById('allowed-roles-list');
        if (allowedRolesContainer) {
            allowedRolesContainer.innerHTML = `
                <div class="alert alert-danger">
                    Erro ao carregar cargos permitidos: ${error.message || 'Erro desconhecido'}
                </div>`;
        }
    }
}

async function updateAllowedRoles(action, roleId) {
    try {
        if (!action || !roleId) {
            throw new Error('Parâmetros inválidos para atualizar cargos permitidos');
        }
        
        const buttonId = action === 'add' ? 'add-allowed-role' : null;
        if (buttonId) toggleLoading(buttonId, true);
        
        await fetchWithErrorHandling('/api/allowed_roles', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                action: action,
                id: roleId
            })
        });
        
        await loadAllowedRoles();
        showToast(`Cargo ${action === 'add' ? 'adicionado' : 'removido'} com sucesso!`, 'success');
        
        if (action === 'add') {
            const roleIdInput = document.getElementById('allowed-role-id');
            if (roleIdInput) roleIdInput.value = '';
        }
    } catch (error) {
        console.error('Erro ao atualizar cargos permitidos:', error);
        showToast('Erro ao atualizar cargos permitidos: ' + (error.message || 'Erro desconhecido'), 'error');
    } finally {
        toggleLoading('add-allowed-role', false);
    }
}

// Funções para histórico
async function loadWarningsHistory() {
    try {
        const warnings = await fetchWithErrorHandling('/api/warnings_history?days=30&limit=50');
        
        let html = '';
        if (warnings.length > 0) {
            warnings.forEach(warning => {
                html += `
                    <tr>
                        <td>${warning.user_name || warning.user_id || 'ID desconhecido'}</td>
                        <td>${warning.warning_type || 'N/A'}</td>
                        <td>${warning.warning_date ? new Date(warning.warning_date).toLocaleString() : 'N/A'}</td>
                    </tr>`;
            });
        } else {
            html = '<tr><td colspan="3" class="text-center">Nenhum aviso registrado</td></tr>';
        }
        
        const warningsTable = document.getElementById('warnings-table');
        if (warningsTable) {
            warningsTable.innerHTML = html;
        }
    } catch (error) {
        console.error('Erro ao carregar histórico de avisos:', error);
        
        const warningsTable = document.getElementById('warnings-table');
        if (warningsTable) {
            warningsTable.innerHTML = `
                <tr><td colspan="3" class="text-center text-danger">Erro ao carregar avisos: ${error.message || 'Erro desconhecido'}</td></tr>`;
        }
    }
}

async function loadKicksHistory() {
    try {
        const kicks = await fetchWithErrorHandling('/api/kicks_history?days=30&limit=50');
        
        let html = '';
        if (kicks.length > 0) {
            kicks.forEach(kick => {
                html += `
                    <tr>
                        <td>${kick.user_name || kick.user_id || 'ID desconhecido'}</td>
                        <td>${kick.kick_date ? new Date(kick.kick_date).toLocaleString() : 'N/A'}</td>
                        <td>${kick.reason || 'N/A'}</td>
                    </tr>`;
            });
        } else {
            html = '<tr><td colspan="3" class="text-center">Nenhuma expulsão registrada</td></tr>';
        }
        
        const kicksTable = document.getElementById('kicks-table');
        if (kicksTable) {
            kicksTable.innerHTML = html;
        }
    } catch (error) {
        console.error('Erro ao carregar histórico de expulsões:', error);
        
        const kicksTable = document.getElementById('kicks-table');
        if (kicksTable) {
            kicksTable.innerHTML = `
                <tr><td colspan="3" class="text-center text-danger">Erro ao carregar expulsões: ${error.message || 'Erro desconhecido'}</td></tr>`;
        }
    }
}

// Funções para executar comandos
async function runBotCommand(command, data = {}) {
    try {
        if (!command) {
            throw new Error('Nenhum comando especificado');
        }
        
        const result = await fetchWithErrorHandling('/api/run_command', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                command: command,
                ...data
            })
        });
        
        if (result.status === 'success') {
            showToast(`Comando ${command} executado com sucesso!`, 'success');
            return result;
        } else {
            throw new Error(result.message || 'Erro desconhecido');
        }
    } catch (error) {
        console.error(`Erro ao executar comando ${command}:`, error);
        showToast(`Erro ao executar comando ${command}: ${error.message || 'Erro desconhecido'}`, 'error');
        return null;
    }
}

// Configurar eventos dos botões
function setupEventListeners() {
    try {
        // Whitelist
        const addUserWhitelistBtn = document.getElementById('add-user-whitelist');
        if (addUserWhitelistBtn) {
            addUserWhitelistBtn.addEventListener('click', () => {
                const userId = document.getElementById('whitelist-user-id')?.value;
                if (userId) {
                    updateWhitelist('add', 'user', userId);
                } else {
                    showToast('Por favor, insira um ID de usuário válido', 'warning');
                }
            });
        }
        
        const addRoleWhitelistBtn = document.getElementById('add-role-whitelist');
        if (addRoleWhitelistBtn) {
            addRoleWhitelistBtn.addEventListener('click', () => {
                const roleId = document.getElementById('whitelist-role-id')?.value;
                if (roleId) {
                    updateWhitelist('add', 'role', roleId);
                } else {
                    showToast('Por favor, insira um ID de cargo válido', 'warning');
                }
            });
        }
        
        // Cargos permitidos
        const addAllowedRoleBtn = document.getElementById('add-allowed-role');
        if (addAllowedRoleBtn) {
            addAllowedRoleBtn.addEventListener('click', () => {
                const roleId = document.getElementById('allowed-role-id')?.value;
                if (roleId) {
                    updateAllowedRoles('add', roleId);
                } else {
                    showToast('Por favor, insira um ID de cargo válido', 'warning');
                }
            });
        }
        
        // Logs e eventos
        const refreshLogsBtn = document.getElementById('refreshLogs');
        if (refreshLogsBtn) {
            refreshLogsBtn.addEventListener('click', loadLogs);
        }
        
        // Configurações
        const saveConfigBtn = document.getElementById('save-config');
        if (saveConfigBtn) {
            saveConfigBtn.addEventListener('click', saveConfig);
        }
        
        const restartBotBtn = document.getElementById('restart-bot');
        if (restartBotBtn) {
            restartBotBtn.addEventListener('click', restartBot);
        }
        
        const backupBotBtn = document.getElementById('backup-bot');
        if (backupBotBtn) {
            backupBotBtn.addEventListener('click', createBackup);
        }
        
        const exportReportBtn = document.getElementById('export-report');
        if (exportReportBtn) {
            exportReportBtn.addEventListener('click', exportReport);
        }
        
        // Comandos do bot
        const syncCommandsBtn = document.getElementById('sync-commands-btn');
        if (syncCommandsBtn) {
            syncCommandsBtn.addEventListener('click', () => {
                if (confirm('Deseja sincronizar os comandos do bot agora?')) {
                    runBotCommand('sync_commands');
                }
            });
        }
        
        const cleanupDataBtn = document.getElementById('cleanup-data-btn');
        if (cleanupDataBtn) {
            cleanupDataBtn.addEventListener('click', () => {
                if (confirm('Deseja limpar dados antigos do banco de dados agora?')) {
                    const days = prompt('Dias para manter (padrão: 60):', '60');
                    runBotCommand('cleanup_data', { days: parseInt(days) || 60 });
                }
            });
        }
        
        const forceCheckBtn = document.getElementById('force-check-btn');
        if (forceCheckBtn) {
            forceCheckBtn.addEventListener('click', () => {
                const userId = document.getElementById('force-check-user')?.value;
                if (userId) {
                    if (confirm(`Deseja forçar verificação de inatividade para o usuário ${userId}?`)) {
                        runBotCommand('force_check', { member_id: userId });
                    }
                } else {
                    showToast('Por favor, insira um ID de usuário válido', 'warning');
                }
            });
        }
        
        // Carregar históricos quando as abas são clicadas
        const warningsTab = document.getElementById('warnings-tab');
        if (warningsTab) {
            warningsTab.addEventListener('click', loadWarningsHistory);
        }
        
        const kicksTab = document.getElementById('kicks-tab');
        if (kicksTab) {
            kicksTab.addEventListener('click', loadKicksHistory);
        }
        
        // Botão de atualizar detalhes da guilda
        const refreshGuildDetailsBtn = document.getElementById('refresh-guild-details');
        if (refreshGuildDetailsBtn) {
            refreshGuildDetailsBtn.addEventListener('click', () => {
                if (currentGuildId) {
                    loadGuildDetails(currentGuildId);
                }
            });
        }
    } catch (error) {
        console.error('Erro ao configurar event listeners:', error);
        showToast('Erro ao configurar interações da página: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Função de inicialização do dashboard
function initializeDashboard() {
    try {
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
    } catch (error) {
        console.error('Erro ao inicializar dashboard:', error);
        showToast('Erro ao inicializar dashboard: ' + (error.message || 'Erro desconhecido'), 'error');
    }
}

// Limpar intervalos quando a página é fechada
window.addEventListener('beforeunload', () => {
    try {
        refreshIntervals.forEach(interval => {
            if (interval) clearInterval(interval);
        });
    } catch (error) {
        console.error('Erro ao limpar intervalos:', error);
    }
});

// Gerenciador de erros global
window.addEventListener('error', function(event) {
    console.error('Erro global:', event.error);
    showToast('Ocorreu um erro inesperado', 'error');
});

// Inicializar o dashboard quando o DOM estiver carregado
document.addEventListener('DOMContentLoaded', initializeDashboard);