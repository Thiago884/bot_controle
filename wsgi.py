# Importa e aplica o monkey-patching do gevent o mais cedo possível
from gevent import monkey
monkey.patch_all()

import os
# Importa a instância do Flask 'app' do seu arquivo web_panel.py
from web_panel import app as application

# O Gunicorn espera que a aplicação WSGI seja chamada 'application' por padrão.
# Se a sua instância do Flask em web_panel.py se chamasse algo diferente de 'app',
# você precisaria ajustar a linha acima (ex: from web_panel import my_flask_app as application).

# Adicione uma rota básica para o health check do Render, se ainda não houver
# no seu web_panel.py para a rota '/'
# Se web_panel.py já tiver uma rota para '/', esta pode ser redundante,
# mas não causará problemas. É uma boa prática ter uma rota / para health checks.
@application.route('/')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    # Este bloco é para execução local usando 'python wsgi.py'
    # Gunicorn será usado na produção, então este bloco não será executado no Render.
    port = int(os.environ.get("PORT", 8080))
    application.run(host='0.0.0.0', port=port)