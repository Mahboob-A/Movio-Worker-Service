"""
WSGI config for movio_worker_service project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

# TODO: Change the settings to .production in production environment 
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'movio_worker_service.settings.dev')

application = get_wsgi_application()
