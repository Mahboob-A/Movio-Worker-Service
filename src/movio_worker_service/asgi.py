"""
ASGI config for movio_worker_service project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

# TODO: Change the settings to .production in production environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'movio_worker_service.settings.dev')

application = get_asgi_application()
