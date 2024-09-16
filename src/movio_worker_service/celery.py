from __future__ import absolute_import, unicode_literals
import os
from django.conf import settings
from celery import Celery

# TODO: Change the settings environment into .production in production environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "movio_worker_service.settings.dev")

app = Celery("movio_worker_service")

app.config_from_object("django.conf:settings", namespace="CELERY")

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

# app.conf.broker_url = "redis://movio-worker-service-redis-container:6379/0"
# app.conf.result_backend = "redis://movio-worker-service-redis-container:6379/0"
