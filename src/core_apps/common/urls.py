from django.urls import path

from core_apps.common.views import MovioWorkerHealthcheck

urlpatterns = [
        path("healthcheck/", MovioWorkerHealthcheck.as_view(), name="healthcheck"),
]


