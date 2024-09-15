
from django.contrib import admin
from django.urls import path, include 
from django.conf import settings

urlpatterns = [
    path(settings.ADMIN_URL, admin.site.urls),
    
    # Common APP 
    path("api/v1/common/", include("core_apps.common.urls")), 
    
             
]
