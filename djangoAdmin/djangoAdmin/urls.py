from django.contrib import admin
from django.urls import path, include
from posts.views import import_api

urlpatterns = [
    path("admin/", admin.site.urls),
    path('', include('django_prometheus.urls')),

    path("api/import/start/", import_api.start_import, name="start_import"),
    path("api/import/progress/<uuid:task_id>/", import_api.import_progress, name="import_progress"),
]