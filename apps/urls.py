"""Main endpoint of all apps urls."""

from django.conf.urls import include, url

app_name = 'apps'

urlpatterns = [
    url(r'^', include('apps.mapper.urls')),
    url(r'ozon', include('apps.ozon.urls')),
]
