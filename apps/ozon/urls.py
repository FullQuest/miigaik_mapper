"""Ozon urls."""
from django.conf.urls import url
from django.urls import include, path
from rest_framework import routers

from .views.views import (
    AuthDataAPIView,
    AuthKeyViewSet,
    DomainViewSet,
    FeedUrlViewSet,
    OzonOffersErrorsReportView,
)

router = routers.DefaultRouter()
router.register(r'authkeys', AuthKeyViewSet)
router.register(r'feeds', FeedUrlViewSet)

urlpatterns = [
    url(r'^/domains/', DomainViewSet.as_view()),
    url(r'^/check-auth-data/', AuthDataAPIView.as_view()),

    path('/reports/', include([
        path(
            route='<domain_id>/',
            view=OzonOffersErrorsReportView.as_view(),
            name='ozon_offers_errors_report'),
    ])),
    path('/', include(router.urls)),
]
