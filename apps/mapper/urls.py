"""Endpoints for mapper app."""

from django.urls import include, path
from rest_framework import routers
from rest_framework_nested import routers as nested_routers

from .views.views import (
     AttributeMapViewSet,
     CategoryMapBatchDeleteView,
     CategoryMapViewSet,
     FeedCategoryAttributeByIdView,
     FeedCategoryAttributeValueByIdView,
     FeedCategoryAttributeValueView,
     FeedCategoryAttributeView,
     FeedCategoryByIdView,
     FeedCategoryView,
     FeedMarketplaceSettingsViewSet,
     FeedMarketplaceSettingsBatchDeleteView,
     FeedMetaViewSet,
     MarketCategoryAttributeView,
     MarketplaceCategoryAttributesAndValues,
     MarketCategoryAttributeByIdView,
     MarketAttributeValueByIdView,
     MarketAttributeValueView,
     MarketCategoryByIdView,
     MarketCategoryView,
     MarketplaceViewSet,
     ValueMapViewSet,
     FeedMappingReportView,
     FeedCategoryAttributeNameView,
     FeedCategoryMarketAttributeNameView,
     FeedMappingsCopyView,
)

router = routers.DefaultRouter()
router.register(r'feed', FeedMetaViewSet)
router.register(r'marketplace', MarketplaceViewSet)
router.register(r'category_mapping', CategoryMapViewSet)
router.register(
     r'feed/(?P<feed_id>\d+)/marketplace/(?P<marketplace_id>\d+)/settings',
     FeedMarketplaceSettingsViewSet,
     basename='settings')


mapping_router = nested_routers.NestedSimpleRouter(
     router,
     r'category_mapping',
     lookup='category_mapping',
)

mapping_router.register(
     r'attribute_mapping',
     AttributeMapViewSet,
     basename='attribute_mapping',
)

nested_mapping_router = nested_routers.NestedSimpleRouter(
     mapping_router,
     r'attribute_mapping',
     lookup='attribute_mapping',
)

nested_mapping_router.register(
     r'value_mapping',
     ValueMapViewSet,
     basename='value_mapping',
)


urlpatterns = [
     path('mapper/', include([
          path('feed/', include([
               path('<feed_id>/categories',
                    FeedCategoryView.as_view(),
                    name='feed_categories'),
               path('categories/<category_id>',
                    FeedCategoryByIdView.as_view(),
                    name='feed_category_by_id'),
               path('categories/<category_id>/attributes',
                    FeedCategoryAttributeView.as_view(),
                    name='feed_category_attributes'),
               path('categories/<category_id>/attribute_names',
                    FeedCategoryAttributeNameView.as_view(),
                    name='feed_category_attribute_names'),
               path(('categories/<category_id>/market/<market_id>/'
                     'attribute_names'),
                    FeedCategoryMarketAttributeNameView.as_view(),
                    name='feed_category_attribute_names'),
               path('categories/attributes/<attribute_id>',
                    FeedCategoryAttributeByIdView.as_view(),
                    name='feed_category_attribute_by_id'),
               path('categories/attributes/<attribute_id>/values',
                    FeedCategoryAttributeValueView.as_view(),
                    name='feed_category_attribute_value'),
               path('categories/attributes/values/<value_id>',
                    FeedCategoryAttributeValueByIdView.as_view(),
                    name='feed_category_attribute_value_by_id'),
               path('<feed_id>/marketplace/<marketplace_id>/report/',
                    FeedMappingReportView.as_view(),
                    name='feed_marketplace_mapping_report'),
               path('<from_feed_id>/copy',
                    FeedMappingsCopyView.as_view(),
                    name='feed_copy_mappings'),
               ])),
          path('marketplace/', include([
               path('<marketplace_id>/categories',
                    MarketCategoryView.as_view(),
                    name='market_categories'),
               path('categories/<category_id>',
                    MarketCategoryByIdView.as_view(),
                    name='market_category_by_id'),
               path('categories/<category_id>/attributes',
                    MarketCategoryAttributeView.as_view(),
                    name='market_category_attributes'),
               path('categories/<category_id>/attributes_values',
                    MarketplaceCategoryAttributesAndValues.as_view(),
                    name='market_category_attributes_and_values_update'),
               path('categories/attributes/<attribute_id>',
                    MarketCategoryAttributeByIdView.as_view(),
                    name='market_category_attribute_by_id'),
               path('categories/attributes/<attribute_id>/values',
                    MarketAttributeValueView.as_view(),
                    name='market_category_attribute_values'),
               path('categories/attributes/values/<value_id>',
                    MarketAttributeValueByIdView.as_view(),
                    name='market_category_attribute_value_by_id'),
               ])),
          path('', include(router.urls)),
          path('', include(mapping_router.urls)),
          path('', include(nested_mapping_router.urls)),
          path('category_mapping/batch_delete',
               CategoryMapBatchDeleteView.as_view(),
               name='batch_delete'),
          path('settings/batch_delete',
               FeedMarketplaceSettingsBatchDeleteView.as_view(),
               name='settings_batch_delete'),
     ])),
]
