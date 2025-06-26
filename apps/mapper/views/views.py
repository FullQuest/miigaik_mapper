"""Views for mapper."""
import re

from django.db import transaction
from django.db.models.functions import Length

from apps.mapper.models import (
    AttributeMap,
    CategoryMap,
    FeedCategory,
    FeedCategoryAttribute,
    FeedCategoryAttributeValue,
    FeedMeta,
    FeedMarketplaceSettings,
    MarketCategory,
    MarketAttributeValue,
    MarketCategoryAttribute,
    Marketplace,
    ValueMap,
)

from apps.mapper.permissions import (
    IsStaffOrAdmin,
    IsAccountant,
)
from apps.mapper.reports.reports import FeedMapperReport
from apps.mapper.reports.scripts.mapper_report_to_email import (
    run_mapper_report_maker_detached,
)
from apps.mapper.fetchers.ozon.ozon_single_category_fetcher import (
    update_ozon_category,
)

from apps.mapper.serializers.serializers import (
    AttributeMapSerializer,
    CategoryMapSerializer,
    FeedCategoryAttributeSerializer,
    FeedCategoryAttributeValueSerializer,
    FeedCategorySerializer,
    FeedCategoryListSerializer,
    FeedMarketplaceSettingsSerializer,
    FeedMetaCustomSerializer,
    FeedMetaSerializer,
    MarketAttributeValueSerializer,
    MarketCategoryAttributeSerializer,
    MarketCategorySerializer,
    MarketplaceSerializer,
    ValueMapSerializer,
)
from apps.mapper.utils.optimized_queries import get_feed_category_tree_data

from apps.mapper.utils.utils import (
    make_marketplace_category_tree,
    map_attribute_equal_values,
    map_attributes_by_name,
    copy_mapping,
)
from rest_framework import status
from rest_framework.exceptions import NotFound
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet

VALUES_SEARCH_RESULT_LENGTH = 100
OZON = 'ozon'
WILDBERRIES = 'wildberries'


class MarketplaceCategoryAttributesAndValues(APIView):
    """API for update category attributes and their values."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def put(self, request: Request, category_id: int):
        """Update category data."""
        try:
            category = MarketCategory.objects.get(id=category_id)

            if not category.source_id:
                raise Exception('No source id in category')

            if f'{category.marketplace}' == OZON:
                update_ozon_category(category.source_id)
                return Response(status=status.HTTP_200_OK)

            return Response(
                {'error': f'marketplace {category.marketplace} not allowed'},
                status=status.HTTP_403_FORBIDDEN,
            )

        except MarketCategory.DoesNotExist:
            return Response(
                {'error': f'category {category_id} not found'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as err:
            return Response(
                {'error': f'Err updating category data {err}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class CategoryMapViewSet(ModelViewSet):
    """API for category mapping."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = CategoryMapSerializer
    queryset = CategoryMap.objects.all()
    http_method_names = ['get', 'post', 'delete']

    def create(self, request, *args, **kwargs):
        """Override default create method to create multiple instances."""
        serializer = self.get_serializer(
            data=request.data,
            many=isinstance(request.data, list),
        )
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)

        response = Response(
            serializer.data,
            status=status.HTTP_201_CREATED,
            headers=headers,
        )

        map_instances = response.data if isinstance(response.data, list) else [
            response.data,
        ]

        for instance in map_instances:
            category_map_id = instance['id']

            map_attributes_by_name(category_map_id)

        return response


class CategoryMapBatchDeleteView(APIView):
    """Class for batch CategoryMap delete."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def delete(self, request: Request):
        """Delete batch CategoryMap objects by their ids.

        How to use:

        Send url with query params like this:

        DELETE /api/v1/mapper/category_mapping/batch_delete?ids=1,2,3

        where ids are category mapping ids.
        """
        query_params = request.query_params
        category_map_ids = query_params.get('ids', '').split(',')

        for category_map_id in category_map_ids:
            try:
                CategoryMap.objects.get(id=category_map_id).delete()
            except CategoryMap.DoesNotExist:
                raise NotFound(
                    detail=f'Category mapping with {category_map_id} id'
                           f' not found!',
                )

        return Response(
            data=f'Category mappings with {category_map_ids}'
                 f' ids are successfully deleted!',
            status=status.HTTP_200_OK,
        )


class AttributeMapViewSet(ModelViewSet):
    """API for attribute mapping."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = AttributeMapSerializer
    http_method_names = ['get', 'post', 'delete']

    def get_queryset(self):
        """DB query set."""
        category_map = self.kwargs['category_mapping_pk']

        queryset = AttributeMap.objects.filter(
            category_map=category_map,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()

    def create(self, request, *args, **kwargs):
        """Create attribute map."""
        response = super(AttributeMapViewSet, self).create(
            request,
            *args,
            **kwargs,
        )
        instance = response.data
        map_attribute_equal_values(instance['id'])
        return response


class ValueMapViewSet(ModelViewSet):
    """API for value mapping."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = ValueMapSerializer
    http_method_names = ['get', 'post', 'delete']

    def get_queryset(self):
        """DB query set."""
        attribute_map = self.kwargs['attribute_mapping_pk']

        queryset = ValueMap.objects.filter(
            attribute_map=attribute_map,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()


class FeedCategoryAttributeNameView(APIView):
    """Endpoint for feed parent category attribute names reading."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(
        self, request: Request, category_id: int,
    ) -> Response:
        """Return child categories attribute names."""

        def get_child_attribute_names(category_id):
            names = set()
            for attribute in FeedCategoryAttribute.objects.filter(
                category_id=category_id,
                deleted=False,
            ):
                names.add(attribute.name)

            for category in FeedCategory.objects.filter(
                parent_id=category_id,
                deleted=False,
            ):
                names.update(get_child_attribute_names(category.id))
            return names

        attribute_names = get_child_attribute_names(category_id)

        return Response(
            data=[{'name': name} for name in sorted(list(attribute_names))],
            status=status.HTTP_200_OK,
        )


class FeedCategoryMarketAttributeNameView(APIView):
    """Endpoint for mapper reports."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(
        self, request: Request, category_id: int, market_id: int,
    ) -> Response:
        """Return child mapped categories attribute names."""

        def get_child_attribute_names(feed_category_id):
            names = set()

            market_category_ids = CategoryMap.objects.filter(
                feed_category_id=feed_category_id,
                marketplace_category__marketplace__id=market_id,
                marketplace_category__deleted=False
            ).values_list('marketplace_category_id', flat=True)

            names.update(
                MarketCategoryAttribute.objects.filter(
                    deleted=False,
                    category_id__in=market_category_ids,
                ).values_list('attribute__name', flat=True)
            )

            for category in FeedCategory.objects.filter(
                parent_id=feed_category_id,
                deleted=False,
            ):
                names.update(get_child_attribute_names(category.id))
            return names

        attribute_names = get_child_attribute_names(category_id)

        return Response(
            data=[{'name': name} for name in sorted(list(attribute_names))],
            status=status.HTTP_200_OK,
        )




class FeedMetaViewSet(ModelViewSet):
    """API for feed manipulations."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin, IsAccountant)
    queryset = FeedMeta.objects.all()

    def get_serializer_class(self):
        """Add custom serializer."""
        if self.action in ['create', 'update', 'partial_update']:
            return FeedMetaCustomSerializer

        return FeedMetaSerializer


class FeedCategoryView(ListAPIView):
    """Endpoint for reading feed category tree."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = FeedCategoryListSerializer

    def get_queryset(self):
        """DB query set."""
        feed_id = self.kwargs['feed_id']

        queryset = FeedCategory.objects.filter(
            feed=feed_id,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()

    def get(self, request: Request, feed_id: int) -> Response:
        """Return category tree."""

        data = get_feed_category_tree_data(feed_id)
        if data:
            return Response(data=data, status=status.HTTP_200_OK)

        else:
            raise NotFound()


class FeedCategoryByIdView(APIView):
    """View for reading feed category by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, category_id: int) -> Response:
        """Get feed category by id."""
        try:
            category = FeedCategory.objects.get(id=category_id)
            data = FeedCategorySerializer(category).data

        except FeedCategory.DoesNotExist:
            return Response(
                {'detail': 'Category not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)

    def patch(self, request: Request, category_id: int) -> Response:
        """Patch specific object fields."""
        category = get_object_or_404(FeedCategory, id=category_id)
        serializer = FeedCategorySerializer(
            category,
            data=request.data,
            partial=True,
        )

        if serializer.is_valid():
            serializer.save()
            return Response(data=serializer.data, status=status.HTTP_200_OK)

        return Response(
            {'detail': 'Invalid data'},
            status=status.HTTP_400_BAD_REQUEST,
        )


class FeedCategoryAttributeView(ListAPIView):
    """Endpoint for reading category attributes."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = FeedCategoryAttributeSerializer

    def get_queryset(self):
        """DB query set."""
        category_id = self.kwargs['category_id']

        queryset = FeedCategoryAttribute.objects.filter(
            category=category_id,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()


class FeedCategoryAttributeByIdView(APIView):
    """Endpoint for reading category attribute by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, attribute_id: int) -> Response:
        """Get feed category attribute by id."""
        try:
            attribute = FeedCategoryAttribute.objects.get(id=attribute_id)
            data = FeedCategoryAttributeSerializer(attribute).data

        except FeedCategoryAttribute.DoesNotExist:
            return Response(
                {'detail': 'Attribute not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)


class FeedCategoryAttributeValueView(ListAPIView):
    """Endpoint for reading category attribute values."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = FeedCategoryAttributeValueSerializer

    def get_queryset(self):
        """DB query set."""
        attribute_id = self.kwargs['attribute_id']

        queryset = FeedCategoryAttributeValue.objects.filter(
            attribute=attribute_id,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()


class FeedCategoryAttributeValueByIdView(APIView):
    """Endpoint for reading category attribute value by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, value_id: int) -> Response:
        """Get feed category attribute value by id."""
        try:
            value = FeedCategoryAttributeValue.objects.get(id=value_id)
            data = FeedCategoryAttributeValueSerializer(value).data

        except FeedCategoryAttributeValue.DoesNotExist:
            return Response(
                {'detail': 'Value not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)


class MarketplaceViewSet(ModelViewSet):
    """Endpoint for marketplace manipulations."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = MarketplaceSerializer
    queryset = Marketplace.objects.all()
    http_method_names = ['get']


class MarketCategoryView(ListAPIView):
    """Endpoint for reading market categories."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = MarketCategorySerializer

    def get_queryset(self):
        """DB query set."""
        marketplace_id = self.kwargs['marketplace_id']

        queryset = MarketCategory.objects.filter(
            marketplace=marketplace_id,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()

    def get(self, request: Request, marketplace_id: int) -> Response:
        """Return category tree."""
        categories = MarketCategory.objects.filter(
            marketplace=marketplace_id,
        )

        if categories:

            raw_data = MarketCategorySerializer(categories, many=True).data

            data = make_marketplace_category_tree(raw_data)

            return Response(data=data, status=status.HTTP_200_OK)

        else:

            raise NotFound()


class MarketCategoryByIdView(APIView):
    """Endpoint for reading market category by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, category_id: int) -> Response:
        """Get marketplace category by id."""
        try:
            category = MarketCategory.objects.get(id=category_id)
            data = MarketCategorySerializer(category).data

        except MarketCategory.DoesNotExist:
            return Response(
                {'detail': 'Category not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)


class MarketCategoryAttributeView(ListAPIView):
    """Endpoint for reading market category attributes."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = MarketCategoryAttributeSerializer

    def get_queryset(self):
        """DB query set."""
        category_id = self.kwargs['category_id']

        queryset = MarketCategoryAttribute.objects.filter(
            category=category_id,
            attribute__disabled=False,
        )

        if queryset:
            return queryset
        else:
            raise NotFound()


class MarketCategoryAttributeByIdView(APIView):
    """Endpoint for reading market category attribute by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, attribute_id: int) -> Response:
        """Get market category attribute by id."""
        try:
            attribute = MarketCategoryAttribute.objects.get(id=attribute_id)
            data = MarketCategoryAttributeSerializer(attribute).data

        except MarketCategoryAttribute.DoesNotExist:
            return Response(
                {'detail': 'Attribute not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)


class MarketAttributeValueView(ListAPIView):
    """Endpoint for reading market category attribute values."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = MarketAttributeValueSerializer

    def get_queryset(self):
        """DB query set."""
        attribute_id = self.kwargs['attribute_id']

        category_attribute = MarketCategoryAttribute.objects.get(
            id=attribute_id,
        )

        search_string = self.request.query_params.get("search", "")

        if not category_attribute.attribute.dictionary:
            raise NotFound()

        filters = {
            'dictionary': category_attribute.attribute.dictionary.id,
            'deleted': False,
        }

        if search_string:
            filters['value__icontains'] = search_string

        queryset = MarketAttributeValue.objects.filter(
            **filters,
        ).order_by(
            Length('value').asc(),
            'value',
        )

        if queryset:
            return queryset[:VALUES_SEARCH_RESULT_LENGTH]
        else:
            raise NotFound()


class MarketAttributeValueByIdView(APIView):
    """Endpoint for reading market category attribute value by id."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(self, request: Request, value_id: int) -> Response:
        """Get market category attribute value by id."""
        try:
            value = MarketAttributeValue.objects.get(id=value_id)
            data = MarketAttributeValueSerializer(value).data

        except MarketAttributeValue.DoesNotExist:
            return Response(
                {'detail': 'Value not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(data=data, status=status.HTTP_200_OK)


class FeedMarketplaceSettingsViewSet(ModelViewSet):
    """Endpoint for FeedMarketplaceSettings manipulations."""

    permission_classes = IsAuthenticated, IsStaffOrAdmin
    serializer_class = FeedMarketplaceSettingsSerializer
    http_method_names = ['get', 'post', 'delete']

    def get_queryset(self):
        """DB query set."""
        feed_id = self.kwargs['feed_id']
        marketplace_id = self.kwargs['marketplace_id']
        queryset = FeedMarketplaceSettings.objects.filter(
            feed_id=feed_id,
            marketplace_id=marketplace_id,
        )
        if queryset:
            return queryset
        else:
            raise NotFound()

    def create(self, request, *args, **kwargs):
        """Override default create method."""
        request_data_list = request.data
        if not isinstance(request_data_list, list):
            request_data_list = [request_data_list]
        for _data in request_data_list:
            _data.update({
                'feed': kwargs['feed_id'],
                'marketplace': kwargs['marketplace_id'],
            })

        serializer = self.get_serializer(
            data=request.data,
            many=isinstance(request.data, list),
        )
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)

        return Response(
            serializer.data,
            status=status.HTTP_201_CREATED,
            headers=headers,
        )


class FeedMarketplaceSettingsBatchDeleteView(APIView):
    """Class for batch CategorySettings delete."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def delete(self, request: Request, *args, **kwargs):
        """Delete batch FeedMarketplaceSettings objects by their ids.

        How to use:

        Send url with query params like this:

        DELETE /api/v1/mapper/settings/batch_delete?ids=1,2,3

        where ids are setting ids.
        """
        query_params = request.query_params
        settings_ids = query_params.get('ids', '').split(',')

        for settings_id in settings_ids:
            try:
                FeedMarketplaceSettings.objects.get(
                    id=settings_id,
                ).delete()
            except FeedMarketplaceSettings.DoesNotExist:
                raise NotFound(
                    detail=f'Settings with {settings_id} id not found!',
                )

        return Response(
            data=f'Settings with {settings_ids}'
                 f' ids are successfully deleted!',
            status=status.HTTP_200_OK,
        )


class FeedMappingReportView(APIView):
    """Endpoint for mapper reports."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(
        self, request: Request, marketplace_id: int, feed_id: int,
    ) -> Response:
        """Build mapper report and return url."""
        report_url = FeedMapperReport(marketplace_id, feed_id).build_report()

        return Response(
            data={
                'report_url': report_url,
            },
            status=status.HTTP_200_OK,
        )

    def post(
        self, request: Request, marketplace_id: int, feed_id: int,
    ) -> Response:
        """Generate mapper report in a subprocess and send it to email."""
        try:
            emails = getattr(request.data, 'get', {}.get)('email')
            if not emails or type(emails) is not str:
                raise Exception('No emails provided')

            valid_emails = [
                email for email in emails.split(' ')
                if is_email_valid(email)
            ]
            if not valid_emails:
                raise Exception(
                    f'All provided emails are invalid. {emails.split(" ")}',
                )

        except Exception as err:
            return Response(
                data={
                    'message': f'Error: {err}',
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            run_mapper_report_maker_detached(feed_id, valid_emails)
            return Response(
                status=status.HTTP_202_ACCEPTED,
                data={'message': 'report generation started'},
            )

        except Exception as err:
            return Response(
                data={
                    'message': (
                        f'Report generation not started. Error: {err}'
                    ),
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def is_email_valid(email: str) -> bool:
    """Check if email str is valid."""
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if (re.fullmatch(regex, email)):
        return True
    return False


class FeedMappingsCopyView(APIView):
    """Endpoint for feed mappings copy."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    @transaction.atomic
    def post(
        self, request: Request, from_feed_id: int,
    ) -> Response:
        """Create feed and copy mappings from other one."""
        serializer = FeedMetaCustomSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        response = Response(
            serializer.data,
            status=status.HTTP_201_CREATED,
        )

        copy_mapping(
            from_feed_id,
            serializer.data['id'],
        )

        return response
