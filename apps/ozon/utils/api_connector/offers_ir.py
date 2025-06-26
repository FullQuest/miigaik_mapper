"""Intermediate representation of offer related objects for Ozon API."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Offer:
    """Ozon offer representation."""

    barcode: str
    category_id: int
    name: str
    offer_id: str
    price: str
    old_price: str
    vat: str
    height: int
    depth: int
    width: int
    dimension_unit: str
    weight: int
    weight_unit: str
    images: List[str]
    images360: str
    primary_image: str
    attributes: List[Any]
    complex_attributes: List[Any]
    new_description_category_id: Optional[int] = None


def convert_to_ozon_offer(ozon_offer: Dict[str, Any]) -> Offer:
    """Convert Ozon offer dict to Offer data class.

    :param ozon_offer: Dictionary representation of Ozon offer
    :type: Dict[str, Any]

    :return offer: Offer dataclass
    :rtype: Offer
    """
    offer = Offer(
        barcode=ozon_offer['barcode'],
        category_id=ozon_offer['category_id'],
        name=ozon_offer['name'],
        offer_id=ozon_offer['offer_id'],
        price=str(ozon_offer['price']),
        old_price=ozon_offer['old_price'],
        vat=ozon_offer['vat'],
        height=int(float(ozon_offer['height'])),
        depth=int(float(ozon_offer['depth'])),
        width=int(float(ozon_offer['width'])),
        weight=int(float(ozon_offer['weight'])),
        dimension_unit=ozon_offer['dimension_unit'],
        weight_unit=ozon_offer['weight_unit'],
        images=ozon_offer['images'],
        images360=ozon_offer['images360'],
        primary_image=ozon_offer['primary_image'],
        attributes=ozon_offer['attributes'],
        complex_attributes=ozon_offer['complex_attributes'],
        new_description_category_id=ozon_offer.get('new_description_category_id'),
    )

    return offer

def convert_offer_to_item(
    offer: Offer,
) -> Dict[str, Any]:
    """Convert Offer dataclass to request item

    :param offer: Offer dataclass

    :return item: item Dictionary
    """

    item = {
        'barcode': offer.barcode,
        'description_category_id': int(
            f'{offer.category_id}'.split('_')[0]
        ),
        'type_id': int(
            f'{offer.category_id}'.split('_')[1]
        ),
        'name': offer.name,
        'offer_id': offer.offer_id,
        'price': offer.price,
        'old_price': offer.old_price,
        'vat': offer.vat,
        'height': offer.height,
        'depth': offer.depth,
        'width': offer.width,
        'dimension_unit': offer.dimension_unit,
        'weight': offer.weight,
        'weight_unit': offer.weight_unit,
        'images': offer.images,
        'images360': offer.images360,
        'primary_image': offer.primary_image,
        'attributes': offer.attributes,
    }

    if offer.new_description_category_id:
        item['new_description_category_id'] = offer.new_description_category_id

    if offer.complex_attributes:
        item['complex_attributes'] = offer.complex_attributes

    return item
