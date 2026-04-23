from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


# ── Request ────────────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str = Field(..., description="Natural-language product description", min_length=2)
    limit: int = Field(10, ge=1, le=50)
    in_stock_only: bool = Field(False, description="Filter to products with HIGH stock at ≥1 location")


# ── Response building blocks ───────────────────────────────────────────────────

class ProductPriceStats(BaseModel):
    product_id: str
    brand: Optional[str]
    description: str
    temperature_indicator: Optional[str]
    avg_rating: Optional[float]
    total_review_count: Optional[int]
    locations_available: int
    min_price: Optional[float]
    max_price: Optional[float]
    avg_price: Optional[float]
    price_stddev: Optional[float]
    min_promo_price: Optional[float]
    locations_with_promo: int


class LocationPrice(BaseModel):
    location_id: str
    store_name: str
    address_line1: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    size: Optional[str]
    sold_by: Optional[str]
    regular_price: Optional[float]
    promo_price: Optional[float]
    stock_level: Optional[str]
    fulfillment_in_store: Optional[bool]
    fulfillment_curbside: Optional[bool]
    fulfillment_delivery: Optional[bool]
    fulfillment_ship_to_home: Optional[bool]
    price_as_of: Optional[str]


# ── Endpoint responses ─────────────────────────────────────────────────────────

class SearchResponse(BaseModel):
    query: str
    results: list[ProductPriceStats]


class ProductLocationsResponse(BaseModel):
    product_id: str
    brand: Optional[str]
    description: str
    locations: list[LocationPrice]
