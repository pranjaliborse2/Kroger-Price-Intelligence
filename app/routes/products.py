from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    ProductLocationsResponse,
    SearchRequest,
    SearchResponse,
)
from app.services.search import get_product_locations, search_products

router = APIRouter(tags=["products"])


@router.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """
    Embed a natural-language product description and return the closest
    matching products with price statistics across all Houston locations.
    """
    results = await search_products(
        query=request.query,
        limit=request.limit,
        in_stock_only=request.in_stock_only,
    )
    return SearchResponse(query=request.query, results=results)


@router.get("/products/{product_id}/locations", response_model=ProductLocationsResponse)
async def product_locations(product_id: str):
    """
    Return per-location price and availability for a single product,
    sorted cheapest-first.
    """
    header, locations = await get_product_locations(product_id)
    if not header:
        raise HTTPException(status_code=404, detail=f"Product {product_id!r} not found.")
    return ProductLocationsResponse(**header, locations=locations)
