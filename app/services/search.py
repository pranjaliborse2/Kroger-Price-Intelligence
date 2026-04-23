from __future__ import annotations

from app.database import acquire
from app.models.schemas import LocationPrice, ProductPriceStats


async def search_products(
    query: str,
    limit: int = 10,
    in_stock_only: bool = False,
) -> list[ProductPriceStats]:
    """
    Full-text search against product description and brand using PostgreSQL
    tsvector/tsquery, ranked by ts_rank. Falls back to ILIKE if the query
    contains no words recognised by the text-search parser.
    """
    stock_filter = "AND ps.locations_available > 0" if in_stock_only else ""

    sql = f"""
        SELECT
            ps.product_id,
            ps.brand,
            ps.description,
            ps.temperature_indicator,
            ps.avg_rating,
            ps.total_review_count,
            ps.locations_available,
            ps.min_price,
            ps.max_price,
            ps.avg_price,
            ps.price_stddev,
            ps.min_promo_price,
            ps.locations_with_promo
        FROM vw_product_price_stats ps
        JOIN dim_products p ON p.product_id = ps.product_id
        WHERE (
            to_tsvector('english', COALESCE(p.description, '') || ' ' || COALESCE(p.brand, ''))
            @@ websearch_to_tsquery('english', $1)
        )
        {stock_filter}
        ORDER BY ts_rank(
            to_tsvector('english', COALESCE(p.description, '') || ' ' || COALESCE(p.brand, '')),
            websearch_to_tsquery('english', $1)
        ) DESC
        LIMIT $2;
    """

    async with acquire() as conn:
        rows = await conn.fetch(sql, query, limit)

    # If full-text search returns nothing, fall back to ILIKE
    if not rows:
        fallback_sql = f"""
            SELECT
                ps.product_id,
                ps.brand,
                ps.description,
                ps.temperature_indicator,
                ps.avg_rating,
                ps.total_review_count,
                ps.locations_available,
                ps.min_price,
                ps.max_price,
                ps.avg_price,
                ps.price_stddev,
                ps.min_promo_price,
                ps.locations_with_promo
            FROM vw_product_price_stats ps
            JOIN dim_products p ON p.product_id = ps.product_id
            WHERE (
                p.description ILIKE $1
                OR p.brand     ILIKE $1
            )
            {stock_filter}
            ORDER BY ps.avg_price ASC NULLS LAST
            LIMIT $2;
        """
        pattern = f"%{query}%"
        async with acquire() as conn:
            rows = await conn.fetch(fallback_sql, pattern, limit)

    return [ProductPriceStats(**dict(row)) for row in rows]


async def get_product_locations(product_id: str) -> tuple[dict, list[LocationPrice]]:
    """
    Return product header info and per-location price/fulfillment details,
    sorted cheapest-first.
    """
    sql = """
        SELECT
            product_id,
            brand,
            product_description  AS description,
            location_id,
            store_name,
            address_line1,
            city,
            state,
            zip_code,
            latitude,
            longitude,
            size,
            sold_by,
            regular_price,
            promo_price,
            stock_level,
            fulfillment_in_store,
            fulfillment_curbside,
            fulfillment_delivery,
            fulfillment_ship_to_home,
            price_as_of::TEXT AS price_as_of
        FROM vw_product_location_detail
        WHERE product_id = $1
        ORDER BY regular_price ASC NULLS LAST;
    """

    async with acquire() as conn:
        rows = await conn.fetch(sql, product_id)

    if not rows:
        return {}, []

    first  = dict(rows[0])
    header = {
        "product_id":  first["product_id"],
        "brand":       first["brand"],
        "description": first["description"],
    }
    locations = [
        LocationPrice(
            location_id=r["location_id"],
            store_name=r["store_name"],
            address_line1=r["address_line1"],
            city=r["city"],
            state=r["state"],
            zip_code=r["zip_code"],
            latitude=r["latitude"],
            longitude=r["longitude"],
            size=r["size"],
            sold_by=r["sold_by"],
            regular_price=r["regular_price"],
            promo_price=r["promo_price"],
            stock_level=r["stock_level"],
            fulfillment_in_store=r["fulfillment_in_store"],
            fulfillment_curbside=r["fulfillment_curbside"],
            fulfillment_delivery=r["fulfillment_delivery"],
            fulfillment_ship_to_home=r["fulfillment_ship_to_home"],
            price_as_of=r["price_as_of"],
        )
        for r in rows
    ]
    return header, locations
