"""
product_fetcher.py
For every Kroger location stored in dim_locations, searches a configurable
list of terms via the Kroger Products API and loads results into the
kroger_price_intel database.

Tables populated:
    dim_products, dim_product_categories, dim_product_declarations,
    dim_product_allergens, dim_product_images,
    dim_product_nutrition, dim_product_nutrients,
    fact_product_location_prices, dim_product_aisle_locations

Each run appends new rows to fact_product_location_prices (price history).
All other tables are upserted (idempotent).

Run from project root:
    python -m src.ingestion.product_fetcher
    python -m src.ingestion.product_fetcher --terms "whole milk" "butter" "eggs"
"""
import argparse
import os
import time

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

SLEEP_SEC  = 0.2    # pause between API calls
API_LIMIT  = 50     # max products per search request (Kroger API max)

# Default search terms — covers broad grocery categories.
# Extend or override via --terms CLI argument.
DEFAULT_SEARCH_TERMS: list[str] = [
    # Dairy
    "whole milk", "skim milk", "almond milk", "oat milk",
    "butter", "cheddar cheese", "yogurt", "eggs",
    # Bread & Bakery
    "white bread", "whole wheat bread",
    # Beverages
    "orange juice", "apple juice", "coffee", "tea",
    # Pantry staples
    "sugar", "flour", "rice", "pasta", "olive oil", "vegetable oil",
    # Snacks
    "potato chips", "cookies",
    # Frozen
    "frozen pizza", "ice cream",
    # Meat & Seafood
    "chicken breast", "ground beef", "salmon",
    # Produce
    "fruits", "vegetables"
]


# ── Kroger API ────────────────────────────────────────────────────────────────

def _get_token() -> str:
    resp = requests.post(
        "https://api.kroger.com/v1/connect/oauth2/token",
        data={"grant_type": "client_credentials", "scope": "product.compact"},
        auth=(os.getenv("KROGER_CLIENT_ID"), os.getenv("KROGER_CLIENT_SECRET")),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _search_products(token: str, location_id: str, term: str) -> list[dict]:
    resp = requests.get(
        "https://api.kroger.com/v1/products",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "filter.term":       term,
            "filter.locationId": location_id,
            "filter.limit":      API_LIMIT,
        },
        timeout=15,
    )
    if resp.status_code == 401:
        raise PermissionError("Token expired")
    resp.raise_for_status()
    return resp.json().get("data", [])


# ── Database ──────────────────────────────────────────────────────────────────

def _get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "kroger_price_intel"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def _load_location_ids(cur) -> list[str]:
    cur.execute("SELECT location_id FROM dim_locations ORDER BY location_id;")
    return [row[0] for row in cur.fetchall()]


# ── Product upsert helpers ────────────────────────────────────────────────────

def _upsert_product(cur, p: dict) -> None:
    info = p.get("itemInformation") or {}
    temp = p.get("temperature") or {}
    rnr  = p.get("ratingsAndReviews") or {}

    cur.execute(
        """
        INSERT INTO dim_products (
            product_id, brand, description, country_origin, snap_eligible,
            receipt_description, non_gmo, non_gmo_claim_name, organic_claim_name,
            certified_for_passover, hypoallergenic,
            temperature_indicator, heat_sensitive,
            item_depth, item_height, item_width, gross_weight, net_weight,
            avg_rating, total_review_count,
            allergens_description, product_page_uri
        ) VALUES (
            %(product_id)s, %(brand)s, %(description)s, %(country_origin)s, %(snap_eligible)s,
            %(receipt_description)s, %(non_gmo)s, %(non_gmo_claim_name)s, %(organic_claim_name)s,
            %(certified_for_passover)s, %(hypoallergenic)s,
            %(temperature_indicator)s, %(heat_sensitive)s,
            %(item_depth)s, %(item_height)s, %(item_width)s, %(gross_weight)s, %(net_weight)s,
            %(avg_rating)s, %(total_review_count)s,
            %(allergens_description)s, %(product_page_uri)s
        )
        ON CONFLICT (product_id) DO UPDATE SET
            avg_rating         = EXCLUDED.avg_rating,
            total_review_count = EXCLUDED.total_review_count,
            updated_at         = NOW();
        """,
        {
            "product_id":           p.get("productId"),
            "brand":                p.get("brand"),
            "description":          p.get("description"),
            "country_origin":       p.get("countryOrigin"),
            "snap_eligible":        p.get("snapEligible"),
            "receipt_description":  p.get("receiptDescription"),
            "non_gmo":              p.get("nonGmo"),
            "non_gmo_claim_name":   p.get("nonGmoClaimName"),
            "organic_claim_name":   p.get("organicClaimName"),
            "certified_for_passover": p.get("certifiedForPassover"),
            "hypoallergenic":       p.get("hypoallergenic"),
            "temperature_indicator": temp.get("indicator"),
            "heat_sensitive":       temp.get("heatSensitive"),
            "item_depth":           _to_float(info.get("depth")),
            "item_height":          _to_float(info.get("height")),
            "item_width":           _to_float(info.get("width")),
            "gross_weight":         info.get("grossWeight"),
            "net_weight":           info.get("netWeight"),
            "avg_rating":           rnr.get("averageOverallRating"),
            "total_review_count":   rnr.get("totalReviewCount"),
            "allergens_description": p.get("allergensDescription"),
            "product_page_uri":     p.get("productPageURI"),
        },
    )


def _upsert_categories(cur, product_id: str, categories: list[str]) -> None:
    if not categories:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_product_categories (product_id, category)
        VALUES %s ON CONFLICT DO NOTHING;
        """,
        [(product_id, c) for c in categories],
    )


def _upsert_declarations(cur, product_id: str, declarations: list[str]) -> None:
    if not declarations:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_product_declarations (product_id, declaration)
        VALUES %s ON CONFLICT DO NOTHING;
        """,
        [(product_id, d) for d in declarations],
    )


def _upsert_allergens(cur, product_id: str, allergens: list[dict]) -> None:
    if not allergens:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_product_allergens (product_id, allergen_name, level_of_containment)
        VALUES %s ON CONFLICT DO NOTHING;
        """,
        [(product_id, a.get("name"), a.get("levelOfContainmentName")) for a in allergens],
    )


def _upsert_images(cur, product_id: str, images: list[dict]) -> None:
    rows = []
    for img in images:
        perspective = img.get("perspective")
        featured    = img.get("featured", False)
        for s in img.get("sizes", []):
            rows.append((product_id, perspective, featured, s.get("size"), s.get("url")))
    if not rows:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_product_images (product_id, perspective, featured, size, url)
        VALUES %s ON CONFLICT (product_id, perspective, size) DO UPDATE SET
            url      = EXCLUDED.url,
            featured = EXCLUDED.featured;
        """,
        rows,
    )


def _upsert_nutrition(cur, product_id: str, nutrition_list: list[dict]) -> None:
    if not nutrition_list:
        return
    n = nutrition_list[0]   # API returns a single-element list

    serving   = n.get("servingSize") or {}
    uom       = serving.get("unitOfMeasure") or {}

    cur.execute(
        """
        INSERT INTO dim_product_nutrition (
            product_id, ingredient_statement,
            serving_size_quantity, serving_size_uom_code, serving_size_uom_name,
            daily_value_reference
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            ingredient_statement = EXCLUDED.ingredient_statement
        RETURNING id;
        """,
        (
            product_id,
            n.get("ingredientStatement"),
            serving.get("quantity"),
            uom.get("code"),
            uom.get("name"),
            n.get("dailyValueIntakeReference"),
        ),
    )
    nutrition_id = cur.fetchone()[0]

    nutrients = n.get("nutrients") or []
    if nutrients:
        # Replace nutrient rows for this nutrition block
        cur.execute("DELETE FROM dim_product_nutrients WHERE nutrition_id = %s;", (nutrition_id,))
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dim_product_nutrients (
                nutrition_id, nutrient_code, display_name, description,
                quantity, percent_daily_intake,
                uom_code, uom_name, precision_code
            ) VALUES %s;
            """,
            [
                (
                    nutrition_id,
                    nut.get("code"),
                    nut.get("displayName"),
                    nut.get("description"),
                    nut.get("quantity"),
                    nut.get("percentDailyIntake"),
                    (nut.get("unitOfMeasure") or {}).get("code"),
                    (nut.get("unitOfMeasure") or {}).get("name"),
                    (nut.get("precision") or {}).get("code"),
                )
                for nut in nutrients
            ],
        )


def _insert_price_fact(cur, product_id: str, location_id: str, item: dict) -> None:
    price  = item.get("price") or {}
    inv    = item.get("inventory") or {}
    ful    = item.get("fulfillment") or {}
    eff    = (price.get("effectiveDate") or {}).get("value")
    exp    = (price.get("expirationDate") or {}).get("value")

    cur.execute(
        """
        INSERT INTO fact_product_location_prices (
            product_id, location_id, item_id, size, sold_by,
            regular_price, promo_price,
            price_effective_date, price_expiration_date,
            stock_level,
            fulfillment_curbside, fulfillment_delivery,
            fulfillment_in_store, fulfillment_ship_to_home
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            product_id,
            location_id,
            item.get("itemId"),
            item.get("size"),
            item.get("soldBy"),
            price.get("regular"),
            price.get("promo"),
            eff,
            exp,
            inv.get("stockLevel"),
            ful.get("curbside"),
            ful.get("delivery"),
            ful.get("inStore"),
            ful.get("shipToHome"),
        ),
    )


def _upsert_aisle_location(cur, product_id: str, location_id: str, aisle_list: list[dict]) -> None:
    if not aisle_list:
        return
    a = aisle_list[0]
    cur.execute(
        """
        INSERT INTO dim_product_aisle_locations (product_id, location_id, bay_number, aisle_description)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id, location_id) DO UPDATE SET
            bay_number        = EXCLUDED.bay_number,
            aisle_description = EXCLUDED.aisle_description;
        """,
        (product_id, location_id, a.get("bayNumber"), a.get("description")),
    )


# ── Utilities ─────────────────────────────────────────────────────────────────

def _to_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _load_product(cur, p: dict, location_id: str) -> None:
    product_id = p.get("productId")
    if not product_id:
        return

    items = p.get("items") or []
    if not items:
        return  # no price data for this location — skip

    _upsert_product(cur, p)
    _upsert_categories(cur, product_id, p.get("categories") or [])
    _upsert_declarations(cur, product_id, p.get("manufacturerDeclarations") or [])
    _upsert_allergens(cur, product_id, p.get("allergens") or [])
    _upsert_images(cur, product_id, p.get("images") or [])
    _upsert_nutrition(cur, product_id, p.get("nutritionInformation") or [])
    _insert_price_fact(cur, product_id, location_id, items[0])
    _upsert_aisle_location(cur, product_id, location_id, p.get("aisleLocations") or [])


# ── Main ──────────────────────────────────────────────────────────────────────

def main(search_terms: list[str] | None = None) -> None:
    terms = search_terms or DEFAULT_SEARCH_TERMS

    print("Acquiring Kroger API token...")
    token = _get_token()

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            location_ids = _load_location_ids(cur)
    finally:
        pass    # keep connection open

    total_locations = len(location_ids)
    total_calls     = total_locations * len(terms)
    print(f"{total_locations} locations × {len(terms)} terms = {total_calls} API calls\n")

    call_count  = 0
    insert_count = 0

    for loc_idx, location_id in enumerate(location_ids, 1):
        print(f"[{loc_idx}/{total_locations}] location {location_id}")

        with conn:                  # one transaction per location
            with conn.cursor() as cur:
                for term in terms:
                    try:
                        products = _search_products(token, location_id, term)
                    except PermissionError:
                        print("  Token expired — refreshing...")
                        token    = _get_token()
                        products = _search_products(token, location_id, term)
                    except Exception as e:
                        print(f"  WARNING [{term}]: {e}")
                        continue

                    for p in products:
                        _load_product(cur, p, location_id)
                        insert_count += 1

                    call_count += 1
                    time.sleep(SLEEP_SEC)

        print(f"  {len(terms)} terms done — {insert_count} product-location rows so far")

    print(f"\nProduct load complete. {insert_count} rows inserted across {call_count} API calls.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Kroger product data into the warehouse.")
    parser.add_argument(
        "--terms", nargs="+", metavar="TERM",
        help="Search terms to use (overrides the default list).",
    )
    args = parser.parse_args()
    main(search_terms=args.terms)
