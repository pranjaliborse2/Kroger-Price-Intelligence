-- ============================================================
-- PRODUCT API DATA MODEL
-- Database: kroger_price_intel
-- Schema: public
--
-- Tables (normalized):
--   dim_products                  - Product master (one row per UPC)
--   dim_product_categories        - Product → category mapping
--   dim_product_declarations      - Manufacturer declarations (Kosher, GMO Free, etc.)
--   dim_product_allergens         - Allergen containment info
--   dim_product_images            - Image URLs per perspective/size
--   dim_product_nutrition         - Nutrition block (ingredients, serving size)
--   dim_product_nutrients         - Individual nutrient rows
--   fact_product_location_prices  - Price + availability per product × location (fact)
--   dim_product_aisle_locations   - Aisle/bay placement per product × location
--
-- Views (for app/analytics):
--   vw_product_price_stats        - Price statistics aggregated across locations
--   vw_product_location_detail    - Per-location price + fulfillment detail
-- ============================================================


-- ------------------------------------------------------------
-- dim_products
-- One row per unique UPC / productId.
-- Columns sourced from the product-level fields in the API response.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    product_id              VARCHAR(20)     PRIMARY KEY,    -- UPC == productId
    brand                   TEXT,
    description             TEXT,
    country_origin          TEXT,
    snap_eligible           BOOLEAN,
    receipt_description     TEXT,
    non_gmo                 BOOLEAN,
    non_gmo_claim_name      TEXT,
    organic_claim_name      TEXT,
    certified_for_passover  BOOLEAN,
    hypoallergenic          BOOLEAN,
    temperature_indicator   TEXT,                           -- Ambient | Refrigerated | Frozen
    heat_sensitive          BOOLEAN,
    item_depth              NUMERIC(8, 2),                  -- inches
    item_height             NUMERIC(8, 2),
    item_width              NUMERIC(8, 2),
    gross_weight            TEXT,                           -- raw string e.g. "4.58 [lb_av]"
    net_weight              TEXT,
    avg_rating              NUMERIC(3, 2),
    total_review_count      INTEGER,
    allergens_description   TEXT,
    product_page_uri        TEXT,
    ingested_at             TIMESTAMP WITH TIME ZONE    DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE    DEFAULT NOW()
);


-- ------------------------------------------------------------
-- dim_product_categories
-- One product can belong to multiple categories
-- e.g. ["Dairy", "Natural & Organic"]
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_categories (
    product_id  VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id),
    category    VARCHAR(200)    NOT NULL,
    PRIMARY KEY (product_id, category)
);


-- ------------------------------------------------------------
-- dim_product_declarations
-- Manufacturer declarations such as "GMO Free", "Kosher", "Vegan",
-- "Dairy Free", etc.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_declarations (
    product_id  VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id),
    declaration VARCHAR(200)    NOT NULL,
    PRIMARY KEY (product_id, declaration)
);


-- ------------------------------------------------------------
-- dim_product_allergens
-- Per-allergen containment level (Contains | May Contain)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_allergens (
    product_id              VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id),
    allergen_name           VARCHAR(200)    NOT NULL,
    level_of_containment    VARCHAR(50),                -- "Contains" | "May Contain"
    PRIMARY KEY (product_id, allergen_name)
);


-- ------------------------------------------------------------
-- dim_product_images
-- One row per (product, perspective, size) combination.
-- perspective: front | back | left | right | top | bottom
-- size:        xlarge | large | medium | small | thumbnail
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_images (
    id          SERIAL          PRIMARY KEY,
    product_id  VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id),
    perspective VARCHAR(50)     NOT NULL,
    featured    BOOLEAN         DEFAULT FALSE,
    size        VARCHAR(20)     NOT NULL,
    url         TEXT            NOT NULL,
    UNIQUE (product_id, perspective, size)
);


-- ------------------------------------------------------------
-- dim_product_nutrition
-- One row per product (the API returns a single nutrition block).
-- Serving size and ingredient statement live here.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_nutrition (
    id                      SERIAL          PRIMARY KEY,
    product_id              VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id)    UNIQUE,
    ingredient_statement    TEXT,
    serving_size_quantity   NUMERIC(8, 2),
    serving_size_uom_code   VARCHAR(10),                -- G21 = Cup US, GRM = Gram, etc.
    serving_size_uom_name   VARCHAR(50),
    daily_value_reference   TEXT            -- disclaimer text
);


-- ------------------------------------------------------------
-- dim_product_nutrients
-- Individual nutrient rows within a nutrition block.
-- Linked to dim_product_nutrition via nutrition_id.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_nutrients (
    id                      SERIAL          PRIMARY KEY,
    nutrition_id            INTEGER         NOT NULL    REFERENCES dim_product_nutrition(id),
    nutrient_code           VARCHAR(20),                -- e.g. FAT, SUGAR, CHOL-, FASAT
    display_name            VARCHAR(100),
    description             TEXT,
    quantity                NUMERIC,
    percent_daily_intake    NUMERIC,
    uom_code                VARCHAR(10),                -- GRM, MGM, MC, etc.
    uom_name                VARCHAR(50),
    precision_code          VARCHAR(50)                 -- APPROXIMATELY | EXACTLY
);


-- ------------------------------------------------------------
-- fact_product_location_prices
-- Central fact table: one row per (product × location × fetch).
-- Captures price, promo, stock level, and fulfillment modes.
-- A new row is inserted on each data refresh so price history
-- is preserved.  Use fetched_at to get the latest snapshot.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_product_location_prices (
    id                          SERIAL              PRIMARY KEY,
    product_id                  VARCHAR(20)         NOT NULL    REFERENCES dim_products(product_id),
    location_id                 VARCHAR(20)         NOT NULL    REFERENCES dim_locations(location_id),
    item_id                     VARCHAR(20),
    size                        VARCHAR(50),                    -- e.g. "2 lb", "64 fl oz"
    sold_by                     VARCHAR(20),                    -- UNIT | WEIGHT
    regular_price               NUMERIC(10, 2),
    promo_price                 NUMERIC(10, 2),                 -- NULL if no active promo
    price_effective_date        TIMESTAMP WITH TIME ZONE,
    price_expiration_date       TIMESTAMP WITH TIME ZONE,
    stock_level                 VARCHAR(50),                    -- HIGH | LOW | TEMPORARILY_OUT_OF_STOCK
    fulfillment_curbside        BOOLEAN,
    fulfillment_delivery        BOOLEAN,
    fulfillment_in_store        BOOLEAN,
    fulfillment_ship_to_home    BOOLEAN,
    fetched_at                  TIMESTAMP WITH TIME ZONE    DEFAULT NOW()
);

-- Indexes to support common query patterns
CREATE INDEX IF NOT EXISTS idx_fplp_product        ON fact_product_location_prices (product_id);
CREATE INDEX IF NOT EXISTS idx_fplp_location       ON fact_product_location_prices (location_id);
CREATE INDEX IF NOT EXISTS idx_fplp_fetched_at     ON fact_product_location_prices (fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_fplp_product_loc    ON fact_product_location_prices (product_id, location_id);


-- ------------------------------------------------------------
-- dim_product_aisle_locations
-- Where a product physically sits inside a specific store.
-- Sourced from aisleLocations[] in the API response.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_aisle_locations (
    product_id          VARCHAR(20)     NOT NULL    REFERENCES dim_products(product_id),
    location_id         VARCHAR(20)     NOT NULL    REFERENCES dim_locations(location_id),
    bay_number          VARCHAR(10),
    aisle_description   VARCHAR(200),               -- e.g. "AISLE 6"
    PRIMARY KEY (product_id, location_id)
);




-- ============================================================
-- ANALYTICAL VIEWS
-- ============================================================

-- ------------------------------------------------------------
-- vw_product_price_stats
-- Aggregates min / max / avg / stddev of regular price across
-- all locations for the latest fetch of each product.
-- Primary query target for the "find cheapest location" app.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_product_price_stats AS
WITH latest_prices AS (
    -- Keep only the most recent fetch per (product, location)
    SELECT DISTINCT ON (product_id, location_id)
        product_id,
        location_id,
        regular_price,
        promo_price,
        stock_level,
        fulfillment_in_store,
        fulfillment_curbside,
        fulfillment_delivery
    FROM fact_product_location_prices
    ORDER BY product_id, location_id, fetched_at DESC
)
SELECT
    p.product_id,
    p.brand,
    p.description,
    p.temperature_indicator,
    p.avg_rating,
    p.total_review_count,
    COUNT(DISTINCT lp.location_id)              AS locations_available,
    MIN(lp.regular_price)                       AS min_price,
    MAX(lp.regular_price)                       AS max_price,
    ROUND(AVG(lp.regular_price), 2)             AS avg_price,
    ROUND(STDDEV(lp.regular_price)::NUMERIC, 2) AS price_stddev,
    MIN(lp.promo_price)                         AS min_promo_price,
    MAX(lp.promo_price)                         AS max_promo_price,
    COUNT(*) FILTER (WHERE lp.promo_price IS NOT NULL) AS locations_with_promo
FROM dim_products p
JOIN latest_prices lp ON p.product_id = lp.product_id
GROUP BY
    p.product_id,
    p.brand,
    p.description,
    p.temperature_indicator,
    p.avg_rating,
    p.total_review_count;


-- ------------------------------------------------------------
-- vw_product_location_detail
-- Flat view joining product master + location master + latest
-- price/availability.  The app can query this view directly
-- after identifying matching products.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_product_location_detail AS
WITH latest_prices AS (
    SELECT DISTINCT ON (product_id, location_id)
        product_id,
        location_id,
        item_id,
        size,
        sold_by,
        regular_price,
        promo_price,
        stock_level,
        fulfillment_curbside,
        fulfillment_delivery,
        fulfillment_in_store,
        fulfillment_ship_to_home,
        fetched_at
    FROM fact_product_location_prices
    ORDER BY product_id, location_id, fetched_at DESC
)
SELECT
    -- Product fields
    p.product_id,
    p.brand,
    p.description                           AS product_description,
    p.snap_eligible,
    p.temperature_indicator,
    -- Price fields
    lp.size,
    lp.sold_by,
    lp.regular_price,
    lp.promo_price,
    lp.stock_level,
    lp.fulfillment_curbside,
    lp.fulfillment_delivery,
    lp.fulfillment_in_store,
    lp.fulfillment_ship_to_home,
    lp.fetched_at                           AS price_as_of,
    -- Location fields
    l.location_id,
    l.name                                  AS store_name,
    l.address_line1,
    l.city,
    l.state,
    l.zip_code,
    l.county,
    l.latitude,
    l.longitude,
    l.chain
FROM dim_products p
JOIN latest_prices lp   ON p.product_id    = lp.product_id
JOIN dim_locations l    ON lp.location_id  = l.location_id;
