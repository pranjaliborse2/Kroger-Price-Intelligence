-- ============================================================
-- LOCATION API DATA MODEL
-- Database: kroger_price_intel
-- Schema: public
--
-- Tables (normalized):
--   dim_locations           - Store master (one row per locationId)
--   dim_store_hours         - Daily open/close times per store
--   dim_departments         - Department reference (lookup)
--   dim_store_departments   - Store × department mapping + contact info
--   dim_dept_geo            - Departments with their own address/geolocation
--                             (e.g. offsite Pickup, standalone Pharmacy)
--
-- Source: Kroger Location API  GET /v1/locations
-- Geographic scope: Houston, TX (Inner Loop + Greater Houston)
-- ============================================================


-- ------------------------------------------------------------
-- dim_locations
-- One row per unique Kroger store.
-- Address, geolocation, and timezone are stored flat for
-- easy joins with fact_product_location_prices.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id     VARCHAR(20)     PRIMARY KEY,        -- e.g. "03400017"
    store_number    VARCHAR(10),
    division_number VARCHAR(10),
    chain           VARCHAR(50),                        -- e.g. "KROGER"
    name            VARCHAR(200),                       -- e.g. "Kroger - Studemont"
    phone           VARCHAR(20),
    -- Address
    address_line1   VARCHAR(300),
    city            VARCHAR(100),
    state           CHAR(2),
    zip_code        VARCHAR(10),
    county          VARCHAR(100),
    -- Geolocation
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    lat_lng         VARCHAR(50),                        -- formatted "lat,lng" string from API
    -- Hours / timezone
    timezone        VARCHAR(50),                        -- e.g. "America/Chicago"
    gmt_offset      VARCHAR(100),                       -- e.g. "(UTC-06:00) Central Time (US Canada)"
    open24          BOOLEAN,
    -- Market segmentation (Houston-specific)
    market_segment  VARCHAR(50),                        -- Inner Loop | Greater Houston
    -- Metadata
    ingested_at     TIMESTAMP WITH TIME ZONE    DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_locations_zip    ON dim_locations (zip_code);
CREATE INDEX IF NOT EXISTS idx_locations_city   ON dim_locations (city, state);


-- ------------------------------------------------------------
-- dim_store_hours
-- Normalized daily hours per store.
-- One row per (location_id, day_of_week).
-- Stores that are open24=true will have open_time / close_time NULL.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_store_hours (
    location_id     VARCHAR(20)     NOT NULL    REFERENCES dim_locations(location_id),
    day_of_week     VARCHAR(10)     NOT NULL,   -- monday | tuesday | ... | sunday
    open_time       TIME,                       -- NULL if open24
    close_time      TIME,                       -- NULL if open24
    open24          BOOLEAN         DEFAULT FALSE,
    PRIMARY KEY (location_id, day_of_week)
);


-- ------------------------------------------------------------
-- dim_departments
-- Reference / lookup table for department types.
-- One row per unique (departmentId, name) pair across all stores.
-- e.g. "94" → "Pickup", "48" → "Starbucks"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_departments (
    department_id   VARCHAR(10)     PRIMARY KEY,
    name            VARCHAR(200)    NOT NULL
);


-- ------------------------------------------------------------
-- dim_store_departments
-- Which departments are present at each store, with optional
-- contact phone and offsite flag.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_store_departments (
    location_id     VARCHAR(20)     NOT NULL    REFERENCES dim_locations(location_id),
    department_id   VARCHAR(10)     NOT NULL    REFERENCES dim_departments(department_id),
    phone           VARCHAR(20),
    offsite         BOOLEAN,
    PRIMARY KEY (location_id, department_id)
);


-- ------------------------------------------------------------
-- dim_dept_geo
-- Some departments (e.g. offsite Pickup, standalone Pharmacy)
-- have their own address and/or geolocation distinct from the
-- parent store.  Sourced from departments[].address and
-- departments[].geolocation in the API response.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_dept_geo (
    location_id     VARCHAR(20)     NOT NULL    REFERENCES dim_locations(location_id),
    department_id   VARCHAR(10)     NOT NULL    REFERENCES dim_departments(department_id),
    address_line1   VARCHAR(300),
    city            VARCHAR(100),
    state           CHAR(2),
    zip_code        VARCHAR(10),
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    lat_lng         VARCHAR(50),
    PRIMARY KEY (location_id, department_id)
);
