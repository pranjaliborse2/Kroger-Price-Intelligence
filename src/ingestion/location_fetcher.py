"""
location_fetcher.py
Fetches all Kroger store locations for Houston, TX across both Inner Loop
and Greater Houston zipcodes, then loads them into the kroger_price_intel
PostgreSQL database.

Tables populated:
    dim_locations, dim_store_hours, dim_departments,
    dim_store_departments, dim_dept_geo

Run from project root:
    python -m src.ingestion.location_fetcher
"""
import json
import os
import time
import configparser

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

_HERE        = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(_HERE, "..", "..", "config")
DAYS         = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
SLEEP_SEC    = 0.15   # pause between API calls to stay under rate limits


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


def _fetch_locations(token: str, zip_code: str, radius: int, limit: int) -> list[dict]:
    resp = requests.get(
        "https://api.kroger.com/v1/locations",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "filter.zipCode.near": zip_code,
            "filter.radiusInMiles": radius,
            "filter.limit": limit,
        },
        timeout=10,
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


def _upsert_locations(cur, locations: list[dict], segment: str) -> None:
    sql = """
        INSERT INTO dim_locations (
            location_id, store_number, division_number, chain, name, phone,
            address_line1, city, state, zip_code, county,
            latitude, longitude, lat_lng,
            timezone, gmt_offset, open24, market_segment
        ) VALUES %s
        ON CONFLICT (location_id) DO UPDATE SET
            name           = EXCLUDED.name,
            phone          = EXCLUDED.phone,
            open24         = EXCLUDED.open24,
            updated_at     = NOW();
    """
    rows = []
    for loc in locations:
        addr = loc.get("address", {})
        geo  = loc.get("geolocation", {})
        hrs  = loc.get("hours", {})
        rows.append((
            loc["locationId"],
            loc.get("storeNumber"),
            loc.get("divisionNumber"),
            loc.get("chain"),
            loc.get("name"),
            loc.get("phone"),
            addr.get("addressLine1"),
            addr.get("city"),
            addr.get("state"),
            addr.get("zipCode"),
            addr.get("county"),
            geo.get("latitude"),
            geo.get("longitude"),
            geo.get("latLng"),
            hrs.get("timezone"),
            hrs.get("gmtOffset"),
            hrs.get("open24", False),
            segment,
        ))
    psycopg2.extras.execute_values(cur, sql, rows)


def _upsert_store_hours(cur, loc: dict) -> None:
    sql = """
        INSERT INTO dim_store_hours (location_id, day_of_week, open_time, close_time, open24)
        VALUES %s
        ON CONFLICT (location_id, day_of_week) DO UPDATE SET
            open_time  = EXCLUDED.open_time,
            close_time = EXCLUDED.close_time,
            open24     = EXCLUDED.open24;
    """
    hrs  = loc.get("hours", {})
    rows = []
    for day in DAYS:
        day_info = hrs.get(day) or {}
        rows.append((
            loc["locationId"],
            day,
            day_info.get("open"),
            day_info.get("close"),
            day_info.get("open24", False),
        ))
    psycopg2.extras.execute_values(cur, sql, rows)


def _upsert_departments(cur, departments: list[dict]) -> None:
    sql = """
        INSERT INTO dim_departments (department_id, name)
        VALUES %s
        ON CONFLICT (department_id) DO NOTHING;
    """
    rows = [
        (d["departmentId"], d["name"])
        for d in departments
        if d.get("departmentId") and d.get("name")
    ]
    if rows:
        psycopg2.extras.execute_values(cur, sql, rows)


def _upsert_store_departments(cur, location_id: str, departments: list[dict]) -> None:
    sql = """
        INSERT INTO dim_store_departments (location_id, department_id, phone, offsite)
        VALUES %s
        ON CONFLICT (location_id, department_id) DO UPDATE SET
            phone   = EXCLUDED.phone,
            offsite = EXCLUDED.offsite;
    """
    rows = [
        (location_id, d["departmentId"], d.get("phone"), d.get("offsite"))
        for d in departments
        if d.get("departmentId")
    ]
    if rows:
        psycopg2.extras.execute_values(cur, sql, rows)


def _upsert_dept_geo(cur, location_id: str, departments: list[dict]) -> None:
    sql = """
        INSERT INTO dim_dept_geo (
            location_id, department_id,
            address_line1, city, state, zip_code,
            latitude, longitude, lat_lng
        ) VALUES %s
        ON CONFLICT (location_id, department_id) DO UPDATE SET
            address_line1 = EXCLUDED.address_line1,
            latitude      = EXCLUDED.latitude,
            longitude     = EXCLUDED.longitude;
    """
    rows = []
    for d in departments:
        dept_id = d.get("departmentId")
        addr    = d.get("address") or {}
        geo     = d.get("geolocation") or {}
        if dept_id and (addr or geo):
            rows.append((
                location_id, dept_id,
                addr.get("addressLine1"),
                addr.get("city"),
                addr.get("state"),
                addr.get("zipCode"),
                geo.get("latitude"),
                geo.get("longitude"),
                geo.get("latLng"),
            ))
    if rows:
        psycopg2.extras.execute_values(cur, sql, rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    config = configparser.ConfigParser()
    config.read(os.path.join(CONFIG_PATH, "config.ini"))

    with open(os.path.join(CONFIG_PATH, "houston_zipcodes.json")) as f:
        zipcodes = json.load(f)

    segments = {
        "Inner Loop": {
            "zipcodes": zipcodes["Inner Loop"],
            "radius":   int(config["Location API Radius Param"]["zipcode_radius_InnerLoop"]),
            "limit":    int(config["Location API Filter Param"]["Innerloop_filter"]),
        },
        "Greater Houston": {
            "zipcodes": zipcodes["Greater Houston"],
            "radius":   int(config["Location API Radius Param"]["zipcode_radius_GreaterHou"]),
            "limit":    int(config["Location API Filter Param"]["GreaterHou_filter"]),
        },
    }

    print("Acquiring Kroger API token...")
    token = _get_token()

    # Collect unique locations; first segment to find a store "owns" it.
    seen: dict[str, tuple[dict, str]] = {}  # locationId → (loc_dict, segment)

    for segment, cfg in segments.items():
        zips   = cfg["zipcodes"]
        radius = cfg["radius"]
        limit  = cfg["limit"]
        print(f"\n[{segment}] {len(zips)} zipcodes, radius={radius}mi, limit={limit}")

        for i, zipcode in enumerate(zips, 1):
            try:
                locs = _fetch_locations(token, zipcode, radius, limit)
            except PermissionError:
                print("  Token expired — refreshing...")
                token = _get_token()
                locs  = _fetch_locations(token, zipcode, radius, limit)
            except Exception as e:
                print(f"  WARNING zip {zipcode}: {e}")
                continue

            for loc in locs:
                loc_id = loc["locationId"]
                if loc_id not in seen:
                    seen[loc_id] = (loc, segment)

            if i % 20 == 0 or i == len(zips):
                print(f"  {i}/{len(zips)} zipcodes — {len(seen)} unique stores")
            time.sleep(SLEEP_SEC)

    print(f"\nTotal unique locations found: {len(seen)}")

    # ── Load into database ────────────────────────────────────────────────────
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Bulk upsert locations grouped by segment
                by_segment: dict[str, list[dict]] = {"Inner Loop": [], "Greater Houston": []}
                for loc, seg in seen.values():
                    by_segment[seg].append(loc)

                for seg, locs in by_segment.items():
                    if locs:
                        print(f"Upserting {len(locs)} locations [{seg}]...")
                        _upsert_locations(cur, locs, seg)

                # Department reference table (global, all stores)
                all_depts = [d for loc, _ in seen.values() for d in loc.get("departments", [])]
                print(f"Upserting {len(set(d.get('departmentId') for d in all_depts))} departments...")
                _upsert_departments(cur, all_depts)

                # Per-location: hours, store↔department mapping, department geo
                print("Upserting store hours and department assignments...")
                for loc, _ in seen.values():
                    loc_id = loc["locationId"]
                    depts  = loc.get("departments", [])
                    _upsert_store_hours(cur, loc)
                    _upsert_store_departments(cur, loc_id, depts)
                    _upsert_dept_geo(cur, loc_id, depts)

        print("Location load complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
