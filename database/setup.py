"""
database/setup.py
Creates the kroger_price_intel database (if it doesn't exist) and applies
both schema files in the correct order.

Run from project root:
    python -m database.setup
"""
import os
import sys

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

load_dotenv()

DB_NAME  = os.getenv("DB_NAME", "kroger_price_intel")
DB_HOST  = os.getenv("DB_HOST", "localhost")
DB_PORT  = int(os.getenv("DB_PORT", "5432"))
DB_USER  = os.getenv("DB_USER")
DB_PASS  = os.getenv("DB_PASSWORD")

_HERE         = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FILES  = [
    os.path.join(_HERE, "schemas", "location_schema.sql"),
    os.path.join(_HERE, "schemas", "product_schema.sql"),
]


def _connect(dbname: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=dbname, user=DB_USER, password=DB_PASS,
    )


def create_database() -> None:
    """Connect to the default 'postgres' database and create kroger_price_intel."""
    conn = _connect("postgres")
    conn.autocommit = True          # CREATE DATABASE must run outside a transaction
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        if cur.fetchone():
            print(f"Database '{DB_NAME}' already exists — skipping creation.")
        else:
            cur.execute(f'CREATE DATABASE "{DB_NAME}";')
            print(f"Database '{DB_NAME}' created.")
    conn.close()


def apply_schemas() -> None:
    """Run each schema SQL file against kroger_price_intel."""
    conn = _connect(DB_NAME)
    try:
        with conn:
            with conn.cursor() as cur:
                for path in SCHEMA_FILES:
                    print(f"Applying {os.path.basename(path)}...")
                    with open(path, encoding="utf-8") as f:
                        cur.execute(f.read())
        print("All schemas applied.")
    finally:
        conn.close()


if __name__ == "__main__":
    print(f"Setting up PostgreSQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}\n")
    try:
        create_database()
        apply_schemas()
        print("\nSetup complete. You can now run the ingestion scripts.")
    except psycopg2.OperationalError as e:
        print(f"\nERROR: Could not connect to PostgreSQL.\n{e}")
        print("Check that PostgreSQL is running and your .env credentials are correct.")
        sys.exit(1)
