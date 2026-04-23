import requests
import os
from dotenv import load_dotenv
load_dotenv()

client_id = os.getenv('KROGER_CLIENT_ID')
client_secret = os.getenv("KROGER_CLIENT_SECRET")

# Step 1 — Get access token
def get_access_token():
    response = requests.post(
        "https://api.kroger.com/v1/connect/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "scope": "product.compact"
        },
        auth=(client_id, client_secret)
    )
    return response.json()["access_token"]

# Step 2 — Use token in all subsequent calls
def get_headers(token):
    return {"Authorization": f"Bearer {token}"}

# Step 3 — Example: find a Kroger store near a zip code
def get_locations(token, zip_code):
    response = requests.get(
        "https://api.kroger.com/v1/locations",
        headers=get_headers(token),
        params={
            "filter.zipCode.near": zip_code,
            "filter.radiusInMiles": 10,
            "filter.limit": 5
        }
    )
    return response.json()

# Step 4 — Example: search products at a specific store
def search_products(token, location_id, search_term):
    response = requests.get(
        "https://api.kroger.com/v1/products",
        headers=get_headers(token),
        params={
            "filter.term": search_term,
            "filter.locationId": location_id,
            "filter.limit": 10
        }
    )
    return response.json()


# --- Run it ---
token = get_access_token()

locations = get_locations(token, "77019")  # Chicago zip, change to yours
print(locations)

# location_id = locations["data"][0]["locationId"]
# products = search_products(token, location_id, "whole milk")
# print(products)
