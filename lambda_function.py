import json
from scraper import Scraper
from dbhandler import DBHandler
from pymongo import MongoClient
import os

BASE_URL = "https://www.otodom.pl"
SEARCH_PATH = (
    "/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji"
    "?limit=72"
    "&ownerTypeSingleSelect=ALL"
    "&priceMax=750000"
    "&areaMin=40"
    "&locations=%5Bmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%2Fochota%2C"
    "mazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%2Fwlochy%5D"
    "&extras=%5BGARAGE%5D"
    "&by=DEFAULT"
    "&direction=DESC"
    "&page={page}"
)

def lambda_handler(event, context):
    print("Lambda started")

    url = f"{BASE_URL}{SEARCH_PATH.format(page='1')}"
    print("Search URL:", url)

    os.getenv('MONGO_URI') 
    if not MONGO_URI:
        print("ERROR: MONGO_URI not set")
        return {
            "statusCode": 500,
            "body": "Missing MONGO_URI"
        }

    client = MongoClient(MONGO_URI)
    print("Connected to MongoDB")

    scraper = Scraper()
    db_handler = DBHandler(client)

    print("Fetching first page to get total listings")
    first_page = scraper.fetch_listings(page=1)
    total_listings = scraper.get_number_of_listings(first_page)
    print("Total listings reported:", total_listings)

    print("Extracting listings")
    listings = scraper.extract_listing()
    print(f"Found {len(listings)} listings")

    inserted = 0
    for listing in listings:
        if db_handler.upsert_scd2(listing, initial_append=False):
            inserted += 1

    print(f"Upsert completed. New/changed records: {inserted}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "success",
            "total_listings": len(listings),
            "inserted": inserted
        })
    }
