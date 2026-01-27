from scraper import Scraper
from dbhandler import DBHandler
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging

LOG_FILE = os.path.join(os.path.dirname(__file__), "homefinder.log")
logger = logging.getLogger(__name__)
root_logger = logging.getLogger()
if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    root_logger.addHandler(fh)
    root_logger.setLevel(logging.INFO)

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
logger.info("Connected to MongoDB")
scraper = Scraper()
db_handler = DBHandler(client)
total_listings = scraper.get_number_of_listings(scraper.fetch_listings(page=1))
listings = scraper.extract_listing()
logger.info(f"Found {len(listings)} listings:\n")
for listing in listings:
    db_handler.upsert_scd2(listing, initial_append=False)
logger.info("Upsert completed for fetched listings")