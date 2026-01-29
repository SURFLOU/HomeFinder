from datetime import datetime
from pymongo import MongoClient
from typing import Dict, Optional
from pydantic import BaseModel, Field
import logging
import re

logger = logging.getLogger(__name__)

class ListingBase(BaseModel):
    url: str
    main_price: Optional[int]
    price_per_m2: Optional[int]
    short_description: Optional[str]
    description: Optional[str]
    street: Optional[str]
    subdistrict: Optional[str]
    district: Optional[str]
    number_of_rooms: Optional[int]
    area_m2: Optional[float]
    floor_number: Optional[int]


class ListingSCD2(ListingBase):
    valid_from: datetime
    valid_to: Optional[datetime] = None
    is_current: bool = True
    is_announced: bool = Field(default=False)
    is_price_change: bool = Field(default=False)  


class DBHandler:
    def __init__(self, client: MongoClient):
        self.client = client
        self.db = self.client["flatsdb"]
        self.collection = self.db["listings"]

        self.collection.create_index(
            [("url", 1), ("is_current", 1)],
            unique=True,
            partialFilterExpression={"is_current": True}
        )
        logger.info("Ensured unique partial index on (url, is_current)")

    def normalize_url(self, url: str) -> str:
        """
        Normalize Otodom URLs so:
        - /hpr/pl/oferta/... -> /pl/oferta/...
        - remove query params
        """
        if not url:
            return url

        url = re.sub(r"/hpr/", "/", url)
        url = url.split("?")[0]
        return url.rstrip("/")

    def _business_fields(self, doc: Dict) -> Dict:
        """Remove SCD & announcement metadata before comparison"""
        ignore = {
            "_id",
            "valid_from",
            "valid_to",
            "is_current",
            "is_announced",
            "is_price_change",
        }
        return {k: v for k, v in doc.items() if k not in ignore}

    def upsert_scd2(self, raw_listing: Dict, initial_append: bool = False) -> bool:
        now = datetime.utcnow()

        raw_listing = raw_listing.copy()
        raw_listing["url"] = self.normalize_url(raw_listing["url"])

        listing_base = ListingBase(**raw_listing)

        current = self.collection.find_one({
            "url": listing_base.url,
            "is_current": True
        })

        if not current:
            new_doc = ListingSCD2(
                **listing_base.dict(),
                valid_from=now,
                is_current=True,
                is_announced=initial_append,
                is_price_change=False
            ).dict()
            self.collection.insert_one(new_doc)
            return True

        same_business = self._business_fields(current) == self._business_fields(raw_listing)
        if same_business:
            return False

        prev_price = current.get("main_price")
        curr_price = listing_base.main_price

        prev_ppm2 = current.get("price_per_m2")
        curr_ppm2 = listing_base.price_per_m2

        value_to_null = (
            (prev_price is not None and curr_price is None) or
            (prev_ppm2 is not None and curr_ppm2 is None)
        )

        if value_to_null:
            return False

        is_null_to_value = (
            prev_price == curr_price and
            prev_ppm2 is None and
            curr_ppm2 is not None
        )

        is_real_price_change = (
            prev_price is not None and
            curr_price is not None and
            prev_price != curr_price
        )

        self.collection.update_one(
            {"_id": current["_id"]},
            {"$set": {"valid_to": now, "is_current": False}}
        )

        new_doc = ListingSCD2(
            **listing_base.dict(),
            valid_from=now,
            is_current=True,
            is_announced=is_null_to_value,
            is_price_change=is_real_price_change
        ).dict()

        self.collection.insert_one(new_doc)
        return True