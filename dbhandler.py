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
        """
        Returns:
            True  -> new SCD row inserted
            False -> skipped (no change)
        """

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


        if self._business_fields(current) == self._business_fields(raw_listing):
            return False

        price_changed = current.get("main_price") != listing_base.main_price

        self.collection.update_one(
            {"_id": current["_id"]},
            {"$set": {"valid_to": now, "is_current": False}}
        )

        new_doc = ListingSCD2(
            **listing_base.dict(),
            valid_from=now,
            valid_to=None,
            is_current=True,
            is_announced=False,          
            is_price_change=price_changed
        ).dict()

        self.collection.insert_one(new_doc)
        return True
