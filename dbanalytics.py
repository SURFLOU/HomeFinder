from datetime import datetime, timedelta
from pymongo import MongoClient
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class DBAnalytics:
    def __init__(self, client: MongoClient):
        self.client = client
        self.db = self.client["flatsdb"]
        self.collection = self.db["listings"]
        logger.debug("DBAnalytics initialized for database 'flatsdb'.")

    def count_active_offers(self) -> int:
        return self.collection.count_documents({
            "is_current": True
        })

    def price_changes_today(self) -> List[Dict]:
        start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)

        pipeline = [
            {
                "$match": {
                    "is_current": True,
                    "valid_from": {
                        "$gte": start,
                        "$lt": end
                    }
                }
            },
            {
                "$lookup": {
                    "from": "listings",
                    "let": {"url": "$url", "from": "$valid_from"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$url", "$$url"]},
                                        {"$lt": ["$valid_from", "$$from"]}
                                    ]
                                }
                            }
                        },
                        {"$sort": {"valid_from": -1}},
                        {"$limit": 1}
                    ],
                    "as": "previous"
                }
            },
            {
                "$unwind": "$previous"
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": ["$main_price", "$previous.main_price"]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "url": 1,
                    "old_price": "$previous.main_price",
                    "new_price": "$main_price",
                    "price_diff": {
                        "$subtract": ["$main_price", "$previous.main_price"]
                    },
                    "effective_from": "$valid_from",
                    "district": 1,
                    "subdistrict": 1,
                    "area_m2": 1
                }
            }
        ]

        return list(self.collection.aggregate(pipeline))
    
    def count_active_offers(self) -> int:
        return self.collection.count_documents({"is_current": True})

    def get_new_unannounced_flats(self) -> List[Dict]:
        return list(self.collection.find({
            "is_current": True,
            "is_announced": False
        }))

    def mark_as_announced(self, url: str):
        self.collection.update_one(
            {"url": url, "is_current": True},
            {"$set": {"is_announced": True}}
        )

    def get_top_active(self, n: int) -> List[Dict]:
        return list(
            self.collection.find(
                {"is_current": True},
                {"_id": 0}
            )
            .sort("main_price", 1)  
            .limit(n)
        )
