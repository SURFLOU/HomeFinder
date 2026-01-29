from pymongo import MongoClient
from typing import List, Dict
from datetime import datetime


class DBAnalytics:
    def __init__(self, client: MongoClient):
        self.client = client
        self.db = self.client["flatsdb"]
        self.collection = self.db["listings"]

    def get_current_unannounced(self) -> List[Dict]:
        return list(self.collection.find({
            "is_current": True,
            "is_announced": False
        }))

    def mark_announced(self, _id):
        self.collection.update_one(
            {"_id": _id},
            {"$set": {"is_announced": True}}
        )

    def get_previous_price(self, url: str):
        prev = self.collection.find_one(
            {
                "url": url,
                "is_current": False,
                "main_price": {"$ne": None}
            },
            sort=[("valid_from", -1)]
        )
        return prev["main_price"] if prev else None

    def process_announcements(self, send_new, send_price_change):
        current = self.get_current_unannounced()

        by_url = {}
        for doc in current:
            by_url.setdefault(doc["url"], []).append(doc)

        for url, docs in by_url.items():
            doc = docs[0]

            if doc.get("is_price_change"):
                send_price_change(doc)
                self.mark_announced(doc["_id"])
                continue

            send_new(doc)
            self.mark_announced(doc["_id"])

    def count_active_offers(self) -> int:
        return self.collection.count_documents({"is_current": True})

    def get_top_active(self, n: int) -> List[Dict]:
        return list(
            self.collection.find({"is_current": True})
            .sort("price_per_m2", 1)
            .limit(n)
        )
