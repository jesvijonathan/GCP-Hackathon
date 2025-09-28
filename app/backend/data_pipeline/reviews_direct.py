#!/usr/bin/env python3
# data_pipeline/reviews_direct.py

import random, uuid, datetime as dt, math
from typing import Any, Dict, List, Optional
from .base import ts_of_record, make_doc_key, upsert_many

def generate_stream(mongo_uri: str, db_name: str, merchant: str, n_reviews: int, n_products: int, merchant_score: float, trend_plan: List[Dict[str, Any]], seed: Optional[int]=None):
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    start_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=365*2)
    products = [{"product_id": f"p_{uuid.uuid4().hex[:8]}", "product_name": f"{merchant} Product {chr(ord('A') + (i % 26))}", "sku": f"SKU-{uuid.uuid4().hex[:6].upper()}", "category": rng.choice(["Tools","Garden","Decor","Lighting","Plumbing","Electrical","Storage","Outdoor","Paint","Flooring"])} for i in range(max(1, n_products))]
    docs: List[Dict[str, Any]] = []
    for i in range(n_reviews):
        created_dt = start_dt + dt.timedelta(minutes=rng.randint(0, 365*2*24*60))
        prod = rng.choice(products)
        rating = rng.randint(1,5)
        sentiment = "positive" if rating>=4 else ("negative" if rating<=2 else "neutral")
        verified = rng.random() < 0.86
        order_id = f"o_{uuid.uuid4().hex[:10]}" if verified and rng.random() < 0.95 else None
        pd = None
        if order_id:
            days_before = rng.randint(2,60)
            pd = (created_dt - dt.timedelta(days=days_before, hours=rng.randint(0,23))).replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z")
        rec = {
            "review_id": f"r_{uuid.uuid4().hex[:10]}",
            "merchant": merchant,
            "product_id": prod["product_id"],
            "product_name": prod["product_name"],
            "sku": prod["sku"],
            "user_id": f"u_{uuid.uuid4().hex[:10]}",
            "username": f"user_{uuid.uuid4().hex[:6]}",
            "title": rng.choice(["Excellent!","Very good","It's okay","Not great","Terrible"]),
            "body": rng.choice(["Solid product.","Happy overall.","Mixed feelings.","Disappointed.","Would recommend."]),
            "rating": rating,
            "sentiment_score": 0.0,
            "sentiment_label": sentiment,
            "created_at": created_dt.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z"),
            "verified_purchase": verified,
            "country": rng.choice(["GB","US","DE","FR","ES","IT"]),
            "language": rng.choice(["en","de","fr","es","it"]),
            "platform": rng.choice(["web","ios","android"]),
            "helpful_votes": rng.randint(0, 100),
            "unhelpful_votes": rng.randint(0, 20),
            "views": rng.randint(0, 50000),
            "images": [],
            "tags": [],
            "merchant_response": None,
            "order_id": order_id,
            "purchase_date": pd,
            "return_status": rng.choice(["none","returned","exchanged"]),
            "event_label": None,
        }
        ts = ts_of_record("reviews", rec)
        rec["ts"] = ts
        rec["dt"] = dt.datetime.utcfromtimestamp(ts).isoformat()+"Z" if ts else None
        rec["doc_key"] = make_doc_key(merchant, "reviews", rec)
        docs.append(rec)
    upsert_many("reviews", docs)
