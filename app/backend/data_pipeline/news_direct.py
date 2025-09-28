#!/usr/bin/env python3
# data_pipeline/news_direct.py

import random, uuid, datetime as dt, math
from typing import Any, Dict, List, Optional
from .base import ts_of_record, make_doc_key, upsert_many

def build_named_entities(merchant: str, rng: random.Random) -> Dict[str, List[str]]:
    orgs = [merchant, "CompetitorCo", "PartnerLabs", "RegulatoryBody", "IndustryGroup"]
    persons = [f"{rng.choice(['Alex','Sam','Chris','Jordan','Taylor','Ava','Noah'])} {rng.choice(['Smith','Brown','Taylor','Wilson','King'])}" for _ in range(3)]
    locs = rng.sample(["London","New York","Paris","Berlin","Manchester","Dublin","Madrid","Milan"], k=3)
    return {"organizations": list(dict.fromkeys(orgs))[:5], "persons": persons, "locations": locs}

def compute_sentiment_and_risk(rng: random.Random) -> (float, str, float):
    score = max(-1.0, min(1.0, rng.gauss(0.1, 0.45)))
    label = "negative" if score <= -0.2 else ("positive" if score >= 0.2 else "neutral")
    risk = round(100.0 * (1.0 - (score + 1.0) / 2.0), 2)
    return score, label, risk

def generate_stream(mongo_uri: str, db_name: str, merchant: str, n_articles: int, trend_plan: List[Dict[str, Any]], seed: Optional[int]=None):
    random.seed(seed)
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    start_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=365*2)
    docs: List[Dict[str, Any]] = []
    for i in range(n_articles):
        pub_dt = start_dt + dt.timedelta(minutes=rng.randint(0, 365*2*24*60))
        score, label, risk = compute_sentiment_and_risk(rng)
        publisher = rng.choice(["bbc.co.uk","reuters.com","theguardian.com","cityam.com","cnbc.com","wired.com"])
        section = rng.choice(["Business","Technology","Markets","UK","Retail","Opinion"])
        authors = [rng.choice(["Alex","Sam","Chris","Jordan","Taylor","Ava","Noah"]) + " " + rng.choice(["Smith","Brown","Taylor","Wilson","King"])]
        ne = build_named_entities(merchant, rng)
        pv = int(min(5_000_000, rng.lognormvariate(mu=math.log(1500+1e-9), sigma=1.2)))
        shares = int(max(0, rng.gauss(0.015 * math.sqrt(pv + 1) * 100, 25)))
        comments = int(max(0, rng.gauss(0.008 * math.sqrt(pv + 1) * 100, 15)))
        rec = {
            "article_id": f"n_{uuid.uuid4().hex[:10]}",
            "merchant": merchant,
            "title": f"{merchant} update {i}",
            "subtitle": None,
            "content": "Short article content...",
            "published_at": pub_dt.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z"),
            "updated_at": None,
            "publisher": publisher,
            "source_domain": publisher,
            "url": f"https://{publisher}/{pub_dt.date().isoformat()}/{uuid.uuid4().hex[:6]}",
            "amp_url": None,
            "author": authors[0],
            "authors": authors,
            "section": section,
            "categories": [section],
            "keywords": [merchant.lower().replace(" ",""), "update"],
            "named_entities": ne,
            "external_links": [],
            "image_urls": [],
            "language": "en",
            "region": "UK",
            "country": "GB",
            "is_paywalled": (publisher in {"ft.com","wsj.com"}) and (rng.random() < 0.3),
            "is_opinion": rng.random() < 0.12,
            "is_exclusive": rng.random() < 0.06,
            "is_breaking": rng.random() < 0.10,
            "pageviews": pv,
            "shares": shares,
            "comments": comments,
            "reading_time_min": round(rng.uniform(2.0, 6.5), 1),
            "sentiment_score": score,
            "sentiment_label": label,
            "risk_score": risk,
        }
        ts = ts_of_record("news", rec)
        rec["ts"] = ts
        rec["dt"] = dt.datetime.utcfromtimestamp(ts).isoformat()+"Z" if ts else None
        rec["doc_key"] = make_doc_key(merchant, "news", rec)
        docs.append(rec)
    upsert_many("news", docs)
