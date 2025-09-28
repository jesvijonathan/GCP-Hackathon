#!/usr/bin/env python3
# data_pipeline/tweets_direct.py

import random, uuid, datetime as dt
from typing import Any, Dict, List, Optional
from .base import ts_of_record, make_doc_key, upsert_many

def choose_language(rng: random.Random) -> str:
    return "ENG" if rng.random() < 0.99 else rng.choice(["ES","FR","DE","IT","PT","PL","NL","RO"])

def build_content(merchant: str, sentiment_label: str, event_label: Optional[str], include_merchant_in_text: bool, rng: random.Random) -> str:
    pos = ["Great job by {m}.","{m} announced something exciting.","Impressed with {m}.","Positive update from {m}.","{m} looking strong."]
    neu = ["Anyone tried {m}?","What's your take on {m}?","Reading about {m}.","General discussion about {m}.","Checking on {m}."]
    neg = ["Not a good look for {m}.","Concerned about {m}.","This could be rough for {m}.","Disappointed with {m}.","Serious questions for {m}."]
    if event_label:
        el = event_label.lower()
        if any(k in el for k in ["breach","leak","outage","lawsuit"]):
            neg += ["Data concerns at {m}.","Hearing about issues at {m}.","Security story around {m} again."]
        if any(k in el for k in ["launch","product","feature"]):
            pos += ["{m} launched a new product.","Launch day for {m}.","New feature from {m}."]
    template = rng.choice(pos if sentiment_label=="positive" else (neg if sentiment_label=="negative" else neu))
    return template.format(m=(merchant if include_merchant_in_text else "them"))

def generate_metrics(base_like_scale: float, event_intensity: float, rng: random.Random):
    like_multiplier = 1.0 + 0.7 * abs(event_intensity)
    import math
    raw = rng.lognormvariate(mu=math.log(12 * base_like_scale + 1e-9), sigma=1.0)
    like_count = int(min(5000, raw * like_multiplier))
    reply_ratio = rng.uniform(0.05, 0.30)
    retweet_ratio = rng.uniform(0.10, 0.85)
    quote_ratio = rng.uniform(0.05, 0.60)
    reply_count = int(round(like_count * reply_ratio))
    retweet_count = int(round(like_count * retweet_ratio))
    quote_count = int(round(max(0, reply_count) * quote_ratio))
    imp_factor = rng.lognormvariate(mu=3.0, sigma=0.8)
    imp_factor = max(2.0, min(imp_factor, 200.0))
    impression_count = int(max(like_count + 1, like_count * imp_factor))
    url_ctr = rng.betavariate(1.2, 30.0)
    profile_ctr = rng.betavariate(1.2, 50.0)
    url_link_clicks = int(round(impression_count * url_ctr))
    user_profile_clicks = int(round(impression_count * profile_ctr))
    public = {"like_count": like_count, "reply_count": reply_count, "retweet_count": retweet_count, "quote_count": quote_count}
    non_public = {"impression_count": impression_count, "url_link_clicks": url_link_clicks, "user_profile_clicks": user_profile_clicks}
    return public, non_public

def generate_stream(mongo_uri: str, db_name: str, merchant: str, n_tweets: int, trend_plan: List[Dict[str, Any]], seed: Optional[int]=None, chunks: int=4):
    random.seed(seed)
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    # Precompute event intensities per day (simplified)
    day_events = {}
    for e in trend_plan or []:
        m = e.get("month","")
        if m:
            try:
                y, mm = [int(x) for x in m.split("-")]
                c = dt.date(y, mm, 15)
                day_events[c] = e
            except: pass
    start_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=365*2)
    docs_all: List[Dict[str, Any]] = []
    for i in range(n_tweets):
        created_at = (start_dt + dt.timedelta(minutes=rng.randint(0, 365*2*24*60))).isoformat().replace("+00:00","Z")
        # Internal sentiment guiding content
        sentiment_val = rng.gauss(0.1, 0.45)
        sentiment_label = "negative" if sentiment_val <= -0.2 else ("positive" if sentiment_val >= 0.2 else "neutral")
        include_merchant_text = rng.random() < rng.uniform(0.45, 0.75)
        # Event intensity approx via month anchor
        d = dt.date.fromisoformat(created_at[:10])
        ev = day_events.get(dt.date(d.year, d.month, 15))
        event_intensity = float(ev.get("intensity",0.0)) if ev else 0.0
        content = build_content(merchant, sentiment_label, ev.get("label") if ev else None, include_merchant_text, rng)
        public, non_public = generate_metrics(rng.uniform(0.6,1.4), event_intensity, rng)
        lang = choose_language(rng)
        rec = {
            "tweet_id": f"t_{uuid.uuid4().hex[:12]}",
            "author_id": f"u_{uuid.uuid4().hex[:10]}",
            "content": content,
            "created_at": created_at,
            "lang": lang,
            "public_metrics": public,
            "non_public_metrics": non_public,
            "entities": {"hashtags": [], "mentions": [], "urls": [], "cashtags": []},
            "context_annotations": [],
            "possibly_sensitive": (sentiment_label=="negative" and rng.random()<0.05),
            "conversation_id": None,
            "referenced_tweets": None,
            "attachments_media_keys": None,
            "edit_history_tweet_ids": None,
            "withheld": None,
            "sentiment_label": sentiment_label,
            "merchant": merchant,
        }
        ts = ts_of_record("tweets", rec)
        rec["ts"] = ts
        rec["dt"] = dt.datetime.utcfromtimestamp(ts).isoformat()+"Z" if ts else None
        rec["doc_key"] = make_doc_key(merchant, "tweets", rec)
        docs_all.append(rec)
    upsert_many("tweets", docs_all)
