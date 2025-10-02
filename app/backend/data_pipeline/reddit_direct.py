#!/usr/bin/env python3
# data_pipeline/reddit_direct.py

import random, uuid, datetime as dt, math
from typing import Any, Dict, List, Optional
from .base import ts_of_record, make_doc_key, upsert_many

def choose_subreddit(event_label: Optional[str], rng: random.Random) -> str:
    base = ["news","technology","investing","stocks","AskUK","DIY","HomeImprovement"]
    return rng.choice(base)

def build_title_and_text(merchant: str, sentiment_label: str, event_label: Optional[str], subreddit: str, rng: random.Random):
    pos = ["{m} launches new product","Strong update from {m}","Impressed with {m}","Great move by {m}"]
    neu = ["Discussion: {m}","What do you think about {m}?","Question about {m}","Reading about {m}"]
    neg = ["Issue at {m}","Outage at {m}?","Data breach at {m}?","Concerns with {m}"]
    if event_label and any(k in event_label.lower() for k in ["breach","outage","lawsuit","recall"]):
        neg += ["Security incident at {m}","Legal trouble for {m}"]
    if event_label and any(k in event_label.lower() for k in ["launch","feature","product"]):
        pos += ["Feature rollout from {m}","Launch day for {m}"]
    title = rng.choice(pos if sentiment_label=="positive" else (neg if sentiment_label=="negative" else neu)).format(m=merchant)
    body = None if rng.random()<0.5 else rng.choice(["Sharing thoughts","Any experiences?","Looking for advice","Heads-up"])
    return title, body

def generate_post_metrics(base_pop_scale: float, event_intensity: float, sentiment_label: str, is_self: bool, rng: random.Random):
    pop_multiplier = 1.0 + 0.8 * abs(event_intensity)
    ups = int(min(50000, rng.lognormvariate(mu=math.log(18*base_pop_scale+1e-9), sigma=1.1) * pop_multiplier))
    if sentiment_label=="positive": ratio = float(max(0.55, min(rng.betavariate(20,6),0.99)))
    elif sentiment_label=="negative": ratio = float(max(0.50, min(rng.betavariate(10,8),0.95)))
    else: ratio = float(max(0.52, min(rng.betavariate(14,8),0.97)))
    downs = int(max(0, round(ups * (1 - ratio) / max(1e-6, ratio))))
    score = int(max(0, ups - downs))
    base_c = rng.uniform(0.4, 0.9) if is_self else rng.uniform(0.25, 0.7)
    num_comments = int(max(0, rng.gauss(base_c * math.sqrt(ups + 1), 2.0) * (1.0 + 0.5 * abs(event_intensity))))
    lam = 0.01 * math.sqrt(max(1, ups))
    award_count = int(rng.poisson(lam=lam)) if hasattr(rng, "poisson") else int(min(10, lam))
    return ups, ratio, downs, score, num_comments, award_count

def generate_stream(mongo_uri: str, db_name: str, merchant: str, n_posts: int, trend_plan: List[Dict[str, Any]], seed: Optional[int]=None):
    random.seed(seed)
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    start_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=365*2)
    docs: List[Dict[str, Any]] = []
    for i in range(n_posts):
        created_dt = start_dt + dt.timedelta(minutes=rng.randint(0, 365*2*24*60))
        created_utc = int(created_dt.timestamp())
        sub = choose_subreddit(None, rng)
        sentiment = rng.choice(["positive","neutral","negative"])
        title, selftext = build_title_and_text(merchant, sentiment, None, sub, rng)
        ups, ratio, downs, score, num_comments, awards = generate_post_metrics(rng.uniform(0.6,1.6), 0.0, sentiment, selftext is not None, rng)
        rec = {
            "id": f"rd_{uuid.uuid4().hex[:8]}",
            "name": f"t3_{uuid.uuid4().hex[:6]}",
            "author": f"u/{uuid.uuid4().hex[:6]}",
            "author_fullname": f"t2_{uuid.uuid4().hex[:6]}",
            "author_id": f"u_{uuid.uuid4().hex[:10]}",
            "title": title,
            "selftext": selftext,
            "created_utc": created_utc,
            "created_at": created_dt.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z"),
            "subreddit": f"r/{sub}",
            "permalink": f"/{sub}/comments/{uuid.uuid4().hex[:6]}/post",
            "url": None,
            "is_self": selftext is not None,
            "flair_text": rng.choice(["Discussion","News","Advice"]),
            "over_18": rng.random() < 0.02,
            "spoiler": rng.random() < 0.03,
            "locked": rng.random() < 0.02,
            "stickied": rng.random() < 0.01,
            "num_comments": num_comments,
            "upvote_ratio": round(ratio, 3),
            "ups": ups,
            "downs": downs,
            "score": score,
            "award_count": awards,
            "keywords": [merchant.lower().replace(" ", "")],
            "lang": "en",
            "sentiment_label": sentiment,
            "merchant": merchant,
        }
        ts = ts_of_record("reddit", rec)
        rec["ts"] = ts
        rec["dt"] = dt.datetime.utcfromtimestamp(ts).isoformat()+"Z" if ts else None
        rec["doc_key"] = make_doc_key(merchant, "reddit", rec)
        docs.append(rec)
    upsert_many("reddit", docs)
