#!/usr/bin/env python3
# data_pipeline/stock_direct.py

import random, datetime as dt, math
from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from .base import make_doc_key

def parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def generate_stream(mongo_uri: str, db_name: str, merchant: str, stock_cfg: Dict[str, Any], start_date: str, end_date: str, seed: Optional[int]=None):
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    client = MongoClient(mongo_uri); db = client[db_name]
    # Meta
    meta = {
        "ticker": stock_cfg.get("ticker"),
        "currency": stock_cfg.get("currency","GBP"),
        "start_date": start_date,
        "end_date": end_date,
        "base_price": round(float(stock_cfg.get("base_price", 100.0)), 4),
        "shares_outstanding": int(stock_cfg.get("shares_outstanding", 2_000_000_000)),
        "avg_daily_volume": int(stock_cfg.get("avg_daily_volume", 5_000_000)),
        "mu_annual_input": round(float(stock_cfg.get("mu_annual", 0.08)), 6),
        "sigma_annual_input": round(float(stock_cfg.get("sigma_annual", 0.35)), 6),
    }
    trend_plan = stock_cfg.get("trend_plan") or []
    db["stocks_meta"].update_one({"merchant": merchant}, {"$set": {"merchant": merchant, "meta": meta, "trend_plan": trend_plan}}, upsert=True)

    # Prices (GBM-like)
    sdate = parse_date(start_date); edate = parse_date(end_date)
    days = (edate - sdate).days + 1
    price = meta["base_price"]
    sigma = float(stock_cfg.get("sigma_annual", 0.35))
    mu_annual = float(stock_cfg.get("mu_annual", 0.08))
    dt_year = 1.0 / 252.0
    prices_docs = []
    for i in range(days):
        d = sdate + dt.timedelta(days=i)
        mu_day = mu_annual * dt_year
        shock = rng.gauss(mu_day, sigma * math.sqrt(dt_year))
        price = max(0.01, price * (1.0 + shock))
        rec = {"merchant": merchant, "date": d.isoformat(), "close": round(price, 2)}
        rec["doc_key"] = make_doc_key(merchant, "stocks_prices", rec)
        dt_obj = dt.datetime.fromisoformat(f"{d.isoformat()}T00:00:00").replace(tzinfo=dt.timezone.utc)
        rec["ts"] = dt_obj.timestamp(); rec["dt"] = dt_obj.isoformat().replace("+00:00","Z")
        prices_docs.append(rec)

    # Earnings and actions (simple)
    earns_docs = []
    acts_docs = []
    for e in trend_plan:
        m = str(e.get("month",""))
        try:
            y, mm = [int(x) for x in m.split("-")]
            d = dt.date(y, mm, 15)
            rec_e = {"merchant": merchant, "date": d.isoformat(), "eps_actual": round(rng.uniform(0.01, 2.0), 4), "eps_estimate": round(rng.uniform(0.01, 2.0),4)}
            rec_e["doc_key"] = make_doc_key(merchant, "stocks_earnings", rec_e)
            dt_obj = dt.datetime.fromisoformat(f"{d.isoformat()}T00:00:00").replace(tzinfo=dt.timezone.utc)
            rec_e["ts"] = dt_obj.timestamp(); rec_e["dt"] = dt_obj.isoformat().replace("+00:00","Z")
            earns_docs.append(rec_e)
            rec_a = {"merchant": merchant, "date": d.isoformat(), "type": "dividend", "amount": round(rng.uniform(0.01, 0.5), 4)}
            rec_a["doc_key"] = make_doc_key(merchant, "stocks_actions", rec_a)
            rec_a["ts"] = dt_obj.timestamp(); rec_a["dt"] = dt_obj.isoformat().replace("+00:00","Z")
            acts_docs.append(rec_a)
        except: pass

    # Bulk write
    from .base import upsert_many
    upsert_many("stocks_prices", prices_docs)
    upsert_many("stocks_earnings", earns_docs)
    upsert_many("stocks_actions", acts_docs)
    client.close()
