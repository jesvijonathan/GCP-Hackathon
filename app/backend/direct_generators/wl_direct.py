#!/usr/bin/env python3
# data_pipeline/wl_direct.py

import random, uuid, datetime as dt
from typing import Any, Dict, List, Optional
from .base import ts_of_record, make_doc_key, upsert_many

def generate_stream(mongo_uri: str, db_name: str, merchant: str, n_transactions: int, trend_plan: List[Dict[str, Any]], seed: Optional[int]=None):
    rng = random.Random(seed if isinstance(seed, int) else random.randint(1,10_000_000))
    start_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=365*2)
    docs: List[Dict[str, Any]] = []
    brands = ["Visa","Mastercard","Amex","Discover","Maestro","JCB","UnionPay"]
    mccs = ["5411","5812","5999","6011","5732","4789","7399","7995","4900","4814","5699","5921","6051","5813"]
    statuses = ["authorized","captured","declined","chargeback","refunded"]
    types = ["authorization","capture","refund","credit","oct","pre_auth","reconciliation","offline","gambling","chargeback","reversal","moto"]
    cities = ["London","Manchester","Birmingham","Leeds","Edinburgh"]
    countries = ["GB","US","IE","DE","FR","ES","IT"]
    for i in range(n_transactions):
        txn_dt = start_dt + dt.timedelta(minutes=rng.randint(0, 365*2*24*60))
        status = rng.choice(statuses)
        mcc = rng.choice(mccs)
        brand = rng.choice(brands)
        bin6 = f"{rng.randint(400000, 999999)}"; last4 = f"{rng.randint(1000, 9999)}"
        amt = round(rng.uniform(1.0, 500.0), 2)
        rec = {
            "txn_id": f"wlt_{uuid.uuid4().hex[:8]}",
            "merchant": merchant,
            "merchant_id": f"{uuid.uuid4().hex[:8]}",
            "acceptor_id": f"acc_{uuid.uuid4().hex[:10]}",
            "txn_time": txn_dt.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z"),
            "completion_time": (txn_dt + dt.timedelta(minutes=rng.randint(1,90))).replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z"),
            "status": status,
            "txn_type": rng.choice(types),
            "amount": amt,
            "currency_code": "GBP",
            "mcc": mcc,
            "card_brand": brand,
            "card_bin": bin6,
            "card_last4": last4,
            "pan_masked": f"{bin6}******{last4}",
            "place": rng.choice(cities),
            "country_code": rng.choice(countries),
            "risk_score": round(rng.uniform(0.0, 100.0), 1),
            "risk_factors": [],
            "risk_flags": {"high_risk_mcc": mcc in ("7995","6011","6051"), "fraud_suspected": rng.random()<0.03, "shady_region": rng.random()<0.02},
            "offline": rng.random() < 0.05,
            "international": rng.random() < 0.08,
            "user_id": f"u_{uuid.uuid4().hex[:10]}",
            "user_name": f"user_{uuid.uuid4().hex[:6]}",
            "terminal_id": f"T{rng.randint(100000, 999999)}",
            "terminal_type": rng.choice(["ecommerce","keypad","contactless","moto"]),
        }
        ts = ts_of_record("wl", rec)
        rec["ts"] = ts
        rec["dt"] = dt.datetime.utcfromtimestamp(ts).isoformat()+"Z" if ts else None
        rec["doc_key"] = make_doc_key(merchant, "wl", rec)
        docs.append(rec)
    upsert_many("wl_transactions", docs)
