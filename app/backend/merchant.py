#!/usr/bin/env python3
# merchant.py
# Merchant onboarding and preset generation service for MongoDB.
#
# Merchant fields follow WLMerchant builder patterns in wl_data.py \ue202turn0file17\ue202turn0file19\ue202turn0file12.
# Preset block generation mirrors preset_data.py functions (random_trend_plan, core_to_stream_trends, stockify/wlify) \ue202turn0file6\ue202turn0file2\ue202turn0file18\ue202turn0file15.

import os
import uuid
import random
import datetime as dt
from typing import Any, Dict, Optional, List
from pymongo.database import Database
from pymongo import ASCENDING

# --------------------------- Helpers ---------------------------

def dt_to_iso(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.UTC)
    return dt_obj.astimezone(dt.UTC).isoformat().replace("+00:00","Z")

def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")

def random_time_hms(rng: random.Random) -> str:
    h = rng.randint(0,23); m = rng.randint(0,59); s = rng.randint(0,59)
    return f"{h:02d}:{m:02d}:{s:02d}"

def random_phone(rng: random.Random) -> str:
    return f"+{rng.randint(1,99)}-{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}"

def make_merchant_code(rng: random.Random) -> str:
    if rng.random() < 0.6:
        return f"{rng.randint(10_000_000, 99_999_999)}"
    else:
        return f"{rng.randint(100_000, 999_999)}00"

def pick_acceptor_mcc(rng: random.Random) -> str:
    mccs = ["5411","5812","5999","5732","4789","7399","4814","5699","4900","5921","5813","6051","6011","7995"]
    return rng.choice(mccs)

def random_ticker(name: str) -> str:
    letters = "".join([c for c in (name or "").upper() if c.isalpha()])
    if len(letters) >= 3:
        return letters[:3]
    return "MRC"

# --------------------------- Preset helpers (mirroring preset_data.py) ---------------------------

def random_trend_plan_months(start: str, end: str, rng: random.Random, n_min=3, n_max=5) -> List[Dict[str, Any]]:
    # Similar to preset_data.py random_trend_plan \ue202turn0file6
    from datetime import date
    def parse_month(s):
        y, m, *_ = s.split("-")
        return date(int(y), int(m), 1)
    s = parse_month(start[:7])
    e = parse_month(end[:7])
    months = []
    cur_y, cur_m = s.year, s.month
    while (cur_y < e.year) or (cur_y == e.year and cur_m <= e.month):
        months.append(f"{cur_y}-{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    if not months:
        return []
    k = rng.randint(n_min, min(n_max, max(3, len(months))))
    picks = sorted(rng.sample(range(len(months)), k=k))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership", "buyback", "expansion"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall", "regulatory fine", "scandal"]
    neu_labels = ["normal", "promo period", "seasonal buzz"]
    plan = []
    for idx in picks:
        m = months[idx]
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels); intensity = rng.uniform(0.65, 0.95)
        elif r < 0.70:
            label = rng.choice(pos_labels); intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neu_labels); intensity = rng.uniform(0.45, 0.70)
        item = {"month": m, "intensity": round(float(intensity), 2), "label": label}
        if rng.random() < 0.6:
            item["posts"] = f"{rng.randint(2,15)}%"  # preset_data.py pattern \ue202turn0file0
        plan.append(item)
    return plan

def core_to_stream_trends(core: List[Dict[str, Any]], rng: random.Random) -> Dict[str, List[Dict[str, Any]]]:
    # Mirrors preset_data.py core_to_stream_trends \ue202turn0file2
    import copy
    def with_share(key: str, lo=2, hi=20):
        out = []
        for e in core:
            ee = copy.deepcopy(e)
            if rng.random() < 0.6:
                ee[key] = f"{rng.randint(lo, hi)}%"
            else:
                ee.pop("posts", None)
            out.append(ee)
        return out
    # Stock and WL follow separate stockify/wlify below
    return {
        "tweets": with_share("posts", 2, 15),
        "reddit": with_share("posts", 2, 15),
        "news": with_share("articles", 2, 15),
        "reviews": with_share("reviews", 2, 22),
    }

def stockify(core: List[Dict[str, Any]], rng: random.Random) -> List[Dict[str, Any]]:
    # Mirrors preset_data.py stockify \ue202turn0file18
    out = []
    for e in core:
        ee = {k: e[k] for k in ("month", "intensity", "label")}
        lab = ee["label"].lower()
        if any(k in lab for k in ["breach", "lawsuit", "outage", "recall", "fine", "scandal"]):
            ret = rng.uniform(-0.15, -0.05)
            vol = rng.choice(["+40%", "+60%", "1.4x", "1.6x"])
            volm = rng.choice(["x1.6", "x2.0", "180%"])
        elif any(k in lab for k in ["launch", "award", "partnership", "buyback", "expansion"]):
            ret = rng.uniform(0.04, 0.14)
            vol = rng.choice(["+20%", "+40%", "1.2x", "1.4x"])
            volm = rng.choice(["x1.3", "x1.6", "150%"])
        else:
            ret = rng.uniform(-0.01, 0.02)
            vol = rng.choice(["+10%", "1.1x", "1.0x"])
            volm = rng.choice(["x1.1", "110%", "x1.0"])
        ee["return"] = round(float(ret), 3)
        ee["volatility"] = vol
        ee["volume"] = volm
        out.append(ee)
    return out

def wlify(core: List[Dict[str, Any]], rng: random.Random) -> List[Dict[str, Any]]:
    # Mirrors preset_data.py wlify (attach risk-related fields probabilistically) \ue202turn0file11
    out = []
    for e in core:
        ee = {k: e[k] for k in ("month", "intensity", "label")}
        if rng.random() < 0.6:
            ee["transactions"] = f"{rng.randint(2, 20)}%"
        lab = ee["label"].lower()
        if any(k in lab for k in ["breach", "lawsuit", "scandal", "outage", "recall", "fine"]):
            ee["high_risk_intensity"] = round(rng.uniform(0.30, 0.70), 3)
            ee["decline_rate"] = round(rng.uniform(0.15, 0.35), 3) if rng.random() < 0.6 else None
            if rng.random() < 0.5:
                ee["gambling_share"] = round(rng.uniform(0.04, 0.15), 3)
        elif any(k in lab for k in ["launch","award","partnership","buyback","expansion"]):
            ee["high_risk_intensity"] = round(rng.uniform(0.05, 0.15), 3)
            ee["decline_rate"] = round(rng.uniform(0.02, 0.08), 3) if rng.random() < 0.4 else None
            if rng.random() < 0.4:
                ee["gambling_share"] = round(rng.uniform(0.02, 0.08), 3)
        else:
            ee["high_risk_intensity"] = round(rng.uniform(0.08, 0.22), 3)
            ee["decline_rate"] = round(rng.uniform(0.05, 0.12), 3) if rng.random() < 0.5 else None
            if rng.random() < 0.5:
                ee["gambling_share"] = round(rng.uniform(0.03, 0.12), 3)
        out.append(ee)
    return out

def random_preset_block(name: str, start_date: str, end_date: str, rng: random.Random) -> Dict[str, Any]:
    # Mirrors preset_data.py random_merchant_block \ue202turn0file15
    core = random_trend_plan_months(start_date, end_date, rng)
    tr = core_to_stream_trends(core, rng)

    tweets_n = rng.randint(5000, 20000)
    reddit_n = rng.randint(3000, 12000)
    news_n = rng.randint(3000, 9000)
    reviews_n = rng.randint(12000, 40000)
    products_n = rng.randint(5, 12)
    merchant_score = round(rng.uniform(3.7, 4.5), 2)

    stock_base = round(rng.uniform(2.0, 400.0), 2)
    stock_sigma = round(rng.uniform(0.25, 0.55), 2)
    shares_out = rng.randrange(100_000_000, 5_000_000_000, 10_000)
    avg_vol = rng.randrange(1_000_000, 15_000_000, 1000)
    total_ret = round(rng.uniform(-0.25, 0.60), 3)

    wl_n = rng.randint(20000, 180000)

    ticker = random_ticker(name)

    return {
        "merchant_name": name,
        "start_date": start_date,
        "end_date": end_date,
        "seed": rng.randint(1, 10_000_000),
        "tweets": {"enabled": True, "n_tweets": tweets_n, "trend_plan": tr["tweets"]},
        "reddit": {"enabled": True, "n_posts": reddit_n, "trend_plan": tr["reddit"]},
        "news": {"enabled": True, "n_articles": news_n, "trend_plan": tr["news"]},
        "reviews": {"enabled": True, "n_products": products_n, "merchant_score": merchant_score, "n_reviews": reviews_n, "trend_plan": tr["reviews"]},
        "stock": {
            "enabled": True,
            "ticker": ticker,
            "base_price": stock_base,
            "sigma_annual": stock_sigma,
            "avg_daily_volume": int(avg_vol),
            "shares_outstanding": int(shares_out),
            "target_total_return": total_ret,
            "trend_plan": stockify(core, rng),
        },
        "wl": {"enabled": True, "n_transactions": wl_n, "trend_plan": wlify(core, rng)},
    }

def merge_overrides(block: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(block)
    for k, v in overrides.items():
        if k in ("tweets","reddit","news","reviews","stock","wl") and isinstance(v, dict):
            merged = dict(out.get(k, {}))
            merged.update(v)
            out[k] = merged
        else:
            out[k] = v
    return out

# --------------------------- Service ---------------------------

class MerchantService:
    def __init__(self, db: Database):
        self.db = db

    def ensure_indexes(self):
        # merchants: unique merchant_name
        try:
            self.db["merchants"].create_index([("merchant_name", ASCENDING)], unique=True, name="uniq_merchant_name", background=True)
        except Exception:
            pass
        try:
            self.db["merchants"].create_index([("merchant_id", ASCENDING)], unique=True, name="uniq_merchant_id", background=True)
        except Exception:
            pass
        # preset: unique merchant_name
        try:
            self.db["preset"].create_index([("merchant_name", ASCENDING)], unique=True, name="uniq_preset_merchant", background=True)
        except Exception:
            pass

    def list_merchant_names(self) -> List[str]:
        try:
            return sorted([d["merchant_name"] for d in self.db["merchants"].find({}, {"merchant_name":1, "_id":0})])
        except Exception:
            return []

    def list_preset_names(self) -> List[str]:
        try:
            return sorted([d["merchant_name"] for d in self.db["preset"].find({}, {"merchant_name":1, "_id":0})])
        except Exception:
            return []

    def get_merchant(self, merchant_name: str) -> Optional[Dict[str, Any]]:
        return self.db["merchants"].find_one({"merchant_name": merchant_name}, projection={"_id":0})

    def get_preset(self, merchant_name: str) -> Optional[Dict[str, Any]]:
        return self.db["preset"].find_one({"merchant_name": merchant_name}, projection={"_id":0})

    def _auto_generate_fields(self, merchant_name: str, seed: Optional[int]) -> Dict[str, Any]:
        rng = random.Random(seed if isinstance(seed, int) else random.randint(1, 10_000_000))
        slug = slugify(merchant_name)
        merchant_id = f"acc_{uuid.uuid4().hex[:10]}"
        merchant_code = make_merchant_code(rng)
        country = rng.choice(["GB", "US", "IE", "DE", "FR", "ES", "IT"])
        currency = {"GB": "GBP", "US": "USD", "IE": "EUR", "DE": "EUR", "FR": "EUR", "ES": "EUR", "IT": "EUR"}.get(country, "USD")
        city = rng.choice(["London", "Manchester", "Birmingham", "Edinburgh", "Dublin", "New York", "Berlin", "Paris", "Madrid", "Milan"])
        tzmap = {"GB": "Europe/London", "IE": "Europe/Dublin", "US": "America/New_York", "DE": "Europe/Berlin", "FR": "Europe/Paris", "ES": "Europe/Madrid", "IT": "Europe/Rome"}
        tz = tzmap.get(country, "UTC")

        cut_off = random_time_hms(rng)
        now = dt.datetime.now(dt.timezone.utc)
        act_start = now - dt.timedelta(days=rng.randint(300, 1200))
        act_end = None if rng.random() < 0.9 else (now + dt.timedelta(days=rng.randint(30, 300)))

        doc = {
            "merchant_name": merchant_name,
            "merchant_id": merchant_id,
            "merchant_code": merchant_code,
            "legal_name": f"{merchant_name} Ltd.",
            "acceptor_name": f"{merchant_name} Acceptor",
            "acceptor_category_code": pick_acceptor_mcc(rng),
            "url": f"https://{slug}.example.com",
            "language_code": "en",
            "time_zone_id": tz,
            "country_code": country,
            "country_sub_division_code": rng.choice(["LND","NY","BER","PAR","MAD","MAN", None]),
            "home_country_code": country,
            "region_id": rng.choice(["UK","EU","US"]),
            "city": city,
            "postal_code": f"{rng.randint(10000, 99999)}",
            "street": f"{rng.randint(1, 250)} {rng.choice(['Main St','High St','Market Rd','Broadway','King St'])}",
            "currency_code": currency,
            "tax_id": f"TAX-{uuid.uuid4().hex[:8].upper()}",
            "trade_register_number": f"REG-{uuid.uuid4().hex[:8].upper()}",
            "iban": None,
            "domiciliary_bank_number": f"BANK-{rng.randint(100000,999999)}",
            "cut_off_time": cut_off,
            "activation_flag": True if rng.random() < 0.98 else False,
            "activation_time": dt_to_iso(now - dt.timedelta(days=rng.randint(0, 300))),
            "activation_start_time": dt_to_iso(act_start),
            "activation_end_time": (dt_to_iso(act_end) if act_end else None),
            "city_category_code": rng.choice(["URB","SUB","RUR", None]),
            "business_service_phone_number": random_phone(rng),
            "customer_service_phone_number": random_phone(rng),
            "additional_contact_information": "Contact support via portal.",
            "description": "Acquirer/acceptor profile for merchant risk simulation.",
            "created_at": dt_to_iso(dt.datetime.now(dt.UTC)),
            "updated_at": dt_to_iso(dt.datetime.now(dt.UTC)),
        }
        return doc

    def _merge_details(self, auto_doc: Dict[str, Any], details: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(auto_doc)
        for k, v in (details or {}).items():
            # Do not replace merchant_id if present in DB; onboarding controls id retention
            out[k] = v
        out["updated_at"] = dt_to_iso(dt.datetime.now(dt.UTC))
        return out

    def onboard_and_generate_preset(
        self,
        merchant_name: str,
        start_date: str,
        end_date: str,
        deep_scan: bool,
        details: Dict[str, Any],
        preset_overrides: Dict[str, Any],
        seed: Optional[int]
    ) -> Dict[str, Any]:
        # Upsert merchant
        existing = self.db["merchants"].find_one({"merchant_name": merchant_name})
        if existing:
            merchant_id = existing.get("merchant_id") or f"acc_{uuid.uuid4().hex[:10]}"
            auto_doc = self._auto_generate_fields(merchant_name, seed)
            auto_doc["merchant_id"] = merchant_id  # keep same id if present
            merged = self._merge_details(auto_doc, details)
            # Preserve created_at from existing
            merged["created_at"] = existing.get("created_at") or merged["created_at"]
            # Optional range on merchant doc
            merged["start_date"] = start_date
            merged["end_date"] = end_date
            self.db["merchants"].update_one({"merchant_name": merchant_name}, {"$set": merged}, upsert=True)
            merchant_doc = self.get_merchant(merchant_name)
        else:
            auto_doc = self._auto_generate_fields(merchant_name, seed)
            auto_doc["start_date"] = start_date
            auto_doc["end_date"] = end_date
            doc = self._merge_details(auto_doc, details)
            self.db["merchants"].update_one({"merchant_name": merchant_name}, {"$set": doc}, upsert=True)
            merchant_doc = self.get_merchant(merchant_name)

        # Generate preset if missing or deep_scan
        preset_doc = self.generate_or_update_preset(
            merchant_name=merchant_name,
            start_date=start_date,
            end_date=end_date,
            deep_scan=deep_scan,
            preset_overrides=preset_overrides,
            seed=seed
        )

        return {"merchant": merchant_doc, "preset": preset_doc}

    def generate_or_update_preset(
        self,
        merchant_name: str,
        start_date: str,
        end_date: str,
        deep_scan: bool,
        preset_overrides: Dict[str, Any],
        seed: Optional[int]
    ) -> Dict[str, Any]:
        existing = self.get_preset(merchant_name)
        rng = random.Random(seed if isinstance(seed, int) else random.randint(1, 10_000_000))
        if existing and not deep_scan:
            # Keep existing
            return existing
        base = random_preset_block(merchant_name, start_date, end_date, rng)
        if preset_overrides:
            base = merge_overrides(base, preset_overrides)
        base["updated_at"] = dt_to_iso(dt.datetime.now(dt.UTC))
        if not existing:
            base["created_at"] = dt_to_iso(dt.datetime.now(dt.UTC))
        self.db["preset"].update_one({"merchant_name": merchant_name}, {"$set": base}, upsert=True)
        return self.get_preset(merchant_name)


# End of merchant.py

# You can run the server with:
# - uvicorn main:app --host 0.0.0.0 --port 8000

# Endpoints:
# - POST /v1/onboard
#   Body: { "merchant_name": "Kingfisher", "deep_scan": false, "details": { ... }, "preset_overrides": { ... }, "start_date": "2022-01-01", "end_date": "2026-01-01", "seed": 777 }
#   Returns: { status: "accepted", task_id }

# - GET /v1/tasks/{task_id}
#   Returns task status and, when done, the merchant and preset docs.

# - GET /v1/merchants -> list names
# - GET /v1/merchants/{merchant_name} -> merchant doc
# - GET /v1/preset -> list names with preset
# - GET /v1/preset/{merchant_name} -> preset doc
# - POST /v1/preset/rebuild/{merchant_name}?seed=777 -> force regenerate preset off-thread

# Implementation choices match your requirements:
# - Merchant name is the sole unique identifier; we keep the same merchant_id on updates.
# - Default preset range is now-3y to now+1y when not provided.
# - Preset generation only occurs if missing, or when deep_scan=true.
# - Tasks run off the main thread (ThreadPoolExecutor) for non-blocking onboarding.
# - Multi-threading support is in place for future parallel needs.
# - Only two collections are used: merchants and preset.