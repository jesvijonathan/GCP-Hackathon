#!/usr/bin/env python3
"""
Preset generator and bulk ingestion tool for merchants.

Features:
- CLI with subcommands:
  - generate: creates a preset.json for N merchants (with optional explicit names or a base set)
  - ingest: reads a preset.json and bulk-loads into MongoDB using MerchantService
  - streams: reads a preset.json and generates stream data (reddit, news, reviews, stock, wl) into MongoDB
- The preset contains:
  - global: start_date, end_date, output_dir, defaults
  - merchants: each has merchant_name, start_date, end_date, seed, and nested blocks
    for tweets, reddit, news, reviews, stock, wl (with trend_plan data)
- Ingestion uses the existing MerchantService (from merchant.py) to onboard and generate presets.
- You can pass --mongo-uri and --db to override environment defaults.
- In generate mode you can supply custom JSON blocks to override per-merchant fields.

Usage examples:
  - Generate 20 merchants with a custom date window and default seed:
      python preset.py generate --num 20 --start_date 2020-01-01 --end_date 2023-01-01 --out preset.json --seed 1234
  - Ingest an existing preset.json into MongoDB:
      python preset.py ingest --preset preset.json --mongo-uri mongodb://127.0.0.1:27017 --db merchant_analytics --deep-scan
  - Generate streams for all merchants in preset.json:
      python preset.py streams --preset preset.json --mongo-uri mongodb://127.0.0.1:27017 --db merchant_analytics
"""

import os
import json
import argparse
import random
import datetime as dt
import time
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ASCENDING
from merchant import MerchantService

# Stream generators (as a package)
from data_pipeline.base import init_env as dp_init_env
from data_pipeline import reddit_direct, news_direct, reviews_direct, stock_direct, wl_direct

# ---------------- Helpers ----------------

BASE_NAMES = [
    "HomeGear","BuildPro","BrightLite","GardenCore","FixItCo","ToolNest",
    "ApexDIY","PrimeHome","HandyMart","CraftWorks","ValueFix","MegaBuild",
    "UrbanTools","GreenWorks","SmartDIY","HomeForge","SolidCore","EdgeTools"
]

def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")

def random_ticker(name: str) -> str:
    letters = "".join([c for c in (name or "").upper() if c.isalpha()])
    return letters[:3] if len(letters) >= 3 else "MRC"

def random_time_hms(rng: random.Random) -> str:
    h = rng.randint(0,23); m = rng.randint(0,59); s = rng.randint(0,59)
    return f"{h:02d}:{m:02d}:{s:02d}"

def random_trend_plan(start: str, end: str, rng: random.Random, n_min=3, n_max=5) -> List[Dict[str, Any]]:
    from datetime import date
    def parse_month(s: str):
        y, m, *_ = s.split("-")
        return date(int(y), int(m), 1)
    s = parse_month(start)
    e = parse_month(end)
    months: List[str] = []
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
    plan: List[Dict[str, Any]] = []
    for idx in picks:
        m = months[idx]
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels); intensity = rng.uniform(0.65, 0.95)
        elif r < 0.70:
            label = rng.choice(pos_labels); intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neu_labels); intensity = rng.uniform(0.45, 0.70)
        item: Dict[str, Any] = {"month": m, "intensity": round(float(intensity), 2), "label": label}
        if rng.random() < 0.6:
            item["posts"] = f"{rng.randint(2,15)}%"
        plan.append(item)
    return plan

def core_to_stream_trends(core: List[Dict[str, Any]], rng: random.Random) -> Dict[str, List[Dict[str, Any]]]:
    import copy
    def with_share(key: str, lo=2, hi=20) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for e in core:
            ee = copy.deepcopy(e)
            if rng.random() < 0.6:
                ee[key] = f"{rng.randint(lo, hi)}%"
            else:
                ee.pop("posts", None)
            out.append(ee)
        return out

    def stockify() -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for e in core:
            ee = {k: e[k] for k in ("month", "intensity", "label")}
            lab = ee["label"].lower()
            if any(k in lab for k in ["breach", "lawsuit", "outage", "recall", "fine", "scandal"]):
                ret = rng.uniform(-0.15, -0.05)
                vol = rng.choice(["+20%","+40%","1.2x","1.4x","1.6x"])
                volm = rng.choice(["x1.3","x1.6","150%","180%"])
            elif any(k in lab for k in ["launch", "award", "partnership", "buyback", "expansion"]):
                ret = rng.uniform(0.04, 0.14)
                vol = rng.choice(["+10%","+20%","+40%","1.2x","1.4x"])
                volm = rng.choice(["x1.1","x1.3","x1.6","110%","150%"])
            else:
                ret = rng.uniform(-0.01, 0.02)
                vol = rng.choice(["+10%","1.1x","1.0x"])
                volm = rng.choice(["x1.1","110%","x1.0"])
            ee["return"] = round(float(ret), 3)
            ee["volatility"] = vol
            ee["volume"] = volm
            out.append(ee)
        return out

    def wlify() -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
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

    return {
        "tweets": with_share("posts", 2, 15),
        "reddit": with_share("posts", 2, 15),
        "news": with_share("articles", 2, 15),
        "reviews": with_share("reviews", 2, 22),
        "stock": stockify(),
        "wl": wlify()
    }

def random_merchant_block(name: str, start_date: str, end_date: str, rng: random.Random) -> Dict[str, Any]:
    core = random_trend_plan(start_date[:7], end_date[:7], rng)
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
            "trend_plan": tr["stock"],
        },
        "wl": {"enabled": True, "n_transactions": wl_n, "trend_plan": tr["wl"]},
    }

def merge_custom_into_random(random_block: Dict[str, Any], custom_block: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(random_block)
    for key, val in custom_block.items():
        if key in ("tweets", "reddit", "news", "reviews", "stock", "wl") and isinstance(val, dict):
            merged = dict(random_block.get(key, {}))
            merged.update(val)
            out[key] = merged
        else:
            out[key] = val
    return out

def generate_preset(merchant_names: List[str], start_date: str, end_date: str, seed: int, customs: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    rng = random.Random(seed)
    merchants: List[Dict[str, Any]] = []
    used_names = set()
    for nm in merchant_names:
        base_nm = nm
        if base_nm in used_names:
            base_nm = f"{nm}_{rng.randint(1000,9999)}"
        used_names.add(base_nm)
        block = random_merchant_block(base_nm, start_date, end_date, rng)
        merchants.append(block)

    if customs:
        for c in customs:
            cname = c.get("merchant_name") or (merchant_names[0] if merchant_names else "Merchant")
            idx = next((i for i, m in enumerate(merchants) if m.get("merchant_name", "").lower() == str(cname).lower()), None)
            if idx is None:
                nm = cname
                new_block = random_merchant_block(nm, start_date, end_date, rng)
                merged = merge_custom_into_random(new_block, c)
                merchants.append(merged)
            else:
                merged = merge_custom_into_random(merchants[idx], c)
                merchants[idx] = merged

    preset = {
        "global": {
            "start_date": start_date,
            "end_date": end_date,
            "output_dir": ".",
            "defaults": {
                "wl": {"n_transactions": 50000}
            },
        },
        "merchants": merchants
    }
    return preset

def save_preset(preset: Dict[str, Any], out_path: str) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(preset, f, ensure_ascii=False, indent=2)
    return os.path.abspath(out_path)

# ---------------- Index helpers ----------------

def ensure_stream_indexes(mongo_uri: str, db_name: str):
    client = MongoClient(mongo_uri); db = client[db_name]
    cols = ["reddit","news","reviews","wl_transactions","stocks_prices","stocks_earnings","stocks_actions"]
    for c in cols:
        try: db[c].create_index([("doc_key", ASCENDING)], unique=True, name="uniq_doc_key", background=True)
        except Exception: pass
        try: db[c].create_index([("merchant", ASCENDING)], name="idx_merchant", background=True)
        except Exception: pass
    for c in ["stocks_prices","stocks_earnings","stocks_actions"]:
        try: db[c].create_index([("date", ASCENDING)], name="idx_date", background=True)
        except Exception: pass
    client.close()

# ---------------- Ingestion helpers ----------------

def _ensure_mongo(conn_uri: Optional[str], db_name: Optional[str]):
    if conn_uri is None:
        conn_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    if db_name is None:
        db_name = os.getenv("DB_NAME", "merchant_analytics")
    client = MongoClient(conn_uri)
    db = client[db_name]
    return client, db

def ingest_preset_to_mongo(preset_path: str, mongo_uri: Optional[str] = None, db_name: Optional[str] = None, deep_scan: bool = False, seed_override: Optional[int] = None, max_workers: int = 8, single: bool = False) -> List[Dict[str, Any]]:
    with open(preset_path, "r", encoding="utf-8") as f:
        preset = json.load(f)
    merchants = preset.get("merchants", [])
    global_start = preset.get("global", {}).get("start_date") or preset.get("start_date", "now")
    global_end = preset.get("global", {}).get("end_date") or preset.get("end_date", "now")

    client, db = _ensure_mongo(mongo_uri, db_name)
    service = MerchantService(db)
    results: List[Dict[str, Any]] = []

    try:
        service.ensure_indexes()
    except Exception:
        pass

    from concurrent.futures import ThreadPoolExecutor, as_completed

    total = len(merchants)
    print(f"[ingest] Starting onboarding for {total} merchants (single={single}, workers={max_workers if not single else 1})...")
    started = time.time()
    processed = 0

    def ingest_block(block: Dict[str, Any]) -> Dict[str, Any]:
        name = block.get("merchant_name")
        if not name:
            return {"merchant_name": None, "status":"error", "error": "missing merchant_name"}
        s_date = block.get("start_date") or global_start
        e_date = block.get("end_date") or global_end
        details = {k: v for k, v in block.items() if k not in ("merchant_name",)}
        seed = block.get("seed", seed_override)
        res = service.onboard_and_generate_preset(
            merchant_name=name,
            start_date=s_date,
            end_date=e_date,
            deep_scan=deep_scan,
            details=details,
            preset_overrides={},
            seed=seed
        )
        return {"merchant_name": name, "status": "ok", "result": res}

    if single:
        for m in merchants:
            try:
                r = ingest_block(m)
                results.append(r)
                processed += 1
                print(f"[ingest] {processed}/{total} done: {r.get('merchant_name')} -> {r.get('status')}")
            except Exception as ex2:
                processed += 1
                print(f"[ingest] {processed}/{total} error: {ex2}")
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(ingest_block, m) for m in merchants]
            for fut in as_completed(futures):
                processed += 1
                try:
                    r = fut.result()
                    results.append(r)
                    nm = r.get("merchant_name")
                    st = r.get("status")
                    print(f"[ingest] {processed}/{total} done: {nm} -> {st}")
                except Exception as ex2:
                    print(f"[ingest] {processed}/{total} error: {ex2}")

    took = int(time.time() - started)
    print(f"[ingest] Completed {processed}/{total} merchants in {took}s.")
    client.close()
    return results

# ---------------- Streams generation helpers ----------------

def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def _days_between(start_date: str, end_date: str) -> int:
    try:
        s = _date(start_date)
        e = _date(end_date)
        return (e - s).days + 1 if e >= s else 0
    except Exception:
        return 0

def estimate_counts_for_block(block: Dict[str, Any], global_start: str, global_end: str, scale: float, scale_stocks: bool) -> Dict[str, int]:
    start_date = block.get("start_date", global_start)
    end_date = block.get("end_date", global_end)
    r = block.get("reddit", {}) or {}
    n = block.get("news", {}) or {}
    rv = block.get("reviews", {}) or {}
    st = block.get("stock", {}) or {}
    wl = block.get("wl", {}) or {}

    stock_tp = st.get("trend_plan", []) or []
    rp = int(r.get("n_posts", 0)) if r.get("enabled") else 0
    na = int(n.get("n_articles", 0)) if n.get("enabled") else 0
    rr = int(rv.get("n_reviews", 0)) if rv.get("enabled") else 0
    wt = int(wl.get("n_transactions", 0)) if wl.get("enabled") else 0
    sd = _days_between(start_date, end_date) if st.get("enabled") else 0

    if scale < 1.0:
        rp = int(rp * scale)
        na = int(na * scale)
        rr = int(rr * scale)
        wt = int(wt * scale)
        if scale_stocks and sd > 0:
            sd = max(1, int(sd * scale))

    return {
        "reddit": rp,
        "news": na,
        "reviews": rr,
        "stocks_prices": sd,
        "stocks_earnings": len(stock_tp) if st.get("enabled") else 0,
        "stocks_actions": len(stock_tp) if st.get("enabled") else 0,
        "wl_transactions": wt,
    }

def sum_counts(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
    keys = {"reddit","news","reviews","stocks_prices","stocks_earnings","stocks_actions","wl_transactions"}
    return {k: int(a.get(k,0)) + int(b.get(k,0)) for k in keys}

def fmt_int(n: int) -> str:
    return f"{n:,}"

def print_counts(prefix: str, counts: Dict[str, int]) -> None:
    print(
        f"{prefix} reddit={fmt_int(counts['reddit'])}, "
        f"news={fmt_int(counts['news'])}, "
        f"reviews={fmt_int(counts['reviews'])}, "
        f"stocks_prices={fmt_int(counts['stocks_prices'])}, "
        f"stocks_earnings={fmt_int(counts['stocks_earnings'])}, "
        f"stocks_actions={fmt_int(counts['stocks_actions'])}, "
        f"wl_transactions={fmt_int(counts['wl_transactions'])}"
    )

def generate_streams_from_preset(
    preset_path: str,
    mongo_uri: Optional[str] = None,
    db_name: Optional[str] = None,
    max_workers: int = 8,
    only: Optional[List[str]] = None,
    scale: float = 1.0,
    scale_stocks: bool = False,
    single: bool = False
) -> List[Dict[str, Any]]:
    if mongo_uri is None:
        mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    if db_name is None:
        db_name = os.getenv("DB_NAME", "merchant_analytics")
    dp_init_env(mongo_uri, db_name)
    ensure_stream_indexes(mongo_uri, db_name)

    with open(preset_path, "r", encoding="utf-8") as f:
        preset = json.load(f)

    merchants = preset.get("merchants", [])
    global_start = preset.get("global", {}).get("start_date") or preset.get("start_date", "now")
    global_end = preset.get("global", {}).get("end_date") or preset.get("end_date", "now")

    streams = [s.strip() for s in (only or ["reddit","news","reviews","stock","wl"])]

    total_counts = {"reddit":0,"news":0,"reviews":0,"stocks_prices":0,"stocks_earnings":0,"stocks_actions":0,"wl_transactions":0}
    for m in merchants:
        total_counts = sum_counts(total_counts, estimate_counts_for_block(m, global_start, global_end, scale, scale_stocks))
    print_counts(f"[streams] Estimated totals (scale={scale}, stocks_scaled={scale_stocks}):", total_counts)

    results: List[Dict[str, Any]] = []
    total = len(merchants)
    processed = 0
    started = time.time()
    done_counts = {"reddit":0,"news":0,"reviews":0,"stocks_prices":0,"stocks_earnings":0,"stocks_actions":0,"wl_transactions":0}

    def scaled_value(v: int) -> int:
        return int(v * scale) if scale < 1.0 else int(v)

    def run_one(block: Dict[str, Any]) -> Dict[str, Any]:
        name = block.get("merchant_name")
        seed = block.get("seed")
        exp = estimate_counts_for_block(block, global_start, global_end, scale, scale_stocks)
        t0 = time.time()

        # reddit
        if "reddit" in streams and block.get("reddit", {}).get("enabled"):
            r = block["reddit"]
            n_posts = scaled_value(int(r.get("n_posts", 0)))
            reddit_direct.generate_stream(mongo_uri, db_name, name, n_posts, r.get("trend_plan", []), seed)

        # news
        if "news" in streams and block.get("news", {}).get("enabled"):
            n = block["news"]
            n_articles = scaled_value(int(n.get("n_articles", 0)))
            news_direct.generate_stream(mongo_uri, db_name, name, n_articles, n.get("trend_plan", []), seed)

        # reviews
        if "reviews" in streams and block.get("reviews", {}).get("enabled"):
            rv = block["reviews"]
            n_reviews = scaled_value(int(rv.get("n_reviews", 0)))
            n_products = max(1, scaled_value(int(rv.get("n_products", 0))) or int(rv.get("n_products", 0)))
            reviews_direct.generate_stream(mongo_uri, db_name, name, n_reviews, n_products, rv.get("merchant_score", 0.0), rv.get("trend_plan", []), seed)

        # stock (optional scale via window compression)
        if "stock" in streams and block.get("stock", {}).get("enabled"):
            st = block["stock"]
            s_date = block.get("start_date", global_start)
            e_date = block.get("end_date", global_end)
            if scale_stocks:
                days = _days_between(s_date, e_date)
                target_days = max(1, int(days * scale)) if days > 0 else 0
                if target_days > 0 and s_date and e_date:
                    # Compress window by moving end_date earlier
                    try:
                        sd = dt.date.fromisoformat(s_date)
                        ed_scaled = sd + dt.timedelta(days=target_days - 1)
                        e_date = ed_scaled.isoformat()
                    except Exception:
                        pass
            stock_direct.generate_stream(mongo_uri, db_name, name, st, s_date, e_date, seed)

        # wl
        if "wl" in streams and block.get("wl", {}).get("enabled"):
            wl = block["wl"]
            n_txn = scaled_value(int(wl.get("n_transactions", 0)))
            wl_direct.generate_stream(mongo_uri, db_name, name, n_txn, wl.get("trend_plan", []), seed)

        t1 = time.time()
        return {"merchant_name": name, "status": "ok", "counts": exp, "elapsed": round(t1 - t0, 2)}

    print(f"[streams] Generating streams for {total} merchants (single={single}, workers={max_workers if not single else 1})...")
    if single:
        for m in merchants:
            processed += 1
            try:
                r = run_one(m)
                results.append(r)
                nm = r.get("merchant_name")
                cnt = r.get("counts", {})
                done_counts = sum_counts(done_counts, cnt)
                print_counts(f"[streams] {processed}/{total} done: {nm} ->", cnt)
                print_counts(f"[streams] cumulative:", done_counts)
            except Exception as ex2:
                print(f"[streams] {processed}/{total} error: {ex2}")
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(run_one, m) for m in merchants]
            for fut in as_completed(futs):
                processed += 1
                try:
                    r = fut.result()
                    results.append(r)
                    nm = r.get("merchant_name")
                    cnt = r.get("counts", {})
                    done_counts = sum_counts(done_counts, cnt)
                    print_counts(f"[streams] {processed}/{total} done: {nm} ->", cnt)
                    print_counts(f"[streams] cumulative:", done_counts)
                except Exception as ex2:
                    print(f"[streams] {processed}/{total} error: {ex2}")

    took = round(time.time() - started, 2)
    print(f"[streams] Completed {processed}/{total} merchants in {took}s.")
    print_counts("[streams] Final cumulative:", done_counts)
    return results

# ---------------- CLI ----------------

def generate_preset_cli(args: argparse.Namespace) -> None:
    names: List[str] = []
    if args.names:
        names = [n.strip() for n in args.names.split(",") if n.strip()]
    elif args.names_file:
        try:
            with open(args.names_file, "r", encoding="utf-8") as f:
                for line in f:
                    ln = line.strip()
                    if ln:
                        names.append(ln)
        except Exception as e:
            print(f"Error reading names file: {e}")
            return
    if not names and args.num > 0:
        names = BASE_NAMES[: min(args.num, len(BASE_NAMES))]
        i = 0
        while len(names) < min(args.num, 5000):
            names.append(f"Merchant_{i}_{random.randint(1000,9999)}")
            i += 1

    if args.merchant_name:
        if not any(n.lower() == args.merchant_name.lower() for n in names):
            names.append(args.merchant_name)

    now = dt.datetime.now(dt.timezone.utc)
    start_date = args.start_date or (now - dt.timedelta(days=365*3)).strftime("%Y-%m-%d")
    end_date = args.end_date or (now + dt.timedelta(days=365)).strftime("%Y-%m-%d")

    customs: List[Dict[str, Any]] = []
    if args.custom:
        for p in args.custom:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        customs.extend(data)
                    elif isinstance(data, dict):
                        customs.append(data)
            except Exception as e:
                print(f"Warning: could not load custom {p}: {e}")

    print(f"[generate] Building preset for {len(names)} merchants...")
    t0 = time.time()
    preset = generate_preset(names, start_date, end_date, args.seed, customs or None)
    out_path = args.out or "preset.json"
    saved = save_preset(preset, out_path)
    t1 = time.time()
    print(f"[generate] Preset written to: {saved} in {round(t1 - t0, 2)}s")

    totals = {"reddit":0,"news":0,"reviews":0,"stocks_prices":0,"stocks_earnings":0,"stocks_actions":0,"wl_transactions":0}
    for m in preset.get("merchants", []):
        totals = sum_counts(totals, estimate_counts_for_block(m, start_date, end_date, scale=1.0, scale_stocks=False))
    print_counts("[generate] Estimated stream docs if you run 'streams':", totals)

    if not args.no_confirm:
        proceed = input("Ingest into MongoDB now? (y/N): ").strip().lower()
        if proceed in ("y", "yes"):
            results = ingest_preset_to_mongo(saved, mongo_uri=None, db_name=None, deep_scan=False, max_workers=8, single=False)
            print("Ingestion results:")
            for r in results:
                print(r)
        else:
            print("Skipping ingestion. You can ingest later using: python preset.py ingest --preset ...")

def ingest_preset_cli(args: argparse.Namespace) -> None:
    preset_path = args.preset
    if not os.path.exists(preset_path):
        print(f"Preset file not found: {preset_path}")
        return
    results = ingest_preset_to_mongo(
        preset_path,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        deep_scan=args.deep_scan,
        max_workers=args.max_workers,
        single=args.single
    )
    print("Ingestion completed. Summary:")
    for r in results:
        print(r)

    if getattr(args, "with_streams", False):
        print("Generating streams as requested (--with-streams)...")
        sres = generate_streams_from_preset(
            preset_path,
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            max_workers=args.max_workers,
            only=None,
            scale=args.scale,
            scale_stocks=args.scale_stocks,
            single=args.single
        )
        for r in sres:
            print(r)

def streams_preset_cli(args: argparse.Namespace) -> None:
    preset_path = args.preset
    if not os.path.exists(preset_path):
        print(f"Preset file not found: {preset_path}")
        return
    only = [s.strip() for s in (args.only or "").split(",")] if args.only else None
    results = generate_streams_from_preset(
        preset_path,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        max_workers=args.max_workers,
        only=only,
        scale=args.scale,
        scale_stocks=args.scale_stocks,
        single=args.single
    )
    print("Streams generation completed. Summary:")
    for r in results:
        print(r)

def generate_cli():
    parser = argparse.ArgumentParser(description="Preset: generate and/or ingest merchant data presets.")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate a preset.json for N merchants.")
    gen.add_argument("--num", type=int, required=True, help="Number of merchants to generate.")
    gen.add_argument("--start_date", type=str, default=None, help="Start date (YYYY-MM-DD). If omitted, defaults to now-3y.")
    gen.add_argument("--end_date", type=str, default=None, help="End date (YYYY-MM-DD). If omitted, defaults to now+1y.")
    gen.add_argument("--names", type=str, default=None, help="Comma-separated merchant names.")
    gen.add_argument("--names-file", type=str, default=None, help="Path to a file containing merchant names (one per line).")
    gen.add_argument("--out", type=str, default="preset.json", help="Output preset.json path.")
    gen.add_argument("--seed", type=int, default=777, help="Base RNG seed for reproducibility.")
    gen.add_argument("--custom", type=str, nargs="*", default=None, help="Path(s) to custom JSON blocks to override per-merchant blocks.")
    gen.add_argument("--merchant-name", type=str, default=None, help="Optional quick single merchant name.")
    gen.add_argument("--no-confirm", action="store_true", help="Do not wait for user confirmation before ingest (generate only).")

    ing = sub.add_parser("ingest", help="Ingest a preset.json into MongoDB using MerchantService.")
    ing.add_argument("--preset", type=str, required=True, help="Path to preset.json to ingest.")
    ing.add_argument("--mongo-uri", type=str, default=None, help="Mongo URI (overrides MONGO_URI env).")
    ing.add_argument("--db", type=str, default=None, help="Database name (overrides DB_NAME env).")
    ing.add_argument("--deep-scan", action="store_true", help="Run deep scan (force regen).")
    ing.add_argument("--max-workers", type=int, default=8, help="Max parallel ingestion workers.")
    ing.add_argument("--with-streams", action="store_true", help="Generate streams right after ingest.")
    ing.add_argument("--single", action="store_true", help="Run in single-threaded mode (no parallel workers).")
    ing.add_argument("--scale", type=float, default=1.0, help="Scale factor for counts when auto-running streams (0.0-1.0).")
    ing.add_argument("--scale-stocks", action="store_true", help="Scale stocks by compressing date window.")

    st = sub.add_parser("streams", help="Generate stream data for merchants from preset.json.")
    st.add_argument("--preset", type=str, required=True, help="Path to preset.json.")
    st.add_argument("--mongo-uri", type=str, default=None, help="Mongo URI (overrides env).")
    st.add_argument("--db", type=str, default=None, help="Database name (overrides env).")
    st.add_argument("--max-workers", type=int, default=8, help="Parallel workers.")
    st.add_argument("--only", type=str, default=None, help="Comma-separated subset: reddit,news,reviews,stock,wl")
    st.add_argument("--scale", type=float, default=1.0, help="Scale factor for counts (0.0-1.0).")
    st.add_argument("--scale-stocks", action="store_true", help="Scale stocks by compressing date window.")
    st.add_argument("--single", action="store_true", help="Run in single-threaded mode (no parallel workers).")

    args = parser.parse_args()

    if args.command == "generate":
        generate_preset_cli(args)
    elif args.command == "ingest":
        ingest_preset_cli(args)
    elif args.command == "streams":
        streams_preset_cli(args)

if __name__ == "__main__":
    generate_cli()