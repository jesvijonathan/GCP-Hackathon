# python evaluate.py --api-base http://127.0.0.1:8000 --merchant HomeGear --sim-now "2022-10-22 00:00:00" --mongo-uri "mongodb://127.0.0.1:27017" --db merchant_analytics --collection merchant_evaluations --progress-collection merchant_eval_progress --accel x1.0 --interval-mins 30 --max-workers 16 --backfill

# evaluate.py
import os
import sys
import time
import math
import json
import argparse
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pymongo import MongoClient, ASCENDING

# --------------------------- Helpers ---------------------------

def parse_multiplier(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower().replace(" ", "")
    try:
        if s.endswith("%"):
            return 1.0 + float(s[:-1]) / 100.0
        if s.startswith("x"):
            return float(s[1:])
        if "x" in s and s.count("x") == 1:
            return float(s.split("x", 1)[1])
        return float(s)
    except:
        return None

def parse_window_to_timedelta(window: str) -> dt.timedelta:
    import re
    s = (window or "").strip().lower()
    m = re.match(r"^(\d+)\s*([smhdw])$", s)
    if not m:
        raise ValueError("Window must look like 15m, 1h, 24h, 7d, 1w")
    n = int(m.group(1)); unit = m.group(2)
    return {"s":dt.timedelta(seconds=n),"m":dt.timedelta(minutes=n),"h":dt.timedelta(hours=n),"d":dt.timedelta(days=n),"w":dt.timedelta(weeks=n)}[unit]

def parse_any_dt(x: Optional[str]) -> Optional[dt.datetime]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        sec = float(x)
        if sec > 1e12:
            sec = sec / 1000.0
        return dt.datetime.fromtimestamp(sec, tz=dt.UTC)
    s = str(x).strip()
    if not s:
        return None
    sl = s.lower()
    if sl in ("now", "utcnow", "current"):
        return dt.datetime.now(dt.UTC)
    import re
    m = re.match(r"^now\s*([+-])\s*(\d+)\s*([smhdw])$", sl)
    if m:
        sign = -1 if m.group(1) == "-" else 1
        num = int(m.group(2))
        unit = m.group(3)
        td = parse_window_to_timedelta(f"{num}{unit}")
        return dt.datetime.now(dt.UTC) + (sign * td)
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if "T" in s or "+" in s or s.count("-") >= 2:
            d0 = dt.datetime.fromisoformat(s)
            return d0 if d0.tzinfo else d0.replace(tzinfo=dt.UTC)
    except Exception:
        pass
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            d0 = dt.datetime.strptime(s, fmt)
            if fmt in ("%Y/%m/%d", "%Y-%m-%d"):
                d0 = d0.replace(hour=23, minute=59, second=59)
            return d0.replace(tzinfo=dt.UTC)
        except Exception:
            continue
    try:
        v = float(s)
        return dt.datetime.fromtimestamp(v, tz=dt.UTC)
    except Exception:
        pass
    raise ValueError(f"Unrecognized datetime format: {x}")

def dt_to_iso_z(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.UTC)
    return dt_obj.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

def ensure_utc(dt_obj: dt.datetime) -> dt.datetime:
    return dt_obj if dt_obj.tzinfo else dt_obj.replace(tzinfo=dt.UTC)

# --------------------------- API client ---------------------------

class DataAPI:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def merchants(self) -> List[str]:
        r = self.session.get(f"{self.base_url}/v1/merchants", timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("merchants") or []

    def get_data(self, merchant: str, since_iso: Optional[str], until_iso: Optional[str], include_stock_meta: bool = False, allow_future: bool = False, order: str = "asc", limit: int = 0) -> Dict[str, Any]:
        params = {
            "streams": "all",
            "since": since_iso,
            "until": until_iso,
            "limit": limit,  # limit<=0 => unlimited
            "order": order,
            "include_stock_meta": include_stock_meta,
            "allow_future": allow_future
        }
        r = self.session.get(f"{self.base_url}/v1/{merchant}/data", params={k: v for k, v in params.items() if v is not None}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def stock_meta_start(self, merchant: str) -> Optional[dt.datetime]:
        params = {"include_stock_meta": True, "limit": 1, "window": "1d"}
        r = self.session.get(f"{self.base_url}/v1/{merchant}/stock", params=params, timeout=self.timeout)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        meta = data.get("meta") or {}
        sd = meta.get("start_date")
        if sd:
            try:
                return parse_any_dt(str(sd))
            except Exception:
                return None
        return None

# --------------------------- Mongo sink ---------------------------

class EvalStore:
    def __init__(self, mongo_uri: str, db_name: str, collection: str, progress_collection: Optional[str] = None):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db[collection]
        self.progress_coll = self.db[progress_collection] if progress_collection else None
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.coll.create_index([("merchant", ASCENDING), ("window_end", ASCENDING)], unique=True)
        self.coll.create_index([("merchant", ASCENDING), ("window_end_ts", ASCENDING)])

        if self.progress_coll is not None:
            self.progress_coll.create_index([("merchant", ASCENDING), ("phase", ASCENDING)], unique=True)

    def upsert_eval(self, doc: Dict[str, Any]):
        self.coll.update_one(
            {"merchant": doc["merchant"], "window_end": doc["window_end"]},
            {"$set": doc},
            upsert=True
        )

    def update_progress(self, merchant: str, phase: str, payload: Dict[str, Any]):
        if self.progress_coll is None:
            return
        doc = {"merchant": merchant, "phase": phase}
        self.progress_coll.update_one(doc, {"$set": dict(doc, **payload)}, upsert=True)

# --------------------------- Risk model ---------------------------

def _sentiment_val(label: Optional[str]) -> float:
    if not label:
        return 0.0
    l = str(label).lower()
    if l == "positive": return 1.0
    if l == "negative": return -1.0
    return 0.0

def _safe_mean(arr: List[float]) -> float:
    arr = [float(x) for x in arr if x is not None]
    return (sum(arr) / len(arr)) if arr else 0.0

def _zscore(value: float, mean: float, std: float) -> float:
    if std is None or std <= 1e-12:
        return 0.0
    return (value - mean) / std

def compute_wl_component(wl_txns: List[Dict[str, Any]]) -> Tuple[float, Dict[str, Any]]:
    n = len(wl_txns)
    if n == 0:
        return 0.0, {"count": 0}
    avg_risk = _safe_mean([t.get("risk_score", 0.0) for t in wl_txns])
    status = [t.get("status") for t in wl_txns]
    declines = sum(1 for s in status if s == "declined")
    chargebacks = sum(1 for s in status if s == "chargeback")
    decline_rate = declines / max(1, n)
    chargeback_rate = chargebacks / max(1, n)
    international_share = sum(1 for t in wl_txns if t.get("international")) / max(1, n)
    offline_share = sum(1 for t in wl_txns if t.get("offline")) / max(1, n)
    shady_share = sum(1 for t in wl_txns if t.get("shady_region")) / max(1, n)
    high_risk_mcc_share = sum(1 for t in wl_txns if (t.get("risk_flags") or {}).get("high_risk_mcc")) / max(1, n)
    banned_hits = sum(1 for t in wl_txns if t.get("banned_card")) / max(1, n)
    rf_all = []
    for t in wl_txns:
        rfl = t.get("risk_factors") or []
        rf_all.extend([x for x in rfl if x])
    wmap = {
        "gambling": 2.0, "crypto_transfer": 2.0, "adult_content": 1.5,
        "card_not_present": 1.1, "cross_border": 1.2, "offline_swipe": 1.1,
        "blocked_card": 2.0, "velocity": 1.6, "synthetic_id": 1.8,
        "disputed": 2.2, "refund_abuse": 1.3, "merchant_risk_category": 1.2
    }
    factor_score = 0.0
    if rf_all:
        from collections import Counter
        c = Counter(rf_all)
        for k, v in c.items():
            factor_score += wmap.get(k, 1.0) * (v / max(1, n))
    wl_score = (
        avg_risk * 0.60 +
        (decline_rate * 100.0) * 0.25 +
        (chargeback_rate * 100.0) * 0.25 +
        (international_share * 100.0) * 0.05 +
        (offline_share * 100.0) * 0.03 +
        (shady_share * 100.0) * 0.10 +
        (high_risk_mcc_share * 100.0) * 0.08 +
        (banned_hits * 100.0) * 0.06 +
        (factor_score * 20.0)
    )
    wl_score = max(0.0, min(100.0, wl_score))
    details = {
        "count": n,
        "avg_risk": round(avg_risk, 2),
        "decline_rate": round(decline_rate, 4),
        "chargeback_rate": round(chargeback_rate, 4),
        "international_share": round(international_share, 4),
        "offline_share": round(offline_share, 4),
        "shady_share": round(shady_share, 4),
        "high_risk_mcc_share": round(high_risk_mcc_share, 4),
        "banned_hits_share": round(banned_hits, 4)
    }
    return wl_score, details

def compute_market_component(stock: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    prices = (stock or {}).get("prices") or []
    if not prices:
        return 0.0, {"count": 0}
    abs_ret = _safe_mean([abs(p.get("return_pct") or 0.0) for p in prices]) * 100.0
    vold = _safe_mean([p.get("volatility_day") or 0.0 for p in prices])
    vol_annual_pct = (vold / math.sqrt(1.0/252.0)) * 100.0 if vold > 0 else 0.0
    vol_spike_proxy = _safe_mean([float(p.get("event_intensity") or 0.0) for p in prices]) * 100.0
    earn_days = sum(1 for p in prices if p.get("is_earnings"))
    mkt_score = (
        abs_ret * 0.50 +
        vol_annual_pct * 0.30 +
        vol_spike_proxy * 0.20 +
        earn_days * 3.0
    )
    mkt_score = max(0.0, min(100.0, mkt_score))
    details = {
        "count": len(prices),
        "abs_return_pct_mean": round(abs_ret, 3),
        "vol_annual_pct": round(vol_annual_pct, 3),
        "event_intensity_mean": round(vol_spike_proxy, 3),
        "earnings_days": earn_days
    }
    return mkt_score, details

def compute_sentiment_component(streams: Dict[str, List[Dict[str, Any]]]) -> Tuple[float, Dict[str, Any]]:
    sentiments = []
    counts = {}
    for name in ("tweets", "reddit", "news", "reviews"):
        items = streams.get(name) or []
        counts[name] = len(items)
        for it in items:
            label = it.get("sentiment_label")
            sentiments.append(_sentiment_val(label))
    mean_sent = _safe_mean(sentiments)
    sent_risk = (1.0 - ((mean_sent + 1.0) / 2.0)) * 100.0
    news_items = streams.get("news") or []
    news_risks = [float(x.get("risk_score") or 0.0) for x in news_items if x.get("risk_score") is not None]
    news_boost = _safe_mean(news_risks) * 0.10
    score = max(0.0, min(100.0, sent_risk + news_boost))
    details = {
        "mean_sentiment": round(mean_sent, 4),
        "sentiment_risk": round(sent_risk, 2),
        "news_risk_boost": round(news_boost, 2),
        "counts": counts
    }
    return score, details

def compute_volume_component(streams: Dict[str, List[Dict[str, Any]]], baseline: Dict[str, Tuple[float, float]]) -> Tuple[float, Dict[str, Any]]:
    counts = {k: len(streams.get(k) or []) for k in ("tweets", "reddit", "news", "reviews")}
    buzz = float(sum(counts.values()))
    mean, std = baseline.get("buzz", (0.0, 0.0))
    z = _zscore(buzz, mean, std)
    score = max(0.0, min(100.0, (max(0.0, z) * 15.0)))
    details = {"buzz": buzz, "z": round(z, 3), "counts": counts, "baseline_mean": round(mean, 3), "baseline_std": round(std, 3)}
    return score, details

def aggregate_risk(wl_score: float, mkt_score: float, sent_score: float, vol_score: float, incident_bump: float = 0.0) -> float:
    base = wl_score * 0.45 + mkt_score * 0.25 + sent_score * 0.20 + vol_score * 0.10
    total = base + incident_bump
    return max(0.0, min(100.0, total))

def extract_incident_bump(stock: Dict[str, Any], wl_txns: List[Dict[str, Any]]) -> Tuple[float, List[str]]:
    drivers = []
    bump = 0.0
    for p in (stock or {}).get("prices") or []:
        el = str(p.get("event_label") or "").lower()
        if any(k in el for k in ["breach", "outage", "recall", "lawsuit", "fine", "scandal"]):
            bump += 2.0
            drivers.append(f"stock_event:{el}")
    labels = [str(t.get("event_label") or "").lower() for t in wl_txns]
    if any("fraud ring" in l for l in labels):
        bump += 4.0; drivers.append("wl_event:fraud ring")
    if any("system outage" in l for l in labels):
        bump += 3.0; drivers.append("wl_event:system outage")
    if any("gambling spike" in l for l in labels):
        bump += 3.0; drivers.append("wl_event:gambling spike")
    return bump, drivers

# --------------------------- Evaluator ---------------------------

class MerchantEvaluator:
    def __init__(self, api: DataAPI, store: EvalStore, merchants: List[str], interval: dt.timedelta, sim_now_start: dt.datetime, accel: float, include_stock_meta: bool = True, max_workers: int = 8):
        self.api = api
        self.store = store
        self.merchants = merchants
        self.interval = interval
        self.sim_now_start = ensure_utc(sim_now_start)
        self.wall_start_ts = time.time()
        self.accel = max(0.0, float(accel))
        self.include_stock_meta = include_stock_meta
        self.max_workers = max(1, int(max_workers))
        self.baselines: Dict[str, Dict[str, Tuple[float, float]]] = {}
        self.progress_lock = threading.Lock()

    def current_sim_now(self) -> dt.datetime:
        elapsed = time.time() - self.wall_start_ts
        sim_now = self.sim_now_start + dt.timedelta(seconds=elapsed * self.accel)
        return ensure_utc(sim_now)

    def earliest_dt(self, merchant: str) -> Optional[dt.datetime]:
        sd = self.api.stock_meta_start(merchant)
        if sd:
            return ensure_utc(sd)
        try:
            resp = self.api.get_data(merchant, None, None, include_stock_meta=False, allow_future=False, order="asc", limit=1)
            rng = (resp.get("range") or {}).get("tweets") or {}
            since = rng.get("since")
            if since:
                return ensure_utc(parse_any_dt(since))
        except Exception:
            pass
        return None

    def _last_window_end(self, merchant: str) -> Optional[dt.datetime]:
        doc = self.store.coll.find_one({"merchant": merchant}, sort=[("window_end_ts", -1)])
        if doc and doc.get("window_end"):
            try:
                return ensure_utc(parse_any_dt(doc["window_end"]))
            except Exception:
                return None
        return None

    def _evaluate_window(self, merchant: str, win_start: dt.datetime, win_end: dt.datetime, baseline: Dict[str, Tuple[float, float]], is_backfill: bool) -> float:
        since_iso = dt_to_iso_z(win_start)
        until_iso = dt_to_iso_z(win_end)
        resp = self.api.get_data(merchant, since_iso, until_iso, include_stock_meta=self.include_stock_meta, allow_future=False, order="asc", limit=0)
        data = resp.get("data") or {}
        tweets = data.get("tweets") or []
        reddit = data.get("reddit") or []
        news = data.get("news") or []
        reviews = data.get("reviews") or []
        wl = data.get("wl") or []
        stock = data.get("stock") or {}
        buzz_count = float(len(tweets) + len(reddit) + len(news) + len(reviews))
        wl_score, wl_details = compute_wl_component(wl)
        mkt_score, mkt_details = compute_market_component(stock)
        sent_score, sent_details = compute_sentiment_component({"tweets": tweets, "reddit": reddit, "news": news, "reviews": reviews})
        vol_score, vol_details = compute_volume_component({"tweets": tweets, "reddit": reddit, "news": news, "reviews": reviews}, {"buzz": baseline.get("buzz", (0.0, 0.0))})
        bump, bump_drivers = extract_incident_bump(stock, wl)
        total = aggregate_risk(wl_score, mkt_score, sent_score, vol_score, incident_bump=bump)
        total_records = len(wl) + len(tweets) + len(reddit) + len(news) + len(reviews) + len(stock.get("prices") or [])
        confidence = max(0.0, min(1.0, (total_records / 200.0)))
        drivers = []
        if wl_details.get("decline_rate", 0) > 0.10: drivers.append("High decline rate")
        if wl_details.get("chargeback_rate", 0) > 0.02: drivers.append("Chargebacks elevated")
        if wl_details.get("high_risk_mcc_share", 0) > 0.10: drivers.append("High-risk MCC exposure")
        if wl_details.get("shady_share", 0) > 0.05: drivers.append("Shady regions detected")
        if mkt_details.get("abs_return_pct_mean", 0) > 2.0: drivers.append("Price swings")
        if mkt_details.get("vol_annual_pct", 0) > 40.0: drivers.append("Volatility elevated")
        if sent_details.get("mean_sentiment", 0) < -0.2: drivers.append("Negative sentiment")
        if vol_details.get("z", 0) > 2.0: drivers.append("Activity spike")
        drivers.extend(bump_drivers)
        doc = {
            "merchant": merchant,
            "window_start": since_iso,
            "window_end": until_iso,
            "window_start_ts": win_start.timestamp(),
            "window_end_ts": win_end.timestamp(),
            "sim_now": dt_to_iso_z(self.current_sim_now()),
            "is_backfill": bool(is_backfill),
            "scores": {
                "total": round(total, 2),
                "wl": round(wl_score, 2),
                "market": round(mkt_score, 2),
                "sentiment": round(sent_score, 2),
                "volume": round(vol_score, 2),
                "incident_bump": round(bump, 2)
            },
            "confidence": round(confidence, 3),
            "details": {
                "wl": wl_details,
                "market": mkt_details,
                "sentiment": sent_details,
                "volume": vol_details
            },
            "counts": {
                "tweets": len(tweets),
                "reddit": len(reddit),
                "news": len(news),
                "reviews": len(reviews),
                "wl": len(wl),
                "stock_prices": len(stock.get("prices") or [])
            },
            "drivers": drivers,
            "created_at": dt_to_iso_z(dt.datetime.now(dt.UTC))
        }
        self.store.upsert_eval(doc)
        return buzz_count

    def backfill_and_loop(self):
        target_now = self.current_sim_now()
        all_windows: List[Tuple[str, dt.datetime, dt.datetime]] = []
        per_merchant_buzz: Dict[str, List[float]] = defaultdict(list)
        locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

        # Build backfill windows
        for merchant in self.merchants:
            start_dt = self.earliest_dt(merchant)
            if not start_dt:
                print(f"[WARN] Could not determine earliest date for {merchant}; skipping.")
                continue
            cur = start_dt
            while cur < target_now:
                win_start = cur
                win_end = min(cur + self.interval, target_now)
                all_windows.append((merchant, win_start, win_end))
                cur = win_end

        total = len(all_windows)
        if total == 0:
            print("[INFO] No backfill windows; proceeding to continuous loop.")
        else:
            print(f"[INFO] Backfill: {total} windows across {len(self.merchants)} merchants (interval={self.interval}).")

        started_ts = time.time()
        completed = 0

        # Execute backfill in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = []
            for (merchant, ws, we) in all_windows:
                futures.append(ex.submit(self._evaluate_window, merchant, ws, we, {"buzz": (0.0, 0.0)}, True))
            for fut in as_completed(futures):
                try:
                    buzz_count = float(fut.result())
                except Exception as e:
                    buzz_count = 0.0
                    print(f"[ERROR] Backfill window failed: {e}")
                with self.progress_lock:
                    completed += 1
                    elapsed = max(1e-6, time.time() - started_ts)
                    rate = completed / elapsed
                    percent = (completed / max(1, total)) * 100.0
                    eta_sec = (total - completed) / max(1e-6, rate)
                    if completed % max(1, total // 100 or 10) == 0 or completed == total:
                        print(f"[PROGRESS] {completed}/{total} ({percent:.2f}%) | {rate:.2f} win/s | ETA {eta_sec:.1f}s")
                # Record per-merchant buzz (needs merchant id; we don't have here)
                # We can’t know merchant here because result doesn’t carry it; adjust by wrapping payload:
                # For simplicity, skip precise per-merchant buzz collection here; compute baseline later from stored docs.

        # Compute baselines from stored docs (per-merchant)
        for merchant in self.merchants:
            # Derive buzz baseline from backfill docs in DB
            cursor = self.store.coll.find({"merchant": merchant, "is_backfill": True}, projection={"counts": 1}, sort=[("window_end_ts", ASCENDING)])
            buzz_vals = []
            for d in cursor:
                c = (d.get("counts") or {})
                buzz_vals.append(float(c.get("tweets", 0) + c.get("reddit", 0) + c.get("news", 0) + c.get("reviews", 0)))
            if buzz_vals:
                mean_buzz = sum(buzz_vals) / len(buzz_vals)
                std_buzz = (sum((b - mean_buzz) ** 2 for b in buzz_vals) / max(1, len(buzz_vals))) ** 0.5
                self.baselines[merchant] = {"buzz": (mean_buzz, std_buzz)}
            else:
                self.baselines[merchant] = {"buzz": (0.0, 0.0)}
            # Update progress doc
            self.store.update_progress(merchant, "backfill", {
                "completed_windows": completed,
                "total_windows": total,
                "percent": round((completed / max(1, total)) * 100.0, 2),
                "eta_sec": 0.0
            })

        print("[INFO] Starting continuous 30-minute evaluation loop (parallel across merchants)...")
        # Continuous loop: evaluate next window per merchant when ready, in parallel
        while True:
            tasks = []
            due = []
            sim_now = self.current_sim_now()
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                for merchant in self.merchants:
                    baseline = self.baselines.get(merchant) or {"buzz": (0.0, 0.0)}
                    last_end = self._last_window_end(merchant)
                    if not last_end:
                        last_end = self.earliest_dt(merchant) or sim_now - self.interval
                    next_start = last_end
                    next_end = next_start + self.interval
                    if next_end <= sim_now:
                        due.append((merchant, next_start, next_end))
                        tasks.append(ex.submit(self._evaluate_window, merchant, next_start, next_end, baseline, False))
                if tasks:
                    for fut in as_completed(tasks):
                        try:
                            fut.result()
                        except Exception as e:
                            print(f"[ERROR] Continuous window failed: {e}")
                else:
                    # Sleep until simulated time reaches the earliest next_end
                    # Compute minimum delta among merchants
                    min_wait_sec = 2.0
                    for merchant in self.merchants:
                        last_end = self._last_window_end(merchant)
                        if not last_end:
                            continue
                        next_start = last_end
                        next_end = next_start + self.interval
                        delta_sim = (next_end - sim_now).total_seconds()
                        wait_sec = delta_sim / max(1e-9, self.accel)
                        if wait_sec > 0:
                            min_wait_sec = min(min_wait_sec, wait_sec)
                    time.sleep(max(0.5, min_wait_sec))

# --------------------------- CLI ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Merchant risk evaluation engine (parallel backfill + parallel continuous).")
    ap.add_argument("--api-base", default=os.getenv("DATA_API_BASE", "http://127.0.0.1:8000"))
    ap.add_argument("--merchant", action="append", help="Merchant name (repeatable). If omitted, evaluates all merchants from API.")
    ap.add_argument("--sim-now", required=True, help="Simulated 'now' start (ISO, 'YYYY/MM/DD HH:MM:SS', 'now', or epoch).")
    ap.add_argument("--accel", default="x1.0", help="Time acceleration (e.g., 10, x5000, 200%).")
    ap.add_argument("--interval-mins", type=int, default=30, help="Evaluation cadence in minutes (default: 30).")
    ap.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "merchant_analytics"))
    ap.add_argument("--collection", default="merchant_evaluations")
    ap.add_argument("--progress-collection", default="merchant_eval_progress", help="Optional collection to store progress/percent.")
    ap.add_argument("--max-workers", type=int, default=min(32, (os.cpu_count() or 4) * 4), help="Thread pool size for parallel windows/merchants.")
    ap.add_argument("--backfill", action="store_true", help="Perform initial backfill from earliest date to sim-now, then loop.")
    args = ap.parse_args()

    try:
        sim_now_start = parse_any_dt(args.sim_now)
    except Exception as e:
        print(f"[ERROR] Invalid --sim-now: {e}")
        sys.exit(1)

    accel = parse_multiplier(args.accel) or 1.0
    interval = dt.timedelta(minutes=max(1, args.interval_mins))

    api = DataAPI(args.api_base)
    if args.merchant:
        merchants = args.merchant
    else:
        try:
            merchants = api.merchants()
        except Exception as e:
            print(f"[ERROR] Could not fetch merchants: {e}")
            sys.exit(1)
    if not merchants:
        print("[ERROR] No merchants to evaluate.")
        sys.exit(1)

    store = EvalStore(args.mongo_uri, args.db, args.collection, progress_collection=args.progress_collection)
    evaluator = MerchantEvaluator(api, store, merchants, interval, sim_now_start, accel, include_stock_meta=True, max_workers=args.max_workers)

    if args.backfill:
        evaluator.backfill_and_loop()
    else:
        print("[INFO] Skipping backfill; running continuous loop from sim-now.")
        # Evaluate initial window ending at sim-now per merchant in parallel
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            tasks = []
            for merchant in merchants:
                baseline = {"buzz": (0.0, 0.0)}
                last_end = evaluator.earliest_dt(merchant) or evaluator.current_sim_now() - interval
                next_start = last_end
                next_end = next_start + interval
                while next_end <= evaluator.current_sim_now():
                    tasks.append(ex.submit(evaluator._evaluate_window, merchant, next_start, next_end, baseline, False))
                    next_start = next_end
                    next_end = next_start + interval
            for fut in as_completed(tasks):
                try:
                    fut.result()
                except Exception as e:
                    print(f"[ERROR] Initial continuous window failed: {e}")
        evaluator.backfill_and_loop()

if __name__ == "__main__":
    main()

# python3 .\evaluator.py --api-base http://127.0.0.1:8000 --merchant HomeGear --sim-now "2022-10-22 00:00:00" --mongo-uri "mongodb://127.0.0.1:27017" --db merchant_analytics --collection merchant_evaluations --progress-collection merchant_eval_progress --accel x1.0 --interval-mins 30 --max-workers 16 --backfill