#!/usr/bin/env python3
"""Simplified risk evaluator.

Removes background job/backfill complexity. Windows are computed on-demand when
requested via API (fetch or summary). Each window document (risk_scores) is
upserted so repeated requests are cheap.
"""
from __future__ import annotations
import datetime as dt
import statistics
import uuid  # left for potential future use (idempotent ops)
import threading  # retained but not heavily used after simplification
import os
from typing import Dict, Any, List, Optional, Tuple

from pymongo.collection import Collection
from pymongo import ASCENDING, ReturnDocument

# Collections to scan for earliest ts references
STREAM_COLLECTIONS = [
    "tweets", "reddit", "news", "reviews", "wl_transactions", "stocks_prices"
]

INTERVAL_MAP = {
    "30m": 30,
    "1h": 60,
    "1d": 60*24,
    "30min": 30,
    "1hour": 60,
    "1day": 60*24
}

COMPONENT_WEIGHTS = {
    "tweet_sentiment": 1.0,
    "reddit_sentiment": 1.0,
    "news_sentiment": 1.0,
    "reviews_rating": 1.0,
    "wl_flag_ratio": 1.5,
    "stock_volatility": 1.0
}

# Alternate / fallback field names for dynamic inference
ALT_SENTIMENT_FIELDS = [
    "sentiment", "compound", "polarity", "score", "sentiment_score", "vader_compound"
]
ALT_REVIEW_FIELDS = ["rating", "stars", "score"]
ALT_PRICE_FIELDS = ["price", "close", "adj_close", "value"]
ALT_STATUS_FIELDS = ["status", "state"]

class RiskEvaluator:
    def __init__(self, db):
        self.db = db
        self.col: Collection = db["risk_scores"]
        self._ts_scale_cache: Dict[tuple, str] = {}
        self._positive_words = {"good","great","positive","win","launch","feature","award","impressed","strong","excited","growth","up","improve","excellent","love","amazing","awesome","partnership","expansion"}
        self._negative_words = {"bad","negative","breach","fraud","lawsuit","outage","recall","scandal","issue","problem","downtime","leak","concern","concerns","weak","loss","down","decline","bug","risk","risks","angry","disappointed"}
        # Minimal in-memory job registry (compat shim for existing endpoints)
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def ensure_indexes(self):
        self.col.create_index([
            ("merchant", ASCENDING),
            ("interval_minutes", ASCENDING),
            ("window_start_ts", ASCENDING)
        ], unique=True, name="uniq_window")
        self.col.create_index([("merchant", ASCENDING), ("interval_minutes", ASCENDING), ("window_end_ts", ASCENDING)])

    # ---------- Utilities ----------

    @staticmethod
    def _iso(ts: float) -> str:
        return dt.datetime.fromtimestamp(ts, tz=dt.UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _floor_window(ts: float, interval_minutes: int) -> float:
        size_s = interval_minutes * 60
        return (int(ts) // size_s) * size_s

    @staticmethod
    def _normalize_sentiment(avg_sent: Optional[float]) -> Optional[float]:
        if avg_sent is None:
            return None
        # Assume avg_sent in [-1,1], convert so 1 means highest risk => invert positivity
        return max(0.0, min(1.0, 1 - (avg_sent + 1) / 2))

    @staticmethod
    def _normalize_reviews(avg_rating: Optional[float]) -> Optional[float]:
        if avg_rating is None:
            return None
        # rating 1..5 (or 0..5) -> risk high when rating low
        return max(0.0, min(1.0, 1 - (avg_rating / 5.0)))

    @staticmethod
    def _normalize_volatility(std_ret: Optional[float]) -> Optional[float]:
        if std_ret is None:
            return None
        # Normalize by 5% typical daily-ish std; cap at 1
        return max(0.0, min(1.0, std_ret / 0.05))

    @staticmethod
    def _ratio(num: int, den: int) -> Optional[float]:
        if den <= 0:
            return None
        return num / den

    def earliest_timestamp(self, merchant: str) -> Optional[float]:
        earliest: Optional[float] = None
        for cname in STREAM_COLLECTIONS:
            doc = self.db[cname].find_one({"merchant": merchant}, sort=[("ts", ASCENDING)], projection={"ts": 1, "_id": 0})
            if not doc:
                # case-insensitive fallback (always attempt if exact not found)
                try:
                    doc = self.db[cname].find_one({"merchant": {"$regex": f"^{merchant}$", "$options": "i"}}, sort=[("ts", ASCENDING)], projection={"ts":1, "_id":0})
                except Exception:
                    doc = None
            if not doc:
                continue
            t_parsed = self._parse_ts(doc.get("ts"))
            if t_parsed is None:
                continue
            if earliest is None or t_parsed < earliest:
                earliest = t_parsed
        return earliest

    def last_scored_end(self, merchant: str, interval_minutes: int) -> Optional[float]:
        doc = self.col.find_one({"merchant": merchant, "interval_minutes": interval_minutes}, sort=[("window_end_ts", -1)], projection={"window_end_ts":1, "_id":0})
        if doc:
            return float(doc["window_end_ts"])
        return None

    # ---------- Component extraction helpers ----------

    def _avg_sentiment(self, coll_name: str, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _meta = self._fetch_interval_docs(coll_name, merchant, start_ts, end_ts)
        if not docs:
            return None
        vals: List[float] = []
        for d in docs:
            val = None
            for f in ALT_SENTIMENT_FIELDS:
                if f in d:
                    try:
                        vf = float(d.get(f))
                        if -10 <= vf <= 10:
                            val = vf
                            break
                    except Exception:
                        continue
            if val is None:
                # scan any numeric -1..1
                for k,v in d.items():
                    if k == 'ts':
                        continue
                    try:
                        fv = float(v)
                    except Exception:
                        continue
                    if -1 <= fv <= 1:
                        val = fv
                        break
            if val is None:
                # Heuristic text-based inference (fallback)
                txt_fields = []
                for tf in ("text","content","headline","title","review","body"):
                    if tf in d and isinstance(d[tf], str):
                        txt_fields.append(d[tf])
                if txt_fields:
                    score = self._infer_sentiment_from_text(" ".join(txt_fields))
                    if score is not None:
                        val = score
            if val is not None:
                val = max(-1.0, min(1.0, val))
                vals.append(val)
        if not vals:
            return None
        return sum(vals)/len(vals)

    def _avg_reviews(self, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _meta = self._fetch_interval_docs("reviews", merchant, start_ts, end_ts)
        if not docs:
            return None
        vals: List[float] = []
        for d in docs:
            val = None
            for f in ALT_REVIEW_FIELDS:
                if f in d:
                    try:
                        fv = float(d.get(f))
                        if -1 <= fv <= 10:
                            val = fv
                            break
                    except Exception:
                        continue
            if val is not None:
                vals.append(max(0.0, min(5.0, val)))
        if not vals:
            return None
        return sum(vals)/len(vals)

    def _wl_flag_ratio(self, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _meta = self._fetch_interval_docs("wl_transactions", merchant, start_ts, end_ts)
        if not docs:
            return None
        total=0; flagged=0
        for d in docs:
            status_val = None
            for f in ALT_STATUS_FIELDS:
                if f in d:
                    status_val = str(d.get(f, "")).lower()
                    break
            if not status_val:
                continue
            total += 1
            if any(k in status_val for k in ["flag", "fraud", "chargeback", "blocked", "suspicious"]):
                flagged += 1
        return self._ratio(flagged, total)

    def _stock_volatility(self, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _meta = self._fetch_interval_docs("stocks_prices", merchant, start_ts, end_ts, sample_limit=1200)
        if len(docs) < 3:
            return None
        prices: List[float] = []
        for d in docs:
            val = None
            for f in ALT_PRICE_FIELDS:
                if f in d:
                    try:
                        fv = float(d.get(f))
                        if fv > 0:
                            val = fv
                            break
                    except Exception:
                        continue
            if val is not None:
                prices.append(val)
        if len(prices) < 3:
            return None
        rets = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                rets.append((prices[i] - prices[i-1]) / prices[i-1])
        if len(rets) < 2:
            return None
        try:
            return statistics.pstdev(rets)
        except Exception:
            return None

        # ---------- Unified collection fetch (experimental) ----------
        def _collect_window_data(self, merchant: str, start_ts: float, end_ts: float, sample_limit: int = 600) -> Dict[str, Dict[str, Any]]:
            """Fetch all underlying stream docs once per window.
            Returns mapping: collection_name -> {docs, meta, count, strategy}.
            This avoids re-querying for both averages and counts; still capped by sample_limit.
            """
            collections = {
                "tweets": "tweets",
                "reddit": "reddit",
                "news": "news",
                "reviews": "reviews",
                "wl": "wl_transactions",
                "prices": "stocks_prices",
            }
            bundle: Dict[str, Dict[str, Any]] = {}
            for label, cname in collections.items():
                docs, meta = self._fetch_interval_docs(cname, merchant, start_ts, end_ts, sample_limit=sample_limit)
                # Count strategy: if meta used then len(docs) else 0, but also attempt fallback count only if no docs
                if meta.get("used"):
                    count = len(docs)
                    strategy = meta.get("used")
                else:
                    # fallback to existing counting logic (expensive) only if we truly have no docs
                    count, strategy = self._count_interval_docs(cname, merchant, start_ts, end_ts)
                bundle[label] = {"docs": docs, "meta": meta, "count": count, "strategy": strategy}
            return bundle

        def _extract_sentiment_from_docs(self, docs: List[Dict[str, Any]]) -> Optional[float]:
            if not docs:
                return None
            vals: List[float] = []
            for d in docs:
                val = None
                for f in ALT_SENTIMENT_FIELDS:
                    if f in d:
                        try:
                            vf = float(d.get(f))
                            if -10 <= vf <= 10:
                                val = vf
                                break
                        except Exception:
                            continue
                if val is None:
                    # scan numeric -1..1
                    for k, v in d.items():
                        if k == "ts":
                            continue
                        try:
                            fv = float(v)
                        except Exception:
                            continue
                        if -1 <= fv <= 1:
                            val = fv
                            break
                if val is None:
                    txt_fields = []
                    for tf in ("text","content","headline","title","review","body"):
                        if tf in d and isinstance(d[tf], str):
                            txt_fields.append(d[tf])
                    if txt_fields:
                        score = self._infer_sentiment_from_text(" ".join(txt_fields))
                        if score is not None:
                            val = score
                if val is not None:
                    vals.append(max(-1.0, min(1.0, val)))
            if not vals:
                return None
            return sum(vals) / len(vals)

        def _extract_reviews_from_docs(self, docs: List[Dict[str, Any]]) -> Optional[float]:
            if not docs:
                return None
            vals: List[float] = []
            for d in docs:
                val = None
                for f in ALT_REVIEW_FIELDS:
                    if f in d:
                        try:
                            fv = float(d.get(f))
                            if -1 <= fv <= 10:
                                val = fv
                                break
                        except Exception:
                            continue
                if val is not None:
                    vals.append(max(0.0, min(5.0, val)))
            if not vals:
                return None
            return sum(vals) / len(vals)

        def _extract_wl_ratio_from_docs(self, docs: List[Dict[str, Any]]) -> Optional[float]:
            if not docs:
                return None
            total = 0
            flagged = 0
            for d in docs:
                status_val = None
                for f in ALT_STATUS_FIELDS:
                    if f in d:
                        status_val = str(d.get(f, "")).lower()
                        break
                if not status_val:
                    continue
                total += 1
                if any(k in status_val for k in ["flag", "fraud", "chargeback", "blocked", "suspicious"]):
                    flagged += 1
            return self._ratio(flagged, total)

        def _extract_volatility_from_docs(self, docs: List[Dict[str, Any]]) -> Optional[float]:
            if not docs or len(docs) < 3:
                return None
            prices: List[float] = []
            for d in docs:
                val = None
                for f in ALT_PRICE_FIELDS:
                    if f in d:
                        try:
                            fv = float(d.get(f))
                            if fv > 0:
                                val = fv
                                break
                        except Exception:
                            continue
                if val is not None:
                    prices.append(val)
            if len(prices) < 3:
                return None
            rets = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    rets.append((prices[i] - prices[i-1]) / prices[i-1])
            if len(rets) < 2:
                return None
            try:
                return statistics.pstdev(rets)
            except Exception:
                return None

    # ---------- Scoring core ----------

    def compute_window(self, merchant: str, interval_minutes: int, start_ts: float) -> Dict[str, Any]:
        end_ts = start_ts + interval_minutes*60
        tweet_avg = self._avg_sentiment("tweets", merchant, start_ts, end_ts)
        reddit_avg = self._avg_sentiment("reddit", merchant, start_ts, end_ts)
        news_avg = self._avg_sentiment("news", merchant, start_ts, end_ts)
        review_avg = self._avg_reviews(merchant, start_ts, end_ts)
        wl_ratio = self._wl_flag_ratio(merchant, start_ts, end_ts)
        std_ret = self._stock_volatility(merchant, start_ts, end_ts)
        comp = {
            "tweet_sentiment": self._normalize_sentiment(tweet_avg),
            "reddit_sentiment": self._normalize_sentiment(reddit_avg),
            "news_sentiment": self._normalize_sentiment(news_avg),
            "reviews_rating": self._normalize_reviews(review_avg),
            "wl_flag_ratio": wl_ratio if wl_ratio is not None else None,
            "stock_volatility": self._normalize_volatility(std_ret)
        }
        synthetic = False
        c_t,_=self._count_interval_docs("tweets",merchant,start_ts,end_ts)
        c_r,_=self._count_interval_docs("reddit",merchant,start_ts,end_ts)
        c_n,_=self._count_interval_docs("news",merchant,start_ts,end_ts)
        c_rev,_=self._count_interval_docs("reviews",merchant,start_ts,end_ts)
        c_wl,_=self._count_interval_docs("wl_transactions",merchant,start_ts,end_ts)
        c_p,_=self._count_interval_docs("stocks_prices",merchant,start_ts,end_ts)
        counts={"tweets":c_t,"reddit":c_r,"news":c_n,"reviews":c_rev,"wl":c_wl,"prices":c_p}
        available={k:v for k,v in comp.items() if v is not None}
        if not available:
            allow_synth = os.getenv("RISK_SYNTHETIC_FALLBACK", "1").lower() in ("1","true","yes","on")
            if allow_synth:
                # Inline synthetic fallback so UI always has something meaningful.
                import random
                seed_val = (hash((merchant, interval_minutes, int(start_ts))) & 0xffffffff)
                rnd = random.Random(seed_val)
                comp = {
                    "tweet_sentiment": round(rnd.uniform(0.2,0.8),3),
                    "reddit_sentiment": round(rnd.uniform(0.2,0.8),3),
                    "news_sentiment": round(rnd.uniform(0.2,0.8),3),
                    "reviews_rating": round(rnd.uniform(0.1,0.9),3),
                    "wl_flag_ratio": round(rnd.uniform(0.0,0.4),3),
                    "stock_volatility": round(rnd.uniform(0.05,0.9),3)
                }
                available = comp.copy()
                synthetic = True
        if available:
            total_w=sum(COMPONENT_WEIGHTS[k] for k in available)
            weighted_sum=sum(available[k]*COMPONENT_WEIGHTS[k] for k in available)
            score_0_1=weighted_sum/total_w if total_w>0 else None
        else:
            score_0_1=None
        # confidence weighted by counts
        mapping=[("tweet_sentiment","tweets"),("reddit_sentiment","reddit"),("news_sentiment","news"),("reviews_rating","reviews"),("wl_flag_ratio","wl"),("stock_volatility","prices")]
        denom=sum(counts[m] for _,m in mapping if counts[m]>0)
        num=sum(counts[m] for c,m in mapping if comp[c] is not None and counts[m]>0)
        confidence=(num/denom) if denom>0 else (len(available)/len(COMPONENT_WEIGHTS))
        risk_score= round(score_0_1*100,2) if score_0_1 is not None else None
        now_ts=dt.datetime.now(dt.UTC).timestamp()
        doc={
            "merchant":merchant,
            "interval_minutes":interval_minutes,
            "window_start_ts":start_ts,
            "window_end_ts":end_ts,
            "window_start_iso":self._iso(start_ts),
            "window_end_iso":self._iso(end_ts),
            "score":risk_score,
            "confidence":round(confidence,3),
            "components":comp,
            "weights_used":COMPONENT_WEIGHTS,
            "computed_at_ts":now_ts,
            "computed_at_iso":self._iso(now_ts),
            "raw_counts":counts,
            "synthetic": synthetic
        }
        self.col.find_one_and_update({"merchant":merchant,"interval_minutes":interval_minutes,"window_start_ts":start_ts},{"$set":doc},upsert=True,return_document=ReturnDocument.AFTER)
        return doc

    # ---------- Timestamp scale detection ----------
    def _collection_looks_ms(self, coll_name: str, merchant: str) -> bool:
        key = (merchant, coll_name)
        cached = self._ts_scale_cache.get(key)
        if cached:
            return cached == "ms"
        doc = self.db[coll_name].find_one({"merchant": merchant}, sort=[("ts", -1)], projection={"ts":1, "_id":0})
        if not doc:
            self._ts_scale_cache[key] = "s"
            return False
        t_val = self._parse_ts(doc.get("ts"))
        if t_val is None:
            self._ts_scale_cache[key] = "s"
            return False
        t = t_val
        scale = "ms" if t > 1e12 else "s"
        self._ts_scale_cache[key] = scale
        return scale == "ms"

    # ---------- Unified interval doc fetcher ----------
    def _fetch_interval_docs(self, coll_name: str, merchant: str, start_ts: float, end_ts: float, sample_limit: int = 600) -> Tuple[List[Dict[str,Any]], Dict[str,Any]]:
        """Attempt multiple query strategies (seconds, ms, merchant vs merchant_name) and fallback sampling.
        Returns (docs, meta) where meta records which strategy succeeded.
        """
        coll = self.db[coll_name]
        strategies = []
        # seconds merchant
        strategies.append(({"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_s"))
        # ms merchant
        strategies.append(({"merchant": merchant, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchant_ms"))
        # seconds merchant_name
        strategies.append(({"merchant_name": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_s"))
        # ms merchant_name
        strategies.append(({"merchant_name": merchant, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchantName_ms"))
        meta = {"attempts": [], "used": None}
        for q, label in strategies:
            try:
                docs = list(coll.find(q, projection={"_id":0}).limit(sample_limit))
            except Exception:
                docs = []
            meta["attempts"].append({"label": label, "count": len(docs)})
            if docs:
                meta["used"] = label
                return docs, meta
        # Case-insensitive fallbacks (always attempt if no docs yet)
        if merchant:
            ci_strategies = []
            ci = {"$regex": f"^{merchant}$", "$options": "i"}
            ci_strategies.append(({"merchant": ci, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_ci_s"))
            ci_strategies.append(({"merchant": ci, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchant_ci_ms"))
            ci_strategies.append(({"merchant_name": ci, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_ci_s"))
            ci_strategies.append(({"merchant_name": ci, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchantName_ci_ms"))
            for q, label in ci_strategies:
                try:
                    docs = list(coll.find(q, projection={"_id":0}).limit(sample_limit))
                except Exception:
                    docs = []
                meta["attempts"].append({"label": label, "count": len(docs)})
                if docs:
                    meta["used"] = label
                    return docs, meta
        # Fallback: broad recent docs (merchant only) without ts constraint to detect time drift
        fallback_docs = list(coll.find({"merchant": merchant}, projection={"_id":0}).sort([("ts", -1)]).limit(sample_limit))
        if not fallback_docs:
            # try merchant_name (BUGFIX: use proper sort tuple list)
            try:
                fallback_docs = list(coll.find({"merchant_name": merchant}, projection={"_id":0}).sort([("ts", -1)]).limit(sample_limit))
            except Exception:
                fallback_docs = []
        meta["fallback_count"] = len(fallback_docs)
        if fallback_docs:
            # Filter manually by ts parsing (accept both seconds/ms and string numbers)
            filtered: List[Dict[str,Any]] = []
            for d in fallback_docs:
                ts_v_norm = self._parse_ts(d.get("ts"), normalize=True)
                if ts_v_norm is None:
                    continue
                if start_ts <= ts_v_norm < end_ts:
                    filtered.append(d)
            if filtered:
                meta["used"] = "manual_filter"
                meta["manual_source_count"] = len(fallback_docs)
                meta["manual_filtered"] = len(filtered)
                return filtered, meta
        return [], meta

    # ---------- Count helper with ms + merchant_name fallbacks ----------
    def _count_interval_docs(self, coll_name: str, merchant: str, start_ts: float, end_ts: float) -> Tuple[int, str]:
        coll = self.db[coll_name]
        strategies = [
            ( {"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_s" ),
            ( {"merchant": merchant, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchant_ms" ),
            ( {"merchant_name": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_s" ),
            ( {"merchant_name": merchant, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchantName_ms" )
        ]
        for q,label in strategies:
            try:
                c = coll.count_documents(q, limit=1000)
            except Exception:
                c = 0
            if c > 0:
                return c, label
        # Case-insensitive fallback counts (always attempt if still zero)
        if merchant:
            ci = {"$regex": f"^{merchant}$", "$options": "i"}
            ci_strategies = [
                (({"merchant": ci, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_ci_s")),
                (({"merchant": ci, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchant_ci_ms")),
                (({"merchant_name": ci, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_ci_s")),
                (({"merchant_name": ci, "ts": {"$gte": start_ts*1000.0, "$lt": end_ts*1000.0}}, "merchantName_ci_ms")),
            ]
            for q,label in ci_strategies:
                try:
                    c = coll.count_documents(q, limit=1000)
                except Exception:
                    c = 0
                if c > 0:
                    return c, label
        # As last resort count manually by scanning recent docs and parsing ISO / mixed ts
        sample = list(coll.find({"merchant": merchant}, projection={"_id":0,"ts":1}).sort([("ts", -1)]).limit(400))
        if not sample:
            sample = list(coll.find({"merchant_name": merchant}, projection={"_id":0,"ts":1}).sort([("ts", -1)]).limit(400))
        manual = 0
        for d in sample:
            t_norm = self._parse_ts(d.get("ts"), normalize=True)
            if t_norm is None:
                continue
            if start_ts <= t_norm < end_ts:
                manual += 1
        if manual > 0:
            return manual, "manual_scan"
        return 0, "none"

    # ---------- Timestamp parsing helper supporting ISO strings ----------
    def _parse_ts(self, raw: Any, normalize: bool = True) -> Optional[float]:
        if raw is None:
            return None
        # Already numeric
        if isinstance(raw, (int, float)):
            v = float(raw)
            if normalize and v > 1e12:  # assume ms
                return v / 1000.0
            return v
        # Numeric inside string
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return None
            # Try float
            try:
                v = float(s)
                if normalize and v > 1e12:
                    return v / 1000.0
                return v
            except Exception:
                pass
            # Try ISO
            try:
                if s.endswith('Z'):
                    s_mod = s[:-1]
                else:
                    s_mod = s
                dt_obj = dt.datetime.fromisoformat(s_mod)
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=dt.UTC)
                return dt_obj.astimezone(dt.UTC).timestamp()
            except Exception:
                return None
        return None

    # (Deprecated backfill/job methods removed) 

    def fetch_scores(self, merchant: str, interval_label: str, limit: int = 500, since: Optional[float]=None, until: Optional[float]=None) -> List[Dict[str, Any]]:
        interval_label = interval_label.lower()
        if interval_label not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[interval_label]
        q: Dict[str, Any] = {"merchant": merchant, "interval_minutes": interval_minutes}
        time_q: Dict[str, Any] = {}
        if since is not None:
            time_q["$gte"] = since
        if until is not None:
            time_q["$lte"] = until
        if time_q:
            q["window_start_ts"] = time_q
        cur = self.col.find(q, sort=[("window_start_ts", -1)], projection={"_id":0}).limit(limit)
        return list(cur)

    def ensure_windows(self, merchant: str, interval_label: str, since_ts: float, until_ts: float) -> List[Dict[str, Any]]:
        """Compute any missing aligned windows covering [since_ts, until_ts)."""
        ilabel = interval_label.lower()
        if ilabel not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[ilabel]
        size = interval_minutes * 60
        start_aligned = self._floor_window(since_ts, interval_minutes)
        end_aligned = self._floor_window(until_ts, interval_minutes)
        if end_aligned <= start_aligned:
            return []
        existing = {d["window_start_ts"]: d for d in self.col.find({
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "window_start_ts": {"$gte": start_aligned, "$lt": end_aligned}
        }, projection={"_id":0})}
        out: List[Dict[str, Any]] = []
        ts_iter = start_aligned
        while ts_iter < end_aligned:
            doc = existing.get(ts_iter)
            if not doc:
                doc = self.compute_window(merchant, interval_minutes, ts_iter)
            out.append(doc)
            ts_iter += size
        return out

    # ---------- Compatibility job-like facade (synchronous) ----------
    def trigger_or_status(self, merchant: str, interval_label: str, submit_fn=None, max_backfill_hours: int | None = 24) -> Dict[str, Any]:
        """Synchronous compute disguised as a 'job' for existing API endpoint.
        max_backfill_hours: how far back to compute from now (default 24h)."""
        ilabel = interval_label.lower()
        if ilabel not in INTERVAL_MAP:
            raise ValueError("Unsupported interval; choose one of: " + ", ".join(sorted(INTERVAL_MAP.keys())))
        interval_minutes = INTERVAL_MAP[ilabel]
        hours = max_backfill_hours if (max_backfill_hours is not None) else 24
        now_ts = dt.datetime.now(dt.UTC).timestamp()
        since_ts = now_ts - hours*3600
        windows = self.ensure_windows(merchant, ilabel, since_ts, now_ts)
        job_id = f"sync-{uuid.uuid4().hex[:8]}"
        status = {
            "job_id": job_id,
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "state": "done",
            "started_ts": now_ts,
            "total_windows": len(windows),
            "processed_windows": len(windows),
            "percent": 100.0,
            "error": None
        }
        self.jobs[job_id] = status
        return status

    def job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self.jobs.get(job_id)

    # ---------- Summaries for UI consumption ----------
    def summarize_scores(self, merchant: str, interval_label: str, lookback: int = 50, since_ts: float | None = None, until_ts: float | None = None) -> Dict[str, Any]:
        """Return a lightweight summary for a given merchant+interval.
        Includes latest window, previous window, delta, trend arrays, component averages.
        Designed for frontend dashboards (avoids heavy client post-processing).
        """
        lookback = max(5, min(int(lookback or 50), 500))  # sane bounds
        rows_full = self.fetch_scores(merchant, interval_label, limit=lookback)
        if since_ts is not None or until_ts is not None:
            rows = [r for r in rows_full if (
                (since_ts is None or r.get("window_start_ts",0) >= since_ts) and
                (until_ts is None or r.get("window_end_ts",0) <= until_ts)
            )]
        else:
            rows = rows_full
        if not rows:
            return {
                "interval": interval_label,
                "count": 0,
                "latest": None,
                "previous": None,
                "delta": None,
                "trend": [],
                "trend_confidence": [],
                "component_latest": None,
                "component_avg": None,
                "avg_score": None,
                "avg_confidence": None,
            }
        # rows are newest first -> reverse for chronological trend
        chron = list(reversed(rows))
        trend = []
        trend_conf = []
        scores_numeric = []
        confidences = []
        comp_accum: Dict[str, List[float]] = {}
        for r in chron:
            s = r.get("score")
            c = r.get("confidence")
            if isinstance(s, (int,float)):
                trend.append({"t": r.get("window_end_ts"), "s": s})
                scores_numeric.append(float(s))
            else:
                trend.append({"t": r.get("window_end_ts"), "s": None})
            if isinstance(c, (int,float)):
                trend_conf.append({"t": r.get("window_end_ts"), "c": float(c)})
                confidences.append(float(c))
            comps = (r.get("components") or {})
            for k,v in comps.items():
                if isinstance(v, (int,float)):
                    comp_accum.setdefault(k, []).append(float(v))
        latest = rows[0]
        previous = rows[1] if len(rows) > 1 else None
        latest_score = latest.get("score") if latest else None
        prev_score = previous.get("score") if previous else None
        delta = None
        if isinstance(latest_score, (int,float)) and isinstance(prev_score, (int,float)):
            delta = round(float(latest_score) - float(prev_score), 2)
        comp_avg = {}
        for k, arr in comp_accum.items():
            if arr:
                comp_avg[k] = round(sum(arr)/len(arr), 4)
        avg_score = round(sum(scores_numeric)/len(scores_numeric), 2) if scores_numeric else None
        avg_confidence = round(sum(confidences)/len(confidences), 3) if confidences else None
        return {
            "interval": interval_label,
            "count": len(rows),
            "latest": latest,
            "previous": previous,
            "delta": delta,
            "trend": trend,  # chronological order
            "trend_confidence": trend_conf,
            "component_latest": (latest or {}).get("components"),
            "component_avg": comp_avg or None,
            "avg_score": avg_score,
            "avg_confidence": avg_confidence,
        }

    # ---------- Deep probe diagnostics (per-window collection fetch meta) ----------
    def probe_window(self, merchant: str, interval_minutes: int, start_ts: float, end_ts: float) -> Dict[str, Any]:
        """Inspect each underlying collection for a single window.
        Returns counts + query strategy meta + up to 2 sample docs (timestamps only) per stream.
        Helps debug why components are None.
        """
        streams = {
            "tweets": "tweets",
            "reddit": "reddit",
            "news": "news",
            "reviews": "reviews",
            "wl_transactions": "wl_transactions",
            "stocks_prices": "stocks_prices",
        }
        out: Dict[str, Any] = {"window_start_ts": start_ts, "window_end_ts": end_ts}
        details = {}
        for label, cname in streams.items():
            docs, meta = self._fetch_interval_docs(cname, merchant, start_ts, end_ts, sample_limit=50)
            count, strategy_used = self._count_interval_docs(cname, merchant, start_ts, end_ts)
            # sample up to 2 docs with normalized ts for quick inspection
            samples = []
            for d in docs[:2]:
                ts_norm = self._parse_ts(d.get("ts"), normalize=True)
                samples.append({k: d.get(k) for k in ("ts",) if k in d} | ({"ts_norm": ts_norm} if ts_norm is not None else {}))
            details[label] = {
                "count_est": count,
                "count_strategy": strategy_used,
                "fetch_used": meta.get("used"),
                "fetch_attempts": meta.get("attempts"),
                "samples": samples,
            }
        out["streams"] = details
        return out

    def probe_range(self, merchant: str, interval_minutes: int, since_ts: float, until_ts: float, max_windows: int = 3) -> Dict[str, Any]:
        """Probe up to last N windows inside a range without mutating risk_scores."""
        size = interval_minutes * 60
        # Align boundaries
        start_floor = self._floor_window(since_ts, interval_minutes)
        end_floor = self._floor_window(until_ts, interval_minutes)
        windows: List[float] = []
        ts_iter = start_floor
        while ts_iter < end_floor:
            windows.append(ts_iter)
            ts_iter += size
        if not windows:
            return {"windows": [], "notes": ["No aligned windows in range"]}
        chosen = windows[-max_windows:]
        probes = [self.probe_window(merchant, interval_minutes, w, w+size) for w in chosen]
        return {"interval_minutes": interval_minutes, "windows": probes, "total_windows_in_range": len(windows)}

    # ---------- Text sentiment heuristic ----------
    def _infer_sentiment_from_text(self, text: str) -> Optional[float]:
        if not text:
            return None
        try:
            txt = text.lower()
            # count hits
            pos_hits = 0
            neg_hits = 0
            # simple token split
            for tok in txt.replace("\n"," ").split():
                t = tok.strip(".,!?;:\"()[]{}").lower()
                if not t:
                    continue
                if t in self._positive_words:
                    pos_hits += 1
                elif t in self._negative_words:
                    neg_hits += 1
            total = pos_hits + neg_hits
            if total == 0:
                return None
            # Map (pos-neg)/total to [-1,1]
            score = (pos_hits - neg_hits) / total
            return max(-1.0, min(1.0, score))
        except Exception:
            return None

