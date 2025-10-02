#!/usr/bin/env python3
"""Risk evaluation + merchant evaluation timeline builder.

This module is based on the previously provided `risk_eval_old.py` but adds a
lightweight secondary persistence layer (`merchant_evaluations` collection)
designed for the unified frontend component `MerchantRisk.vue` and the
`manual_data/view_score.py` explorer.

Two related but distinct document schemas are produced:

1. risk_scores (window/component oriented; internal / legacy endpoints)
   { merchant, interval_minutes, window_start_ts, window_end_ts, score, confidence,
     components:{...}, weights_used:{...}, raw_counts:{...}, synthetic:bool }

2. merchant_evaluations (UI friendly; mirrors view_score expectations)
   { merchant, window_start, window_end, window_start_ts, window_end_ts,
     created_at, algo_version, is_backfill, scores:{ total, wl, market, sentiment,
     volume, incident_bump }, counts:{ tweets, reddit, news, reviews, wl, stock_prices },
     confidence, drivers:[...]}.

The evaluation logic reuses the same component extraction helpers. New derived
components are mapped into the simplified keys expected by the UI:
 - wl            -> wl_flag_ratio (0..1) * 100
 - market        -> stock_volatility normalized (0..1) * 100
 - sentiment     -> average of (tweet, reddit, news, reviews) risk-normalized * 100
 - volume        -> heuristic activity risk from aggregate counts (scaled 0..100)
 - incident_bump -> discrete additive bump (0..100) when anomaly/flag patterns appear
 - total         -> (weighted blend of the above primary components + incident bump) capped 0..100

All numeric components are kept inside [0,100]. Confidence is shared between
schemas (0..1). The module avoids external ML libs for portability.
"""
from __future__ import annotations

import datetime as dt
import os
import random
import statistics
from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, ReturnDocument

# Stream collections consulted for scoring
STREAM_COLLECTIONS = [
    "tweets", "reddit", "news", "reviews", "wl_transactions", "stocks_prices"
]

INTERVAL_MAP = {
    "30m": 30,
    "1h": 60,
    "1d": 60 * 24,
    "30min": 30,
    "1hour": 60,
    "1day": 60 * 24,
}

# Base weights for internal risk_scores (kept for compatibility)
COMPONENT_WEIGHTS = {
    "tweet_sentiment": 1.0,
    "reddit_sentiment": 1.0,
    "news_sentiment": 1.0,
    "reviews_rating": 1.0,
    "wl_flag_ratio": 1.5,
    "stock_volatility": 1.0,
}

ALT_SENTIMENT_FIELDS = [
    "sentiment", "compound", "polarity", "score", "sentiment_score", "vader_compound"
]
ALT_REVIEW_FIELDS = ["rating", "stars", "score"]
ALT_PRICE_FIELDS = ["price", "close", "adj_close", "value"]
ALT_STATUS_FIELDS = ["status", "state"]


class RiskEvaluator:
    """Primary risk & evaluation engine."""

    def __init__(self, db):
        self.db = db
        self.col = db["risk_scores"]
        self.eval_col = db["merchant_evaluations"]
        self._ts_scale_cache: Dict[tuple, str] = {}
        self._positive_words = {
            "good",
            "great",
            "positive",
            "win",
            "launch",
            "feature",
            "award",
            "impressed",
            "strong",
            "excited",
            "growth",
            "up",
            "improve",
            "excellent",
            "love",
            "amazing",
            "awesome",
            "partnership",
            "expansion",
        }
        self._negative_words = {
            "bad",
            "negative",
            "breach",
            "fraud",
            "lawsuit",
            "outage",
            "recall",
            "scandal",
            "issue",
            "problem",
            "downtime",
            "leak",
            "concern",
            "concerns",
            "weak",
            "loss",
            "down",
            "decline",
            "bug",
            "risk",
            "risks",
            "angry",
            "disappointed",
        }
        self.jobs: Dict[str, Dict[str, Any]] = {}

    # ---------------- Indexes -----------------
    def ensure_indexes(self):
        self.col.create_index(
            [
                ("merchant", ASCENDING),
                ("interval_minutes", ASCENDING),
                ("window_start_ts", ASCENDING),
            ],
            unique=True,
            name="uniq_window",
        )
        self.col.create_index(
            [
                ("merchant", ASCENDING),
                ("interval_minutes", ASCENDING),
                ("window_end_ts", ASCENDING),
            ]
        )
        # Evaluation timeline indexes
        self.eval_col.create_index(
            [("merchant", ASCENDING), ("window_end_ts", ASCENDING)], name="eval_end"
        )
        self.eval_col.create_index(
            [("merchant", ASCENDING), ("window_start_ts", ASCENDING)], name="eval_start"
        )

    # ---------------- Utilities -----------------
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
        # Assume original sentiment in [-1,1]; risk increases with negativity.
        return max(0.0, min(1.0, 1 - (avg_sent + 1) / 2))

    @staticmethod
    def _normalize_reviews(avg_rating: Optional[float]) -> Optional[float]:
        if avg_rating is None:
            return None
        return max(0.0, min(1.0, 1 - (avg_rating / 5.0)))

    @staticmethod
    def _normalize_volatility(std_ret: Optional[float]) -> Optional[float]:
        if std_ret is None:
            return None
        return max(0.0, min(1.0, std_ret / 0.05))

    @staticmethod
    def _ratio(num: int, den: int) -> Optional[float]:
        if den <= 0:
            return None
        return num / den

    # ---------------- Primitive data extraction -----------------
    def _avg_sentiment(
        self, coll_name: str, merchant: str, start_ts: float, end_ts: float
    ) -> Optional[float]:
        docs, _ = self._fetch_interval_docs(coll_name, merchant, start_ts, end_ts)
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
                # scan generic numeric -1..1
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
                # naive heuristic on text content
                txt_fields = []
                for tf in ("text", "content", "headline", "title", "review", "body"):
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

    def _avg_reviews(self, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _ = self._fetch_interval_docs("reviews", merchant, start_ts, end_ts)
        if not docs:
            return None
        vals: List[float] = []
        for d in docs:
            for f in ALT_REVIEW_FIELDS:
                if f in d:
                    try:
                        fv = float(d.get(f))
                        if -1 <= fv <= 10:
                            vals.append(max(0.0, min(5.0, fv)))
                            break
                    except Exception:
                        continue
        if not vals:
            return None
        return sum(vals) / len(vals)

    def _wl_flag_ratio(self, merchant: str, start_ts: float, end_ts: float) -> Optional[float]:
        docs, _ = self._fetch_interval_docs("wl_transactions", merchant, start_ts, end_ts)
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

    def _stock_volatility(
        self, merchant: str, start_ts: float, end_ts: float
    ) -> Optional[float]:
        docs, _ = self._fetch_interval_docs(
            "stocks_prices", merchant, start_ts, end_ts, sample_limit=1200
        )
        if len(docs) < 3:
            return None
        prices: List[float] = []
        for d in docs:
            for f in ALT_PRICE_FIELDS:
                if f in d:
                    try:
                        fv = float(d.get(f))
                        if fv > 0:
                            prices.append(fv)
                            break
                    except Exception:
                        continue
        if len(prices) < 3:
            return None
        rets = []
        for i in range(1, len(prices)):
            if prices[i - 1] > 0:
                rets.append((prices[i] - prices[i - 1]) / prices[i - 1])
        if len(rets) < 2:
            return None
        try:
            return statistics.pstdev(rets)
        except Exception:
            return None

    # ---------------- Core risk_scores window -----------------
    def compute_window(self, merchant: str, interval_minutes: int, start_ts: float) -> Dict[str, Any]:
        end_ts = start_ts + interval_minutes * 60
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
            "stock_volatility": self._normalize_volatility(std_ret),
        }
        counts = self._counts_bundle(merchant, start_ts, end_ts)
        available = {k: v for k, v in comp.items() if v is not None}
        synthetic = False
        if not available:
            allow_synth = (
                os.getenv("RISK_SYNTHETIC_FALLBACK", "1").lower()
                in ("1", "true", "yes", "on")
            )
            if allow_synth:
                rnd = random.Random(hash((merchant, interval_minutes, int(start_ts))) & 0xFFFFFFFF)
                comp = {
                    "tweet_sentiment": round(rnd.uniform(0.2, 0.8), 3),
                    "reddit_sentiment": round(rnd.uniform(0.2, 0.8), 3),
                    "news_sentiment": round(rnd.uniform(0.2, 0.8), 3),
                    "reviews_rating": round(rnd.uniform(0.1, 0.9), 3),
                    "wl_flag_ratio": round(rnd.uniform(0.0, 0.4), 3),
                    "stock_volatility": round(rnd.uniform(0.05, 0.9), 3),
                }
                available = comp.copy()
                synthetic = True
        if available:
            total_w = sum(COMPONENT_WEIGHTS[k] for k in available)
            weighted_sum = sum(available[k] * COMPONENT_WEIGHTS[k] for k in available)
            score_0_1 = weighted_sum / total_w if total_w > 0 else None
        else:
            score_0_1 = None
        mapping = [
            ("tweet_sentiment", "tweets"),
            ("reddit_sentiment", "reddit"),
            ("news_sentiment", "news"),
            ("reviews_rating", "reviews"),
            ("wl_flag_ratio", "wl"),
            ("stock_volatility", "prices"),
        ]
        denom = sum(counts[m] for _, m in mapping if counts[m] > 0)
        num = sum(counts[m] for c, m in mapping if comp[c] is not None and counts[m] > 0)
        confidence = (num / denom) if denom > 0 else (len(available) / len(COMPONENT_WEIGHTS))
        risk_score = round(score_0_1 * 100, 2) if score_0_1 is not None else None
        now_ts = dt.datetime.now(dt.UTC).timestamp()
        doc = {
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "window_start_ts": start_ts,
            "window_end_ts": end_ts,
            "window_start_iso": self._iso(start_ts),
            "window_end_iso": self._iso(end_ts),
            "score": risk_score,
            "confidence": round(confidence, 3),
            "components": comp,
            "weights_used": COMPONENT_WEIGHTS,
            "computed_at_ts": now_ts,
            "computed_at_iso": self._iso(now_ts),
            "raw_counts": counts,
            "synthetic": synthetic,
        }
        self.col.find_one_and_update(
            {
                "merchant": merchant,
                "interval_minutes": interval_minutes,
                "window_start_ts": start_ts,
            },
            {"$set": doc},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        # Also write evaluation format
        self._upsert_evaluation_doc(merchant, interval_minutes, start_ts, end_ts, doc)
        return doc

    # ---------------- Counts helper -----------------
    def _counts_bundle(self, merchant: str, start_ts: float, end_ts: float) -> Dict[str, int]:
        def c(coll_name: str) -> int:
            coll = self.db[coll_name]
            try:
                return coll.count_documents({"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, limit=2000)
            except Exception:
                return 0

        return {
            "tweets": c("tweets"),
            "reddit": c("reddit"),
            "news": c("news"),
            "reviews": c("reviews"),
            "wl": c("wl_transactions"),
            "prices": c("stocks_prices"),
        }

    # ---------------- Evaluation doc (merchant_evaluations) -----------------
    def _upsert_evaluation_doc(
        self,
        merchant: str,
        interval_minutes: int,
        start_ts: float,
        end_ts: float,
        base_doc: Dict[str, Any],
    ):
        comp = base_doc.get("components") or {}
        counts = base_doc.get("raw_counts") or {}
        # Primary risk components in 0..1 (ensure bounds)
        tweet = comp.get("tweet_sentiment")
        reddit = comp.get("reddit_sentiment")
        news = comp.get("news_sentiment")
        reviews = comp.get("reviews_rating")
        wl_ratio = comp.get("wl_flag_ratio")
        vol = comp.get("stock_volatility")
        sentiment_vals = [v for v in [tweet, reddit, news, reviews] if isinstance(v, (int, float))]
        sentiment = sum(sentiment_vals) / len(sentiment_vals) if sentiment_vals else None
        market = vol
        wl_component = wl_ratio
        # Volume heuristic (activity anomaly): compare aggregate to reference scale
        activity_total = sum(counts.get(k, 0) for k in ["tweets", "reddit", "news", "reviews"])
        volume = min(1.0, activity_total / 200.0) if activity_total > 0 else None
        # Incident bump: escalate if wl ratio or volatility high
        incident_bump = 0.0
        if isinstance(wl_component, (int, float)) and wl_component > 0.3:
            incident_bump += min(1.0, (wl_component - 0.3) * 2.0) * 0.4  # up to 0.4
        if isinstance(market, (int, float)) and market > 0.6:
            incident_bump += min(0.6, (market - 0.6) * 1.5) * 0.3  # up to additional 0.18
        incident_bump = min(1.0, incident_bump)
        # Aggregate weighted score (exclude bump then add scaled bump)
        weighted_parts = []
        weights_eval = {"wl": 1.4, "market": 1.0, "sentiment": 1.0, "volume": 0.6}
        if wl_component is not None:
            weighted_parts.append((wl_component, weights_eval["wl"]))
        if market is not None:
            weighted_parts.append((market, weights_eval["market"]))
        if sentiment is not None:
            weighted_parts.append((sentiment, weights_eval["sentiment"]))
        if volume is not None:
            weighted_parts.append((volume, weights_eval["volume"]))
        if weighted_parts:
            total_w = sum(w for _, w in weighted_parts)
            blended = sum(v * w for v, w in weighted_parts) / total_w
            total_score_0_1 = min(1.0, blended + incident_bump * 0.5)
        else:
            total_score_0_1 = None
        eval_doc = {
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "window_start_ts": start_ts,
            "window_end_ts": end_ts,
            "window_start": self._iso(start_ts),
            "window_end": self._iso(end_ts),
            "created_at": self._iso(dt.datetime.now(dt.UTC).timestamp()),
            "algo_version": "v1",
            "is_backfill": False,  # future extension
            "scores": {
                "total": round(total_score_0_1 * 100, 2) if total_score_0_1 is not None else None,
                "wl": round(wl_component * 100, 2) if wl_component is not None else None,
                "market": round(market * 100, 2) if market is not None else None,
                "sentiment": round(sentiment * 100, 2) if sentiment is not None else None,
                "volume": round(volume * 100, 2) if volume is not None else None,
                "incident_bump": round(incident_bump * 100, 2),
            },
            "counts": {
                "tweets": counts.get("tweets", 0),
                "reddit": counts.get("reddit", 0),
                "news": counts.get("news", 0),
                "reviews": counts.get("reviews", 0),
                "wl": counts.get("wl", 0),
                "stock_prices": counts.get("prices", 0),
            },
            "confidence": base_doc.get("confidence"),
            "drivers": self._driver_list(base_doc),
        }
        self.eval_col.find_one_and_update(
            {
                "merchant": merchant,
                "interval_minutes": interval_minutes,
                "window_start_ts": start_ts,
            },
            {"$set": eval_doc},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return eval_doc

    def _driver_list(self, base_doc: Dict[str, Any]) -> List[str]:
        comp = base_doc.get("components") or {}
        vals = [(k, v) for k, v in comp.items() if isinstance(v, (int, float))]
        vals.sort(key=lambda x: x[1], reverse=True)
        return [k for k, _ in vals[:4]]

    # ---------------- Timestamp parsing & interval fetch logic -----------------
    def _fetch_interval_docs(
        self,
        coll_name: str,
        merchant: str,
        start_ts: float,
        end_ts: float,
        sample_limit: int = 600,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        coll = self.db[coll_name]
        strategies = [
            ({"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_s"),
            (
                {
                    "merchant": merchant,
                    "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0},
                },
                "merchant_ms",
            ),
            (
                {"merchant_name": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}},
                "merchantName_s",
            ),
            (
                {
                    "merchant_name": merchant,
                    "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0},
                },
                "merchantName_ms",
            ),
        ]
        meta = {"attempts": [], "used": None}
        for q, label in strategies:
            try:
                docs = list(coll.find(q, projection={"_id": 0}).limit(sample_limit))
            except Exception:
                docs = []
            meta["attempts"].append({"label": label, "count": len(docs)})
            if docs:
                meta["used"] = label
                return docs, meta
        # Simple fallback: last sample docs filtered manually
        fallback_docs = list(
            coll.find({"merchant": merchant}, projection={"_id": 0})
            .sort([("ts", -1)])
            .limit(sample_limit)
        )
        meta["fallback_count"] = len(fallback_docs)
        filtered: List[Dict[str, Any]] = []
        for d in fallback_docs:
            ts_v = self._parse_ts(d.get("ts"), normalize=True)
            if ts_v is None:
                continue
            if start_ts <= ts_v < end_ts:
                filtered.append(d)
        if filtered:
            meta["used"] = "manual_filter"
            return filtered, meta
        return [], meta

    def _parse_ts(self, raw: Any, normalize: bool = True) -> Optional[float]:
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            v = float(raw)
            if normalize and v > 1e12:
                return v / 1000.0
            return v
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return None
            try:
                v = float(s)
                if normalize and v > 1e12:
                    return v / 1000.0
                return v
            except Exception:
                pass
            try:
                if s.endswith("Z"):
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

    # ---------------- Public evaluation range helpers -----------------
    def ensure_windows(
        self, merchant: str, interval_label: str, since_ts: float, until_ts: float
    ) -> List[Dict[str, Any]]:
        interval_label = interval_label.lower()
        if interval_label not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[interval_label]
        size = interval_minutes * 60
        start_aligned = self._floor_window(since_ts, interval_minutes)
        end_aligned = self._floor_window(until_ts, interval_minutes)
        if end_aligned <= start_aligned:
            return []
        existing = {
            d["window_start_ts"]: d
            for d in self.col.find(
                {
                    "merchant": merchant,
                    "interval_minutes": interval_minutes,
                    "window_start_ts": {"$gte": start_aligned, "$lt": end_aligned},
                },
                projection={"_id": 0},
            )
        }
        out: List[Dict[str, Any]] = []
        ts_iter = start_aligned
        while ts_iter < end_aligned:
            doc = existing.get(ts_iter)
            if not doc:
                doc = self.compute_window(merchant, interval_minutes, ts_iter)
            out.append(doc)
            ts_iter += size
        return out

    def fetch_evaluations(
        self,
        merchant: str,
        interval_label: str = "30m",
        limit: int = 500,
        since: Optional[float] = None,
        until: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {"merchant": merchant}
        if since is not None or until is not None:
            ts_q: Dict[str, Any] = {}
            if since is not None:
                ts_q["$gte"] = since
            if until is not None:
                ts_q["$lte"] = until
            q["window_end_ts"] = ts_q
        cur = self.eval_col.find(q, projection={"_id": 0}).sort(
            [("window_end_ts", -1)]
        ).limit(limit)
        return list(cur)

    def ensure_evaluations(
        self,
        merchant: str,
        interval_label: str,
        since_ts: float,
        until_ts: float,
    ) -> List[Dict[str, Any]]:
        # Simply chain on ensure_windows because evaluation docs are created during compute.
        return self.ensure_windows(merchant, interval_label, since_ts, until_ts)

    # ---------------- Legacy compatibility / diagnostics -----------------
    def fetch_scores(
        self,
        merchant: str,
        interval_label: str,
        limit: int = 500,
        since: Optional[float] = None,
        until: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        interval_label = interval_label.lower()
        if interval_label not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[interval_label]
        q: Dict[str, Any] = {"merchant": merchant, "interval_minutes": interval_minutes}
        if since is not None or until is not None:
            ts_q: Dict[str, Any] = {}
            if since is not None:
                ts_q["$gte"] = since
            if until is not None:
                ts_q["$lte"] = until
            q["window_start_ts"] = ts_q
        cur = self.col.find(q, sort=[("window_start_ts", -1)], projection={"_id": 0}).limit(limit)
        return list(cur)

    def summarize_scores(
        self,
        merchant: str,
        interval_label: str,
        lookback: int = 50,
        since_ts: float | None = None,
        until_ts: float | None = None,
    ) -> Dict[str, Any]:
        lookback = max(5, min(int(lookback or 50), 500))
        rows_full = self.fetch_scores(merchant, interval_label, limit=lookback)
        if since_ts is not None or until_ts is not None:
            rows = [
                r
                for r in rows_full
                if (
                    (since_ts is None or r.get("window_start_ts", 0) >= since_ts)
                    and (until_ts is None or r.get("window_end_ts", 0) <= until_ts)
                )
            ]
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
        chron = list(reversed(rows))
        trend = []
        trend_conf = []
        scores_numeric = []
        confidences = []
        comp_accum: Dict[str, List[float]] = {}
        for r in chron:
            s = r.get("score")
            c = r.get("confidence")
            if isinstance(s, (int, float)):
                trend.append({"t": r.get("window_end_ts"), "s": s})
                scores_numeric.append(float(s))
            else:
                trend.append({"t": r.get("window_end_ts"), "s": None})
            if isinstance(c, (int, float)):
                trend_conf.append({"t": r.get("window_end_ts"), "c": float(c)})
                confidences.append(float(c))
            comps = r.get("components") or {}
            for k, v in comps.items():
                if isinstance(v, (int, float)):
                    comp_accum.setdefault(k, []).append(float(v))
        latest = rows[0]
        previous = rows[1] if len(rows) > 1 else None
        latest_score = latest.get("score") if latest else None
        prev_score = previous.get("score") if previous else None
        delta = None
        if isinstance(latest_score, (int, float)) and isinstance(prev_score, (int, float)):
            delta = round(float(latest_score) - float(prev_score), 2)
        comp_avg = {}
        for k, arr in comp_accum.items():
            if arr:
                comp_avg[k] = round(sum(arr) / len(arr), 4)
        avg_score = (
            round(sum(scores_numeric) / len(scores_numeric), 2)
            if scores_numeric
            else None
        )
        avg_confidence = (
            round(sum(confidences) / len(confidences), 3) if confidences else None
        )
        return {
            "interval": interval_label,
            "count": len(rows),
            "latest": latest,
            "previous": previous,
            "delta": delta,
            "trend": trend,
            "trend_confidence": trend_conf,
            "component_latest": (latest or {}).get("components"),
            "component_avg": comp_avg or None,
            "avg_score": avg_score,
            "avg_confidence": avg_confidence,
        }

    def _count_interval_docs(
        self, coll_name: str, merchant: str, start_ts: float, end_ts: float
    ) -> Tuple[int, str]:
        coll = self.db[coll_name]
        strategies = [
            ({"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_s"),
            (
                {
                    "merchant": merchant,
                    "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0},
                },
                "merchant_ms",
            ),
            (
                {"merchant_name": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}},
                "merchantName_s",
            ),
            (
                {
                    "merchant_name": merchant,
                    "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0},
                },
                "merchantName_ms",
            ),
        ]
        for q, label in strategies:
            try:
                c = coll.count_documents(q, limit=1000)
            except Exception:
                c = 0
            if c > 0:
                return c, label
        return 0, "none"

    def probe_window(
        self, merchant: str, interval_minutes: int, start_ts: float, end_ts: float
    ) -> Dict[str, Any]:
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
            count, strategy = self._count_interval_docs(cname, merchant, start_ts, end_ts)
            samples = []
            for d in docs[:2]:
                ts_norm = self._parse_ts(d.get("ts"), normalize=True)
                samples.append({k: d.get(k) for k in ("ts",) if k in d} | ({"ts_norm": ts_norm} if ts_norm is not None else {}))
            details[label] = {
                "count_est": count,
                "count_strategy": strategy,
                "fetch_used": meta.get("used"),
                "fetch_attempts": meta.get("attempts"),
                "samples": samples,
            }
        out["streams"] = details
        return out

    def probe_range(
        self,
        merchant: str,
        interval_minutes: int,
        since_ts: float,
        until_ts: float,
        max_windows: int = 3,
    ) -> Dict[str, Any]:
        size = interval_minutes * 60
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
        probes = [self.probe_window(merchant, interval_minutes, w, w + size) for w in chosen]
        return {
            "interval_minutes": interval_minutes,
            "windows": probes,
            "total_windows_in_range": len(windows),
        }

    def stream_counts(self, merchant: str) -> Dict[str, int]:
        out = {}
        for cname in STREAM_COLLECTIONS:
            try:
                out[cname] = self.db[cname].count_documents({"merchant": merchant}, limit=1_000_000)
            except Exception:
                out[cname] = 0
        return out

    # ---------------- Sentiment heuristic -----------------
    def _infer_sentiment_from_text(self, text: str) -> Optional[float]:
        if not text:
            return None
        try:
            txt = text.lower()
            pos_hits = 0
            neg_hits = 0
            for tok in txt.replace("\n", " ").split():
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
            return max(-1.0, min(1.0, (pos_hits - neg_hits) / total))
        except Exception:
            return None
