#!/usr/bin/env python3
"""Risk evaluation + merchant evaluation timeline builder.

This module builds per-window risk scores and UI-oriented merchant evaluation
documents. It supports both historical backfill jobs and a realtime mode that
only keeps the latest windows fresh. See `RiskEvaluator` for orchestration.
"""

from __future__ import annotations
import os, json, time, datetime as dt, statistics, random, threading, bisect
from typing import Any, Dict, List, Optional, Tuple
from pymongo import ASCENDING, ReturnDocument

# ---------------- Configuration (restored) -----------------
DEFAULT_RISK_EVAL_CONFIG: Dict[str, Any] = {
    "weights": {
        "wl": 1.2,
        "market": 0.4,
        "sentiment": 1.0,
        "volume": 1.1,
    },
    "internal_component_weights": {
        "tweet_sentiment": 1.0,
        "reddit_sentiment": 1.0,
        "news_sentiment": 1.0,
        "reviews_rating": 1.0,
        "wl_flag_ratio": 1.5,
        "stock_volatility": 1.0,
    },
    "volume": {
        "activity_divisor": 200.0,
        "min_activity_for_nonzero": 1,
    },
    "incident": {
        "wl_threshold": 0.3,
        "wl_delta_scale": 2.0,
        "wl_multiplier": 0.4,
        "market_threshold": 0.6,
        "market_delta_scale": 1.5,
        "market_delta_cap": 0.6,
        "market_multiplier": 0.3,
        "max_total": 1.0,
    },
    "dampen": {
        "enabled": True,
        "blend_power": 0.5,
        "blend_scale": 0.85,
        "incident_scale": 0.35,
    },
    "stability": {
        "ema_enabled": True,
        "ema_alpha_rise": 0.6,
        "ema_alpha_fall": 0.45,
        "min_delta_pct": 0.15,
        "hysteresis": {
            "high_threshold": 0.80,
            "release_delta": 0.05,
            "consecutive_required": 2,
        },
        "replace_total_with_smoothed": True,
        "max_rise_per_window_pct": 8.0,
        "max_drop_per_window_pct": 12.0,
        "soft_near_high": {
            "enabled": False,
            "buffer": 0.10,
            "alpha_scale": 0.7,
        },
    },
}

# Interval label mapping (minutes)
INTERVAL_MAP: Dict[str, int] = {
    "30m": 30,
    "30min": 30,
    "1h": 60,
    "1hour": 60,
    "1d": 60 * 24,
    "1day": 60 * 24,
}

# ---- Missing constant definitions (restored) ----
# Component weights used inside compute_window (normalized blending of available components)
COMPONENT_WEIGHTS: Dict[str, float] = {
    "tweet_sentiment": 1.0,
    "reddit_sentiment": 1.0,
    "news_sentiment": 1.0,
    "reviews_rating": 1.0,
    "wl_flag_ratio": 1.5,
    "stock_volatility": 1.0,
}

# Alternate field name candidates for flexible ingestion
ALT_SENTIMENT_FIELDS = ["sentiment", "score", "polarity", "senti", "sent"]
ALT_REVIEW_FIELDS = ["rating", "stars", "score", "review_rating"]
ALT_STATUS_FIELDS = ["status", "state", "flag", "flag_status"]
ALT_PRICE_FIELDS = ["price", "close", "adj_close", "close_price", "last"]

# Stream collection canonical names (used for range discovery & diagnostics)
STREAM_COLLECTIONS = [
    "tweets",
    "reddit",
    "news",
    "reviews",
    "wl_transactions",
    "stocks_prices",
]


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (override or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _load_external_config() -> Dict[str, Any]:
    # File based
    cfg = dict(DEFAULT_RISK_EVAL_CONFIG)
    here = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(here, "risk_eval_config.json")
    if os.path.isfile(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_cfg = json.load(f)
            cfg = _deep_merge(cfg, file_cfg)
        except Exception:
            pass
    # Env var based
    env_json = os.getenv("RISK_EVAL_CONFIG_JSON")
    if env_json:
        try:
            env_cfg = json.loads(env_json)
            cfg = _deep_merge(cfg, env_cfg)
        except Exception:
            pass
    return cfg


RISK_EVAL_CONFIG = _load_external_config()


class RiskEvaluator:
    """Primary risk & evaluation engine."""

    def __init__(self, db):
        self.db = db
        self.col = db["risk_scores"]
        self.eval_col = db["merchant_evaluations"]
        # Persistent jobs collection (new)
        self.jobs_col = db["risk_eval_jobs"]
        # Dispatcher state
        self.max_concurrent_jobs = int(os.getenv("RISK_MAX_CONCURRENT", "3"))  # configurable
        self.dispatch_poll_seconds = float(os.getenv("RISK_DISPATCH_POLL", "1.2"))
        self._dispatch_initialized = False
        self._ts_scale_cache: Dict[tuple, str] = {}
        self._ts_field_cache: Dict[tuple, Dict[str, Any]] = {}  # (coll, merchant) -> {field, scale, detected_at}
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
        # Prefetch cache (per merchant + interval) built per backfill batch
        # Structure: {(merchant, interval_minutes): { 'range': (start_ts, end_ts), 'streams': {stream: [docs_sorted_by_ts]}, 'ts_field': 'ts' }}
        self._prefetch: Dict[tuple, Dict[str, Any]] = {}
        # Metrics / diagnostics counters
        self.metrics: Dict[str, int] = {
            "windows_computed": 0,
            "prefetch_hits": 0,
            "fast_path_hits": 0,
        }

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
        # Jobs indexes
        try:
            self.jobs_col.create_index([
                ("merchant", ASCENDING),
                ("interval", ASCENDING),
                ("status", ASCENDING),
            ], name="job_lookup")
            self.jobs_col.create_index([("created_at_ts", ASCENDING)], name="job_created")
            self.jobs_col.create_index([("priority", ASCENDING), ("created_at_ts", ASCENDING)], name="job_priority_fifo")
        except Exception:
            pass
        # Optional performance indexes on underlying stream collections (merchant, ts)
        for cname in ["tweets","reddit","news","reviews","wl_transactions","stocks_prices"]:
            try:
                self.db[cname].create_index([("merchant", ASCENDING), ("ts", ASCENDING)], name="merchant_ts")
            except Exception:
                pass

        # On startup, mark any stale running/queued jobs as recovered (non-fatal) so UI doesn't show phantom progress
        try:
            self.jobs_col.update_many({"status": {"$in": ["queued", "running"]}}, {"$set": {"status": "error", "error": "stale_after_restart"}})
        except Exception:
            pass
        # Realtime autostart (corrected indentation): proactively ensure latest window(s) for each merchant
        try:
            if os.getenv("RISK_REALTIME_MODE", "0").lower() in ("1","true","yes","on"):
                intervals_env = os.getenv("RISK_AUTOSTART_INTERVALS", "30m")
                intervals = [s.strip() for s in intervals_env.split(',') if s.strip()]
                intervals = [i for i in intervals if i in INTERVAL_MAP]
                merchants = list(self.db["merchants"].find({}, projection={"merchant_name":1, "_id":0}).limit(1000))
                for m in merchants:
                    name = m.get("merchant_name")
                    if not name:
                        continue
                    for ilabel in intervals:
                        try:
                            interval_minutes = INTERVAL_MAP[ilabel]
                            size = interval_minutes * 60
                            now_ts = dt.datetime.now(dt.UTC).timestamp()
                            last_start = self._floor_window(now_ts - size, interval_minutes)
                            existing = self.col.find_one({
                                "merchant": name, "interval_minutes": interval_minutes, "window_start_ts": last_start
                            }, projection={"computed_at_ts":1, "window_end_ts":1, "_id":0})
                            needs = True
                            if existing:
                                comp_ts = existing.get("computed_at_ts") or 0
                                if (now_ts - comp_ts) < (size * 0.5):
                                    needs = False
                            if needs:
                                self.ensure_latest_window(name, ilabel)
                        except Exception:
                            pass
        except Exception:
            pass
        if not self._dispatch_initialized:
            try:
                import threading, time
                def dispatcher_loop():
                    while True:
                        try:
                            self._dispatch_cycle()
                        except Exception:
                            pass
                        time.sleep(self.dispatch_poll_seconds)
                t = threading.Thread(target=dispatcher_loop, name="risk-dispatch", daemon=True)
                t.start()
                self._dispatch_initialized = True
            except Exception:
                pass

    def _dispatch_cycle(self):
        """Promote queued jobs to running respecting max concurrency and priority FIFO ordering."""
        now = dt.datetime.now(dt.UTC)
        running_count = self.jobs_col.count_documents({"status": "running"})
        realtime: bool | None = None  # (fixed trailing comma bug)
        if running_count >= self.max_concurrent_jobs:
            return
        slots = self.max_concurrent_jobs - running_count
        # Find highest priority (lowest number) queued jobs ordered by priority then created_at_ts
        queued = list(self.jobs_col.find({"status": "queued"}, projection={"_id":0, "job_id":1, "priority":1, "created_at_ts":1}).sort([
            ("priority", 1), ("created_at_ts", 1)
        ]).limit(slots))
        if not queued:
            return
        for q in queued:
            job_id = q.get("job_id")
            if not job_id:
                continue
            if realtime is None:
                realtime = os.getenv("RISK_REALTIME_MODE", "0").lower() in ("1","true","yes","on")
            # Atomically promote queued->running; a race safe check
            res = self.jobs_col.update_one({"job_id": job_id, "status": "queued"}, {"$set": {"status": "running", "started_at": now.isoformat().replace('+00:00','Z')}})
            if res.modified_count == 1:
                # Start execution inline (window computation) using thread
                self._start_job_execution(job_id)

    def _start_job_execution(self, job_id: str):
        """Execute a queued/running job by computing its missing windows in batches.

        Relies on job doc fields:
          - _missing_windows (planned list of window_start_ts)
          - planned (count)
          - processed (progress)
        Updates: processed, throughput_wpm, eta_seconds, status=done.
        """
        rec = self.jobs_col.find_one({"job_id": job_id})
        if not rec:
            return
        merchant = rec.get("merchant")
        interval_label = rec.get("interval")
        interval_minutes = INTERVAL_MAP.get(interval_label)
        if interval_minutes is None:
            return
        planned = int(rec.get("planned") or 0)
        win_list = rec.get("_missing_windows") or []
        if not isinstance(win_list, list):
            win_list = []
        missing_windows: List[float] = [float(w) for w in win_list][:planned] if planned else []
        if not missing_windows:
            # Nothing to do -> mark done immediately
            try:
                self.jobs_col.update_one({"job_id": job_id}, {"$set": {"status": "done", "completed_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')}})
            except Exception:
                pass
            return
        per_job_threads = max(1, min(int(os.getenv("RISK_PER_JOB_THREADS", "4")), 16))
        batch_size = max(1, int(os.getenv("RISK_PER_JOB_BATCH", "16")))
        adaptive_batches = os.getenv("RISK_BATCH_ADAPT", "1").lower() in ("1","true","yes","on")
        target_wpm = float(os.getenv("RISK_TARGET_WPM", "30"))  # heuristic target windows/min
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Mark execution start explicitly (helps UI when prefetch is large and no windows processed yet)
        try:
            self.jobs_col.update_one({"job_id": job_id}, {"$set": {
                "exec_started_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z'),
                "stage": "starting",
            }})
        except Exception:
            pass

        def run_job():
            nonlocal batch_size  # allow adaptive adjustments without UnboundLocalError
            try:
                start_ts = dt.datetime.now(dt.UTC).timestamp()
                processed = 0
                idx = 0
                # Optional bulk prefetch to amortize DB calls across many windows
                try:
                    enable_prefetch = os.getenv("RISK_PREFETCH", "1").lower() in ("1","true","yes","on")
                    prefetch_threshold = int(os.getenv("RISK_PREFETCH_MIN_WINDOWS", "12"))
                    if enable_prefetch and len(missing_windows) >= prefetch_threshold:
                        # Update stage so UI doesn't appear frozen during potentially heavy prefetch
                        try:
                            self.jobs_col.update_one({"job_id": job_id}, {"$set": {"stage": "prefetch_building"}})
                        except Exception:
                            pass
                        self._build_prefetch_cache(merchant, interval_minutes, missing_windows)
                        try:
                            self.jobs_col.update_one({"job_id": job_id}, {"$set": {"stage": "prefetch_ready"}})
                        except Exception:
                            pass
                except Exception:
                    # Non-fatal: fall back silently
                    try:
                        self.jobs_col.update_one({"job_id": job_id}, {"$set": {"stage": "prefetch_failed"}})
                    except Exception:
                        pass
                last_adapt_check = 0
                # Set compute stage before entering while loop
                try:
                    self.jobs_col.update_one({"job_id": job_id}, {"$set": {"stage": "computing"}})
                except Exception:
                    pass
                while idx < len(missing_windows) and processed < planned:
                    batch = missing_windows[idx: idx + batch_size]
                    idx += batch_size
                    remaining = planned - processed
                    if len(batch) > remaining:
                        batch = batch[:remaining]
                    if per_job_threads == 1 or len(batch) == 1:
                        for w in batch:
                            try:
                                self.compute_window(merchant, interval_minutes, w)
                            except Exception as e:
                                try:
                                    self.jobs_col.update_one({"job_id": job_id}, {"$push": {"errors": str(e)}})
                                except Exception:
                                    pass
                            processed += 1
                            now_m = dt.datetime.now(dt.UTC).timestamp()
                            elapsed = max(0.001, now_m - start_ts)
                            wpm = (processed / elapsed) * 60.0
                            eta = None if wpm <= 0 else (planned - processed) / (wpm / 60.0)
                            percent = round(min(100.0, (processed / planned) * 100.0), 4) if planned else 100.0
                            upd = {"processed": processed, "percent": percent, "updated_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z'), "throughput_wpm": round(wpm,2)}
                            if eta is not None:
                                upd["eta_seconds"] = round(max(0.0, eta), 2)
                            try:
                                self.jobs_col.update_one({"job_id": job_id}, {"$set": upd})
                            except Exception:
                                pass
                        # Adaptive batch sizing (single-thread mode only) every few seconds
                        if adaptive_batches:
                            now_chk = time.time()
                            if now_chk - last_adapt_check > 5:
                                last_adapt_check = now_chk
                                if wpm < target_wpm and batch_size < 128:
                                    batch_size = min(128, batch_size * 2)
                                elif wpm > target_wpm * 2 and batch_size > 4:
                                    batch_size = max(4, batch_size // 2)
                    else:
                        with ThreadPoolExecutor(max_workers=per_job_threads, thread_name_prefix=f"riskwin-{merchant}") as pool:
                            futures = {pool.submit(self.compute_window, merchant, interval_minutes, w): w for w in batch}
                            for fut in as_completed(futures):
                                try:
                                    _ = fut.result()
                                except Exception as e:
                                    try:
                                        self.jobs_col.update_one({"job_id": job_id}, {"$push": {"errors": str(e)}})
                                    except Exception:
                                        pass
                                processed += 1
                                now_m = dt.datetime.now(dt.UTC).timestamp()
                                elapsed = max(0.001, now_m - start_ts)
                                wpm = (processed / elapsed) * 60.0
                                eta = None if wpm <= 0 else (planned - processed) / (wpm / 60.0)
                                percent = round(min(100.0, (processed / planned) * 100.0), 4) if planned else 100.0
                                upd = {"processed": processed, "percent": percent, "updated_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z'), "throughput_wpm": round(wpm,2)}
                                if eta is not None:
                                    upd["eta_seconds"] = round(max(0.0, eta), 2)
                                try:
                                    self.jobs_col.update_one({"job_id": job_id}, {"$set": upd})
                                except Exception:
                                    pass
                        if adaptive_batches:
                            now_chk = time.time()
                            if now_chk - last_adapt_check > 5:
                                last_adapt_check = now_chk
                                if wpm < target_wpm and batch_size < 128:
                                    batch_size = min(128, batch_size * 2)
                                elif wpm > target_wpm * 2 and batch_size > 4:
                                    batch_size = max(4, batch_size // 2)
                # completion
                try:
                    self.jobs_col.update_one({"job_id": job_id}, {"$set": {"status": "done", "stage": "done", "percent": 100.0, "completed_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')}})
                except Exception:
                    pass
            except Exception as e:
                # Top-level failure; mark job error so UI stops showing 0%
                try:
                    self.jobs_col.update_one({"job_id": job_id}, {"$set": {"status": "error", "stage": "error", "error": str(e), "updated_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')}})
                except Exception:
                    pass
        threading.Thread(target=run_job, name=f"risk-job-{job_id}", daemon=True).start()

    # ----- Fast latest window helper (real-time tick) -----
    def ensure_latest_window(self, merchant: str, interval_label: str) -> Dict[str, Any]:
        ilabel = interval_label.lower()
        if ilabel not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[ilabel]
        now_ts = dt.datetime.now(dt.UTC).timestamp()
        size = interval_minutes * 60
        last_start = self._floor_window(now_ts - size, interval_minutes)
        doc = self.col.find_one({
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "window_start_ts": last_start
        })
        if not doc:
            doc = self.compute_window(merchant, interval_minutes, last_start)
        # Return simplified payload for quick display
        return {
            "merchant": merchant,
            "interval": ilabel,
            "window_start_ts": doc.get("window_start_ts"),
            "window_end_ts": doc.get("window_end_ts"),
            "score": doc.get("score"),
            "confidence": doc.get("confidence"),
        }

    def ensure_latest_window_at(self, merchant: str, interval_label: str, now_ts: float) -> Dict[str, Any]:
        """Variant of ensure_latest_window that allows a simulated 'now' timestamp (epoch seconds).
        Useful for replay / simulation modes where the caller advances time.
        """
        ilabel = interval_label.lower()
        if ilabel not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[ilabel]
        size = interval_minutes * 60
        last_start = self._floor_window(now_ts - size, interval_minutes)
        doc = self.col.find_one({
            "merchant": merchant,
            "interval_minutes": interval_minutes,
            "window_start_ts": last_start
        })
        if not doc:
            doc = self.compute_window(merchant, interval_minutes, last_start)
        return {
            "merchant": merchant,
            "interval": ilabel,
            "window_start_ts": doc.get("window_start_ts"),
            "window_end_ts": doc.get("window_end_ts"),
            "score": doc.get("score"),
            "confidence": doc.get("confidence"),
            "simulated_now_ts": now_ts,
        }

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
        pf_key = (merchant, interval_minutes)
        tweet_avg = reddit_avg = news_avg = review_avg = wl_ratio = std_ret = None
        used_prefetch = False
        counts: Optional[Dict[str, int]] = None
        cache = self._prefetch.get(pf_key)
        fast_mode = os.getenv("RISK_FAST_MODE", "0").lower() in ("1","true","yes","on")
        if fast_mode and cache and cache.get('range') and cache['range'][0] <= start_ts and cache['range'][1] >= end_ts and cache.get('prefix_metrics'):
            try:
                used_prefetch = True
                pm = cache['prefix_metrics']
                tweet_avg = self._prefix_avg(pm.get('tweets'), start_ts, end_ts)
                reddit_avg = self._prefix_avg(pm.get('reddit'), start_ts, end_ts)
                news_avg = self._prefix_avg(pm.get('news'), start_ts, end_ts)
                review_avg = self._prefix_avg(pm.get('reviews'), start_ts, end_ts)
                wl_ratio = self._prefix_ratio(pm.get('wl_transactions'), start_ts, end_ts)
                # For volatility still slice (prefix variance not implemented yet)
                streams = cache.get('streams', {})
                std_ret = self._stock_volatility_prefetch(streams.get('stocks_prices'), start_ts, end_ts)
                counts = self._counts_prefetch_fast(cache, start_ts, end_ts)
                self.metrics["fast_path_hits"] += 1
            except Exception:
                used_prefetch = False
        if not used_prefetch:
            cache = self._prefetch.get(pf_key)
            if cache and cache.get('range') and cache['range'][0] <= start_ts and cache['range'][1] >= end_ts:
                try:
                    used_prefetch = True
                    streams = cache.get('streams', {})
                    tweet_avg = self._avg_sentiment_prefetch(streams.get('tweets'), start_ts, end_ts)
                    reddit_avg = self._avg_sentiment_prefetch(streams.get('reddit'), start_ts, end_ts)
                    news_avg = self._avg_sentiment_prefetch(streams.get('news'), start_ts, end_ts)
                    review_avg = self._avg_reviews_prefetch(streams.get('reviews'), start_ts, end_ts)
                    wl_ratio = self._wl_flag_ratio_prefetch(streams.get('wl_transactions'), start_ts, end_ts)
                    std_ret = self._stock_volatility_prefetch(streams.get('stocks_prices'), start_ts, end_ts)
                    if counts is None:
                        counts = self._counts_prefetch_fast(cache, start_ts, end_ts)
                    self.metrics["prefetch_hits"] += 1
                except Exception:
                    used_prefetch = False
        if not used_prefetch:
            tweet_avg = self._avg_sentiment("tweets", merchant, start_ts, end_ts)
            reddit_avg = self._avg_sentiment("reddit", merchant, start_ts, end_ts)
            news_avg = self._avg_sentiment("news", merchant, start_ts, end_ts)
            review_avg = self._avg_reviews(merchant, start_ts, end_ts)
            wl_ratio = self._wl_flag_ratio(merchant, start_ts, end_ts)
            std_ret = self._stock_volatility(merchant, start_ts, end_ts)
            counts = self._counts_bundle(merchant, start_ts, end_ts)
        if counts is None:
            counts = self._counts_bundle(merchant, start_ts, end_ts)
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
        # Metrics
        try:
            self.metrics["windows_computed"] += 1
        except Exception:
            pass
        return doc

    # ---------------- Prefetch Support -----------------
    def _build_prefetch_cache(self, merchant: str, interval_minutes: int, window_starts: List[float]):
        if not window_starts:
            return
        # Determine overall span to cover (pad by one window to simplify boundary logic)
        first = min(window_starts)
        last = max(window_starts) + interval_minutes * 60
        span_key = (merchant, interval_minutes)
        coll_map = [
            ("tweets", "tweets"),
            ("reddit", "reddit"),
            ("news", "news"),
            ("reviews", "reviews"),
            ("wl_transactions", "wl_transactions"),
            ("stocks_prices", "stocks_prices"),
        ]
        streams: Dict[str, List[Dict[str, Any]]] = {}
        for logical, cname in coll_map:
            try:
                # Query both merchant & merchant_name (case-insensitive) once
                cursor = self.db[cname].find({
                    "$or": [
                        {"merchant": merchant}, {"merchant_name": merchant},
                        {"merchant": {"$regex": f"^{merchant}$", "$options": "i"}},
                        {"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}},
                    ],
                    "ts": {"$gte": first - 1, "$lt": last + 1}
                }, projection={"_id":0}).sort([("ts", 1)])
                docs = list(cursor)
                # Build auxiliary ts list for bisect
                ts_index = [d.get('ts') for d in docs if isinstance(d.get('ts'), (int,float))]
                streams[cname] = {"docs": docs, "ts_index": ts_index}
            except Exception:
                streams[cname] = {"docs": [], "ts_index": []}
        prefix_metrics = {}
        if os.getenv("RISK_FAST_MODE", "0").lower() in ("1","true","yes","on"):
            try:
                prefix_metrics = self._build_prefix_metrics(streams)
            except Exception:
                prefix_metrics = {}
        self._prefetch[span_key] = {"range": (first - 1, last + 1), "streams": streams, "prefix_metrics": prefix_metrics, "built_at": time.time()}
        # Optionally prune old caches
        if len(self._prefetch) > 64:
            # Remove oldest by built_at
            ordered = sorted(self._prefetch.items(), key=lambda kv: kv[1].get('built_at', 0))
            for k,_ in ordered[: len(self._prefetch)//4 or 1]:
                self._prefetch.pop(k, None)

    # ---- Prefetch adaptation helpers ----
    def _slice_prefetch(self, bundle: Optional[Dict[str, Any]], start: float, end: float) -> List[Dict[str, Any]]:
        if not bundle:
            return []
        docs = bundle.get('docs') or []
        ts_index = bundle.get('ts_index') or []
        if not ts_index:
            return [d for d in docs if isinstance(d.get('ts'), (int,float)) and start <= d['ts'] < end]
        lo = bisect.bisect_left(ts_index, start)
        hi = bisect.bisect_left(ts_index, end)
        return docs[lo:hi]

    def _avg_sentiment_prefetch(self, bundle: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        docs = self._slice_prefetch(bundle, start, end)
        if not docs:
            return None
        vals: List[float] = []
        for d in docs:
            ts = d.get('ts')
            if not isinstance(ts, (int,float)):
                continue
            val = None
            for f in ALT_SENTIMENT_FIELDS:
                if f in d:
                    try:
                        vf = float(d.get(f));
                        if -10 <= vf <= 10:
                            val = vf; break
                    except Exception:
                        continue
            if val is None:
                for k,v in d.items():
                    if k == 'ts': continue
                    try: fv=float(v)
                    except Exception: continue
                    if -1 <= fv <= 1:
                        val=fv; break
            if val is not None:
                vals.append(max(-1.0, min(1.0, val)))
        if not vals:
            return None
        return sum(vals)/len(vals)

    def _avg_reviews_prefetch(self, bundle: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        docs = self._slice_prefetch(bundle, start, end)
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
        return sum(vals)/len(vals)

    def _wl_flag_ratio_prefetch(self, bundle: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        docs = self._slice_prefetch(bundle, start, end)
        if not docs:
            return None
        total = 0; flagged = 0
        for d in docs:
            status_val = None
            for f in ALT_STATUS_FIELDS:
                if f in d:
                    status_val = str(d.get(f,'')).lower(); break
            if not status_val: continue
            total += 1
            if any(k in status_val for k in ["flag","fraud","chargeback","blocked","suspicious"]):
                flagged += 1
        if total <= 0:
            return None
        return flagged / total

    def _stock_volatility_prefetch(self, bundle: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        docs = self._slice_prefetch(bundle, start, end)
        if not docs:
            return None
        prices: List[float] = []
        for d in docs:
            for f in ALT_PRICE_FIELDS:
                if f in d:
                    try:
                        fv = float(d.get(f));
                        if fv > 0: prices.append(fv); break
                    except Exception:
                        continue
        if len(prices) < 3:
            return None
        rets=[]
        for i in range(1,len(prices)):
            if prices[i-1] > 0:
                rets.append((prices[i]-prices[i-1])/prices[i-1])
        if len(rets) < 2:
            return None
        try:
            return statistics.pstdev(rets)
        except Exception:
            return None

    # ---------------- Counts helper -----------------
    def _counts_bundle(self, merchant: str, start_ts: float, end_ts: float) -> Dict[str, int]:
        """Return per-stream document counts (tweets/reddit/news/reviews/wl/prices) using
        multiple query strategies to handle schema variation (merchant vs merchant_name,
        seconds vs milliseconds). First non-zero query wins; otherwise fallback to
        manual scan + dynamic timestamp field detection."""

        def detect_timestamp_field(coll_name: str) -> Dict[str, Any]:
            """Detect timestamp field + scale (seconds / ms) for a merchant in a collection.
            Returns {field, scale, confidence} or {} if not found. Caches result.
            """
            cache_key = (coll_name, merchant)
            if cache_key in self._ts_field_cache:
                return self._ts_field_cache[cache_key]
            coll = self.db[coll_name]
            # sample both merchant & merchant_name (case-insensitive)
            try:
                sample_docs = list(
                    coll.find(
                        {"$or": [
                            {"merchant": merchant}, {"merchant_name": merchant},
                            {"merchant": {"$regex": f"^{merchant}$", "$options": "i"}},
                            {"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}},
                        ]},
                        projection={"_id": 0}
                    ).limit(300)
                )
            except Exception:
                sample_docs = []
            if not sample_docs:
                self._ts_field_cache[cache_key] = {}
                return {}
            candidate_fields: Dict[str, Dict[str, Any]] = {}
            potential = set()
            for d in sample_docs:
                for k in d.keys():
                    if k.lower() in {"merchant", "merchant_name"}: continue
                    potential.add(k)
            # Heuristic: prefer obvious time keys
            preferred_order = ["ts","timestamp","time","created_at","createdAt","date","datetime"] + list(potential)
            for field in preferred_order:
                hits = 0; ms_hits = 0; sec_hits = 0
                for d in sample_docs:
                    if field not in d: continue
                    v = d.get(field)
                    ts_parsed = self._parse_ts(v, normalize=True)
                    if ts_parsed is not None:
                        hits += 1
                        if isinstance(v, (int,float)) and float(v) > 1e12:
                            ms_hits += 1
                        else:
                            sec_hits += 1
                if hits > 0:
                    scale = 'ms' if ms_hits > sec_hits else 's'
                    candidate_fields[field] = {"field": field, "scale": scale, "hits": hits, "total": len(sample_docs)}
            if not candidate_fields:
                self._ts_field_cache[cache_key] = {}
                return {}
            # Choose field with highest hits ratio (ties -> earlier in order)
            best = None
            best_ratio = -1.0
            for f in preferred_order:
                meta = candidate_fields.get(f)
                if not meta: continue
                ratio = meta["hits"] / max(1, meta["total"])
                if ratio > best_ratio:
                    best_ratio = ratio
                    best = meta
            if best:
                best["detected_at"] = dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')
                self._ts_field_cache[cache_key] = best
                return best
            self._ts_field_cache[cache_key] = {}
            return {}

        def count_strategies(coll_name: str) -> int:
            coll = self.db[coll_name]
            # Build candidate queries (ordered by most expected usages)
            queries = [
                ({"merchant": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_s"),
                ({"merchant": merchant, "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0}}, "merchant_ms"),
                ({"merchant_name": merchant, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_s"),
                ({"merchant_name": merchant, "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0}}, "merchantName_ms"),
                # Case-insensitive merchant (seconds + ms)
                ({"merchant": {"$regex": f"^{merchant}$", "$options": "i"}, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchant_rx_s"),
                ({"merchant": {"$regex": f"^{merchant}$", "$options": "i"}, "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0}}, "merchant_rx_ms"),
                ({"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}, "ts": {"$gte": start_ts, "$lt": end_ts}}, "merchantName_rx_s"),
                ({"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}, "ts": {"$gte": start_ts * 1000.0, "$lt": end_ts * 1000.0}}, "merchantName_rx_ms"),
            ]
            verbose = os.getenv("RISK_COUNTS_VERBOSE", "0").lower() in ("1","true","yes")
            for q, label in queries:
                try:
                    cnt = coll.count_documents(q, limit=2000)
                except Exception:
                    cnt = 0
                if verbose:
                    print(f"[counts_bundle] {coll_name} {label} -> {cnt}", flush=True)
                if cnt > 0:
                    return cnt
            # Manual sample fallback: handles ISO string timestamps or alternate time field names
            try:
                sample = list(
                    coll.find({"$or": [
                        {"merchant": merchant}, {"merchant_name": merchant},
                        {"merchant": {"$regex": f"^{merchant}$", "$options": "i"}},
                        {"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}}
                    ]}, projection={"_id":0}).limit(400)
                )
            except Exception:
                sample = []
            if not sample:
                return 0
            alt_time_fields = ["ts","timestamp","time","created_at","createdAt","date","datetime"]
            manual = 0
            for d in sample:
                ts_val = None
                for tf in alt_time_fields:
                    if tf in d:
                        ts_val = self._parse_ts(d.get(tf), normalize=True)
                        if ts_val is not None:
                            break
                if ts_val is None:
                    continue
                if start_ts <= ts_val < end_ts:
                    manual += 1
            if verbose:
                print(f"[counts_bundle] {coll_name} manual_scan -> {manual}", flush=True)
            if manual > 0:
                return manual
            # Final attempt: dynamic detected field (could be non 'ts') for a direct range query
            meta = detect_timestamp_field(coll_name)
            if meta.get('field'):
                f = meta['field']
                # Build query on detected field only if numeric; for strings we'll rescan
                # Quick numeric detection
                numeric_query = {"$and": [
                    {"$or": [
                        {"merchant": merchant}, {"merchant_name": merchant},
                        {"merchant": {"$regex": f"^{merchant}$", "$options": "i"}},
                        {"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}},
                    ]},
                    {f: {"$gte": start_ts if meta['scale']=='s' else start_ts*1000.0, "$lt": end_ts if meta['scale']=='s' else end_ts*1000.0}}
                ]}
                try:
                    cnt2 = coll.count_documents(numeric_query, limit=2000)
                except Exception:
                    cnt2 = 0
                if verbose:
                    print(f"[counts_bundle] {coll_name} detected_field({f},{meta.get('scale')}) -> {cnt2}", flush=True)
                if cnt2 > 0:
                    return cnt2
            return 0

        return {
            "tweets": count_strategies("tweets"),
            "reddit": count_strategies("reddit"),
            "news": count_strategies("news"),
            "reviews": count_strategies("reviews"),
            "wl": count_strategies("wl_transactions"),
            "prices": count_strategies("stocks_prices"),
        }

    # ---------------- Historical range helper (for auto backfill) -----------------
    def discover_data_range(self, merchant: str) -> Optional[Dict[str, float]]:
        """Scan stream collections to find earliest & latest parseable timestamps for a merchant.
        Returns {earliest, latest} in epoch seconds or None.
        """
        earliest = None; latest = None
        for cname in STREAM_COLLECTIONS:
            coll = self.db[cname]
            try:
                # sample earliest & latest by ts if exists else generic scan
                docs = list(coll.find({"$or": [
                        {"merchant": merchant}, {"merchant_name": merchant},
                        {"merchant": {"$regex": f"^{merchant}$", "$options": "i"}},
                        {"merchant_name": {"$regex": f"^{merchant}$", "$options": "i"}},
                    ]}, projection={"_id":0}).limit(500))
            except Exception:
                docs = []
            for d in docs:
                # attempt parse across known keys
                for key in ("ts","timestamp","time","created_at","createdAt","date","datetime"):
                    if key not in d: continue
                    tsv = self._parse_ts(d.get(key), normalize=True)
                    if tsv is None: continue
                    if earliest is None or tsv < earliest: earliest = tsv
                    if latest is None or tsv > latest: latest = tsv
        if earliest is None or latest is None:
            return None
        return {"earliest": earliest, "latest": latest}

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
        # Volume heuristic (activity anomaly): configurable scaling
        activity_total = sum(counts.get(k, 0) for k in ["tweets", "reddit", "news", "reviews"])
        vol_cfg = RISK_EVAL_CONFIG.get("volume", {})
        activity_divisor = float(vol_cfg.get("activity_divisor", 200.0)) or 200.0
        min_act = int(vol_cfg.get("min_activity_for_nonzero", 1))
        volume = min(1.0, activity_total / activity_divisor) if activity_total >= min_act else 0.0

        # Incident bump (config driven)
        inc_cfg = RISK_EVAL_CONFIG.get("incident", {})
        incident_bump = 0.0
        wl_th = float(inc_cfg.get("wl_threshold", 0.3))
        wl_scale = float(inc_cfg.get("wl_delta_scale", 2.0))
        wl_mul = float(inc_cfg.get("wl_multiplier", 0.4))
        mk_th = float(inc_cfg.get("market_threshold", 0.6))
        mk_scale = float(inc_cfg.get("market_delta_scale", 1.5))
        mk_cap = float(inc_cfg.get("market_delta_cap", 0.6))
        mk_mul = float(inc_cfg.get("market_multiplier", 0.3))
        mk_total_cap = float(inc_cfg.get("max_total", 1.0))
        if isinstance(wl_component, (int, float)) and wl_component > wl_th:
            incident_bump += min(1.0, (wl_component - wl_th) * wl_scale) * wl_mul
        if isinstance(market, (int, float)) and market > mk_th:
            incident_bump += min(mk_cap, (market - mk_th) * mk_scale) * mk_mul
        incident_bump = min(mk_total_cap, incident_bump)

        # Weighted blend (config weights)
        weighted_parts = []
        weights_eval = RISK_EVAL_CONFIG.get("weights", {})
        if wl_component is not None and weights_eval.get("wl"):
            weighted_parts.append((wl_component, float(weights_eval["wl"])))
        if market is not None and weights_eval.get("market"):
            weighted_parts.append((market, float(weights_eval["market"])))
        if sentiment is not None and weights_eval.get("sentiment"):
            weighted_parts.append((sentiment, float(weights_eval["sentiment"])))
        if volume is not None and weights_eval.get("volume"):
            weighted_parts.append((volume, float(weights_eval["volume"])))
        if weighted_parts:
            total_w = sum(w for _, w in weighted_parts) or 1.0
            blended = sum(v * w for v, w in weighted_parts) / total_w
            damp_cfg = RISK_EVAL_CONFIG.get("dampen", {})
            if damp_cfg.get("enabled", True):
                power = float(damp_cfg.get("blend_power", 0.5)) or 0.5
                blend_scale = float(damp_cfg.get("blend_scale", 0.85))
                incident_scale = float(damp_cfg.get("incident_scale", 0.35))
                compressed = blended ** power
                total_score_0_1 = min(1.0, compressed * blend_scale + incident_bump * incident_scale)
            else:
                total_score_0_1 = min(1.0, blended + incident_bump * 0.3)
        else:
            total_score_0_1 = None
        # ---------------- Stability smoothing & hysteresis -----------------
        raw_total_score_0_1 = total_score_0_1
        stability_cfg = RISK_EVAL_CONFIG.get("stability", {})
        smoothed_total_0_1 = raw_total_score_0_1
        prev_state: Dict[str, Any] = {}
        if stability_cfg.get("ema_enabled") and raw_total_score_0_1 is not None:
            # Fetch previous evaluation strictly before this window for consistent smoothing
            prev_eval = self.eval_col.find_one(
                {
                    "merchant": merchant,
                    "interval_minutes": interval_minutes,
                    "window_end_ts": {"$lt": end_ts},
                },
                sort=[("window_end_ts", -1)],
                projection={
                    "scores.total": 1,
                    "scores.total_raw": 1,
                    "scores_smoothed.total": 1,
                    "state": 1,
                },
            )
            prev_smoothed = None
            if prev_eval:
                # Prefer stored smoothed value chain if present
                prev_smoothed = (
                    ((prev_eval.get("scores_smoothed") or {}).get("total"))
                    or (prev_eval.get("scores") or {}).get("total")
                )
                if isinstance(prev_smoothed, (int, float)):
                    prev_smoothed = float(prev_smoothed) / 100.0  # stored as percent
                else:
                    prev_smoothed = None
                prev_state = prev_eval.get("state") or {}
            if prev_smoothed is not None:
                rise_alpha = float(stability_cfg.get("ema_alpha_rise", 0.4))
                fall_alpha = float(stability_cfg.get("ema_alpha_fall", 0.25))
                alpha = rise_alpha if raw_total_score_0_1 > prev_smoothed else fall_alpha
                # Optional softening near high-threshold region
                hysteresis_tmp = (stability_cfg.get("hysteresis") or {})
                soft_cfg = (stability_cfg.get("soft_near_high") or {})
                if soft_cfg.get("enabled") and isinstance(raw_total_score_0_1, (int,float)):
                    high_th_soft = float(hysteresis_tmp.get("high_threshold", 0.8))
                    buffer = float(soft_cfg.get("buffer", 0.1))
                    region_start = max(0.0, high_th_soft - buffer)
                    if raw_total_score_0_1 >= region_start and raw_total_score_0_1 < high_th_soft:
                        alpha_scale = float(soft_cfg.get("alpha_scale", 0.5))
                        alpha *= max(0.0, min(1.0, alpha_scale))
                alpha = max(0.0, min(1.0, alpha))
                smoothed_total_0_1 = prev_smoothed + alpha * (raw_total_score_0_1 - prev_smoothed)
                # Minimum delta suppression
                min_delta_pct = float(stability_cfg.get("min_delta_pct", 0.3))
                if abs((smoothed_total_0_1 - prev_smoothed) * 100) < min_delta_pct:
                    smoothed_total_0_1 = prev_smoothed
                # Hard cap per-window movement after suppression
                max_rise = stability_cfg.get("max_rise_per_window_pct")
                max_drop = stability_cfg.get("max_drop_per_window_pct")
                if isinstance(max_rise, (int,float)) and prev_smoothed is not None:
                    delta_pp = (smoothed_total_0_1 - prev_smoothed) * 100
                    if delta_pp > float(max_rise):
                        smoothed_total_0_1 = prev_smoothed + float(max_rise)/100.0
                if isinstance(max_drop, (int,float)) and prev_smoothed is not None:
                    delta_pp2 = (smoothed_total_0_1 - prev_smoothed) * 100
                    if delta_pp2 < -float(max_drop):
                        smoothed_total_0_1 = prev_smoothed - float(max_drop)/100.0
        # Hysteresis application (stateful high-risk labeling) - optional
        hysteresis_cfg = stability_cfg.get("hysteresis", {}) if stability_cfg else {}
        risk_state = prev_state.get("risk_state", "normal")
        consecutive_high = int(prev_state.get("consecutive_high", 0))
        if smoothed_total_0_1 is not None and hysteresis_cfg:
            high_th = float(hysteresis_cfg.get("high_threshold", 0.8))
            release_delta = float(hysteresis_cfg.get("release_delta", 0.05))
            consecutive_required = int(hysteresis_cfg.get("consecutive_required", 2))
            if smoothed_total_0_1 >= high_th:
                consecutive_high += 1
            elif smoothed_total_0_1 < (high_th - release_delta):
                consecutive_high = 0
                risk_state = "normal"
            # Update risk state only if consecutive requirement reached
            if consecutive_high >= consecutive_required:
                risk_state = "high"
        # Choose which total becomes exposed
        expose_smoothed = stability_cfg.get("replace_total_with_smoothed", True)
        exported_total = smoothed_total_0_1 if expose_smoothed else raw_total_score_0_1
        # ---------------- End stability logic -----------------
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
                # Total is potentially smoothed for UI stability
                "total": round(exported_total * 100, 2) if exported_total is not None else None,
                "total_raw": round(raw_total_score_0_1 * 100, 2) if raw_total_score_0_1 is not None else None,
                "wl": round(wl_component * 100, 2) if wl_component is not None else None,
                "market": round(market * 100, 2) if market is not None else None,
                "sentiment": round(sentiment * 100, 2) if sentiment is not None else None,
                "volume": round(volume * 100, 2) if volume is not None else None,
                "incident_bump": round(incident_bump * 100, 2),
            },
            # Internal chain for future smoothing; percentages kept like scores.total
            "scores_smoothed": {"total": round(smoothed_total_0_1 * 100, 2) if smoothed_total_0_1 is not None else None},
            "counts": {
                "tweets": counts.get("tweets", 0),
                "reddit": counts.get("reddit", 0),
                "news": counts.get("news", 0),
                "reviews": counts.get("reviews", 0),
                "wl": counts.get("wl", 0),
                "stock_prices": counts.get("prices", 0),
                # Alias for UI expecting 'prices'
                "prices": counts.get("prices", 0),
            },
            "raw_activity_total": activity_total,
            "confidence": base_doc.get("confidence"),
            "drivers": self._driver_list(base_doc),
            "state": {
                "risk_state": risk_state,
                "consecutive_high": consecutive_high,
            },
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
        # Always include the window that contains 'until_ts' by extending one full interval
        # (previous logic excluded the partially-complete latest window, causing empty/"loading" states)
        end_aligned = self._floor_window(until_ts, interval_minutes) + size
        # Safeguard: if range collapses (since in same bucket as until), force exactly one window
        if end_aligned <= start_aligned:
            end_aligned = start_aligned + size
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
        # Ensure interval scoping so mixed-interval evaluation docs don't interfere
        interval_label_l = (interval_label or "30m").lower()
        interval_minutes = None
        try:
            interval_minutes = INTERVAL_MAP.get(interval_label_l)
        except Exception:
            interval_minutes = None
        q: Dict[str, Any] = {"merchant": merchant}
        if interval_minutes is not None:
            q["interval_minutes"] = interval_minutes
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

    # ---------------- Window planning / job orchestration (added) -----------------
    def plan_missing_windows_for_range(self, merchant: str, interval_minutes: int, since_ts: float, until_ts: float) -> List[float]:
        """Return list of window_start_ts that are missing in risk_scores for the given range.
        Range is [since_ts, until_ts). Windows are aligned to interval.
        """
        if until_ts <= since_ts:
            return []
        size = interval_minutes * 60
        start_aligned = self._floor_window(since_ts, interval_minutes)
        end_aligned = self._floor_window(until_ts, interval_minutes)
        if end_aligned <= start_aligned:
            end_aligned = start_aligned + size
        existing = {
            d["window_start_ts"]: 1
            for d in self.col.find(
                {
                    "merchant": merchant,
                    "interval_minutes": interval_minutes,
                    "window_start_ts": {"$gte": start_aligned, "$lt": end_aligned},
                },
                projection={"window_start_ts": 1, "_id": 0},
            )
        }
        missing: List[float] = []
        ts_iter = start_aligned
        while ts_iter < end_aligned:
            if ts_iter not in existing:
                missing.append(ts_iter)
            ts_iter += size
        return missing

    def trigger_or_status(
        self,
        merchant: str,
        interval_label: str,
        submit_fn=None,
        max_backfill_hours: int | None = 168,
        priority: int = 5,
        realtime: bool | None = None,
        now_ts: float | None = None,
    ) -> Dict[str, Any]:
        """Trigger (or fetch status of) a risk evaluation backfill job for a merchant+interval.

        Enhancements vs previous implementation:
          - Persistent job record in Mongo (risk_eval_jobs) with progress + ETA
          - Deduplicates active jobs across process restarts
          - In-memory cache retained for fast status but source of truth is Mongo
        """
        ilabel = interval_label.lower()
        if ilabel not in INTERVAL_MAP:
            raise ValueError("Unsupported interval")
        interval_minutes = INTERVAL_MAP[ilabel]
        # Allow simulated time override for planning (replay / historical focus)
        now_ts = float(now_ts) if now_ts is not None else dt.datetime.now(dt.UTC).timestamp()
        # Resolve realtime mode (explicit param overrides env)
        realtime_mode = (
            realtime
            if realtime is not None
            else os.getenv("RISK_REALTIME_MODE", "0").lower() in ("1", "true", "yes", "on")
        )
        # Lookup existing active job in persistent store
        existing = None
        try:
            existing = self.jobs_col.find_one({
                "merchant": merchant,
                "interval": ilabel,
                "status": {"$in": ["queued", "running"]}
            })
        except Exception:
            existing = None
        if existing:
            total_existing = self.col.count_documents({"merchant": merchant, "interval_minutes": interval_minutes})
            out = {k: existing.get(k) for k in existing.keys() if k != "_id"}
            out.update({"merchant": merchant, "interval": ilabel, "total_windows": total_existing})
            # Compute percent if missing
            planned = out.get("planned") or out.get("missing_planned")
            processed = out.get("processed")
            if planned:
                out["percent"] = round(min(100.0, (processed or 0)/planned * 100.0), 2)
            return out
        # Determine earliest existing window (use merchant baseline if present for consistent alignment)
        baseline_ts = None
        try:
            mdoc = self.db["merchants"].find_one({"merchant_name": merchant}, projection={"risk_eval_baseline_ts":1, "_id":0})
            if mdoc and isinstance(mdoc.get("risk_eval_baseline_ts"), (int,float)):
                baseline_ts = float(mdoc["risk_eval_baseline_ts"])
        except Exception:
            baseline_ts = None
        first_doc = self.col.find_one(
            {"merchant": merchant, "interval_minutes": interval_minutes},
            sort=[("window_start_ts", 1)],
            projection={"window_start_ts":1, "_id":0}
        )
        if first_doc and isinstance(first_doc.get("window_start_ts"), (int,float)):
            earliest = float(first_doc["window_start_ts"])
        else:
            cap_hours = max_backfill_hours if max_backfill_hours is not None else 168
            earliest = baseline_ts if baseline_ts is not None else (now_ts - cap_hours * 3600)
        if baseline_ts is not None and earliest > baseline_ts:
            earliest = baseline_ts
        # Rewind simulation support: if simulated now is earlier than earliest existing window,
        # we need to generate historical windows up to now_ts (instead of returning nothing)
        if now_ts < earliest:
            cap_hours = max_backfill_hours if max_backfill_hours is not None else 168
            # Backfill horizon anchored at simulated now
            rewind_start = now_ts - cap_hours * 3600
            if baseline_ts is not None and rewind_start < baseline_ts:
                rewind_start = baseline_ts
            earliest = self._floor_window(rewind_start, interval_minutes)
        # Realtime trimming: focus only on the most recent N windows (skip deep historical)
        if realtime_mode:
            recent_n = int(os.getenv("RISK_REALTIME_RECENT_WINDOWS", "6") or 6)
            recent_n = max(1, min(recent_n, 500))
            # Earliest allowed timestamp boundary
            earliest_realtime = now_ts - recent_n * interval_minutes * 60
            if earliest < earliest_realtime:
                earliest = self._floor_window(earliest_realtime, interval_minutes)
        # Horizon clamp for simulated time: limit analysis to last RISK_SIM_LOOKBACK_DAYS (default 10d)
        # Applies only when caller provided a simulated now (now_ts explicitly) to speed up rewinds.
        if now_ts is not None:
            try:
                lookback_days = int(os.getenv("RISK_SIM_LOOKBACK_DAYS", "10") or 10)
            except Exception:
                lookback_days = 10
            lookback_days = max(1, min(lookback_days, 120))  # safety bounds 1..120 days
            horizon_start = now_ts - lookback_days * 86400
            if earliest < horizon_start:
                earliest = self._floor_window(horizon_start, interval_minutes)
        # Forward-only simulation mode: do NOT backfill historical beyond limited backtrack windows.
        # Instead only compute windows strictly after the last existing window up to simulated now.
        # Enabled by default (RISK_SIM_FORWARD_ONLY=1) when a simulated now is provided.
        if now_ts is not None and os.getenv("RISK_SIM_FORWARD_ONLY", "1").lower() in ("1","true","yes","on"):
            backtrack_windows = 1
            try:
                backtrack_windows = int(os.getenv("RISK_SIM_FORWARD_BACKTRACK_WINDOWS", "1") or 1)
            except Exception:
                backtrack_windows = 1
            backtrack_windows = max(0, min(backtrack_windows, 12))  # allow small context if desired
            # Find last existing window strictly before now_ts
            last_existing = self.col.find_one(
                {"merchant": merchant, "interval_minutes": interval_minutes, "window_start_ts": {"$lt": now_ts}},
                sort=[("window_start_ts", -1)],
                projection={"window_start_ts": 1, "_id": 0}
            )
            if last_existing and isinstance(last_existing.get("window_start_ts"), (int, float)):
                last_start = float(last_existing["window_start_ts"])
                # Earliest allowed = last_start + interval OR simulated now backtrack
                earliest_candidate = last_start + interval_minutes * 60
            else:
                # No prior window: start at simulated now minus one interval (or zero if backtrack_windows=0)
                earliest_candidate = now_ts - (backtrack_windows * interval_minutes * 60)
            min_allowed = now_ts - (backtrack_windows * interval_minutes * 60)
            if earliest_candidate < min_allowed:
                earliest_candidate = min_allowed
            # Align to interval grid
            earliest_forward = self._floor_window(earliest_candidate, interval_minutes)
            if earliest_forward < earliest:
                # We only raise earliest (move it forward), never move earlier
                earliest = earliest_forward
        missing_windows = self.plan_missing_windows_for_range(merchant, interval_minutes, earliest, now_ts)
        if realtime_mode and missing_windows:
            # Keep only the most recent windows if we still have more than needed
            recent_n = int(os.getenv("RISK_REALTIME_RECENT_WINDOWS", "6") or 6)
            recent_n = max(1, min(recent_n, 500))
            if len(missing_windows) > recent_n:
                missing_windows = missing_windows[-recent_n:]
        job_id = f"{merchant}:{ilabel}:{int(now_ts)}"
        if not missing_windows:
            # Nothing to do; return synthetic done job
            total_windows = self.col.count_documents({"merchant": merchant, "interval_minutes": interval_minutes})
            return {
                "merchant": merchant,
                "interval": ilabel,
                "job_id": job_id,
                "planned": 0,
                "processed": 0,
                "status": "done",
                "baseline_ts": baseline_ts,
                "created_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z'),
                "created_at_ts": now_ts,
                "priority": int(priority),
                "total_windows": total_windows,
                "percent": 100.0,
                "realtime": realtime_mode,
            }
        # Concurrency decision
        running_count = 0
        try:
            running_count = self.jobs_col.count_documents({"status": "running"})
        except Exception:
            running_count = 0
        initial_status = "running" if running_count < self.max_concurrent_jobs else "queued"
        job_doc = {
            "job_id": job_id,
            "merchant": merchant,
            "interval": ilabel,
            "planned": len(missing_windows),
            "processed": 0,
            "status": initial_status,
            "created_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z'),
            "created_at_ts": now_ts,
            "max_backfill_hours": max_backfill_hours,
            "priority": int(priority),
            "baseline_ts": baseline_ts,
            "_missing_windows": missing_windows,
            "realtime": realtime_mode,
        }
        try:
            self.jobs_col.insert_one(job_doc)
        except Exception:
            pass
        # Start immediately if running
        if initial_status == "running":
            self._start_job_execution(job_id)
        total_windows = self.col.count_documents({"merchant": merchant, "interval_minutes": interval_minutes})
        # Prepare status response
        out = {k: v for k, v in job_doc.items() if k != "_id"}
        out["job_id"] = job_id
        out["total_windows"] = total_windows
        if out.get("planned"):
            out["percent"] = round(min(100.0, (out.get("processed") or 0)/ out["planned"] * 100.0), 2)
        return out

    def job_status(self, job_id: str) -> Dict[str, Any] | None:
        # Prefer persistent record
        try:
            rec = self.jobs_col.find_one({"job_id": job_id})
            if rec:
                out = {k: rec.get(k) for k in rec.keys() if k != "_id"}
                planned = out.get("planned") or out.get("missing_planned")
                processed = out.get("processed")
                if planned:
                    out["percent"] = round(min(100.0, (processed or 0)/planned * 100.0), 2)
                return out
        except Exception:
            pass
        # Fallback to in-memory
        rec2 = self.jobs.get(job_id)
        if rec2:
            planned = rec2.get("planned") or rec2.get("missing_planned")
            processed = rec2.get("processed")
            if planned:
                rec2["percent"] = round(min(100.0, (processed or 0)/planned * 100.0), 2)
        return rec2

# -------- Prefix-metric helpers (fast mode) appended outside class if mis-indented fallback --------
    def _build_prefix_metrics(self, streams: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        for sname in ["tweets","reddit","news"]:
            bundle = streams.get(sname) or {}
            docs = bundle.get('docs') or []
            ts_index = bundle.get('ts_index') or []
            ps=[0.0]; pc=[0]
            for d in docs:
                ts = d.get('ts')
                if not isinstance(ts,(int,float)):
                    ps.append(ps[-1]); pc.append(pc[-1]); continue
                val=None
                for f in ALT_SENTIMENT_FIELDS:
                    if f in d:
                        try:
                            vf=float(d.get(f));
                            if -10 <= vf <= 10: val=max(-1.0,min(1.0,vf)); break
                        except Exception: continue
                if val is None:
                    for k,v in d.items():
                        if k=='ts': continue
                        try: fv=float(v)
                        except Exception: continue
                        if -1 <= fv <= 1: val=fv; break
                if val is None:
                    ps.append(ps[-1]); pc.append(pc[-1]); continue
                ps.append(ps[-1]+float(val)); pc.append(pc[-1]+1)
            metrics[sname]={"ts_index": ts_index, "psum": ps, "pcount": pc}
        # Reviews
        bundle = streams.get('reviews') or {}
        docs = bundle.get('docs') or []
        ts_index = bundle.get('ts_index') or []
        ps=[0.0]; pc=[0]
        for d in docs:
            val=None
            for f in ALT_REVIEW_FIELDS:
                if f in d:
                    try:
                        fv=float(d.get(f));
                        if -1 <= fv <= 10: val=max(0.0,min(5.0,fv)); break
                    except Exception: continue
            if val is None: ps.append(ps[-1]); pc.append(pc[-1]); continue
            ps.append(ps[-1]+float(val)); pc.append(pc[-1]+1)
        metrics['reviews']={"ts_index": ts_index, "psum": ps, "pcount": pc}
        # WL
        wl_bundle = streams.get('wl_transactions') or {}
        wl_docs = wl_bundle.get('docs') or []
        wl_ts = wl_bundle.get('ts_index') or []
        total_p=[0]; flagged_p=[0]
        for d in wl_docs:
            status_val=None
            for f in ALT_STATUS_FIELDS:
                if f in d: status_val=str(d.get(f,'')).lower(); break
            tot=0; flg=0
            if status_val:
                tot=1
                if any(k in status_val for k in ["flag","fraud","chargeback","blocked","suspicious"]): flg=1
            total_p.append(total_p[-1]+tot); flagged_p.append(flagged_p[-1]+flg)
        metrics['wl_transactions']={"ts_index": wl_ts, "total_p": total_p, "flagged_p": flagged_p}
        # Prices just ts_index copy
        price_bundle = streams.get('stocks_prices') or {}
        metrics['stocks_prices']={"ts_index": price_bundle.get('ts_index') or []}
        return metrics

    def _prefix_avg(self, metric: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        if not metric: return None
        ts_index = metric.get('ts_index') or []
        if not ts_index: return None
        psum = metric.get('psum'); pcount = metric.get('pcount')
        if not (isinstance(psum,list) and isinstance(pcount,list)): return None
        lo = bisect.bisect_left(ts_index, start); hi = bisect.bisect_left(ts_index, end)
        if hi>=len(psum) or lo>=len(psum): return None
        total = psum[hi]-psum[lo]; cnt = pcount[hi]-pcount[lo]
        if cnt<=0: return None
        return total/cnt

    def _prefix_ratio(self, metric: Optional[Dict[str, Any]], start: float, end: float) -> Optional[float]:
        if not metric: return None
        ts_index = metric.get('ts_index') or []
        if not ts_index: return None
        total_p = metric.get('total_p'); flagged_p = metric.get('flagged_p')
        if not (isinstance(total_p,list) and isinstance(flagged_p,list)): return None
        lo = bisect.bisect_left(ts_index, start); hi = bisect.bisect_left(ts_index, end)
        if hi>=len(total_p) or lo>=len(total_p): return None
        tot = total_p[hi]-total_p[lo]
        if tot<=0: return None
        flg = flagged_p[hi]-flagged_p[lo] if hi < len(flagged_p) and lo < len(flagged_p) else 0
        return flg/tot if tot>0 else None

    def _counts_prefetch_fast(self, cache: Dict[str, Any], start: float, end: float) -> Dict[str, int]:
        pm = cache.get('prefix_metrics') or {}
        out = {"tweets":0,"reddit":0,"news":0,"reviews":0,"wl":0,"prices":0}
        def span(metric):
            if not metric: return 0
            ts_index = metric.get('ts_index') or []
            if not ts_index: return 0
            lo = bisect.bisect_left(ts_index, start); hi = bisect.bisect_left(ts_index, end)
            return max(0, hi-lo)
        out['tweets']=span(pm.get('tweets'))
        out['reddit']=span(pm.get('reddit'))
        out['news']=span(pm.get('news'))
        out['reviews']=span(pm.get('reviews'))
        wl_metric = pm.get('wl_transactions')
        if wl_metric:
            ts_index = wl_metric.get('ts_index') or []
            lo = bisect.bisect_left(ts_index, start); hi = bisect.bisect_left(ts_index, end)
            tot = wl_metric.get('total_p') or []
            if hi < len(tot) and lo < len(tot): out['wl'] = max(0,(tot[hi]-tot[lo]))
        prices_metric = pm.get('stocks_prices') or {}
        out['prices']=span(prices_metric)
        return out

    def list_jobs(self, active: bool = True) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if active:
            query["status"] = {"$in": ["queued", "running"]}
        try:
            cursor = self.jobs_col.find(query, projection={"_id": 0}).sort([("created_at_ts", -1)]).limit(500)
            jobs = list(cursor)
        except Exception:
            # fallback to in-memory
            jobs = []
            for jid, rec in self.jobs.items():
                if active and rec.get("status") not in {"queued","running"}:
                    continue
                jobs.append({"job_id": jid, **rec})
        # Compute percent for each
        for j in jobs:
            planned = j.get("planned") or j.get("missing_planned") or 0
            processed = j.get("processed") or 0
            if planned:
                j["percent"] = round(min(100.0, processed / planned * 100.0), 2)
            elif j.get("status") == "done":
                j["percent"] = 100.0
        return jobs

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

    def get_metrics(self) -> Dict[str, Any]:
        out = dict(self.metrics)
        out["prefetch_cache_entries"] = len(self._prefetch)
        out["generated_at"] = dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')
        return out
