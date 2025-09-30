#!/usr/bin/env python3
# main.py
# Entrypoint FastAPI server for merchant onboarding, preset generation, and stream data retrieval (tweets/reddit/news/reviews/wl/stock) in MongoDB.
# Run: uvicorn main:app --host 0.0.0.0 --port 8000
#
# Conventions:
# - Merchant unique identifier: merchant_name (string; mandatory)
# - Collections: "merchants" (merchant details), "preset" (per-merchant preset block)
# - Stream collections (by generators): "tweets", "reddit", "news", "reviews", "wl_transactions", "stocks_prices", "stocks_earnings", "stocks_actions", "stocks_meta"
# - Off-main-thread execution using ThreadPoolExecutor tasks (returns task_id to poll)
#
# Date parsing and ISO formatting compatible with "data_api" patterns.

import os
import uuid
import time
import datetime as dt
import random
from typing import Any, Dict, Optional, List, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient, ASCENDING, DESCENDING
from concurrent.futures import ThreadPoolExecutor, Future
import threading

from dotenv import load_dotenv
load_dotenv("common.env")

# Config (env)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "merchant_analytics")
LOG_REQUESTS = str(os.getenv("MAIN_LOG_REQUESTS", "true")).lower() in ("1","true","yes","on")
MAX_WORKERS = int(os.getenv("MAIN_MAX_WORKERS", str(os.cpu_count() or 4)))

# Mongo connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def log(msg: str):
    if LOG_REQUESTS:
        ts = dt.datetime.now(dt.UTC).isoformat().replace("+00:00","Z")
        print(f"[{ts}] {msg}", flush=True)

def dt_to_iso(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.UTC)
    return dt_obj.astimezone(dt.UTC).isoformat().replace("+00:00","Z")

# Date parsing compatible with multiple formats
def parse_any_dt(x: Optional[str]) -> Optional[dt.datetime]:
    import re
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
    s_lower = s.lower()
    if s_lower in ("now", "utcnow", "current"):
        return dt.datetime.now(dt.UTC)
    m = re.match(r"^now\s*([+-])\s*(\d+)\s*([smhdw])$", s_lower)
    if m:
        unit = m.group(3)
        n = int(m.group(2))
        sign = -1 if m.group(1) == "-" else 1
        td_map = {"s":"seconds","m":"minutes","h":"hours","d":"days","w":"weeks"}
        delta = dt.timedelta(**{td_map[unit]: n})
        return dt.datetime.now(dt.UTC) + sign*delta
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
    return None

def parse_window_to_timedelta(window: str) -> dt.timedelta:
    import re
    s = (window or "").strip().lower()
    m = re.match(r"^(\d+)\s*([smhdw])$", s)
    if not m:
        raise ValueError("Window must look like 15m, 1h, 24h, 7d, 1w")
    n = int(m.group(1)); unit = m.group(2)
    return {
        "s": dt.timedelta(seconds=n),
        "m": dt.timedelta(minutes=n),
        "h": dt.timedelta(hours=n),
        "d": dt.timedelta(days=n),
        "w": dt.timedelta(weeks=n),
    }[unit]

# ---------- Task Manager ----------

class TaskRecord(BaseModel):
    task_id: str
    status: str  # pending | running | done | error
    submitted_at: str
    updated_at: str
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

class TaskManager:
    def __init__(self, max_workers: int = MAX_WORKERS):
        self.pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="onboard")
        self.tasks: Dict[str, TaskRecord] = {}
        self.lock = threading.Lock()

    def submit(self, fn, *args, **kwargs) -> str:
        tid = uuid.uuid4().hex
        now = dt_to_iso(dt.datetime.now(dt.UTC))
        with self.lock:
            self.tasks[tid] = TaskRecord(task_id=tid, status="pending", submitted_at=now, updated_at=now)
        def wrapper():
            self._update(tid, status="running")
            try:
                res = fn(*args, **kwargs)
                self._update(tid, status="done", result=res)
            except Exception as e:
                self._update(tid, status="error", error=str(e))
        future: Future = self.pool.submit(wrapper)
        return tid

    def _update(self, tid: str, **fields):
        with self.lock:
            t = self.tasks.get(tid)
            if not t:
                return
            for k, v in fields.items():
                setattr(t, k, v)
            t.updated_at = dt_to_iso(dt.datetime.now(dt.UTC))
            self.tasks[tid] = t

    def status(self, tid: str) -> Optional[TaskRecord]:
        with self.lock:
            return self.tasks.get(tid)

TASKS = TaskManager()

# ---------- Service import ----------

# MerchantService implements onboarding and preset generation
from merchant import MerchantService  # same directory
from risk_eval import RiskEvaluator

SERVICE = MerchantService(db)
RISK = RiskEvaluator(db)

# Ensure indexes for new evaluation collection
try:
    RISK.ensure_indexes()
except Exception as _e:
    log(f"RiskEvaluator.ensure_indexes error: {_e}")

# ---------- FastAPI setup ----------

app = FastAPI(title="Merchant Onboarding & Data API", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup_event():
    try:
        SERVICE.ensure_indexes()
        RISK.ensure_indexes()
        log(f"Indexes ensured on Mongo {MONGO_URI}/{DB_NAME} (service + risk)")
    except Exception as e:
        log(f"Startup error ensuring indexes: {e}")

# Middleware logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.time()
    resp = None
    try:
        resp = await call_next(request)
        return resp
    finally:
        took = int((time.time() - started) * 1000)
        if LOG_REQUESTS:
            try:
                log(f'{request.method} {request.url.path}?{request.url.query} {resp.status_code if resp else "-"} {took}ms')
            except Exception:
                pass

# ---------- Models ----------

class OnboardRequest(BaseModel):
    merchant_name: str = Field(..., description="Mandatory unique identifier (name)")
    deep_scan: bool = Field(False, description="Force regenerate preset even if exists")
    details: Optional[Dict[str, Any]] = Field(None, description="Optional merchant fields to override auto-generated")
    preset_overrides: Optional[Dict[str, Any]] = Field(None, description="Optional overrides for generated preset block")
    start_date: Optional[str] = Field(None, description="ISO/date for preset start; default now-3y")
    end_date: Optional[str] = Field(None, description="ISO/date for preset end; default now+1y")
    seed: Optional[int] = Field(None, description="Optional seed for reproducible generation")

# ---------- Core helpers for data endpoints ----------

DEFAULT_STREAMS = ["tweets","reddit","news","reviews","stock","wl"]
STREAM_COLLECTION_MAP = {
    "tweets": "tweets",
    "reddit": "reddit",
    "news": "news",
    "reviews": "reviews",
    "wl": "wl_transactions",
}

def compute_range_params(window: Optional[str], since: Optional[str], until: Optional[str], allow_future: bool, now_iso: Optional[str]) -> Tuple[float, float, str, str]:
    now_dt = parse_any_dt(now_iso) or dt.datetime.now(dt.UTC)
    if window and window.strip():
        delta = parse_window_to_timedelta(window.strip())
        s = now_dt - delta
        u = now_dt
    else:
        s = parse_any_dt(since) or (now_dt - dt.timedelta(days=7))
        u = parse_any_dt(until) or now_dt
    if not allow_future:
        # Cap until at now
        if u > now_dt:
            u = now_dt
    s_ts = s.timestamp()
    u_ts = u.timestamp()
    return s_ts, u_ts, s.astimezone(dt.UTC).isoformat().replace("+00:00","Z"), u.astimezone(dt.UTC).isoformat().replace("+00:00","Z")

def query_stream(coll_name: str, merchant_name: str, s_ts: float, u_ts: float, order: str, limit: int) -> List[Dict[str, Any]]:
    coll = db[coll_name]
    q: Dict[str, Any] = {"merchant": merchant_name}
    q["ts"] = {"$gte": s_ts, "$lte": u_ts}
    sort_key = DESCENDING if (order or "desc").lower() == "desc" else ASCENDING
    try:
        cursor = coll.find(q, projection={"_id":0}).sort([("ts", sort_key)])
        if limit and limit > 0:
            cursor = cursor.limit(int(limit))
        return list(cursor)
    except Exception as e:
        log(f"query_stream error [{coll_name}/{merchant_name}]: {e}")
        return []

def fetch_stock(merchant_name: str, s_ts: float, u_ts: float, order: str, limit: int, include_meta: bool) -> Dict[str, Any]:
    prices = query_stream("stocks_prices", merchant_name, s_ts, u_ts, order, limit)
    earnings = query_stream("stocks_earnings", merchant_name, s_ts, u_ts, order, limit)
    actions = query_stream("stocks_actions", merchant_name, s_ts, u_ts, order, limit)
    out = {
        "prices": prices,
        "earnings": earnings,
        "corporate_actions": actions
    }
    if include_meta:
        try:
            mdoc = db["stocks_meta"].find_one({"merchant": merchant_name}, projection={"_id":0})
            out["meta"] = (mdoc or {}).get("meta", {})
        except Exception:
            out["meta"] = {}
    return out

def normalize_streams_arg(streams: str) -> List[str]:
    s = (streams or "").strip().lower()
    if not s or s == "all":
        return DEFAULT_STREAMS
    items = [x.strip() for x in s.split(",") if x.strip()]
    # keep only supported
    final = []
    for it in items:
        if it in DEFAULT_STREAMS:
            final.append(it)
    return final or DEFAULT_STREAMS

# ---------- Routes ----------

@app.get("/")
def root():
    return {
        "name": "Merchant Onboarding & Data API",
        "version": "0.2.0",
        "notes": [
            "POST /v1/onboard runs off main thread and returns a task_id.",
            "Merchant unique key: merchant_name.",
            "Collections: merchants, preset, risk_scores.",
            "Data endpoints: GET /v1/{merchant}/data and /v1/{merchant}/stock",
            "Risk scoring: POST /v1/{merchant}/risk-eval/trigger?interval=30m |1h|1d then GET /v1/{merchant}/risk-eval/scores"
        ]
    }

@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        return {"status": "ok", "mongo": MONGO_URI, "db": DB_NAME}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status":"error","detail":str(e)})

@app.get("/v1/merchants")
def list_merchants():
    return {"merchants": SERVICE.list_merchant_names()}

@app.get("/v1/merchants/{merchant_name}")
def get_merchant(merchant_name: str):
    doc = SERVICE.get_merchant(merchant_name)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    return {"merchant": doc}

@app.get("/v1/preset/{merchant_name}")
def get_preset(merchant_name: str):
    doc = SERVICE.get_preset(merchant_name)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Preset not found for: {merchant_name}")
    return {"preset": doc}

@app.get("/v1/preset")
def list_presets():
    return {"merchants_with_preset": SERVICE.list_preset_names()}

@app.get("/v1/tasks/{task_id}")
def task_status(task_id: str):
    tr = TASKS.status(task_id)
    if not tr:
        raise HTTPException(status_code=404, detail="Task not found")
    return tr.dict()

@app.post("/v1/onboard")
def onboard(req: OnboardRequest):
    # Normalize dates
    now = dt.datetime.now(dt.UTC)
    sdt = parse_any_dt(req.start_date) if req.start_date else (now - dt.timedelta(days=365*3))
    edt = parse_any_dt(req.end_date) if req.end_date else (now + dt.timedelta(days=365))
    if sdt and edt and sdt > edt:
        raise HTTPException(status_code=400, detail="start_date cannot be after end_date")
    # Submit off-main-thread job
    def job():
        return SERVICE.onboard_and_generate_preset(
            merchant_name=req.merchant_name.strip(),
            start_date=sdt.date().isoformat(),
            end_date=edt.date().isoformat(),
            deep_scan=req.deep_scan,
            details=req.details or {},
            preset_overrides=req.preset_overrides or {},
            seed=req.seed
        )
    tid = TASKS.submit(job)
    return {"status": "accepted", "task_id": tid}

@app.post("/v1/preset/rebuild/{merchant_name}")
def rebuild_preset(merchant_name: str, seed: Optional[int] = None):
    def job():
        # Fetch current merchant to get names, ensure exists
        m = SERVICE.get_merchant(merchant_name)
        if not m:
            raise RuntimeError(f"Merchant not found: {merchant_name}")
        # Use default 3y prior, 1y future if absent on merchant doc
        now = dt.datetime.now(dt.UTC)
        s = m.get("start_date") or (now - dt.timedelta(days=365*3)).date().isoformat()
        e = m.get("end_date") or (now + dt.timedelta(days=365)).date().isoformat()
        return SERVICE.generate_or_update_preset(merchant_name, start_date=s, end_date=e, deep_scan=True, preset_overrides={}, seed=seed)
    tid = TASKS.submit(job)
    return {"status": "accepted", "task_id": tid}

# ---------- Data endpoints (tweets/reddit/news/reviews/wl/stock) ----------

@app.get("/v1/{merchant_name}/data")
def get_streams_data(
    merchant_name: str,
    streams: str = "all",
    order: str = "desc",
    limit: int = 5000,
    window: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    include_stock_meta: bool = False,
    allow_future: bool = False,
    now: Optional[str] = None
):
    # Validate merchant
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")

    # Compute range
    try:
        s_ts, u_ts, s_iso, u_iso = compute_range_params(window, since, until, allow_future, now)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid range: {e}")

    # Normalize streams
    stream_list = normalize_streams_arg(streams)

    # Fetch data per stream
    data: Dict[str, Any] = {}
    range_info: Dict[str, Dict[str, str]] = {}
    limits: Dict[str, Any] = {}

    # Tweets
    if "tweets" in stream_list:
        data["tweets"] = query_stream(STREAM_COLLECTION_MAP["tweets"], merchant_name, s_ts, u_ts, order, limit)
        range_info["tweets"] = {"since": s_iso, "until": u_iso}

    # Reddit
    if "reddit" in stream_list:
        data["reddit"] = query_stream(STREAM_COLLECTION_MAP["reddit"], merchant_name, s_ts, u_ts, order, limit)
        range_info["reddit"] = {"since": s_iso, "until": u_iso}

    # News
    if "news" in stream_list:
        data["news"] = query_stream(STREAM_COLLECTION_MAP["news"], merchant_name, s_ts, u_ts, order, limit)
        range_info["news"] = {"since": s_iso, "until": u_iso}

    # Reviews
    if "reviews" in stream_list:
        data["reviews"] = query_stream(STREAM_COLLECTION_MAP["reviews"], merchant_name, s_ts, u_ts, order, limit)
        range_info["reviews"] = {"since": s_iso, "until": u_iso}

    # WL
    if "wl" in stream_list:
        data["wl"] = query_stream(STREAM_COLLECTION_MAP["wl"], merchant_name, s_ts, u_ts, order, limit)
        range_info["wl"] = {"since": s_iso, "until": u_iso}

    # Stock (composite)
    if "stock" in stream_list:
        data["stock"] = fetch_stock(merchant_name, s_ts, u_ts, order, limit, include_meta=include_stock_meta)
        range_info["stock"] = {"since": s_iso, "until": u_iso}

    return {
        "merchant": merchant_name,
        "order": order,
        "range": range_info,
        "limits": limits,  # placeholder for future min/max/total
        "data": data,
    }

@app.get("/v1/{merchant_name}/stock")
def get_stock_data(
    merchant_name: str,
    order: str = "desc",
    limit: int = 5000,
    window: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    include_stock_meta: bool = False,
    allow_future: bool = False,
    now: Optional[str] = None
):
    # Validate merchant
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")

    # Compute range
    try:
        s_ts, u_ts, s_iso, u_iso = compute_range_params(window, since, until, allow_future, now)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid range: {e}")

    stock = fetch_stock(merchant_name, s_ts, u_ts, order, limit, include_meta=include_stock_meta)
    return {
        "merchant": merchant_name,
        "order": order,
        "range": {"stock": {"since": s_iso, "until": u_iso}},
        "data": stock
    }

# ---------------- Risk Evaluation Endpoints (moved above __main__) ----------------

def _parse_backfill_param(raw: str | None) -> int | None:
    """Convert raw query string for max_backfill_hours into int or None.
    Rules:
      - missing (None): return default cap (168)
      - empty string / whitespace: unlimited (None)
      - keywords: none, full, all, unlimited, unlimited, no, off -> unlimited (None)
      - numeric > 0: that number
      - numeric <= 0: unlimited (None)
    """
    if raw is None:
        return 168  # default 7d cap
    s = raw.strip().lower()
    if s == "":
        return None
    if s in {"none","full","all","unlimited","no","off"}:
        return None
    try:
        v = int(s)
        if v <= 0:
            return None
        return v
    except Exception:
        # Fallback: ignore bad value -> default cap
        return 168

@app.post("/v1/{merchant_name}/risk-eval/trigger")
def trigger_risk_eval(
    merchant_name: str,
    interval: str = "30m",
    autoseed: bool = False,
    max_backfill_hours: str | None = None,
    bootstrap: bool = False,
    bootstrap_hours: int = 24,
    bootstrap_step: int = 60,
    since: float | None = None,
    until: float | None = None
):
    # Validate merchant
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    actions: list[dict] = []
    autoseed_result = None
    # Optional lightweight auto-seed if no underlying data exists yet
    if autoseed:
        try:
            needed = ["tweets","reddit","news","reviews","wl_transactions","stocks_prices"]
            empty = True
            for coll in needed:
                if db[coll].count_documents({"merchant": merchant_name}, limit=1) > 0:
                    empty = False
                    break
            if empty:
                try:
                    autoseed_result = seed_risk_data(merchant_name=merchant_name, hours=36, step=60, volatility=0.01, confirm="yes")
                    actions.append({"action":"autoseed","detail":"seeded 36h synthetic"})
                    log(f"Auto-seeded synthetic data for {merchant_name} (autoseed) before triggering risk eval")
                except Exception as se:
                    actions.append({"action":"autoseed","error": str(se)})
                    log(f"Autoseed failed (non-fatal) for {merchant_name}: {se}")
        except Exception as ie:
            actions.append({"action":"autoseed_inspect","error": str(ie)})
            log(f"Autoseed inspection failed for {merchant_name}: {ie}")
    try:
        cap = _parse_backfill_param(max_backfill_hours)
        status = RISK.trigger_or_status(merchant_name, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=cap)
        # Optional explicit range backfill (only missing windows in [since, until])
        if since is not None and until is not None and until > since:
            from risk_eval import INTERVAL_MAP
            ilabel = interval.lower()
            if ilabel in INTERVAL_MAP:
                iv = INTERVAL_MAP[ilabel]
                missing = RISK.plan_missing_windows_for_range(merchant_name, iv, float(since), float(until))
                if missing:
                    actions.append({"action":"range_backfill_plan","missing_windows": len(missing)})
                    # Launch a separate job for those windows only
                    def runner():
                        for idx, w in enumerate(missing, start=1):
                            RISK.compute_window(merchant_name, iv, w)
                    TASKS.pool.submit(runner)
                else:
                    actions.append({"action":"range_backfill_plan","missing_windows": 0})
        # If no windows planned AND bootstrap requested, attempt a quick seed + retry
        if bootstrap and status.get("total_windows",0) == 0:
            try:
                bh = max(1, min(int(bootstrap_hours or 24), 72))
                bstp = max(5, min(int(bootstrap_step or 60), 180))
                # Seed a smaller recent range so earliest_timestamp exists
                seed_res = seed_risk_data(merchant_name=merchant_name, hours=bh, step=bstp, volatility=0.01, confirm="yes")
                actions.append({"action":"bootstrap_seed","hours": bh, "step": bstp, "seeded": seed_res.get("seeded")})
                # Retry trigger (cap backfill to bootstrap hours + small cushion)
                retry_cap = bh + 2
                status = RISK.trigger_or_status(merchant_name, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=retry_cap)
                actions.append({"action":"bootstrap_retry","windows": status.get("total_windows")})
            except Exception as be:
                actions.append({"action":"bootstrap_seed","error": str(be)})
        return {"job": status, "actions": actions, "autoseed_result": autoseed_result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Convenience GET alias so users can paste the URL in a browser and still trigger the job.
@app.get("/v1/{merchant_name}/risk-eval/trigger")
def trigger_risk_eval_get(merchant_name: str, interval: str = "30m", autoseed: bool = False, max_backfill_hours: str | None = None):
    return trigger_risk_eval(merchant_name=merchant_name, interval=interval, autoseed=autoseed, max_backfill_hours=max_backfill_hours)

@app.get("/v1/{merchant_name}/risk-eval/job/{job_id}")
def risk_eval_job_status(merchant_name: str, job_id: str):
    job = RISK.job_status(job_id)
    if not job or job.get("merchant") != merchant_name:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job": job}

@app.get("/v1/{merchant_name}/risk-eval/scores")
def get_risk_scores(
    merchant_name: str,
    interval: str = "30m",
    limit: int = 500,
    since: float | None = None,
    until: float | None = None
):
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    try:
        scores = RISK.fetch_scores(merchant_name, interval, limit=limit, since=since, until=until)
        return {"merchant": merchant_name, "interval": interval, "count": len(scores), "scores": scores}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/{merchant_name}/risk-eval/summary")
def get_risk_summary(
    merchant_name: str,
    interval: str = "auto",
    lookback: int = 50,
    since: float | None = None,
    until: float | None = None,
    now: float | None = None,
    window: str | None = None,
    fake: bool = False
):
    """Lightweight summary of recent risk scores for dashboard consumption.
    Parameters:
      - interval: 30m | 1h | 1d | auto (auto picks the finest interval with >= 3 windows in range)
      - lookback: max windows to fetch (upper bound before range filtering)
      - since/until: optional epoch seconds to bound windows (after fetch), paired with 'now' & 'window' convenience.
      - window + now: if provided (e.g. window=6h) compute since=now-window.
    """
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    try:
        # Derive time range from window & now if given
        rng_since = since
        rng_until = until
        if window:
            try:
                delta = parse_window_to_timedelta(window)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid window: {e}")
            base_now = dt.datetime.now(dt.UTC).timestamp() if now is None else float(now)
            rng_until = base_now if rng_until is None else rng_until
            rng_since = (base_now - delta.total_seconds()) if rng_since is None else rng_since

        chosen_interval = interval
        intervals_order = ["30m","1h","1d"]
        if interval == "auto":
            # Try each interval from finest; choose first with >=3 windows in (since, until)
            for cand in intervals_order:
                try:
                    rows = RISK.fetch_scores(merchant_name, cand, limit=lookback, since=rng_since, until=rng_until)
                except ValueError:
                    continue
                if len(rows) >= 3:
                    chosen_interval = cand
                    break
            if chosen_interval == "auto":
                # fallback to coarsest if nothing found
                chosen_interval = "1d"
        summary = RISK.summarize_scores(merchant_name, chosen_interval, lookback=lookback, since_ts=rng_since, until_ts=rng_until)

        # ---------- Fake data injection (debug / demo) ----------
        want_fake = fake or (os.getenv("RISK_FAKE_MODE","0").lower() in ("1","true","yes"))
        if want_fake and (summary.get("count",0) == 0):
            # fabricate a synthetic timeline
            import math, random as _rnd
            seed_val = hash((merchant_name, chosen_interval, int((rng_until or 0)//3600))) & 0xffffffff
            _rnd.seed(seed_val)
            # choose step seconds from interval label
            step_map = {"30m":1800, "1h":3600, "1d":86400}
            step = step_map.get(chosen_interval, 3600)
            end_ts = (rng_until or (dt.datetime.now(dt.UTC).timestamp()))
            start_ts = end_ts - step * 20
            points = []
            ts_iter = start_ts
            base = _rnd.uniform(25, 65)
            for i in range(20):
                noise = _rnd.uniform(-8, 8)
                seasonal = 10 * math.sin(i/4.0)
                val = max(0, min(100, base + seasonal + noise))
                points.append({"t": ts_iter + step, "s": round(val,2)})
                ts_iter += step
            confid = []
            for p in points:
                confid.append({"t": p["t"], "c": round(_rnd.uniform(0.4,0.95),3)})
            latest_point = points[-1]
            comp_keys = ["tweet_sentiment","reddit_sentiment","news_sentiment","reviews_rating","wl_flag_ratio","stock_volatility"]
            latest_components = {k: round(_rnd.uniform(0,1),3) for k in comp_keys}
            avg_components = {k: round(sum(_rnd.uniform(0,1) for _ in range(8))/8,3) for k in comp_keys}
            summary = {
                "interval": chosen_interval,
                "count": len(points),
                "latest": {"score": latest_point["s"], "confidence": confid[-1]["c"], "components": latest_components},
                "previous": {"score": points[-2]["s"], "confidence": confid[-2]["c"]} if len(points) > 1 else None,
                "delta": round(points[-1]["s"] - points[-2]["s"],2) if len(points) > 1 else None,
                "trend": points,
                "trend_confidence": confid,
                "component_latest": latest_components,
                "component_avg": avg_components,
                "avg_score": round(sum(p["s"] for p in points)/len(points),2),
                "avg_confidence": round(sum(c["c"] for c in confid)/len(confid),3)
            }
            summary["_fake"] = True

        return {"merchant": merchant_name, "interval_selected": chosen_interval, **summary, "range_used": {"since": rng_since, "until": rng_until}}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/{merchant_name}/risk-eval/seed")
def seed_risk_data(
    merchant_name: str,
    hours: int = 48,
    step: int = 60,
    volatility: float = 0.01,
    confirm: str = "yes"
):
    """Quick synthetic seeding helper so risk scoring has baseline data.
    NOT for production; gated by confirm parameter.
    Creates evenly spaced docs over the past <hours> with <step> minute spacing.
    Collections touched: tweets, reddit, news, reviews, wl_transactions, stocks_prices.
    """
    if confirm.lower() not in ("yes","y","true","1"):
        raise HTTPException(status_code=400, detail="Set confirm=yes to proceed (safety guard)")
    # Validate merchant exists
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    hours = max(1, min(hours, 24*14))  # cap at 14 days
    step = max(5, min(step, 180))      # 5 min .. 3h
    now_dt = dt.datetime.now(dt.UTC)
    start_dt = now_dt - dt.timedelta(hours=hours)
    base_price = 100.0
    # Try existing meta for base price
    meta = db["stocks_meta"].find_one({"merchant": merchant_name}) or {}
    if isinstance(meta.get("base_price"), (int,float)):
        base_price = float(meta["base_price"])
    prices = []
    tweets_docs = []
    reddit_docs = []
    news_docs = []
    reviews_docs = []
    wl_docs = []
    price = base_price
    ts_iter = start_dt
    idx = 0
    while ts_iter <= now_dt:
        ts_sec = ts_iter.timestamp()
        # Random walk for price
        drift = random.uniform(-volatility, volatility)
        price = max(1.0, price * (1 + drift))
        prices.append({"merchant": merchant_name, "ts": ts_sec, "price": round(price, 2)})
        # Sentiment signals
        sent_t = random.uniform(-1, 1)
        sent_r = sent_t + random.uniform(-0.2,0.2)
        sent_n = (sent_t + sent_r)/2 + random.uniform(-0.2,0.2)
        tweets_docs.append({"merchant": merchant_name, "ts": ts_sec, "sentiment": round(sent_t,3), "text": f"synthetic tweet {idx}"})
        reddit_docs.append({"merchant": merchant_name, "ts": ts_sec, "sentiment": round(sent_r,3), "title": f"synthetic reddit {idx}"})
        news_docs.append({"merchant": merchant_name, "ts": ts_sec, "sentiment": round(sent_n,3), "headline": f"synthetic news {idx}"})
        # Reviews every 3rd point
        if idx % 3 == 0:
            reviews_docs.append({"merchant": merchant_name, "ts": ts_sec, "rating": random.randint(2,5), "review": f"auto review {idx}"})
        # WL txn with flagged ratio ~10%
        status = "flagged" if (idx % 10 == 0) else "ok"
        wl_docs.append({"merchant": merchant_name, "ts": ts_sec, "status": status, "amount": round(random.uniform(10,500),2)})
        idx += 1
        ts_iter += dt.timedelta(minutes=step)
    # Bulk inserts (ignore duplicate errors if rerun)
    def safe_bulk(coll, docs):
        if not docs: return 0
        try:
            db[coll].insert_many(docs, ordered=False)
            return len(docs)
        except Exception:
            return 0
    inserted = {
        "tweets": safe_bulk("tweets", tweets_docs),
        "reddit": safe_bulk("reddit", reddit_docs),
        "news": safe_bulk("news", news_docs),
        "reviews": safe_bulk("reviews", reviews_docs),
        "wl_transactions": safe_bulk("wl_transactions", wl_docs),
        "stocks_prices": safe_bulk("stocks_prices", prices),
    }
    return {"seeded": inserted, "windows_potential": idx, "range": {"start": start_dt.isoformat().replace('+00:00','Z'), "end": now_dt.isoformat().replace('+00:00','Z')}}

@app.post("/v1/{merchant_name}/risk-eval/boost-seed")
def boost_seed_and_recompute(
    merchant_name: str,
    hours: int = 6,
    step: int = 10,
    volatility: float = 0.01,
    interval: str = "30m",
    force_recompute: bool = True,
    confirm: str = "yes"
):
    """Dense seeding helper to quickly populate recent windows with more granular data.
    Optionally wipes existing recent risk_scores for the given interval so recompute produces fresh scores.
    """
    if confirm.lower() not in ("yes","y","true","1"):
        raise HTTPException(status_code=400, detail="Set confirm=yes to proceed (safety guard)")
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    # Clamp inputs
    hours = max(1, min(hours, 24*3))  # up to 3 days for dense seed
    step = max(5, min(step, 60))      # 5..60 minutes
    now_dt = dt.datetime.now(dt.UTC)
    start_dt = now_dt - dt.timedelta(hours=hours)
    # Reuse seeding logic but with finer step
    try:
        _ = seed_risk_data(merchant_name=merchant_name, hours=hours, step=step, volatility=volatility, confirm="yes")
    except HTTPException as he:
        if he.status_code != 400:
            raise
    # Optional recompute: delete existing risk_scores overlapping this recent span
    if force_recompute:
        try:
            from risk_eval import INTERVAL_MAP  # local import to avoid top-cycle
            ilabel = interval.lower()
            if ilabel not in INTERVAL_MAP:
                raise HTTPException(status_code=400, detail="Unsupported interval for recompute")
            interval_minutes = INTERVAL_MAP[ilabel]
            deleted = db["risk_scores"].delete_many({
                "merchant": merchant_name,
                "interval_minutes": interval_minutes,
                "window_start_ts": {"$gte": start_dt.timestamp() - interval_minutes*60}
            }).deleted_count
        except Exception as e:
            log(f"boost-seed recompute delete error: {e}")
            deleted = 0
    else:
        deleted = 0
    # Trigger a capped recompute immediately (limit backfill to hours provided)
    cap_hours = hours + 2  # small cushion
    status = RISK.trigger_or_status(merchant_name, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=cap_hours)
    return {
        "status": "ok",
        "dense_seed_hours": hours,
        "step_minutes": step,
        "deleted_recent_scores": deleted,
        "triggered_job": status
    }

@app.get("/v1/{merchant_name}/risk-eval/stream-counts")
def risk_stream_counts(merchant_name: str):
    """Lightweight counts + earliest ts diagnostics to help debug empty risk scores.
    Returns document counts (capped) in each source collection and earliest timestamp.
    """
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    coll_map = {
        "tweets": "tweets",
        "reddit": "reddit",
        "news": "news",
        "reviews": "reviews",
        "wl": "wl_transactions",
        "prices": "stocks_prices"
    }
    out = {}
    earliest = None
    for label, cname in coll_map.items():
        try:
            cnt = db[cname].count_documents({"merchant": merchant_name})
            out[label] = cnt
            doc = db[cname].find_one({"merchant": merchant_name}, sort=[("ts", ASCENDING)], projection={"ts":1, "_id":0})
            if doc and isinstance(doc.get("ts"), (int,float)):
                t = float(doc["ts"])
                if earliest is None or t < earliest:
                    earliest = t
        except Exception as e:
            out[label] = f"error: {e}"  # surface issue
    if earliest is not None:
        earliest_iso = dt.datetime.fromtimestamp(earliest, tz=dt.UTC).isoformat().replace("+00:00","Z")
    else:
        earliest_iso = None
    return {"merchant": merchant_name, "counts": out, "earliest_ts": earliest, "earliest_iso": earliest_iso}

@app.get("/v1/{merchant_name}/risk-eval/diagnostics")
def risk_eval_diagnostics(merchant_name: str):
    """Deeper diagnostics for empty risk scores.
    For each underlying stream collection return:
      - total count
      - earliest/latest ts (+ iso)
      - sample field keys
      - inferred timestamp scale (seconds vs milliseconds)
      - notes about expected fields
    """
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    coll_specs = {
        "tweets": {"coll": "tweets", "expected": ["sentiment"], "purpose": "sentiment -1..1"},
        "reddit": {"coll": "reddit", "expected": ["sentiment"], "purpose": "sentiment -1..1"},
        "news": {"coll": "news", "expected": ["sentiment"], "purpose": "sentiment -1..1"},
        "reviews": {"coll": "reviews", "expected": ["rating"], "purpose": "rating 1..5"},
        "wl_transactions": {"coll": "wl_transactions", "expected": ["status"], "purpose": "fraud / flagged status"},
        "stocks_prices": {"coll": "stocks_prices", "expected": ["price","close"], "purpose": "price series for volatility"},
    }
    now_ts = dt.datetime.now(dt.UTC).timestamp()
    diag = {}
    for label, spec in coll_specs.items():
        cname = spec["coll"]
        c = db[cname]
        try:
            total = c.count_documents({"merchant": merchant_name})
            earliest_doc = c.find({"merchant": merchant_name}, sort=[("ts", ASCENDING)], projection={"_id":0}).limit(1)
            latest_doc = c.find({"merchant": merchant_name}, sort=[("ts", -1)], projection={"_id":0}).limit(1)
            earliest_data = next(iter(earliest_doc), None)
            latest_data = next(iter(latest_doc), None)
            if earliest_data and isinstance(earliest_data.get("ts"), (int,float)):
                e_ts = float(earliest_data["ts"])
            else:
                e_ts = None
            if latest_data and isinstance(latest_data.get("ts"), (int,float)):
                l_ts = float(latest_data["ts"])
            else:
                l_ts = None
            def iso(ts):
                if ts is None: return None
                try:
                    # Detect ms epoch ( > 1e12 ~ Nov 2001 threshold )
                    if ts > 1e12: # treat as milliseconds
                        ts_s = ts / 1000.0
                    else:
                        ts_s = ts
                    return dt.datetime.fromtimestamp(ts_s, tz=dt.UTC).isoformat().replace("+00:00","Z")
                except Exception:
                    return None
            # Infer scale
            scale = None
            if l_ts is not None:
                scale = "milliseconds" if l_ts > 1e12 else "seconds"
            notes = []
            # Expected field presence
            sample = latest_data or earliest_data or {}
            for field in spec["expected"]:
                if field not in sample:
                    notes.append(f"missing expected field '{field}'")
            if scale == "milliseconds":
                notes.append("timestamp appears to be in milliseconds; risk evaluator expects seconds -> windows will be empty")
            # Future / stale detection
            if l_ts is not None and scale == "seconds" and l_ts < now_ts - 3600*168:
                notes.append("latest data older than 7d; with backfill cap risk windows may not include it")
            if l_ts is not None and ((scale == "seconds" and l_ts > now_ts + 3600) or (scale == "milliseconds" and l_ts/1000.0 > now_ts + 3600)):
                notes.append("latest data appears in the future (>1h ahead)")
            diag[label] = {
                "count": total,
                "earliest_ts": e_ts,
                "earliest_iso": iso(e_ts) if e_ts is not None else None,
                "latest_ts": l_ts,
                "latest_iso": iso(l_ts) if l_ts is not None else None,
                "timestamp_scale": scale,
                "sample_keys": list(sample.keys())[:20],
                "expected_fields": spec["expected"],
                "purpose": spec["purpose"],
                "notes": notes
            }
        except Exception as e:
            diag[label] = {"error": str(e)}
    return {"merchant": merchant_name, "streams": diag, "now_iso": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')}

@app.get("/v1/{merchant_name}/risk-eval/probe")
def risk_eval_probe(
    merchant_name: str,
    interval: str = "30m",
    window: str | None = None,
    since: float | None = None,
    until: float | None = None,
    now: float | None = None,
    max_windows: int = 3
):
    """Deep probe of underlying raw stream presence for recent windows (no score mutation).
    Usage patterns:
      - /v1/acme/risk-eval/probe?interval=30m&window=6h  (probe last 6h aligned 30m windows)
      - /v1/acme/risk-eval/probe?interval=1h&since=...&until=...
    Returns per-window per-stream counts, query strategies used, and sample timestamps for debugging why risk components are None.
    """
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    from risk_eval import INTERVAL_MAP
    ilabel = interval.lower()
    if ilabel not in INTERVAL_MAP:
        raise HTTPException(status_code=400, detail="Unsupported interval")
    interval_minutes = INTERVAL_MAP[ilabel]
    try:
        rng_since = since
        rng_until = until
        if window:
            delta = parse_window_to_timedelta(window)
            base_now = dt.datetime.now(dt.UTC).timestamp() if now is None else float(now)
            rng_until = base_now if rng_until is None else rng_until
            rng_since = base_now - delta.total_seconds() if rng_since is None else rng_since
        if rng_since is None or rng_until is None:
            # default last 6h
            end_ts = dt.datetime.now(dt.UTC).timestamp()
            rng_until = rng_until or end_ts
            rng_since = rng_since or (end_ts - 6*3600)
        probe = RISK.probe_range(merchant_name, interval_minutes, rng_since, rng_until, max_windows=max_windows)
        return {"merchant": merchant_name, "interval": ilabel, "range": {"since": rng_since, "until": rng_until}, **probe}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # When running as a script, all routes (including risk eval) are now already registered above.
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    log(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

# ---- Merchant Evaluation Endpoints (moved ABOVE __main__ run for registration) ----
# NOTE: If this block appears twice in file, the earlier occurrence (before uvicorn.run)
# will be the active one; the later duplicate (if any) can be removed once verified.

@app.get("/v1/{merchant_name}/evaluations")
def get_merchant_evaluations(
    merchant_name: str,
    interval: str = "30m",
    limit: int = 500,
    since: float | None = None,
    until: float | None = None,
    ensure: bool = True,
    now: str | None = None,
):
    try:
        base_now_dt = parse_any_dt(now) if now else None
        now_ts = (base_now_dt or dt.datetime.now(dt.UTC)).timestamp()
        if since is None and until is None:
            since_calc = now_ts - 24*3600
            until_calc = now_ts
        else:
            since_calc = since if since is not None else (now_ts - 24*3600)
            until_calc = until if until is not None else now_ts
        if ensure:
            try:
                RISK.ensure_evaluations(merchant_name, interval, since_calc, until_calc)
            except Exception as e:
                log(f"ensure_evaluations error: {e}")
        rows = RISK.fetch_evaluations(merchant_name, interval_label=interval, limit=limit, since=since, until=until)
        return {"merchant": merchant_name, "interval": interval, "count": len(rows), "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/evaluations/summary")
def evaluations_summary(interval: str = "30m", limit_per: int = 5):
    try:
        coll = db["merchant_evaluations"]
        merchants = list(coll.distinct("merchant"))[:100]
        out = []
        for m in merchants:
            rows = list(coll.find({"merchant": m}, projection={"_id":0}).sort([("window_end_ts", -1)]).limit(limit_per))
            if rows:
                latest = rows[0]
                out.append({
                    "merchant": m,
                    "latest_total": (latest.get("scores") or {}).get("total"),
                    "latest_confidence": latest.get("confidence"),
                    "rows": rows,
                })
        return {"interval": interval, "merchants": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------- New Merchant Evaluation Endpoints (for MerchantRisk.vue) --------------

@app.get("/v1/{merchant_name}/evaluations")
def get_merchant_evaluations(
    merchant_name: str,
    interval: str = "30m",
    limit: int = 500,
    since: float | None = None,
    until: float | None = None,
    ensure: bool = True,
    now: str | None = None,
):
    """Return merchant evaluation timeline (merchant_evaluations collection).
    If ensure=true, compute any missing windows first (cheap idempotent upserts).
    """
    try:
        base_now_dt = parse_any_dt(now) if now else None
        now_ts = (base_now_dt or dt.datetime.now(dt.UTC)).timestamp()
        if since is None and until is None:
            # default lookback 24h for ensure
            since_calc = now_ts - 24*3600
            until_calc = now_ts
        else:
            since_calc = since if since is not None else (now_ts - 24*3600)
            until_calc = until if until is not None else now_ts
        if ensure:
            try:
                RISK.ensure_evaluations(merchant_name, interval, since_calc, until_calc)
            except Exception as e:
                log(f"ensure_evaluations error: {e}")
        rows = RISK.fetch_evaluations(merchant_name, interval_label=interval, limit=limit, since=since, until=until)
        return {"merchant": merchant_name, "interval": interval, "count": len(rows), "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/evaluations/summary")
def evaluations_summary(interval: str = "30m", limit_per: int = 5):
    """Lightweight multi-merchant snapshot for dashboards.
    Returns latest N evaluation windows per merchant (descending by end_ts).
    """
    try:
        coll = db["merchant_evaluations"]
        # Distinct merchants (could be large; cap to 100 for safety)
        merchants = list(coll.distinct("merchant"))[:100]
        out = []
        for m in merchants:
            rows = list(
                coll.find({"merchant": m}, projection={"_id":0})
                .sort([("window_end_ts", -1)])
                .limit(limit_per)
            )
            if rows:
                latest = rows[0]
                out.append({
                    "merchant": m,
                    "latest_total": ((latest.get("scores") or {}).get("total")),
                    "latest_confidence": latest.get("confidence"),
                    "rows": rows,
                })
        return {"interval": interval, "merchants": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))