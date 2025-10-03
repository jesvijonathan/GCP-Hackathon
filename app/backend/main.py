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

from fastapi import FastAPI, HTTPException, Request, Body
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
DASHBOARD_EXCLUDE = {x.strip() for x in os.getenv("DASHBOARD_EXCLUDE", "MER001").split(",") if x.strip()}

# --------------- Lightweight Response Cache (Streams) -----------------
# Simple in-memory LRU-ish cache for /v1/{merchant}/data responses.
# Purpose: absorb rapid repeat queries from UI (e.g., multiple watcher triggers) within a short horizon.
STREAM_RESPONSE_CACHE: dict[str, tuple[float, dict]] = {}
STREAM_RESPONSE_CACHE_MAX = 200  # max entries

def _stream_cache_get(key: str, ttl: float) -> Optional[dict]:
    ent = STREAM_RESPONSE_CACHE.get(key)
    if not ent:
        return None
    ts, payload = ent
    if (time.time() - ts) > ttl:
        # stale
        STREAM_RESPONSE_CACHE.pop(key, None)
        return None
    return payload

def _stream_cache_put(key: str, payload: dict):
    STREAM_RESPONSE_CACHE[key] = (time.time(), payload)
    # Trim if over size (remove oldest by timestamp)
    if len(STREAM_RESPONSE_CACHE) > STREAM_RESPONSE_CACHE_MAX:
        # sort by ts ascending and drop oldest 10% (at least 1)
        items = sorted(STREAM_RESPONSE_CACHE.items(), key=lambda kv: kv[1][0])
        drop = max(1, int(len(items) * 0.1))
        for k,_ in items[:drop]:
            STREAM_RESPONSE_CACHE.pop(k, None)

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

def resolve_sim_now(explicit_now: Optional[str]) -> tuple[dt.datetime, bool, Optional[str]]:
    """Resolve a simulated 'now' timestamp.
    Priority: explicit query parameter -> env var RISK_SIM_NOW -> real current UTC.
    Returns (datetime_utc, is_simulated, source_raw_string).
    """
    # 1. Explicit query param
    if explicit_now:
        dt_parsed = parse_any_dt(explicit_now)
        if dt_parsed:
            if dt_parsed.tzinfo is None:
                dt_parsed = dt_parsed.replace(tzinfo=dt.UTC)
            return dt_parsed.astimezone(dt.UTC), True, explicit_now
    # 2. Environment variable
    env_raw = os.getenv("RISK_SIM_NOW")
    if env_raw:
        dt_env = parse_any_dt(env_raw)
        if dt_env:
            if dt_env.tzinfo is None:
                dt_env = dt_env.replace(tzinfo=dt.UTC)
            return dt_env.astimezone(dt.UTC), True, env_raw
    # 3. Real now
    real = dt.datetime.now(dt.UTC)
    return real, False, None

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
from risk_eval import INTERVAL_MAP as RISK_INTERVAL_MAP

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
        # restriction history index already attempted above
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
    """Request body for /v1/onboard
    Fields limited to what is required to create (or regenerate) a merchant + preset.
    Flags & restrictions belong to the patch endpoint and are excluded here.
    """
    merchant_name: str = Field(..., description="Unique merchant identifier")
    deep_scan: bool = Field(False, description="Force regenerate preset even if exists")
    details: Optional[Dict[str, Any]] = Field(None, description="Optional merchant details metadata")
    preset_overrides: Optional[Dict[str, Any]] = Field(None, description="Optional overrides for generated preset block")
    start_date: Optional[str] = Field(None, description="ISO start date (default now-3y)")
    end_date: Optional[str] = Field(None, description="ISO end date (default now+1y)")
    seed: Optional[int] = Field(None, description="Optional RNG seed for reproducible generation")

class MerchantPatch(BaseModel):
    """PATCH body for /v1/merchants/{merchant_name}.
    All fields optional; path parameter supplies the merchant identity.
    (Older clients sending merchant_name or extra onboarding fields will be ignored gracefully.)
    """
    activation_flag: Optional[bool] = None
    auto_action: Optional[bool] = None
    restrictions: Optional[Dict[str, Any]] = None
    # Backward compatibility catch-all (ignored): allow arbitrary extra keys without 422
    class Config:
        extra = "allow"

# --- Single Restriction Record Model & Index ---
class MerchantRestrictionRecord(BaseModel):
    """Single current restriction record per merchant."""
    merchant_name: str
    updated_at: str
    restrictions: Dict[str, Any]

try:
    db["merchant_res"].create_index([("merchant_name", ASCENDING)], unique=True)
except Exception as _e:
    log(f"merchant_res single-record index error: {_e}")

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
    """Resolve the [since, until] range for stream queries.
    Adjustments:
      - If a simulated now is provided in the future while allow_future is False, clamp to real UTC now.
      - Gracefully recover from malformed window by falling back to 7d.
      - Ensure start <= end; if inverted, default to a 7d window ending at end.
    """
    real_now = dt.datetime.now(dt.UTC)
    candidate_now = parse_any_dt(now_iso) or real_now
    if (not allow_future) and candidate_now > real_now:
        candidate_now = real_now
    now_dt = candidate_now
    if window and window.strip():
        try:
            delta = parse_window_to_timedelta(window.strip())
        except Exception:
            delta = dt.timedelta(days=7)
        s = now_dt - delta
        u = now_dt
    else:
        s = parse_any_dt(since) or (now_dt - dt.timedelta(days=7))
        u = parse_any_dt(until) or now_dt
    if not allow_future and u > real_now:
        u = real_now
    if s > u:
        s = u - dt.timedelta(days=7)
    s_ts = s.timestamp()
    u_ts = u.timestamp()
    return (
        s_ts,
        u_ts,
        s.astimezone(dt.UTC).isoformat().replace("+00:00","Z"),
        u.astimezone(dt.UTC).isoformat().replace("+00:00","Z"),
    )

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
    names = SERVICE.list_merchant_names()
    if DASHBOARD_EXCLUDE:
        names = [n for n in names if n not in DASHBOARD_EXCLUDE]
    return {"merchants": names}

@app.get("/v1/merchants/{merchant_name}")
def get_merchant(merchant_name: str):
    doc = SERVICE.get_merchant(merchant_name)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    return {"merchant": doc}

@app.patch("/v1/merchants/{merchant_name}")
def patch_merchant_flags(
    merchant_name: str,
    activation_flag: Optional[bool] = None,
    auto_action: Optional[bool] = None,
    payload: MerchantPatch | None = Body(None)
):
    """Patch merchant operational flags and restrictions.
    Request patterns supported:
      1) Query params only: /v1/merchants/foo?activation_flag=false
      2) JSON body with any subset: {"activation_flag": true, "auto_action": false}
      3) JSON body with restrictions: {"restrictions": {...}}
      4) Legacy / accidental wrapper: {"payload": {"restrictions": {...}}}
    Any unknown extra keys are ignored (Config.extra = allow) to avoid 422s when frontend evolves.
    """
    # Support clients accidentally sending a raw dict (FastAPI tried to coerce but gave 422 earlier)
    if payload is None:
        # Attempt to read from request body already parsed by dependency (FastAPI passes None if empty)
        # Nothing further we can do here without direct Request object; continue.
        pass
    doc = SERVICE.get_merchant(merchant_name)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    updates: Dict[str, Any] = {}
    if activation_flag is not None:
        updates["activation_flag"] = bool(activation_flag)
    elif payload and payload.activation_flag is not None:
        updates["activation_flag"] = bool(payload.activation_flag)
    if auto_action is not None:
        updates["auto_action"] = bool(auto_action)
    elif payload and payload.auto_action is not None:
        updates["auto_action"] = bool(payload.auto_action)
    if payload and payload.restrictions is not None:
        if not isinstance(payload.restrictions, dict):
            raise HTTPException(status_code=400, detail="restrictions must be an object")
        updates["restrictions"] = payload.restrictions
    if not updates:
        return {"merchant": doc, "updated": False}
    updates["updated_at"] = dt_to_iso(dt.datetime.now(dt.UTC))
    try:
        db["merchants"].update_one({"merchant_name": merchant_name}, {"$set": updates})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {e}")
    new_doc = SERVICE.get_merchant(merchant_name)
    # Upsert single restriction record if changed
    restriction_record = None
    if "restrictions" in updates:
        try:
            restr = updates.get("restrictions") or {}
            if not restr:
                # Clear both merchant doc field and single record
                try:
                    db["merchants"].update_one({"merchant_name": merchant_name}, {"$unset": {"restrictions": ""}})
                except Exception:
                    pass
                db["merchant_res"].delete_one({"merchant_name": merchant_name})
            else:
                restriction_record = {
                    "merchant_name": merchant_name,
                    "updated_at": dt_to_iso(dt.datetime.now(dt.UTC)),
                    "restrictions": restr,
                }
                db["merchant_res"].update_one(
                    {"merchant_name": merchant_name},
                    {"$set": restriction_record},
                    upsert=True,
                )
        except Exception as e:
            log(f"restriction upsert failed [{merchant_name}]: {e}")
    return {"merchant": new_doc, "updated": True, "restriction_record": restriction_record}

@app.delete("/v1/merchants/{merchant_name}")
def delete_merchant(merchant_name: str, purge: bool = False):
    """Delete a merchant from dashboard context while retaining raw source data.

    Removes documents from collections directly tied to dashboard / risk evaluation:
      - merchants (merchant base profile)
      - preset (synthetic preset block)
      - merchant_evaluations (latest eval summaries)
      - risk_scores (historic evaluation windows)
      - merchant_res (single restriction record)

    Does NOT delete data streams (tweets/news/reddit/reviews/wl/stock) so historical
    analytics can be reconstructed if the merchant is re-onboarded.

    If query param purge=true is passed, also remove restriction + evaluation jobs
    artifacts (best-effort) but still leave streams untouched.
    """
    doc = SERVICE.get_merchant(merchant_name)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    removed = {}
    try:
        r_merchants = db["merchants"].delete_one({"merchant_name": merchant_name})
        removed["merchants"] = r_merchants.deleted_count
        r_preset = db["preset"].delete_one({"merchant_name": merchant_name})
        removed["preset"] = r_preset.deleted_count
        r_eval = db["merchant_evaluations"].delete_many({"merchant": merchant_name})
        removed["merchant_evaluations"] = r_eval.deleted_count
        r_scores = db["risk_scores"].delete_many({"merchant": merchant_name})
        removed["risk_scores"] = r_scores.deleted_count
        r_res = db["merchant_res"].delete_one({"merchant_name": merchant_name})
        removed["merchant_res"] = r_res.deleted_count
        if purge:
            # Attempt to remove any queued jobs referencing this merchant (best-effort: implementation may vary)
            try:
                from risk_eval import RISK
                RISK.drop_jobs_for_merchant(merchant_name)  # if method exists; ignore otherwise
                removed["jobs"] = "requested"
            except Exception:
                pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {e}")
    return {"status": "deleted", "merchant": merchant_name, "removed": removed}

@app.get("/v1/merchants/{merchant_name}/restrictions")
def get_current_restrictions(merchant_name: str):
    mdoc = SERVICE.get_merchant(merchant_name)
    if not mdoc:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    rec = db["merchant_res"].find_one({"merchant_name": merchant_name}, projection={"_id":0})
    return {"merchant": merchant_name, "restrictions": (rec or {}).get("restrictions")}

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
    now: Optional[str] = None,
    parallel: bool = True,
    cache_ttl: float = 2.0,
    profile: bool = False,
):
    # Validate merchant
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")

    # Simulated time resolution (reuse risk evaluation semantics)
    sim_dt, is_sim, sim_source = resolve_sim_now(now)
    # Compute range relative to simulated or real now
    try:
        s_ts, u_ts, s_iso, u_iso = compute_range_params(window, since, until, allow_future, sim_dt.isoformat())
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid range: {e}")

    # Normalize streams
    stream_list = normalize_streams_arg(streams)

    # Fetch data per stream (deferred until after cache check)
    data: Dict[str, Any] = {}
    range_info: Dict[str, Dict[str, str]] = {}
    limits: Dict[str, Any] = {}

    # Cache lookup key (exclude profile flag to allow reuse; include range + params that affect dataset)
    cache_key = f"{merchant_name}|{sorted(stream_list)}|{order}|{int(limit)}|{int(s_ts)}|{int(u_ts)}|{int(include_stock_meta)}"
    if cache_ttl > 0:
        cached = _stream_cache_get(cache_key, cache_ttl)
        if cached is not None:
            if profile:
                cached = {**cached, "cache_hit": True}
            return cached

    data: Dict[str, Any] = {}
    range_info: Dict[str, Dict[str, str]] = {}
    limits: Dict[str, Any] = {}
    timings: Dict[str, float] = {}

    def _record(name: str, start: float):
        if profile:
            timings[name] = round((time.time() - start) * 1000.0, 2)

    # Worker function map
    def fetch_simple(label: str, coll_key: str):
        t0 = time.time()
        try:
            data[label] = query_stream(STREAM_COLLECTION_MAP[coll_key], merchant_name, s_ts, u_ts, order, limit)
            range_info[label] = {"since": s_iso, "until": u_iso}
        finally:
            _record(label, t0)

    def fetch_stock_wrap():
        t0 = time.time()
        try:
            data["stock"] = fetch_stock(merchant_name, s_ts, u_ts, order, limit, include_meta=include_stock_meta)
            range_info["stock"] = {"since": s_iso, "until": u_iso}
        finally:
            _record("stock", t0)

    # Decide execution strategy
    simple_map = {
        "tweets": (fetch_simple, "tweets"),
        "reddit": (fetch_simple, "reddit"),
        "news": (fetch_simple, "news"),
        "reviews": (fetch_simple, "reviews"),
        "wl": (fetch_simple, "wl"),
    }

    tasks: list[Future] = []
    if parallel and len(stream_list) > 1:
        # Launch parallel queries
        for st in stream_list:
            if st == "stock":
                tasks.append(TASKS.pool.submit(fetch_stock_wrap))
            else:
                fn, key = simple_map.get(st, (None, None))
                if fn:
                    tasks.append(TASKS.pool.submit(fn, st, key))
        # Wait for completion
        for fut in tasks:
            try:
                fut.result()
            except Exception as e:
                log(f"streams parallel fetch error: {e}")
    else:
        # Serial fallback
        for st in stream_list:
            if st == "stock":
                fetch_stock_wrap()
            else:
                fn, key = simple_map.get(st, (None, None))
                if fn:
                    fn(st, key)

    resp = {
        "merchant": merchant_name,
        "order": order,
        "range": range_info,
        "limits": limits,
        "data": data,
        "simulated": is_sim,
        "sim_now_ts": sim_dt.timestamp(),
        "sim_source": sim_source,
        "since_ts": s_ts,
        "until_ts": u_ts,
    }
    if profile:
        resp["timings_ms"] = timings
        resp["parallel"] = parallel and len(stream_list) > 1
    if cache_ttl > 0:
        _stream_cache_put(cache_key, resp)
    return resp
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

# ---------- Dashboard overview (top merchants by latest evaluation) ----------

@app.get("/v1/dashboard/overview")
def dashboard_overview(
    interval: str = "30m",
    top: int = 5,
    include_all: bool = True,
    include_inactive: bool = False,
    now: str | None = None,
):
    """Return latest evaluation per merchant and the top-N by total risk score.
    interval: window label (30m|1h|1d)
    top: number of top merchants to include
    include_all: include the full merchant list with their latest evaluation
    """
    interval_key = interval.lower()
    if interval_key not in RISK_INTERVAL_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported interval '{interval}'.")
    interval_minutes = RISK_INTERVAL_MAP[interval_key]
    # Resolve simulated now (query param -> env -> real now)
    sim_now_dt, is_sim, sim_source = resolve_sim_now(now)
    gen_now_ts = sim_now_dt.timestamp()
    eval_col = db["merchant_evaluations"]
    # Aggregate latest evaluation per merchant for the interval, respecting simulated now if provided
    match_filter: dict[str, Any] = {"interval_minutes": interval_minutes}
    if is_sim:
        match_filter["window_end_ts"] = {"$lte": gen_now_ts}
    pipeline = [
        {"$match": match_filter},
        {"$sort": {"window_end_ts": -1}},
        {"$group": {"_id": "$merchant", "doc": {"$first": "$$ROOT"}}},
        {"$project": {"_id": 0, "merchant": "$_id", "evaluation": "$doc", "risk_score": "$doc.scores.total", "confidence": "$doc.confidence"}},
    ]
    try:
        latest = list(eval_col.aggregate(pipeline))
    except Exception as e:
        log(f"dashboard_overview aggregation error: {e}")
        raise HTTPException(status_code=500, detail="Failed to aggregate evaluations")
    # If simulated rewind and missing recent windows, optionally trigger lightweight ensure (capped)
    if not latest:
        try:
            cap_hours = 168
            for mdoc in db["merchants"].find({}, projection={"merchant_name":1, "_id":0}):
                mname = mdoc.get("merchant_name")
                if not mname or mname in DASHBOARD_EXCLUDE:
                    continue
                # Only ensure a very small range ending at sim_now to avoid heavy backfill
                until_ts = gen_now_ts
                since_ts = until_ts - interval_minutes * 60 * 6  # last 6 windows
                try:
                    RISK.ensure_windows(mname, interval_key, since_ts, until_ts)
                except Exception:
                    pass
            latest = list(eval_col.aggregate(pipeline))
        except Exception:
            pass
    # Fetch merchant base docs for enrichment
    merchant_docs = {m.get("merchant_name"): m for m in db["merchants"].find({}, projection={"_id": 0})}
    def _coerce_bool(v, default=True):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"false", "0", "no", "off", "inactive"}:
                return False
            if s in {"true", "1", "yes", "on", "active"}:
                return True
            # Fallback: non-empty string -> True
            return True
        return bool(v)

    out_all = []
    inactive_count = 0
    for row in latest:
        mname = row["merchant"]
        if mname in DASHBOARD_EXCLUDE:
            continue
        details = merchant_docs.get(mname) or {}
        # Normalized shape for frontend
        eval_doc = row.get("evaluation") or {}
        scores = (eval_doc.get("scores") or {})
        activation_flag = _coerce_bool(details.get("activation_flag", True), default=True)
        auto_action = _coerce_bool(details.get("auto_action", False), default=False)
        if not activation_flag:
            inactive_count += 1
            if not include_inactive:
                continue  # fully skip inactive merchant from overview payload
        restrictions = details.get("restrictions") if isinstance(details.get("restrictions"), dict) else None
        out_all.append({
            "merchant": mname,
            "risk_score": scores.get("total"),
            "confidence": row.get("confidence"),
            "window_end": eval_doc.get("window_end"),
            "scores": scores,
            "counts": eval_doc.get("counts"),
            "drivers": eval_doc.get("drivers"),
            "activation_flag": activation_flag,
            "auto_action": auto_action,
            "has_restrictions": bool(restrictions),
            "restrictions": restrictions,
            "details": {
                k: details.get(k) for k in [
                    "merchant_name", "status", "category", "country", "created_at", "updated_at"
                ] if k in details
            }
        })
    # Compute top merchants (exclude None scores then sort desc)
    # Exclude inactive merchants from ranking/analytics
    scored = [r for r in out_all if isinstance(r.get("risk_score"), (int, float)) and r.get("activation_flag", True)]
    scored.sort(key=lambda x: x.get("risk_score"), reverse=True)
    top_list = scored[: max(1, min(int(top or 5), 50))]
    # Filter future windows relative to simulated now by post-processing (pipeline can't reuse local var)
    resp = {
        "generated_at": dt.datetime.fromtimestamp(gen_now_ts, tz=dt.UTC).isoformat().replace("+00:00", "Z"),
        "interval": interval_key,
        "sim_now_ts": gen_now_ts,
        "simulated": is_sim,
        "sim_source": sim_source,
        "top_merchants": top_list,
        "inactive_count": inactive_count if not include_inactive else sum(1 for r in out_all if not r.get("activation_flag", True)),
    }
    if include_all:
        resp["all_merchants"] = out_all
    # If we have merchants with no evaluation yet, list them with null risk_score
    if include_all:
        existing_names = {r["merchant"] for r in out_all}
        all_names = set(n for n in SERVICE.list_merchant_names() if n not in DASHBOARD_EXCLUDE)
        missing = sorted(all_names - existing_names)
        for mname in missing:
            md = merchant_docs.get(mname, {})
            m_active = _coerce_bool(md.get("activation_flag", True), default=True)
            if (not m_active) and (not include_inactive):
                inactive_count += 1
                continue
            resp.setdefault("all_merchants", []).append({
                "merchant": mname,
                "risk_score": None,
                "confidence": None,
                "window_end": None,
                "scores": {},
                "counts": {},
                "drivers": [],
                "activation_flag": m_active,
                "auto_action": _coerce_bool(md.get("auto_action", False), default=False),
                "details": {}
            })
    if DASHBOARD_EXCLUDE:
        resp["excluded"] = sorted(DASHBOARD_EXCLUDE)
    return resp

@app.get("/v1/dashboard/series")
def dashboard_series(
    interval: str = "30m",
    top: int = 5,
    lookback: int = 48,
    include_components: bool = True,
    ensure: bool = True,
    async_ensure: bool = True,
    include_axis: bool = True,
    now: str | None = None,
):
    """Return time-series evaluation windows for top-N merchants.
    Params:
      interval: evaluation window label
      top: number of merchants to include (based on latest risk score)
      lookback: number of recent windows per merchant (max 500)
      include_components: include per-window component scores & counts
    """
    interval_key = interval.lower()
    if interval_key not in RISK_INTERVAL_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported interval '{interval}'.")
    interval_minutes = RISK_INTERVAL_MAP[interval_key]
    lookback = max(5, min(int(lookback or 48), 500))
    eval_col = db["merchant_evaluations"]
    # Latest evaluation per merchant (respect simulated now if provided)
    match_filter: dict[str, Any] = {"interval_minutes": interval_minutes}
    sim_now_dt, is_sim, sim_source = resolve_sim_now(now)
    if is_sim:
        match_filter["window_end_ts"] = {"$lte": sim_now_dt.timestamp()}
    pipeline = [
        {"$match": match_filter},
        {"$sort": {"window_end_ts": -1}},
        {"$group": {"_id": "$merchant", "doc": {"$first": "$$ROOT"}}},
        {"$project": {"_id": 0, "merchant": "$_id", "risk_score": "$doc.scores.total", "confidence": "$doc.confidence", "window_end_ts": "$doc.window_end_ts"}},
    ]
    try:
        latest = list(eval_col.aggregate(pipeline))
    except Exception as e:
        log(f"dashboard_series aggregation error: {e}")
        raise HTTPException(status_code=500, detail="Aggregation failed")
    # Filter exclusions & sort
    latest = [r for r in latest if r.get("merchant") not in DASHBOARD_EXCLUDE]
    # Filter out inactive merchants (need merchant docs)
    merchant_docs_raw = {m.get("merchant_name"): m for m in db["merchants"].find({}, projection={"_id":0, "merchant_name":1, "activation_flag":1, "auto_action":1})}
    def _coerce_bool(v, default=True):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        if isinstance(v, (int,float)):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"false","0","no","off","inactive"}: return False
            if s in {"true","1","yes","on","active"}: return True
            return True
        return bool(v)
    merchant_docs = {k: {**v, "activation_flag": _coerce_bool(v.get("activation_flag", True), True), "auto_action": _coerce_bool(v.get("auto_action", False), False)} for k, v in merchant_docs_raw.items()}
    latest = [r for r in latest if merchant_docs.get(r.get("merchant"), {}).get("activation_flag", True)]
    latest_sorted = [r for r in latest if isinstance(r.get("risk_score"), (int, float))]
    latest_sorted.sort(key=lambda x: x.get("risk_score"), reverse=True)
    selected_merchants = [r["merchant"] for r in latest_sorted[: max(1, min(int(top or 5), 50))]]
    # Fallback: if no merchants have any evaluations yet, pick from merchant registry directly
    if not selected_merchants:
        try:
            candidate = [m for m in SERVICE.list_merchant_names() if m not in DASHBOARD_EXCLUDE]
            # filter out inactive from fallback list
            candidate = [m for m in candidate if merchant_docs.get(m, {}).get("activation_flag", True)]
            candidate.sort()  # deterministic subset
            selected_merchants = candidate[: max(1, min(int(top or 5), 50))]
        except Exception as e:
            log(f"dashboard_series fallback merchant list error: {e}")
    # Fallback if not enough merchants with scores
    if len(selected_merchants) < top:
        extra = [r["merchant"] for r in latest if r["merchant"] not in selected_merchants]
        for m in extra:
            if len(selected_merchants) >= top:
                break
            selected_merchants.append(m)
    # Additional fallback: pull from full merchant registry (even if they have no evaluations yet)
    if len(selected_merchants) < top:
        try:
            registry_names = [m for m in SERVICE.list_merchant_names() if m not in DASHBOARD_EXCLUDE]
            # filter inactive
            registry_names = [m for m in registry_names if merchant_docs.get(m, {}).get("activation_flag", True)]
            for m in registry_names:
                if len(selected_merchants) >= top:
                    break
                if m not in selected_merchants:
                    selected_merchants.append(m)
        except Exception as e:
            log(f"dashboard_series registry fallback error: {e}")
    # Reuse parsed simulated now (if any) else real now
    now_ts = sim_now_dt.timestamp()
    interval_seconds = interval_minutes * 60
    since_ts = now_ts - lookback * interval_seconds
    # Ensure evaluations if requested (optionally async via job system for progress reporting)
    jobs_started = []
    # Auto-force ensure if caller disabled ensure but we have zero scored evaluations (cold start)
    force_ensure = False
    if not ensure and not latest_sorted and selected_merchants:
        force_ensure = True
        ensure = True
        log(f"dashboard_series: forcing ensure (cold start) interval={interval_key} merchants={len(selected_merchants)}")
    if ensure:
        # Adaptive backfill cap: widen if simulated now is far in the past relative to real now
        real_now_ts = dt.datetime.now(dt.UTC).timestamp()
        # Distance (hours) from real now
        delta_hours = (real_now_ts - now_ts) / 3600.0
        # Minimum horizon to cover lookback fully
        needed_hours = max(lookback * interval_minutes / 60.0, 1)
        base_cap = 168  # default 7d
        # If simulating past beyond base_cap or lookback exceeds base, expand
        adaptive_cap = max(base_cap, needed_hours)
        if delta_hours > base_cap:
            # expand up to distance (capped) to allow rewind planning
            adaptive_cap = min(delta_hours + interval_minutes/60.0, 24*90)  # cap 90d
        adaptive_cap = int(adaptive_cap + 0.999)
        if async_ensure:
            for m in selected_merchants:
                try:
                    status = RISK.trigger_or_status(m, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=adaptive_cap, now_ts=now_ts)
                    planned = status.get("planned") or status.get("missing_planned") or 0
                    processed = status.get("processed") or 0
                    if planned:
                        status["percent"] = round(min(100.0, processed / planned * 100.0), 2)
                    status["adaptive_backfill_hours"] = adaptive_cap
                    jobs_started.append(status)
                except Exception as e:
                    log(f"dashboard_series async trigger error [{m}]: {e}")
        else:
            for m in selected_merchants:
                try:
                    RISK.ensure_evaluations(m, interval, since_ts, now_ts)
                except Exception as e:
                    log(f"dashboard_series ensure_evaluations error [{m}]: {e}")
    # Fetch series per merchant
    merchants_series = []
    for m in selected_merchants:
        cur = eval_col.find(
            {"merchant": m, "interval_minutes": interval_minutes},
            projection={"_id": 0},
            sort=[("window_end_ts", -1)],
            limit=lookback,
        )
        rows = list(cur)
        # Drop any windows that end after simulated now (if sim time is in the past)
        if rows and now_ts:
            rows = [r for r in rows if (r.get("window_end_ts") or 0) <= now_ts + 1e-6]
        rows.sort(key=lambda x: x.get("window_end_ts", 0))  # ascending for charting
        if not include_components:
            slim = []
            for r in rows:
                slim.append({
                    "window_end": r.get("window_end"),
                    "window_end_ts": r.get("window_end_ts"),
                    "scores": {"total": (r.get("scores", {}) or {}).get("total")},
                    "counts": r.get("counts", {}),
                })
            rows = slim
        merchants_series.append({
            "merchant": m,
            "series": rows,
            "activation_flag": merchant_docs.get(m, {}).get("activation_flag", True),
            "auto_action": merchant_docs.get(m, {}).get("auto_action", False)
        })
    # Axis baseline metadata (global earliest baseline among selected merchants)
    axis_baseline_ts = None
    if include_axis:
        try:
            base_docs = list(db["merchants"].find({"merchant_name": {"$in": selected_merchants}}, projection={"_id":0, "merchant_name":1, "risk_eval_baseline_ts":1}))
            for md in base_docs:
                bts = md.get("risk_eval_baseline_ts")
                if isinstance(bts, (int,float)):
                    if axis_baseline_ts is None or bts < axis_baseline_ts:
                        axis_baseline_ts = float(bts)
        except Exception:
            axis_baseline_ts = None
    resp = {
        "generated_at": dt.datetime.fromtimestamp(now_ts, tz=dt.UTC).isoformat().replace("+00:00", "Z"),
        "interval": interval_key,
        "sim_now_ts": now_ts,
        "simulated": is_sim,
        "sim_source": sim_source,
        "top": len(selected_merchants),
        "lookback": lookback,
        "since_ts": since_ts,
        "until_ts": now_ts,
        "merchants": merchants_series,
    }
    if include_axis:
        resp["axis"] = {"baseline_ts": axis_baseline_ts, "until_ts": now_ts, "interval_seconds": interval_seconds}
    if jobs_started:
        # Only include active/queued/running jobs to limit payload size
        resp["jobs"] = [j for j in jobs_started if j.get("status") in {"queued","running"} or j.get("missing_planned")]
    if force_ensure:
        resp["ensure_forced"] = True
    return resp

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
    until: float | None = None,
    priority: int = 5,
    realtime: bool | None = None,
    now: str | None = None,
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
    # Parse 'now' which may be ISO8601, epoch seconds, or milliseconds
    parsed_now_ts: float | None = None
    parsed_now_source: str | None = None
    if now is not None:
        # Try datetime parse first
        try:
            ndt = parse_any_dt(now)
            if ndt:
                parsed_now_ts = ndt.timestamp()
                parsed_now_source = "datetime"
        except Exception:
            ndt = None
        if parsed_now_ts is None:
            # Try numeric (seconds or ms)
            try:
                f = float(now)
                # Heuristic: treat large values as ms
                if f > 10_000_000_000:  # > year 2286 if seconds, so probably ms
                    f = f / 1000.0
                parsed_now_ts = f
                parsed_now_source = parsed_now_source or "epoch"
            except Exception:
                parsed_now_ts = None
                parsed_now_source = "invalid"
    try:
        cap = _parse_backfill_param(max_backfill_hours)
        status = RISK.trigger_or_status(merchant_name, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=cap, priority=priority, realtime=realtime, now_ts=parsed_now_ts)
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
                status = RISK.trigger_or_status(merchant_name, interval, submit_fn=TASKS.pool.submit, max_backfill_hours=retry_cap, realtime=realtime, now_ts=parsed_now_ts)
                actions.append({"action":"bootstrap_retry","windows": status.get("total_windows")})
            except Exception as be:
                actions.append({"action":"bootstrap_seed","error": str(be)})
        return {"job": status, "actions": actions, "autoseed_result": autoseed_result, "parsed_now_ts": parsed_now_ts, "parsed_now_source": parsed_now_source}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Convenience GET alias so users can paste the URL in a browser and still trigger the job.
@app.get("/v1/{merchant_name}/risk-eval/trigger")
def trigger_risk_eval_get(merchant_name: str, interval: str = "30m", autoseed: bool = False, max_backfill_hours: str | None = None, priority: int = 5, realtime: bool | None = None, now: str | None = None):
    return trigger_risk_eval(merchant_name=merchant_name, interval=interval, autoseed=autoseed, max_backfill_hours=max_backfill_hours, priority=priority, realtime=realtime, now=now)

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
    fake: bool = False,
    include_axis: bool = True
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

        axis_meta = None
        if include_axis:
            try:
                mdoc = db["merchants"].find_one({"merchant_name": merchant_name}, projection={"_id":0, "risk_eval_baseline_ts":1})
                bts = mdoc.get("risk_eval_baseline_ts") if mdoc else None
                if isinstance(bts, (int,float)):
                    axis_meta = {"baseline_ts": float(bts), "until_ts": rng_until or dt.datetime.now(dt.UTC).timestamp()}
            except Exception:
                axis_meta = None
        resp = {"merchant": merchant_name, "interval_selected": chosen_interval, **summary, "range_used": {"since": rng_since, "until": rng_until}}
        if axis_meta:
            resp["axis"] = axis_meta
        return resp
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

@app.get("/v1/risk-eval/jobs")
def list_risk_jobs(active: bool = True):
    """Return risk evaluation jobs (persistent). active=true => queued/running only."""
    jobs = RISK.list_jobs(active=active)
    return {"jobs": jobs, "count": len(jobs)}

@app.get("/v1/{merchant_name}/risk-eval/latest")
def latest_risk_snapshot(merchant_name: str, interval: str = "30m", now: float | None = None):
    """Return (and if necessary compute) the most recent completed window quickly.
    Parameters:
      - interval: window size label
      - now: optional simulated 'now' epoch seconds for replay; if provided the window
        aligned to (now - interval) is ensured instead of real current time.
    """
    if not SERVICE.get_merchant(merchant_name):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant_name}")
    try:
        if now is not None:
            snap = RISK.ensure_latest_window_at(merchant_name, interval, float(now))
        else:
            snap = RISK.ensure_latest_window(merchant_name, interval)
        return {"snapshot": snap}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
    sim_filter_future: bool = True,
    asyncEnsure: bool = True,
    maxEnsureSeconds: float = 4.0,
):
    try:
        log(f"[eval-endpoint] merchant={merchant_name} interval={interval} since={since} until={until} ensure={ensure} now={now}")
        sim_dt, is_sim, sim_src = resolve_sim_now(now)
        now_ts = sim_dt.timestamp()
        # Derive ensure range (since/until provided as epoch seconds already)
        if since is None and until is None:
            # default 24h lookback for merchant view
            since_calc = now_ts - 24*3600
            until_calc = now_ts
        else:
            since_calc = since if since is not None else (now_ts - 24*3600)
            until_calc = until if until is not None else now_ts
        # Clamp if simulated time is in the FUTURE (previous logic only handled rewinds)
        real_now_ts = dt.datetime.now(dt.UTC).timestamp()
        if is_sim:
            if now_ts < real_now_ts:
                until_calc = min(until_calc, now_ts)
            elif now_ts > real_now_ts:
                # Disallow large forward simulation for this endpoint (avoid hanging on empty future)
                forward_ahead = now_ts - real_now_ts
                # Allow up to 5 minutes forward; clamp anything beyond
                if forward_ahead > 300:
                    until_calc = real_now_ts
                    now_ts = real_now_ts
                    log(f"[eval-endpoint] clamped future simulated now (ahead {forward_ahead:.1f}s) -> real now")
        # Safety: never let until exceed real now by > 60s (stray clock drift)
        if until_calc > real_now_ts + 60:
            until_calc = real_now_ts
        # Ensure since <= until after clamps
        if since_calc > until_calc:
            since_calc = max(0.0, until_calc - 24*3600)
        # Expand backfill horizon automatically if rewinding far in the past (similar logic to dashboard_series)
        backfill_started = False
        backfill_duration_ms = None
        missing_planned = 0
        if ensure:
            try:
                ilabel = interval.lower()
                from risk_eval import INTERVAL_MAP
                if ilabel in INTERVAL_MAP:
                    iv = INTERVAL_MAP[ilabel]
                    # Plan first (cheap) to avoid blocking if a large gap
                    try:
                        missing_list = RISK.plan_missing_windows_for_range(merchant_name, iv, since_calc, until_calc)
                        missing_planned = len(missing_list)
                    except Exception:
                        missing_list = []
                    if missing_list:
                        if asyncEnsure:
                            # Launch background thread so HTTP request does not hang
                            def _bg():
                                try:
                                    RISK.ensure_windows(merchant_name, ilabel, since_calc, until_calc)
                                except Exception as _e:
                                    log(f"[eval-endpoint] background ensure error: {_e}")
                            import threading
                            threading.Thread(target=_bg, name=f"eval-ensure-{merchant_name}-{ilabel}", daemon=True).start()
                            backfill_started = True
                        else:
                            t0 = time.time()
                            RISK.ensure_windows(merchant_name, ilabel, since_calc, until_calc)
                            backfill_duration_ms = int((time.time()-t0)*1000)
                    else:
                        # Nothing missing; no-op
                        pass
            except Exception as e:
                log(f"ensure_evaluations (non-fatal) error: {e}")
        # Fetch (respect original since/until filters if provided so UI table matches query)
        rows = RISK.fetch_evaluations(merchant_name, interval_label=interval, limit=limit, since=since, until=until)
        log(f"[eval-endpoint] fetched rows={len(rows)} (pre-fallback)")
        # Fallback: if no evaluations but we are simulating and ensured windows should exist, widen a bit & retry once
        if not rows and ensure and is_sim:
            try:
                from risk_eval import INTERVAL_MAP
                iv = INTERVAL_MAP.get(interval.lower())
                if iv:
                    widen = iv * 3 * 60  # widen ensure by 3 extra windows backward
                    back_since = max(0.0, since_calc - widen)
                    RISK.ensure_evaluations(merchant_name, interval, back_since, until_calc)
                    rows = RISK.fetch_evaluations(merchant_name, interval_label=interval, limit=limit, since=since, until=until)
                    log(f"[eval-endpoint] post-fallback rows={len(rows)} widened_since={back_since}")
            except Exception as fe:
                log(f"merchant_evaluations fallback ensure failed: {fe}")
        if is_sim and sim_filter_future:
            rows = [r for r in rows if (r.get('window_end_ts') or 0) <= now_ts + 1e-6]
        log(f"[eval-endpoint] returning rows={len(rows)} simulated={is_sim}")
        return {
            "merchant": merchant_name,
            "interval": interval,
            "count": len(rows),
            "rows": rows,
            "simulated": is_sim,
            "sim_now_ts": now_ts,
            "sim_source": sim_src,
            "backfill_async": backfill_started,
            "missing_planned": missing_planned,
            "backfill_duration_ms": backfill_duration_ms,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/risk-eval/queue-summary")
def risk_eval_queue_summary():
    """Aggregate queue stats for frontend (active jobs ordered by priority then FIFO)."""
    try:
        jobs_active = RISK.list_jobs(active=True)
        # Order: priority asc (None -> large), created_at_ts asc
        def _prio(j):
            p = j.get("priority")
            if not isinstance(p, (int,float)):
                return 999
            return p
        jobs_active.sort(key=lambda j: (_prio(j), j.get("created_at_ts", 0)))
        total_active = len(jobs_active)
        # Derive position map per merchant+interval (first occurrence)
        positions = {}
        for idx, j in enumerate(jobs_active, start=1):
            key = f"{j.get('merchant')}:{j.get('interval')}"
            if key not in positions:
                positions[key] = idx
        summary = {
            "active_jobs": jobs_active,
            "total_active": total_active,
            "positions": positions,
            "generated_at": dt.datetime.now(dt.UTC).isoformat().replace('+00:00','Z')
        }
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"queue summary error: {e}")

@app.get("/v1/risk-eval/metrics")
def risk_eval_metrics():
    try:
        return RISK.get_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# (Removed duplicate evaluation endpoints; kept single canonical versions above)

@app.get("/v1/{merchant_name}/window-counts")
def window_counts(
    merchant_name: str,
    interval: str = "30m",
    lookback: int = 24,
    now: float | None = None,
):
    """Return raw per-window counts (no scoring) so we can see if volume/activity should exist.
    Useful when volume/activity charts are blank – verifies the counting strategy.
    """
    from risk_eval import INTERVAL_MAP
    ilabel = interval.lower()
    if ilabel not in INTERVAL_MAP:
        raise HTTPException(status_code=400, detail="Unsupported interval")
    iv = INTERVAL_MAP[ilabel]
    now_ts = float(now) if now is not None else dt.datetime.now(dt.UTC).timestamp()
    start_ts = now_ts - lookback * iv * 60
    # Ensure windows so counts are computed
    try:
        RISK.ensure_windows(merchant_name, ilabel, start_ts, now_ts)
    except Exception as e:
        log(f"window_counts ensure error: {e}")
    coll = db["risk_scores"]
    rows = list(coll.find({
        "merchant": merchant_name,
        "interval_minutes": iv,
        "window_start_ts": {"$gte": start_ts}
    }, projection={"_id":0, "window_start_ts":1, "window_end_ts":1, "raw_counts":1}).sort([("window_start_ts",1)]))
    return {"merchant": merchant_name, "interval": ilabel, "windows": len(rows), "rows": rows}