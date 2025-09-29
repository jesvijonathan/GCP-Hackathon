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

SERVICE = MerchantService(db)

# ---------- FastAPI setup ----------

app = FastAPI(title="Merchant Onboarding & Data API", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup_event():
    try:
        SERVICE.ensure_indexes()
        log(f"Indexes ensured on Mongo {MONGO_URI}/{DB_NAME}")
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
            "Collections: merchants, preset.",
            "Data endpoints: GET /v1/{merchant}/data and /v1/{merchant}/stock"
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    log(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)