#!/usr/bin/env python3
# mongo_api.py
# Run: uvicorn mongo_api:app --host 0.0.0.0 --port 8000
# Requires: pip install fastapi uvicorn pymongo python-dotenv

import os, re, json, math, time, datetime as dt
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo import MongoClient, ASCENDING, DESCENDING
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "merchant_analytics")
LOG_REQUESTS = str(os.getenv("DATA_API_LOG_REQUESTS", "true")).lower() in ("1","true","yes","on")
DEFAULT_WINDOWS = {"tweets":"1h","reddit":"3h","news":"6h","reviews":"1d","stock":"1d","wl":"6h"}
DEFAULT_LIMITS = {"tweets":500,"reddit":500,"news":500,"reviews":500,"stock":250,"wl":500}
SUPPORTED_STREAMS = ("tweets","reddit","news","reviews","stock","wl")

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

def parse_window_to_timedelta(window: str) -> dt.timedelta:
    s = (window or "").strip().lower()
    m = re.match(r"^(\d+)\s*([smhdw])$", s)
    if not m: raise HTTPException(status_code=400, detail="Window must look like 15m, 1h, 24h, 7d, 1w")
    n = int(m.group(1)); unit = m.group(2)
    return {"s":dt.timedelta(seconds=n),"m":dt.timedelta(minutes=n),"h":dt.timedelta(hours=n),"d":dt.timedelta(days=n),"w":dt.timedelta(weeks=n)}[unit]

def parse_any_dt(x: Optional[str]) -> Optional[dt.datetime]:
    if x is None: return None
    if isinstance(x, (int,float)):
        sec = float(x); sec = sec/1000.0 if sec>1e12 else sec
        return dt.datetime.fromtimestamp(sec, tz=dt.UTC)
    s = str(x).strip()
    if not s: return None
    if s.lower() in ("now","utcnow","current"): return dt.datetime.now(dt.UTC)
    m = re.match(r"^now\s*([+-])\s*(\d+)\s*([smhdw])$", s.lower())
    if m:
        sign = -1 if m.group(1)=="-" else 1
        td = parse_window_to_timedelta(m.group(2)+m.group(3))
        return dt.datetime.now(dt.UTC) + sign*td
    try:
        if s.endswith("Z"): return dt.datetime.fromisoformat(s.replace("Z","+00:00"))
        if "T" in s or "+" in s or s.count("-")>=2:
            d0 = dt.datetime.fromisoformat(s)
            return d0 if d0.tzinfo else d0.replace(tzinfo=dt.UTC)
    except Exception:
        pass
    for fmt in ("%Y/%m/%d %H:%M:%S","%Y-%m-%d %H:%M:%S","%Y/%m/%d","%Y-%m-%d"):
        try:
            d0 = dt.datetime.strptime(s, fmt)
            if fmt in ("%Y/%m/%d","%Y-%m-%d"):
                d0 = d0.replace(hour=23, minute=59, second=59)
            return d0.replace(tzinfo=dt.UTC)
        except Exception:
            continue
    try:
        v = float(s)
        return dt.datetime.fromtimestamp(v, tz=dt.UTC)
    except Exception:
        pass
    raise HTTPException(status_code=400, detail=f"Unrecognized datetime format: {x}")

def normalize_streams_param(streams_param: Optional[str]) -> List[str]:
    if not streams_param or streams_param.lower() in ("all","any","*"):
        return list(SUPPORTED_STREAMS)
    out = []
    for s in streams_param.split(","):
        s2 = s.strip().lower()
        if not s2: continue
        if s2 not in SUPPORTED_STREAMS:
            raise HTTPException(status_code=400, detail=f"Unsupported stream: {s2}")
        out.append(s2)
    seen=set(); uniq=[]
    for s in out:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def compute_window(since: Optional[str], until: Optional[str], window: Optional[str], sim_now: dt.datetime, streams: List[str], cap_to_now: bool=True):
    until_dt = parse_any_dt(until) if until else sim_now
    since_dt = parse_any_dt(since) if since else None
    if cap_to_now and until_dt and until_dt>sim_now: until_dt = sim_now
    if since_dt and until_dt and since_dt>until_dt: since_dt = until_dt - dt.timedelta(seconds=1)
    window_map: Dict[str,str] = {}
    if since_dt is None and window:
        td = parse_window_to_timedelta(window)
        since_dt = until_dt - td
        for s in streams: window_map[s] = window
    elif since_dt is None and not window:
        for s in streams: window_map[s] = DEFAULT_WINDOWS.get(s,"1d")
    return (since_dt, until_dt, window_map)

def per_stream_limits(streams: List[str], limit: Optional[int]) -> Dict[str,int]:
    lims={}
    for s in streams:
        if limit is None:
            lims[s] = DEFAULT_LIMITS.get(s, 500)
        elif int(limit) <= 0:
            lims[s] = 0  # unlimited
        else:
            lims[s] = int(limit)
    return lims

def merchants_list() -> List[str]:
    names = db["stocks_meta"].distinct("merchant")
    if names:
        return sorted(names)
    candidates = []
    for coll in ("tweets","reddit","news","reviews","wl_transactions","stocks_prices"):
        try:
            vals = db[coll].distinct("merchant")
            candidates.extend(vals)
        except Exception:
            pass
    return sorted(list(dict.fromkeys(candidates)))

def streams_for_merchant(m: str) -> List[str]:
    present = []
    coll_map = {"tweets":"tweets","reddit":"reddit","news":"news","reviews":"reviews","wl":"wl_transactions","stock":"stocks_prices"}
    for s, coll in coll_map.items():
        try:
            if db[coll].count_documents({"merchant": m}, limit=1) > 0:
                present.append(s)
        except Exception:
            pass
    return present

def query_list(coll_name: str, merchant: str, since_ts: Optional[float], until_ts: Optional[float], order: str, limit: int) -> List[Dict[str,Any]]:
    coll = db[coll_name]
    q = {"merchant": merchant}
    if since_ts is not None or until_ts is not None:
        ts_q = {}
        if since_ts is not None: ts_q["$gte"] = since_ts
        if until_ts is not None: ts_q["$lte"] = until_ts
        q["ts"] = ts_q
    sort_dir = ASCENDING if (order or "asc").lower() == "asc" else DESCENDING
    cursor = coll.find(q, projection={"_id":0,"doc_key":0}).sort("ts", sort_dir)
    if limit and limit>0:
        cursor = cursor.limit(int(limit))
    return list(cursor)

def query_stock(merchant: str, since_ts: Optional[float], until_ts: Optional[float], order: str, limit: int, include_meta: bool) -> Dict[str,Any]:
    sort_dir = ASCENDING if (order or "asc").lower() == "asc" else DESCENDING
    def q(coll_name):
        qd = {"merchant": merchant}
        if since_ts is not None or until_ts is not None:
            ts_q={}
            if since_ts is not None: ts_q["$gte"]=since_ts
            if until_ts is not None: ts_q["$lte"]=until_ts
            qd["ts"] = ts_q
        cur = db[coll_name].find(qd, projection={"_id":0,"doc_key":0}).sort("ts", sort_dir)
        if limit and limit>0:
            cur = cur.limit(int(limit))
        return list(cur)
    prices = q("stocks_prices")
    earnings = q("stocks_earnings")
    actions = q("stocks_actions")
    out = {"prices": prices, "earnings": earnings, "corporate_actions": actions}
    if include_meta:
        meta_doc = db["stocks_meta"].find_one({"merchant": merchant}, projection={"_id":0})
        if meta_doc:
            out["meta"] = meta_doc.get("meta") or {}
            out["trend_plan"] = meta_doc.get("trend_plan") or []
    return out

app = FastAPI(title="Merchant Data API (Mongo)", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.time()
    resp = None
    try:
        resp = await call_next(request)
        return resp
    finally:
        took = int((time.time() - started)*1000)
        if LOG_REQUESTS:
            try:
                log(f'{request.method} {request.url.path}?{request.url.query} {resp.status_code if resp else "-"} {took}ms')
            except Exception:
                pass

@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        return {"status":"ok","mongo":MONGO_URI,"db":DB_NAME}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status":"error","detail":str(e)})

@app.get("/v1/merchants")
def list_merchants_route():
    return {"merchants": merchants_list()}

@app.get("/v1/{merchant}/streams")
def list_streams_route(merchant: str):
    all_merchants = set(merchants_list())
    if merchant not in all_merchants:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant}")
    return {"merchant": merchant, "streams": streams_for_merchant(merchant)}

@app.get("/v1/{merchant}/data")
def get_data_window(
    merchant: str,
    streams: Optional[str] = Query(default="all"),
    now: Optional[str] = Query(default=None),
    window: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None, description="Use 0 or negative for no limit."),
    order: Optional[str] = Query(default="asc"),
    include_stock_meta: Optional[bool] = Query(default=False),
    allow_future: Optional[bool] = Query(default=False)
):
    if merchant not in set(merchants_list()):
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant}")
    streams_list = normalize_streams_param(streams)
    sim_now = parse_any_dt(now) if now else dt.datetime.now(dt.UTC)
    since_dt, until_dt, window_map = compute_window(since, until, window, sim_now, streams_list, cap_to_now=(not allow_future))
    lims = per_stream_limits(streams_list, limit)

    s_ts = since_dt.timestamp() if since_dt else None
    u_ts = until_dt.timestamp() if until_dt else None

    out_data: Dict[str,Any] = {}
    counts: Dict[str,int] = {}
    for s in streams_list:
        if s == "stock":
            data = query_stock(merchant, s_ts, u_ts, order or "asc", lims[s], include_stock_meta)
            out_data[s] = data
            counts[s] = len(data.get("prices", []))
        else:
            coll = s if s != "wl" else "wl_transactions"
            docs = query_list(coll, merchant, s_ts, u_ts, order or "asc", lims[s])
            out_data[s] = docs
            counts[s] = len(docs)

    resp = {
        "merchant": merchant,
        "sim_now": dt_to_iso(sim_now),
        "range": {
            s: {
                "window": window_map.get(s),
                "since": dt_to_iso(since_dt) if since_dt else None,
                "until": dt_to_iso(until_dt) if until_dt else None
            } for s in streams_list
        },
        "limits": {s: lims[s] for s in streams_list},
        "order": order or "asc",
        "counts": counts,
        "data": out_data
    }
    return resp

@app.get("/v1/{merchant}/{stream}")
def get_single_stream(
    merchant: str,
    stream: str,
    now: Optional[str] = Query(default=None),
    window: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None, description="Use 0 or negative for no limit."),
    order: Optional[str] = Query(default="asc"),
    include_stock_meta: Optional[bool] = Query(default=False),
    allow_future: Optional[bool] = Query(default=False)
):
    stream = (stream or "").lower()
    if stream not in SUPPORTED_STREAMS:
        raise HTTPException(status_code=400, detail=f"Unsupported stream: {stream}")
    payload = get_data_window(
        merchant=merchant, streams=stream, now=now, window=window, since=since, until=until,
        limit=limit, order=order, include_stock_meta=include_stock_meta, allow_future=allow_future
    )
    return {
        "merchant": payload["merchant"],
        "sim_now": payload["sim_now"],
        "range": {stream: payload["range"][stream]},
        "limits": {stream: payload["limits"][stream]},
        "order": payload["order"],
        "count": payload["counts"].get(stream, 0),
        "data": payload["data"].get(stream)
    }

@app.get("/")
def root():
    return {
        "name": "Merchant Data API (Mongo)",
        "version": "1.0.0",
        "streams": list(SUPPORTED_STREAMS),
        "notes": [
            "Same endpoints/params as your file-backed API.",
            "Set MONGO_URI and DB_NAME via env or defaults.",
            "Windows (15m, 1h, 24h, 7d) or absolute since/until; limit<=0 means unlimited.",
            "include_stock_meta adds meta & trend_plan."
        ]
    }