
#!/usr/bin/env python3
# data_api.py
# A lightweight realtime-ish API over the generated datasets in ./output
# Requires: fastapi, uvicorn (pip install fastapi uvicorn)
# Run: python data_api.py
# Then: http://127.0.0.1:8000/docs

import os
import json
import glob
import time
import math
import re
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from datetime import timedelta, date  # keep timedelta alias for type hints

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# --------------------------- Config ---------------------------

LOG_REQUESTS: bool = str(os.getenv("DATA_API_LOG_REQUESTS", "true")).lower() in ("1", "true", "yes", "y", "on")
DEFAULT_OUTPUT_DIR = os.getenv("DATA_API_OUTPUT_DIR", "output")
DEFAULT_MANIFEST = os.getenv("DATA_API_MANIFEST", "")

DEFAULT_WINDOWS = {
    "tweets": "1h",
    "reddit": "3h",
    "news": "6h",
    "reviews": "1d",
    "stock": "1d",
    "wl": "6h"  # NEW
}
DEFAULT_LIMITS = {
    "tweets": 500,
    "reddit": 500,
    "news": 500,
    "reviews": 500,
    "stock": 250,
    "wl": 500  # NEW
}
SUPPORTED_STREAMS = ("tweets", "reddit", "news", "reviews", "stock", "wl")  # NEW

def log(msg: str):
    if LOG_REQUESTS:
        ts = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        print(f"[{ts}] {msg}", flush=True)

def posix_path(p: str) -> str:
    return p.replace("\\", "/")

def path_exists(p: str) -> bool:
    try:
        return os.path.exists(p)
    except Exception:
        return False

def find_latest_manifest(base_dir: str = DEFAULT_OUTPUT_DIR) -> Optional[str]:
    try:
        cands = glob.glob(os.path.join(base_dir, "main_manifest_*.json"))
        if not cands:
            return None
        latest = max(cands, key=os.path.getctime)
        return latest
    except Exception:
        return None

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

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

def dt_to_iso(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.UTC)
    return dt_obj.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

def parse_window_to_timedelta(window: str) -> timedelta:
    s = (window or "").strip().lower()
    m = re.match(r"^(\d+)\s*([smhdw])$", s)
    if not m:
        raise ValueError("Window must look like 15m, 1h, 24h, 7d, 1w")
    n = int(m.group(1))
    unit = m.group(2)
    return {
        "s": timedelta(seconds=n),
        "m": timedelta(minutes=n),
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
        "w": timedelta(weeks=n),
    }[unit]

def parse_any_dt(x: Optional[str]) -> Optional[dt.datetime]:
    """
    Parse many date-time formats:
    - ISO with Z/offset, or 'YYYY/MM/DD HH:MM:SS', 'YYYY-MM-DD HH:MM:SS', date-only (treated as 23:59:59)
    - Epoch seconds/millis
    - Special tokens: 'now', 'utcnow', 'current'
    - Relative forms: 'now-1h', 'now+30m', 'now-7d'
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        sec = float(x)
        if sec > 1e12:  # ms
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
        sign = -1 if m.group(1) == "-" else 1
        num = int(m.group(2))
        unit = m.group(3)
        td = parse_window_to_timedelta(f"{num}{unit}")
        return dt.datetime.now(dt.UTC) + (sign * td)

    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if "T" in s or "+" in s or s.count("-") >= 2:
            dt_obj = dt.datetime.fromisoformat(s)
            return dt_obj if dt_obj.tzinfo else dt_obj.replace(tzinfo=dt.UTC)
    except Exception:
        pass

    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            dt_obj = dt.datetime.strptime(s, fmt)
            if fmt in ("%Y/%m/%d", "%Y-%m-%d"):
                dt_obj = dt_obj.replace(hour=23, minute=59, second=59)
            return dt_obj.replace(tzinfo=dt.UTC)
        except Exception:
            continue

    try:
        if " " in s and "T" not in s:
            s2 = s.replace(" ", "T")
            if not s2.endswith("Z") and "+" not in s2:
                s2 += "Z"
            return dt.datetime.fromisoformat(s2.replace("Z", "+00:00"))
    except Exception:
        pass

    try:
        v = float(s)
        return dt.datetime.fromtimestamp(v, tz=dt.UTC)
    except Exception:
        pass

    raise ValueError(f"Unrecognized datetime format: {x}")

def ensure_utc(dt_obj: dt.datetime) -> dt.datetime:
    return dt_obj if dt_obj.tzinfo else dt_obj.replace(tzinfo=dt.UTC)

def robust_json_load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# --------------------------- DataStore ---------------------------

class DataStore:
    def __init__(self, manifest_path: Optional[str] = None, base_output: Optional[str] = None):
        self.base_output = base_output or DEFAULT_OUTPUT_DIR
        self.manifest_path = manifest_path or DEFAULT_MANIFEST or find_latest_manifest(self.base_output)
        if not self.manifest_path or not path_exists(self.manifest_path):
            self.manifest_path = find_latest_manifest(self.base_output)
        if not self.manifest_path or not path_exists(self.manifest_path):
            raise RuntimeError("No manifest found. Generate data first with main_data.py.")

        self.manifest_ctime = os.path.getctime(self.manifest_path)
        self.manifest = robust_json_load(self.manifest_path)
        self.output_dir = self.manifest.get("output_dir") or os.path.abspath(self.base_output)
        self.merchant_map: Dict[str, Dict[str, str]] = self._build_merchant_map(self.manifest)
        self.cache: Dict[str, Dict[str, Dict[str, Any]]] = {}  # merchant -> stream -> {data, path, mtime}

    def _build_merchant_map(self, manifest: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        for m in (manifest.get("merchants") or []):
            name = m.get("merchant")
            paths = m.get("paths") or {}
            if name:
                out[name] = {k: v for k, v in paths.items() if v}
        return out

    def streams_for(self, merchant: str) -> List[str]:
        if merchant not in self.merchant_map:
            return []
        return [k for k in self.merchant_map[merchant].keys() if k in SUPPORTED_STREAMS]  # data_api.py pattern \ue202turn0file8

    def reload_manifest_if_changed(self):
        try:
            ctime = os.path.getctime(self.manifest_path)
            if ctime != self.manifest_ctime:
                self.manifest_ctime = ctime
                self.manifest = robust_json_load(self.manifest_path)
                self.output_dir = self.manifest.get("output_dir") or self.output_dir
                self.merchant_map = self._build_merchant_map(self.manifest)
                self.cache.clear()
                log("Manifest reloaded.")
        except Exception as e:
            log(f"Manifest reload error: {e}")

    def _annotate_ts_stock(self, prices: List[Dict[str, Any]], earnings: List[Dict[str, Any]], actions: List[Dict[str, Any]]):
        for rec in prices:
            if "_ts" in rec:
                continue
            try:
                d = rec.get("date")
                if d:
                    ts = parse_any_dt(str(d))
                    rec["_ts"] = ts.timestamp() if ts else None
                else:
                    rec["_ts"] = None
            except Exception:
                rec["_ts"] = None
        prices.sort(key=lambda r: (r.get("_ts") or 0.0))

        for rec in earnings:
            if "_ts" in rec:
                continue
            try:
                dt_obj = parse_any_dt(rec.get("date"))
                rec["_ts"] = dt_obj.timestamp() if dt_obj else None
            except Exception:
                rec["_ts"] = None
        earnings.sort(key=lambda r: (r.get("_ts") or 0.0))

        for rec in actions:
            if "_ts" in rec:
                continue
            try:
                dt_obj = parse_any_dt(rec.get("date"))
                rec["_ts"] = dt_obj.timestamp() if dt_obj else None
            except Exception:
                rec["_ts"] = None
        actions.sort(key=lambda r: (r.get("_ts") or 0.0))

    def _annotate_ts_list(self, stream: str, records: List[Dict[str, Any]]):
        for rec in records:
            if "_ts" in rec:
                continue
            try:
                if stream == "reddit":
                    cu = rec.get("created_utc")
                    if isinstance(cu, (int, float)) and cu > 0:
                        rec["_ts"] = float(cu)
                    else:
                        ts = parse_any_dt(rec.get("created_at"))
                        rec["_ts"] = ts.timestamp() if ts else None
                elif stream == "news":
                    ts = parse_any_dt(rec.get("published_at"))
                    rec["_ts"] = ts.timestamp() if ts else None
                elif stream == "wl":  # NEW: transactions use txn_time
                    ts = parse_any_dt(rec.get("txn_time"))
                    rec["_ts"] = ts.timestamp() if ts else None
                else:
                    ts = parse_any_dt(rec.get("created_at"))
                    rec["_ts"] = ts.timestamp() if ts else None
            except Exception:
                rec["_ts"] = None
        records.sort(key=lambda r: (r.get("_ts") or 0.0))  # data_api.py uses per-stream timestamp annotation \ue202turn0file8

    def _ensure_stream_loaded(self, merchant: str, stream: str):
        if merchant not in self.merchant_map:
            raise KeyError(f"Merchant not found: {merchant}")
        if stream not in SUPPORTED_STREAMS:
            raise KeyError(f"Unsupported stream: {stream}")
        path = self.merchant_map[merchant].get(stream)
        if not path or not path_exists(path):
            raise FileNotFoundError(f"Path not found for {merchant}/{stream}: {path}")

        mcache = self.cache.setdefault(merchant, {})
        sc = mcache.get(stream)
        mtime = os.path.getmtime(path)
        if sc and sc.get("mtime") == mtime:
            return

        # Load JSON and annotate, stream-specific
        data = robust_json_load(path)
        if stream == "stock":
            prices = data.get("prices") or []
            earnings = data.get("earnings") or []
            actions = data.get("corporate_actions") or []
            self._annotate_ts_stock(prices, earnings, actions)
            payload = {
                "meta": data.get("meta") or {},
                "trend_plan": data.get("trend_plan") or [],
                "prices": prices,
                "earnings": earnings,
                "corporate_actions": actions,
            }
        elif stream == "wl":  # NEW: transactions list
            txns = data.get("transactions") or []
            self._annotate_ts_list(stream, txns)
            payload = txns
        else:
            self._annotate_ts_list(stream, data)
            payload = data
        mcache[stream] = {"data": payload, "path": path, "mtime": mtime}  # coverage same as existing \ue202turn0file8

    def _filter_by_ts(self, records: List[Dict[str, Any]], s_ts: Optional[float], u_ts: Optional[float], desc: bool, limit: int) -> List[Dict[str, Any]]:
        def in_range(ts: Optional[float]) -> bool:
            if ts is None:
                return False
            if s_ts is not None and ts < s_ts:
                return False
            if u_ts is not None and ts > u_ts:
                return False
            return True
        it = (rec for rec in (reversed(records) if desc else records) if in_range(rec.get("_ts")))
        out = []
        for rec in it:
            if "_ts" in rec:
                rec = dict(rec)
                rec.pop("_ts", None)
            out.append(rec)
            if len(out) >= limit:
                break
        return out

    def get_data(
        self,
        merchant: str,
        streams: List[str],
        since: Optional[dt.datetime],
        until: Optional[dt.datetime],
        limits: Dict[str, int],
        order: str = "asc",
        include_stock_meta: bool = False
    ) -> Dict[str, Any]:
        self.reload_manifest_if_changed()
        if merchant not in self.merchant_map:
            raise KeyError(f"Merchant not found: {merchant}")

        results: Dict[str, Any] = {}
        s_ts = since.timestamp() if since else None
        u_ts = until.timestamp() if until else None
        desc = (order or "asc").lower() == "desc"

        for stream in streams:
            self._ensure_stream_loaded(merchant, stream)
            payload = self.cache[merchant][stream]["data"]
            raw_limit = limits.get(stream, 1000)
            limit = int(1_000_000_000) if raw_limit is None or int(raw_limit) <= 0 else int(raw_limit)

            if stream == "stock":
                prices = payload.get("prices", [])
                earnings = payload.get("earnings", [])
                actions = payload.get("corporate_actions", [])
                flt_prices = self._filter_by_ts(prices, s_ts, u_ts, desc, limit)
                flt_earnings = self._filter_by_ts(earnings, s_ts, u_ts, desc, max(50, limit))
                flt_actions = self._filter_by_ts(actions, s_ts, u_ts, desc, max(50, limit))
                out = {"prices": flt_prices, "earnings": flt_earnings, "corporate_actions": flt_actions}
                if include_stock_meta:
                    out["meta"] = payload.get("meta", {})
                    out["trend_plan"] = payload.get("trend_plan", [])
                results[stream] = out
            else:
                # wl and other list streams
                records = payload
                flt = self._filter_by_ts(records, s_ts, u_ts, desc, limit)
                results[stream] = flt
        return results

# --------------------------- API Setup ---------------------------

app = FastAPI(title="Merchant Data API", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)
DATASTORE: Optional[DataStore] = None

def get_store() -> DataStore:
    global DATASTORE
    if DATASTORE is None:
        manifest = DEFAULT_MANIFEST or find_latest_manifest(DEFAULT_OUTPUT_DIR)
        if not manifest:
            raise HTTPException(status_code=500, detail="No manifest found in ./output. Generate datasets first.")
        DATASTORE = DataStore(manifest_path=manifest, base_output=DEFAULT_OUTPUT_DIR)
        log(f"DataStore initialized with manifest: {DATASTORE.manifest_path}")
    return DATASTORE

# --------------------------- Middleware logging ---------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.time()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        took_ms = int((time.time() - started) * 1000)
        if LOG_REQUESTS:
            try:
                client = request.client.host if request.client else "-"
                ua = request.headers.get("user-agent", "-")
                log(f'{request.method} {request.url.path}?{request.url.query} {response.status_code if response else "-"} {took_ms}ms {client} UA="{ua}"')
            except Exception:
                pass

# --------------------------- Route helpers ---------------------------

def normalize_streams_param(streams_param: Optional[str]) -> List[str]:
    if not streams_param or streams_param.lower() in ("all", "any", "*"):
        return list(SUPPORTED_STREAMS)
    items = [s.strip().lower() for s in streams_param.split(",") if s.strip()]
    out = []
    for s in items:
        if s not in SUPPORTED_STREAMS:
            raise HTTPException(status_code=400, detail=f"Unsupported stream: {s}")
        out.append(s)
    return list(dict.fromkeys(out))  # data_api.py helper keeps streams normalized \ue202turn0file8

def compute_window(
    since: Optional[str],
    until: Optional[str],
    window: Optional[str],
    sim_now: dt.datetime,
    streams: List[str],
    cap_to_now: bool = True
) -> Tuple[Optional[dt.datetime], Optional[dt.datetime], Dict[str, str]]:
    try:
        until_dt = parse_any_dt(until) if until else sim_now
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        since_dt = parse_any_dt(since) if since else None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if cap_to_now and until_dt and until_dt > sim_now:
        until_dt = sim_now

    if since_dt and until_dt and since_dt > until_dt:
        since_dt = until_dt - timedelta(seconds=1)

    window_map: Dict[str, str] = {}
    if since_dt is None and window:
        try:
            td = parse_window_to_timedelta(window)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid window: {window}")
        since_dt = ensure_utc(until_dt) - td
        for s in streams:
            window_map[s] = window
    elif since_dt is None and not window:
        since_dt = None
        for s in streams:
            window_map[s] = DEFAULT_WINDOWS.get(s, "1d")

    return (ensure_utc(since_dt) if since_dt else None, ensure_utc(until_dt), window_map)

def per_stream_limits_param(streams: List[str], limit: Optional[int]) -> Dict[str, int]:
    lims = {}
    for s in streams:
        if limit is None:
            lims[s] = DEFAULT_LIMITS.get(s, 500)
        elif int(limit) <= 0:
            lims[s] = 0  # 0 means unlimited; handled in get_data
        else:
            lims[s] = int(limit)
    return lims

# --------------------------- Routes ---------------------------

@app.get("/health")
def health():
    try:
        store = get_store()
        return {"status": "ok", "manifest": posix_path(store.manifest_path), "output_dir": posix_path(store.output_dir)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

@app.get("/v1/merchants")
def list_merchants():
    store = get_store()
    return {"merchants": store.merchants()}

@app.get("/v1/{merchant}/streams")
def list_streams(merchant: str):
    store = get_store()
    if merchant not in store.merchant_map:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant}")
    return {"merchant": merchant, "streams": store.streams_for(merchant)}

@app.post("/v1/reload")
def reload_manifest():
    global DATASTORE
    DATASTORE = None
    log("DataStore reset requested.")
    return {"status": "ok", "message": "Datastore will reinitialize on next request."}

@app.get("/v1/{merchant}/data")
def get_data_window(
    merchant: str,
    streams: Optional[str] = Query(default="all"),
    now: Optional[str] = Query(default=None, description="ISO/'now'/'now-1h'/epoch"),
    window: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None, description="Use 0 or negative for no limit."),
    order: Optional[str] = Query(default="asc"),
    include_stock_meta: Optional[bool] = Query(default=False),
    allow_future: Optional[bool] = Query(default=False)
):
    store = get_store()
    if merchant not in store.merchant_map:
        raise HTTPException(status_code=404, detail=f"Merchant not found: {merchant}")

    streams_list = normalize_streams_param(streams)
    try:
        sim_now = parse_any_dt(now) if now else dt.datetime.now(dt.UTC)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    since_dt, until_dt, window_map = compute_window(since, until, window, sim_now, streams_list, cap_to_now=(not allow_future))

    since_per_stream: Dict[str, Optional[dt.datetime]] = {}
    until_per_stream: Dict[str, Optional[dt.datetime]] = {}
    for s in streams_list:
        u = until_dt
        if since_dt is not None:
            since_per_stream[s] = since_dt
            until_per_stream[s] = u
        else:
            w = window_map.get(s, "1d")
            try:
                td = parse_window_to_timedelta(w)
            except Exception:
                td = timedelta(days=1)
            since_per_stream[s] = ensure_utc(u) - td
            until_per_stream[s] = u

    lims = per_stream_limits_param(streams_list, limit)

    out_data: Dict[str, Any] = {}
    counts: Dict[str, int] = {}
    for s in streams_list:
        data = store.get_data(
            merchant=merchant,
            streams=[s],
            since=since_per_stream[s],
            until=until_per_stream[s],
            limits=lims,
            order=order or "asc",
            include_stock_meta=include_stock_meta
        )
        out_data[s] = data.get(s)
        if s == "stock":
            counts[s] = len(out_data[s].get("prices", []) if isinstance(out_data[s], dict) else [])
        else:
            counts[s] = len(out_data[s] if isinstance(out_data[s], list) else [])
    resp = {
        "merchant": merchant,
        "sim_now": dt_to_iso(sim_now),
        "range": {
            s: {
                "window": window_map.get(s),
                "since": dt_to_iso(since_per_stream[s]) if since_per_stream[s] else None,
                "until": dt_to_iso(until_per_stream[s]) if until_per_stream[s] else None
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
        merchant=merchant,
        streams=stream,
        now=now,
        window=window,
        since=since,
        until=until,
        limit=limit,
        order=order,
        include_stock_meta=include_stock_meta,
        allow_future=allow_future
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
        "name": "Merchant Data API",
        "version": "1.3.0",
        "endpoints": [
            "GET /health",
            "GET /v1/merchants",
            "GET /v1/{merchant}/streams",
            "GET /v1/{merchant}/data?streams=all|csv&now=&window=&since=&until=&limit=&order=&include_stock_meta=&allow_future=",
            "GET /v1/{merchant}/{stream}?now=&window=&since=&until=&limit=&order=&include_stock_meta=&allow_future=",
            "POST /v1/reload"
        ],
        "notes": [
            "Streams: tweets, reddit, news, reviews, stock, wl",
            "Simulated 'now' supports ISO (e.g., 2022/10/22 00:00:00), 'now', 'now-1h', or epoch seconds/millis",
            "Windows: 15m, 1h, 3h, 24h, 7d, etc. If omitted, per-stream defaults apply",
            "Absolute range via since/until. 'until' defaults to simulated now and is capped to it unless allow_future=true",
            "Set limit=0 or limit<0 to return all records within the range.",
            f"Request logging is {'ON' if LOG_REQUESTS else 'OFF'} (env DATA_API_LOG_REQUESTS=true/false)",
        ]
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    log(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run("data_api:app", host="0.0.0.0", port=port, reload=False)
