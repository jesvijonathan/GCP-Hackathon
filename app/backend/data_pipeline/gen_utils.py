import json, hashlib, re, datetime as dt
from typing import Any, Dict, List, Optional
from pymongo import UpdateOne

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def parse_window_to_timedelta(window: str) -> dt.timedelta:
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

def parse_any_dt(x: Optional[str]) -> Optional[dt.datetime]:
    if x is None: return None
    if isinstance(x, (int, float)):
        sec = float(x); sec = sec/1000.0 if sec > 1e12 else sec
        return dt.datetime.fromtimestamp(sec, tz=dt.UTC)
    s = str(x).strip()
    if not s: return None
    if s.lower() in ("now", "utcnow", "current"): return dt.datetime.now(dt.UTC)
    try:
        if s.endswith("Z"): return dt.datetime.fromisoformat(s.replace("Z","+00:00"))
        if "T" in s or "+" in s or s.count("-") >= 2:
            d0 = dt.datetime.fromisoformat(s)
            return d0 if d0.tzinfo else d0.replace(tzinfo=dt.UTC)
    except Exception:
        pass
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            d0 = dt.datetime.strptime(s, fmt)
            if fmt in ("%Y/%m/%d","%Y-%m-%d"): d0 = d0.replace(hour=23, minute=59, second=59)
            return d0.replace(tzinfo=dt.UTC)
        except Exception:
            continue
    try:
        v = float(s)
        return dt.datetime.fromtimestamp(v, tz=dt.UTC)
    except Exception:
        pass
    return None

def ts_of_record(stream: str, rec: Dict[str, Any]) -> Optional[float]:
    try:
        if stream == "reddit":
            cu = rec.get("created_utc")
            if isinstance(cu, (int, float)) and cu > 0: return float(cu)
            d = rec.get("created_at")
        elif stream == "news":
            d = rec.get("published_at")
        elif stream == "wl":
            d = rec.get("txn_time")
        elif stream.startswith("stocks_"):
            d = rec.get("date")
        else:
            d = rec.get("created_at") or rec.get("date")
        if not d: return None
        dt_obj = parse_any_dt(d)
        return dt_obj.timestamp() if dt_obj else None
    except Exception:
        return None

def make_doc_key(merchant: str, stream: str, rec: Dict[str, Any]) -> str:
    parts = [merchant, stream]
    for k in ("tweet_id","id","review_id","txn_id","url","guid"):
        v = rec.get(k)
        if v:
            parts.append(str(v))
            return "|".join(parts)
    fingerprint = json.dumps(
        {k: rec.get(k) for k in ("title","content","created_at","published_at","txn_time","date","body")},
        sort_keys=True
    )
    parts.append(sha1(fingerprint))
    return "|".join(parts)

def upsert_many(coll, docs: List[Dict[str, Any]], chunk=2000):
    if not docs: return
    ops = []
    for d in docs:
        ops.append(UpdateOne({"doc_key": d.get("doc_key")}, {"$set": d}, upsert=True))
    if ops:
        coll.bulk_write(ops, ordered=False)