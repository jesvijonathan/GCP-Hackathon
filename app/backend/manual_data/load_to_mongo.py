#!/usr/bin/env python3
# load_to_mongo.py
# Usage:
#   python load_to_mongo.py --manifest <path> --mongo mongodb://127.0.0.1:27017 --db merchant_analytics
# Requires: pip install pymongo

import os, json, argparse, hashlib, re, math, datetime as dt
from typing import Any, Dict, List, Optional
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

# --------------------------- Utils ---------------------------

def robust_json_load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def parse_window_to_timedelta(window: str) -> dt.timedelta:
    s = (window or "").strip().lower()
    m = re.match(r"^(\d+)\s*([smhdw])$", s)
    if not m:
        raise ValueError("Window must look like 15m, 1h, 24h, 7d, 1w")
    n = int(m.group(1))
    unit = m.group(2)
    return {
        "s": dt.timedelta(seconds=n),
        "m": dt.timedelta(minutes=n),
        "h": dt.timedelta(hours=n),
        "d": dt.timedelta(days=n),
        "w": dt.timedelta(weeks=n),
    }[unit]

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
    if s.lower() in ("now", "utcnow", "current"):
        return dt.datetime.now(dt.UTC)
    m = re.match(r"^now\s*([+-])\s*(\d+)\s*([smhdw])$", s.lower())
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
    return None

def ts_of_record(stream: str, rec: Dict[str, Any]) -> Optional[float]:
    try:
        if stream == "reddit":
            cu = rec.get("created_utc")
            if isinstance(cu, (int, float)) and cu > 0:
                return float(cu)
            d = rec.get("created_at")
        elif stream == "news":
            d = rec.get("published_at")
        elif stream == "wl":
            d = rec.get("txn_time")
        elif stream.startswith("stocks_"):
            d = rec.get("date")
        else:
            d = rec.get("created_at") or rec.get("date")
        if not d:
            return None
        dt_obj = parse_any_dt(d)
        return dt_obj.timestamp() if dt_obj else None
    except Exception:
        return None

def make_doc_key(merchant: str, stream: str, rec: Dict[str, Any]) -> str:
    parts = [merchant, stream]
    # Prefer natural IDs if present
    for k in ("tweet_id","id","review_id","txn_id","url","guid"):
        v = rec.get(k)
        if v:
            parts.append(str(v))
            return "|".join(parts)
    # Fallback: hash some stable fields
    fingerprint = json.dumps(
        {k: rec.get(k) for k in ("title","content","created_at","published_at","txn_time","date","body")},
        sort_keys=True
    )
    parts.append(sha1(fingerprint))
    return "|".join(parts)

def chunked(iterable, n):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_many(coll, docs: List[Dict[str, Any]], chunk=2000):
    if not docs:
        return
    for chunk_docs in chunked(docs, chunk):
        ops = []
        for d in chunk_docs:
            ops.append(UpdateOne({"doc_key": d["doc_key"]}, {"$set": d}, upsert=True))
        if ops:
            coll.bulk_write(ops, ordered=False)

# --------------------------- Index management ---------------------------

COMMON_COLL = [
    "tweets","reddit","news","reviews",
    "wl_transactions","stocks_prices","stocks_earnings","stocks_actions"
]

def dedupe_doc_key(coll):
    pipeline = [
        {"$group": {"_id": "$doc_key", "ids": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    dupes = list(coll.aggregate(pipeline))
    removed = 0
    for d in dupes:
        ids = d.get("ids") or []
        if not ids:
            continue
        keep = ids[0]
        to_delete = ids[1:]
        if to_delete:
            res = coll.delete_many({"_id": {"$in": to_delete}})
            removed += int(res.deleted_count or 0)
    if removed:
        print(f"[INFO] Deduped {removed} duplicates in {coll.name}")

def ensure_indexes(db, enforce_unique_doc_key=True, try_dedupe=True):
    # merchant+ts always present (non-unique)
    for coll_name in COMMON_COLL:
        c = db[coll_name]
        try:
            c.create_index([("merchant", ASCENDING), ("ts", ASCENDING)], name="merchant_ts", background=True)
        except Exception:
            pass

        # Handle doc_key index: drop conflicting, create desired (unique by default)
        try:
            # Detect existing doc_key indexes
            existing_doc_key_ix = None
            for ix in c.list_indexes():
                key = ix.get("key", {})
                if "doc_key" in key:
                    existing_doc_key_ix = ix
                    break

            if existing_doc_key_ix:
                # If options differ (unique mismatch or different name), drop and recreate
                current_unique = bool(existing_doc_key_ix.get("unique", False))
                if current_unique != enforce_unique_doc_key:
                    c.drop_index(existing_doc_key_ix["name"])

            # Try to create desired index
            c.create_index([("doc_key", ASCENDING)], unique=enforce_unique_doc_key, name="doc_key_idx", background=True)

        except OperationFailure as e:
            msg = str(e)
            # Conflict with a different name/options -> drop and recreate
            if "IndexOptionsConflict" in msg or "already exists with a different name" in msg:
                try:
                    for ix in c.list_indexes():
                        if "doc_key" in ix.get("key", {}):
                            c.drop_index(ix["name"])
                    c.create_index([("doc_key", ASCENDING)], unique=enforce_unique_doc_key, name="doc_key_idx", background=True)
                except OperationFailure as e2:
                    # Duplicate key error while enforcing unique
                    if enforce_unique_doc_key and ("E11000" in str(e2) or "duplicate key" in str(e2).lower()):
                        if try_dedupe:
                            dedupe_doc_key(c)
                            # Retry unique index
                            c.create_index([("doc_key", ASCENDING)], unique=True, name="doc_key_idx", background=True)
                        else:
                            print(f"[WARN] Duplicates prevent unique doc_key on {coll_name}. Using non-unique index.")
                            c.create_index([("doc_key", ASCENDING)], name="doc_key_idx", background=True)
                    else:
                        raise
            else:
                # Duplicate key error when first creating unique
                if enforce_unique_doc_key and ("E11000" in msg or "duplicate key" in msg.lower()):
                    if try_dedupe:
                        dedupe_doc_key(c)
                        c.create_index([("doc_key", ASCENDING)], unique=True, name="doc_key_idx", background=True)
                    else:
                        print(f"[WARN] Duplicates prevent unique doc_key on {coll_name}. Using non-unique index.")
                        c.create_index([("doc_key", ASCENDING)], name="doc_key_idx", background=True)
                else:
                    raise

    # stocks_meta: unique merchant
    try:
        db["stocks_meta"].create_index([("merchant", ASCENDING)], unique=True, name="uniq_merchant", background=True)
    except Exception:
        pass

# --------------------------- Loaders ---------------------------

def load_stream_list(db, merchant: str, stream: str, path: str):
    print(f"Loading {stream} for {merchant} from {path}")
    data = robust_json_load(path)
    if not isinstance(data, list):
        raise ValueError(f"{stream} JSON must be a list.")
    out = []
    for rec in data:
        ts = ts_of_record(stream, rec)
        dt_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else None
        doc = dict(rec)
        doc.update({"merchant": merchant, "ts": ts, "dt": dt_iso})
        doc_key = make_doc_key(merchant, stream, rec)
        doc["doc_key"] = doc_key
        out.append(doc)
    coll_name = stream if stream != "wl" else "wl_transactions"
    upsert_many(db[coll_name], out)

def load_stock(db, merchant: str, path: str):
    print(f"Loading stock for {merchant} from {path}")
    data = robust_json_load(path)
    meta = data.get("meta") or {}
    trend_plan = data.get("trend_plan") or []

    # meta (one doc per merchant)
    db["stocks_meta"].update_one(
        {"merchant": merchant},
        {"$set": {"merchant": merchant, "meta": meta, "trend_plan": trend_plan}},
        upsert=True
    )

    # prices
    prices = data.get("prices") or []
    docs_p = []
    for rec in prices:
        ts = ts_of_record("stocks_prices", rec)
        dt_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else None
        doc = dict(rec)
        doc.update({"merchant": merchant, "ts": ts, "dt": dt_iso})
        doc["doc_key"] = make_doc_key(merchant, "stocks_prices", rec)
        docs_p.append(doc)
    upsert_many(db["stocks_prices"], docs_p)

    # earnings
    earnings = data.get("earnings") or []
    docs_e = []
    for rec in earnings:
        ts = ts_of_record("stocks_earnings", rec)
        dt_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else None
        doc = dict(rec)
        doc.update({"merchant": merchant, "ts": ts, "dt": dt_iso})
        doc["doc_key"] = make_doc_key(merchant, "stocks_earnings", rec)
        docs_e.append(doc)
    upsert_many(db["stocks_earnings"], docs_e)

    # corporate actions
    actions = data.get("corporate_actions") or []
    docs_a = []
    for rec in actions:
        ts = ts_of_record("stocks_actions", rec)
        dt_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else None
        doc = dict(rec)
        doc.update({"merchant": merchant, "ts": ts, "dt": dt_iso})
        doc["doc_key"] = make_doc_key(merchant, "stocks_actions", rec)
        docs_a.append(doc)
    upsert_many(db["stocks_actions"], docs_a)

def load_wl(db, merchant: str, path: str):
    print(f"Loading wl for {merchant} from {path}")
    data = robust_json_load(path)
    txns = data.get("transactions") or data  # allow either {"transactions": [...]} or plain list
    if not isinstance(txns, list):
        raise ValueError("WL JSON must have 'transactions' list or be a list.")
    out = []
    for rec in txns:
        ts = ts_of_record("wl", rec)
        dt_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else None
        doc = dict(rec)
        doc.update({"merchant": merchant, "ts": ts, "dt": dt_iso})
        doc["doc_key"] = make_doc_key(merchant, "wl", rec)
        out.append(doc)
    upsert_many(db["wl_transactions"], out)

# --------------------------- Main ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Load manifest JSON data into MongoDB.")
    ap.add_argument("--manifest", required=True, help="Path to main_manifest_*.json")
    ap.add_argument("--mongo", default="mongodb://127.0.0.1:27017", help="Mongo URI")
    ap.add_argument("--db", default="merchant_analytics", help="Database name")
    ap.add_argument("--non_unique_doc_key", action="store_true", help="Do not enforce unique doc_key")
    args = ap.parse_args()

    man = robust_json_load(args.manifest)
    merchants = man.get("merchants", [])
    client = MongoClient(args.mongo)
    db = client[args.db]

    # Create merchant+ts indexes first; doc_key unique index will be adjusted after load if needed
    ensure_indexes(db, enforce_unique_doc_key=(not args.non_unique_doc_key), try_dedupe=True)

    for m in merchants:
        merchant = m.get("merchant")
        paths = m.get("paths") or {}
        if not merchant:
            continue
        if paths.get("tweets") and os.path.exists(paths["tweets"]):
            load_stream_list(db, merchant, "tweets", paths["tweets"])
        if paths.get("reddit") and os.path.exists(paths["reddit"]):
            load_stream_list(db, merchant, "reddit", paths["reddit"])
        if paths.get("news") and os.path.exists(paths["news"]):
            load_stream_list(db, merchant, "news", paths["news"])
        if paths.get("reviews") and os.path.exists(paths["reviews"]):
            load_stream_list(db, merchant, "reviews", paths["reviews"])
        if paths.get("stock") and os.path.exists(paths["stock"]):
            load_stock(db, merchant, paths["stock"])
        if paths.get("wl") and os.path.exists(paths["wl"]):
            load_wl(db, merchant, paths["wl"])

    # Final pass to enforce unique doc_key (dedupe if required)
    ensure_indexes(db, enforce_unique_doc_key=(not args.non_unique_doc_key), try_dedupe=True)

    print("Done.")

if __name__ == "__main__":
    main()