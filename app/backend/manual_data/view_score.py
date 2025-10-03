
import os
import sys
import math
import argparse
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient, ASCENDING, DESCENDING
import streamlit as st

# --------------------------- CLI args (for streamlit) ---------------------------

def get_cli_args():
    parser = argparse.ArgumentParser(description="Visualize merchant evaluation risk scores from MongoDB.")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"))
    parser.add_argument("--db", default=os.getenv("DB_NAME", "merchant_analytics"))
    parser.add_argument("--collection", default="merchant_evaluations")
    parser.add_argument("--progress-collection", default="merchant_eval_progress")
    args, _ = parser.parse_known_args()
    return args

ARGS = get_cli_args()

# --------------------------- Mongo helpers ---------------------------

@st.cache_resource
def get_mongo(mongo_uri: str, db_name: str):
    client = MongoClient(mongo_uri)
    return client[db_name]

@st.cache_data(ttl=30)
def list_merchants(_db, collection: str) -> List[str]:
    try:
        return sorted(_db[collection].distinct("merchant"))
    except Exception:
        return []

@st.cache_data(ttl=30)
def load_docs(
    _db,
    collection: str,
    merchant: str,
    since_ts: Optional[float],
    until_ts: Optional[float],
    order: str = "asc",
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    coll = _db[collection]
    q = {"merchant": merchant}
    if since_ts is not None or until_ts is not None:
        ts_q = {}
        if since_ts is not None:
            ts_q["$gte"] = since_ts
        if until_ts is not None:
            ts_q["$lte"] = until_ts
        q["window_end_ts"] = ts_q
    sort_dir = ASCENDING if (order or "asc").lower() == "asc" else DESCENDING
    cur = coll.find(q, projection={"_id": 0}).sort("window_end_ts", sort_dir)
    if limit and limit > 0:
        cur = cur.limit(int(limit))
    return list(cur)

@st.cache_data(ttl=10)
def load_progress(_db, progress_collection: str, merchant: str) -> Optional[Dict[str, Any]]:
    try:
        return _db[progress_collection].find_one({"merchant": merchant, "phase": "backfill"}, projection={"_id": 0})
    except Exception:
        return None

# --------------------------- Data processing ---------------------------

def to_df(docs: List[Dict[str, Any]]) -> pd.DataFrame:
    if not docs:
        return pd.DataFrame()
    rows = []
    for d in docs:
        scores = d.get("scores") or {}
        counts = d.get("counts") or {}
        rows.append({
            "merchant": d.get("merchant"),
            "window_start": d.get("window_start"),
            "window_end": d.get("window_end"),
            "window_start_ts": d.get("window_start_ts"),
            "window_end_ts": d.get("window_end_ts"),
            "is_backfill": bool(d.get("is_backfill")),
            "sim_now": d.get("sim_now"),
            "created_at": d.get("created_at"),
            "algo_version": d.get("algo_version"),
            "total": float(scores.get("total") or 0.0),
            "wl": float(scores.get("wl") or 0.0),
            "market": float(scores.get("market") or 0.0),
            "sentiment": float(scores.get("sentiment") or 0.0),
            "volume": float(scores.get("volume") or 0.0),
            "incident_bump": float(scores.get("incident_bump") or 0.0),
            "confidence": float(d.get("confidence") or 0.0),
            "tweets": int(counts.get("tweets") or 0),
            "reddit": int(counts.get("reddit") or 0),
            "news": int(counts.get("news") or 0),
            "reviews": int(counts.get("reviews") or 0),
            "wl_count": int(counts.get("wl") or 0),
            "stock_prices": int(counts.get("stock_prices") or 0),
            "drivers": d.get("drivers") or []
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["dt"] = pd.to_datetime(df["window_end"], utc=True, errors="coerce")
        df = df.sort_values("dt")
    return df

def ema(series: pd.Series, span: int) -> pd.Series:
    try:
        return series.ewm(span=max(1, int(span)), adjust=False).mean()
    except Exception:
        return series

def agg_top_drivers(df: pd.DataFrame, top_k: int = 20) -> pd.DataFrame:
    if df.empty or "drivers" not in df.columns:
        return pd.DataFrame(columns=["driver", "count"])
    drivers = []
    for lst in df["drivers"]:
        if isinstance(lst, list):
            drivers.extend([x for x in lst if x])
    if not drivers:
        return pd.DataFrame(columns=["driver", "count"])
    s = pd.Series(drivers).value_counts().reset_index()
    s.columns = ["driver", "count"]
    return s.head(top_k)

# --------------------------- Streamlit UI ---------------------------

st.set_page_config(page_title="Merchant Risk Evaluation", layout="wide")
st.title("Merchant Risk Evaluation — Visualizer")

db = get_mongo(ARGS.mongo_uri, ARGS.db)
merchants = list_merchants(db, ARGS.collection)

if not merchants:
    st.error("No merchants found in Mongo. Check your connection/collection.")
    st.stop()

with st.sidebar:
    st.subheader("Controls")
    merchant = st.selectbox("Merchant", merchants, index=0)

    # Discover range from DB quickly (sample head/tail)
    # Load earliest and latest by sorting if possible (limit small)
    earliest_docs = load_docs(db, ARGS.collection, merchant, None, None, order="asc", limit=1)
    latest_docs = load_docs(db, ARGS.collection, merchant, None, None, order="desc", limit=1)
    if earliest_docs and latest_docs:
        min_dt = pd.to_datetime(earliest_docs[0].get("window_start"), utc=True)
        max_dt = pd.to_datetime(latest_docs[0].get("window_end"), utc=True)
    else:
        min_dt = pd.Timestamp(dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc))
        max_dt = pd.Timestamp(dt.datetime.now(dt.timezone.utc))

    dtr = st.date_input("Date range", value=(min_dt.date(), max_dt.date()))
    if isinstance(dtr, tuple) and len(dtr) == 2:
        since_dt = dt.datetime.combine(dtr[0], dt.time(0, 0, 0), tzinfo=dt.timezone.utc)
        until_dt = dt.datetime.combine(dtr[1], dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
    else:
        since_dt = min_dt.to_pydatetime()
        until_dt = max_dt.to_pydatetime()

    order = st.selectbox("Order", ["asc", "desc"], index=0)
    limit = st.number_input("Max docs (0=all)", min_value=0, value=0, step=100)
    smooth_span = st.slider("Smoothing (EMA span)", min_value=1, max_value=50, value=5)
    show_components = st.checkbox("Show components", value=True)
    show_confidence = st.checkbox("Show confidence", value=True)
    show_incident_bump = st.checkbox("Show incident bump", value=True)
    show_backfill_only = st.checkbox("Filter: backfill only", value=False)
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)

# Auto refresh
if auto_refresh:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=30_000, key="risk_autorefresh")
    except Exception:
        st.caption("Install streamlit-autorefresh for auto-refresh: pip install streamlit-autorefresh")

# Load docs in selected range
since_ts = since_dt.timestamp() if since_dt else None
until_ts = until_dt.timestamp() if until_dt else None
docs = load_docs(db, ARGS.collection, merchant, since_ts, until_ts, order=order, limit=limit)
df = to_df(docs)
if show_backfill_only and not df.empty:
    df = df[df["is_backfill"] == True]

if df.empty:
    st.info("No evaluation documents found for the selected filters.")
    st.stop()

# KPI header
colA, colB, colC, colD, colE = st.columns(5)
with colA:
    st.metric("Windows", len(df))
with colB:
    st.metric("Last total", f"{df['total'].iloc[-1]:.2f}")
with colC:
    st.metric("Avg total", f"{df['total'].mean():.2f}")
with colD:
    st.metric("Max total", f"{df['total'].max():.2f}")
with colE:
    st.metric("Confidence (avg)", f"{df['confidence'].mean():.2f}")

st.markdown("---")

# Main chart: total risk over time
fig_total = go.Figure()
fig_total.add_trace(go.Scatter(x=df["dt"], y=ema(df["total"], smooth_span), name="Total risk", mode="lines", line=dict(color="#1f77b4", width=2)))
# Risk threshold bands
for thr, color in [(25, "#e0e0e0"), (50, "#cccccc"), (75, "#bbbbbb")]:
    fig_total.add_hline(y=thr, line=dict(color=color, width=1, dash="dot"), annotation_text=f"{thr}", annotation_position="top left")
if show_confidence:
    fig_total.add_trace(go.Scatter(x=df["dt"], y=ema(df["confidence"] * 100.0, smooth_span), name="Confidence (%)", mode="lines", yaxis="y2", line=dict(color="#ff7f0e", width=1)))
fig_total.update_layout(
    title="Total risk score over time",
    xaxis_title="Window end",
    yaxis=dict(title="Risk (0..100)"),
    yaxis2=dict(title="Confidence (%)", overlaying="y", side="right", showgrid=False),
    template="plotly_white",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
)
st.plotly_chart(fig_total, use_container_width=True)

# Components chart
if show_components:
    fig_comp = go.Figure()
    fig_comp.add_trace(go.Scatter(x=df["dt"], y=ema(df["wl"], smooth_span), name="WL", mode="lines"))
    fig_comp.add_trace(go.Scatter(x=df["dt"], y=ema(df["market"], smooth_span), name="Market", mode="lines"))
    fig_comp.add_trace(go.Scatter(x=df["dt"], y=ema(df["sentiment"], smooth_span), name="Sentiment", mode="lines"))
    fig_comp.add_trace(go.Scatter(x=df["dt"], y=ema(df["volume"], smooth_span), name="Volume/Anomaly", mode="lines"))
    if show_incident_bump:
        fig_comp.add_trace(go.Bar(x=df["dt"], y=df["incident_bump"], name="Incident bump", marker_color="#d62728", opacity=0.35))
    fig_comp.update_layout(
        title="Risk components",
        xaxis_title="Window end",
        yaxis=dict(title="Score component (0..100)"),
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
    )
    st.plotly_chart(fig_comp, use_container_width=True)

# Activity counts (buzz)
counts_cols = ["tweets", "reddit", "news", "reviews"]
buzz_df = df[["dt"] + counts_cols].copy()
buzz_df["buzz"] = buzz_df[counts_cols].sum(axis=1)
fig_buzz = go.Figure()
fig_buzz.add_trace(go.Scatter(x=buzz_df["dt"], y=ema(buzz_df["buzz"].astype(float), smooth_span), name="Buzz (total activity)", mode="lines", line=dict(color="#2ca02c")))
for col, color in zip(counts_cols, ["#1f77b4", "#ff7f0e", "#9467bd", "#8c564b"]):
    fig_buzz.add_trace(go.Scatter(x=buzz_df["dt"], y=ema(buzz_df[col].astype(float), smooth_span), name=col.title(), mode="lines", line=dict(color=color, width=1)))
fig_buzz.update_layout(template="plotly_white", title="Activity counts per window", xaxis_title="Window end", yaxis_title="Count")
st.plotly_chart(fig_buzz, use_container_width=True)

# Top drivers
st.subheader("Top drivers")
drv_df = agg_top_drivers(df, top_k=25)
if drv_df.empty:
    st.info("No drivers found.")
else:
    st.plotly_chart(px.bar(drv_df, x="driver", y="count", title="Drivers (frequency)").update_layout(template="plotly_white"), use_container_width=True)

# Recent windows table
st.subheader("Recent windows")
show_cols = ["dt", "total", "wl", "market", "sentiment", "volume", "incident_bump", "confidence", "tweets", "reddit", "news", "reviews", "wl_count", "stock_prices", "is_backfill", "algo_version"]
st.dataframe(df.sort_values("dt", ascending=False).head(200)[show_cols], use_container_width=True)

# Progress (if available)
st.markdown("---")
st.subheader("Backfill progress")
prog = load_progress(db, ARGS.progress_collection, merchant)
if prog:
    cols = st.columns(4)
    with cols[0]:
        st.metric("Completed", prog.get("completed_windows"))
    with cols[1]:
        st.metric("Total", prog.get("total_windows"))
    with cols[2]:
        pct = prog.get("percent")
        st.metric("Percent", f"{pct:.2f}%" if isinstance(pct, (int, float)) else pct)
    with cols[3]:
        eta = prog.get("eta_sec")
        st.metric("ETA (s)", f"{eta:.1f}" if isinstance(eta, (int, float)) else eta)
else:
    st.info("No progress document found (optional).")

# Footer
st.caption(f"Mongo: {ARGS.mongo_uri} / DB: {ARGS.db} / Coll: {ARGS.collection} / Progress: {ARGS.progress_collection}")

# py -3 -m  streamlit run .\view_score.py -- --mongo-uri "mongodb://127.0.0.1:27017" --db merchant_analytics --collection merchant_evaluations --progress-collection merchant_eval_progress