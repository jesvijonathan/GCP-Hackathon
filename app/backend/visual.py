
#!/usr/bin/env python3
# dashboard.py
# Run: python3 -m streamlit run "./dashboard.py"

import os
import time
import math
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional reliable autorefresh helper
try:
    from streamlit_autorefresh import st_autorefresh
    HAVE_AUTOREFRESH = True
except Exception:
    HAVE_AUTOREFRESH = False

# --------------------------- Config ---------------------------

DEFAULT_API_BASE = os.getenv("DATA_API_BASE", "http://127.0.0.1:8000")
REQUEST_TIMEOUT = float(os.getenv("DATA_API_TIMEOUT", "10.0"))
LOG_REQUESTS = str(os.getenv("DASH_LOG_REQUESTS", "true")).lower() in ("1","true","yes","on")

DEFAULT_STREAMS = ["tweets","reddit","news","reviews","stock","wl"]
STREAM_TO_DF_KEY = {
    "tweets": "tweets",
    "reddit": "reddit",
    "news": "news",
    "reviews": "reviews",
    "stock": "stock",
    "wl": "wl",
}

# --------------------------- Session init ---------------------------

def init_state():
    def set_default(key, value):
        if key not in st.session_state:
            st.session_state[key] = value

    set_default("api_base", DEFAULT_API_BASE)
    set_default("order", "desc")
    set_default("range_mode", "Window")
    set_default("window_all", "7d")
    set_default("since_str", "")
    set_default("until_str", "")
    set_default("no_limit", False)
    set_default("limit_val", 5000)

    set_default("auto_refresh", False)
    set_default("refresh_sec", 10)
    set_default("allow_future", False)

    set_default("include_stock_meta", True)
    set_default("stock_window_days", 90)

    set_default("merchant_select", "HomeGear")
    set_default("streams_select", DEFAULT_STREAMS)
    set_default("max_parallel_fetches", 4)

    # Advanced performance: chunked range retrieval (splits since/until into N-day chunks per stream)
    set_default("chunk_enabled", False)
    set_default("chunk_days", 7)

    # Time accel simulation (optional)
    set_default("simulate_now_input", "")
    set_default("time_accel_str", "1")
    set_default("time_accel", 1.0)
    # Base simulated time references
    set_default("sim_base_dt", dt.datetime.now(dt.UTC))
    set_default("wall_start_ts", time.time())

# --------------------------- Request helpers ---------------------------

def build_requests_session() -> requests.Session:
    s = requests.Session()
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500,502,503,504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=32)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    except Exception:
        pass
    s.headers.update({"Accept": "application/json"})
    return s

def api_get(session: requests.Session, base: str, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = base.rstrip("/") + path
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=300)
def cached_merchants(api_base: str) -> List[str]:
    try:
        session = build_requests_session()
        return api_get(session, api_base, "/v1/merchants", params={}).get("merchants", [])
    except Exception:
        return []

# --------------------------- Dataframe helpers ---------------------------

def to_df(obj: Any) -> pd.DataFrame:
    try:
        return pd.DataFrame(obj or [])
    except Exception:
        return pd.DataFrame()

def ensure_sentiment_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    if "sentiment_label" not in d.columns:
        d["sentiment_label"] = None
    if "sentiment_score" not in d.columns:
        d["sentiment_score"] = None
    return d

def parse_dt_column(df: pd.DataFrame, stream: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    try:
        if stream == "tweets" and "created_at" in d.columns:
            d["dt"] = pd.to_datetime(d["created_at"], utc=True, errors="coerce")
        elif stream == "reddit":
            if "created_at" in d.columns:
                d["dt"] = pd.to_datetime(d["created_at"], utc=True, errors="coerce")
            elif "created_utc" in d.columns:
                d["dt"] = pd.to_datetime(d["created_utc"], unit="s", utc=True, errors="coerce")
        elif stream == "news" and "published_at" in d.columns:
            d["dt"] = pd.to_datetime(d["published_at"], utc=True, errors="coerce")
        elif stream == "reviews" and "created_at" in d.columns:
            d["dt"] = pd.to_datetime(d["created_at"], utc=True, errors="coerce")
        elif stream == "wl" and "txn_time" in d.columns:
            d["dt"] = pd.to_datetime(d["txn_time"], utc=True, errors="coerce")
        elif stream == "stock_prices" and "date" in d.columns:
            d["dt"] = pd.to_datetime(d["date"], utc=True, errors="coerce")
        else:
            # fallback
            for col in ("created_at", "date", "timestamp", "dt"):
                if col in d.columns:
                    d["dt"] = pd.to_datetime(d[col], utc=True, errors="coerce")
                    break
            if "dt" not in d.columns:
                d["dt"] = pd.NaT
    except Exception:
        d["dt"] = pd.NaT
    return d

# --------------------------- Plot helpers ---------------------------

def plot_sentiment_counts(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    if df is None or df.empty:
        return None
    if "sentiment_label" not in df.columns:
        return None
    vc = df["sentiment_label"].fillna("neutral").astype(str).value_counts()
    fig = px.bar(vc, title=title)
    fig.update_layout(template="plotly_white")
    return fig

def plot_top_hashtags(df: pd.DataFrame) -> Optional[go.Figure]:
    if df is None or df.empty:
        return None
    text_col = None
    for c in ("text","full_text","body"):
        if c in df.columns:
            text_col = c; break
    if not text_col:
        return None
    tags = []
    for s in df[text_col].dropna():
        s = str(s)
        tags.extend([x[1:] for x in pd.Series(s).str.findall(r"#([0-9A-Za-z_]+)").explode().dropna()])
    if not len(tags):
        return None
    s = pd.Series(tags).value_counts().reset_index().rename(columns={"index":"hashtag",0:"count"})
    fig = px.bar(s.head(20), x="hashtag", y="count", title="Top hashtags")
    fig.update_layout(template="plotly_white")
    return fig

def plot_top_subreddits(df: pd.DataFrame) -> Optional[go.Figure]:
    if df is None or df.empty or "subreddit" not in df.columns:
        return None
    vc = df["subreddit"].astype(str).value_counts().head(20)
    fig = px.bar(vc, title="Top subreddits")
    fig.update_layout(template="plotly_white")
    return fig

def plot_news_scatter(df: pd.DataFrame) -> Optional[go.Figure]:
    if df is None or df.empty:
        return None
    if "sentiment_score" in df.columns and "pageviews" in df.columns:
        fig = px.scatter(df, x="sentiment_score", y="pageviews",
                         color="publisher" if "publisher" in df.columns else None,
                         title="News sentiment vs pageviews", opacity=0.6)
        fig.update_layout(template="plotly_white")
        return fig
    return None

def plot_reviews_hist(df: pd.DataFrame) -> Optional[go.Figure]:
    if df is None or df.empty or "rating" not in df.columns:
        return None
    fig = px.histogram(df, x="rating", nbins=5, title="Ratings distribution")
    fig.update_layout(template="plotly_white")
    return fig

def plot_stock(prices_df: pd.DataFrame, earnings_df: pd.DataFrame, actions_df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    if prices_df is None or prices_df.empty or "date" not in prices_df.columns or "close" not in prices_df.columns:
        return None
    p = prices_df.copy()
    p["date_dt"] = pd.to_datetime(p["date"], utc=True, errors="coerce")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=p["date_dt"], y=p["close"], mode="lines", name="Close"))
    # Events (earnings/actions)
    if earnings_df is not None and not earnings_df.empty and "date" in earnings_df.columns:
        e = earnings_df.copy()
        e["date_dt"] = pd.to_datetime(e["date"], utc=True, errors="coerce")
        fig.add_trace(go.Scatter(x=e["date_dt"], y=e.get("eps_actual", None), mode="markers", name="EPS actual", marker=dict(color="#9467bd")))
    if actions_df is not None and not actions_df.empty and "date" in actions_df.columns:
        a = actions_df.copy()
        a["date_dt"] = pd.to_datetime(a["date"], utc=True, errors="coerce")
        fig.add_trace(go.Scatter(x=a["date_dt"], y=a.get("amount", None), mode="markers", name="Actions", marker=dict(color="#8c564b")))
    fig.update_layout(template="plotly_white", title=title, xaxis_title="Date", yaxis_title="Price")
    return fig

def plot_activity_timeline(tweets, reddit, news, reviews, freq="D") -> Optional[go.Figure]:
    def count_by(df):
        if df is None or df.empty or "dt" not in df.columns:
            return pd.DataFrame(columns=["dt","count"])
        s = df.dropna(subset=["dt"]).set_index("dt").resample(freq).size().rename("count").reset_index()
        return s
    t = count_by(tweets).assign(source="tweets")
    r = count_by(reddit).assign(source="reddit")
    n = count_by(news).assign(source="news")
    rv = count_by(reviews).assign(source="reviews")
    comb = pd.concat([t,r,n,rv], ignore_index=True)
    if comb.empty:
        return None
    fig = px.line(comb, x="dt", y="count", color="source", title=f"Activity timeline ({'hourly' if freq=='H' else 'daily'})")
    fig.update_layout(template="plotly_white")
    return fig

def plot_sentiment_timeline(tweets, reddit, news, reviews, freq="D") -> Optional[go.Figure]:
    def avg_sent(df):
        if df is None or df.empty or "dt" not in df.columns:
            return pd.DataFrame(columns=["dt","avg"])
        if "sentiment_score" not in df.columns:
            return pd.DataFrame(columns=["dt","avg"])
        s = df.dropna(subset=["dt"]).set_index("dt")["sentiment_score"].resample(freq).mean().rename("avg").reset_index()
        return s
    t = avg_sent(tweets).assign(source="tweets")
    r = avg_sent(reddit).assign(source="reddit")
    n = avg_sent(news).assign(source="news")
    rv = avg_sent(reviews).assign(source="reviews")
    comb = pd.concat([t,r,n,rv], ignore_index=True)
    if comb.empty:
        return None
    fig = px.line(comb, x="dt", y="avg", color="source", title=f"Sentiment timeline ({'hourly' if freq=='H' else 'daily'})")
    fig.update_layout(template="plotly_white")
    return fig

def plot_buzz_vs_price(tweets, reddit, news, reviews, prices) -> Optional[go.Figure]:
    # Build daily buzz index and overlay price
    frames = []
    for name, df in [("tweets", tweets), ("reddit", reddit), ("news", news), ("reviews", reviews)]:
        if df is not None and not df.empty and "dt" in df.columns:
            s = df.dropna(subset=["dt"]).copy()
            s["date"] = s["dt"].dt.date
            frames.append(s[["date"]].assign(source=name))
    if not frames:
        return None
    comb = pd.concat(frames, ignore_index=True)
    bb = comb.groupby(["date"]).size().rename("buzz").reset_index()
    pr = prices.copy() if prices is not None else pd.DataFrame()
    if pr.empty or "date" not in pr.columns:
        return None
    pr["date_dt"] = pd.to_datetime(pr["date"], utc=True, errors="coerce").dt.date
    m = bb.merge(pr[["date_dt","close"]].rename(columns={"date_dt":"date"}), on="date", how="left")
    fig = make_buzz_price_fig(m)
    return fig

def make_buzz_price_fig(df: pd.DataFrame) -> Optional[go.Figure]:
    if df is None or df.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["buzz"], name="Buzz", marker_color="#9edae5", opacity=0.5))
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], name="Close", yaxis="y2", line=dict(color="#1f77b4")))
    fig.update_layout(
        title="Buzz vs Price",
        yaxis=dict(title="Buzz"),
        yaxis2=dict(title="Close", overlaying="y", side="right", showgrid=False),
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
    )
    return fig

# --------------------------- Fetching (parallel + chunk options) ---------------------------

from typing import Dict, Any

def build_base_params(
    streams: str,
    order: str,
    limit: int,
    range_mode: str,
    window: str,
    since: str,
    until: str,
    allow_future: bool,
    now: str,                       # accepts now
    include_stock_meta: bool = False
) -> Dict[str, Any]:
    p = {
        "streams": streams,
        "order": order,
        "limit": 0 if limit <= 0 else int(limit),
        "allow_future": bool(allow_future),
        "now": now,  # forwards now to the API
    }

    if include_stock_meta:
        p["include_stock_meta"] = True

    if range_mode == "Window" and (window or "").strip():
        p["window"] = (window or "").strip()
    else:
        if (since or "").strip():
            p["since"] = (since or "").strip()
        p["until"] = (until or "").strip() or now

    return p


@st.cache_data(ttl=15, show_spinner=False)
def fetch_single_stream(api_base: str, merchant: str, stream: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # Calls /v1/{merchant}/data with streams=<stream>
    session = build_requests_session()
    resp = api_get(session, api_base, f"/v1/{merchant}/data", params={**params, "streams": stream})
    data = resp.get("data", {})
    rng = resp.get("range", {})
    return data, rng

def chunk_ranges(since_iso: str, until_iso: str, chunk_days: int) -> List[Tuple[str,str]]:
    s = pd.to_datetime(since_iso, utc=True, errors="coerce")
    u = pd.to_datetime(until_iso, utc=True, errors="coerce")
    if pd.isna(s) or pd.isna(u) or chunk_days <= 0:
        return []
    out = []
    cur = s
    while cur < u:
        nxt = min(u, cur + pd.Timedelta(days=int(chunk_days)))
        out.append((cur.isoformat().replace("+00:00","Z"), nxt.isoformat().replace("+00:00","Z")))
        cur = nxt
    return out

@st.cache_data(ttl=10, show_spinner=False)
def fetch_stream_chunked(api_base: str, merchant: str, stream: str, base_params: Dict[str, Any], since_iso: str, until_iso: str, chunk_days: int) -> pd.DataFrame:
    # Sequentially fetch stream data in chunks using since/until ranges to avoid huge payloads
    ranges = chunk_ranges(since_iso, until_iso, chunk_days)
    session = build_requests_session()
    rows: List[Dict[str, Any]] = []
    for (s, u) in ranges:
        p = dict(base_params)
        p.pop("window", None)
        p["since"] = s
        p["until"] = u
        try:
            resp = api_get(session, api_base, f"/v1/{merchant}/data", params={**p, "streams": stream})
            data = resp.get("data", {})
            arr = (data.get(stream) or [])
            if isinstance(arr, list):
                rows.extend(arr)
        except Exception as e:
            if LOG_REQUESTS:
                print(f"[chunk_fetch] {merchant}/{stream} {s}..{u} error: {e}")
    return to_df(rows)

def fetch_streams_parallel(api_base: str, merchant: str, streams: List[str], params: Dict[str, Any], max_workers: int=4, chunk_mode: bool=False, chunk_days: int=7, since_iso: Optional[str]=None, until_iso: Optional[str]=None) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    # For chunk_mode we use sequential chunked retrieval (per stream)
    if chunk_mode and since_iso and until_iso:
        for s in streams:
            df = fetch_stream_chunked(api_base, merchant, s, params, since_iso, until_iso, chunk_days)
            out[s] = df
        return out

    # Parallel single-call per stream
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for s in streams:
            futures.append(ex.submit(fetch_single_stream, api_base, merchant, s, params))
        for s, fut in zip(streams, futures):
            try:
                data, _rng = fut.result()
            except Exception as e:
                data, _rng = {}, {}
            # Flatten per stream
            if s == "stock":
                # composite
                stock_data = data.get("stock", {})
                out["stock_prices"] = to_df(stock_data.get("prices"))
                out["stock_earnings"] = to_df(stock_data.get("earnings"))
                out["stock_actions"] = to_df(stock_data.get("corporate_actions"))
                out["stock_meta"] = stock_data.get("meta") or {}
            else:
                out[s] = to_df(data.get(s))
    return out

# --------------------------- App ---------------------------

init_state()
st.set_page_config(page_title="Merchant Live Analytics (Optimized)", layout="wide")

# Sidebar controls
st.sidebar.header("API & Controls")
st.sidebar.text_input("API base URL", key="api_base")
st.sidebar.selectbox("Order", options=["asc","desc"], key="order")

st.sidebar.radio("Range mode", options=["Window", "Since/Until"], index=0, key="range_mode")
use_custom_window = (st.session_state.range_mode == "Window")
st.sidebar.text_input("Window (e.g., 1h, 6h, 1d, 7d)", key="window_all", disabled=not use_custom_window)

use_absolute = (st.session_state.range_mode == "Since/Until")
st.sidebar.text_input("Since (ISO/epoch)", key="since_str", disabled=not use_absolute)
st.sidebar.text_input("Until (ISO/epoch)", key="until_str", disabled=not use_absolute)

st.sidebar.checkbox("No limit (return all in range)", key="no_limit")
st.sidebar.number_input("Limit per stream", min_value=1, max_value=100000, key="limit_val", step=500, disabled=st.session_state.no_limit)

# Streams selection (optimize payload)
st.sidebar.markdown("---")
st.sidebar.multiselect("Streams", DEFAULT_STREAMS, key="streams_select")

# Performance tuning
st.sidebar.markdown("---")
st.sidebar.checkbox("Chunked fetch (split range into N-day chunks)", key="chunk_enabled")
st.sidebar.number_input("Chunk size (days)", min_value=1, max_value=60, step=1, key="chunk_days", disabled=not st.session_state.chunk_enabled)
st.sidebar.number_input("Max parallel fetches", min_value=1, max_value=8, key="max_parallel_fetches", step=1)
st.sidebar.checkbox("Include stock meta", key="include_stock_meta")

# Auto-refresh
st.sidebar.markdown("---")
st.sidebar.checkbox("Auto refresh", key="auto_refresh")
st.sidebar.number_input("Refresh interval (seconds)", min_value=2, max_value=3600, key="refresh_sec", step=1)
st.sidebar.checkbox("Allow future data (beyond 'now')", key="allow_future")

# Time accel and simulated now
st.sidebar.markdown("---")
st.sidebar.text_input("Simulated 'now' start (optional)", key="simulate_now_input", help="ISO, 'YYYY/MM/DD HH:MM:SS', 'now', or epoch")
st.sidebar.text_input("Time acceleration (x)", key="time_accel_str", help="Examples: 10, x5000, 200%, 1e6")
def parse_multiplier(s: str) -> Optional[float]:
    try:
        ss = (s or "").strip().lower()
        if not ss: return 1.0
        if ss.startswith("x"):
            return float(ss[1:])
        if ss.endswith("%"):
            return float(ss[:-1]) / 100.0
        return float(ss)
    except Exception:
        return None
acc_parsed = parse_multiplier(st.session_state.time_accel_str)
if acc_parsed is not None and math.isfinite(acc_parsed) and acc_parsed >= 0:
    st.session_state.time_accel = float(acc_parsed)

st.sidebar.number_input("Jump value", key="jump_value", step=1.0, help="Can be negative to rewind")
st.sidebar.selectbox("Jump unit", options=["seconds","minutes","hours","days"], key="jump_unit")
if st.sidebar.button("Add jump"):
    val = float(st.session_state.jump_value)
    unit = st.session_state.jump_unit
    delta = dt.timedelta(**{unit: val})
    st.session_state.sim_base_dt = st.session_state.sim_base_dt + delta

col_time1, col_time2 = st.sidebar.columns(2)
with col_time1:
    if st.button("Apply base start"):
        # parse user-provided start
        def parse_user_dt(s):
            if not s: return None
            try:
                if s.endswith("Z"):
                    return dt.datetime.fromisoformat(s.replace("Z","+00:00"))
                return dt.datetime.fromisoformat(s)
            except Exception:
                return None
        parsed = parse_user_dt(st.session_state.simulate_now_input)
        st.session_state.sim_base_dt = parsed or dt.datetime.now(dt.UTC)
        st.session_state.wall_start_ts = time.time()
with col_time2:
    if st.button("Reset to base now"):
        st.session_state.wall_start_ts = time.time()

# Merchants
try:
    merchants_list = cached_merchants(st.session_state.api_base)
except Exception as e:
    st.error(f"Could not fetch merchants: {e}")
    merchants_list = [st.session_state.merchant_select]
merchant = st.sidebar.selectbox("Merchant", options=merchants_list or ["HomeGear"], index=0, key="merchant_select")

# Helper: use earliest since from stock meta
st.sidebar.markdown("---")
col_since = st.sidebar.columns(2)
with col_since[0]:
    if st.button("Earliest"):
        try:
            session = build_requests_session()
            resp = api_get(session, st.session_state.api_base, f"/v1/{merchant}/stock",
                           params={"include_stock_meta": True, "limit": 1, "window": "1d",
                                   "now": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")})
            meta = (resp.get("data") or {}).get("meta") or {}
            sd = str(meta.get("start_date") or "").strip()
            if sd:
                st.session_state.since_str = f"{sd}T00:00:00Z"
        except Exception:
            pass

# Auto-refresh
elapsed = time.time() - float(st.session_state.wall_start_ts)
accel = max(0.0, float(st.session_state.time_accel))
sim_now_dt = st.session_state.sim_base_dt + dt.timedelta(seconds=elapsed * accel)
sim_now_iso = sim_now_dt.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

if st.session_state.auto_refresh:
    if HAVE_AUTOREFRESH:
        st_autorefresh(interval=int(st.session_state.refresh_sec * 1000), key="auto_refresh_counter")
    else:
        st.warning("Auto-refresh helper not installed. Run: pip install streamlit-autorefresh")

if st.sidebar.button("Update now"):
    _rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if callable(_rerun):
        _rerun()

# --------------------------- Fetch data (optimized) ---------------------------

# Base params for all streams
params_all = build_base_params(
    streams="all",
    order=st.session_state.order,
    limit=0 if st.session_state.no_limit else int(st.session_state.limit_val),
    range_mode=st.session_state.range_mode,
    window=st.session_state.window_all,
    since=st.session_state.since_str,
    until=st.session_state.until_str,
    allow_future=st.session_state.allow_future,
    now=sim_now_iso,
    include_stock_meta=st.session_state.include_stock_meta
)

# Compute explicit since/until for potential chunk mode
if st.session_state.range_mode == "Window" and (st.session_state.window_all or "").strip():
    # Convert window -> explicit since/until (for progress display)
    try:
        delta = pd.to_timedelta(st.session_state.window_all.strip())
        s_iso = (pd.Timestamp(sim_now_iso).tz_convert("UTC") - delta).isoformat().replace("+00:00","Z")
        u_iso = sim_now_iso
    except Exception:
        s_iso = st.session_state.since_str.strip() or sim_now_iso
        u_iso = st.session_state.until_str.strip() or sim_now_iso
else:
    s_iso = st.session_state.since_str.strip() or sim_now_iso
    u_iso = st.session_state.until_str.strip() or sim_now_iso

streams_selected = [s for s in (st.session_state.streams_select or []) if s in DEFAULT_STREAMS] or DEFAULT_STREAMS

st.caption(f"Fetching streams {streams_selected} for merchant '{merchant}' with params: {params_all}")

try:
    df_map = fetch_streams_parallel(
        api_base=st.session_state.api_base,
        merchant=merchant,
        streams=streams_selected,
        params=params_all,
        max_workers=int(st.session_state.max_parallel_fetches),
        chunk_mode=bool(st.session_state.chunk_enabled),
        chunk_days=int(st.session_state.chunk_days),
        since_iso=s_iso,
        until_iso=u_iso
    )
except Exception as e:
    st.error(f"API error: {e}")
    st.stop()

# --------------------------- Prepare dataframes ---------------------------

tweets_df = parse_dt_column(ensure_sentiment_columns(df_map.get("tweets")), "tweets")
reddit_df = parse_dt_column(ensure_sentiment_columns(df_map.get("reddit")), "reddit")
news_df = parse_dt_column(ensure_sentiment_columns(df_map.get("news")), "news")
reviews_df = parse_dt_column(ensure_sentiment_columns(df_map.get("reviews")), "reviews")

# WL transactions
wl_df = parse_dt_column(df_map.get("wl"), "wl")

# Stock frames
stock_prices_df = parse_dt_column(df_map.get("stock_prices"), "stock_prices")
stock_earnings_df = to_df(df_map.get("stock_earnings"))
stock_actions_df = to_df(df_map.get("stock_actions"))
stock_meta = df_map.get("stock_meta") or {}

# Determine timeline frequency (hourly when range <=3 days)
freq = "D"
try:
    sdt = pd.to_datetime(s_iso, utc=True)
    udt = pd.to_datetime(u_iso, utc=True)
    span_days = max(1, (udt - sdt).days)
    freq = "H" if span_days <= 3 else "D"
except Exception:
    pass

# --------------------------- Layout (top tabs) ---------------------------

kpi_container = st.container()
tabs_container = st.container()

with kpi_container:
    kpi_cols = st.columns(7)
    with kpi_cols[0]:
        st.metric("Tweets", len(tweets_df))
    with kpi_cols[1]:
        st.metric("Reddit", len(reddit_df))
    with kpi_cols[2]:
        st.metric("News", len(news_df))
    with kpi_cols[3]:
        st.metric("Reviews", len(reviews_df))
    with kpi_cols[4]:
        st.metric("WL txns", len(wl_df))
    with kpi_cols[5]:
        st.metric("Stock bars", len(stock_prices_df))
    with kpi_cols[6]:
        st.metric("Refresh (s)", st.session_state.refresh_sec)

st.markdown("---")

with tabs_container:
    tabs = st.tabs(["Overview", "Timeline", "Social (Tweets/Reddit)", "News", "Reviews", "Stock", "WL"])

# Overview
with tabs[0]:
    col1, col2 = st.columns([2,1])
    with col1:
        fig_stock = plot_stock(stock_prices_df, stock_earnings_df, stock_actions_df, title=f"Stock ({stock_meta.get('ticker') or ''}) — window")
        if fig_stock:
            st.plotly_chart(fig_stock, use_container_width=True, key=f"stock_chart_overview_{merchant}")
        fig_news_scatter = plot_news_scatter(news_df)
        if fig_news_scatter:
            st.plotly_chart(fig_news_scatter, use_container_width=True, key=f"news_scatter_overview_{merchant}")
        fig_buzz = plot_buzz_vs_price(tweets_df, reddit_df, news_df, reviews_df, stock_prices_df)
        if fig_buzz:
            st.plotly_chart(fig_buzz, use_container_width=True, key=f"buzz_vs_price_{merchant}")
    with col2:
        frames = []
        for name, df in [("tweets", tweets_df), ("reddit", reddit_df), ("news", news_df), ("reviews", reviews_df)]:
            if not df.empty and "sentiment_label" in df.columns:
                frames.append(df[["sentiment_label"]].assign(source=name))
        comb = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not comb.empty:
            vc = comb.groupby(["source","sentiment_label"]).size().reset_index(name="count")
            fig = px.bar(vc, x="source", y="count", color="sentiment_label", barmode="stack", title="Sentiment counts by source")
            fig.update_layout(template="plotly_white")
            st.plotly_chart(fig, use_container_width=True, key=f"sentiment_combined_{merchant}")
    st.caption(f"Simulated now (server): {sim_now_iso} (speed {accel:g}x)")
    st.json({"range": {"since": s_iso, "until": u_iso}, "order": st.session_state.order, "streams": streams_selected})

# Timeline
with tabs[1]:
    st.subheader("Activity timeline")
    fig_act = plot_activity_timeline(tweets_df, reddit_df, news_df, reviews_df, freq=freq)
    if fig_act:
        st.plotly_chart(fig_act, use_container_width=True, key=f"timeline_activity_{merchant}")
    st.subheader("Sentiment timeline")
    fig_sent = plot_sentiment_timeline(tweets_df, reddit_df, news_df, reviews_df, freq=freq)
    if fig_sent:
        st.plotly_chart(fig_sent, use_container_width=True, key=f"timeline_sentiment_{merchant}")

# Social
with tabs[2]:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Tweets")
        fig_t_sent = plot_sentiment_counts(tweets_df, "Tweet sentiment")
        if fig_t_sent:
            st.plotly_chart(fig_t_sent, use_container_width=True, key=f"tweets_sent_{merchant}")
        fig_tags = plot_top_hashtags(tweets_df)
        if fig_tags:
            st.plotly_chart(fig_tags, use_container_width=True, key=f"tweets_tags_{merchant}")
        if not tweets_df.empty:
            st.dataframe(tweets_df.head(100), use_container_width=True, key=f"tweets_table_{merchant}")
    with c2:
        st.subheader("Reddit")
        fig_r_sent = plot_sentiment_counts(reddit_df, "Reddit sentiment")
        if fig_r_sent:
            st.plotly_chart(fig_r_sent, use_container_width=True, key=f"reddit_sent_{merchant}")
        fig_subs = plot_top_subreddits(reddit_df)
        if fig_subs:
            st.plotly_chart(fig_subs, use_container_width=True, key=f"reddit_subs_{merchant}")
        if not reddit_df.empty:
            st.dataframe(reddit_df.head(100), use_container_width=True, key=f"reddit_table_{merchant}")

# News
with tabs[3]:
    st.subheader("News")
    fig_n_sent = plot_sentiment_counts(news_df, "News sentiment")
    if fig_n_sent:
        st.plotly_chart(fig_n_sent, use_container_width=True, key=f"news_sent_{merchant}")
    fig_n_scatter = plot_news_scatter(news_df)
    if fig_n_scatter:
        st.plotly_chart(fig_n_scatter, use_container_width=True, key=f"news_scatter_tab_{merchant}")
    if not news_df.empty:
        st.dataframe(news_df.head(100), use_container_width=True, key=f"news_table_{merchant}")

# Reviews
with tabs[4]:
    st.subheader("Reviews")
    fig_rev_hist = plot_reviews_hist(reviews_df)
    if fig_rev_hist:
        st.plotly_chart(fig_rev_hist, use_container_width=True, key=f"reviews_hist_{merchant}")
    fig_rev_sent = plot_sentiment_counts(reviews_df, "Review sentiment")
    if fig_rev_sent:
        st.plotly_chart(fig_rev_sent, use_container_width=True, key=f"reviews_sent_{merchant}")
    if not reviews_df.empty:
        st.dataframe(reviews_df.head(100), use_container_width=True, key=f"reviews_table_{merchant}")

# Stock
with tabs[5]:
    st.subheader(f"Stock ({stock_meta.get('ticker') or ''})")
    fig_s = plot_stock(stock_prices_df, stock_earnings_df, stock_actions_df, title=f"Stock ({stock_meta.get('ticker') or ''})")
    if fig_s:
        st.plotly_chart(fig_s, use_container_width=True, key=f"stock_chart_tab_{merchant}")
    if not stock_prices_df.empty:
        if "close" in stock_prices_df.columns:
            # derive return_pct if not present
            s = stock_prices_df.copy().sort_values("date")
            s["return_pct"] = s["close"].pct_change()
            st.plotly_chart(px.histogram(s, x="return_pct", nbins=60, title="Daily return distribution").update_layout(template="plotly_white"),
                            use_container_width=True, key=f"stock_return_hist_{merchant}")
        if "volume" in stock_prices_df.columns:
            st.plotly_chart(px.line(stock_prices_df, x="date", y="volume", title="Daily volume").update_layout(template="plotly_white"),
                            use_container_width=True, key=f"stock_volume_line_{merchant}")
    st.json({"meta": stock_meta} if st.session_state.include_stock_meta else {"note": "Stock meta not requested"})

# WL
with tabs[6]:
    st.subheader("WL — Transactions analytics")
    if wl_df.empty:
        st.info("No WL transactions in range.")
    else:
        wl_view = wl_df.dropna(subset=["dt"]).copy().sort_values("dt")
        # Transaction rate over time
        wlf = wl_view.set_index("dt").resample(freq).size().reset_index(name="count")
        fig_rate = px.line(wlf, x="dt", y="count", title=f"Transaction rate ({'hourly' if freq=='H' else 'daily'})")
        fig_rate.update_layout(template="plotly_white", height=360)
        st.plotly_chart(fig_rate, use_container_width=True, key=f"wl_rate_{merchant}")
        # Approval/Decline ratio
        if "status" in wl_view.columns:
            status_counts = wl_view["status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            fig_status = px.pie(status_counts, names="status", values="count", title="Approval/Decline/Other status mix")
            fig_status.update_layout(template="plotly_white", height=360)
            st.plotly_chart(fig_status, use_container_width=True, key=f"wl_status_{merchant}")
        # High-risk factors
        if "risk_factors" in wl_view.columns:
            rf = wl_view["risk_factors"].dropna()
            factors = []
            for lst in rf:
                if isinstance(lst, list):
                    factors.extend([t for t in lst if t])
            if factors:
                fvc = pd.Series(factors).value_counts().reset_index()
                fvc.columns = ["factor","count"]
                fig_rf = px.bar(fvc.head(20), x="factor", y="count", title="Top risk factors")
                fig_rf.update_layout(template="plotly_white")
                st.plotly_chart(fig_rf, use_container_width=True, key=f"wl_risk_factors_{merchant}")
        # Recent transactions
        st.subheader("Recent transactions")
        cols = [c for c in ["txn_time","amount","currency_code","status","decline_reason","mcc","card_brand","card_last4","terminal_type","country_code","user_name","risk_score","risk_factors","shady_region","international","offline"] if c in wl_view.columns]
        if cols:
            st.dataframe(wl_view.sort_values("dt", ascending=False).head(100)[cols], use_container_width=True, key=f"wl_recent_{merchant}")

# --------------------------- Tips ---------------------------
