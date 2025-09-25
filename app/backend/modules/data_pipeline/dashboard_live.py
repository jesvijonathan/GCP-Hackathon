#!/usr/bin/env python3
# dashboard_live.py
# Run: python3 -m streamlit run "./dashboard_live.py"

import os
import json
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
import streamlit.components.v1 as components

# Reliable autorefresh helper
try:
    from streamlit_autorefresh import st_autorefresh
    HAVE_AUTOREFRESH = True
except Exception:
    HAVE_AUTOREFRESH = False


# --------------------------- Config ---------------------------

DEFAULT_API_BASE = os.getenv("DATA_API_BASE", "http://127.0.0.1:8000")
REQUEST_TIMEOUT = float(os.getenv("DATA_API_TIMEOUT", "10.0"))
LOG_REQUESTS = str(os.getenv("DASH_LOG_REQUESTS", "true")).lower() in ("1", "true", "yes", "y", "on")


# --------------------------- Session init ---------------------------

def init_state():
    def set_default(key, value):
        if key not in st.session_state:
            st.session_state[key] = value

    set_default("api_base", DEFAULT_API_BASE)
    set_default("order", "asc")
    set_default("range_mode", "Window")
    set_default("window_all", "1d")
    set_default("since_str", "")
    set_default("until_str", "")
    set_default("no_limit", False)
    set_default("limit_val", 500)
    set_default("auto_refresh", False)
    set_default("refresh_sec", 10)
    set_default("include_stock_meta", True)
    set_default("allow_future", False)
    set_default("simulate_now_input", "")
    set_default("time_accel_str", "x1")
    set_default("time_accel", 1.0)
    set_default("jump_value", 0.0)
    set_default("jump_unit", "seconds")
    set_default("stock_window_days", 7)
    set_default("sim_base_dt", dt.datetime.now(dt.UTC))
    set_default("wall_start_ts", time.time())
    set_default("merchant_select", "HomeGear")


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
        if s.endswith("x"):
            return float(s[:-1])
        return float(s)
    except:
        return None


def parse_user_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


# --------------------------- API helpers ---------------------------

def api_get(base: str, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = base.rstrip("/") + path
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=300)
def cached_merchants(api_base: str) -> List[str]:
    try:
        return api_get(api_base, "/v1/merchants", params={}).get("merchants", [])
    except Exception:
        return []


# --------------------------- Dataframe helpers ---------------------------

def to_df(obj: Any) -> pd.DataFrame:
    try:
        return pd.DataFrame(obj or [])
    except Exception:
        return pd.DataFrame()


def ensure_sentiment_columns(df: pd.DataFrame, stream: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "sentiment_label" not in df.columns:
        df["sentiment_label"] = None
    if "sentiment_score" not in df.columns:
        df["sentiment_score"] = None
    return df


def make_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df.copy()


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
        else:
            # fallback: try common fields
            for col in ("created_at", "date", "timestamp"):
                if col in d.columns:
                    d["dt"] = pd.to_datetime(d[col], utc=True, errors="coerce")
                    break
            if "dt" not in d.columns:
                d["dt"] = pd.NaT
    except Exception:
        d["dt"] = pd.NaT
    return d


# --------------------------- Plot helpers ---------------------------

def plot_activity_timeline(tw: pd.DataFrame, rd: pd.DataFrame, nw: pd.DataFrame, rv: pd.DataFrame, freq: str) -> Optional[go.Figure]:
    def agg(df):
        if df.empty or "dt" not in df.columns:
            return pd.DataFrame(columns=["dt", "count"])
        dd = df.dropna(subset=["dt"]).sort_values("dt")
        if dd.empty:
            return pd.DataFrame(columns=["dt", "count"])
        return dd.set_index("dt").resample(freq).size().reset_index(name="count")
    atw = agg(tw).rename(columns={"count":"tweets"})
    ard = agg(rd).rename(columns={"count":"reddit"})
    anw = agg(nw).rename(columns={"count":"news"})
    arv = agg(rv).rename(columns={"count":"reviews"})
    ts = atw.merge(ard, on="dt", how="outer").merge(anw, on="dt", how="outer").merge(arv, on="dt", how="outer")
    if ts.empty:
        return None
    for c in ["tweets","reddit","news","reviews"]:
        if c in ts.columns:
            ts[c] = ts[c].fillna(0).astype(int)
        else:
            ts[c] = 0
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["tweets"], name="Tweets", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["reddit"], name="Reddit", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["news"], name="News", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["reviews"], name="Reviews", mode="lines"))
    fig.update_layout(template="plotly_white", title="Activity timeline")
    return fig


def plot_sentiment_timeline(tw: pd.DataFrame, rd: pd.DataFrame, nw: pd.DataFrame, rv: pd.DataFrame, freq: str) -> Optional[go.Figure]:
    def to_val(label):
        if label == "positive":
            return 1
        if label == "negative":
            return -1
        return 0
    def agg(df, name):
        if df.empty or "dt" not in df.columns or "sentiment_label" not in df.columns:
            return pd.DataFrame(columns=["dt", name])
        dd = df.dropna(subset=["dt"]).sort_values("dt")
        if dd.empty:
            return pd.DataFrame(columns=["dt", name])
        dd["_val"] = dd["sentiment_label"].map(to_val)
        return dd.set_index("dt").resample(freq)["_val"].mean().rename(name).reset_index()
    stw = agg(tw, "tweets")
    srd = agg(rd, "reddit")
    snw = agg(nw, "news")
    srv = agg(rv, "reviews")
    ts = stw.merge(srd, on="dt", how="outer").merge(snw, on="dt", how="outer").merge(srv, on="dt", how="outer")
    if ts.empty:
        return None
    for c in ["tweets","reddit","news","reviews"]:
        if c in ts.columns:
            ts[c] = ts[c].fillna(0.0)
        else:
            ts[c] = 0.0
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["tweets"], name="Tweets", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["reddit"], name="Reddit", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["news"], name="News", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["reviews"], name="Reviews", mode="lines"))
    fig.update_layout(template="plotly_white", title="Sentiment timeline (avg by bin)")
    return fig


def plot_sentiment_counts(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    if df is None or df.empty or "sentiment_label" not in df.columns:
        return None
    vc = df["sentiment_label"].value_counts().reset_index()
    vc.columns = ["sentiment_label", "count"]
    fig = px.bar(vc, x="sentiment_label", y="count", title=title)
    fig.update_layout(template="plotly_white")
    return fig


def plot_top_hashtags(tweets_df: pd.DataFrame) -> Optional[go.Figure]:
    if tweets_df is None or tweets_df.empty:
        return None
    tags = []
    if "hashtags" in tweets_df.columns:
        for lst in tweets_df["hashtags"].dropna():
            if isinstance(lst, list):
                tags.extend([t for t in lst if t])
    elif "entities" in tweets_df.columns:
        for ent in tweets_df["entities"].dropna():
            if isinstance(ent, dict):
                hs = ent.get("hashtags")
                if isinstance(hs, list):
                    tags.extend([t for t in hs if t])
    if not tags:
        return None
    s = pd.Series(tags).value_counts().head(20)
    fig = px.bar(s, title="Top hashtags")
    fig.update_layout(template="plotly_white")
    return fig


def plot_top_subreddits(reddit_df: pd.DataFrame) -> Optional[go.Figure]:
    if reddit_df is None or reddit_df.empty or "subreddit" not in reddit_df.columns:
        return None
    subs = reddit_df["subreddit"].astype(str).value_counts().head(20)
    fig = px.bar(subs, title="Top subreddits")
    fig.update_layout(template="plotly_white")
    return fig


def plot_reviews_hist(reviews_df: pd.DataFrame) -> Optional[go.Figure]:
    if reviews_df is None or reviews_df.empty or "rating" not in reviews_df.columns:
        return None
    fig = px.histogram(reviews_df, x="rating", nbins=5, title="Ratings distribution")
    fig.update_layout(template="plotly_white")
    return fig


def plot_news_scatter(news_df: pd.DataFrame) -> Optional[go.Figure]:
    if news_df is None or news_df.empty:
        return None
    if "pageviews" in news_df.columns and "sentiment_score" in news_df.columns:
        fig = px.scatter(news_df, x="sentiment_score", y="pageviews", color=news_df.get("publisher"), title="News sentiment vs pageviews", opacity=0.6)
        fig.update_layout(template="plotly_white")
        return fig
    return None


def plot_stock(prices_df: pd.DataFrame, earnings_df: pd.DataFrame, actions_df: pd.DataFrame, title: str = "Stock") -> Optional[go.Figure]:
    if prices_df is None or prices_df.empty:
        return None
    fig = go.Figure()
    cols = set(prices_df.columns)
    if {"open","high","low","close","date"}.issubset(cols):
        fig.add_trace(go.Candlestick(x=prices_df["date"], open=prices_df["open"], high=prices_df["high"], low=prices_df["low"], close=prices_df["close"],
                                     name="OHLC", increasing_line_color="#2ca02c", decreasing_line_color="#d62728", opacity=0.85))
    elif {"close","date"}.issubset(cols):
        fig.add_trace(go.Scatter(x=prices_df["date"], y=prices_df["close"], name="Close", mode="lines", line=dict(color="#1f77b4")))
    for col, name, color in [("ma20","MA20","#ff7f0e"),("ma50","MA50","#2ca02c"),("ma200","MA200","#9467bd")]:
        if col in prices_df.columns and prices_df[col].notna().any():
            fig.add_trace(go.Scatter(x=prices_df["date"], y=prices_df[col], name=name, mode="lines", line=dict(color=color, width=1.0)))
    fig.update_layout(template="plotly_white", title=title, xaxis_title="Date", yaxis_title="Price")
    return fig


def plot_buzz_vs_price(tw: pd.DataFrame, rd: pd.DataFrame, nw: pd.DataFrame, rv: pd.DataFrame, prices: pd.DataFrame) -> Optional[go.Figure]:
    def agg(df, name):
        if df is None or df.empty or "dt" not in df.columns:
            return pd.DataFrame(columns=["dt", name])
        dd = df.dropna(subset=["dt"]).sort_values("dt")
        if dd.empty:
            return pd.DataFrame(columns=["dt", name])
        return dd.set_index("dt").resample("D").size().rename(name).reset_index()
    atw = agg(tw, "tweets")
    ard = agg(rd, "reddit")
    anw = agg(nw, "news")
    arv = agg(rv, "reviews")
    ts = atw.merge(ard, on="dt", how="outer").merge(anw, on="dt", how="outer").merge(arv, on="dt", how="outer")
    if ts.empty:
        return None
    for c in ["tweets","reddit","news","reviews"]:
        ts[c] = ts[c].fillna(0).astype(float)
    ts["buzz"] = ts[["tweets","reddit","news","reviews"]].sum(axis=1)
    # z-score for buzz
    if ts["buzz"].std(ddof=0) > 0:
        ts["buzz_z"] = (ts["buzz"] - ts["buzz"].mean()) / ts["buzz"].std(ddof=0)
    else:
        ts["buzz_z"] = 0.0
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts["dt"], y=ts["buzz_z"], name="Buzz index (z)", mode="lines", line=dict(color="#2ca02c")))
    if prices is not None and not prices.empty and "date" in prices.columns and "close" in prices.columns:
        fig.add_trace(go.Scatter(x=prices["date"], y=prices["close"], name="Close", mode="lines", yaxis="y2", line=dict(color="#1f77b4")))
        fig.update_layout(yaxis=dict(title="Buzz (z-score)"),
                          yaxis2=dict(title="Price", overlaying="y", side="right"),
                          template="plotly_white", title="Buzz vs Price")
    else:
        fig.update_layout(template="plotly_white", title="Buzz index")
    return fig


# --------------------------- Sidebar clock ---------------------------

def render_sidebar_clock(sim_now_iso: str, accel: float):
    accel = max(0.0, float(accel))
    accel_js = min(accel, 1e12)
    html = f"""
    <style>
      .sb-clock {{
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Courier New", monospace;
        font-size: 14px; padding: 8px 8px; border: 1px solid #eaeaea; border-radius: 6px;
        margin-bottom: 8px; color: #0b5fa4; background: #fff;
      }}
      .sb-clock .label {{ color: #333; font-weight: 600; margin-right: 6px; }}
      .sb-clock .accel {{ color: #555; font-size: 12px; margin-left: 4px; }}
    </style>
    <div class="sb-clock">
      <div><span class="label">Simulated UTC:</span><span id="simclock"></span></div>
      <div class="accel">Speed: {accel:g}x</div>
    </div>
    <script>
      const accel = {accel_js};
      function pad(n){{return n.toString().padStart(2,'0');}}
      function fmt(d){{
          const yyyy=d.getUTCFullYear(); const mm=pad(d.getUTCMonth()+1); const dd=pad(d.getUTCDate());
          const hh=pad(d.getUTCHours()); const mi=pad(d.getUTCMinutes()); const ss=pad(d.getUTCSeconds());
          return `${{yyyy}}-${{mm}}-${{dd}} ${{hh}}:${{mi}}:${{ss}}Z`;
      }}
      let baseMs = Date.parse("{sim_now_iso}");
      function tick() {{
          baseMs += Math.max(0, 1000 * accel);
          const el = document.getElementById("simclock");
          if (el) el.textContent = fmt(new Date(baseMs));
      }}
      const el0 = document.getElementById("simclock");
      if (el0) el0.textContent = fmt(new Date(baseMs));
      setInterval(tick, 1000);
    </script>
    """
    components.html(html, height=85, scrolling=False)


# --------------------------- App ---------------------------

init_state()

st.set_page_config(page_title="Merchant Live Analytics", layout="wide")

st.sidebar.header("API & Controls")
st.sidebar.text_input("API base URL", key="api_base")
st.sidebar.selectbox("Order", options=["asc","desc"], key="order")

st.sidebar.radio("Range mode", options=["Window", "Since/Until"], index=0, key="range_mode")
use_custom_window = (st.session_state.range_mode == "Window")
st.sidebar.text_input("Window (e.g., 1h, 6h, 1d, 7d)", key="window_all", disabled=not use_custom_window)

use_absolute = (st.session_state.range_mode == "Since/Until")
st.sidebar.text_input("Since (ISO/epoch)", key="since_str", disabled=not use_absolute, help="Example: 2022/01/01 00:00:00")
st.sidebar.text_input("Until (ISO/epoch)", key="until_str", disabled=not use_absolute)

# Merchants
try:
    merchants_list = cached_merchants(st.session_state.api_base)
except Exception as e:
    st.error(f"Could not fetch merchants: {e}")
    merchants_list = [st.session_state.merchant_select]
merchant = st.sidebar.selectbox("Merchant", options=merchants_list or ["HomeGear"], index=0, key="merchant_select")

# Time accel and simulated now
st.sidebar.markdown("---")
st.sidebar.text_input("Simulated 'now' start (optional)", key="simulate_now_input", help="ISO, 'YYYY/MM/DD HH:MM:SS', 'now', or epoch")
st.sidebar.text_input("Time acceleration (x)", key="time_accel_str", help="Examples: 10, x5000, 200%, 1e12")
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
        parsed = parse_user_dt(st.session_state.simulate_now_input)
        st.session_state.sim_base_dt = parsed or dt.datetime.now(dt.UTC)
        st.session_state.wall_start_ts = time.time()
with col_time2:
    if st.button("Reset to base now"):
        st.session_state.wall_start_ts = time.time()

# Helper: use earliest since from stock meta
st.sidebar.markdown("---")
col_since = st.sidebar.columns(2)
with col_since[0]:
    if st.button("Earliest"):
        try:
            merchant_q = st.session_state.get("merchant_select", "HomeGear")
            resp = api_get(st.session_state.api_base, f"/v1/{merchant_q}/stock",
                           params={"include_stock_meta": True, "limit": 1, "window": "1d",
                                   "now": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")})
            meta = (resp.get("data") or {}).get("meta") or {}
            sd = str(meta.get("start_date") or "").strip()
            if sd:
                st.session_state.since_str = f"{sd}T00:00:00Z"
        except Exception:
            pass

st.sidebar.checkbox("No limit (return all in range)", key="no_limit")
st.sidebar.number_input("Limit per stream", min_value=1, max_value=100000, key="limit_val", step=100, disabled=st.session_state.no_limit)

st.sidebar.markdown("---")
st.sidebar.checkbox("Auto refresh", key="auto_refresh")
st.sidebar.number_input("Refresh interval (seconds)", min_value=1, max_value=3600, key="refresh_sec", step=1)
st.sidebar.checkbox("Allow future data (beyond simulated now)", key="allow_future")

# Compute current simulated server time (advances with wall clock, accelerated)
elapsed = time.time() - float(st.session_state.wall_start_ts)
accel = max(0.0, float(st.session_state.time_accel))
sim_now_dt = st.session_state.sim_base_dt + dt.timedelta(seconds=elapsed * accel)
sim_now_iso = sim_now_dt.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

with st.sidebar:
    render_sidebar_clock(sim_now_iso, accel)

# Auto-refresh
if st.session_state.auto_refresh:
    if HAVE_AUTOREFRESH:
        st_autorefresh(interval=int(st.session_state.refresh_sec * 1000), key="auto_refresh_counter")
    else:
        st.warning("Auto-refresh helper not installed. Run: pip install streamlit-autorefresh")

# Manual refresh
if st.sidebar.button("Update now"):
    _rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if callable(_rerun):
        _rerun()

# --------------------------- Fetch data (simple, stable) ---------------------------

params_all = {
    "streams": "all",
    "order": st.session_state.order,
    "limit": 0 if st.session_state.no_limit else int(st.session_state.limit_val),
    "include_stock_meta": st.session_state.include_stock_meta,
    "allow_future": st.session_state.allow_future,
    "now": sim_now_iso,
}
if st.session_state.range_mode == "Window" and st.session_state.window_all.strip():
    params_all["window"] = st.session_state.window_all.strip()
if st.session_state.range_mode == "Since/Until":
    if st.session_state.since_str.strip():
        params_all["since"] = st.session_state.since_str.strip()
    params_all["until"] = st.session_state.until_str.strip() or sim_now_iso

st.caption(f"Fetching /v1/{merchant}/data with params: {params_all}")
try:
    all_resp = api_get(st.session_state.api_base, f"/v1/{merchant}/data", params=params_all)
except Exception as e:
    st.error(f"API error: {e}")
    st.stop()

# Stock single-stream (larger limit)
params_stock = {
    "order": st.session_state.order,
    "limit": 0 if st.session_state.no_limit else max(int(st.session_state.limit_val), 5000),
    "include_stock_meta": st.session_state.include_stock_meta,
    "window": f"{st.session_state.stock_window_days}d" if st.session_state.range_mode == "Window" else None,
    "allow_future": st.session_state.allow_future,
    "now": sim_now_iso,
}
if st.session_state.range_mode == "Since/Until":
    if st.session_state.since_str.strip():
        params_stock["since"] = st.session_state.since_str.strip()
    params_stock["until"] = st.session_state.until_str.strip() or sim_now_iso

st.caption(f"Fetching /v1/{merchant}/stock with params: {params_stock}")
try:
    stock_resp = api_get(st.session_state.api_base, f"/v1/{merchant}/stock", params={k:v for k,v in params_stock.items() if v is not None})
    stock_data = stock_resp.get("data", {})
except Exception as e:
    st.warning(f"Stock fetch error (using data from all-stream call): {e}")
    stock_data = all_resp.get("data", {}).get("stock") or {}

# --------------------------- Prepare dataframes ---------------------------

tweets_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("tweets")), "tweets")), "tweets")
reddit_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("reddit")), "reddit")), "reddit")
news_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("news")), "news")), "news")
reviews_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("reviews")), "reviews")), "reviews")

# WL transactions
wl_df = parse_dt_column(to_df(all_resp.get("data", {}).get("wl")), "wl")

stock_prices_df = to_df(stock_data.get("prices"))
stock_earnings_df = to_df(stock_data.get("earnings"))
stock_actions_df = to_df(stock_data.get("corporate_actions"))
stock_meta = stock_data.get("meta") or {}

# Determine timeline frequency
rng = all_resp.get("range", {})
r_any = next(iter(rng.values()), {})
since_iso = r_any.get("since")
until_iso = r_any.get("until")
freq = "D"
try:
    if since_iso and until_iso:
        sdt = pd.to_datetime(since_iso, utc=True)
        udt = pd.to_datetime(until_iso, utc=True)
        span_days = (udt - sdt).days
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
        fig_stock = plot_stock(stock_prices_df, stock_earnings_df, stock_actions_df, title=f"Stock ({stock_meta.get('ticker') or ''}) — {st.session_state.stock_window_days}d")
        if fig_stock:
            st.plotly_chart(fig_stock, use_container_width=True, key=f"stock_chart_overview_{merchant}_{st.session_state.stock_window_days}")
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
    st.json({"range": all_resp.get("range"), "limits": all_resp.get("limits"), "order": all_resp.get("order")})

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
            st.dataframe(tweets_df.head(50), use_container_width=True, key=f"tweets_table_{merchant}")
    with c2:
        st.subheader("Reddit")
        fig_r_sent = plot_sentiment_counts(reddit_df, "Reddit sentiment")
        if fig_r_sent:
            st.plotly_chart(fig_r_sent, use_container_width=True, key=f"reddit_sent_{merchant}")
        fig_subs = plot_top_subreddits(reddit_df)
        if fig_subs:
            st.plotly_chart(fig_subs, use_container_width=True, key=f"reddit_subs_{merchant}")
        if not reddit_df.empty:
            st.dataframe(reddit_df.head(50), use_container_width=True, key=f"reddit_table_{merchant}")

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
        st.dataframe(news_df.head(50), use_container_width=True, key=f"news_table_{merchant}")

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
        st.dataframe(reviews_df.head(50), use_container_width=True, key=f"reviews_table_{merchant}")

# Stock
with tabs[5]:
    st.subheader(f"Stock ({stock_meta.get('ticker') or ''})")
    fig_s = plot_stock(stock_prices_df, stock_earnings_df, stock_actions_df, title=f"Stock ({stock_meta.get('ticker') or ''}) — {st.session_state.stock_window_days}d")
    if fig_s:
        st.plotly_chart(fig_s, use_container_width=True, key=f"stock_chart_tab_{merchant}")
    if not stock_prices_df.empty:
        if "return_pct" in stock_prices_df.columns:
            st.plotly_chart(px.histogram(stock_prices_df, x="return_pct", nbins=60, title="Daily return distribution"),
                            use_container_width=True, key=f"stock_return_hist_{merchant}")
        if "volume" in stock_prices_df.columns:
            st.plotly_chart(px.line(stock_prices_df, x="date", y="volume", title="Daily volume"),
                            use_container_width=True, key=f"stock_volume_line_{merchant}")
    st.json({"meta": stock_meta} if st.session_state.include_stock_meta else {"note": "Stock meta not requested"})

# WL (new tab)
with tabs[6]:
    st.subheader("WL — Transactions analytics")
    if wl_df.empty:
        st.info("No WL transactions in range.")
    else:
        wl_view = wl_df.dropna(subset=["dt"]).copy().sort_values("dt")

        # Transaction rate over time (resample by freq)
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

        # Geo choropleth (by country_code)
        if "country_code" in wl_view.columns:
            cc = wl_view["country_code"].fillna("Unknown")
            geo_counts = cc.value_counts().reset_index()
            geo_counts.columns = ["country_code", "count"]
            try:
                fig_geo = px.choropleth(geo_counts, locations="country_code", color="count", color_continuous_scale="Blues",
                                        title="Transactions by country (merchant/issuer country_code)")
                fig_geo.update_layout(template="plotly_white", height=420)
                st.plotly_chart(fig_geo, use_container_width=True, key=f"wl_geo_{merchant}")
            except Exception:
                st.plotly_chart(px.bar(geo_counts.head(20), x="country_code", y="count", title="Top countries"),
                                use_container_width=True, key=f"wl_geo_bar_{merchant}")

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

        # Recent transactions table
        st.subheader("Recent transactions")
        cols = [c for c in ["txn_time","amount","currency_code","status","decline_reason","mcc","card_brand","card_last4","terminal_type","country_code","user_name","risk_score","risk_factors","shady_region","international","offline"] if c in wl_view.columns]
        if cols:
            st.dataframe(wl_view.sort_values("dt", ascending=False).head(50)[cols], use_container_width=True, key=f"wl_recent_{merchant}")

st.markdown("---")
st.caption("Stable dashboard: top tabs preserved, reliable auto-refresh, scroll stays put. Use huge time acceleration (e.g., 1e12), size your time jump, and the 'Earliest' button to auto-fill the start date from stock meta. For extremely large ranges, consider increasing the limit or using a window (e.g., 7d) to keep the payload manageable.")