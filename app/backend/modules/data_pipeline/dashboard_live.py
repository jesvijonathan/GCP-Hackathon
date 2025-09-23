

#!/usr/bin/env python3
# dashboard_live.py
# Run: python3 -m streamlit run "./dashboard_live.py"

import os
import json
import time
import math
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import pandas as pd
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

    # Controls
    set_default("api_base", DEFAULT_API_BASE)
    set_default("order", "asc")
    set_default("range_mode", "Window")  # "Window" | "Since/Until"
    set_default("window_all", "1d")
    set_default("since_str", "")
    set_default("until_str", "")
    set_default("no_limit", False)
    set_default("limit_val", 5000)
    set_default("auto_refresh", True)
    set_default("refresh_sec", 20)
    set_default("stock_window_days", 30)
    set_default("include_stock_meta", True)
    set_default("allow_future", False)

    # Time controls
    set_default("simulate_now_input", "")
    set_default("time_accel_str", "200.0")  # accepts 10, x5000, 1e12, 200%
    set_default("time_accel", 200.0)

    # Sizeable jump input
    set_default("jump_value", 1.0)
    set_default("jump_unit", "days")  # seconds/minutes/hours/days

    # Simulated time baseline and wall clock
    set_default("sim_base_dt", dt.datetime.now(dt.UTC))
    set_default("wall_start_ts", time.time())

init_state()


# --------------------------- Utilities ---------------------------

def api_get(base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{base.rstrip('/')}{path}"
    params = params or {}
    if LOG_REQUESTS:
        ts = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        print(f"[{ts}] GET {url} params={params}", flush=True)
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def to_df(records: Optional[List[Dict[str, Any]]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)

def parse_user_dt(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    if s.lower() in ("now", "utcnow", "current"):
        return dt.datetime.now(dt.UTC)
    try:
        d = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(d):
            return None
        return d.to_pydatetime()
    except Exception:
        return None

def parse_multiplier(val) -> Optional[float]:
    # Accept "x2", "2x", "200%", "1e12", 2.0 -> 2.0
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except:
            return None
    s = str(val).strip().lower().replace(" ", "")
    try:
        if s.endswith("x"):
            return float(s[:-1])
        if s.startswith("x"):
            return float(s[1:])
        if s.endswith("%"):
            return 1.0 + float(s[:-1]) / 100.0
        return float(s)
    except:
        return None

def preserve_scroll():
    # Save/restore scroll position across reruns (sessionStorage)
    components.html("""
    <script>
      (function(){
        const key = "st_scroll_top";
        const saved = sessionStorage.getItem(key);
        if (saved) { window.scrollTo(0, parseInt(saved)); }
        let ticking = false;
        window.addEventListener('scroll', function(){
          if (!ticking) {
            window.requestAnimationFrame(function(){
              sessionStorage.setItem(key, String(window.scrollY || window.pageYOffset));
              ticking = false;
            });
            ticking = true;
          }
        });
      })();
    </script>
    """, height=0, width=0)

preserve_scroll()


# --------------------------- Data prep helpers ---------------------------

NEG_KEYS = ["breach", "fraud", "lawsuit", "boycott", "downtime", "outage", "recall", "regulatory", "fine", "leak", "crisis", "scandal", "layoff"]
POS_KEYS = ["new product", "launch", "award", "partnership", "expansion", "feature", "investment", "earnings beat", "milestone", "hiring", "buyback"]

def label_from_score(score: float) -> str:
    if score is None:
        return "neutral"
    try:
        s = float(score)
    except Exception:
        return "neutral"
    if s <= -0.2:
        return "negative"
    if s >= 0.2:
        return "positive"
    return "neutral"

def derive_sentiment_label_from_text(text: str) -> str:
    if not isinstance(text, str):
        return "neutral"
    t = text.lower()
    if any(k in t for k in NEG_KEYS):
        return "negative"
    if any(k in t for k in POS_KEYS):
        return "positive"
    return "neutral"

def ensure_sentiment_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "sentiment_label" not in d.columns:
        if "sentiment_score" in d.columns:
            d["sentiment_label"] = d["sentiment_score"].apply(label_from_score)
        elif source == "reviews" and "rating" in d.columns:
            d["sentiment_score"] = d["rating"].astype(float).map(lambda r: max(-1.0, min(1.0, (r - 3.0) / 2.0)))
            d["sentiment_label"] = d["sentiment_score"].apply(label_from_score)
        elif source == "tweets":
            if "content" in d.columns:
                d["sentiment_label"] = d["content"].apply(derive_sentiment_label_from_text)
            else:
                d["sentiment_label"] = "neutral"
        else:
            text_col = None
            for c in ("title", "content", "body", "selftext"):
                if c in d.columns:
                    text_col = c
                    break
            if text_col:
                d["sentiment_label"] = d[text_col].apply(derive_sentiment_label_from_text)
            else:
                d["sentiment_label"] = "neutral"
    d["sentiment_label"] = d["sentiment_label"].astype(str).fillna("neutral")
    return d

def make_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    # Reddit 'edited' can be False or int timestamp. Convert to string to avoid Arrow issues.
    if "edited" in d.columns:
        d["edited"] = d["edited"].apply(lambda v: "" if v is None else str(v))
    # Convert list/dict columns to JSON strings
    for col in d.columns:
        if d[col].dtype == object:
            sample = d[col].dropna().head(5).tolist()
            if any(isinstance(v, (list, dict)) for v in sample):
                d[col] = d[col].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else ("" if v is None else str(v)))
    return d

def parse_dt_column(df: pd.DataFrame, source: str) -> pd.DataFrame:
    d = df.copy()
    if df.empty:
        d["dt"] = pd.to_datetime([])
        return d
    if source == "news" and "published_at" in d.columns:
        d["dt"] = pd.to_datetime(d["published_at"], errors="coerce", utc=True)
    elif source == "reddit":
        if "created_at" in d.columns:
            d["dt"] = pd.to_datetime(d["created_at"], errors="coerce", utc=True)
        elif "created_utc" in d.columns:
            d["dt"] = pd.to_datetime(d["created_utc"], unit="s", errors="coerce", utc=True)
        else:
            d["dt"] = pd.NaT
    else:
        if "created_at" in d.columns:
            d["dt"] = pd.to_datetime(d["created_at"], errors="coerce", utc=True)
        else:
            d["dt"] = pd.NaT
    return d


# --------------------------- Plot helpers ---------------------------

def plot_sentiment_counts(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    if df.empty or "sentiment_label" not in df.columns:
        return None
    vc = df["sentiment_label"].astype(str).value_counts()
    if vc.empty:
        return None
    fig = px.bar(vc, title=title, labels={"index": "sentiment", "value": "count"})
    fig.update_layout(template="plotly_white")
    return fig

def plot_top_hashtags(tweets_df: pd.DataFrame) -> Optional[go.Figure]:
    if tweets_df.empty:
        return None
    tags = []
    if "entities" in tweets_df.columns:
        for ents in tweets_df["entities"].dropna():
            try:
                hs = ents.get("hashtags", []) if isinstance(ents, dict) else []
                for h in hs:
                    if isinstance(h, str) and h:
                        tags.append(h)
            except Exception:
                continue
    elif "hashtags" in tweets_df.columns:
        for hs in tweets_df["hashtags"].dropna():
            if isinstance(hs, list):
                tags.extend([t for t in hs if t])
    if not tags:
        return None
    vc = pd.Series(tags).value_counts().head(20)
    fig = px.bar(vc, title="Top hashtags")
    fig.update_layout(template="plotly_white")
    return fig

def plot_top_subreddits(reddit_df: pd.DataFrame) -> Optional[go.Figure]:
    if reddit_df.empty or "subreddit" not in reddit_df.columns:
        return None
    vc = reddit_df["subreddit"].astype(str).value_counts().head(20)
    if vc.empty:
        return None
    fig = px.bar(vc, title="Top subreddits")
    fig.update_layout(template="plotly_white")
    return fig

def plot_news_scatter(news_df: pd.DataFrame) -> Optional[go.Figure]:
    if news_df.empty or "pageviews" not in news_df.columns:
        return None
    color = "publisher" if "publisher" in news_df.columns else None
    fig = px.scatter(
        news_df, x="sentiment_score", y="pageviews",
        color=color, title="News sentiment vs pageviews", opacity=0.6
    )
    fig.update_layout(template="plotly_white")
    return fig

def plot_reviews_hist(reviews_df: pd.DataFrame) -> Optional[go.Figure]:
    if reviews_df.empty or "rating" not in reviews_df.columns:
        return None
    fig = px.histogram(reviews_df, x="rating", nbins=5, title="Ratings distribution")
    fig.update_layout(template="plotly_white")
    return fig

def plot_stock(prices_df: pd.DataFrame, earnings_df: pd.DataFrame, actions_df: pd.DataFrame, title: str = "Stock (Close)") -> Optional[go.Figure]:
    if prices_df.empty or "date" not in prices_df.columns:
        return None
    df = prices_df.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    fig = go.Figure()
    if set(["open","high","low","close"]).issubset(df.columns):
        fig.add_trace(go.Candlestick(
            x=df["date_dt"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name="OHLC", increasing_line_color="#2ca02c", decreasing_line_color="#d62728", opacity=0.85
        ))
    else:
        ycol = "close" if "close" in df.columns else "adj_close"
        fig.add_trace(go.Scatter(x=df["date_dt"], y=df[ycol], name="Close", mode="lines", line=dict(color="#1f77b4")))
    # Earnings markers
    if not earnings_df.empty and "date" in earnings_df.columns:
        e = earnings_df.copy()
        e["date_dt"] = pd.to_datetime(e["date"], errors="coerce")
        if not e.empty and "close" in df.columns:
            fig.add_trace(go.Scatter(
                x=e["date_dt"], y=[df["close"].max()*1.01]*len(e),
                mode="markers", marker=dict(color="#d62728", symbol="x", size=9), name="Earnings"
            ))
    # Dividends markers
    if not actions_df.empty and "type" in actions_df.columns and "date" in actions_df.columns:
        divs = actions_df[actions_df["type"] == "dividend"].copy()
        divs["date_dt"] = pd.to_datetime(divs["date"], errors="coerce")
        if not divs.empty and "close" in df.columns:
            fig.add_trace(go.Scatter(
                x=divs["date_dt"], y=[df["close"].max()*0.99]*len(divs),
                mode="markers", marker=dict(color="#8c564b", symbol="triangle-up", size=8), name="Dividend"
            ))
    fig.update_layout(template="plotly_white", height=480, title=title, xaxis_title="Date", yaxis_title="Price")
    return fig

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
    fig.update_layout(template="plotly_white", title=f"Activity timeline ({'hourly' if freq=='H' else 'daily'})", height=400, legend_orientation="h")
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
    fig.add_hline(y=0, line_color="gray")
    fig.update_layout(template="plotly_white", title=f"Sentiment timeline ({'hourly' if freq=='H' else 'daily'})", height=400, legend_orientation="h")
    return fig

def plot_buzz_vs_price(tw: pd.DataFrame, rd: pd.DataFrame, nw: pd.DataFrame, rv: pd.DataFrame, prices_df: pd.DataFrame) -> Optional[go.Figure]:
    def daily_counts(df, col="dt", weight_col=None):
        if df.empty or col not in df.columns:
            return pd.DataFrame(columns=["date","val"])
        d = df.dropna(subset=[col]).copy()
        # UTC-aware, floor to day, then strip tz (naive) to match stock
        d["date"] = pd.to_datetime(d[col], utc=True, errors="coerce").dt.floor("D").dt.tz_localize(None)
        if weight_col and weight_col in d.columns:
            grp = d.groupby("date")[weight_col].sum().reset_index(name="val")
        else:
            grp = d.groupby("date").size().reset_index(name="val")
        return grp

    dtw = daily_counts(tw)
    drd = daily_counts(rd)
    dnw = daily_counts(nw, weight_col="pageviews" if "pageviews" in nw.columns else None)
    drv = daily_counts(rv)

    buzz = dtw.merge(drd, on="date", how="outer", suffixes=("_tw","_rd")) \
              .merge(dnw, on="date", how="outer") \
              .merge(drv, on="date", how="outer", suffixes=("_nw","_rv"))
    if buzz.empty:
        return None

    for c in ["val", "val_tw", "val_rd", "val_nw", "val_rv"]:
        if c in buzz.columns:
            buzz[c] = buzz[c].fillna(0.0)

    vals = []
    if "val_tw" in buzz.columns: vals.append(buzz["val_tw"].astype(float))
    if "val_rd" in buzz.columns: vals.append(buzz["val_rd"].astype(float))
    if "val_nw" in buzz.columns: vals.append(buzz["val_nw"].astype(float))
    if "val_rv" in buzz.columns: vals.append(buzz["val_rv"].astype(float))
    if not vals:
        return None

    buzz["raw_sum"] = sum(vals)
    mu = buzz["raw_sum"].mean()
    sd = buzz["raw_sum"].std() if buzz["raw_sum"].std() else 1.0
    buzz["buzz_z"] = (buzz["raw_sum"] - mu) / (sd if sd != 0 else 1.0)
    buzz_plot = buzz[["date","buzz_z"]].copy()

    if prices_df.empty or "date" not in prices_df.columns:
        return None

    p = prices_df.copy()
    p["date"] = pd.to_datetime(p["date"], utc=True, errors="coerce").dt.floor("D").dt.tz_localize(None)

    merged = buzz_plot.merge(p[["date","close"]], on="date", how="inner")
    if merged.empty:
        return None

    fig = go.Figure()
    fig.add_trace(go.Bar(x=merged["date"], y=merged["buzz_z"], name="Buzz (z)", marker_color="#9edae5"))
    fig.add_trace(go.Scatter(x=merged["date"], y=merged["close"], name="Close", mode="lines", yaxis="y2", line=dict(color="#1f77b4")))
    fig.update_layout(
        template="plotly_white",
        title="Buzz vs Price (daily)",
        yaxis=dict(title="Buzz (z)"),
        yaxis2=dict(title="Close", overlaying="y", side="right"),
        height=420
    )
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


# --------------------------- UI ---------------------------

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

# Helper: use earliest since from stock meta
col_since = st.sidebar.columns(2)
with col_since[0]:
    if st.button("Earliest"):
        try:
            # Quick meta fetch
            resp = api_get(st.session_state.api_base, f"/v1/{"HomeGear" if "merchant_select" not in st.session_state else st.session_state.merchant_select}/stock",
                           params={"include_stock_meta": True, "limit": 1, "window": "1d", "now": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")})
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

st.sidebar.markdown("---")
# Time: base and acceleration (huge values OK)
st.sidebar.text_input("Simulated 'now' start (optional)", key="simulate_now_input", help="ISO, 'YYYY/MM/DD HH:MM:SS', 'now', or epoch")
st.sidebar.text_input("Time acceleration (x)", key="time_accel_str", help="Examples: 10, x5000, 200%, 1e12")
acc_parsed = parse_multiplier(st.session_state.time_accel_str)
if acc_parsed is not None and math.isfinite(acc_parsed) and acc_parsed >= 0:
    st.session_state.time_accel = float(acc_parsed)

# Sizeable jump input
st.sidebar.number_input("Jump value", key="jump_value", step=1.0, help="Can be negative to rewind")
st.sidebar.selectbox("Jump unit", options=["seconds","minutes","hours","days"], key="jump_unit")
if st.sidebar.button("Add jump"):
    val = float(st.session_state.jump_value)
    unit = st.session_state.jump_unit
    delta = dt.timedelta(**{unit: val})
    # Adjust base so simulated now shifts by delta (doesn't affect accel)
    st.session_state.sim_base_dt = st.session_state.sim_base_dt + delta

# Explicit time-base controls
col_time1, col_time2 = st.sidebar.columns(2)
with col_time1:
    if st.button("Apply base start"):
        parsed = parse_user_dt(st.session_state.simulate_now_input)
        st.session_state.sim_base_dt = parsed or dt.datetime.now(dt.UTC)
        st.session_state.wall_start_ts = time.time()
with col_time2:
    if st.button("Reset to base now"):
        st.session_state.wall_start_ts = time.time()

# Merchants list caching
@st.cache_data(ttl=300)
def cached_merchants(api_base: str) -> List[str]:
    return api_get(api_base, "/v1/merchants").get("merchants", [])

try:
    merchants_list = cached_merchants(st.session_state.api_base)
except Exception as e:
    st.error(f"Could not fetch merchants: {e}")
    merchants_list = []
merchant = st.sidebar.selectbox("Merchant", options=merchants_list or ["HomeGear"], index=0, key="merchant_select")

# Compute current simulated server time (advances with wall clock, accelerated)
elapsed = time.time() - float(st.session_state.wall_start_ts)
accel = max(0.0, float(st.session_state.time_accel))
sim_now_dt = st.session_state.sim_base_dt + dt.timedelta(seconds=elapsed * accel)
sim_now_iso = sim_now_dt.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

# Sidebar clock (always rendered)
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

# Multi-stream window fetch
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

# Prepare dataframes
tweets_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("tweets")), "tweets")), "tweets")
reddit_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("reddit")), "reddit")), "reddit")
news_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("news")), "news")), "news")
reviews_df = parse_dt_column(make_arrow_compatible(ensure_sentiment_columns(to_df(all_resp.get("data", {}).get("reviews")), "reviews")), "reviews")

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
    kpi_cols = st.columns(6)
    with kpi_cols[0]:
        st.metric("Tweets", len(tweets_df))
    with kpi_cols[1]:
        st.metric("Reddit", len(reddit_df))
    with kpi_cols[2]:
        st.metric("News", len(news_df))
    with kpi_cols[3]:
        st.metric("Reviews", len(reviews_df))
    with kpi_cols[4]:
        st.metric("Stock bars", len(stock_prices_df))
    with kpi_cols[5]:
        st.metric("Refresh (s)", st.session_state.refresh_sec)

st.markdown("---")

with tabs_container:
    tabs = st.tabs(["Overview", "Timeline", "Social (Tweets/Reddit)", "News", "Reviews", "Stock"])

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

st.markdown("---")
st.caption("Stable dashboard: top tabs preserved, reliable auto-refresh, scroll stays put. Use huge time acceleration (e.g., 1e12), size your time jump, and the 'Use earliest Since' button to auto-fill the start date from stock meta. For extremely large ranges, consider increasing the limit or using a window (e.g., 7d) to keep the payload manageable.")