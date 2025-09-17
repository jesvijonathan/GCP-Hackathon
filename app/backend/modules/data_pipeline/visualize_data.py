import os
import json
import math
import warnings
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd

# Interactive UI
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import glob

# Optional streaming JSON
try:
    import ijson
    HAVE_IJSON = True
except Exception:
    HAVE_IJSON = False

# Optional sentiment (VADER)
SIA = None
try:
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    try:
        # Do not download here; just attempt to init
        SIA = SentimentIntensityAnalyzer()
    except Exception:
        SIA = None
except Exception:
    SIA = None

warnings.filterwarnings("ignore", category=FutureWarning)


# -------------------------- Utils --------------------------

def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")


def parse_date_str(s: str) -> date:
    if isinstance(s, (datetime, pd.Timestamp)):
        return s.date()
    if isinstance(s, date):
        return s
    if isinstance(s, str) and len(s) == 7:
        return date.fromisoformat(s + "-01")
    return date.fromisoformat(s)


def month_midpoint(month: str) -> pd.Timestamp:
    d = parse_date_str(month)
    return pd.Timestamp(year=d.year, month=d.month, day=15, tz="UTC")


def safe_json_load(path: str, limit: Optional[int] = None) -> Any:
    # Stream first N items from a big list if ijson available and limit set
    if limit is not None and HAVE_IJSON:
        out = []
        with open(path, "rb") as f:
            for idx, item in enumerate(ijson.items(f, "item")):
                out.append(item)
                if idx + 1 >= limit:
                    break
        return out
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def posix_path(p: str) -> str:
    return p.replace("\\", "/")


# -------------------------- Sentiment --------------------------

POS_WORDS = ["great", "good", "love", "positive", "impressed", "excited", "promising", "strong", "award", "launch", "feature", "partnership", "buyback", "expansion"]
NEG_WORDS = ["bad", "breach", "leak", "outage", "lawsuit", "concern", "disappointed", "scandal", "crisis", "recall", "fraud", "regulatory", "fine"]

def basic_lexicon_score(text: str) -> float:
    t = (text or "").lower()
    score = 0.0
    for w in POS_WORDS:
        if w in t:
            score += 0.2
    for w in NEG_WORDS:
        if w in t:
            score -= 0.25
    return float(np.clip(score, -1.0, 1.0))


def compute_sentiment_series(texts: pd.Series, langs: Optional[pd.Series] = None, use_vader: bool = True) -> pd.Series:
    scores = []
    if use_vader and SIA is not None:
        for i, txt in enumerate(texts):
            text = str(txt or "")
            c = SIA.polarity_scores(text).get("compound", 0.0)
            if langs is not None and i < len(langs):
                lang = langs.iloc[i]
                if str(lang).upper() not in ("EN", "ENG"):
                    c *= 0.6
            scores.append(c)
    else:
        scores = [basic_lexicon_score(str(t)) for t in texts]
    return pd.Series(scores, index=texts.index, dtype=float)


def label_from_score(s: float) -> str:
    if s <= -0.2:
        return "negative"
    elif s >= 0.2:
        return "positive"
    return "neutral"


# -------------------------- Normalizers --------------------------

def normalize_tweets(path: str, limit: Optional[int] = None, use_vader: bool = True) -> pd.DataFrame:
    raw = safe_json_load(path, limit=limit)
    if not isinstance(raw, list):
        raise ValueError("Tweets JSON must be a list.")
    df = pd.json_normalize(raw, sep=".")
    if "created_at" not in df.columns:
        raise ValueError("Tweets missing created_at.")
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["date"] = df["created_at"].dt.date

    rename_map = {
        "public_metrics.like_count": "like_count",
        "public_metrics.reply_count": "reply_count",
        "public_metrics.retweet_count": "retweet_count",
        "public_metrics.quote_count": "quote_count",
        "non_public_metrics.impression_count": "impression_count",
        "non_public_metrics.url_link_clicks": "url_link_clicks",
        "non_public_metrics.user_profile_clicks": "user_profile_clicks",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    for col in ["hashtags", "mentions", "urls", "cashtags"]:
        src = f"entities.{col}"
        if src in df.columns:
            df[col] = df[src]
        elif col not in df.columns:
            df[col] = [[] for _ in range(len(df))]

    if "sentiment_score" not in df.columns or df["sentiment_score"].isna().all():
        df["sentiment_score"] = compute_sentiment_series(df.get("content", pd.Series([""]*len(df))), df.get("lang"), use_vader=use_vader)
    if "sentiment_label" not in df.columns:
        df["sentiment_label"] = df["sentiment_score"].apply(label_from_score)
    return df


def normalize_reddit(path: str, limit: Optional[int] = None, use_vader: bool = True) -> pd.DataFrame:
    raw = safe_json_load(path, limit=limit)
    if not isinstance(raw, list):
        raise ValueError("Reddit JSON must be a list.")
    df = pd.json_normalize(raw, sep=".")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    elif "created_utc" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_utc"], unit="s", utc=True, errors="coerce")
    else:
        raise ValueError("Reddit missing created_at/created_utc.")
    df["date"] = df["created_at"].dt.date

    if "sentiment_score" not in df.columns or df["sentiment_score"].isna().all():
        text = (df.get("title", "").astype(str) + " " + df.get("selftext", "").fillna("").astype(str))
        df["sentiment_score"] = compute_sentiment_series(text, None, use_vader=use_vader)
    if "sentiment_label" not in df.columns:
        df["sentiment_label"] = df["sentiment_score"].apply(label_from_score)
    return df


def normalize_news(path: str, limit: Optional[int] = None, use_vader: bool = True) -> pd.DataFrame:
    raw = safe_json_load(path, limit=limit)
    if not isinstance(raw, list):
        raise ValueError("News JSON must be a list.")
    df = pd.json_normalize(raw, sep=".")
    if "published_at" not in df.columns:
        raise ValueError("News missing published_at.")
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    df["date"] = df["published_at"].dt.date

    if "sentiment_score" not in df.columns or df["sentiment_score"].isna().all():
        text = (df.get("title", "").astype(str) + " " + df.get("content", "").fillna("").astype(str))
        df["sentiment_score"] = compute_sentiment_series(text, None, use_vader=use_vader)
    if "sentiment_label" not in df.columns:
        df["sentiment_label"] = df["sentiment_score"].apply(label_from_score)
    return df


def normalize_reviews(path: str, limit: Optional[int] = None, use_vader: bool = True) -> pd.DataFrame:
    raw = safe_json_load(path, limit=limit)
    if not isinstance(raw, list):
        raise ValueError("Reviews JSON must be a list.")
    df = pd.json_normalize(raw, sep=".")
    if "created_at" not in df.columns:
        raise ValueError("Reviews missing created_at.")
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["date"] = df["created_at"].dt.date
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0).astype(int)

    if "sentiment_score" not in df.columns or df["sentiment_score"].isna().all():
        text = (df.get("title", "").astype(str) + " " + df.get("body", "").fillna("").astype(str))
        df["sentiment_score"] = compute_sentiment_series(text, df.get("language"), use_vader=use_vader)
    if "sentiment_label" not in df.columns:
        df["sentiment_label"] = df["sentiment_score"].apply(label_from_score)
    return df


def normalize_stock(path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    raw = safe_json_load(path)
    prices = pd.json_normalize(raw.get("prices", []))
    if "date" not in prices.columns:
        raise ValueError("Stock prices missing date.")
    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce").dt.date
    earnings = pd.json_normalize(raw.get("earnings", []))
    if not earnings.empty and "date" in earnings.columns:
        earnings["date"] = pd.to_datetime(earnings["date"], utc=True, errors="coerce").dt.date
    actions = pd.json_normalize(raw.get("corporate_actions", []))
    if not actions.empty and "date" in actions.columns:
        actions["date"] = pd.to_datetime(actions["date"], utc=True, errors="coerce").dt.date
    meta = raw.get("meta", {})
    trend_plan = raw.get("trend_plan", [])
    return prices, earnings, actions, {"meta": meta, "trend_plan": trend_plan}


# -------------------------- Manifest & Preset --------------------------

def load_manifest(manifest_path: str) -> Dict[str, Dict[str, str]]:
    man = safe_json_load(manifest_path)
    mapping = {}
    for entry in man.get("merchants", []):
        name = str(entry.get("merchant"))
        paths = entry.get("paths", {})
        mapping[name] = {k: posix_path(v) for k, v in paths.items()}
    return mapping


def load_preset(preset_path: Optional[str]) -> Dict[str, Any]:
    if not preset_path:
        return {"global": {}, "merchants": []}
    return safe_json_load(preset_path)


def preset_block_for(preset: Dict[str, Any], merchant: str) -> Dict[str, Any]:
    for m in preset.get("merchants", []):
        if str(m.get("merchant_name")).lower() == merchant.lower():
            return m
    return {}


# -------------------------- Aggregation --------------------------

def zscore(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return s * 0.0
    return (s - mu) / sd


def cross_channel_daily(
    tweets: Optional[pd.DataFrame],
    reddit: Optional[pd.DataFrame],
    news: Optional[pd.DataFrame],
    reviews: Optional[pd.DataFrame],
    stock_prices: Optional[pd.DataFrame]
) -> pd.DataFrame:
    idx = None
    cols = {}
    # Tweets
    if tweets is not None and not tweets.empty:
        tid_col = "tweet_id" if "tweet_id" in tweets.columns else ("content" if "content" in tweets.columns else None)
        t_daily = tweets.groupby("date").agg(
            tweets_count=(tid_col, "count"),
            tweets_sent=("sentiment_score", "mean"),
            tweets_likes=("like_count", "sum") if "like_count" in tweets.columns else (tid_col, "count"),
            tweets_impressions=("impression_count", "sum") if "impression_count" in tweets.columns else (tid_col, "count")
        )
        cols["tweets"] = t_daily
        idx = t_daily.index if idx is None else idx.union(t_daily.index)
    # Reddit
    if reddit is not None and not reddit.empty:
        rid_col = "id" if "id" in reddit.columns else ("title" if "title" in reddit.columns else None)
        r_daily = reddit.groupby("date").agg(
            reddit_count=(rid_col, "count"),
            reddit_sent=("sentiment_score", "mean"),
            reddit_comments=("num_comments", "sum") if "num_comments" in reddit.columns else (rid_col, "count"),
            reddit_ups=("ups", "sum") if "ups" in reddit.columns else (rid_col, "count")
        )
        cols["reddit"] = r_daily
        idx = r_daily.index if idx is None else idx.union(r_daily.index)
    # News
    if news is not None and not news.empty:
        n_daily = news.groupby("date").agg(
            news_count=("title", "count"),
            news_sent=("sentiment_score", "mean"),
            news_pageviews=("pageviews", "sum") if "pageviews" in news.columns else ("title", "count"),
            news_shares=("shares", "sum") if "shares" in news.columns else ("title", "count")
        )
        cols["news"] = n_daily
        idx = n_daily.index if idx is None else idx.union(n_daily.index)
    # Reviews
    if reviews is not None and not reviews.empty:
        v_daily = reviews.groupby("date").agg(
            reviews_count=("rating", "count"),
            reviews_avg_rating=("rating", "mean"),
            reviews_sent=("sentiment_score", "mean"),
            reviews_helpful=("helpful_votes", "sum") if "helpful_votes" in reviews.columns else ("rating", "count")
        )
        cols["reviews"] = v_daily
        idx = v_daily.index if idx is None else idx.union(v_daily.index)
    # Stock
    if stock_prices is not None and not stock_prices.empty:
        s_daily = stock_prices.set_index("date")[["open", "high", "low", "close", "volume", "return_pct", "volatility_day"]].copy()
        cols["stock"] = s_daily
        idx = s_daily.index if idx is None else idx.union(s_daily.index)

    if idx is None:
        return pd.DataFrame()

    idx = pd.Index(sorted(idx), name="date")
    out = pd.DataFrame(index=idx)
    for _, df in cols.items():
        out = out.join(df, how="left")

    fill_zero_cols = ["tweets_count", "reddit_count", "news_count", "news_pageviews", "reviews_count", "reddit_comments", "reddit_ups", "tweets_likes", "tweets_impressions", "news_shares", "reviews_helpful"]
    for c in fill_zero_cols:
        if c in out.columns:
            out[c] = out[c].fillna(0.0)

    # Buzz index: counts + pageviews + engagement contributions
    z_components = []
    weights = []
    for c, w in [
        ("tweets_count", 1.0),
        ("reddit_count", 1.0),
        ("news_count", 1.0),
        ("news_pageviews", 0.7),
        ("tweets_likes", 0.3),
        ("reddit_comments", 0.4),
        ("reviews_count", 0.5),
    ]:
        if c in out.columns:
            z_components.append(zscore(out[c]))
            weights.append(w)
    if z_components:
        w = np.array(weights, dtype=float)
        w = w / (w.sum() if w.sum() != 0 else 1)
        out["buzz_index"] = np.average(np.vstack(z_components), axis=0, weights=w)

    out.index.name = "date"
    daily = out.reset_index()
    if "date" not in daily.columns and "index" in daily.columns:
        daily = daily.rename(columns={"index": "date"})
    return daily


def lagged_cross_corr(x: pd.Series, y: pd.Series, max_lag: int = 7) -> pd.DataFrame:
    out = []
    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            xs = x.shift(lag); ys = y
        else:
            xs = x; ys = y.shift(-lag)
        valid = xs.notna() & ys.notna()
        c = xs[valid].corr(ys[valid]) if valid.sum() >= 3 else np.nan
        out.append({"lag": lag, "corr": c})
    return pd.DataFrame(out)


# -------------------------- Plotly charts --------------------------

def fig_stock_with_events(prices: pd.DataFrame, earnings: pd.DataFrame, actions: pd.DataFrame, trend_plan: List[Dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    if set(["open", "high", "low", "close"]).issubset(set(prices.columns)):
        fig.add_trace(go.Candlestick(
            x=prices["date"], open=prices["open"], high=prices["high"], low=prices["low"], close=prices["close"],
            name="OHLC", increasing_line_color="#2ca02c", decreasing_line_color="#d62728", opacity=0.8
        ))
    else:
        fig.add_trace(go.Scatter(x=prices["date"], y=prices["close"], name="Close", mode="lines", line=dict(color="#1f77b4")))

    for col, name, color in [("ma20", "MA20", "#ff7f0e"), ("ma50", "MA50", "#2ca02c"), ("ma200", "MA200", "#9467bd")]:
        if col in prices.columns and prices[col].notna().any():
            fig.add_trace(go.Scatter(x=prices["date"], y=prices[col], name=name, mode="lines", line=dict(color=color, width=1.2)))

    if "bb20_upper" in prices.columns and "bb20_lower" in prices.columns:
        fig.add_trace(go.Scatter(x=prices["date"], y=prices["bb20_upper"], name="BB Upper", line=dict(color="#1f77b4", width=0.8), opacity=0.5))
        fig.add_trace(go.Scatter(x=prices["date"], y=prices["bb20_lower"], name="BB Lower", line=dict(color="#1f77b4", width=0.8), opacity=0.5, fill="tonexty", fillcolor="rgba(31,119,180,0.1)"))

    if earnings is not None and not earnings.empty:
        fig.add_trace(go.Scatter(
            x=earnings["date"], y=[prices["close"].max()*1.01]*len(earnings),
            mode="markers", marker=dict(color="#d62728", symbol="x", size=10), name="Earnings"
        ))
    if actions is not None and not actions.empty:
        divs = actions[actions.get("type") == "dividend"]
        if not divs.empty:
            fig.add_trace(go.Scatter(
                x=divs["date"], y=[prices["close"].max()*0.99]*len(divs),
                mode="markers", marker=dict(color="#8c564b", symbol="triangle-up", size=9), name="Dividend"
            ))

    for e in (trend_plan or []):
        try:
            m = str(e.get("month")); lab = e.get("label", "")
            fig.add_shape(type="line", x0=month_midpoint(m), x1=month_midpoint(m), y0=0, y1=1,
                          xref="x", yref="paper", line=dict(color="gray", dash="dot", width=1))
            fig.add_annotation(x=month_midpoint(m), y=1, yref="paper", text=lab, showarrow=False, yanchor="bottom", font=dict(size=9), textangle=90, opacity=0.7)
        except Exception:
            pass

    fig.update_layout(height=450, template="plotly_white", title="Stock price with events",
                      xaxis_title="Date", yaxis_title="Price")
    return fig


def fig_buzz_vs_price(daily: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "buzz_index" in daily.columns:
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["buzz_index"], name="Buzz index (z)", mode="lines", line=dict(color="#2ca02c")))
    if "close" in daily.columns:
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["close"], name="Close", mode="lines", yaxis="y2", line=dict(color="#1f77b4")))
    fig.update_layout(height=400, template="plotly_white", title="Buzz vs Price",
                      xaxis=dict(title="Date"), yaxis=dict(title="Buzz (z-score)"),
                      yaxis2=dict(title="Price", overlaying="y", side="right"))
    return fig


def fig_correlation_heatmap(daily: pd.DataFrame) -> Optional[go.Figure]:
    cols = [c for c in ["tweets_count", "reddit_count", "news_count", "news_pageviews", "tweets_likes", "reddit_comments", "reviews_count", "reviews_avg_rating", "buzz_index", "return_pct", "volume", "volatility_day", "close"] if c in daily.columns]
    if len(cols) < 3:
        return None
    df = daily[cols].copy().dropna(how="all")
    corr = df.corr()
    fig = go.Figure(data=go.Heatmap(z=corr.values, x=corr.columns, y=corr.index, colorscale="RdBu", zmin=-1, zmax=1, colorbar=dict(title="corr")))
    fig.update_layout(height=450, template="plotly_white", title="Cross-channel correlation matrix")
    return fig


def fig_lead_lag(daily: pd.DataFrame, max_lag: int = 7) -> Optional[go.Figure]:
    if "buzz_index" not in daily.columns or "return_pct" not in daily.columns:
        return None
    ll = lagged_cross_corr(daily["buzz_index"], daily["return_pct"], max_lag=max_lag)
    fig = px.bar(ll, x="lag", y="corr", title="Lead/Lag: Buzz (x leads stock returns when lag>0)")
    fig.add_hline(y=0, line_color="gray")
    fig.update_layout(template="plotly_white", height=300)
    return fig


# -------------------------- Streamlit app --------------------------

st.set_page_config(page_title="Merchant Risk Analytics", layout="wide")

st.sidebar.title("Data sources")
# Auto-find the latest manifest file
default_manifest = ""
try:
    manifest_files = glob.glob('output/main_manifest_*.json')
    if manifest_files:
        default_manifest = max(manifest_files, key=os.path.getctime)
except Exception:
    pass

default_preset = "preset.json" if os.path.exists("preset.json") else ""
manifest_path = st.sidebar.text_input("Manifest path", value=default_manifest, help="Path to main manifest_*.json from main_data.py")
preset_path = st.sidebar.text_input("Preset path (optional)", value=default_preset)

st.sidebar.markdown("---")
st.sidebar.markdown("Performance (sampling)")
limit_tweets = st.sidebar.number_input("Tweets max rows", min_value=1000, max_value=500000, value=10000, step=5000)
limit_reddit = st.sidebar.number_input("Reddit max rows", min_value=1000, max_value=200000, value=30000, step=5000)
limit_news = st.sidebar.number_input("News max rows", min_value=1000, max_value=200000, value=20000, step=5000)
limit_reviews = st.sidebar.number_input("Reviews max rows", min_value=1000, max_value=200000, value=20000, step=5000)

st.sidebar.markdown("---")
disable_vader = st.sidebar.checkbox("Disable VADER (use keyword fallback)", value=(SIA is None))
use_vader = (not disable_vader) and (SIA is not None)

st.sidebar.markdown("---")
show_tables = st.sidebar.checkbox("Show sample tables", value=False)
max_table_rows = st.sidebar.slider("Max table rows to display", 10, 200, 50, 10)

st.title("Merchant Risk Analytics & Preset Editor")

if not manifest_path or not os.path.exists(manifest_path):
    st.info("Enter a valid manifest path to begin (e.g., output/main_manifest_*.json).")
    st.stop()

@st.cache_data(show_spinner=False)
def cache_manifest(path: str):
    return load_manifest(path)

@st.cache_data(show_spinner=False)
def cache_preset(path: Optional[str]):
    return load_preset(path)

manifest_map = cache_manifest(manifest_path)
preset = cache_preset(preset_path if preset_path else None)

if not manifest_map:
    st.error("Manifest has no merchants.")
    st.stop()

merchant_names = sorted(manifest_map.keys(), key=lambda x: x.lower())
selected_merchants = st.multiselect("Select merchants", merchant_names, default=merchant_names[: min(3, len(merchant_names))])

# Portfolio overview
with st.expander("Portfolio overview (from manifest + preset)", expanded=True):
    rows = []
    for m in merchant_names:
        pblock = preset_block_for(preset, m)
        g = preset.get("global", {})
        rows.append({
            "merchant": m,
            "start_date": pblock.get("start_date", g.get("start_date", "")),
            "end_date": pblock.get("end_date", g.get("end_date", "")),
            "tweets_n": pblock.get("tweets", {}).get("n_tweets"),
            "reddit_n": pblock.get("reddit", {}).get("n_posts"),
            "news_n": pblock.get("news", {}).get("n_articles"),
            "reviews_n": pblock.get("reviews", {}).get("n_reviews"),
            "products": pblock.get("reviews", {}).get("n_products"),
            "stock_ticker": pblock.get("stock", {}).get("ticker"),
            "stock_base": pblock.get("stock", {}).get("base_price"),
            "stock_sigma": pblock.get("stock", {}).get("sigma_annual"),
            "stock_ret_target": pblock.get("stock", {}).get("target_total_return"),
        })
    df_port = pd.DataFrame(rows)
    st.dataframe(df_port, height=320)

st.markdown("---")

for merchant in selected_merchants:
    st.header(f"Merchant: {merchant}")
    paths = manifest_map[merchant]
    pblock = preset_block_for(preset, merchant)

    colA, colB, colC = st.columns([1,1,1])
    with colA:
        st.caption("Files")
        st.write({k: os.path.basename(v) for k, v in paths.items()})

    with colB:
        with st.expander("Preset summary", expanded=False):
            st.json({
                "range": [
                    pblock.get("start_date", preset.get("global", {}).get("start_date")),
                    pblock.get("end_date", preset.get("global", {}).get("end_date"))
                ],
                "tweets": pblock.get("tweets", {}),
                "reddit": pblock.get("reddit", {}),
                "news": pblock.get("news", {}),
                "reviews": pblock.get("reviews", {}),
                "stock": pblock.get("stock", {}),
            })

    with colC:
        st.caption("Sampling")
        st.write({"tweets": int(limit_tweets), "reddit": int(limit_reddit), "news": int(limit_news), "reviews": int(limit_reviews)})
        st.caption("Sentiment engine")
        st.write("VADER" if use_vader else "Keyword fallback")

    @st.cache_data(show_spinner=True)
    def load_all(paths: Dict[str, str], limits: Dict[str, int], use_vader_local: bool):
        tweets = reddit = news = reviews = None
        prices = earnings = actions = None
        stock_meta = {}
        try:
            if "tweets" in paths and os.path.exists(paths["tweets"]):
                tweets = normalize_tweets(paths["tweets"], limit=limits["tweets"], use_vader=use_vader_local)
        except Exception as e:
            st.warning(f"Tweets load error: {e}")
        try:
            if "reddit" in paths and os.path.exists(paths["reddit"]):
                reddit = normalize_reddit(paths["reddit"], limit=limits["reddit"], use_vader=use_vader_local)
        except Exception as e:
            st.warning(f"Reddit load error: {e}")
        try:
            if "news" in paths and os.path.exists(paths["news"]):
                news = normalize_news(paths["news"], limit=limits["news"], use_vader=use_vader_local)
        except Exception as e:
            st.warning(f"News load error: {e}")
        try:
            if "reviews" in paths and os.path.exists(paths["reviews"]):
                reviews = normalize_reviews(paths["reviews"], limit=limits["reviews"], use_vader=use_vader_local)
        except Exception as e:
            st.warning(f"Reviews load error: {e}")
        try:
            if "stock" in paths and os.path.exists(paths["stock"]):
                prices, earnings, actions, stock_meta = normalize_stock(paths["stock"])
        except Exception as e:
            st.warning(f"Stock load error: {e}")
        return tweets, reddit, news, reviews, prices, earnings, actions, stock_meta

    tweets, reddit, news, reviews, prices, earnings, actions, stock_meta = load_all(paths, {
        "tweets": int(limit_tweets), "reddit": int(limit_reddit), "news": int(limit_news), "reviews": int(limit_reviews)
    }, use_vader)

    daily = cross_channel_daily(tweets, reddit, news, reviews, prices)

    # Date filtering
    if not daily.empty:
        start_d, end_d = daily["date"].min(), daily["date"].max()
        date_range = st.slider(f"{merchant} date range", min_value=start_d, max_value=end_d, value=(start_d, end_d))
        mask = (daily["date"] >= date_range[0]) & (daily["date"] <= date_range[1])
        daily_view = daily.loc[mask].copy()
        prices_view = prices.loc[(prices["date"] >= date_range[0]) & (prices["date"] <= date_range[1])] if prices is not None and not prices.empty else pd.DataFrame()
        earnings_view = earnings.loc[(earnings["date"] >= date_range[0]) & (earnings["date"] <= date_range[1])] if earnings is not None and not earnings.empty else pd.DataFrame()
        actions_view = actions.loc[(actions["date"] >= date_range[0]) & (actions["date"] <= date_range[1])] if actions is not None and not actions.empty else pd.DataFrame()
    else:
        daily_view = daily
        prices_view = prices
        earnings_view = earnings
        actions_view = actions

    tabs = st.tabs(["Overview", "Social (Tweets/Reddit)", "News", "Reviews", "Stock", "Preset editor"])

    # Overview
    with tabs[0]:
        kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
        with kpi1:
            st.metric("Tweets", len(tweets) if tweets is not None else 0)
        with kpi2:
            st.metric("Reddit posts", len(reddit) if reddit is not None else 0)
        with kpi3:
            st.metric("News articles", len(news) if news is not None else 0)
        with kpi4:
            st.metric("Reviews", len(reviews) if reviews is not None else 0)
        with kpi5:
            st.metric("Stock days", len(prices) if prices is not None and not prices.empty else 0)

    col1, col2 = st.columns([2,1])
    with col1:
        if prices_view is not None and not prices_view.empty:
            trend_stock = pblock.get("stock", {}).get("trend_plan", []) if pblock else stock_meta.get("trend_plan", [])
            st.plotly_chart(
                fig_stock_with_events(
                    prices_view if not prices_view.empty else prices,
                    earnings_view,
                    actions_view,
                    trend_stock
                ),
                width="stretch",
                key=f"stock_chart_{hash(prices_view.to_string())}"  # unique per prices_view content
            )

        if not daily_view.empty:
            st.plotly_chart(
                fig_buzz_vs_price(daily_view),
                width="stretch",
                key=f"buzz_chart_{hash(daily_view.to_string())}"  # unique per daily_view content
            )

        with col2:
            if not daily_view.empty:
                fig_corr = fig_correlation_heatmap(daily_view)
                if fig_corr is not None:
                    st.plotly_chart(fig_corr, width="stretch")
                ll = fig_lead_lag(daily_view, max_lag=7)
                if ll is not None:
                    st.plotly_chart(ll, width="stretch")

        if show_tables and not daily_view.empty:
            st.subheader("Daily metrics (sample)")
            st.dataframe(daily_view.tail(max_table_rows))

    # Social
    with tabs[1]:
        c1, c2 = st.columns(2)
        if tweets is not None and not tweets.empty:
            with c1:
                st.subheader("Tweets")
                by_label = tweets["sentiment_label"].value_counts()
                st.plotly_chart(px.bar(by_label, title="Tweet sentiment counts"), width="stretch")
                if "hashtags" in tweets.columns:
                    tags = []
                    for lst in tweets["hashtags"].dropna():
                        if isinstance(lst, list):
                            tags.extend([t for t in lst if t])
                    if len(tags):
                        tags_s = pd.Series(tags).value_counts().head(20)
                        st.plotly_chart(px.bar(tags_s, title="Top hashtags"), width="stretch")
                if show_tables:
                    st.dataframe(tweets.sample(min(max_table_rows, len(tweets))))
        if reddit is not None and not reddit.empty:
            with c2:
                st.subheader("Reddit")
                by_label_r = reddit["sentiment_label"].value_counts()
                st.plotly_chart(px.bar(by_label_r, title="Reddit sentiment counts"), width="stretch")
                if "subreddit" in reddit.columns:
                    subs = reddit["subreddit"].astype(str).value_counts().head(20)
                    st.plotly_chart(px.bar(subs, title="Top subreddits"), width="stretch")
                if show_tables:
                    st.dataframe(reddit.sample(min(max_table_rows, len(reddit))))

    # News
    with tabs[2]:
        if news is not None and not news.empty:
            st.subheader("News")
            if "pageviews" in news.columns:
                fig = px.scatter(news, x="sentiment_score", y="pageviews",
                                 color="publisher" if "publisher" in news.columns else None,
                                 title="News sentiment vs pageviews", opacity=0.6)
                st.plotly_chart(fig, width="stretch")
            if "publisher" in news.columns:
                pubs = news["publisher"].astype(str).value_counts().head(20)
                st.plotly_chart(px.bar(pubs, title="Top publishers"), width="stretch")
            if show_tables:
                st.dataframe(news.sample(min(max_table_rows, len(news))))
        else:
            st.info("No news data available.")

    # Reviews
    with tabs[3]:
        if reviews is not None and not reviews.empty:
            st.subheader("Reviews")
            if "rating" in reviews.columns:
                st.plotly_chart(px.histogram(reviews, x="rating", nbins=5, title="Ratings distribution"), width="stretch")
            if "rating" in reviews.columns and "date" in reviews.columns:
                rv = reviews.copy()
                rv["date_dt"] = pd.to_datetime(rv["date"], errors="coerce")
                rv["month"] = rv["date_dt"].dt.to_period("M").dt.to_timestamp(how="S")
                rev_m = rv.groupby("month").agg(avg_rating=("rating","mean"), count=("rating","count")).reset_index()
                if not rev_m.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=rev_m["month"], y=rev_m["avg_rating"], mode="lines+markers", name="Avg rating", line=dict(color="#d62728")))
                    fig.add_trace(go.Bar(x=rev_m["month"], y=rev_m["count"], name="Count", yaxis="y2", marker_color="#9edae5", opacity=0.5))
                    fig.update_layout(title="Average rating & volume (monthly)",
                                      yaxis=dict(title="Avg rating"),
                                      yaxis2=dict(title="Reviews", overlaying="y", side="right"),
                                      template="plotly_white")
                    st.plotly_chart(fig, width="stretch")
            if "product_name" in reviews.columns:
                top_prod = reviews.groupby("product_name").agg(count=("rating","count"), avg=("rating","mean")).reset_index().sort_values("count", ascending=False).head(20)
                if not top_prod.empty:
                    figp = go.Figure()
                    figp.add_trace(go.Bar(x=top_prod["product_name"], y=top_prod["count"], name="Count", marker_color="#9edae5"))
                    figp.add_trace(go.Scatter(x=top_prod["product_name"], y=top_prod["avg"], mode="lines+markers", name="Avg rating", yaxis="y2", line=dict(color="#d62728")))
                    figp.update_layout(title="Top products by reviews and average rating", xaxis_tickangle=-45,
                                       yaxis=dict(title="Reviews"), yaxis2=dict(title="Avg rating", overlaying="y", side="right"),
                                       template="plotly_white")
                    st.plotly_chart(figp, width="stretch")
            if show_tables:
                st.dataframe(reviews.sample(min(max_table_rows, len(reviews))))
        else:
            st.info("No reviews data available.")

    # Stock
    with tabs[4]:
        if prices is not None and not prices.empty:
            trend_stock = pblock.get("stock", {}).get("trend_plan", []) if pblock else stock_meta.get("trend_plan", [])
            
            # Stock main chart
            st.plotly_chart(
                fig_stock_with_events(
                    prices_view if not prices_view.empty else prices,
                    earnings_view,
                    actions_view,
                    trend_stock
                ),
                width="stretch",
                key=f"stock_chart_main_{hash(prices.to_string())}"  # unique per data
            )

            # Daily return histogram
            if "return_pct" in prices.columns:
                st.plotly_chart(
                    px.histogram(prices, x="return_pct", nbins=60, title="Daily return distribution"),
                    width="stretch",
                    key=f"hist_return_{hash(prices.to_string())}"
                )

            # Daily volume line chart
            if "volume" in prices.columns:
                st.plotly_chart(
                    px.line(prices, x="date", y="volume", title="Daily volume"),
                    width="stretch",
                    key=f"line_volume_{hash(prices.to_string())}"
                )

            # Event window analysis
            st.subheader("Event window analysis (stock trend plan)")
            ev_df = pd.DataFrame(pblock.get("stock", {}).get("trend_plan", []))
            if not ev_df.empty and "month" in ev_df.columns:
                window = st.slider(
                    "Event window (days before/after midpoint)", 1, 15, 5, 1, key=f"win_{merchant}"
                )
                returns_list = []
                for _, row in ev_df.iterrows():
                    m = str(row.get("month"))
                    mid = month_midpoint(m).date()
                    mask = (prices["date"] >= (mid - timedelta(days=window))) & (prices["date"] <= (mid + timedelta(days=window)))
                    seg = prices.loc[mask, ["date","return_pct"]].copy()
                    seg["rel_day"] = (pd.to_datetime(seg["date"]) - pd.Timestamp(mid)).dt.days
                    returns_list.append(seg[["rel_day","return_pct"]])
                if returns_list:
                    cat = pd.concat(returns_list, ignore_index=True)
                    agg = cat.groupby("rel_day")["return_pct"].mean().reset_index()
                    st.plotly_chart(
                        px.bar(agg, x="rel_day", y="return_pct", title="Avg return around events"),
                        width="stretch",
                        key=f"event_window_{hash(cat.to_string())}"
                    )
            else:
                st.info("No stock trend plan in preset for event window analysis.")
        else:
            st.info("No stock data available.")


    # Preset editor
    with tabs[5]:
        st.subheader("Preset editor (this merchant)")
        st.caption("Edit trend plans and key fields. Download a patch JSON to apply later.")
        stream_names = ["tweets", "reddit", "news", "reviews", "stock"]
        edited_blocks = {}
        col_ed1, col_ed2 = st.columns(2)
        for i, stream in enumerate(stream_names):
            container = col_ed1 if i % 2 == 0 else col_ed2
            with container:
                st.markdown(f"#### {stream.capitalize()} trend plan")
                plan = pblock.get(stream, {}).get("trend_plan", [])
                dfp = pd.DataFrame(plan) if isinstance(plan, list) else pd.DataFrame(columns=["month","intensity","label"])
                if dfp.empty:
                    dfp = pd.DataFrame(columns=["month","intensity","label"])
                edited = st.data_editor(dfp, num_rows="dynamic", key=f"edit_{merchant}_{stream}")
                edited_blocks[stream] = edited.to_dict(orient="records")

        st.markdown("#### Basic fields")
        basic_cols = st.columns(3)

        with basic_cols[0]:
            start_new = st.text_input(
                "start_date",
                value=pblock.get("start_date", preset.get("global", {}).get("start_date", "2020-01-01")),
                key=f"start_date_{merchant}"  # unique key
            )
            end_new = st.text_input(
                "end_date",
                value=pblock.get("end_date", preset.get("global", {}).get("end_date", "2024-12-31")),
                key=f"end_date_{merchant}"
            )

        with basic_cols[1]:
            ticker_new = st.text_input(
                "stock.ticker",
                value=pblock.get("stock", {}).get("ticker", ""),
                key=f"ticker_{merchant}"
            )
            base_price_new = st.number_input(
                "stock.base_price",
                value=float(pblock.get("stock", {}).get("base_price", 100.0)),
                key=f"base_price_{merchant}"
            )

        with basic_cols[2]:
            sigma_new = st.number_input(
                "stock.sigma_annual",
                value=float(pblock.get("stock", {}).get("sigma_annual", 0.35)),
                key=f"sigma_annual_{merchant}"
            )
            ret_target_new = st.number_input(
                "stock.target_total_return",
                value=float(pblock.get("stock", {}).get("target_total_return", 0.1)),
                key=f"target_return_{merchant}"
            )

        patch_block = {
            "merchant_name": merchant,
            "start_date": start_new,
            "end_date": end_new,
            "tweets": {"trend_plan": edited_blocks["tweets"]},
            "reddit": {"trend_plan": edited_blocks["reddit"]},
            "news": {"trend_plan": edited_blocks["news"]},
            "reviews": {"trend_plan": edited_blocks["reviews"]},
            "stock": {
                "ticker": ticker_new,
                "base_price": base_price_new,
                "sigma_annual": sigma_new,
                "target_total_return": ret_target_new,
                "trend_plan": edited_blocks["stock"]
            }
        }
        patch_json = json.dumps({"merchants": [patch_block]}, indent=2)
        st.code(patch_json, language="json")
        st.download_button("Download patch JSON", data=patch_json, file_name=f"preset_patch_{slugify(merchant)}.json", mime="application/json")

st.markdown("---")
st.caption("Tip: Adjust sampling sizes for very large JSON files. Install ijson to stream the first N records efficiently. Use the preset editor to tweak trend plans and download a patch JSON for batch regeneration.")


# py -3 -m streamlit run '.\visualize_data.py'
# python3 .\visualize_data.py analyze --manifest .\output\output\main_manifest_20250918_012937.json --preset .\preset.json --outdir .\analysis