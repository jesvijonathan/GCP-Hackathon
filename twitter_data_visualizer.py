import argparse
import json
import os
import math
import warnings
from datetime import datetime
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

# Headless backend for servers/CI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# Optional sentiment (VADER)
try:
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    try:
        _ = SentimentIntensityAnalyzer()
    except Exception:
        nltk.download("vader_lexicon")
    SIA = SentimentIntensityAnalyzer()
except Exception:
    SIA = None


warnings.filterwarnings("ignore", category=FutureWarning)


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON must be a list of Tweet-like objects.")
    return data


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def normalize_tweets(raw: list) -> pd.DataFrame:
    # Flatten most nested structures
    df = pd.json_normalize(raw, sep=".")
    # Rename columns for convenience if present
    rename_map = {
        "public_metrics.like_count": "like_count",
        "public_metrics.reply_count": "reply_count",
        "public_metrics.retweet_count": "retweet_count",
        "public_metrics.quote_count": "quote_count",
        "non_public_metrics.impression_count": "impression_count",
        "non_public_metrics.url_link_clicks": "url_link_clicks",
        "non_public_metrics.user_profile_clicks": "user_profile_clicks",
        "entities.hashtags": "hashtags",
        "entities.mentions": "mentions",
        "entities.urls": "urls",
        "entities.cashtags": "cashtags",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # Coerce types and defaults
    # created_at to pandas datetime (UTC)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    else:
        raise ValueError("created_at field is missing.")

    # Ensure lists
    for col in ["hashtags", "mentions", "urls", "cashtags", "context_annotations", "referenced_tweets", "attachments_media_keys", "edit_history_tweet_ids"]:
        if col not in df.columns:
            df[col] = [[] for _ in range(len(df))]
        df[col] = df[col].apply(ensure_list)

    # Numeric metrics with defaults
    for col in ["like_count", "reply_count", "retweet_count", "quote_count", "impression_count", "url_link_clicks", "user_profile_clicks"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Possibly sensitive boolean
    if "possibly_sensitive" not in df.columns:
        df["possibly_sensitive"] = False
    df["possibly_sensitive"] = df["possibly_sensitive"].fillna(False).astype(bool)

    # Language
    if "lang" not in df.columns:
        df["lang"] = "ENG"
    df["lang"] = df["lang"].fillna("ENG").astype(str)

    # Flatten context annotations into domain/entity lists
    def extract_context_domains(x):
        # x is list of {domain: {...}, entity: {...}}
        out = []
        for item in ensure_list(x):
            try:
                name = (item.get("domain") or {}).get("name")
                if name:
                    out.append(str(name))
            except Exception:
                continue
        return out

    def extract_context_entities(x):
        out = []
        for item in ensure_list(x):
            try:
                name = (item.get("entity") or {}).get("name")
                if name:
                    out.append(str(name))
            except Exception:
                continue
        return out

    df["context_domains"] = df["context_annotations"].apply(extract_context_domains)
    df["context_entities"] = df["context_annotations"].apply(extract_context_entities)

    # Count entity lengths
    df["n_hashtags"] = df["hashtags"].apply(len)
    df["n_mentions"] = df["mentions"].apply(len)
    df["n_urls"] = df["urls"].apply(len)
    df["n_cashtags"] = df["cashtags"].apply(len)
    df["n_contexts"] = df["context_annotations"].apply(len)

    # Reference types: retweeted, replied_to, quoted
    def ref_type(x):
        items = ensure_list(x)
        if not items:
            return None
        t = items[0].get("type")
        return t if t in ("retweeted", "replied_to", "quoted") else None

    df["reference_type"] = df["referenced_tweets"].apply(ref_type)

    # Time helpers
    df["date"] = df["created_at"].dt.date
    df["month"] = df["created_at"].dt.to_period("M").astype(str)
    df["hour"] = df["created_at"].dt.hour
    df["weekday"] = df["created_at"].dt.day_name()

    # Derived engagement metrics
    df["eng_like_rate"] = np.where(df["impression_count"] > 0, df["like_count"] / df["impression_count"], np.nan)
    df["eng_reply_rate"] = np.where(df["impression_count"] > 0, df["reply_count"] / df["impression_count"], np.nan)
    df["eng_retweet_rate"] = np.where(df["impression_count"] > 0, df["retweet_count"] / df["impression_count"], np.nan)
    df["eng_quote_rate"] = np.where(df["impression_count"] > 0, df["quote_count"] / df["impression_count"], np.nan)
    df["ctr_url"] = np.where(df["impression_count"] > 0, df["url_link_clicks"] / df["impression_count"], np.nan)
    df["ctr_profile"] = np.where(df["impression_count"] > 0, df["user_profile_clicks"] / df["impression_count"], np.nan)

    return df


def compute_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    # If sentiments already embedded in JSON (unlikely in your last generator), use them
    if "sentiment_score" in df.columns:
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
        have = df["sentiment_score"].notna().any()
        if have:
            pass
        else:
            df.drop(columns=["sentiment_score"], inplace=True)
    if "sentiment_score" not in df.columns:
        # Compute using VADER if available; else simple fallback
        def basic_lexicon_score(text: str) -> float:
            # Very lightweight keyword-based scoring as fallback
            text_l = (text or "").lower()
            pos = ["great", "good", "love", "positive", "impressed", "excited", "promising", "strong"]
            neg = ["bad", "breach", "leak", "outage", "lawsuit", "concern", "disappointed", "scandal", "crisis", "recall", "fraud"]
            score = 0
            for w in pos:
                if w in text_l:
                    score += 0.2
            for w in neg:
                if w in text_l:
                    score -= 0.25
            return float(np.clip(score, -1.0, 1.0))

        scores = []
        if SIA is not None:
            for txt, lang in zip(df.get("content", [""] * len(df)), df.get("lang", ["ENG"] * len(df))):
                # Only score ENG strongly; non-ENG slightly attenuated
                text = str(txt or "")
                c = SIA.polarity_scores(text).get("compound", 0.0)
                if lang != "ENG":
                    c *= 0.6
                scores.append(c)
        else:
            scores = [basic_lexicon_score(str(t)) for t in df.get("content", [""] * len(df))]

        df["sentiment_score"] = pd.to_numeric(scores)

    # Label
    def label_sentiment(s):
        if s <= -0.2:
            return "negative"
        elif s >= 0.2:
            return "positive"
        else:
            return "neutral"

    df["sentiment_label"] = df["sentiment_score"].apply(label_sentiment)
    # Risk proxy 0..100 (higher when more negative)
    df["risk_score"] = (100.0 * (1.0 - (df["sentiment_score"] + 1.0) / 2.0)).round(2)
    return df


def top_n_counts(list_series: pd.Series, n=15) -> pd.DataFrame:
    counter = Counter()
    for lst in list_series.dropna():
        for item in ensure_list(lst):
            if not item:
                continue
            counter[item] += 1
    items, counts = zip(*counter.most_common(n)) if counter else ([], [])
    return pd.DataFrame({"item": items, "count": counts})


def group_time(df: pd.DataFrame):
    # Daily
    daily = df.set_index("created_at").resample("D").agg(
        count=("tweet_id", "count"),
        like_sum=("like_count", "sum"),
        retweet_sum=("retweet_count", "sum"),
        impression_sum=("impression_count", "sum"),
        sensitive=("possibly_sensitive", "sum"),
        avg_sentiment=("sentiment_score", "mean"),
        avg_ctr_url=("ctr_url", "mean"),
        avg_like_rate=("eng_like_rate", "mean"),
    ).reset_index()

    # 12-hour windows (aligned to midnight UTC)
    halfday = df.set_index("created_at").resample("12H").agg(
        count=("tweet_id", "count"),
        like_sum=("like_count", "sum"),
        retweet_sum=("retweet_count", "sum"),
        impression_sum=("impression_count", "sum"),
        avg_sentiment=("sentiment_score", "mean"),
    ).reset_index()

    # Monthly
    monthly = df.groupby("month").agg(
        count=("tweet_id", "count"),
        like_sum=("like_count", "sum"),
        retweet_sum=("retweet_count", "sum"),
        impression_sum=("impression_count", "sum"),
        avg_sentiment=("sentiment_score", "mean"),
    ).reset_index().sort_values("month")

    return daily, halfday, monthly


def detect_spikes(daily: pd.DataFrame, window=30, z_thresh=2.0):
    tmp = daily.copy()
    tmp["roll_mean"] = tmp["count"].rolling(window, min_periods=max(7, window // 4)).mean()
    tmp["roll_std"] = tmp["count"].rolling(window, min_periods=max(7, window // 4)).std()
    tmp["z"] = (tmp["count"] - tmp["roll_mean"]) / tmp["roll_std"]
    spikes = tmp[(tmp["z"] > z_thresh) & tmp["roll_std"].notna()].copy()
    spikes.sort_values("z", ascending=False, inplace=True)
    return tmp, spikes


def prepare_output_dir(outdir: str):
    os.makedirs(outdir, exist_ok=True)


def style():
    sns.set_theme(style="whitegrid", context="talk")


def plot_time_series_daily(daily: pd.DataFrame, spikes: pd.DataFrame, outdir: str):
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(daily["created_at"], daily["count"], color="#1f77b4", alpha=0.65, label="Daily tweets")
    roll = daily["count"].rolling(7, min_periods=3).mean()
    ax.plot(daily["created_at"], roll, color="#d62728", linewidth=2.0, label="7-day avg")
    # Annotate top 5 spikes
    for _, row in spikes.head(5).iterrows():
        ax.axvline(row["created_at"], color="#ff7f0e", linestyle="--", alpha=0.5)
        ax.annotate("Spike", xy=(row["created_at"], row["count"]), xytext=(10, 20),
                    textcoords="offset points", arrowprops=dict(arrowstyle="->", color="#ff7f0e"),
                    fontsize=10, color="#ff7f0e")
    ax.set_title("Daily tweet counts with 7-day average")
    ax.set_xlabel("Date")
    ax.set_ylabel("Tweets")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "timeseries_daily.png"), dpi=150)
    plt.close(fig)


def plot_halfday_series(halfday: pd.DataFrame, outdir: str):
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(halfday["created_at"], halfday["count"], color="#2ca02c", alpha=0.8, label="12-hour count")
    ax.set_title("12-hour tweet activity")
    ax.set_xlabel("Timestamp (UTC)")
    ax.set_ylabel("Tweets")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "timeseries_12h.png"), dpi=150)
    plt.close(fig)


def plot_monthly_sentiment(monthly: pd.DataFrame, outdir: str):
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.bar(monthly["month"], monthly["count"], color="#9edae5", alpha=0.7, label="Tweet count")
    ax1.set_ylabel("Tweets")
    ax1.set_xticks(range(0, len(monthly), max(1, len(monthly)//12)))
    ax1.tick_params(axis='x', rotation=45)
    ax2 = ax1.twinx()
    ax2.plot(monthly["month"], monthly["avg_sentiment"], color="#d62728", marker="o", label="Avg sentiment")
    ax2.set_ylabel("Avg sentiment (VADER compound)")
    ax1.set_title("Monthly volume and sentiment")
    # Combine legends
    lines, labels = [], []
    for ax in [ax1, ax2]:
        L = ax.get_legend_handles_labels()
        lines += L[0]; labels += L[1]
    ax1.legend(lines, labels, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "monthly_volume_sentiment.png"), dpi=150)
    plt.close(fig)


def plot_hashtag_bar(top_hashtags: pd.DataFrame, outdir: str, title="Top hashtags"):
    if top_hashtags.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=top_hashtags, y="item", x="count", ax=ax, color="#1f77b4")
    ax.set_title(title)
    ax.set_xlabel("Count")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "top_hashtags.png"), dpi=150)
    plt.close(fig)


def plot_mentions_bar(top_mentions: pd.DataFrame, outdir: str, title="Top mentions"):
    if top_mentions.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=top_mentions, y="item", x="count", ax=ax, color="#9467bd")
    ax.set_title(title)
    ax.set_xlabel("Count")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "top_mentions.png"), dpi=150)
    plt.close(fig)


def plot_lang_distribution(df: pd.DataFrame, outdir: str):
    fig, ax = plt.subplots(figsize=(8, 5))
    vc = df["lang"].value_counts().reset_index()
    vc.columns = ["lang", "count"]
    sns.barplot(data=vc, x="lang", y="count", ax=ax, palette="viridis")
    ax.set_title("Language distribution")
    ax.set_xlabel("Language")
    ax.set_ylabel("Tweets")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "language_distribution.png"), dpi=150)
    plt.close(fig)


def plot_hour_heatmap(df: pd.DataFrame, outdir: str):
    # Heatmap of volume by hour x weekday
    pivot = df.pivot_table(index="weekday", columns="hour", values="tweet_id", aggfunc="count").fillna(0)
    # Order weekdays
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = pivot.reindex(order)
    fig, ax = plt.subplots(figsize=(14, 5))
    sns.heatmap(pivot, cmap="YlGnBu", ax=ax)
    ax.set_title("Tweet volume by weekday/hour (UTC)")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Weekday")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "weekday_hour_heatmap.png"), dpi=150)
    plt.close(fig)


def plot_metrics_distributions(df: pd.DataFrame, outdir: str):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    sns.histplot(df["like_count"], bins=40, ax=axes[0], color="#1f77b4")
    axes[0].set_title("Like count distribution")
    axes[0].set_xlabel("Likes")
    axes[0].set_yscale("log")
    axes[0].set_xscale("log")

    sns.histplot(df["impression_count"], bins=40, ax=axes[1], color="#2ca02c")
    axes[1].set_title("Impression count distribution")
    axes[1].set_xlabel("Impressions")
    axes[1].set_yscale("log")
    axes[1].set_xscale("log")

    sns.histplot(df["eng_like_rate"].dropna(), bins=40, ax=axes[2], color="#d62728")
    axes[2].set_title("Engagement (likes / impressions)")
    axes[2].set_xlabel("Like rate")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "metrics_distributions.png"), dpi=150)
    plt.close(fig)


def plot_reference_breakdown(df: pd.DataFrame, outdir: str):
    vc = df["reference_type"].fillna("original").value_counts()
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.pie(vc.values, labels=vc.index, autopct="%1.1f%%", startangle=90, colors=sns.color_palette("Set2", n_colors=len(vc)))
    ax.set_title("Tweet type breakdown")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "reference_type_pie.png"), dpi=150)
    plt.close(fig)


def plot_sensitive_over_time(daily: pd.DataFrame, outdir: str):
    if "sensitive" not in daily.columns:
        return
    fig, ax = plt.subplots(figsize=(14, 5))
    rate = np.where(daily["count"] > 0, daily["sensitive"] / daily["count"], np.nan)
    ax.plot(daily["created_at"], rate, color="#8c564b", label="Sensitive rate")
    ax.set_ylim(0, max(0.25, np.nanmax(rate) * 1.1) if not np.isnan(rate).all() else 0.25)
    ax.set_title("Possibly sensitive tweets rate (daily)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Rate")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "sensitive_rate_daily.png"), dpi=150)
    plt.close(fig)


def save_csvs(df: pd.DataFrame, daily: pd.DataFrame, halfday: pd.DataFrame, monthly: pd.DataFrame, outdir: str):
    df.to_csv(os.path.join(outdir, "tweets_flat.csv"), index=False)
    daily.to_csv(os.path.join(outdir, "daily_metrics.csv"), index=False)
    halfday.to_csv(os.path.join(outdir, "halfday_metrics.csv"), index=False)
    monthly.to_csv(os.path.join(outdir, "monthly_metrics.csv"), index=False)
    top_hashtags = top_n_counts(df["hashtags"], n=30)
    top_mentions = top_n_counts(df["mentions"], n=30)
    top_domains = top_n_counts(df["context_domains"], n=30)
    top_entities = top_n_counts(df["context_entities"], n=30)
    top_hashtags.to_csv(os.path.join(outdir, "top_hashtags.csv"), index=False)
    top_mentions.to_csv(os.path.join(outdir, "top_mentions.csv"), index=False)
    top_domains.to_csv(os.path.join(outdir, "top_context_domains.csv"), index=False)
    top_entities.to_csv(os.path.join(outdir, "top_context_entities.csv"), index=False)


def print_and_save_summary(df: pd.DataFrame, daily: pd.DataFrame, monthly: pd.DataFrame, spikes: pd.DataFrame, outdir: str):
    summary = {
        "n_tweets": int(len(df)),
        "date_min": df["created_at"].min().isoformat() if len(df) else None,
        "date_max": df["created_at"].max().isoformat() if len(df) else None,
        "unique_authors": int(df["author_id"].nunique()) if "author_id" in df.columns else None,
        "possibly_sensitive_rate": float((df["possibly_sensitive"].mean() if len(df) else 0)),
        "avg_like": float(df["like_count"].mean() if len(df) else 0),
        "avg_impressions": float(df["impression_count"].mean() if len(df) else 0),
        "avg_eng_like_rate": float(df["eng_like_rate"].mean(skipna=True) if len(df) else 0),
        "avg_sentiment": float(df["sentiment_score"].mean(skipna=True) if "sentiment_score" in df.columns else float("nan")),
        "top_spikes_dates": [str(d.date()) for d in spikes["created_at"].head(5)] if not spikes.empty else [],
    }
    with open(os.path.join(outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    # Print short console summary
    print(json.dumps(summary, indent=2))


def analyze_and_visualize(json_path: str, outdir: str, show: bool = False):
    prepare_output_dir(outdir)
    style()

    raw = load_json(json_path)
    df = normalize_tweets(raw)
    df = compute_sentiment(df)

    # Time groupings
    daily, halfday, monthly = group_time(df)
    daily_with_roll, spikes = detect_spikes(daily)

    # Save CSVs
    save_csvs(df, daily_with_roll, halfday, monthly, outdir)

    # Plots
    plot_time_series_daily(daily_with_roll, spikes, outdir)
    plot_halfday_series(halfday, outdir)
    plot_monthly_sentiment(monthly, outdir)
    plot_hashtag_bar(top_n_counts(df["hashtags"], 20), outdir)
    plot_mentions_bar(top_n_counts(df["mentions"], 20), outdir)
    plot_lang_distribution(df, outdir)
    plot_hour_heatmap(df, outdir)
    plot_metrics_distributions(df, outdir)
    plot_reference_breakdown(df, outdir)
    plot_sensitive_over_time(daily_with_roll, outdir)

    # Save summary
    print_and_save_summary(df, daily_with_roll, monthly, spikes, outdir)

    if show:
        # Optionally open a couple of plots if running locally with GUI
        try:
            from PIL import Image
            img_paths = [
                "timeseries_daily.png",
                "monthly_volume_sentiment.png",
                "top_hashtags.png",
                "metrics_distributions.png"
            ]
            for p in img_paths:
                fp = os.path.join(outdir, p)
                if os.path.exists(fp):
                    Image.open(fp).show()
        except Exception:
            pass

    return {
        "df": df,
        "daily": daily_with_roll,
        "halfday": halfday,
        "monthly": monthly,
        "spikes": spikes
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze and visualize generated tweet JSON.")
    parser.add_argument("--json", required=True, help="Path to the tweets JSON file.")
    parser.add_argument("--outdir", default=None, help="Directory to save outputs (plots, CSVs). Defaults next to JSON.")
    parser.add_argument("--show", action="store_true", help="Attempt to open some plots locally.")
    args = parser.parse_args()

    json_path = args.json
    if not os.path.exists(json_path):
        raise FileNotFoundError(json_path)

    if args.outdir is None:
        base = os.path.splitext(os.path.basename(json_path))[0]
        outdir = os.path.join(os.path.dirname(json_path), f"{base}_analysis")
    else:
        outdir = args.outdir

    analyze_and_visualize(json_path, outdir, show=args.show)


if __name__ == "__main__":
    main()