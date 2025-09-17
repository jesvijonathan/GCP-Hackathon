import random
import uuid
import json
import math
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone, date
import numpy as np
import re


@dataclass
class RedditPost:
    id: str                          # base36-like id (e.g., "abc123")
    name: str                        # fullname ("t3_<id>")
    author: str                      # "u/<username>"
    author_fullname: str             # "t2_<base36>"
    author_id: str                   # synthetic id "u_<hex>"
    title: str
    selftext: Optional[str]
    created_utc: int
    created_at: str
    subreddit: str
    subreddit_id: str                # "t5_<base36>"
    permalink: str
    url: Optional[str]
    is_self: bool
    flair_text: Optional[str]
    over_18: bool
    spoiler: bool
    locked: bool
    stickied: bool
    num_comments: int
    upvote_ratio: float
    ups: int
    downs: int
    score: int
    award_count: int
    keywords: List[str] = field(default_factory=list)
    lang: str = "en"
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    risk_score: Optional[float] = None
    removed_by_category: Optional[str] = None
    is_original_content: Optional[bool] = None
    crosspost_parent: Optional[str] = None
    edited: Optional[Any] = None  # False or int timestamp


# --------------------------- Utilities ---------------------------

def parse_date_str(s: str) -> date:
    if len(s) == 7:
        return date.fromisoformat(s + "-01")
    return date.fromisoformat(s)


def isoformat_dt(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def dt_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def base36(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "0"
    out = []
    while n:
        n, r = divmod(n, 36)
        out.append(chars[r])
    return "".join(reversed(out))


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def make_post_id(counter: int) -> str:
    # Make increasing-looking base36 id with some randomness
    return f"{base36(counter)}{uuid.uuid4().hex[:3]}"


def make_subreddit_id() -> str:
    return f"t5_{base36(random.getrandbits(40))}"


def make_author_ids() -> Tuple[str, str, str]:
    uhex = uuid.uuid4().hex[:10]
    author_id = f"u_{uhex}"
    author_fullname = f"t2_{base36(random.getrandbits(40))}"
    username = f"user_{uhex[:6]}"
    return author_id, author_fullname, username


def slugify(title: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE)
    slug = re.sub(r"\s+", "-", slug.strip()).lower()
    return slug[:60] if slug else "post"


# --------------------------- Trend/Events modeling ---------------------------

def event_sentiment_shift(label: str, rng: random.Random) -> float:
    l = label.lower()
    neg_keys = ["breach", "fraud", "lawsuit", "boycott", "downtime", "outage", "recall", "regulatory", "fine", "leak", "crisis", "scandal", "layoff"]
    pos_keys = ["new product", "launch", "award", "partnership", "expansion", "feature", "investment", "earnings beat", "milestone", "hiring"]
    if any(k in l for k in neg_keys):
        return rng.uniform(-0.9, -0.6)
    if any(k in l for k in pos_keys):
        return rng.uniform(0.4, 0.8)
    return rng.uniform(-0.05, 0.05)


def random_trend_plan(start: date, end: date, rng: random.Random) -> List[Dict[str, Any]]:
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    month_indices = sorted(rng.sample(range(total_months), k=rng.randint(3, 5)))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership", "seasonal sale"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall", "regulatory fine"]
    neutral_labels = ["normal", "seasonal buzz", "promo period"]

    plan = []
    for idx in month_indices:
        y = start.year + (start.month - 1 + idx) // 12
        m = (start.month - 1 + idx) % 12 + 1
        month_str = f"{y}-{m:02d}"
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels)
            intensity = rng.uniform(0.65, 0.95)
        elif r < 0.7:
            label = rng.choice(pos_labels)
            intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neutral_labels)
            intensity = rng.uniform(0.45, 0.70)
        # Optional posts share for a couple of events
        posts_share = None
        if rng.random() < 0.5:
            posts_share = rng.uniform(0.02, 0.15)  # 2%..15%
        e = {"month": month_str, "intensity": round(float(intensity), 2), "label": label}
        if posts_share is not None:
            e["posts"] = posts_share
        plan.append(e)
    return plan


def build_daily_intensity(
    start: date,
    end: date,
    trend_plan: List[Dict[str, Any]],
    rng: random.Random
) -> Tuple[List[date], np.ndarray, Dict[date, List[Dict[str, Any]]]]:
    days = list(dt_range(start, end))
    n = len(days)
    day_of_week = np.array([d.weekday() for d in days], dtype=float)
    day_of_year = np.array([(d - date(d.year, 1, 1)).days for d in days], dtype=float)

    # Baseline: weekly and annual seasonality (Reddit busier midweek and winter evenings)
    base = np.ones(n)
    weekly = 0.18 * np.sin((day_of_week - 1) / 6.0 * 2 * np.pi)  # -0.18..0.18
    annual = 0.12 * np.sin((day_of_year / 365.25) * 2 * np.pi)
    intensity = base + weekly + annual
    intensity = np.clip(intensity, 0.1, None)

    events_by_day: Dict[date, List[Dict[str, Any]]] = {d: [] for d in days}
    for e in trend_plan:
        e_month = parse_date_str(e["month"])
        e_center = date(e_month.year, e_month.month, 15)
        e_intensity = float(e.get("intensity", 0.6))
        e_label = str(e.get("label", "normal"))
        sigma_days = rng.randint(7, 24)
        diffs = np.array([(d - e_center).days for d in days], dtype=float)
        gaussian = np.exp(-(diffs ** 2) / (2 * (sigma_days ** 2)))
        intensity += e_intensity * gaussian * 1.25
        e_shift = event_sentiment_shift(e_label, rng)
        for idx, d in enumerate(days):
            if gaussian[idx] > 0.05:
                events_by_day[d].append({
                    "label": e_label,
                    "gaussian": float(gaussian[idx]),
                    "shift": e_shift
                })

    intensity = np.clip(intensity, 0.05, None)
    return days, intensity, events_by_day


def parse_posts_share(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        v = val.strip().replace(" ", "")
        if v.endswith("%"):
            try:
                return clamp(float(v[:-1]) / 100.0, 0.0, 1.0)
            except:
                return None
        try:
            num = float(v)
            return num if num <= 1 else clamp(num / 100.0, 0.0, 1.0)
        except:
            return None
    try:
        v = float(val)
        return v if v <= 1 else clamp(v / 100.0, 0.0, 1.0)
    except:
        return None


def apply_monthly_post_shares(
    days: List[date],
    base_weights: np.ndarray,
    trend_plan: List[Dict[str, Any]]
) -> np.ndarray:
    # Build month shares from trend_plan "posts" fields
    month_shares: Dict[str, float] = {}
    for e in trend_plan:
        m = e.get("month")
        if not m:
            continue
        share = parse_posts_share(e.get("posts"))
        if share is not None and share > 0:
            month_shares[m] = month_shares.get(m, 0.0) + share

    if not month_shares:
        return base_weights  # no change

    # Normalize if sum shares > 0.95
    sum_shares = sum(month_shares.values())
    if sum_shares > 0.95:
        for k in list(month_shares.keys()):
            month_shares[k] = month_shares[k] / sum_shares * 0.95
        sum_shares = sum(month_shares.values())

    # Compute base totals by month
    months = [f"{d.year}-{d.month:02d}" for d in days]
    unique_months = list(dict.fromkeys(months))
    month_to_total = {m: 0.0 for m in unique_months}
    for w, m in zip(base_weights, months):
        month_to_total[m] += float(w)

    # Compute total weight for "other" months
    specified_months = set(month_shares.keys())
    other_total = sum(w for w, m in zip(base_weights, months) if m not in specified_months)
    remaining_share = max(1e-9, 1.0 - sum_shares)

    # Reassign weights month-wise preserving daily relative shape
    new_w = np.zeros_like(base_weights)
    # Specified months
    for m, share in month_shares.items():
        if m not in month_to_total or month_to_total[m] == 0:
            continue
        # Distribute 'share' across days of month m proportional to base weight
        denom = month_to_total[m]
        for i, dm in enumerate(months):
            if dm == m:
                new_w[i] = base_weights[i] / denom * share

    # Other months
    if other_total > 0 and remaining_share > 0:
        for i, dm in enumerate(months):
            if dm not in specified_months:
                new_w[i] = base_weights[i] / other_total * remaining_share

    # Normalize to sum 1
    s = float(new_w.sum())
    if s <= 0:
        return base_weights
    return new_w / s


# --------------------------- Subreddit and content ---------------------------

def choose_subreddit(event_label: Optional[str], rng: random.Random) -> Tuple[str, str, str]:
    # Returns subreddit name, subreddit_id, flair_text
    base = [
        ("news", 0.9), ("worldnews", 0.5), ("business", 0.8), ("technology", 0.9),
        ("investing", 0.6), ("stocks", 0.6), ("UKInvesting", 0.4),
        ("netsec", 0.3), ("cybersecurity", 0.35), ("privacy", 0.2),
        ("DIY", 0.4), ("HomeImprovement", 0.45),
        ("AskUK", 0.5), ("unitedkingdom", 0.5),
        ("retail", 0.35), ("CustomerService", 0.25)
    ]
    # Bias by event
    if event_label:
        el = event_label.lower()
        if any(k in el for k in ["breach", "outage", "leak", "security", "cyber"]):
            for i, (s, w) in enumerate(base):
                if s in ("netsec", "cybersecurity", "technology", "news"):
                    base[i] = (s, w * 2.0)
        if any(k in el for k in ["launch", "product", "feature"]):
            for i, (s, w) in enumerate(base):
                if s in ("technology", "DIY", "HomeImprovement", "news"):
                    base[i] = (s, w * 1.8)
        if any(k in el for k in ["earnings", "investment", "partnership", "award", "regulatory", "fine"]):
            for i, (s, w) in enumerate(base):
                if s in ("investing", "stocks", "business", "news"):
                    base[i] = (s, w * 1.8)

    names = [f"r/{s}" for s, _ in base]
    weights = np.array([w for _, w in base], dtype=float)
    weights = weights / weights.sum()
    idx = int(rng.choices(range(len(names)), weights=weights, k=1)[0])
    subreddit = names[idx]
    flair_pool = {
        "r/news": ["News", "Breaking", "Business"],
        "r/worldnews": ["News", "Global"],
        "r/business": ["Discussion", "News", "Earnings"],
        "r/technology": ["Tech News", "Product", "Discussion"],
        "r/investing": ["DD", "News", "Discussion"],
        "r/stocks": ["DD", "News", "Earnings"],
        "r/UKInvesting": ["Discussion", "News"],
        "r/netsec": ["Security", "Incident"],
        "r/cybersecurity": ["Security", "Incident", "Discussion"],
        "r/privacy": ["Privacy", "Discussion"],
        "r/DIY": ["Help", "Project", "Discussion"],
        "r/HomeImprovement": ["Help", "Project", "Advice"],
        "r/AskUK": ["Question", "Advice"],
        "r/unitedkingdom": ["News", "Discussion"],
        "r/retail": ["News", "Discussion"],
        "r/CustomerService": ["Question", "Complaint"],
    }
    flair_choices = flair_pool.get(subreddit, ["Discussion"])
    flair_text = rng.choice(flair_choices)
    return subreddit, make_subreddit_id(), flair_text


def build_title_and_text(
    merchant: str,
    sentiment_label: str,
    event_label: Optional[str],
    subreddit: str,
    rng: random.Random
) -> Tuple[str, Optional[str]]:
    m = merchant
    # Title templates by sentiment
    pos = [
        "{m} launches new product line — looks promising",
        "Strong update from {m}",
        "Anyone tried the latest from {m}? Impressed so far",
        "Great move by {m} this week",
        "{m} wins award; community thoughts?"
    ]
    neu = [
        "What do you think about {m} lately?",
        "Discussion: {m} and recent developments",
        "Question for folks using {m}: experiences?",
        "Reading about {m}. Any insights?",
        "Is {m} worth considering for DIY?"
    ]
    neg = [
        "Reported issue at {m} — should we be concerned?",
        "Data breach at {m}? What we know",
        "Outage impacting {m} users",
        "Not a great look for {m} right now",
        "Customer service problems at {m}?"
    ]
    # Event-oriented augmentation
    if event_label:
        el = event_label.lower()
        if any(k in el for k in ["breach", "leak", "outage", "lawsuit", "recall"]):
            neg += [
                "Security incident at {m}: discussion thread",
                "Another breach? {m} needs to tighten security",
                "Legal trouble for {m}? Updates here"
            ]
        if any(k in el for k in ["launch", "product", "feature"]):
            pos += [
                "{m} just launched something new — AMA",
                "Launch day for {m}: first impressions",
                "Feature rollout from {m} — details"
            ]
        if any(k in el for k in ["earnings", "investment", "partnership", "award"]):
            pos += [
                "{m} posts solid earnings — analysis",
                "Partnership news: {m} joins forces with X",
            ]

    # Subreddit-specific style
    if subreddit in ("r/AskUK",):
        neu += [
            "UK folks: what's your experience with {m}?",
            "Is {m} worth it in the UK?"
        ]
    if subreddit in ("r/DIY", "r/HomeImprovement"):
        neu += [
            "Help with {m} tools — recommendations?",
            "Project thread: using {m} supplies"
        ]
    if subreddit in ("r/investing", "r/stocks", "r/UKInvesting"):
        pos += ["Earnings beat from {m}?", "Bullish/bearish case on {m}"]
        neu += ["What’s your thesis on {m}?"]
        neg += ["Bear case on {m} after recent news"]

    if sentiment_label == "positive":
        title = rng.choice(pos).format(m=m)
    elif sentiment_label == "negative":
        title = rng.choice(neg).format(m=m)
    else:
        title = rng.choice(neu).format(m=m)

    # Selftext: sometimes empty (link posts) or short body
    bodies_pos = [
        "Sharing this update I found interesting. Thoughts?",
        "Quick note: found it useful; posting for visibility.",
        "Curious what the community thinks about this move."
    ]
    bodies_neu = [
        "Genuinely curious about experiences.",
        "Any insights appreciated.",
        "Looking for balanced perspectives."
    ]
    bodies_neg = [
        "Sources are still developing; please share updates.",
        "Not sure how widespread this is, but it's concerning.",
        "If affected, please comment with details."
    ]
    if sentiment_label == "positive":
        body_pool = bodies_pos
    elif sentiment_label == "negative":
        body_pool = bodies_neg
    else:
        body_pool = bodies_neu

    # About 60% self posts on discussion subs; lower on news/tech subs
    if subreddit in ("r/AskUK", "r/DIY", "r/HomeImprovement", "r/CustomerService"):
        make_self = rng.random() < 0.75
    else:
        make_self = rng.random() < 0.50

    selftext = rng.choice(body_pool) if make_self else None
    return title, selftext


def extract_keywords(text: str, extra: Optional[str] = None, merchant: Optional[str] = None) -> List[str]:
    stop = set(["the", "and", "for", "with", "this", "that", "are", "was", "but", "you", "any", "about", "into", "from", "your", "have", "has", "what", "who", "why", "how", "when", "where", "their", "them", "they", "she", "he", "its", "it's", "had", "were", "will", "would", "could", "should", "just", "new"])
    raw = (text or "") + " " + (extra or "")
    raw = raw.lower()
    tokens = re.findall(r"[a-z0-9\$]{3,}", raw)
    toks = []
    for t in tokens:
        if t in stop:
            continue
        toks.append(t)
    # ensure merchant token present sometimes
    if merchant:
        mt = merchant.lower().replace(" ", "")
        if mt not in toks:
            toks.append(mt)
    # keep up to 10 unique
    uniq = []
    for t in toks:
        if t not in uniq:
            uniq.append(t)
        if len(uniq) >= 10:
            break
    return uniq


# --------------------------- Sentiment/Risk ---------------------------

def compute_sentiment_and_risk(base_mu: float, day_events: List[Dict[str, Any]], rng: random.Random) -> Tuple[float, str, float, Optional[str]]:
    shift = 0.0
    driver_event = None
    if day_events:
        weights = np.array([e["gaussian"] for e in day_events], dtype=float)
        weights = weights / (weights.sum() + 1e-12)
        shift = float(sum(w * e["shift"] for w, e in zip(weights, day_events)))
        driver_event = max(day_events, key=lambda e: e["gaussian"])["label"]
    mu = clamp(base_mu + shift, -0.95, 0.95)
    score = clamp(rng.gauss(mu, 0.45), -1.0, 1.0)
    if score <= -0.2:
        label = "negative"
    elif score >= 0.2:
        label = "positive"
    else:
        label = "neutral"
    risk = round(100.0 * (1.0 - (score + 1.0) / 2.0), 2)
    return score, label, risk, driver_event


# --------------------------- Metrics ---------------------------

def generate_post_metrics(
    base_pop_scale: float,
    event_intensity: float,
    sentiment_label: str,
    is_self: bool,
    rng: random.Random
) -> Tuple[int, float, int, int, int, int]:
    # Popularity influenced by event intensity and post type
    pop_multiplier = 1.0 + 0.8 * abs(event_intensity)
    if not is_self:
        pop_multiplier *= 1.1  # link posts can travel more on news subs

    # Ups via lognormal heavy tail, then clamp
    ups = int(min(50000, rng.lognormvariate(mu=math.log(18 * base_pop_scale + 1e-9), sigma=1.1) * pop_multiplier))

    # Upvote ratio: neutral-high for positive, lower for negative
    if sentiment_label == "positive":
        ratio = float(clamp(rng.betavariate(20, 6), 0.55, 0.99))
    elif sentiment_label == "negative":
        ratio = float(clamp(rng.betavariate(10, 8), 0.50, 0.95))
    else:
        ratio = float(clamp(rng.betavariate(14, 8), 0.52, 0.97))

    downs = int(max(0, round(ups * (1 - ratio) / max(1e-6, ratio))))
    score = int(max(0, ups - downs))

    # Comments scale sublinearly with ups
    base_c = rng.uniform(0.4, 0.9) if is_self else rng.uniform(0.25, 0.7)
    num_comments = int(max(0, rng.gauss(base_c * math.sqrt(ups + 1), 2.0) * (1.0 + 0.5 * abs(event_intensity))))

    # Awards: small Poisson depending on popularity
    lam = 0.01 * math.sqrt(max(1, ups))  # <= handful typically
    award_count = int(np.random.poisson(lam=lam))
    return ups, ratio, downs, score, num_comments, award_count


# --------------------------- Main generator ---------------------------

def generate_fake_reddit_json(
    merchant_name: str,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    n_posts: int = 10000,
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None
) -> str:
    """
    Generate a fake Reddit submissions dataset related to a merchant.
    - merchant_name: e.g., "Kingfisher"
    - date range: "YYYY-MM-DD" or "YYYY-MM"
    - n_posts: target count; final will vary around it (±10%) but close
    - trend_plan: list of events: {"month":"YYYY-MM","intensity":0..1,"label":str, optional "posts": share e.g., 0.1 or "10%"}
    - seed: RNG seed
    - out_json_path: optional output path; if None an auto name is used
    Returns: path to the JSON file containing a list[RedditPost]
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    # Final post count around requested n_posts; at least 500 for meaningful trends
    final_n = max(int(round(n_posts * rng.uniform(0.9, 1.1))), 500)

    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    # Daily intensity and events map
    days, intensity, events_by_day = build_daily_intensity(start, end, trend_plan, rng)
    weights = intensity / (intensity.sum() + 1e-12)

    # Apply monthly 'posts' shares if provided
    weights = apply_monthly_post_shares(days, weights, trend_plan)

    # Allocate posts per day
    posts_per_day = np.random.multinomial(final_n, weights)

    # Author pool (Zipf-like frequency)
    n_authors = max(300, min(1200, int(final_n / rng.uniform(2.5, 5.0))))
    # Create author ids and usernames
    author_ids = []
    author_fullnames = []
    usernames = []
    for _ in range(n_authors):
        aid, afn, uname = make_author_ids()
        author_ids.append(aid)
        author_fullnames.append(afn)
        usernames.append(uname)
    # Zipf distribution for authors
    zipf_a = rng.uniform(1.3, 2.0)
    zipf_raw = np.random.zipf(a=zipf_a, size=final_n)
    zipf_ranks = np.clip(zipf_raw, 1, n_authors) - 1
    # Shuffle authors for randomness
    indices = list(range(n_authors))
    rng.shuffle(indices)

    # Time-of-day distribution: morning/afternoon/evening
    def sample_time_of_day():
        r = rng.random()
        if r < 0.45:
            hour = int(clamp(rng.gauss(12, 2.5), 6, 17))
        elif r < 0.8:
            hour = int(clamp(rng.gauss(20, 2.5), 12, 23))
        else:
            hour = int(clamp(rng.gauss(9, 1.5), 6, 12))
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        return hour, minute, second

    # Build posts
    posts: List[RedditPost] = []
    counter = 1
    base_mu = rng.uniform(0.02, 0.12)  # neutral to slightly positive baseline

    # For each day
    for d, count in zip(days, posts_per_day):
        if count == 0:
            continue

        day_events = events_by_day.get(d, [])
        event_intensity_val = float(sum(abs(e["shift"]) * e["gaussian"] for e in day_events))

        for _ in range(count):
            # Sentiment
            s_score, s_label, risk_score, driver_event = compute_sentiment_and_risk(base_mu, day_events, rng)

            # Subreddit choice (weights by event)
            subreddit, subreddit_id, flair_text = choose_subreddit(driver_event, rng)

            # Title/selftext
            title, selftext = build_title_and_text(merchant_name, s_label, driver_event, subreddit, rng)

            # Determine post type and URL
            is_self = selftext is not None
            url = None
            if not is_self:
                url = rng.choice([
                    "https://news.example.com/article",
                    "https://blog.example.com/post",
                    "https://investor.example.com/report",
                    "https://security.example.com/incident",
                    "https://shop.example.com/product",
                    "https://media.example.com/press",
                ])

            # Metrics
            base_pop_scale = rng.uniform(0.6, 1.6)
            ups, ratio, downs, score, num_comments, award_count = generate_post_metrics(
                base_pop_scale, event_intensity_val, s_label, is_self, rng
            )

            # Moderation flags
            over_18 = rng.random() < 0.02  # small
            spoiler = rng.random() < 0.03
            locked = rng.random() < (0.02 + 0.05 * (1 if s_label == "negative" else 0))  # a bit higher for negative
            stickied = rng.random() < 0.01
            removed_by_category = None
            if s_label == "negative" and rng.random() < clamp(0.02 + 0.03 * abs(s_score), 0, 0.12):
                removed_by_category = rng.choice(["moderator", "copyright", "spam", "author"])

            # Original content and crosspost
            is_oc = rng.random() < 0.3
            crosspost_parent = None
            if rng.random() < 0.05:
                crosspost_parent = f"t3_{make_post_id(rng.randint(1, 500000))}"

            # Edited
            edited = False
            if is_self and rng.random() < 0.08:
                # 50/50 chance store as timestamp
                ts = datetime(d.year, d.month, d.day, *sample_time_of_day(), tzinfo=timezone.utc)
                edited = int(ts.timestamp())

            # Timestamp
            hour, minute, second = sample_time_of_day()
            created_dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)
            created_utc = int(created_dt.timestamp())
            created_at = isoformat_dt(created_dt)

            # ID and permalink
            pid = make_post_id(counter)
            name = f"t3_{pid}"
            slug = slugify(title)
            permalink = f"/{subreddit}/comments/{pid}/{slug}/"

            # Author selection via zipf ranks
            aidx = indices[zipf_ranks[len(posts) % len(zipf_ranks)]]
            author_id = author_ids[aidx]
            author_fullname = author_fullnames[aidx]
            username = usernames[aidx]

            # Keywords
            keywords = extract_keywords(title, selftext, merchant_name)

            post = RedditPost(
                id=pid,
                name=name,
                author=f"u/{username}",
                author_fullname=author_fullname,
                author_id=author_id,
                title=title,
                selftext=selftext,
                created_utc=created_utc,
                created_at=created_at,
                subreddit=subreddit,
                subreddit_id=subreddit_id,
                permalink=permalink,
                url=url,
                is_self=is_self,
                flair_text=flair_text,
                over_18=over_18,
                spoiler=spoiler,
                locked=locked,
                stickied=stickied,
                num_comments=num_comments,
                upvote_ratio=round(ratio, 3),
                ups=ups,
                downs=downs,
                score=score,
                award_count=award_count,
                keywords=keywords,
                lang="en",
                sentiment_score=round(float(s_score), 4),
                sentiment_label=s_label,
                risk_score=risk_score,
                removed_by_category=removed_by_category,
                is_original_content=is_oc,
                crosspost_parent=crosspost_parent,
                edited=edited
            )
            posts.append(post)
            counter += 1

    # Sort by created_at and ensure strictly increasing
    posts.sort(key=lambda p: p.created_at)
    last_dt = None
    for p in posts:
        cur_dt = datetime.fromisoformat(p.created_at.replace("Z", "+00:00"))
        if last_dt is not None and cur_dt <= last_dt:
            cur_dt = last_dt + timedelta(seconds=1)
            p.created_at = isoformat_dt(cur_dt)
            p.created_utc = int(cur_dt.timestamp())
        last_dt = cur_dt

    # Output list of dicts
    out_list = [asdict(p) for p in posts]

    # Default output path
    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_merchant = merchant_name.lower().replace(" ", "")
        out_json_path = f"reddit_{safe_merchant}_{start_date}_to_{end_date}_{ts}.json".replace(":", "-")

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example usage ---------------------------

if __name__ == "__main__":
    merchant = "Kingfisher"
    path = generate_fake_reddit_json(
        merchant_name=merchant,
        start_date="2020-01-01",
        end_date="2024-12-31",
        n_posts=10000,
        trend_plan=[
            {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach", "posts": "2%"},
            {"month": "2023-09", "intensity": 0.60, "label": "normal", "posts": "10%"},
            {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "posts": "30%"},
        ],
        seed=42,
        out_json_path=None  # auto-named
    )
    print(f"Saved Reddit JSON to: {path}")