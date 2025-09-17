import random
import uuid
import json
import math
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone, date
import numpy as np


@dataclass
class TweetData:
    tweet_id: str
    author_id: str
    content: str
    created_at: str
    lang: str = "ENG"
    public_metrics: Dict[str, int] = field(
        default_factory=lambda: {
            "like_count": 0,
            "reply_count": 0,
            "retweet_count": 0,
            "quote_count": 0
        }
    )
    non_public_metrics: Dict[str, int] = field(
        default_factory=lambda: {
            "impression_count": 0,
            "url_link_clicks": 0,
            "user_profile_clicks": 0
        }
    )
    entities: Dict[str, list] = field(
        default_factory=lambda: {"hashtags": [], "mentions": [], "urls": [], "cashtags": []}
    )
    context_annotations: List[Dict[str, Dict[str, str]]] = field(
        default_factory=lambda: [
            {"domain": {"id": "", "name": "", "description": ""},
             "entity": {"id": "", "name": "", "description": ""}}
        ]
    )
    possibly_sensitive: bool = False
    conversation_id: str = None
    referenced_tweets: List[Dict[str, str]] = None
    in_reply_to_status_id: str = None
    in_reply_to_user_id: str = None
    attachments_media_keys: List[str] = None
    edit_history_tweet_ids: List[str] = None
    withheld: Dict[str, Any] = None


# --------------------------- Utility ---------------------------

def parse_date_str(s: str) -> date:
    # Accept "YYYY-MM-DD" or "YYYY-MM"
    if len(s) == 7:
        return date.fromisoformat(s + "-01")
    return date.fromisoformat(s)


def isoformat_dt(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def dt_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def base62(n: int) -> str:
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "0"
    out = []
    while n:
        n, r = divmod(n, 62)
        out.append(chars[r])
    return "".join(reversed(out))


def make_tweet_id(counter: int) -> str:
    return f"t_{base62(counter)}{uuid.uuid4().hex[:6]}"


def make_user_id() -> str:
    return f"u_{uuid.uuid4().hex[:10]}"


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def choose_language(rng: random.Random) -> str:
    if rng.random() < 0.99:
        return "ENG"
    return rng.choice(["ES", "FR", "DE", "IT", "PT", "PL", "NL", "RO"])


# --------------------------- Trend/Events ---------------------------

def event_sentiment_shift(label: str, rng: random.Random) -> float:
    label_l = label.lower()
    neg_keys = ["breach", "fraud", "lawsuit", "boycott", "outage", "recall", "downtime", "scandal", "leak", "crisis", "layoff"]
    pos_keys = ["launch", "new product", "award", "partnership", "expansion", "feature", "hiring", "milestone", "investment"]
    if any(k in label_l for k in neg_keys):
        return rng.uniform(-0.9, -0.6)
    if any(k in label_l for k in pos_keys):
        return rng.uniform(0.4, 0.8)
    return rng.uniform(-0.05, 0.05)


def random_trend_plan(start: date, end: date, rng: random.Random) -> List[Dict[str, Any]]:
    # 3-5 events across the range, mix of spikes and normal
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    month_indices = sorted(rng.sample(range(total_months), k=rng.randint(3, 5)))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall"]
    neutral_labels = ["normal", "seasonal buzz", "promo period"]

    plan = []
    for idx in month_indices:
        y = start.year + (start.month - 1 + idx) // 12
        m = (start.month - 1 + idx) % 12 + 1
        month_str = f"{y}-{m:02d}"
        # Choose event type
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels)
            intensity = rng.uniform(0.65, 0.95)
        elif r < 0.70:
            label = rng.choice(pos_labels)
            intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neutral_labels)
            intensity = rng.uniform(0.45, 0.70)
        plan.append({"month": month_str, "intensity": round(float(intensity), 2), "label": label})
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

    # Baseline + seasonality
    base = np.ones(n)
    weekly = 0.15 * np.sin((day_of_week - 1) / 6.0 * 2 * np.pi)
    annual = 0.10 * np.sin((day_of_year / 365.25) * 2 * np.pi)
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
        intensity += e_intensity * gaussian * 1.2
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


# --------------------------- Authors/Entities ---------------------------

def make_author_pool(n_authors: int, merchant_name: str, rng: random.Random):
    handles = []
    ids = []
    merch_base = merchant_name.lower().replace(" ", "")
    merchant_handles = [f"@{merch_base}", f"@{merch_base}uk", f"@{merch_base}_support", f"@{merch_base}_careers"]
    for h in merchant_handles:
        handles.append(h)
        ids.append(make_user_id())

    common = [
        "alex", "sam", "chris", "jordan", "taylor", "morgan", "jamie", "lee",
        "casey", "drew", "kiran", "pat", "reese", "parker", "dev", "rida",
        "noah", "mia", "liam", "ava", "ethan", "sofia", "arya", "zane", "nina", "omar"
    ]
    while len(handles) < n_authors:
        name = rng.choice(common)
        suffix = base62(rng.getrandbits(20)).lower()
        handles.append(f"@{name}{suffix}")
        ids.append(make_user_id())

    author_map = dict(zip(ids, handles))
    return ids, author_map


def merchant_related_entities(merchant_name: str):
    merchant_clean = merchant_name.strip()
    ticker = f"${''.join([c for c in merchant_clean.upper() if c.isalpha()])[:3]}" if merchant_clean else "$MRC"
    return {
        "hashtags": [
            f"#{merchant_clean.replace(' ', '')}",
            f"#{merchant_clean.replace(' ', '')}Plc",
            "#Retail", "#HomeImprovement", "#DIY", "#CyberSecurity", "#NewProduct",
            "#Earnings", "#Sustainability", "#CustomerService", "#DataBreach",
            "#LaunchDay", "#RetailNews", "#UKBusiness", "#Investing"
        ],
        "cashtags": [
            ticker, f"{ticker}.L", "$Retail", "$UKX", "$FTSE"
        ],
        "urls": [
            "https://news.example.com/article",
            "https://blog.example.com/post",
            "https://investor.example.com/report",
            "https://shop.example.com/product",
            "https://security.example.com/incident",
            "https://media.example.com/press"
        ]
    }


def build_context_annotations(merchant_name: str, event_label: Optional[str], rng: random.Random):
    domain_pool = [
        ("65", "Business & finance", "Financial information about a business entity"),
        ("66", "Retail", "Retail industry and companies"),
        ("67", "Cybersecurity", "Security incidents and practices"),
        ("68", "Product", "Product announcements and reviews"),
        ("69", "Customer support", "Customer service interactions"),
        ("70", "Sustainability", "Environmental and social responsibility"),
    ]
    result = []
    k = rng.choices([0, 1, 2], weights=[0.45, 0.35, 0.20], k=1)[0]
    for _ in range(k):
        dom = rng.choice(domain_pool)
        if event_label and "breach" in event_label.lower():
            dom = ("67", "Cybersecurity", "Security incidents and practices")
        elif event_label and "launch" in event_label.lower():
            dom = ("68", "Product", "Product announcements and reviews")
        domain = {"id": dom[0], "name": dom[1], "description": dom[2]}
        entity = {
            "id": f"ent_{uuid.uuid4().hex[:6]}",
            "name": merchant_name,
            "description": f"Mentions related to {merchant_name}"
        }
        result.append({"domain": domain, "entity": entity})
    return result


# --------------------------- Content and Metrics ---------------------------

def build_content(merchant: str, sentiment_label: str, event_label: Optional[str], include_merchant_in_text: bool, rng: random.Random) -> str:
    pos_templates = [
        "Great job by {m} today.",
        "{m} just announced something exciting.",
        "Impressed with what {m} is doing.",
        "Positive update from {m}.",
        "{m} looking strong lately."
    ]
    neu_templates = [
        "Anyone tried products from {m}?",
        "What's your take on {m}?",
        "Reading about {m}. Thoughts?",
        "General discussion about {m}.",
        "Checking in on {m} developments."
    ]
    neg_templates = [
        "Not a good look for {m}.",
        "Concerned about {m} right now.",
        "This could be rough for {m}.",
        "Disappointed with {m} lately.",
        "Serious questions for {m}."
    ]

    if event_label:
        el = event_label.lower()
        if "breach" in el or "leak" in el or "outage" in el or "lawsuit" in el:
            neg_templates += [
                "Data concerns at {m}.",
                "Hearing about issues at {m}.",
                "Security story around {m} again."
            ]
        if "launch" in el or "product" in el or "feature" in el:
            pos_templates += [
                "{m} just launched a new product.",
                "Launch day for {m} looks promising.",
                "New feature from {m} caught my eye."
            ]

    if sentiment_label == "positive":
        template = rng.choice(pos_templates)
    elif sentiment_label == "negative":
        template = rng.choice(neg_templates)
    else:
        template = rng.choice(neu_templates)

    if include_merchant_in_text:
        return template.format(m=merchant)
    else:
        return template.format(m="them")


def generate_metrics(base_like_scale: float, event_intensity: float, rng: random.Random) -> Tuple[Dict[str, int], Dict[str, int]]:
    like_multiplier = 1.0 + 0.7 * abs(event_intensity)
    raw = rng.lognormvariate(mu=math.log(12 * base_like_scale + 1e-9), sigma=1.0)
    like_count = int(min(1000, raw * like_multiplier))

    reply_ratio = rng.uniform(0.05, 0.30)
    retweet_ratio = rng.uniform(0.10, 0.85)
    quote_ratio = rng.uniform(0.05, 0.60)

    reply_count = int(round(like_count * reply_ratio))
    retweet_count = int(round(like_count * retweet_ratio))
    quote_count = int(round(max(0, reply_count) * quote_ratio))

    public = {
        "like_count": like_count,
        "reply_count": reply_count,
        "retweet_count": retweet_count,
        "quote_count": quote_count
    }

    imp_factor = rng.lognormvariate(mu=3.0, sigma=0.8)  # ~20x median
    imp_factor = clamp(imp_factor, 2.0, 200.0)
    impression_count = int(max(like_count + 1, like_count * imp_factor))

    url_ctr = rng.betavariate(1.2, 30.0)
    profile_ctr = rng.betavariate(1.2, 50.0)
    url_link_clicks = int(round(impression_count * url_ctr))
    user_profile_clicks = int(round(impression_count * profile_ctr))

    non_public = {
        "impression_count": impression_count,
        "url_link_clicks": url_link_clicks,
        "user_profile_clicks": user_profile_clicks
    }
    return public, non_public


# --------------------------- Generation ---------------------------

def generate_fake_tweets_json(
    merchant_name: str,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    n_tweets: int = 2000,
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None
) -> str:
    """
    Generate a fake dataset of TweetData objects and save as a JSON list.
    - merchant_name: merchant (e.g., "Kingfisher")
    - start_date/end_date: "YYYY-MM-DD" or "YYYY-MM"
    - n_tweets: target number; actual generated will vary randomly around it; at least 1000
    - trend_plan: optional list of {"month":"YYYY-MM", "intensity": float 0..1, "label": str}
                  if None, a gradual plan with random spikes is created
    - seed: optional RNG seed for reproducibility
    - out_json_path: file path to save; if None, an auto name is used
    Returns: the path to the generated JSON file
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    # Final tweet count around requested n_tweets, but at least 1000
    final_n = max(int(round(n_tweets * rng.uniform(0.9, 1.15))), 1000)

    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    days, intensity, events_by_day = build_daily_intensity(start, end, trend_plan, rng)
    weights = intensity / (intensity.sum() + 1e-12)
    tweets_per_day = np.random.multinomial(final_n, weights)

    # Authors
    n_authors = max(200, min(800, int(final_n / rng.uniform(2.5, 4.5))))
    author_ids, author_map = make_author_pool(n_authors, merchant_name, rng)

    # Zipf-like distribution of authors
    zipf_a = rng.uniform(1.3, 2.0)
    zipf_raw = np.random.zipf(a=zipf_a, size=final_n)
    zipf_ranks = np.clip(zipf_raw, 1, n_authors)
    shuffled_authors = author_ids[:]
    rng.shuffle(shuffled_authors)

    # Entities candidates
    ents = merchant_related_entities(merchant_name)

    # Helper: sample time-of-day
    def sample_time_of_day():
        if rng.random() < 0.55:
            hour = int(clamp(rng.gauss(13, 2.5), 6, 18))
        else:
            hour = int(clamp(rng.gauss(20, 2), 12, 23))
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        return hour, minute, second

    # Precompute per-day event intensity score for retweet/reply modulation
    day_to_event_intensity = {}
    for d in days:
        evs = events_by_day.get(d, [])
        if evs:
            day_to_event_intensity[d] = float(sum(abs(e["shift"]) * e["gaussian"] for e in evs))
        else:
            day_to_event_intensity[d] = 0.0

    tweets: List[TweetData] = []
    counter = 1

    # Baseline sentiment mean (slightly positive)
    base_mu = rng.uniform(0.05, 0.15)

    # Create tweets
    for d, count in zip(days, tweets_per_day):
        if count == 0:
            continue

        day_events = events_by_day.get(d, [])
        # Weighted average shift for the day
        if day_events:
            weights_day = np.array([e["gaussian"] for e in day_events], dtype=float)
            weights_day = weights_day / (weights_day.sum() + 1e-12)
            day_shift = float(sum(w * e["shift"] for w, e in zip(weights_day, day_events)))
            driver_event = max(day_events, key=lambda e: e["gaussian"])["label"]
        else:
            day_shift = 0.0
            driver_event = None

        for _ in range(count):
            # Internal sentiment (not stored) guiding content and some flags
            sentiment_val = clamp(rng.gauss(base_mu + day_shift, 0.45), -1.0, 1.0)
            if sentiment_val <= -0.2:
                sentiment_label = "negative"
            elif sentiment_val >= 0.2:
                sentiment_label = "positive"
            else:
                sentiment_label = "neutral"

            include_merchant_text = rng.random() < rng.uniform(0.45, 0.75)
            content = build_content(merchant_name, sentiment_label, driver_event, include_merchant_text, rng)

            # Entities
            hashtags, mentions, urls, cashtags = [], [], [], []

            include_merchant_entity = rng.random() < rng.uniform(0.20, 0.35)

            # Hashtags 0..3
            n_hash = rng.choices([0, 1, 2, 3], weights=[0.45, 0.30, 0.18, 0.07], k=1)[0]
            if include_merchant_entity and n_hash > 0:
                hashtags.append(f"#{merchant_name.replace(' ', '')}")
                n_hash -= 1
            for _ in range(n_hash):
                hashtags.append(rng.choice(ents["hashtags"]))
            hashtags = list(dict.fromkeys(hashtags))

            # Mentions 0..2
            n_ment = rng.choices([0, 1, 2], weights=[0.60, 0.30, 0.10], k=1)[0]
            if include_merchant_entity and n_ment > 0 and rng.random() < 0.7:
                merchant_handles = [h for h in author_map.values() if h.lower().startswith(f"@{merchant_name.lower().replace(' ', '')}")]
                if not merchant_handles:
                    merchant_handles = [f"@{merchant_name.lower().replace(' ', '')}"]
                mentions.append(rng.choice(merchant_handles))
                n_ment -= 1
            for _ in range(n_ment):
                mentions.append(rng.choice(list(author_map.values())))
            mentions = list(dict.fromkeys(mentions))

            # URLs 0..2
            n_url = rng.choices([0, 1, 2], weights=[0.55, 0.35, 0.10], k=1)[0]
            for _ in range(n_url):
                urls.append(rng.choice(ents["urls"]))
            urls = list(dict.fromkeys(urls))

            # Cashtags 0..2
            n_cash = rng.choices([0, 1, 2], weights=[0.70, 0.25, 0.05], k=1)[0]
            for _ in range(n_cash):
                cashtags.append(rng.choice(ents["cashtags"]))
            cashtags = list(dict.fromkeys(cashtags))

            # Sometimes embed some entities directly into content text
            to_embed = []
            if hashtags and rng.random() < 0.7:
                to_embed += rng.sample(hashtags, k=rng.randint(1, min(2, len(hashtags))))
            if mentions and rng.random() < 0.6:
                to_embed += rng.sample(mentions, k=1)
            if urls and rng.random() < 0.55:
                to_embed += rng.sample(urls, k=1)
            if cashtags and rng.random() < 0.4:
                to_embed += rng.sample(cashtags, k=1)
            if to_embed:
                content = f"{content} " + " ".join(to_embed)

            # Context annotations sometimes (0,1,2)
            context_annotations = build_context_annotations(merchant_name, driver_event, rng)

            # Timestamp
            hour, minute, second = sample_time_of_day()
            created_dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)

            # Author assignment
            rank = int(zipf_ranks[len(tweets) % len(zipf_ranks)]) - 1
            author_id = shuffled_authors[min(rank, len(shuffled_authors) - 1)]

            # Language
            lang = choose_language(rng)

            # Metrics
            event_intensity_val = day_to_event_intensity.get(d, 0.0)
            base_like_scale = rng.uniform(0.6, 1.4)
            public_metrics, non_public_metrics = generate_metrics(base_like_scale, event_intensity_val, rng)

            # Sensitivity increases on negative spikes
            base_sensitive_p = rng.uniform(0.05, 0.10)
            if driver_event and any(k in driver_event.lower() for k in ["breach", "lawsuit", "boycott", "recall", "outage", "downtime", "scandal"]):
                base_sensitive_p = min(0.20, base_sensitive_p + rng.uniform(0.05, 0.12))
            possibly_sensitive = rng.random() < base_sensitive_p

            # Withheld depends on internal sentiment (very negative more likely)
            withheld = None
            if sentiment_val <= -0.6 and rng.random() < rng.uniform(0.04, 0.08):
                withheld = {"scope": "tweet", "reason": rng.choice(["legal", "country_withheld"]), "country_codes": rng.sample(["UK", "US", "IN", "DE", "FR", "ES"], k=rng.randint(1, 3))}
            elif sentiment_val <= -0.3 and rng.random() < rng.uniform(0.01, 0.03):
                withheld = {"scope": "tweet", "reason": rng.choice(["policy", "country_withheld"]), "country_codes": rng.sample(["UK", "US", "IN", "DE", "FR", "ES"], k=rng.randint(1, 2))}

            tweet = TweetData(
                tweet_id=make_tweet_id(counter),
                author_id=author_id,
                content=content,
                created_at=isoformat_dt(created_dt),
                lang=lang,
                public_metrics=public_metrics,
                non_public_metrics=non_public_metrics,
                entities={
                    "hashtags": hashtags,
                    "mentions": mentions,
                    "urls": urls,
                    "cashtags": cashtags
                },
                context_annotations=context_annotations if rng.random() > 0.25 else [],  # sometimes empty
                possibly_sensitive=possibly_sensitive,
                conversation_id=None,
                referenced_tweets=None,
                in_reply_to_status_id=None,
                in_reply_to_user_id=None,
                attachments_media_keys=([f"media_key_{uuid.uuid4().hex[:8]}"] if rng.random() < 0.18 else None),
                edit_history_tweet_ids=(None if rng.random() < 0.6 else [f"t_edit_{uuid.uuid4().hex[:6]}"]),
                withheld=withheld
            )
            tweets.append(tweet)
            counter += 1

    # Sort by created_at to ensure increasing order
    tweets.sort(key=lambda t: t.created_at)

    # Ensure strictly increasing timestamps
    last_dt = None
    for t in tweets:
        cur_dt = datetime.fromisoformat(t.created_at.replace("Z", "+00:00"))
        if last_dt is not None and cur_dt <= last_dt:
            cur_dt = last_dt + timedelta(seconds=1)
            t.created_at = isoformat_dt(cur_dt)
        last_dt = cur_dt

    # Add threading and references (after times are sorted)
    id_to_idx = {t.tweet_id: i for i, t in enumerate(tweets)}
    for i, t in enumerate(tweets):
        dt_obj = datetime.fromisoformat(t.created_at.replace("Z", "+00:00"))
        day_key = dt_obj.date()
        event_intensity = day_to_event_intensity.get(day_key, 0.0)

        p_retweet = clamp(0.10 + 0.30 * event_intensity, 0.05, 0.40)
        p_reply = clamp(0.08 + 0.15 * event_intensity, 0.05, 0.25)
        p_quote = clamp(0.04 + 0.10 * event_intensity, 0.02, 0.15)

        r = rng.random()
        selected_type = None
        cum = p_retweet
        if r < cum:
            selected_type = "retweeted"
        else:
            cum += p_reply
            if r < cum:
                selected_type = "replied_to"
            else:
                cum += p_quote
                if r < cum:
                    selected_type = "quoted"

        # Default conversation_id (sometimes None)
        if rng.random() < 0.10:
            t.conversation_id = None
        else:
            t.conversation_id = t.tweet_id

        if selected_type and i > 0:
            ref_idx = rng.randint(max(0, i - 150), i - 1)
            ref_tweet = tweets[ref_idx]
            t.referenced_tweets = [{"type": selected_type, "id": ref_tweet.tweet_id}]

            if selected_type == "replied_to":
                t.in_reply_to_status_id = ref_tweet.tweet_id
                t.in_reply_to_user_id = ref_tweet.author_id
                t.conversation_id = ref_tweet.conversation_id or ref_tweet.tweet_id
                # Sometimes mention the author being replied to
                if rng.random() < 0.6:
                    # We don't store handles per author in the TweetData, so simulate an @mention
                    t.entities["mentions"].append(f"@user_{ref_tweet.author_id[-4:]}")
                    t.entities["mentions"] = list(dict.fromkeys(t.entities["mentions"]))
            elif selected_type == "quoted":
                t.conversation_id = ref_tweet.conversation_id or ref_tweet.tweet_id
                if rng.random() < 0.7:
                    t.entities["urls"].append(f"https://twitter.example.com/{ref_tweet.author_id}/status/{ref_tweet.tweet_id}")
            else:  # retweeted
                t.conversation_id = ref_tweet.conversation_id or ref_tweet.tweet_id

        # Occasionally add more media keys
        if rng.random() < 0.12:
            n_media = rng.randint(1, 3)
            existing = t.attachments_media_keys or []
            t.attachments_media_keys = list(dict.fromkeys(existing + [f"media_key_{uuid.uuid4().hex[:8]}" for _ in range(n_media)]))

        # Sometimes ensure edit history includes original id
        if t.edit_history_tweet_ids is not None and rng.random() < 0.5:
            t.edit_history_tweet_ids = list(dict.fromkeys([t.tweet_id] + t.edit_history_tweet_ids))

    # Prepare JSON data (list only)
    out_list = [asdict(t) for t in tweets]

    # Default output path if not provided
    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_merchant = merchant_name.lower().replace(" ", "")
        out_json_path = f"tweets_{safe_merchant}_{start_date}_to_{end_date}_{ts}.json".replace(":", "-")

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example ---------------------------

if __name__ == "__main__":
    # Example usage
    merchant = "Kingfisher"
    path = generate_fake_tweets_json(
        merchant_name=merchant,
        start_date="2020-01-01",
        end_date="2024-12-31",
        n_tweets=1000000,
        trend_plan=[
            {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach"},
            {"month": "2023-09", "intensity": 0.60, "label": "normal"},
            {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch"},
        ],
        seed=42,
        out_json_path=None  # auto-named file
    )
    print(f"Saved tweets JSON to: {path}")