import random
import uuid
import json
import math
import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone, date
import numpy as np


@dataclass
class NewsArticle:
    article_id: str
    merchant: str
    title: str
    subtitle: Optional[str]
    content: str
    published_at: str
    updated_at: Optional[str]
    publisher: str
    source_domain: str
    source_rank: int
    url: str
    amp_url: Optional[str]
    author: str
    authors: List[str]
    section: str
    categories: List[str]
    keywords: List[str]
    named_entities: Dict[str, List[str]]
    external_links: List[str]
    image_urls: List[str]
    language: str
    region: str
    country: str
    is_paywalled: bool
    is_opinion: bool
    is_exclusive: bool
    is_breaking: bool
    story_cluster_id: Optional[str]
    story_topic: Optional[str]
    google_trends_day_index: int
    pageviews: int
    shares: int
    comments: int
    reading_time_min: float
    sentiment_score: float
    sentiment_label: str
    risk_score: float
    hot_score: float  # composite popularity score


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


def make_article_id(counter: int) -> str:
    return f"n_{base36(counter)}{uuid.uuid4().hex[:5]}"


def slugify(title: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE)
    slug = re.sub(r"\s+", "-", slug.strip()).lower()
    return slug[:80] if slug else "article"


# --------------------------- Trend/Events ---------------------------

def event_sentiment_shift(label: str, rng: random.Random) -> float:
    l = label.lower()
    neg_keys = ["breach", "fraud", "lawsuit", "boycott", "downtime", "outage", "recall", "regulatory", "fine", "leak", "crisis", "scandal", "layoff"]
    pos_keys = ["new product", "launch", "award", "partnership", "expansion", "feature", "investment", "earnings beat", "milestone", "hiring"]
    if any(k in l for k in neg_keys):
        return rng.uniform(-0.9, -0.6)
    if any(k in l for k in pos_keys):
        return rng.uniform(0.45, 0.8)
    return rng.uniform(-0.05, 0.05)


def random_trend_plan(start: date, end: date, rng: random.Random) -> List[Dict[str, Any]]:
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    month_indices = sorted(rng.sample(range(total_months), k=rng.randint(3, 6)))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership", "seasonal sale", "earnings beat"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall", "regulatory fine", "missed earnings"]
    neutral_labels = ["normal", "promo period", "seasonal buzz"]

    plan = []
    for idx in month_indices:
        y = start.year + (start.month - 1 + idx) // 12
        m = (start.month - 1 + idx) % 12 + 1
        month_str = f"{y}-{m:02d}"
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels)
            intensity = rng.uniform(0.65, 0.95)
        elif r < 0.70:
            label = rng.choice(pos_labels)
            intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neutral_labels)
            intensity = rng.uniform(0.45, 0.75)
        share = rng.uniform(0.02, 0.15) if rng.random() < 0.6 else None
        e = {"month": month_str, "intensity": round(float(intensity), 2), "label": label}
        if share is not None:
            e["articles"] = round(share, 3)
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

    # Baseline + seasonality
    base = np.ones(n)
    weekly = 0.22 * np.sin((day_of_week - 1) / 6.0 * 2 * np.pi)  # news busier midweek
    annual = 0.12 * np.sin((day_of_year / 365.25) * 2 * np.pi)   # some annual seasonality
    intensity = base + weekly + annual
    intensity = np.clip(intensity, 0.1, None)

    events_by_day: Dict[date, List[Dict[str, Any]]] = {d: [] for d in days}
    for e in trend_plan:
        e_month = parse_date_str(str(e["month"]))
        e_center = date(e_month.year, e_month.month, 15)
        e_intensity = float(e.get("intensity", 0.6))
        e_label = str(e.get("label", "normal"))
        sigma_days = rng.randint(6, 20)
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


def parse_share(val) -> Optional[float]:
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


def apply_monthly_article_shares(
    days: List[date],
    base_weights: np.ndarray,
    trend_plan: List[Dict[str, Any]]
) -> np.ndarray:
    month_shares: Dict[str, float] = {}
    for e in trend_plan:
        m = e.get("month")
        if not m:
            continue
        share = parse_share(e.get("articles") or e.get("posts"))
        if share is not None and share > 0:
            month_shares[m] = month_shares.get(m, 0.0) + share

    if not month_shares:
        return base_weights

    sum_shares = sum(month_shares.values())
    if sum_shares > 0.95:
        for k in list(month_shares.keys()):
            month_shares[k] = month_shares[k] / sum_shares * 0.95

    months = [f"{d.year}-{d.month:02d}" for d in days]
    unique_months = list(dict.fromkeys(months))
    month_to_total = {m: 0.0 for m in unique_months}
    for w, m in zip(base_weights, months):
        month_to_total[m] += float(w)

    specified_months = set(month_shares.keys())
    other_total = sum(w for w, m in zip(base_weights, months) if m not in specified_months)
    remaining_share = max(1e-9, 1.0 - sum(month_shares.values()))

    new_w = np.zeros_like(base_weights)
    for m, share in month_shares.items():
        if m not in month_to_total or month_to_total[m] == 0:
            continue
        denom = month_to_total[m]
        for i, dm in enumerate(months):
            if dm == m:
                new_w[i] = base_weights[i] / denom * share

    if other_total > 0 and remaining_share > 0:
        for i, dm in enumerate(months):
            if dm not in specified_months:
                new_w[i] = base_weights[i] / other_total * remaining_share

    s = float(new_w.sum())
    if s <= 0:
        return base_weights
    return new_w / s


# --------------------------- Sources and content ---------------------------

def publishers_catalog():
    # name, domain, region, country, base_weight, rank (lower is "bigger")
    pubs = [
        ("BBC News", "bbc.co.uk", "UK", "GB", 1.2, 5),
        ("The Guardian", "theguardian.com", "UK", "GB", 1.0, 12),
        ("Reuters", "reuters.com", "Global", "US", 1.1, 10),
        ("Bloomberg", "bloomberg.com", "Global", "US", 0.9, 15),
        ("Financial Times", "ft.com", "UK", "GB", 0.8, 20),
        ("Sky News", "news.sky.com", "UK", "GB", 0.7, 28),
        ("The Telegraph", "telegraph.co.uk", "UK", "GB", 0.6, 30),
        ("CNBC", "cnbc.com", "US", "US", 0.7, 35),
        ("WSJ", "wsj.com", "US", "US", 0.6, 25),
        ("CNN Business", "edition.cnn.com", "US", "US", 0.6, 40),
        ("TechCrunch", "techcrunch.com", "US", "US", 0.5, 60),
        ("The Verge", "theverge.com", "US", "US", 0.45, 70),
        ("Wired", "wired.com", "US", "US", 0.4, 75),
        ("The Register", "theregister.com", "UK", "GB", 0.45, 80),
        ("Dark Reading", "darkreading.com", "US", "US", 0.3, 110),
        ("BleepingComputer", "bleepingcomputer.com", "US", "US", 0.3, 120),
        ("ZDNet", "zdnet.com", "US", "US", 0.35, 100),
        ("Retail Gazette", "retailgazette.co.uk", "UK", "GB", 0.45, 150),
        ("Retail Week", "retail-week.com", "UK", "GB", 0.35, 180),
        ("City A.M.", "cityam.com", "UK", "GB", 0.4, 140),
        ("Investors Chronicle", "investorschronicle.co.uk", "UK", "GB", 0.25, 220),
        ("MarketWatch", "marketwatch.com", "US", "US", 0.5, 90),
        ("Yahoo Finance", "finance.yahoo.com", "US", "US", 0.6, 85),
        ("Business Insider", "businessinsider.com", "US", "US", 0.55, 95),
    ]
    return pubs


def choose_publisher(event_label: Optional[str], rng: random.Random):
    pubs = publishers_catalog()
    # Adjust weights by event
    # Cyber/incident -> security outlets + global wires
    # Launch/product -> tech outlets + wires
    # Earnings/finance -> finance/business outlets
    adj = []
    for name, domain, region, country, w, rank in pubs:
        w_adj = w
        if event_label:
            l = event_label.lower()
            if any(k in l for k in ["breach", "leak", "outage", "security"]):
                if domain in {"darkreading.com", "bleepingcomputer.com", "theregister.com"}:
                    w_adj *= 2.0
                if domain in {"reuters.com", "bbc.co.uk", "theguardian.com"}:
                    w_adj *= 1.4
            if any(k in l for k in ["launch", "product", "feature"]):
                if domain in {"techcrunch.com", "theverge.com", "wired.com"}:
                    w_adj *= 2.0
                if domain in {"reuters.com", "bbc.co.uk"}:
                    w_adj *= 1.3
            if any(k in l for k in ["earnings", "investment", "partnership", "award", "regulatory", "fine"]):
                if domain in {"ft.com", "bloomberg.com", "cnbc.com", "marketwatch.com", "finance.yahoo.com", "businessinsider.com"}:
                    w_adj *= 1.8
        adj.append((name, domain, region, country, max(0.05, w_adj), rank))
    weights = np.array([w for (_, _, _, _, w, _) in adj], dtype=float)
    weights = weights / weights.sum()
    idx = int(rng.choices(range(len(adj)), weights=weights, k=1)[0])
    return adj[idx]  # tuple with adjusted weight and rank


def choose_language_region(rng: random.Random):
    # Mostly English, small chance other EU languages
    if rng.random() < 0.94:
        return "en", "UK", "GB"
    lang = rng.choice(["en", "fr", "de", "es", "it"])
    region = rng.choice(["EU", "UK", "US"])
    country = {"EU": "DE", "UK": "GB", "US": "US"}[region]
    return lang, region, country


def build_title_and_subtitle(merchant: str, sentiment_label: str, event_label: Optional[str], rng: random.Random) -> Tuple[str, Optional[str], str]:
    m = merchant
    topic = "General"
    pos = [
        "{m} unveils new product line amid positive outlook",
        "Strong demand sees {m} expand offering",
        "{m} announces partnership; shares react",
        "Upbeat sentiment around {m} as updates roll out",
        "{m} earns industry recognition"
    ]
    neu = [
        "What’s next for {m}? Analysts weigh in",
        "{m} in focus as market watches developments",
        "Roundup: recent moves by {m}",
        "Community discussion: {m} and the road ahead",
        "{m} outlines roadmap in latest briefing"
    ]
    neg = [
        "{m} hit by reported incident; responses underway",
        "Concerns rise over {m} after latest report",
        "Regulatory questions surround {m}",
        "Market pressure on {m} following news",
        "{m} faces customer backlash amid issues"
    ]
    subtitle_pool = [
        "Company statement addresses the situation and outlines next steps.",
        "Analysts caution that more data is needed to assess the impact.",
        "Industry observers point to broader sector trends.",
        "Stakeholders react as details continue to emerge.",
        "A closer look at the implications for consumers and investors."
    ]
    if event_label:
        el = event_label.lower()
        if any(k in el for k in ["breach", "leak", "outage", "lawsuit", "recall"]):
            neg += [
                "Security incident at {m} prompts investigation",
                "Legal questions for {m} after developments",
                "Service disruption impacts segment of {m} customers"
            ]
            topic = "Incident"
        if any(k in el for k in ["launch", "product", "feature"]):
            pos += [
                "{m} launches flagship product; early reviews roll in",
                "New feature from {m} aims to improve experience",
            ]
            topic = "Product"
        if any(k in el for k in ["earnings", "investment", "partnership", "award"]):
            pos += [
                "{m} posts earnings update; outlook discussed",
                "Investment news: {m} announces new initiative"
            ]
            topic = "Markets"
        if "normal" in el or "promo" in el or "seasonal" in el:
            topic = "General"

    if sentiment_label == "positive":
        title = rng.choice(pos).format(m=m)
    elif sentiment_label == "negative":
        title = rng.choice(neg).format(m=m)
    else:
        title = rng.choice(neu).format(m=m)

    subtitle = rng.choice(subtitle_pool) if rng.random() < 0.7 else None
    return title, subtitle, topic


def sample_time_of_day(rng: random.Random) -> Tuple[int, int, int]:
    r = rng.random()
    if r < 0.45:
        hour = int(clamp(rng.gauss(9.5, 2.0), 5, 14))
    elif r < 0.8:
        hour = int(clamp(rng.gauss(14.5, 2.5), 10, 19))
    else:
        hour = int(clamp(rng.gauss(20, 2.0), 16, 23))
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return hour, minute, second


def random_author(rng: random.Random) -> str:
    first = rng.choice(["Alex", "Sam", "Chris", "Jordan", "Taylor", "Morgan", "Jamie", "Lee", "Casey", "Drew", "Kiran", "Pat", "Reese", "Parker", "Ava", "Noah", "Liam", "Mia", "Ethan", "Sofia", "Zara", "Owen", "Iris", "Nate"])
    last = rng.choice(["Smith", "Johnson", "Brown", "Taylor", "Wilson", "Davies", "Evans", "Thomas", "Roberts", "Walker", "Hall", "Allen", "Young", "King", "Wright", "Hill", "Green", "Baker", "Adams", "Carter"])
    return f"{first} {last}"


def build_content(merchant: str, event_label: Optional[str], sentiment_label: str, external_pool: List[str], rng: random.Random) -> Tuple[str, List[str]]:
    # Build 2-4 short paragraphs with merchant mentions and 0-3 external links
    m = merchant
    base_sentences = [
        f"{m} featured prominently in sector discussions this week.",
        "Analysts noted a mix of headwinds and tailwinds affecting performance.",
        "Customer feedback appears varied, with several trends emerging.",
        "Industry peers are monitoring the situation closely.",
        "Management outlined priorities in a recent communication.",
        "Market dynamics continue to shape short-term expectations.",
        "Independent observers advise caution until more clarity emerges.",
        "Initial reactions from stakeholders indicate heightened interest."
    ]
    incident_sentences = [
        f"Reports indicate a possible security incident involving {m}.",
        f"Third-party sources referenced by {m} suggest mitigation steps are underway.",
        "Users reported intermittent issues, though the extent remains unclear.",
        "Experts recommend reviewing standard security practices."
    ]
    product_sentences = [
        f"{m} introduced a new product aimed at core customers.",
        "Early feedback highlights usability and performance considerations.",
        "The release aligns with a broader strategy to refresh the portfolio.",
        "Competitors may respond with comparable announcements."
    ]
    finance_sentences = [
        f"Recent filings from {m} show updates on revenue and guidance.",
        "Investor calls emphasized execution and capital allocation.",
        "Analysts provided differing viewpoints on valuation.",
        "Macro conditions were cited as both risk and opportunity."
    ]

    chosen = base_sentences[:]
    if event_label:
        el = event_label.lower()
        if any(k in el for k in ["breach", "leak", "outage", "lawsuit", "recall"]):
            chosen += incident_sentences
        if any(k in el for k in ["launch", "product", "feature"]):
            chosen += product_sentences
        if any(k in el for k in ["earnings", "investment", "partnership", "award", "regulatory", "fine"]):
            chosen += finance_sentences

    # Sentiment tint
    if sentiment_label == "positive":
        chosen += [
            "Commentary from several analysts skewed positive on near-term outlook.",
            "Early adoption metrics appear encouraging, according to initial checks."
        ]
    elif sentiment_label == "negative":
        chosen += [
            "There are lingering concerns about the scope and impact.",
            "Observers warned that further details may shift sentiment."
        ]
    else:
        chosen += [
            "It remains too early to draw definitive conclusions.",
            "Further updates are expected as the story develops."
        ]

    n_par = rng.randint(2, 4)
    paragraphs = []
    for _ in range(n_par):
        n_sent = rng.randint(2, 4)
        sents = rng.sample(chosen, k=min(n_sent, len(chosen)))
        paragraphs.append(" ".join(sents))
    # External links
    n_links = rng.choices([0, 1, 2, 3], weights=[0.5, 0.3, 0.15, 0.05], k=1)[0]
    links = rng.sample(external_pool, k=min(n_links, len(external_pool)))
    if links:
        paragraphs.append("Related coverage: " + " | ".join(links))
    content = "\n\n".join(paragraphs)
    return content, links


def extract_keywords(title: str, content: str, merchant: str) -> List[str]:
    stop = set(["the", "and", "for", "with", "this", "that", "are", "was", "but", "you", "any", "about", "into", "from",
                "your", "have", "has", "what", "who", "why", "how", "when", "where", "their", "them", "they", "she", "he",
                "its", "it's", "had", "were", "will", "would", "could", "should", "just", "new", "news", "update"])
    raw = ((title or "") + " " + (content or "")).lower()
    tokens = re.findall(r"[a-z0-9\$]{3,}", raw)
    uniq = []
    for t in tokens:
        if t in stop:
            continue
        if t not in uniq:
            uniq.append(t)
        if len(uniq) >= 15:
            break
    mt = merchant.lower().replace(" ", "")
    if mt not in uniq:
        uniq.append(mt)
    return uniq[:15]


def build_named_entities(merchant: str, rng: random.Random) -> Dict[str, List[str]]:
    orgs = [merchant, "CompetitorCo", "PartnerLabs", "RegulatoryBody", "IndustryGroup"]
    persons = [f"{rng.choice(['Alex','Sam','Chris','Jordan','Taylor','Ava','Noah'])} {rng.choice(['Smith','Brown','Taylor','Wilson','King'])}" for _ in range(3)]
    locs = rng.sample(["London", "New York", "Paris", "Berlin", "Manchester", "Dublin", "Madrid", "Milan"], k=3)
    return {"organizations": list(dict.fromkeys(orgs))[:5], "persons": persons, "locations": locs}


# --------------------------- Sentiment/Risk and Metrics ---------------------------

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


def generate_article_metrics(
    base_pop: float,
    event_intensity: float,
    sentiment_label: str,
    is_paywalled: bool,
    rng: random.Random
) -> Tuple[int, int, int, float, float]:
    # Pageviews heavy-tailed, boosted by event intensity, reduced if paywalled
    mult = 1.0 + 1.0 * abs(event_intensity)
    if is_paywalled:
        mult *= rng.uniform(0.55, 0.8)
    pv = int(min(5_000_000, rng.lognormvariate(mu=math.log(1500 * base_pop + 1e-9), sigma=1.2) * mult))

    # Shares and comments correlate sublinearly with pageviews
    shares = int(max(0, rng.gauss(0.015 * math.sqrt(pv + 1) * 100, 25)))
    comments = int(max(0, rng.gauss(0.008 * math.sqrt(pv + 1) * 100, 15)))

    # Reading time based on content length; here approximated from pv (we adjust later precisely)
    reading_time_min = rng.uniform(2.0, 6.5)

    # Hot score: combine pv, shares, comments with diminishing returns
    hot = float((pv ** 0.6) + 15 * (shares ** 0.5) + 10 * (comments ** 0.5))
    # Sentiment can nudge hot score (controversy/positivity)
    if sentiment_label == "negative":
        hot *= rng.uniform(1.02, 1.10)
    elif sentiment_label == "positive":
        hot *= rng.uniform(1.00, 1.06)

    return pv, shares, comments, reading_time_min, hot


# --------------------------- Story clustering ---------------------------

class StoryCluster:
    def __init__(self, cluster_id: str, title_seed: str, topic: str, capacity: int):
        self.cluster_id = cluster_id
        self.title_seed = title_seed
        self.topic = topic
        self.capacity = capacity
        self.members = 0

    def can_join(self):
        return self.members < self.capacity

    def join(self):
        self.members += 1


def vary_title(seed: str, rng: random.Random) -> str:
    # Slight variations to simulate rewrites
    suffixes = [
        "— what it means",
        "— key points",
        "as details emerge",
        "in focus",
        "explained",
        "timeline",
        "community reaction",
        "and what to watch"
    ]
    if rng.random() < 0.6:
        return seed
    sep = " " if rng.random() < 0.5 else ": "
    return seed + sep + rng.choice(suffixes)


# --------------------------- Main generator ---------------------------

def generate_fake_news_json(
    merchant_name: str,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    n_articles: int = 8000,
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None
) -> str:
    """
    Generate a fake news dataset related to a merchant across a time range with event-driven spikes.

    trend_plan: list of dicts, e.g.:
      [
        {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach", "articles": "5%"},
        {"month": "2023-09", "intensity": 0.60, "label": "normal"},
        {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "articles": 0.20}
      ]
    You can use 'articles' or 'posts' to allocate share for that month; remaining distributed by baseline.
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    final_n = max(int(round(n_articles * rng.uniform(0.9, 1.1))), 500)

    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    days, intensity, events_by_day = build_daily_intensity(start, end, trend_plan, rng)
    weights = intensity / (intensity.sum() + 1e-12)
    weights = apply_monthly_article_shares(days, weights, trend_plan)

    # Daily Google Trends-like index scaled 0..100
    daily_index = (weights / (weights.max() + 1e-12) * 100.0).round().astype(int)

    per_day = np.random.multinomial(final_n, weights)

    # External links pool
    external_pool = [
        "https://reuters.com/markets",
        "https://www.bloomberg.com/markets",
        "https://www.ft.com/companies/retail",
        "https://www.bbc.co.uk/news/business",
        "https://www.theguardian.com/business",
        "https://techcrunch.com/",
        "https://darkreading.com/",
        "https://bleepingcomputer.com/",
        "https://marketwatch.com/",
        "https://finance.yahoo.com/"
    ]

    # Image URLs pool
    image_pool = [
        "https://img.example.com/photo1.jpg",
        "https://img.example.com/photo2.jpg",
        "https://img.example.com/photo3.jpg",
        "https://img.example.com/photo4.jpg",
        "https://img.example.com/photo5.jpg",
    ]

    # Story clusters per day
    clusters_by_day: Dict[date, List[StoryCluster]] = {}

    articles: List[NewsArticle] = []
    counter = 1
    base_mu = rng.uniform(0.03, 0.12)  # slightly positive baseline

    # Loop days
    for day_idx, (d, count) in enumerate(zip(days, per_day)):
        if count == 0:
            continue

        day_events = events_by_day.get(d, [])
        g_index = int(daily_index[day_idx])

        # Initialize clusters for the day
        n_clusters = max(1, int(round(count * rng.uniform(0.05, 0.15))))
        clusters: List[StoryCluster] = []
        for _ in range(n_clusters):
            topic_label = None
            # Compute temporary sentiment for a cluster seed
            s_score_tmp, s_label_tmp, _, driver_event_tmp = compute_sentiment_and_risk(base_mu, day_events, rng)
            title_seed, _, topic_label = build_title_and_subtitle(merchant_name, s_label_tmp, driver_event_tmp, rng)
            cap = rng.randint(2, min(7, max(2, count // n_clusters + 2)))
            clusters.append(StoryCluster(cluster_id=f"story_{d.isoformat()}_{uuid.uuid4().hex[:6]}", title_seed=title_seed, topic=topic_label, capacity=cap))
        clusters_by_day[d] = clusters

        for _ in range(count):
            s_score, s_label, risk_score, driver_event = compute_sentiment_and_risk(base_mu, day_events, rng)

            # Publisher
            name, domain, region_p, country_p, _, rank = choose_publisher(driver_event, rng)

            # Language/region (bias to publisher country)
            if country_p == "GB":
                language, region, country = ("en", "UK", "GB")
            else:
                language, region, country = choose_language_region(rng)

            # Story cluster join/create
            story_cluster_id = None
            story_topic = None
            title = None
            subtitle = None
            # 60% chance join an existing cluster if available
            joined = False
            joinable = [c for c in clusters if c.can_join()]
            if joinable and rng.random() < 0.6:
                c = rng.choice(joinable)
                story_cluster_id = c.cluster_id
                story_topic = c.topic
                title = vary_title(c.title_seed, rng)
                c.join()
                joined = True

            if not joined:
                title, subtitle, story_topic = build_title_and_subtitle(merchant_name, s_label, driver_event, rng)
                # Optionally open a new small cluster
                if rng.random() < 0.15:
                    cap = rng.randint(2, 4)
                    new_c = StoryCluster(cluster_id=f"story_{d.isoformat()}_{uuid.uuid4().hex[:6]}", title_seed=title, topic=story_topic, capacity=cap)
                    new_c.join()
                    clusters.append(new_c)
                    story_cluster_id = new_c.cluster_id

            # Author(s)
            main_author = random_author(rng)
            coauthors = []
            if rng.random() < 0.20:
                coauthors = [random_author(rng)]
                if rng.random() < 0.15:
                    coauthors.append(random_author(rng))
            authors = [main_author] + coauthors

            # Content and links
            content, ext_links = build_content(merchant_name, driver_event, s_label, external_pool, rng)
            # Reading time updated based on content words (200 wpm)
            words = max(50, len(re.findall(r"\w+", content)))
            reading_time = round(words / 200.0 + rng.uniform(-0.2, 0.4), 1)
            reading_time = float(clamp(reading_time, 1.0, 12.0))

            # Section/Categories
            section = rng.choice(["Business", "Technology", "World", "Markets", "UK", "Retail", "Cybersecurity", "Opinion"])
            categories = list(dict.fromkeys([
                section,
                story_topic or "General",
                rng.choice(["Company News", "Analysis", "Breaking", "Explainer", "Interview", "Regulation", "Earnings"])
            ]))

            # Flags
            is_paywalled = (domain in {"ft.com", "wsj.com"}) and (rng.random() < 0.7)
            is_opinion = rng.random() < 0.12 or section == "Opinion"
            is_exclusive = rng.random() < 0.06
            is_breaking = rng.random() < (0.10 + 0.15 * (1 if driver_event and ("spike" in driver_event.lower() or "breach" in driver_event.lower() or "launch" in driver_event.lower()) else 0))

            # Metrics
            base_pop = rng.uniform(0.7, 1.5)
            pv, shares, comments, rt_min, hot = generate_article_metrics(base_pop, float(sum(abs(e["shift"]) * e["gaussian"] for e in day_events)), s_label, is_paywalled, rng)
            # Override reading_time if computed earlier
            reading_time_min = reading_time

            # Timestamps
            hour, minute, second = sample_time_of_day(rng)
            pub_dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)
            # Updated_at sometimes later same day
            updated_at = None
            if rng.random() < 0.25:
                upd_dt = pub_dt + timedelta(minutes=rng.randint(10, 480))
                updated_at = isoformat_dt(upd_dt)

            # URL, AMP
            slug = slugify(title)
            url = f"https://{domain}/{pub_dt.year}/{pub_dt.month:02d}/{pub_dt.day:02d}/{slug}"
            amp_url = url + "/amp" if rng.random() < 0.4 else None

            # Keywords/entities/images
            keywords = extract_keywords(title, content, merchant_name)
            named_entities = build_named_entities(merchant_name, rng)
            image_urls = rng.sample(image_pool, k=rng.randint(0, 2))

            # Google trends index for the day (0..100)
            g_idx = g_index

            # Build article
            article = NewsArticle(
                article_id=make_article_id(counter),
                merchant=merchant_name,
                title=title,
                subtitle=subtitle,
                content=content,
                published_at=isoformat_dt(pub_dt),
                updated_at=updated_at,
                publisher=name,
                source_domain=domain,
                source_rank=rank,
                url=url,
                amp_url=amp_url,
                author=main_author,
                authors=authors,
                section=section,
                categories=categories,
                keywords=keywords,
                named_entities=named_entities,
                external_links=ext_links,
                image_urls=image_urls,
                language=language,
                region=region,
                country=country,
                is_paywalled=is_paywalled,
                is_opinion=is_opinion,
                is_exclusive=is_exclusive,
                is_breaking=is_breaking,
                story_cluster_id=story_cluster_id,
                story_topic=story_topic,
                google_trends_day_index=g_idx,
                pageviews=pv,
                shares=shares,
                comments=comments,
                reading_time_min=reading_time_min,
                sentiment_score=round(float(s_score), 4),
                sentiment_label=s_label,
                risk_score=risk_score,
                hot_score=round(float(hot), 2)
            )
            articles.append(article)
            counter += 1

    # Sort and ensure strictly increasing published_at
    articles.sort(key=lambda a: a.published_at)
    last_dt = None
    for a in articles:
        cur_dt = datetime.fromisoformat(a.published_at.replace("Z", "+00:00"))
        if last_dt is not None and cur_dt <= last_dt:
            cur_dt = last_dt + timedelta(seconds=1)
            a.published_at = isoformat_dt(cur_dt)
        last_dt = cur_dt

    out_list = [asdict(a) for a in articles]

    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = merchant_name.lower().replace(" ", "")
        out_json_path = f"news_{safe}_{start_date}_to_{end_date}_{ts}.json".replace(":", "-")

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example usage ---------------------------

if __name__ == "__main__":
    merchant = "Kingfisher"
    path = generate_fake_news_json(
        merchant_name=merchant,
        start_date="2020-01-01",
        end_date="2024-12-31",
        n_articles=8000,
        trend_plan=[
            {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach", "articles": "5%"},
            {"month": "2023-09", "intensity": 0.60, "label": "normal", "articles": "8%"},
            {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "articles": "20%"},
        ],
        seed=42,
        out_json_path=None
    )
    print("Saved News JSON to:", path)