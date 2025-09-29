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
class ReviewRecord:
    review_id: str
    merchant: str
    product_id: str
    product_name: str
    sku: str
    user_id: str
    username: str
    title: str
    body: str
    rating: int  # 1..5
    sentiment_score: float  # -1..1
    sentiment_label: str
    created_at: str
    verified_purchase: bool
    country: str
    language: str
    platform: str  # "web" | "ios" | "android"
    helpful_votes: int
    unhelpful_votes: int
    views: int
    images: List[str]
    tags: List[str]
    merchant_response: Optional[str]
    order_id: Optional[str]
    purchase_date: Optional[str]
    return_status: Optional[str]  # "none" | "returned" | "exchanged"
    event_label: Optional[str]


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


def make_review_id(counter: int) -> str:
    return f"r_{base36(counter)}{uuid.uuid4().hex[:4]}"


def make_order_id() -> str:
    return f"o_{uuid.uuid4().hex[:10]}"


def make_user_id_username(rng: random.Random) -> Tuple[str, str]:
    uhex = uuid.uuid4().hex[:10]
    user_id = f"u_{uhex}"
    username = f"user_{uhex[:6]}"
    return user_id, username


def sample_language_country(rng: random.Random) -> Tuple[str, str]:
    pool = [
        ("en", "GB", 0.45),
        ("en", "US", 0.30),
        ("en", "IE", 0.04),
        ("en", "CA", 0.04),
        ("en", "AU", 0.04),
        ("fr", "FR", 0.04),
        ("de", "DE", 0.04),
        ("es", "ES", 0.03),
        ("it", "IT", 0.02)
    ]
    langs = [(l, c) for (l, c, _) in pool]
    weights = np.array([w for (_, _, w) in pool], dtype=float)
    weights = weights / weights.sum()
    idx = int(np.random.choice(len(pool), p=weights))
    return pool[idx][0], pool[idx][1]


def sample_platform(rng: random.Random) -> str:
    return rng.choices(["web", "ios", "android"], weights=[0.55, 0.20, 0.25], k=1)[0]


def slugify(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    slug = re.sub(r"\s+", "-", slug.strip()).lower()
    return slug[:60] if slug else "review"


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
        share = rng.uniform(0.02, 0.15) if rng.random() < 0.5 else None  # optionally allocate % of reviews
        e = {"month": month_str, "intensity": round(float(intensity), 2), "label": label}
        if share is not None:
            e["reviews"] = round(share, 3)
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
    day_of_week = np.array([d.weekday() for d in days], dtype=float)  # 0..6
    day_of_year = np.array([(d - date(d.year, 1, 1)).days for d in days], dtype=float)

    # Baseline activity: weekly seasonality (weekends slightly higher for consumer reviews), annual mild
    base = np.ones(n)
    weekly = 0.12 * np.sin((day_of_week - 5) / 6.0 * 2 * np.pi)  # peak on weekends
    annual = 0.08 * np.sin((day_of_year / 365.25) * 2 * np.pi)
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


def parse_share(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip().replace(" ", "")
        if s.endswith("%"):
            try:
                return clamp(float(s[:-1]) / 100.0, 0.0, 1.0)
            except:
                return None
        try:
            num = float(s)
            return num if num <= 1 else clamp(num / 100.0, 0.0, 1.0)
        except:
            return None
    try:
        v = float(val)
        return v if v <= 1 else clamp(v / 100.0, 0.0, 1.0)
    except:
        return None


def apply_monthly_review_shares(
    days: List[date],
    base_weights: np.ndarray,
    trend_plan: List[Dict[str, Any]]
) -> np.ndarray:
    month_shares: Dict[str, float] = {}
    for e in trend_plan:
        m = e.get("month")
        if not m:
            continue
        share = parse_share(e.get("reviews"))
        if share is not None and share > 0:
            month_shares[m] = month_shares.get(m, 0.0) + share

    if not month_shares:
        return base_weights

    sum_shares = sum(month_shares.values())
    if sum_shares > 0.95:
        for k in list(month_shares.keys()):
            month_shares[k] = month_shares[k] / sum_shares * 0.95

    months = [f"{d.year}-{d.month:02d}" for d in days]
    month_to_total = {m: 0.0 for m in set(months)}
    for w, m in zip(base_weights, months):
        month_to_total[m] += float(w)

    specified = set(month_shares.keys())
    other_total = sum(w for w, m in zip(base_weights, months) if m not in specified)
    remaining_share = max(1e-9, 1.0 - sum(month_shares.values()))

    new_w = np.zeros_like(base_weights)
    for m, share in month_shares.items():
        denom = month_to_total.get(m, 0.0)
        if denom <= 0:
            continue
        for i, dm in enumerate(months):
            if dm == m:
                new_w[i] = base_weights[i] / denom * share

    if other_total > 0 and remaining_share > 0:
        for i, dm in enumerate(months):
            if dm not in specified:
                new_w[i] = base_weights[i] / other_total * remaining_share

    s = float(new_w.sum())
    if s <= 0:
        return base_weights
    return new_w / s


# --------------------------- Products and Users ---------------------------

def build_product_catalog(merchant: str, n_products: int, rng: random.Random):
    cats = ["Tools", "Garden", "Decor", "Lighting", "Plumbing", "Electrical", "Storage", "Outdoor", "Paint", "Flooring"]
    products = []
    for i in range(n_products):
        letter = chr(ord('A') + (i % 26))
        product_name = f"{merchant} Product {letter}"
        product_id = f"p_{base36(i+1)}{uuid.uuid4().hex[:3]}"
        sku = f"SKU-{uuid.uuid4().hex[:6].upper()}"
        category = rng.choice(cats)
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "sku": sku,
            "category": category
        })
    return products


def build_user_pool(n_users: int, rng: random.Random):
    users = []
    for _ in range(n_users):
        user_id, username = make_user_id_username(rng)
        users.append((user_id, username))
    return users


# --------------------------- Content generation ---------------------------

def review_templates(rating: int, product_name: str, merchant: str, rng: random.Random) -> Tuple[str, str, List[str]]:
    tags = []
    if rating >= 5:
        titles = ["Excellent!", "Exceeded expectations", "Perfect purchase", "Highly recommend"]
        bodies = [
            f"Absolutely love this {product_name} from {merchant}. Great quality and value.",
            f"This is exactly what I needed. Build quality is top-notch and works as advertised.",
            f"Super happy with the performance. Would buy again from {merchant}.",
            f"Solid product. Easy to use and reliable so far."
        ]
        tags = ["quality", "value", "recommend", "durable"]
    elif rating == 4:
        titles = ["Very good", "Worth it", "Happy with it", "Solid choice"]
        bodies = [
            f"The {product_name} is pretty good. Minor nitpicks but overall happy.",
            f"Good value for the price. {merchant} delivered on time and as expected.",
            f"Works well and seems sturdy. A few small issues but nothing major.",
            f"Would recommend. Installation/usage was straightforward."
        ]
        tags = ["value", "quality", "reliable"]
    elif rating == 3:
        titles = ["It's okay", "Average", "Decent but could improve", "Mixed feelings"]
        bodies = [
            f"The {product_name} is fine for basic needs. Not amazing, not terrible.",
            f"Average experience. Some pros and cons to consider.",
            f"Build is acceptable. Performance meets expectations but didn't wow me.",
            f"Does the job but there are a few quirks."
        ]
        tags = ["average", "ok", "basic"]
    elif rating == 2:
        titles = ["Disappointed", "Not great", "Could be better", "Issues encountered"]
        bodies = [
            f"Had some problems with the {product_name}. Support from {merchant} was mixed.",
            f"Quality feels lacking. Might work for light use but not for heavy tasks.",
            f"Expected more at this price. Considering a return.",
            f"Setup was frustrating and the instructions were unclear."
        ]
        tags = ["poor-quality", "support", "frustrating"]
    else:  # 1 star
        titles = ["Very poor", "Do not recommend", "Waste of money", "Terrible experience"]
        bodies = [
            f"Serious issues with the {product_name}. Regret buying from {merchant}.",
            f"Broke quickly and customer service was unhelpful.",
            f"Completely missed expectations. Returning it.",
            f"Would not buy again. Overall bad experience."
        ]
        tags = ["defective", "return", "avoid"]

    title = rng.choice(titles)
    body = rng.choice(bodies)
    return title, body, tags


def merchant_response_for_rating(rating: int, rng: random.Random) -> Optional[str]:
    if rating <= 2 and rng.random() < 0.40:
        return rng.choice([
            "We’re sorry to hear this. Please contact our support so we can help.",
            "Thanks for the feedback. We’ll follow up to make this right.",
            "Apologies for the experience. We’re investigating and will reach out."
        ])
    if rating >= 5 and rng.random() < 0.20:
        return rng.choice([
            "Thanks for the kind words! We appreciate your support.",
            "So glad you’re enjoying it. Thanks for choosing us!",
            "Thank you! We’re happy the product worked out."
        ])
    if rating == 3 and rng.random() < 0.10:
        return rng.choice([
            "Thanks for the balanced feedback. We’ll aim to improve.",
            "Appreciate the review. We’ll share this with our team."
        ])
    return None


# --------------------------- Sentiment helpers ---------------------------

def label_from_score(s: float) -> str:
    if s <= -0.2:
        return "negative"
    elif s >= 0.2:
        return "positive"
    return "neutral"


# --------------------------- Main generator ---------------------------

def generate_fake_reviews_json(
    merchant_name: str,
    n_products: int = 8,
    merchant_score: float = 4.2,  # target overall average stars (1..5)
    n_reviews: int = 20000,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None,
    save_summary: bool = True
) -> str:
    """
    Generate a fake customer reviews dataset for a merchant across products and time.

    trend_plan example:
      [
        {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach", "reviews": "5%"},
        {"month": "2023-09", "intensity": 0.60, "label": "normal", "reviews": "10%"},
        {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "reviews": "20%"}
      ]
    'reviews' can be share as 0.1 or "10%". Remaining share is distributed by baseline seasonality.
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    # Final review count around requested; at least 1000
    final_n = max(int(round(n_reviews * rng.uniform(0.9, 1.1))), 1000)

    # Trend plan
    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    # Build daily intensity for review volume and day events
    days, intensity, events_by_day = build_daily_intensity(start, end, trend_plan, rng)
    weights = intensity / (intensity.sum() + 1e-12)
    weights = apply_monthly_review_shares(days, weights, trend_plan)
    reviews_per_day = np.random.multinomial(final_n, weights)

    # Product catalog and popularity shares
    products = build_product_catalog(merchant_name, n_products, rng)
    prod_weights = np.random.dirichlet(alpha=np.ones(n_products) * rng.uniform(0.7, 2.2))
    prod_weights = prod_weights / prod_weights.sum()

    # Calibrate product-level mean ratings to hit target merchant_score (normalized m in 0..1)
    target_stars = float(clamp(merchant_score, 1.0, 5.0))
    m_target = (target_stars - 1.0) / 4.0  # 0..1
    # Sample small product-level deviations
    eps = np.random.normal(loc=0.0, scale=rng.uniform(0.03, 0.08), size=n_products)
    m_products = m_target + eps
    # Enforce weighted mean = target
    delta = float(np.dot(prod_weights, m_products) - m_target)
    m_products = m_products - delta  # shift all equally
    # Clamp to [0.05, 0.95] and re-center slightly if clipped pushed things
    m_products = np.clip(m_products, 0.05, 0.95)
    # Product concentrations (higher -> tighter around mean)
    c_products = np.random.uniform(6.0, 18.0, size=n_products)
    # Slight correlation: better products can have higher concentration
    c_products = c_products + (m_products - m_target) * rng.uniform(10.0, 18.0)
    c_products = np.clip(c_products, 5.0, 30.0)

    # Users pool (duplicates simulate repeat reviewers)
    n_users = max(500, min(4000, int(final_n / rng.uniform(5.0, 20.0))))
    users = build_user_pool(n_users, rng)

    # Images pool
    image_pool = [
        "https://img.example.com/rev1.jpg",
        "https://img.example.com/rev2.jpg",
        "https://img.example.com/rev3.jpg",
        "https://img.example.com/rev4.jpg",
        "https://img.example.com/rev5.jpg",
    ]

    reviews: List[ReviewRecord] = []
    counter = 1

    # For each day allocate reviews, then across products with noise
    day_idx = 0
    for d, count in zip(days, reviews_per_day):
        if count == 0:
            day_idx += 1
            continue

        day_events = events_by_day.get(d, [])
        # Aggregate day sentiment shift (-1..1) and event label
        driver_event = None
        day_shift = 0.0
        if day_events:
            w = np.array([e["gaussian"] for e in day_events], dtype=float)
            w = w / (w.sum() + 1e-12)
            day_shift = float(sum(w * np.array([e["shift"] for e in day_events], dtype=float)))
            driver_event = max(day_events, key=lambda e: e["gaussian"])["label"]

        # Add small noise to product weights day-by-day
        noise = np.random.normal(0.0, 0.08, size=n_products)
        w_day = prod_weights * (1.0 + noise)
        w_day = np.clip(w_day, 0.001, None)
        w_day = w_day / w_day.sum()

        # Allocate per product this day
        per_prod = np.random.multinomial(count, w_day)

        for i_prod, n_p in enumerate(per_prod):
            if n_p == 0:
                continue

            prod = products[i_prod]
            # Adjust mean rating for this day by event shift (small effect)
            # Map day_shift [-1,1] -> delta in mean rating space [-0.12, 0.12]
            delta_m = clamp(day_shift * rng.uniform(0.08, 0.14), -0.20, 0.20)
            m_day = clamp(m_products[i_prod] + delta_m, 0.02, 0.98)
            c_day = float(clamp(c_products[i_prod] + rng.uniform(-1.5, 1.5), 4.0, 35.0))

            for _ in range(n_p):
                # Draw continuous rating in [0,1] using Beta(m_day * c, (1-m_day)*c)
                alpha = max(0.5, m_day * c_day)
                beta = max(0.5, (1.0 - m_day) * c_day)
                r = random.betavariate(alpha, beta)
                # Map to 1..5 stars with slight jitter
                stars_cont = 1.0 + 4.0 * r + rng.uniform(-0.15, 0.15)
                stars = int(clamp(round(stars_cont), 1, 5))

                # Sentiment score from stars, with small noise and day shift influence
                s = clamp(((stars - 3) / 2.0) + rng.uniform(-0.10, 0.10) + day_shift * 0.05, -1.0, 1.0)
                s_label = label_from_score(s)

                # Content
                title, body, taglist = review_templates(stars, prod["product_name"], merchant_name, rng)

                # Timestamp
                # Distribution: morning/afternoon/evening
                rr = rng.random()
                if rr < 0.45:
                    hour = int(clamp(rng.gauss(11.5, 2.5), 7, 16))
                elif rr < 0.85:
                    hour = int(clamp(rng.gauss(19.5, 2.0), 14, 23))
                else:
                    hour = int(clamp(rng.gauss(9.0, 1.5), 6, 12))
                minute = rng.randint(0, 59)
                second = rng.randint(0, 59)
                created_dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)

                # User and platform
                user_id, username = users[rng.randint(0, len(users) - 1)]
                lang, country = sample_language_country(rng)
                platform = sample_platform(rng)

                # Verified purchase and order
                verified = rng.random() < 0.86
                order_id = make_order_id() if verified and rng.random() < 0.95 else None
                purchase_date = None
                if order_id:
                    days_before = rng.randint(2, 60)
                    pd = created_dt - timedelta(days=days_before, hours=rng.randint(0, 23))
                    purchase_date = isoformat_dt(pd)

                # Views and votes (heavy-tailed)
                views = int(min(50000, rng.lognormvariate(mu=math.log(50 + 1e-9), sigma=1.2) * (1.0 + abs(day_shift)*0.4)))
                helpful = int(max(0, rng.gauss(views * rng.uniform(0.005, 0.03), rng.uniform(1.0, 8.0))))
                unhelpful = int(max(0, rng.gauss(views * rng.uniform(0.001, 0.01), rng.uniform(0.5, 4.0))))
                # More engagement for extreme ratings
                if stars in (1, 5):
                    helpful = int(helpful * rng.uniform(1.1, 1.6))
                    views = int(views * rng.uniform(1.05, 1.2))

                # Images
                n_imgs = rng.choices([0, 1, 2, 3], weights=[0.78, 0.15, 0.05, 0.02], k=1)[0]
                if stars >= 5 and rng.random() < 0.15:
                    n_imgs = max(n_imgs, 1)
                if stars <= 2 and rng.random() < 0.10:
                    n_imgs = max(n_imgs, 1)
                images = rng.sample(image_pool, k=min(n_imgs, len(image_pool)))

                # Merchant response and return status
                mresp = merchant_response_for_rating(stars, rng)
                ret_status = None
                if stars <= 2 and rng.random() < 0.20:
                    ret_status = rng.choice(["returned", "exchanged"])
                else:
                    ret_status = "none"

                review = ReviewRecord(
                    review_id=make_review_id(counter),
                    merchant=merchant_name,
                    product_id=prod["product_id"],
                    product_name=prod["product_name"],
                    sku=prod["sku"],
                    user_id=user_id,
                    username=username,
                    title=title,
                    body=body,
                    rating=stars,
                    sentiment_score=round(float(s), 4),
                    sentiment_label=s_label,
                    created_at=isoformat_dt(created_dt),
                    verified_purchase=verified,
                    country=country,
                    language=lang,
                    platform=platform,
                    helpful_votes=helpful,
                    unhelpful_votes=unhelpful,
                    views=views,
                    images=images,
                    tags=taglist,
                    merchant_response=mresp,
                    order_id=order_id,
                    purchase_date=purchase_date,
                    return_status=ret_status,
                    event_label=driver_event
                )
                reviews.append(review)
                counter += 1

        day_idx += 1

    # Sort by created_at and enforce strictly increasing if duplicates
    reviews.sort(key=lambda r: r.created_at)
    last_dt = None
    for r in reviews:
        cur_dt = datetime.fromisoformat(r.created_at.replace("Z", "+00:00"))
        if last_dt is not None and cur_dt <= last_dt:
            cur_dt = last_dt + timedelta(seconds=1)
            r.created_at = isoformat_dt(cur_dt)
        last_dt = cur_dt

    # Prepare output
    out_list = [asdict(r) for r in reviews]

    # Default output path
    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = merchant_name.lower().replace(" ", "")
        out_json_path = f"reviews_{safe}_{start_date}_to_{end_date}_{ts}.json".replace(":", "-")

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    # Optional summary
    if save_summary:
        # Compute realized averages overall and per product
        prod_stats: Dict[str, Dict[str, Any]] = {}
        for p in products:
            prod_stats[p["product_id"]] = {
                "product_name": p["product_name"],
                "sku": p["sku"],
                "category": p["category"],
                "count": 0,
                "avg_rating": 0.0,
                "helpful_votes": 0,
                "views": 0
            }
        total_rating_sum = 0.0
        for r in reviews:
            ps = prod_stats[r.product_id]
            ps["count"] += 1
            ps["avg_rating"] += r.rating
            ps["helpful_votes"] += r.helpful_votes
            ps["views"] += r.views
            total_rating_sum += r.rating
        for pid, ps in prod_stats.items():
            if ps["count"] > 0:
                ps["avg_rating"] = round(ps["avg_rating"] / ps["count"], 3)

        summary = {
            "merchant": merchant_name,
            "start_date": start_date,
            "end_date": end_date,
            "n_products": n_products,
            "n_reviews": len(reviews),
            "target_merchant_score": round(target_stars, 3),
            "realized_merchant_score": round(total_rating_sum / max(1, len(reviews)), 3),
            "products": [
                {
                    "product_id": pid,
                    "product_name": ps["product_name"],
                    "sku": ps["sku"],
                    "category": ps["category"],
                    "review_count": ps["count"],
                    "avg_rating": ps["avg_rating"],
                    "review_share": round(ps["count"] / max(1, len(reviews)), 4)
                }
                for pid, ps in prod_stats.items()
            ],
            "trend_plan": trend_plan
        }
        sum_path = out_json_path.replace(".json", "_summary.json")
        with open(sum_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example usage ---------------------------

if __name__ == "__main__":
    merchant = "Kingfisher"
    path = generate_fake_reviews_json(
        merchant_name=merchant,
        n_products=8,
        merchant_score=4.2,  # target overall stars
        n_reviews=25000,
        start_date="2020-01-01",
        end_date="2024-12-31",
        trend_plan=[
            {"month": "2023-06", "intensity": 0.80, "label": "spike - data breach", "reviews": "6%"},
            {"month": "2023-09", "intensity": 0.60, "label": "normal", "reviews": "9%"},
            {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "reviews": "22%"},
        ],
        seed=42,
        out_json_path=None,
        save_summary=True
    )
    print("Saved reviews JSON to:", path)