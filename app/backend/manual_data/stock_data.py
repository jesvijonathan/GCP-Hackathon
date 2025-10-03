import random
import uuid
import json
import math
import re
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone, date
import numpy as np


# --------------------------- Data classes ---------------------------

@dataclass
class PriceBar:
    date: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int
    vwap: float
    turnover: float
    return_log: float
    return_pct: float
    gap_return_log: float
    intra_return_log: float
    volatility_day: float
    event_label: Optional[str]
    event_intensity: float
    is_earnings: bool
    dividend: float
    split_ratio: Optional[str]
    market_cap: float
    ma5: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    rsi14: Optional[float] = None
    bb20_upper: Optional[float] = None
    bb20_lower: Optional[float] = None


@dataclass
class CorporateAction:
    date: str
    type: str  # "dividend" | "split"
    amount: Optional[float] = None
    currency: Optional[str] = None
    ratio: Optional[str] = None  # e.g., "2:1"
    note: Optional[str] = None


@dataclass
class EarningsEvent:
    date: str
    period: str  # e.g., "2023-Q2"
    eps_estimate: float
    eps_actual: float
    eps_surprise_pct: float
    revenue_estimate: float
    revenue_actual: float
    revenue_surprise_pct: float
    press_url: str
    call_url: str


# --------------------------- Utilities ---------------------------

def parse_date_str(s: str) -> date:
    if len(s) == 7:
        return date.fromisoformat(s + "-01")
    return date.fromisoformat(s)


def isoformat_date(dt: datetime) -> str:
    # Stocks typically use date granularity; keep ISO date
    return dt.date().isoformat()


def business_days(start: date, end: date) -> List[date]:
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:  # Mon-Fri
            days.append(cur)
        cur += timedelta(days=1)
    return days


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def base36(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "0"
    out = []
    while n:
        n, r = divmod(n, 36)
        out.append(chars[r])
    return "".join(reversed(out))


def parse_percent(val) -> Optional[float]:
    # Return as decimal (e.g., "10%" -> 0.10). Accept floats (<=1 means already decimal, >1 means percentage).
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val <= 1 else float(val) / 100.0
    if isinstance(val, str):
        s = val.strip().lower().replace(" ", "")
        if s.endswith("%"):
            try:
                return float(s[:-1]) / 100.0
            except:
                return None
        try:
            x = float(s)
            return x if x <= 1 else x / 100.0
        except:
            return None
    return None


def parse_multiplier(val) -> Optional[float]:
    # Accept "x2", "2x", "200%", 2.0 -> 2.0
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    s = s.replace(" ", "")
    if s.endswith("x"):
        try:
            return float(s[:-1])
        except:
            return None
    if s.startswith("x"):
        try:
            return float(s[1:])
        except:
            return None
    # percent as multiplier
    if s.endswith("%"):
        try:
            return 1.0 + float(s[:-1]) / 100.0
        except:
            return None
    try:
        return float(s)
    except:
        return None


# --------------------------- Trend / Events ---------------------------

def event_sentiment_shift(label: str, rng: random.Random) -> float:
    l = label.lower()
    neg = ["breach", "fraud", "lawsuit", "boycott", "downtime", "outage", "recall", "regulatory", "fine", "leak", "crisis", "scandal", "layoff", "miss"]
    pos = ["new product", "launch", "award", "partnership", "expansion", "feature", "investment", "earnings beat", "milestone", "hiring", "buyback"]
    if any(k in l for k in neg):
        return rng.uniform(-0.9, -0.6)
    if any(k in l for k in pos):
        return rng.uniform(0.4, 0.8)
    return rng.uniform(-0.05, 0.05)


def random_trend_plan(start: date, end: date, rng: random.Random) -> List[Dict[str, Any]]:
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    month_indices = sorted(rng.sample(range(total_months), k=rng.randint(3, 6)))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership", "buyback", "earnings beat"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall", "regulatory fine", "earnings miss"]
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
            car = rng.uniform(-0.18, -0.05)  # cumulative abnormal return that month
        elif r < 0.70:
            label = rng.choice(pos_labels)
            intensity = rng.uniform(0.55, 0.90)
            car = rng.uniform(0.04, 0.16)
        else:
            label = rng.choice(neutral_labels)
            intensity = rng.uniform(0.40, 0.70)
            car = rng.uniform(-0.02, 0.03)
        e = {"month": month_str, "intensity": round(float(intensity), 2), "label": label, "return": round(float(car), 3)}
        # occasionally include volume multiplier
        if rng.random() < 0.5:
            e["volume"] = rng.choice(["x1.3", "x1.6", "x2.0", "150%"])
        plan.append(e)
    return plan


def build_events_by_day(
    trading_days: List[date],
    trend_plan: List[Dict[str, Any]],
    rng: random.Random
) -> Tuple[Dict[date, List[Dict[str, Any]]], Dict[date, float], Dict[date, float]]:
    events_by_day: Dict[date, List[Dict[str, Any]]] = {d: [] for d in trading_days}
    intensity_by_day: Dict[date, float] = {d: 0.0 for d in trading_days}
    extra_drift_by_day: Dict[date, float] = {d: 0.0 for d in trading_days}  # log-return adjustments to meet CAR targets

    # Precompute days by month mapping
    by_month: Dict[str, List[date]] = {}
    for d in trading_days:
        key = f"{d.year}-{d.month:02d}"
        by_month.setdefault(key, []).append(d)

    for e in trend_plan:
        month_str = str(e.get("month"))
        if month_str not in by_month:
            continue
        days_in_month = by_month[month_str]
        if not days_in_month:
            continue
        center = date(parse_date_str(month_str).year, parse_date_str(month_str).month, 15)
        sigma_days = rng.randint(6, 18)
        # Compute gaussian per day
        gvals = np.array([math.exp(-((d - center).days ** 2) / (2 * sigma_days ** 2)) for d in days_in_month], dtype=float)
        if gvals.sum() <= 1e-9:
            gvals = np.ones(len(days_in_month), dtype=float)
        gnorm = gvals / gvals.sum()
        intensity = float(e.get("intensity", 0.6))
        label = str(e.get("label", "normal"))
        shift = event_sentiment_shift(label, rng)

        # Store per-day event influence
        for d, g, gn in zip(days_in_month, gvals, gnorm):
            events_by_day[d].append({"label": label, "gaussian": float(g), "shift": shift, "intensity": intensity})
            intensity_by_day[d] += intensity * float(g)

        # Apply monthly CAR target if provided
        car = parse_percent(e.get("return"))
        if car is not None and abs(car) > 0:
            # Convert to log-return target and distribute across month by gnorm
            r_log_total = math.log(1.0 + car)
            for d, gn in zip(days_in_month, gnorm):
                extra_drift_by_day[d] += float(r_log_total * gn)

    # Normalize intensity into ~0..1 scale
    max_intensity = max(intensity_by_day.values()) if intensity_by_day else 1.0
    for d in intensity_by_day:
        if max_intensity > 0:
            intensity_by_day[d] = float(clamp(intensity_by_day[d] / max_intensity, 0.0, 2.0))
        else:
            intensity_by_day[d] = 0.0

    return events_by_day, intensity_by_day, extra_drift_by_day


# --------------------------- Technicals ---------------------------

def moving_average(arr: np.ndarray, window: int) -> np.ndarray:
    if len(arr) == 0:
        return np.array([])
    w = min(window, len(arr))
    out = np.full(len(arr), np.nan)
    cumsum = np.cumsum(np.insert(arr, 0, 0.0))
    for i in range(w - 1, len(arr)):
        out[i] = (cumsum[i + 1] - cumsum[i + 1 - w]) / w
    return out


def bollinger_bands(arr: np.ndarray, window: int = 20, k: float = 2.0) -> Tuple[np.ndarray, np.ndarray]:
    out_up = np.full(len(arr), np.nan)
    out_lo = np.full(len(arr), np.nan)
    for i in range(window - 1, len(arr)):
        window_arr = arr[i - window + 1:i + 1]
        m = np.mean(window_arr)
        s = np.std(window_arr, ddof=0)
        out_up[i] = m + k * s
        out_lo[i] = m - k * s
    return out_up, out_lo


def rsi(arr: np.ndarray, period: int = 14) -> np.ndarray:
    out = np.full(len(arr), np.nan)
    if len(arr) < period + 1:
        return out
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    # Simple moving average version
    for i in range(period, len(arr)):
        avg_gain = np.mean(gains[i - period:i])
        avg_loss = np.mean(losses[i - period:i])
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


# --------------------------- Main generator ---------------------------

def generate_fake_stock_json(
    merchant_name: str,
    ticker: Optional[str] = None,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    base_price: float = 250.0,
    shares_outstanding: int = 2_000_000_000,  # shares, for market cap
    currency: str = "GBP",
    mu_annual: Optional[float] = None,  # expected annual log-return (e.g., 0.08). If None, inferred or randomized.
    sigma_annual: float = 0.35,  # annualized volatility of log returns
    avg_daily_volume: int = 5_000_000,
    target_end_price: Optional[float] = None,  # optional: approximate this by adjusting drift
    target_total_return: Optional[float] = None,  # e.g., 0.5 for +50%
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None
) -> str:
    """
    Generate a realistic-looking daily stock dataset with OHLCV, events, earnings, dividends, splits, and technicals.

    trend_plan items support:
      {
        "month": "YYYY-MM",
        "intensity": 0.0..1.0,  # raises volume/volatility around that month
        "label": "spike - data breach" | "spike - new product launch" | ...,
        "return": "+8%" | -0.12 | 0.05,  # optional cumulative abnormal return for that month
        "volume": "x2" | "200%" | 1.5,   # optional volume multiplier around that month
        "volatility": "+50%" | 1.3       # optional volatility multiplier around that month
      }

    target_end_price or target_total_return will steer the drift to approximately hit the target by end_date.
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    # Ticker
    if ticker is None:
        # Derive from merchant name letters
        letters = "".join([c for c in merchant_name.upper() if c.isalpha()])
        if len(letters) >= 3:
            ticker = letters[:3]
        else:
            ticker = "MRC"

    # Trading calendar
    days = business_days(start, end)
    if not days:
        raise ValueError("No business days in the provided date range.")

    # Trend plan
    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    # Events mapping
    events_by_day, intensity_by_day, extra_drift_by_day = build_events_by_day(days, trend_plan, rng)

    # Time settings
    n_days = len(days)
    dt = 1.0 / 252.0  # trading year

    # Drift calibration
    # If target provided, infer mu_annual; else randomize around 5-12% annual
    if target_end_price is not None:
        years = (days[-1] - days[0]).days / 365.25
        years = max(years, 1e-6)
        mu_annual = math.log(max(1e-9, target_end_price / base_price)) / years
    elif target_total_return is not None:
        years = (days[-1] - days[0]).days / 365.25
        years = max(years, 1e-6)
        mu_annual = math.log(1.0 + float(target_total_return)) / years
    if mu_annual is None:
        mu_annual = rng.uniform(0.05, 0.12)

    mu_daily_base = mu_annual  # this is log-return annual; we'll apply per-day as mu_annual*dt
    sigma_daily_base = sigma_annual * math.sqrt(dt)

    # Optional per-month volatility multiplier from trend_plan
    vol_mult_by_month: Dict[str, float] = {}
    vol_mult_default = 1.0
    vol_vol = []
    for e in trend_plan:
        m = str(e.get("month", ""))
        vm = parse_multiplier(e.get("volatility"))
        if vm is not None and vm > 0:
            vol_mult_by_month[m] = max(0.2, float(vm))
            vol_vol.append(vm)
    # Volume multiplier by month
    vol_mult_volume_by_month: Dict[str, float] = {}
    for e in trend_plan:
        m = str(e.get("month", ""))
        vm = parse_multiplier(e.get("volume"))
        if vm is not None and vm > 0:
            vol_mult_volume_by_month[m] = max(0.5, float(vm))

    # Earnings schedule: one per quarter; choose ~4 per year
    earnings_events: List[EarningsEvent] = []
    # Start with baseline EPS TTM using a random PE at start
    pe0 = rng.uniform(8.0, 22.0)
    eps_ttm = base_price / pe0 if pe0 > 0 else rng.uniform(0.5, 2.0)
    # Last four quarters EPS initial
    last4 = [max(0.02, eps_ttm / 4.0 + rng.uniform(-0.05, 0.05)) for _ in range(4)]

    # Create rough quarterly dates (pick a business day near end of quarter)
    def quarter_end_dates():
        q_ends = []
        cur = date(start.year, ((start.month - 1) // 3) * 3 + 3, 1)
        while cur <= end:
            # last day of quarter month
            m = cur.month
            y = cur.year
            # last day of month
            if m in (1, 3, 5, 7, 8, 10, 12):
                last_day = 31
            elif m == 2:
                last_day = 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28
            else:
                last_day = 30
            q_end = date(y, m, last_day)
            q_ends.append(q_end)
            # next quarter
            nm = m + 3
            ny = y + (1 if nm > 12 else 0)
            nm = nm if nm <= 12 else nm - 12
            cur = date(ny, nm, 1)
        return q_ends

    qdates = []
    for qd in quarter_end_dates():
        # find nearest business day within +/- 15 days after quarter end
        candidate = qd + timedelta(days=rng.randint(10, 25))
        # snap to nearest business day
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
        if start <= candidate <= end:
            qdates.append(candidate)

    qmap = {d: True for d in qdates}

    # Dividends: 2-4 per year, yield roughly 2-5% annual
    actions: List[CorporateAction] = []
    annual_yield = rng.uniform(0.02, 0.05)
    # distribute across years
    by_year = {}
    for d in days:
        by_year.setdefault(d.year, []).append(d)
    for y, dlist in by_year.items():
        n_div = rng.randint(2, 4)
        chosen = rng.sample(dlist, k=min(n_div, len(dlist)))
        # total annual dividend approx yield * average price (approx base)
        annual_div_total = annual_yield * base_price
        per_div = annual_div_total / max(1, n_div)
        for dd in chosen:
            actions.append(CorporateAction(
                date=dd.isoformat(),
                type="dividend",
                amount=round(per_div + rng.uniform(-0.15, 0.15) * per_div, 4),
                currency=currency,
                note="scheduled dividend"
            ))

    # Splits: rare event
    if rng.random() < 0.07:
        sd = rng.choice(days[int(len(days) * 0.2):int(len(days) * 0.9)])
        ratio = rng.choice(["2:1", "3:1", "3:2"])
        actions.append(CorporateAction(date=sd.isoformat(), type="split", ratio=ratio, note="stock split"))

    # Build a quick lookup for dividends and splits
    dividend_by_day: Dict[date, float] = {}
    split_by_day: Dict[date, str] = {}
    for a in actions:
        ad = parse_date_str(a.date)
        if a.type == "dividend" and a.amount:
            dividend_by_day[ad] = dividend_by_day.get(ad, 0.0) + float(a.amount)
        if a.type == "split" and a.ratio:
            split_by_day[ad] = a.ratio

    # Simulate price path
    prices: List[PriceBar] = []
    prev_close = float(base_price)
    last_sigma = sigma_daily_base
    shock_memory = 0.0  # to create mild volatility clustering
    base_vol_mult = 1.0

    for idx, d in enumerate(days):
        month_key = f"{d.year}-{d.month:02d}"
        event_list = events_by_day.get(d, [])
        event_intensity = float(clamp(intensity_by_day.get(d, 0.0), 0.0, 2.0))
        # pick dominant event label if any
        event_label = None
        if event_list:
            event_label = max(event_list, key=lambda e: e.get("gaussian", 0.0)).get("label")

        # Earnings today?
        is_earn = d in qmap
        eps_est, eps_act, eps_surprise = None, None, None
        rev_est, rev_act, rev_surprise = None, None, None

        if is_earn:
            # Earnings generation
            # Estimate equals previous quarter +/- noise
            est_q = max(0.01, (last4[-1] + rng.uniform(-0.03, 0.03)))
            # Surprise
            eps_surprise = rng.gauss(0.0, 0.08)  # +/-8%
            eps_act = max(0.0, est_q * (1.0 + eps_surprise))
            eps_est = est_q
            # Revenue proxy (random scale)
            rev_est = random.uniform(500, 1500)  # millions
            rev_surprise = rng.gauss(0.0, 0.05)
            rev_act = max(100.0, rev_est * (1.0 + rev_surprise))
            # Update last4 EPS rolling
            last4.pop(0)
            last4.append(eps_act)
            eps_ttm = sum(last4)
            # Save earnings event
            qnum = (d.month - 1) // 3 + 1
            earnings_events.append(EarningsEvent(
                date=d.isoformat(),
                period=f"{d.year}-Q{qnum}",
                eps_estimate=round(float(eps_est), 4),
                eps_actual=round(float(eps_act), 4),
                eps_surprise_pct=round(float(eps_surprise) * 100.0, 2),
                revenue_estimate=round(float(rev_est), 2),
                revenue_actual=round(float(rev_act), 2),
                revenue_surprise_pct=round(float(rev_surprise) * 100.0, 2),
                press_url=f"https://news.example.com/{ticker}/earnings/{d.isoformat()}",
                call_url=f"https://ir.example.com/{ticker}/call/{d.isoformat()}"
            ))

        # Daily drift and vol
        # Base daily drift in log space
        mu_day = mu_daily_base * dt  # annual log drift scaled per day
        # extra drift from monthly CAR target
        mu_day += extra_drift_by_day.get(d, 0.0)
        # sentiment/event tilt: convert shift [-1,1] to small bias
        if event_list:
            weights = np.array([e["gaussian"] for e in event_list], dtype=float)
            weights = weights / (weights.sum() + 1e-12)
            shift = sum(w * e["shift"] for w, e in zip(weights, event_list))
            mu_day += float(shift) * 0.02 * dt  # modest tilt

        # Earnings-induced gap drift
        if is_earn:
            mu_day += rng.uniform(-0.02, 0.02)  # unpredictable but can tilt

        # Volatility: baseline + event intensity + volatility clustering
        vol_mult = vol_mult_by_month.get(month_key, 1.0)
        sigma_day = sigma_daily_base * (1.0 + 1.5 * event_intensity) * vol_mult
        # Add clustering: depend on last shock_memory
        sigma_day *= (1.0 + 0.5 * shock_memory)

        # Earnings days: higher vol
        if is_earn:
            sigma_day *= rng.uniform(1.4, 2.2)

        # Gap vs intraday split
        sigma_gap = sigma_day * rng.uniform(0.25, 0.45)
        sigma_intra = max(1e-8, math.sqrt(max(1e-12, sigma_day**2 - sigma_gap**2)))
        # Drift split between gap and intraday
        mu_gap = mu_day * rng.uniform(0.25, 0.40)
        mu_intra = mu_day - mu_gap

        # Dividend ex-date drop
        dividend = float(dividend_by_day.get(d, 0.0))
        ex_div_factor = 1.0
        if dividend > 0 and prev_close > 0:
            ex_div_factor = max(1e-6, 1.0 - dividend / prev_close)

        # Simulate returns
        z_gap = np.random.normal()
        z_intra = np.random.normal()
        r_gap = mu_gap - 0.5 * sigma_gap**2 + sigma_gap * z_gap
        r_intra = mu_intra - 0.5 * sigma_intra**2 + sigma_intra * z_intra

        # Open and Close
        o = prev_close * ex_div_factor * math.exp(r_gap)
        c = o * math.exp(r_intra)
        # Range: derive approximate high/low
        # Use ranges proportional to daily sigma with randomness
        up_factor = math.exp(abs(np.random.normal(0.0, 0.6)) * sigma_day * rng.uniform(0.8, 1.8))
        dn_factor = math.exp(abs(np.random.normal(0.0, 0.6)) * sigma_day * rng.uniform(0.8, 1.8))
        high = max(o, c) * up_factor
        low = min(o, c) / dn_factor
        # Ensure plausibility
        high = max(high, o, c)
        low = min(low, o, c)
        # VWAP approx
        vwap = (high + low + c) / 3.0

        # Volume: baseline with event and month volume multiplier
        volume_mult = vol_mult_volume_by_month.get(month_key, 1.0)
        vol_noise = np.random.lognormal(mean=math.log(1.0), sigma=0.5)
        volume = int(max(0, avg_daily_volume * (1.0 + 1.8 * event_intensity) * volume_mult * vol_noise))
        # Prevent extreme outliers
        volume = min(volume, int(avg_daily_volume * 40))

        # Returns
        r_total_log = math.log(max(1e-12, c / max(1e-12, prev_close)))
        r_total_pct = math.exp(r_total_log) - 1.0

        # Update shock memory for clustering
        shock_memory = 0.92 * shock_memory + 0.08 * (abs(r_total_log) / (sigma_daily_base + 1e-12))

        # Split handling (note: we record split but keep prices in "post-split adjusted" continuity)
        split_ratio_str = split_by_day.get(d)
        if split_ratio_str:
            # Example: "2:1" means shares double; price halves. We'll rescale open/high/low/close accordingly after recording.
            try:
                a, b = split_ratio_str.split(":")
                a = float(a); b = float(b)
                factor = b / a if a > 0 else 1.0  # e.g., 2:1 -> factor = 0.5
            except Exception:
                factor = 0.5
            o *= factor; high *= factor; low *= factor; c *= factor; vwap *= factor
            # Volume increases inversely by the same factor
            volume = int(volume / max(1e-9, factor))

        # Market cap
        market_cap = float(c * shares_outstanding)

        bar = PriceBar(
            date=d.isoformat(),
            open=round(float(o), 4),
            high=round(float(high), 4),
            low=round(float(low), 4),
            close=round(float(c), 4),
            adj_close=round(float(c), 4),  # keep equal; dividends/splits separately provided
            volume=volume,
            vwap=round(float(vwap), 4),
            turnover=round(float(vwap * volume), 2),
            return_log=round(float(r_total_log), 6),
            return_pct=round(float(r_total_pct), 6),
            gap_return_log=round(float(r_gap), 6),
            intra_return_log=round(float(r_intra), 6),
            volatility_day=round(float(sigma_day), 6),
            event_label=event_label,
            event_intensity=round(float(event_intensity), 4),
            is_earnings=is_earn,
            dividend=round(float(dividend), 4) if dividend > 0 else 0.0,
            split_ratio=split_ratio_str,
            market_cap=round(float(market_cap), 2)
        )
        prices.append(bar)
        prev_close = c

    # Compute technicals
    closes = np.array([p.close for p in prices], dtype=float)
    ma5 = moving_average(closes, 5)
    ma20 = moving_average(closes, 20)
    ma50 = moving_average(closes, 50)
    ma200 = moving_average(closes, 200)
    rsi14 = rsi(closes, 14)
    bb_up, bb_lo = bollinger_bands(closes, 20, 2.0)

    for i, p in enumerate(prices):
        p.ma5 = round(float(ma5[i]), 4) if not math.isnan(ma5[i]) else None
        p.ma20 = round(float(ma20[i]), 4) if not math.isnan(ma20[i]) else None
        p.ma50 = round(float(ma50[i]), 4) if not math.isnan(ma50[i]) else None
        p.ma200 = round(float(ma200[i]), 4) if not math.isnan(ma200[i]) else None
        p.rsi14 = round(float(rsi14[i]), 2) if not math.isnan(rsi14[i]) else None
        p.bb20_upper = round(float(bb_up[i]), 4) if not math.isnan(bb_up[i]) else None
        p.bb20_lower = round(float(bb_lo[i]), 4) if not math.isnan(bb_lo[i]) else None

    # Summary metrics
    start_price_eff = prices[0].close
    end_price_eff = prices[-1].close
    total_return = (end_price_eff / start_price_eff) - 1.0
    years = (days[-1] - days[0]).days / 365.25
    annual_return = (1.0 + total_return) ** (1.0 / max(years, 1e-6)) - 1.0

    # Realized volatility (annualized) from daily log returns
    rets = np.array([p.return_log for p in prices], dtype=float)
    realized_vol_daily = np.std(rets, ddof=1)
    realized_vol_annual = realized_vol_daily / math.sqrt(dt)

    # Max drawdown
    cum_max = np.maximum.accumulate(closes)
    drawdowns = (closes / cum_max) - 1.0
    max_drawdown = float(np.min(drawdowns)) if len(drawdowns) else 0.0

    avg_vol = float(np.mean([p.volume for p in prices])) if prices else 0

    # Build output
    out = {
        "meta": {
            "merchant": merchant_name,
            "ticker": ticker,
            "currency": currency,
            "start_date": start_date,
            "end_date": end_date,
            "base_price": round(float(base_price), 4),
            "shares_outstanding": int(shares_outstanding),
            "avg_daily_volume": int(avg_daily_volume),
            "mu_annual_input": round(float(mu_annual), 6),
            "sigma_annual_input": round(float(sigma_annual), 6),
        },
        "trend_plan": trend_plan,
        "corporate_actions": [asdict(a) for a in actions],
        "earnings": [asdict(e) for e in earnings_events],
        "prices": [asdict(p) for p in prices],
        "summary": {
            "start_price": round(float(start_price_eff), 4),
            "end_price": round(float(end_price_eff), 4),
            "total_return_pct": round(float(total_return) * 100.0, 2),
            "annualized_return_pct": round(float(annual_return) * 100.0, 2),
            "realized_vol_annual_pct": round(float(realized_vol_annual) * 100.0, 2),
            "max_drawdown_pct": round(float(max_drawdown) * 100.0, 2),
            "avg_daily_volume": int(avg_vol),
            "trading_days": len(prices)
        }
    }

    # Output file
    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = merchant_name.lower()
        out_json_path = f"stock_{safe}_{start_date}_to_{end_date}_{ts}.json"

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example usage ---------------------------

if __name__ == "__main__":
    merchant = "Kingfisher"
    path = generate_fake_stock_json(
        merchant_name=merchant,
        ticker="KGF",
        start_date="2020-01-01",
        end_date="2024-12-31",
        base_price=250.0,           # in pence or GBP, your choice; consistent unit throughout
        shares_outstanding=2_100_000_000,
        currency="GBP",
        mu_annual=None,             # will be inferred if target provided, else randomized
        sigma_annual=0.38,
        avg_daily_volume=6_500_000,
        target_total_return=0.12,   # about +12% over the whole period
        trend_plan=[
            {"month": "2023-06", "intensity": 0.85, "label": "spike - data breach", "return": "-10%", "volume": "x2.0", "volatility": "+60%"},
            {"month": "2023-09", "intensity": 0.55, "label": "normal", "return": "1%", "volume": "110%"},
            {"month": "2024-01", "intensity": 0.90, "label": "spike - new product launch", "return": "+14%", "volume": "x1.8", "volatility": "1.4x"},
        ],
        seed=42,
        out_json_path=None
    )
    print("Saved stock JSON to:", path)