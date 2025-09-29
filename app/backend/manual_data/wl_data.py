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
class WLMerchant:
    id: str                                # acceptor id (used as acceptor_id in txns)
    merchant_code: str                     # 8-digit merchant code (for txns' merchant_id)
    name: str                              # required
    legal_name: str                        # required
    acceptor_name: str                     # required
    acceptor_category_code: Optional[str]  # 4-digit MCC
    url: Optional[str]
    language_code: Optional[str]
    time_zone_id: Optional[str]
    country_code: Optional[str]
    country_sub_division_code: Optional[str]
    home_country_code: Optional[str]
    region_id: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    street: Optional[str]
    currency_code: Optional[str]
    tax_id: Optional[str]
    trade_register_number: Optional[str]
    iban: Optional[str]                    # must be null
    domiciliary_bank_number: Optional[str]
    cut_off_time: Optional[str]            # "HH:MM:SS"
    activation_flag: bool = False
    activation_time: Optional[str] = None  # ISO ts
    activation_start_time: Optional[str] = None
    activation_end_time: Optional[str] = None
    city_category_code: Optional[str] = None
    business_service_phone_number: Optional[str] = None
    customer_service_phone_number: Optional[str] = None
    additional_contact_information: Optional[str] = None
    description: Optional[str] = None      # NEW


@dataclass
class WLTransaction:
    txn_id: str
    merchant_id: str                       # 8-digit code (if base 6-digit, padded with "00")
    acceptor_id: str                       # same as current acceptor (WLMerchant.id)
    sno: int                               # incremental 1..N
    amount: float
    currency_code: str
    card_brand: str
    card_bin: str
    card_last4: str
    pan_masked: str
    banned_card: bool
    txn_type: str
    terminal_type: str                     # keypad | ecommerce | moto | atm | contactless
    mcc: str
    terminal_id: str
    place: str
    user_id: str
    user_name: str
    recipient_user_id: Optional[str]
    recipient_user_name: Optional[str]
    recipient_country_code: Optional[str]
    txn_time: str
    completion_time: Optional[str]
    status: str                            # approved | declined | reversed | chargeback | pending | captured | refunded
    decline_reason: Optional[str]
    country_code: str
    city: str
    international: bool                    # moved out of risk_flags
    offline: bool                          # moved out of risk_flags
    shady_region: bool                     # NEW top-level shady region indicator
    risk_flags: Dict[str, bool]            # {"high_risk_mcc": bool, "fraud_suspected": bool, "shady_region": bool}
    risk_factors: List[str]                # e.g., ["gambling","adult_content","crypto_transfer","card_not_present",...]
    risk_score: float                      # 0..100
    event_label: Optional[str]             # dominant event that day (if any)


# --------------------------- Utilities ---------------------------

def parse_date_str(s: str) -> date:
    if len(s) == 7:
        return date.fromisoformat(s + "-01")
    return date.fromisoformat(s)

def isoformat_dt(dt_obj: datetime) -> str:
    return dt_obj.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

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

def parse_share(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            v = float(val)
            return v if v <= 1.0 else v / 100.0
        s = str(val).strip().lower()
        if s.endswith("%"):
            return float(s[:-1]) / 100.0
        return float(s)
    except Exception:
        return None

def random_phone(rng: random.Random) -> str:
    return f"+{rng.randint(1, 9)}{rng.randint(1000000000, 9999999999)}"

def random_time_hms(rng: random.Random) -> str:
    h = rng.randint(0, 23)
    m = rng.randint(0, 59)
    s = rng.randint(0, 59)
    return f"{h:02d}:{m:02d}:{s:02d}"

def sample_time_of_day(rng: random.Random, day: date, gambling_bias: float) -> Tuple[int, int, int]:
    r = rng.random()
    if r < (0.30 + 0.25 * gambling_bias):
        hour = int(clamp(rng.gauss(21.5, 2.2), 18, 23))
    elif r < 0.8:
        hour = int(clamp(rng.gauss(13.5, 2.5), 8, 18))
    else:
        hour = int(clamp(rng.gauss(9.0, 1.5), 6, 12))
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return hour, minute, second

def mask_pan(bin6: str, last4: str) -> str:
    return f"{bin6}******{last4}"

def choose_brand_bin(rng: random.Random) -> Tuple[str, str, str]:
    # Extended brand pool with rough BIN patterns (synthetic, not authoritative)
    brands = [
        ("Visa", 0.40), ("Mastercard", 0.34), ("Amex", 0.06), ("Discover", 0.04),
        ("Maestro", 0.03), ("JCB", 0.03), ("UnionPay", 0.03), ("DinersClub", 0.02),
        ("RuPay", 0.02), ("Mir", 0.01), ("Elo", 0.01), ("Hipercard", 0.01)
    ]
    weights = np.array([w for _, w in brands], dtype=float)
    weights = weights / (weights.sum() + 1e-12)
    idx = int(rng.choices(range(len(brands)), weights=weights, k=1)[0])
    brand = brands[idx][0]
    bin_prefixes = {
        "Visa": ["4" + str(rng.randint(10000, 99999))],
        "Mastercard": [str(rng.choice([51,52,53,54,55])) + str(rng.randint(10000, 99999))],
        "Amex": ["34" + str(rng.randint(1000, 9999)), "37" + str(rng.randint(1000, 9999))],
        "Discover": ["6011" + str(rng.randint(100, 999))],
        "Maestro": ["50" + str(rng.randint(10000, 99999)), "56" + str(rng.randint(10000, 99999))],
        "JCB": ["35" + str(rng.randint(1000, 9999))],
        "UnionPay": ["62" + str(rng.randint(10000, 99999))],
        "DinersClub": ["36" + str(rng.randint(10000, 99999))],
        "RuPay": ["60" + str(rng.randint(10000, 99999))],
        "Mir": ["22" + str(rng.randint(10000, 99999))],
        "Elo": ["4011" + str(rng.randint(100, 999))],
        "Hipercard": ["6062" + str(rng.randint(100, 999))]
    }
    bin6 = rng.choice(bin_prefixes[brand])
    if len(bin6) < 6:
        bin6 = (bin6 + "000000")[:6]
    last4 = f"{rng.randint(0, 9999):04d}"
    return brand, bin6[:6], last4

def choose_mcc(rng: random.Random, high_risk_bias: float, gambling_bias: float) -> str:
    # Include multiple MCCs; amplify high-risk based on bias
    mcc_pool = [
        ("5411", 0.12),  # Grocery
        ("5812", 0.10),  # Eating places
        ("5999", 0.09),  # Misc retail
        ("6011", 0.08),  # ATM/cash advance (riskier)
        ("5732", 0.06),  # Electronics
        ("4789", 0.05),  # Transport
        ("7399", 0.05),  # Business services
        ("7995", 0.05),  # Betting (gambling) HIGH-RISK
        ("4900", 0.05),  # Utilities
        ("4814", 0.05),  # Telecom
        ("5699", 0.04),  # Apparel
        ("5921", 0.03),  # Alcohol
        ("6051", 0.03),  # Quasi cash (crypto/foreign currency) HIGH-RISK
        ("5813", 0.03),  # Bars/nightclubs (adult entertainment risk)
        ("5993", 0.02)   # Cigar stores (age-restricted)
    ]
    weights = np.array([w for _, w in mcc_pool], dtype=float)
    for i, (mcc, w) in enumerate(mcc_pool):
        if mcc in ("7995", "6011", "6051", "5813", "5921"):
            weights[i] *= (1.0 + 2.0 * high_risk_bias + (3.0 if mcc == "7995" else 1.6) * gambling_bias)
        else:
            weights[i] *= (1.0 + 0.25 * high_risk_bias)
    weights = weights / (weights.sum() + 1e-12)
    idx = int(rng.choices(range(len(mcc_pool)), weights=weights, k=1)[0])
    return mcc_pool[idx][0]

def choose_txn_type(rng: random.Random, offline_bias: float, gambling_bias: float) -> str:
    base = [
        ("authorization", 0.30), ("capture", 0.18), ("refund", 0.06),
        ("credit", 0.05), ("oct", 0.04), ("pre_auth", 0.10),
        ("reconciliation", 0.02), ("offline", 0.05), ("gambling", 0.03),
        ("chargeback", 0.03), ("reversal", 0.02), ("moto", 0.02)
    ]
    weights = np.array([w for _, w in base], dtype=float)
    for i, (t, _) in enumerate(base):
        if t == "offline":
            weights[i] *= (1.0 + 2.2 * offline_bias)
        if t == "gambling":
            weights[i] *= (1.0 + 5.0 * gambling_bias)
        if t in ("authorization", "capture"):
            weights[i] *= (1.0 + 0.25 * gambling_bias)
    weights = weights / (weights.sum() + 1e-12)
    idx = int(rng.choices(range(len(base)), weights=weights, k=1)[0])
    return base[idx][0]

def choose_terminal_type(rng: random.Random, txn_type: str) -> str:
    if txn_type in ("offline", "authorization", "capture", "pre_auth"):
        return rng.choices(["keypad", "contactless"], weights=[0.75, 0.25], k=1)[0]
    if txn_type in ("moto",):
        return "moto"
    if txn_type in ("refund", "credit", "oct", "gambling", "chargeback", "reversal", "reconciliation"):
        return rng.choices(["ecommerce", "keypad", "moto"], weights=[0.6, 0.25, 0.15], k=1)[0]
    return "ecommerce"

def choose_status(rng: random.Random, txn_type: str, decline_rate: float, risk_intensity: float, banned_card: bool) -> Tuple[str, Optional[str]]:
    if txn_type in ("refund", "credit"):
        return ("refunded", None)
    # Banned card forces decline
    if banned_card:
        return ("declined", "card_blocked")
    p_decline = clamp(decline_rate, 0.0, 0.95)
    p_chargeback = clamp(0.004 + 0.05 * risk_intensity, 0.0, 0.20)
    p_reversal = clamp(0.01 + 0.03 * risk_intensity, 0.0, 0.20)
    p_pending = clamp(0.01 + 0.03 * decline_rate, 0.0, 0.20)
    r = rng.random()
    if r < p_decline:
        reason = rng.choice(["insufficient_funds", "suspected_fraud", "expired_card", "do_not_honor", "technical_error"])
        return ("declined", reason)
    r2 = rng.random()
    if r2 < p_chargeback and txn_type in ("authorization", "capture", "gambling"):
        return ("chargeback", "cardholder_dispute")
    r3 = rng.random()
    if r3 < p_reversal:
        return ("reversed", "reversal_by_issuer")
    r4 = rng.random()
    if r4 < p_pending:
        return ("pending", None)
    if txn_type == "authorization":
        return ("approved", None)
    if txn_type == "capture":
        return ("captured", None)
    return ("approved", None)

def amount_model(rng: random.Random, mcc: str, gambling_bias: float) -> float:
    base_mu = math.log(30.0)
    base_sigma = 0.95
    if mcc == "7995":
        base_mu = math.log(65.0 + 200.0 * gambling_bias)
        base_sigma = 1.20
    elif mcc in ("6011", "5921", "6051"):
        base_mu = math.log(50.0)
        base_sigma = 1.05
    amt = rng.lognormvariate(mu=base_mu, sigma=base_sigma)
    amt = float(clamp(amt, 0.50, 15000.0))
    if mcc in ("7995", "6051") and rng.random() < (0.02 + 0.10 * gambling_bias):
        amt *= rng.uniform(2.0, 5.0)
        amt = min(50000.0, amt)
    return round(float(amt), 2)

def risk_score_from(flags: Dict[str, bool], decline_rate: float, risk_intensity: float, factors: List[str]) -> float:
    score = 10.0 * (1.0 + 2.0 * risk_intensity)
    if flags.get("high_risk_mcc"): score += 18.0
    if flags.get("fraud_suspected"): score += 25.0
    if flags.get("shady_region"): score += 14.0
    # Factor-based increments
    bump_map = {
        "gambling": 20.0, "adult_content": 16.0, "crypto_transfer": 18.0, "cash_advance": 10.0,
        "atm": 10.0, "cross_border": 10.0, "card_not_present": 8.0, "velocity": 8.0,
        "new_account": 6.0, "synthetic_id": 12.0, "account_takeover": 24.0, "disputed": 10.0,
        "refund_abuse": 9.0, "merchant_risk_category": 8.0
    }
    for f in set(factors or []):
        score += bump_map.get(f, 4.0)
    score += decline_rate * 100.0 * 0.15
    return round(float(clamp(score, 0.0, 100.0)), 2)

def make_merchant_code(rng: random.Random) -> str:
    # 60% chance 8-digit outright; else 6-digit padded with "00"
    if rng.random() < 0.6:
        return f"{rng.randint(10_000_000, 99_999_999)}"
    else:
        return f"{rng.randint(100_000, 999_999)}00"

def pick_acceptor_mcc(rng: random.Random) -> str:
    mccs = ["5411","5812","5999","5732","4789","7399","4814","5699","4900","5921","5813","6051","6011","7995"]
    return rng.choice(mccs)

def apply_monthly_transaction_shares(days: List[date], weights: np.ndarray, trend_plan: List[Dict[str, Any]]) -> np.ndarray:
    by_month_indices: Dict[str, List[int]] = {}
    for i, d in enumerate(days):
        key = f"{d.year}-{d.month:02d}"
        by_month_indices.setdefault(key, []).append(i)
    requested: Dict[str, float] = {}
    for e in trend_plan:
        m = str(e.get("month", ""))
        sh = parse_share(e.get("transactions"))
        if m and sh and sh > 0:
            requested[m] = requested.get(m, 0.0) + float(sh)
    total_req = sum(requested.values())
    if total_req <= 0.0 or total_req >= 0.95:
        return weights
    new_w = weights.copy()
    for m, target in requested.items():
        idxs = by_month_indices.get(m, [])
        if not idxs:
            continue
        cur_sum = float(weights[idxs].sum())
        if cur_sum <= 1e-12:
            continue
        scale = float(target / cur_sum)
        new_w[idxs] = weights[idxs] * scale
    cur_total = float(new_w.sum())
    if cur_total <= 1e-12:
        return weights
    new_w = new_w * (1.0 / cur_total)
    new_w = new_w / (new_w.sum() + 1e-12)
    return new_w


# --------------------------- Trend modeling ---------------------------

def random_trend_plan(start: date, end: date, rng: random.Random) -> List[Dict[str, Any]]:
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    k = rng.randint(3, min(6, max(3, total_months)))
    idxs = sorted(rng.sample(range(total_months), k=k))
    pos_labels = ["normal", "promo period", "seasonal buzz"]
    risk_labels = ["fraud ring", "system outage", "gambling spike"]
    plan = []
    for i in idxs:
        y = start.year + (start.month - 1 + i) // 12
        m = (start.month - 1 + i) % 12 + 1
        lab = rng.choice(pos_labels + risk_labels)
        intensity = rng.uniform(0.45, 0.95) if lab in risk_labels else rng.uniform(0.35, 0.70)
        e = {"month": f"{y}-{m:02d}", "intensity": round(float(intensity), 2), "label": lab}
        if rng.random() < 0.6:
            e["transactions"] = f"{rng.randint(3, 20)}%"
        if lab == "fraud ring":
            e["high_risk_intensity"] = round(rng.uniform(0.30, 0.70), 3)
            e["decline_rate"] = round(rng.uniform(0.15, 0.35), 3) if rng.random() < 0.6 else None
            e["gambling_share"] = round(rng.uniform(0.04, 0.12), 3) if rng.random() < 0.4 else None
        elif lab == "system outage":
            e["high_risk_intensity"] = round(rng.uniform(0.08, 0.20), 3)
            e["decline_rate"] = round(rng.uniform(0.20, 0.50), 3)
            e["gambling_share"] = round(rng.uniform(0.01, 0.05), 3)
        elif lab == "gambling spike":
            e["high_risk_intensity"] = round(rng.uniform(0.20, 0.40), 3)
            e["decline_rate"] = round(rng.uniform(0.06, 0.15), 3)
            e["gambling_share"] = round(rng.uniform(0.08, 0.25), 3)
        else:
            e["high_risk_intensity"] = round(rng.uniform(0.06, 0.18), 3)
            e["decline_rate"] = round(rng.uniform(0.04, 0.10), 3) if rng.random() < 0.4 else None
            e["gambling_share"] = round(rng.uniform(0.02, 0.08), 3) if rng.random() < 0.5 else None
        plan.append(e)
    return plan

def build_daily_intensity(
    start: date,
    end: date,
    trend_plan: List[Dict[str, Any]],
    rng: random.Random
) -> Tuple[List[date], np.ndarray, Dict[date, List[Dict[str, Any]]], Dict[date, float], Dict[date, float], Dict[date, float]]:
    days = list(dt_range(start, end))
    n = len(days)
    dow = np.array([d.weekday() for d in days], dtype=float)
    doy = np.array([(d - date(d.year, 1, 1)).days for d in days], dtype=float)
    base = np.ones(n)
    weekly = 0.20 * np.sin((dow - 2) / 6.0 * 2 * np.pi) + 0.10 * np.sin((dow - 5) / 6.0 * 2 * np.pi)
    annual = 0.10 * np.sin((doy / 365.25) * 2 * np.pi)
    intensity = base + weekly + annual
    intensity = np.clip(intensity, 0.05, None)

    events_by_day: Dict[date, List[Dict[str, Any]]] = {d: [] for d in days}
    highrisk_by_day: Dict[date, float] = {d: 0.0 for d in days}
    decline_by_day: Dict[date, float] = {d: 0.0 for d in days}
    gambling_share_by_day: Dict[date, float] = {d: 0.02 for d in days}

    for e in trend_plan:
        e_month = parse_date_str(str(e["month"]))
        center = date(e_month.year, e_month.month, 15)
        sigma_days = rng.randint(6, 20)
        gvals = np.array([math.exp(-((d - center).days ** 2) / (2 * (sigma_days ** 2))) for d in days], dtype=float)
        intensity += float(e.get("intensity", 0.6)) * gvals * 1.2
        hr = float(parse_share(e.get("high_risk_intensity")) or 0.0)
        dr = float(parse_share(e.get("decline_rate")) or 0.0)
        gs = float(parse_share(e.get("gambling_share")) or 0.0)
        for i, d in enumerate(days):
            if gvals[i] > 0.05:
                events_by_day[d].append({"label": str(e.get("label", "normal")), "gaussian": float(gvals[i])})
                highrisk_by_day[d] += hr * gvals[i]
                decline_by_day[d] += dr * gvals[i]
                gambling_share_by_day[d] += gs * gvals[i]

    intensity = np.clip(intensity, 0.05, None)
    for d in days:
        gambling_share_by_day[d] = float(clamp(gambling_share_by_day[d], 0.0, 0.40))
        decline_by_day[d] = float(clamp(decline_by_day[d], 0.0, 0.80))
        highrisk_by_day[d] = float(clamp(highrisk_by_day[d], 0.0, 1.0))
    return days, intensity, events_by_day, highrisk_by_day, decline_by_day, gambling_share_by_day


# --------------------------- Merchant builder ---------------------------

def build_merchant(merchant_name: str, rng: random.Random) -> WLMerchant:
    slug = "".join(c.lower() if c.isalnum() else "_" for c in merchant_name).strip("_")
    acc_id = f"acc_{uuid.uuid4().hex[:10]}"
    merchant_code = make_merchant_code(rng)
    country = rng.choice(["GB", "US", "IE", "DE", "FR", "ES", "IT"])
    currency = {"GB": "GBP", "US": "USD", "IE": "EUR", "DE": "EUR", "FR": "EUR", "ES": "EUR", "IT": "EUR"}.get(country, "USD")
    city = rng.choice(["London", "Manchester", "Birmingham", "Edinburgh", "Dublin", "New York", "Berlin", "Paris", "Madrid", "Milan"])
    tzmap = {"GB": "Europe/London", "IE": "Europe/Dublin", "US": "America/New_York", "DE": "Europe/Berlin", "FR": "Europe/Paris", "ES": "Europe/Madrid", "IT": "Europe/Rome"}
    tz = tzmap.get(country, "UTC")

    cut_off = random_time_hms(rng)
    now = datetime.now(timezone.utc)
    act_start = now - timedelta(days=rng.randint(300, 1200))
    act_end = None if rng.random() < 0.9 else (now + timedelta(days=rng.randint(30, 300)))

    return WLMerchant(
        id=acc_id,
        merchant_code=merchant_code,
        name=merchant_name,
        legal_name=f"{merchant_name} Ltd.",
        acceptor_name=f"{merchant_name} Acceptor",
        acceptor_category_code=pick_acceptor_mcc(rng),
        url=f"https://{slug}.example.com",
        language_code="en",
        time_zone_id=tz,
        country_code=country,
        country_sub_division_code=rng.choice(["LND", "NY", "BER", "PAR", "MAD", "MAN", None]),
        home_country_code=country,
        region_id=rng.choice(["UK", "EU", "US"]),
        city=city,
        postal_code=f"{rng.randint(10000, 99999)}",
        street=f"{rng.randint(1, 250)} {rng.choice(['Main St','High St','Market Rd','Broadway','King St'])}",
        currency_code=currency,
        tax_id=f"TAX-{uuid.uuid4().hex[:8].upper()}",
        trade_register_number=f"REG-{uuid.uuid4().hex[:8].upper()}",
        iban=None,  # must be null
        domiciliary_bank_number=f"BANK-{rng.randint(100000,999999)}",
        cut_off_time=cut_off,
        activation_flag=True if rng.random() < 0.98 else False,
        activation_time=isoformat_dt(now - timedelta(days=rng.randint(0, 300))),
        activation_start_time=isoformat_dt(act_start),
        activation_end_time=(isoformat_dt(act_end) if act_end else None),
        city_category_code=rng.choice(["URB","SUB","RUR", None]),
        business_service_phone_number=random_phone(rng),
        customer_service_phone_number=random_phone(rng),
        additional_contact_information="Contact support via portal.",
        description="Acquirer/acceptor profile for merchant risk simulation."
    )


# --------------------------- Main generator ---------------------------

def generate_fake_wl_json(
    merchant_name: str,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    n_transactions: int = 50000,
    trend_plan: Optional[List[Dict[str, Any]]] = None,
    seed: Optional[int] = None,
    out_json_path: Optional[str] = None
) -> str:
    """
    Generate WL merchant/acceptor details + probabilistic transactions driven by trend_plan.
    trend_plan items support keys:
      {
        "month": "YYYY-MM",
        "intensity": 0.0..1.0,
        "label": "fraud ring" | "system outage" | "gambling spike" | "normal" | "promo period" | "seasonal buzz",
        "transactions": 0.10 or "10%",
        "high_risk_intensity": 0.0..1.0 or "%",
        "decline_rate": 0.0..1.0 or "%",
        "gambling_share": 0.0..1.0 or "%"
      }
    Returns: path to JSON file with {"merchant":{...}, "trend_plan":[...], "banned_cards":[...], "transactions":[...], "summary":{...}}
    """
    rng = random.Random(seed)
    np.random.seed(seed if seed is not None else rng.randint(0, 2**32 - 1))

    start = parse_date_str(start_date)
    end = parse_date_str(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    final_n = max(int(round(n_transactions * rng.uniform(0.9, 1.1))), 1000)

    if trend_plan is None:
        trend_plan = random_trend_plan(start, end, rng)

    days, intensity, events_by_day, highrisk_by_day, decline_by_day, gambling_share_by_day = build_daily_intensity(start, end, trend_plan, rng)
    weights = intensity / (intensity.sum() + 1e-12)
    weights = apply_monthly_transaction_shares(days, weights, trend_plan)
    txns_per_day = np.random.multinomial(final_n, weights)

    merchant = build_merchant(merchant_name, rng)
    acceptor_id = merchant.id
    merchant_code_8 = merchant.merchant_code
    country = merchant.country_code or "GB"
    city = merchant.city or "London"
    currency = merchant.currency_code or "GBP"

    # Terminal pool
    n_terminals = max(5, int(rng.uniform(10, 40)))
    terminals = [f"T{rng.randint(100000,999999)}" for _ in range(n_terminals)]

    places = [merchant.city or "London", rng.choice(["Manchester","Birmingham","Leeds","Edinburgh","Dublin","Berlin","Paris","Madrid","Milan"])]

    # User ecosystem (repeat users via Zipf-like distribution)
    n_users = max(300, min(6000, int(final_n / rng.uniform(5.0, 25.0))))
    user_ids = [f"u_{uuid.uuid4().hex[:10]}" for _ in range(n_users)]
    user_names = [f"user_{uid[-6:]}" for uid in user_ids]
    zipf_a = rng.uniform(1.3, 2.0)
    zipf_raw = np.random.zipf(a=zipf_a, size=final_n)
    zipf_ranks = np.clip(zipf_raw, 1, n_users) - 1
    indices = list(range(n_users))
    rng.shuffle(indices)

    # Recipient pool (for OCT/credit/gambling/crypto-like)
    n_recips = max(50, min(500, int(final_n / rng.uniform(50.0, 200.0))))
    recip_ids = [f"ru_{uuid.uuid4().hex[:10]}" for _ in range(n_recips)]
    recip_names = [f"recipient_{rid[-6:]}" for rid in recip_ids]
    recip_countries = [rng.choice(["GB","US","IE","DE","FR","ES","IT","NL","SE","PL","PT","AE","SG","HK","IN"]) for _ in range(n_recips)]

    # Banned cards list (bin6+last4 combos)
    n_banned = max(20, min(500, int(final_n * rng.uniform(0.002, 0.02))))
    banned_cards = set()
    for _ in range(n_banned):
        brand, bin6, last4 = choose_brand_bin(rng)
        banned_cards.add(f"{bin6}{last4}")
    banned_cards_list = sorted(list(banned_cards))

    transactions: List[WLTransaction] = []
    counter = 1

    for d, count in zip(days, txns_per_day):
        if count == 0:
            continue

        day_events = events_by_day.get(d, [])
        driver_event = max(day_events, key=lambda e: e["gaussian"])["label"] if day_events else None

        base_decline = 0.08
        decline_rate_day = clamp(base_decline + 0.8 * decline_by_day.get(d, 0.0), 0.0, 0.80)
        risk_intensity_day = float(highrisk_by_day.get(d, 0.0))
        gambling_bias_day = float(gambling_share_by_day.get(d, 0.02))
        if d.weekday() >= 5:
            gambling_bias_day = clamp(gambling_bias_day + 0.03, 0.0, 0.5)

        for k in range(count):
            # Assign repeat user via Zipf rank
            aidx = indices[zipf_ranks[(len(transactions)) % len(zipf_ranks)]]
            user_id = user_ids[aidx]
            user_name = user_names[aidx]

            terminal_id = rng.choice(terminals)
            place = rng.choice(places)
            brand, bin6, last4 = choose_brand_bin(rng)
            pan_key = f"{bin6}{last4}"
            banned_card = (pan_key in banned_cards)

            txn_type = choose_txn_type(rng, offline_bias=(0.5 if (driver_event == "system outage") else 0.0), gambling_bias=gambling_bias_day)
            terminal_type = choose_terminal_type(rng, txn_type)
            mcc = choose_mcc(rng, high_risk_bias=risk_intensity_day, gambling_bias=gambling_bias_day)
            amount = amount_model(rng, mcc, gambling_bias_day)

            hour, minute, second = sample_time_of_day(rng, d, gambling_bias_day)
            txn_dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)
            completion_dt = txn_dt + timedelta(minutes=rng.randint(1, 90)) if rng.random() < 0.92 else None

            international = rng.random() < (0.06 + 0.10 * risk_intensity_day)
            offline = (txn_type == "offline")
            # Recipient details for transfer-like or risky flows
            recipient_user_id = recipient_user_name = recipient_country_code = None
            if txn_type in ("oct", "credit") or mcc in ("6051", "7995"):
                ridx = rng.randint(0, n_recips - 1)
                recipient_user_id = recip_ids[ridx]
                recipient_user_name = recip_names[ridx]
                recipient_country_code = recip_countries[ridx]
                # International more likely if recipient differs
                if recipient_country_code and recipient_country_code != country:
                    international = True

            # Shady region heuristic
            shady_region = False
            if international and rng.random() < (0.20 + 0.30 * risk_intensity_day):
                shady_region = True

            status, decline_reason = choose_status(rng, txn_type, decline_rate_day, risk_intensity_day, banned_card)

            # Risk factors based on type/MCC/flags
            risk_factors: List[str] = []
            if mcc == "7995" or txn_type == "gambling":
                risk_factors.append("gambling")
            if mcc == "5813":
                risk_factors.append("adult_content")
            if mcc == "6051":
                risk_factors.append("crypto_transfer")
            if txn_type in ("moto","ecommerce"):
                risk_factors.append("card_not_present")
            if txn_type in ("oct","credit"):
                risk_factors.append("cross_border" if international else "transfer")
            if mcc == "6011":
                risk_factors.append("cash_advance"); risk_factors.append("atm")
            if offline:
                risk_factors.append("offline_swipe")
            if banned_card:
                risk_factors.append("blocked_card")
            if rng.random() < (0.04 + 0.10 * risk_intensity_day):
                risk_factors.append("velocity")
            if rng.random() < (0.02 + 0.06 * risk_intensity_day):
                risk_factors.append("synthetic_id")
            if status == "chargeback":
                risk_factors.append("disputed")
            if status == "refunded":
                risk_factors.append("refund_abuse") if rng.random() < 0.10 else None
            if risk_intensity_day > 0.25:
                risk_factors.append("merchant_risk_category")

            risk_flags = {
                "high_risk_mcc": (mcc in ("7995", "6011", "6051", "5813", "5921")),
                "fraud_suspected": (status == "declined" and (decline_reason == "suspected_fraud")) or (rng.random() < (0.01 + 0.08 * risk_intensity_day)),
                "shady_region": shady_region
            }

            risk_score = risk_score_from(risk_flags, decline_rate_day, risk_intensity_day, risk_factors)

            txn = WLTransaction(
                txn_id=f"wlt_{base36(counter)}{uuid.uuid4().hex[:5]}",
                merchant_id=merchant_code_8 if len(merchant_code_8) == 8 else (merchant_code_8[:6] + "00"),
                acceptor_id=acceptor_id,
                sno=counter,
                amount=amount,
                currency_code=currency,
                card_brand=brand,
                card_bin=bin6,
                card_last4=last4,
                pan_masked=mask_pan(bin6, last4),
                banned_card=banned_card,
                txn_type=txn_type,
                terminal_type=terminal_type,
                mcc=mcc,
                terminal_id=terminal_id,
                place=place,
                user_id=user_id,
                user_name=user_name,
                recipient_user_id=recipient_user_id,
                recipient_user_name=recipient_user_name,
                recipient_country_code=recipient_country_code,
                txn_time=isoformat_dt(txn_dt),
                completion_time=(isoformat_dt(completion_dt) if completion_dt else None),
                status=status,
                decline_reason=decline_reason,
                country_code=country,
                city=city,
                international=international,
                offline=offline,
                shady_region=shady_region,
                risk_flags=risk_flags,
                risk_factors=risk_factors,
                risk_score=risk_score,
                event_label=driver_event
            )
            transactions.append(txn)
            counter += 1

    # Sort and ensure strictly increasing txn_time
    transactions.sort(key=lambda t: t.txn_time)
    last_dt = None
    for t in transactions:
        cur_dt = datetime.fromisoformat(t.txn_time.replace("Z", "+00:00"))
        if last_dt is not None and cur_dt <= last_dt:
            cur_dt = last_dt + timedelta(seconds=1)
            t.txn_time = isoformat_dt(cur_dt)
        last_dt = cur_dt

    # Summaries
    status_counts = {}
    risk_avg = 0.0
    declines = 0
    banned_hits = 0
    factor_counts: Dict[str, int] = {}
    for t in transactions:
        status_counts[t.status] = status_counts.get(t.status, 0) + 1
        risk_avg += t.risk_score
        declines += 1 if t.status == "declined" else 0
        banned_hits += 1 if t.banned_card else 0
        for f in t.risk_factors or []:
            factor_counts[f] = factor_counts.get(f, 0) + 1
    risk_avg = (risk_avg / max(1, len(transactions))) if transactions else 0.0
    realized_decline_rate = declines / max(1, len(transactions))

    out = {
        "merchant": asdict(merchant),
        "trend_plan": trend_plan,
        "banned_cards": banned_cards_list,  # list of bin6+last4 combos
        "transactions": [asdict(t) for t in transactions],
        "summary": {
            "transaction_count": len(transactions),
            "status_counts": status_counts,
            "avg_risk_score": round(float(risk_avg), 2),
            "decline_rate": round(float(realized_decline_rate), 4),
            "banned_card_hits": int(banned_hits),
            "unique_users": len(set([t.user_id for t in transactions])),
            "risk_factor_counts": factor_counts
        }
    }

    if out_json_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_merchant = merchant_name.lower().replace(" ", "")
        out_json_path = f"wl_{safe_merchant}_{start_date}_to_{end_date}_{ts}.json".replace(":", "-")

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return out_json_path


# --------------------------- Example usage ---------------------------

if __name__ == "__main__":
    merchant = "Kingfisher"
    path = generate_fake_wl_json(
        merchant_name=merchant,
        start_date="2020-01-01",
        end_date="2024-12-31",
        n_transactions=120000,
        trend_plan=[
            {"month": "2023-06", "intensity": 0.85, "label": "fraud ring", "transactions": "12%", "high_risk_intensity": 0.6, "decline_rate": 0.28},
            {"month": "2023-09", "intensity": 0.55, "label": "normal", "transactions": "8%", "decline_rate": "7%"},
            {"month": "2024-01", "intensity": 0.90, "label": "gambling spike", "transactions": "18%", "high_risk_intensity": 0.35, "gambling_share": 0.20}
        ],
        seed=42,
        out_json_path=None
    )
    print("Saved WL JSON to:", path)