import os
import json
import argparse
import random
from datetime import datetime
from typing import List, Dict, Any, Optional


def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")


def random_ticker(name: str) -> str:
    letters = "".join([c for c in (name or "").upper() if c.isalpha()])
    if len(letters) >= 3:
        return letters[:3]
    return "MRC"


def random_trend_plan(start: str, end: str, rng: random.Random, n_min=3, n_max=5) -> List[Dict[str, Any]]:
    from datetime import date
    def parse_month(s):
        y, m, *_ = s.split("-")
        return date(int(y), int(m), 1)
    s = parse_month(start)
    e = parse_month(end)
    months = []
    cur_y, cur_m = s.year, s.month
    while (cur_y < e.year) or (cur_y == e.year and cur_m <= e.month):
        months.append(f"{cur_y}-{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    if not months:
        return []
    k = rng.randint(n_min, min(n_max, max(3, len(months))))
    picks = sorted(rng.sample(range(len(months)), k=k))
    pos_labels = ["spike - new product launch", "feature rollout", "award", "partnership", "buyback", "expansion"]
    neg_labels = ["spike - data breach", "lawsuit", "outage", "recall", "regulatory fine", "scandal"]
    neu_labels = ["normal", "promo period", "seasonal buzz"]
    plan = []
    for idx in picks:
        m = months[idx]
        r = rng.random()
        if r < 0.35:
            label = rng.choice(neg_labels); intensity = rng.uniform(0.65, 0.95)
        elif r < 0.70:
            label = rng.choice(pos_labels); intensity = rng.uniform(0.55, 0.90)
        else:
            label = rng.choice(neu_labels); intensity = rng.uniform(0.45, 0.70)
        item = {"month": m, "intensity": round(float(intensity), 2), "label": label}
        # For content-heavy sources, sometimes set share
        if rng.random() < 0.6:
            item["posts"] = f"{rng.randint(2,15)}%"
        plan.append(item)
    return plan


def core_to_stream_trends(core: List[Dict[str, Any]], rng: random.Random) -> Dict[str, List[Dict[str, Any]]]:
    import copy
    def with_share(key: str, lo=2, hi=20):
        out = []
        for e in core:
            ee = copy.deepcopy(e)
            if rng.random() < 0.6:
                ee[key] = f"{rng.randint(lo, hi)}%"
            else:
                ee.pop("posts", None)
            out.append(ee)
        return out

    def stockify():
        out = []
        for e in core:
            ee = {k: e[k] for k in ("month", "intensity", "label")}
            lab = ee["label"].lower()
            if any(k in lab for k in ["breach", "lawsuit", "outage", "recall", "fine", "scandal"]):
                ret = random.uniform(-0.15, -0.05)
                vol = random.choice(["+40%", "+60%", "1.4x", "1.6x"])
                volm = random.choice(["x1.6", "x2.0", "180%"])
            elif any(k in lab for k in ["launch", "award", "partnership", "buyback", "expansion"]):
                ret = random.uniform(0.04, 0.14)
                vol = random.choice(["+20%", "+40%", "1.2x", "1.4x"])
                volm = random.choice(["x1.3", "x1.6", "150%"])
            else:
                ret = random.uniform(-0.01, 0.02)
                vol = random.choice(["+10%", "1.1x", "1.0x"])
                volm = random.choice(["x1.1", "110%", "x1.0"])
            ee["return"] = round(float(ret), 3)
            ee["volatility"] = vol
            ee["volume"] = volm
            out.append(ee)
        return out

    def wlify():
        out = []
        for e in core:
            ee = {k: e[k] for k in ("month", "intensity", "label")}
            # Optionally attach per-month transaction share
            if rng.random() < 0.6:
                ee["transactions"] = f"{rng.randint(2, 20)}%"
            lab = ee["label"].lower()
            if any(k in lab for k in ["breach", "lawsuit", "scandal", "outage", "recall", "fine"]):
                ee["high_risk_intensity"] = round(random.uniform(0.30, 0.70), 3)
                ee["decline_rate"] = round(random.uniform(0.15, 0.35), 3) if rng.random() < 0.6 else None
                if rng.random() < 0.5:
                    ee["gambling_share"] = round(random.uniform(0.04, 0.15), 3)
            elif any(k in lab for k in ["launch","award","partnership","buyback","expansion"]):
                ee["high_risk_intensity"] = round(random.uniform(0.05, 0.15), 3)
                ee["decline_rate"] = round(random.uniform(0.02, 0.08), 3) if rng.random() < 0.4 else None
                if rng.random() < 0.4:
                    ee["gambling_share"] = round(random.uniform(0.02, 0.08), 3)
            else:
                ee["high_risk_intensity"] = round(random.uniform(0.08, 0.22), 3)
                ee["decline_rate"] = round(random.uniform(0.05, 0.12), 3) if rng.random() < 0.5 else None
                if rng.random() < 0.5:
                    ee["gambling_share"] = round(random.uniform(0.03, 0.12), 3)
            out.append(ee)
        return out

    return {
        "tweets": with_share("posts", 2, 15),
        "reddit": with_share("posts", 2, 15),
        "news": with_share("articles", 2, 15),
        "reviews": with_share("reviews", 2, 22),
        "stock": stockify(),
        "wl": wlify()
    }


def random_merchant_block(name: str, start_date: str, end_date: str, rng: random.Random) -> Dict[str, Any]:
    slug = slugify(name)
    ticker = random_ticker(name)
    core = random_trend_plan(start_date[:7], end_date[:7], rng)
    tr = core_to_stream_trends(core, rng)

    tweets_n = rng.randint(5000, 20000)
    reddit_n = rng.randint(3000, 12000)
    news_n = rng.randint(3000, 9000)
    reviews_n = rng.randint(12000, 40000)
    products_n = rng.randint(5, 12)
    merchant_score = round(rng.uniform(3.7, 4.5), 2)

    stock_base = round(rng.uniform(2.0, 400.0), 2)
    stock_sigma = round(rng.uniform(0.25, 0.55), 2)
    shares_out = rng.randrange(100_000_000, 5_000_000_000, 10_000)
    avg_vol = rng.randrange(1_000_000, 15_000_000, 1000)
    total_ret = round(rng.uniform(-0.25, 0.60), 3)

    wl_n = rng.randint(20000, 180000)

    return {
        "merchant_name": name,
        "start_date": start_date,
        "end_date": end_date,
        "seed": rng.randint(1, 10_000_000),
        "tweets": {
            "enabled": True,
            "n_tweets": tweets_n,
            "trend_plan": tr["tweets"],
        },
        "reddit": {
            "enabled": True,
            "n_posts": reddit_n,
            "trend_plan": tr["reddit"],
        },
        "news": {
            "enabled": True,
            "n_articles": news_n,
            "trend_plan": tr["news"],
        },
        "reviews": {
            "enabled": True,
            "n_products": products_n,
            "merchant_score": merchant_score,
            "n_reviews": reviews_n,
            "trend_plan": tr["reviews"],
        },
        "stock": {
            "enabled": True,
            "ticker": ticker,
            "base_price": stock_base,
            "sigma_annual": stock_sigma,
            "avg_daily_volume": int(avg_vol),
            "shares_outstanding": int(shares_out),
            "target_total_return": total_ret,
            "trend_plan": tr["stock"],
        },
        "wl": {
            "enabled": True,
            "n_transactions": wl_n,
            "trend_plan": tr["wl"]
        }
    }


def merge_custom_into_random(random_block: Dict[str, Any], custom_block: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(random_block)
    for key, val in custom_block.items():
        if key in ("tweets", "reddit", "news", "reviews", "stock", "wl") and isinstance(val, dict):
            merged = dict(random_block.get(key, {}))
            merged.update(val)
            out[key] = merged
        else:
            out[key] = val
    return out


def main():
    ap = argparse.ArgumentParser(description="Build a data_preset.json for merchants.")
    ap.add_argument("--num", type=int, required=True, help="Number of merchants to generate.")
    ap.add_argument("--start_date", default="2020-01-01")
    ap.add_argument("--end_date", default="2024-12-31")
    ap.add_argument("--out", default="preset.json")
    ap.add_argument("--custom", action="append", default=None, help="Path to a JSON file with a custom merchant block (can repeat).")
    ap.add_argument("--merchant_name", default=None, help="Quick custom merchant name (optional).")
    ap.add_argument("--seed", type=int, default=777, help="Base RNG seed for reproducibility.")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    merchants: List[Dict[str, Any]] = []

    base_names = [
        "HomeGear", "BuildPro", "BrightLite", "GardenCore", "FixItCo", "ToolNest",
        "ApexDIY", "PrimeHome", "HandyMart", "CraftWorks", "ValueFix", "MegaBuild",
        "UrbanTools", "GreenWorks", "SmartDIY", "HomeForge", "SolidCore", "EdgeTools"
    ]

    customs: List[Dict[str, Any]] = []
    if args.merchant_name:
        customs.append({"merchant_name": args.merchant_name})
    if args.custom:
        for p in args.custom:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    customs.extend(data)
                elif isinstance(data, dict):
                    customs.append(data)

    count_needed = args.num
    used_names = set()

    def next_name():
        if base_names:
            name = base_names.pop(0)
        else:
            name = f"Merchant_{rng.randint(1000,9999)}"
        while name in used_names:
            name = f"Merchant_{rng.randint(1000,9999)}"
        used_names.add(name)
        return name

    for _ in range(count_needed):
        nm = next_name()
        block = random_merchant_block(nm, args.start_date, args.end_date, rng)
        merchants.append(block)

    for c in customs:
        cname = c.get("merchant_name") or next_name()
        idx = None
        for i, m in enumerate(merchants):
            if m["merchant_name"].lower() == cname.lower():
                idx = i
                break
        if idx is None:
            idx = 0
        merged = merge_custom_into_random(merchants[idx], c)
        merged["start_date"] = merged.get("start_date", args.start_date)
        merged["end_date"] = merged.get("end_date", args.end_date)
        merged["merchant_name"] = merged.get("merchant_name", merchants[idx]["merchant_name"])
        merchants[idx] = merged

    preset = {
        "global": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "output_dir": ".",
            "defaults": {
                "wl": {"n_transactions": 50000}
            },
        },
        "merchants": merchants
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(preset, f, ensure_ascii=False, indent=2)
    print("Saved preset to:", os.path.abspath(args.out))


if __name__ == "__main__":
    main()