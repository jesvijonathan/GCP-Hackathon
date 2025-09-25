#!/usr/bin/env python3
# main_data.py
# Generate all fake datasets from a preset JSON, with console progress bars.

import os
import json
import argparse
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

# Generators
from tweets_data import generate_fake_tweets_json
from reddit_data import generate_fake_reddit_json
from news_data import generate_fake_news_json
from reviews_data import generate_fake_reviews_json
from stock_data import generate_fake_stock_json
from wl_data import generate_fake_wl_json  # NEW


def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def pick_seed(default_seed: Optional[int], fallback: int) -> Optional[int]:
    if isinstance(default_seed, int):
        return default_seed
    return fallback


# --------------------------- Progress helpers ---------------------------

BAR_FILL = "█"
BAR_EMPTY = "·"

def render_bar(cur: int, total: int, length: int = 32) -> str:
    total = max(1, int(total))
    cur = max(0, min(cur, total))
    filled = int(length * (cur / total))
    return BAR_FILL * filled + BAR_EMPTY * (length - filled)

def print_overall_progress(iteration: int, total: int, prefix: str = "Overall", suffix: str = "", length: int = 40, show: bool = True):
    if not show:
        return
    bar = render_bar(iteration, total, length)
    pct = int((iteration / max(1, total)) * 100)
    line = f"\r{prefix}: [{bar}] {iteration}/{total} ({pct}%) {suffix}"
    sys.stdout.write(line)
    sys.stdout.flush()
    if iteration >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()

def print_merchant_progress(name: str, cur: int, total: int, stage: str, length: int = 30, show: bool = True, final: bool = False):
    if not show:
        return
    bar = render_bar(cur, total, length)
    pct = int((cur / max(1, total)) * 100)
    line = f"\r[{name}] [{bar}] {cur}/{total} ({pct}%) - {stage}"
    sys.stdout.write(line)
    sys.stdout.flush()
    if final:
        sys.stdout.write("\n")
        sys.stdout.flush()


# --------------------------- Defaults ---------------------------

def fill_stream_defaults(merchant: Dict[str, Any], global_defaults: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "tweets": {"n_tweets": 12000},
        "reddit": {"n_posts": 7000},
        "news": {"n_articles": 6000},
        "reviews": {"n_reviews": 20000, "n_products": 8, "merchant_score": 4.2},
        "stock": {"base_price": 250.0, "sigma_annual": 0.35, "avg_daily_volume": 5_000_000},
        "wl": {"n_transactions": 50000}  # NEW
    }
    out = {}
    for k, v in defaults.items():
        out[k] = dict(v)
        if k in merchant and isinstance(merchant[k], dict):
            out[k].update(merchant[k])
        if global_defaults.get(k):
            for kk, vv in global_defaults[k].items():
                out[k].setdefault(kk, vv)
    return out


# --------------------------- Generation per merchant ---------------------------

def generate_for_merchant(
    merchant_cfg: Dict[str, Any],
    global_cfg: Dict[str, Any],
    outdir: str,
    base_seed: int,
    timestamp: str,
    show_progress: bool = True
):
    result = {"merchant": merchant_cfg.get("merchant_name"), "paths": {}, "errors": {}}

    merchant_name = merchant_cfg["merchant_name"]
    slug = slugify(merchant_name)
    start_date = merchant_cfg.get("start_date") or global_cfg.get("start_date") or "2020-01-01"
    end_date = merchant_cfg.get("end_date") or global_cfg.get("end_date") or "2026-12-31"

    streams = fill_stream_defaults(merchant_cfg, global_cfg.get("defaults") or {})

    enabled_streams: List[str] = []
    for s in ("tweets", "reddit", "news", "reviews", "stock", "wl"):  # NEW wl
        if (streams.get(s) or {}).get("enabled", True):
            enabled_streams.append(s)
    total_steps = max(1, len(enabled_streams))
    cur_steps = 0

    m_outdir = os.path.join(outdir, slug)
    ensure_dir(m_outdir)

    # Tweets
    if (streams.get("tweets") or {}).get("enabled", True):
        stage = "tweets (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            tcfg = streams["tweets"]
            p = os.path.join(m_outdir, f"tweets_{slug}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_tweets_json(
                merchant_name=merchant_name,
                start_date=start_date,
                end_date=end_date,
                n_tweets=int(tcfg.get("n_tweets", 12000)),
                trend_plan=tcfg.get("trend_plan"),
                seed=pick_seed(tcfg.get("seed"), base_seed + 11),
                out_json_path=p,
            )
            result["paths"]["tweets"] = path
            cur_steps += 1
            stage = "tweets (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        except Exception as e:
            result["errors"]["tweets"] = str(e)
            cur_steps += 1
            stage = f"tweets (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)

    # Reddit
    if (streams.get("reddit") or {}).get("enabled", True):
        stage = "reddit (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            rcfg = streams["reddit"]
            p = os.path.join(m_outdir, f"reddit_{slug}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_reddit_json(
                merchant_name=merchant_name,
                start_date=start_date,
                end_date=end_date,
                n_posts=int(rcfg.get("n_posts", 7000)),
                trend_plan=rcfg.get("trend_plan"),
                seed=pick_seed(rcfg.get("seed"), base_seed + 22),
                out_json_path=p,
            )
            result["paths"]["reddit"] = path
            cur_steps += 1
            stage = "reddit (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        except Exception as e:
            result["errors"]["reddit"] = str(e)
            cur_steps += 1
            stage = f"reddit (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)

    # News
    if (streams.get("news") or {}).get("enabled", True):
        stage = "news (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            ncfg = streams["news"]
            p = os.path.join(m_outdir, f"news_{slug}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_news_json(
                merchant_name=merchant_name,
                start_date=start_date,
                end_date=end_date,
                n_articles=int(ncfg.get("n_articles", 6000)),
                trend_plan=ncfg.get("trend_plan"),
                seed=pick_seed(ncfg.get("seed"), base_seed + 33),
                out_json_path=p,
            )
            result["paths"]["news"] = path
            cur_steps += 1
            stage = "news (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        except Exception as e:
            result["errors"]["news"] = str(e)
            cur_steps += 1
            stage = f"news (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)

    # Reviews
    if (streams.get("reviews") or {}).get("enabled", True):
        stage = "reviews (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            rcfg = streams["reviews"]
            p = os.path.join(m_outdir, f"reviews_{slug}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_reviews_json(
                merchant_name=merchant_name,
                n_products=int(rcfg.get("n_products", 8)),
                merchant_score=float(rcfg.get("merchant_score", 4.2)),
                n_reviews=int(rcfg.get("n_reviews", 20000)),
                start_date=start_date,
                end_date=end_date,
                trend_plan=rcfg.get("trend_plan"),
                seed=pick_seed(rcfg.get("seed"), base_seed + 44),
                out_json_path=p,
                save_summary=True
            )
            result["paths"]["reviews"] = path
            result["paths"]["reviews_summary"] = p.replace(".json", "_summary.json")
            cur_steps += 1
            stage = "reviews (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        except Exception as e:
            result["errors"]["reviews"] = str(e)
            cur_steps += 1
            stage = f"reviews (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)

    # Stock
    if (streams.get("stock") or {}).get("enabled", True):
        stage = "stock (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            scfg = streams["stock"]
            p = os.path.join(m_outdir, f"stock_{(scfg.get('ticker') or slug[:4]).lower()}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_stock_json(
                merchant_name=merchant_name,
                ticker=scfg.get("ticker"),
                start_date=start_date,
                end_date=end_date,
                base_price=float(scfg.get("base_price", 250.0)),
                shares_outstanding=int(scfg.get("shares_outstanding", 2_000_000_000)),
                currency=str(scfg.get("currency", "GBP")),
                mu_annual=scfg.get("mu_annual"),
                sigma_annual=float(scfg.get("sigma_annual", 0.35)),
                avg_daily_volume=int(scfg.get("avg_daily_volume", 5_000_000)),
                target_end_price=scfg.get("target_end_price"),
                target_total_return=scfg.get("target_total_return"),
                trend_plan=scfg.get("trend_plan"),
                seed=pick_seed(scfg.get("seed"), base_seed + 55),
                out_json_path=p
            )
            result["paths"]["stock"] = path
            cur_steps += 1
            stage = "stock (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress, final=False)
        except Exception as e:
            result["errors"]["stock"] = str(e)
            cur_steps += 1
            stage = f"stock (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress, final=False)

    # WL (new)
    if (streams.get("wl") or {}).get("enabled", True):
        stage = "wl (generating)"
        print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress)
        try:
            wcfg = streams["wl"]
            p = os.path.join(m_outdir, f"wl_{slug}_{start_date}_to_{end_date}_{timestamp}.json")
            path = generate_fake_wl_json(
                merchant_name=merchant_name,
                start_date=start_date,
                end_date=end_date,
                n_transactions=int(wcfg.get("n_transactions", 50000)),
                trend_plan=wcfg.get("trend_plan"),
                seed=pick_seed(wcfg.get("seed"), base_seed + 66),
                out_json_path=p
            )
            result["paths"]["wl"] = path
            cur_steps += 1
            stage = "wl (done)"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress, final=True)
        except Exception as e:
            result["errors"]["wl"] = str(e)
            cur_steps += 1
            stage = f"wl (error: {e})"
            print_merchant_progress(merchant_name, cur_steps, total_steps, stage, show=show_progress, final=True)

    return result


# --------------------------- Main ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Generate all fake datasets from a preset JSON.")
    ap.add_argument("--preset", required=True, help="Path to preset.json")
    ap.add_argument("--outdir", default=".", help="Base output directory (default: current dir)")
    ap.add_argument("--seed", type=int, default=1234, help="Base seed to derive stream seeds (default 1234)")
    ap.add_argument("--manifest", default=None, help="Optional path to save a manifest JSON")
    ap.add_argument("--no_progress", action="store_true", help="Disable progress bars/logging")
    args = ap.parse_args()

    show_progress = not args.no_progress

    with open(args.preset, "r", encoding="utf-8") as f:
        preset = json.load(f)

    global_cfg = preset.get("global", {})
    merchants = preset.get("merchants", [])
    if not merchants:
        raise ValueError("Preset has no merchants.")

    base_dir = args.outdir or global_cfg.get("output_dir") or os.getcwd()
    outdir = os.path.join(base_dir, "output")
    ensure_dir(outdir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    manifest = {
        "preset": args.preset,
        "output_dir": os.path.abspath(outdir),
        "generated_at": timestamp,
        "merchants": []
    }

    total_merchants = len(merchants)
    if show_progress:
        print(f"Generating data for {total_merchants} merchants...")
        print_overall_progress(0, total_merchants, prefix="Overall")

    for idx, m in enumerate(merchants, start=1):
        if not m.get("merchant_name"):
            continue
        merchant_name = m["merchant_name"]
        base_seed = int(m.get("seed", args.seed))
        if show_progress:
            sys.stdout.write(f"\nMerchant: {merchant_name}\n")
            sys.stdout.flush()

        t0 = time.time()
        res = generate_for_merchant(m, global_cfg, outdir, base_seed, timestamp, show_progress=show_progress)
        t1 = time.time()
        manifest["merchants"].append(res)

        errs = list(res["errors"].keys())
        summary = f"Done: {res['merchant']} | errors: {errs or '-'} | {int((t1 - t0) * 1000)} ms"
        if show_progress:
            sys.stdout.write(summary + "\n")
            sys.stdout.flush()
        else:
            print(summary)

        print_overall_progress(idx, total_merchants, prefix="Overall")

    manifest_path = args.manifest or os.path.join(outdir, f"main_manifest_{timestamp}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print("Saved manifest to:", manifest_path)


if __name__ == "__main__":
    main()