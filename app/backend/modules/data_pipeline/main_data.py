import os
import json
import argparse
from datetime import datetime
from typing import Dict, Any, Optional

# Import your existing generators
# Make sure these files are in the same folder:
#   tweets_data.py -> generate_fake_tweets_json
#   reddit_data.py -> generate_fake_reddit_json
#   news_data.py   -> generate_fake_news_json
#   reviews_data.py-> generate_fake_reviews_json
#   stock_data.py  -> generate_fake_stock_json
from tweets_data import generate_fake_tweets_json
from reddit_data import generate_fake_reddit_json
from news_data import generate_fake_news_json
from reviews_data import generate_fake_reviews_json
from stock_data import generate_fake_stock_json


def slugify(text: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (text or "")).strip("_")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def pick_seed(default_seed: Optional[int], fallback: int) -> Optional[int]:
    if isinstance(default_seed, int):
        return default_seed
    return fallback


def fill_stream_defaults(merchant: Dict[str, Any], global_defaults: Dict[str, Any]) -> Dict[str, Any]:
    # Global defaults for ranges; counts are "typical-ish"
    defaults = {
        "tweets": {"n_tweets": 12000},
        "reddit": {"n_posts": 7000},
        "news": {"n_articles": 6000},
        "reviews": {"n_reviews": 20000, "n_products": 8, "merchant_score": 4.2},
        "stock": {"base_price": 250.0, "sigma_annual": 0.35, "avg_daily_volume": 5_000_000}
    }
    out = {}
    for k, v in defaults.items():
        out[k] = dict(v)
        if k in merchant and isinstance(merchant[k], dict):
            out[k].update(merchant[k])
        # allow global defaults override if provided
        if global_defaults.get(k):
            for kk, vv in global_defaults[k].items():
                out[k].setdefault(kk, vv)
    return out


def generate_for_merchant(merchant_cfg: Dict[str, Any], global_cfg: Dict[str, Any], outdir: str, base_seed: int, timestamp: str):
    result = {"merchant": merchant_cfg.get("merchant_name"), "paths": {}, "errors": {}}

    merchant_name = merchant_cfg["merchant_name"]
    slug = slugify(merchant_name)
    start_date = merchant_cfg.get("start_date") or global_cfg.get("start_date") or "2020-01-01"
    end_date = merchant_cfg.get("end_date") or global_cfg.get("end_date") or "2024-12-31"

    streams = fill_stream_defaults(merchant_cfg, global_cfg.get("defaults") or {})

    # Output dir per merchant
    m_outdir = os.path.join(outdir, slug)
    ensure_dir(m_outdir)

    # Tweets
    if (streams.get("tweets") or {}).get("enabled", True):
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
        except Exception as e:
            result["errors"]["tweets"] = str(e)

    # Reddit
    if (streams.get("reddit") or {}).get("enabled", True):
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
        except Exception as e:
            result["errors"]["reddit"] = str(e)

    # News
    if (streams.get("news") or {}).get("enabled", True):
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
        except Exception as e:
            result["errors"]["news"] = str(e)

    # Reviews
    if (streams.get("reviews") or {}).get("enabled", True):
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
            # summary path
            result["paths"]["reviews_summary"] = p.replace(".json", "_summary.json")
        except Exception as e:
            result["errors"]["reviews"] = str(e)

    # Stock
    if (streams.get("stock") or {}).get("enabled", True):
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
        except Exception as e:
            result["errors"]["stock"] = str(e)

    return result


def main():
    ap = argparse.ArgumentParser(description="Generate all fake datasets from a preset JSON.")
    ap.add_argument("--preset", required=True, help="Path to preset.json")
    ap.add_argument("--outdir", default=".", help="Base output directory (default: current dir)")
    ap.add_argument("--seed", type=int, default=1234, help="Base seed to derive stream seeds (default 1234)")
    ap.add_argument("--manifest", default=None, help="Optional path to save a manifest JSON")
    args = ap.parse_args()

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

    for m in merchants:
        if not m.get("merchant_name"):
            continue
        base_seed = int(m.get("seed", args.seed))
        res = generate_for_merchant(m, global_cfg, outdir, base_seed, timestamp)
        manifest["merchants"].append(res)
        print(f"Done: {res['merchant']} | errors: {list(res['errors'].keys()) or '-'}")

    # Save manifest
    manifest_path = args.manifest or os.path.join(outdir, f"main_manifest_{timestamp}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print("Saved manifest to:", manifest_path)


if __name__ == "__main__":
    main()

# python .\main_data.py --preset .\preset.json --seed 1234 