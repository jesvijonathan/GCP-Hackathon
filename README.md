<div align="center">

# MerchantPulse360

An explainable merchant intelligence & dynamic risk scoring platform for payments fintechs.

_Multi‑source ingestion → feature engineering → composite + smoothed risk scores → real‑time dashboard & APIs._

</div>

---

## TL;DR
MerchantPulse360 continuously ingests public & contextual merchant signals (news, Reddit, X/Twitter, reviews, watchlist / transactional heuristics, synthetic stock proxies), normalizes and enriches them, computes explainable rolling window risk scores, and exposes both raw timelines and interpreted summaries via a FastAPI backend and a Vue 3 dashboard.

## Solution Overview (4 Pillars)
1. Unified Signal Ingestion – Pluggable adapters consolidate heterogeneous external + synthetic feeds into consistent time‑series collections.
2. Adaptive / Explainable Risk Engine – Rolling window scoring with component weights, confidence, smoothing, hysteresis and synthetic fallback for sparse data.
3. Transparent Insight Delivery – REST endpoints for merchant dossiers, timeline windows, component breakdowns, trend analytics & diagnostics.
4. Policy & Extensibility Layer – Presets, whitelist / overrides, backfill & simulation controls, plus future hooks for ML, alerting & webhooks.

## Key Features
- Multi‑source ingestion (news, Reddit, tweets, reviews, watchlist txns, stock proxies) + synthetic seeding for demos.
- Rolling interval risk evaluation (30m / 1h / 1d) with smoothing, damping, incident bumps & hysteresis state.
- Component explainability (sentiment slices, volatility, wl flag ratio, volume heuristics) + confidence scoring.
- High‑throughput backfill jobs with queued/running progress, ETA, adaptive batch sizing & prefetch cache.
- Simulated time & rewind / forward‑only evaluation modes for reproducible demos & what‑if scenarios.
- Preset generator to rapidly stand up realistic merchant universes & stream volume distributions.
- Diagnostics endpoints (probe, stream counts, evaluation windows, queue summary, metrics) for fast triage.
- Vue 3 dashboard (MerchantRisk view) for score gauge, factor deltas, timelines & alerts (extensible).

## Repository Structure (Condensed)
```
app/
   backend/
      main.py              # FastAPI service (merchants, streams, risk eval, diagnostics)
      merchant.py          # Merchant onboarding & preset integration
      risk_eval.py         # Core windowed risk scoring + evaluation documents + job manager
      preset.py            # Preset & synthetic stream generation CLI
      data_pipeline/       # Source adapters (news, reddit, reviews, tweets, stock, wl)
      manual_data/         # One-off scripts & backfill outputs (manifests)
      common.env           # Local environment sample
      requirements.txt     # Python deps
   frontend/
      src/                 # Vue 3 + Vite SPA (MerchantRisk.vue et al.)
      package.json         # Frontend deps & scripts
README.md               # (this file)
```

## High-Level Architecture
```
                               +----------------------+
 External Sources -> | Ingestion Adapters   |--+
   (news/reddit/etc)  | data_pipeline/*.py   |  | raw docs
                               +-----------+----------+  v
                                                 |      [MongoDB Collections]
                                                 v          tweets, reddit, news,
                               +------------------+   reviews, wl_transactions,
                               | Normalization &  |   stocks_prices/actions/earnings
                               |  Enrichment      |   merchants, preset, risk_scores,
                               +---------+--------+   merchant_evaluations, jobs
                                              |
                                 rolling window fetch
                                              v
                               +------------------+
                               | Risk Evaluator   |  (weights, smoothing, incident bump,
                               | risk_eval.py     |   hysteresis, synthetic fallback)
                               +---------+--------+
                                              |
                                 evaluation docs & scores
                                              v
                               +------------------+
                               | FastAPI Backend  |  main.py (REST)
                               +---------+--------+
                                              |
                                     JSON / CORS
                                              v
                               +------------------+
                               | Vue Dashboard    |  Merchant risk UI
                               +------------------+
```

### Logical Component Tree
```
MerchantPulse360
├─ User Interaction
│  ├─ Web Frontend (Vue SPA)
│  └─ Future: Admin / Policy UI
├─ API Layer (FastAPI)
│  ├─ Merchant CRUD / presets / restrictions
│  ├─ Streams aggregation (/v1/{merchant}/data)
│  ├─ Risk evaluation trigger & timelines
│  └─ Diagnostics & metrics
├─ Risk & Evaluation Engine (risk_eval.py)
│  ├─ Window planning, jobs, prefetch, smoothing
│  └─ Component normalization & explainability
├─ Data Ingestion & Synthetic Generation
│  ├─ Adapters (news, reddit, reviews, tweets, wl, stock)
│  └─ Preset + stream generation (preset.py)
├─ Persistence (MongoDB)
│  ├─ Raw streams & synthetic outputs
│  ├─ Risk windows (risk_scores)
│  ├─ Evaluation snapshots (merchant_evaluations)
│  └─ Job queue (risk_eval_jobs)
└─ Observability (planned): metrics, tracing, alerts
```

## Getting Started

### Prerequisites
- Python 3.12+ (tested with 3.13 locally)
- Node 18+ (for Vite / Vue)
- MongoDB (local or remote) – default URI: `mongodb://127.0.0.1:27017`

### 1. Backend Setup
```powershell
cd app/backend
python -m venv .venv
./.venv/Scripts/Activate.ps1
pip install -r requirements.txt

# (Optional) Export / adjust environment overrides
$env:MONGO_URI="mongodb://127.0.0.1:27017"
$env:DB_NAME="merchant_analytics"

# Run API (FastAPI + Uvicorn)
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
Visit: http://localhost:8000/ for root metadata; /docs for OpenAPI UI.

### 2. Generate Synthetic Merchants & Streams
```powershell
cd app/backend
python preset.py generate --num 5 --out preset.json --seed 123 --no-confirm
python preset.py ingest --preset preset.json --with-streams --scale 0.2 --max-workers 4
```
This creates merchants, generates stream data (scaled down for speed) and enables immediate scoring.

### 3. Trigger Risk Evaluation Backfill
```powershell
curl "http://localhost:8000/v1/Merchant_0_*/risk-eval/trigger?interval=30m&autoseed=false" | jq
```
Check progress:
```powershell
curl http://localhost:8000/v1/risk-eval/jobs | jq
```

### 4. Frontend (Dashboard)
```powershell
cd app/frontend
npm install
npm run dev
```
Visit: http://localhost:5173 (default Vite port). Ensure CORS open (backend sets `allow_origins=["*"]`).

## Core API Surface (Selected)
| Endpoint | Purpose |
|----------|---------|
| `GET /v1/merchants` | List merchant names |
| `POST /v1/onboard` | Async merchant onboarding + preset generation |
| `GET /v1/{merchant}/data` | Aggregate multi-stream window of raw docs |
| `POST /v1/{merchant}/risk-eval/trigger` | Plan/queue risk evaluation job |
| `GET /v1/{merchant}/risk-eval/scores` | Raw risk window documents (risk_scores) |
| `GET /v1/{merchant}/risk-eval/summary` | Condensed latest + trend summary |
| `GET /v1/dashboard/overview` | Latest score per merchant (leaderboard) |
| `GET /v1/dashboard/series` | Time-series risk windows for top N merchants |
| `GET /v1/{merchant}/risk-eval/diagnostics` | Deep stream field/timestamp integrity checks |
| `POST /v1/{merchant}/risk-eval/seed` | Synthetic data seeding (safety gated) |
| `GET /health` | Mongo connectivity check |

OpenAPI live docs: `http://localhost:8000/docs`.

### Example Curl Flow
```powershell
# 1. Onboard a merchant
curl -X POST http://localhost:8000/v1/onboard -H "Content-Type: application/json" -d '{"merchant_name":"acme-tools"}'

# 2. Trigger risk evaluation
curl "http://localhost:8000/v1/acme-tools/risk-eval/trigger?interval=30m"

# 3. Fetch latest scores
curl "http://localhost:8000/v1/acme-tools/risk-eval/scores?interval=30m&limit=10"

# 4. Summary view
curl "http://localhost:8000/v1/acme-tools/risk-eval/summary?interval=auto&lookback=40"
```

## Risk Scoring Model (Simplified)
Inputs per window (aligned start/end):
- Sentiment (tweet / reddit / news normalized 0..1 risk) – inverse of polarity.
- Reviews rating (inverse normalized 0..1 risk).
- Watchlist flag ratio (flagged / total) with threshold escalation.
- Stock volatility (normalized to 0..1 vs 5% baseline).
- Volume proxy (aggregated activity scaled; optional incident bump).

Pipeline Steps:
1. Fetch & normalize component metrics (prefetch fast path if large job).
2. Weight & blend → base composite risk (0..1).
3. Apply damping & incident bump (wl spike, market volatility) under caps.
4. Smooth via adaptive EMA (different rise/fall alphas) + hysteresis for “high risk”.
5. Persist raw `risk_scores` + enriched `merchant_evaluations` (smoothed + drivers + counts).

Confidence = ratio of components with data to all available components weighted by stream counts.

## Synthetic / Demo Modes
Environment toggles allow fully air‑gapped demos:
- `RISK_FAKE_MODE=1` – Inject fabricated timeline if no real windows exist.
- Synthetic seeds (`/v1/{merchant}/risk-eval/seed`) – backfill a realistic 48h / n‑minute sampling.

## Configuration (Key Env Vars)
| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | mongodb://127.0.0.1:27017 | Mongo connection |
| `DB_NAME` | merchant_analytics | Database name |
| `RISK_MAX_CONCURRENT` | 3 | Max concurrent backfill jobs |
| `RISK_PER_JOB_THREADS` | 6 | Threads per job for window compute |
| `RISK_PER_JOB_BATCH` | 24 | Initial batch size (adaptive) |
| `RISK_SIM_FORWARD_ONLY` | 1 | Simulated mode only advances forward windows |
| `RISK_SIM_FORWARD_BACKTRACK_WINDOWS` | 0 | Backtrack allowance when simulating |
| `RISK_FAKE_MODE` | 1 | Allow synthetic summary fabrication |
| `RISK_REALTIME_MODE` | 0 | (Planned) keep freshest windows updated automatically |
| `RISK_PREFETCH` | 1 | Enable bulk stream prefetch for jobs |

Additional tuning keys exist inside `risk_eval.py` (dampen, stability, hysteresis) and external JSON override (`risk_eval_config.json` if created).

## Development Workflow
1. Write / adjust adapter in `data_pipeline/` for new source.
2. Generate merchants (`preset.py generate`) and ingest streams (`preset.py streams`).
3. Trigger risk evaluation job.
4. View progress (`/v1/risk-eval/jobs`) & metrics (`/v1/risk-eval/metrics`).
5. Iterate model weights via config override (env or file) → re-trigger windows.
6. Frontend queries summary & series endpoints; refine UI components.

## Diagnostics & Troubleshooting
- Blank chart? Use `/v1/{merchant}/risk-eval/stream-counts` then `/diagnostics` to confirm timestamp scales (seconds vs ms) & missing fields.
- Slow backfill? Check batch sizing in job doc; adjust `RISK_PER_JOB_BATCH` or disable prefetch.
- No windows after simulated rewind? Ensure `RISK_SIM_LOOKBACK_DAYS` (if added) and backfill caps permit range.
- Confidence low? Streams missing data for interval; inspect probe endpoint `/risk-eval/probe`.

## Roadmap (Proposed Next Steps)
- Add real sentiment NLP pipeline (spaCy / transformer) replacing heuristic + word list.
- Introduce webhook dispatcher (score threshold events → Slack / email / webhook).
- Implement Redis caching for frequent merchant detail & summary calls.
- Add sanctions / adverse media external API adapters.
- Feature Store abstraction layer (parquet export + ML training hooks).
- Role-based auth & JWT (analyst, admin) with policy editing UI.
- Reserve / pricing recommendation prototype using risk trends.

## Contributing
Pull requests welcome (tests & lint coming soon). Please isolate feature toggles behind env flags when adding experimental components.

## License
Proprietary / Hackathon Prototype (choose appropriate license before public release).

## Attribution / Notes
Some data is synthetically generated for demonstration. Replace with real adapters before production use. Ensure regulatory / compliance review for any external feed usage (rate limits, ToS).

---
Questions or want a pitch‑deck one‑pager? Open an issue or extend this README section.
