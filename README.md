<div align="center">

# GCP-Hackathon : AI Powered Fintech Risk Engine

An explainable merchant intelligence & dynamic risk scoring platform for payments fintechs.

_Multi‑source ingestion → feature engineering → composite + smoothed risk scores → real‑time dashboard & APIs._

</div>

---

## TL;DR
This Project continuously ingests public & contextual merchant signals (news, Reddit, X/Twitter, reviews, watchlist / transactional heuristics, synthetic stock proxies), normalizes and enriches them, computes explainable rolling window risk scores, and exposes both raw timelines and interpreted summaries via a FastAPI backend and a Vue 3 dashboard.

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
This Project
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

## Quick Setup & Deployment

### Prerequisites
- **Linux/WSL Environment**: The setup script is designed for Ubuntu/Debian systems
- **Git**: For cloning and version control
- **Bash Shell**: Required to run the setup script

### 🚀 One-Command Setup Options

#### Option 1: Local Development Environment (Recommended for Testing)
```bash
# 1. Install development tools (gcloud, Docker, Terraform)
./setup.sh --dev

# 2. Deploy locally with Docker containers
./setup.sh --deploy-local
```

#### Option 2: Google Cloud Deployment (Production-Ready)
```bash
# 1. Install development tools
./setup.sh --dev

# 2. Deploy to Google Cloud Platform
./setup.sh --deploy-cloud
```

### 📋 Detailed Setup Instructions

#### For Local Development:

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd GCP-Hackathon
   ```

2. **Install Required Tools**
   ```bash
   ./setup.sh --dev
   ```
   This installs:
   - Google Cloud SDK (gcloud)
   - Docker & Docker Compose
   - Terraform
   
   **Note**: Log out and log back in after installation for Docker group changes to take effect.

3. **Deploy Locally**
   ```bash
   ./setup.sh --deploy-local
   ```
   
   On first run, you'll be prompted for:
   - MongoDB Root Username
   - MongoDB Root Password
   
   The script will:
   - Create local configuration in `./app/deployment/terraform.tfvars`
   - Build Docker images for frontend and backend
   - Start MongoDB, backend API, and frontend services
   - Display access URLs

4. **Access Your Application**
   - Frontend: `http://localhost:8080`
   - Backend API: `http://localhost:8000`
   - API Documentation: `http://localhost:8000/docs`
   - MongoDB: `mongodb://localhost:27017`

#### For Google Cloud Deployment:

1. **Prerequisites**
   - GCP Account with billing enabled
   - Project with necessary APIs enabled (Cloud Run, Artifact Registry, etc.)

2. **Install Tools & Authenticate**
   ```bash
   ./setup.sh --dev
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

3. **Deploy to Cloud**
   ```bash
   ./setup.sh --deploy-cloud
   ```
   
   The script will:
   - Build container images using Cloud Build
   - Deploy backend to Google Cloud Run
   - Deploy frontend with proper API endpoints
   - Configure networking and load balancing
   - Display deployment URLs

4. **Skip Image Rebuild (Faster Deployment)**
   ```bash
   ./setup.sh --deploy-cloud --skip-cloudbuild
   ```

### 🛠️ Additional Commands

| Command | Purpose |
|---------|---------|
| `./setup.sh --build-local` | Rebuild local Docker images only |
| `./setup.sh --undo-local` | Destroy local Docker environment |
| `./setup.sh --undo-cloud` | Destroy cloud infrastructure |
| `./setup.sh --help` | Display help menu |

### 🔧 Configuration

#### Environment Variables (Local Development)
Configuration is managed via `./app/deployment/terraform.tfvars`:

```hcl
project_id    = "your-gcp-project-id"  # Required for cloud deployment
region        = "us-central1"           # GCP region
deploy_target = "local"                 # "local" or "cloud"

# MongoDB credentials (local only)
mongo_root_user     = "admin"
mongo_root_password = "your-password"
```

#### Application Configuration
The backend uses these key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | mongodb://127.0.0.1:27017 | MongoDB connection string |
| `DB_NAME` | merchant_analytics | Database name |
| `RISK_MAX_CONCURRENT` | 3 | Max concurrent risk evaluation jobs |
| `RISK_FAKE_MODE` | 1 | Enable synthetic data generation |

### 🐛 Troubleshooting

#### Common Issues:

1. **Docker Permission Denied**
   ```bash
   # Log out and back in, or run:
   sudo usermod -aG docker $USER
   newgrp docker
   ```

2. **Terraform State Issues**
   ```bash
   # Reset local state if needed:
   cd ./app/deployment
   rm -rf .terraform terraform.tfstate*
   terraform init
   ```

3. **Port Already in Use**
   ```bash
   # Check what's using the ports:
   sudo netstat -tulpn | grep :8080
   sudo netstat -tulpn | grep :8000
   
   # Kill processes or change ports in terraform.tfvars
   ```

4. **Cloud Build Failures**
   - Ensure billing is enabled on your GCP project
   - Check that required APIs are enabled:
     ```bash
     gcloud services enable cloudbuild.googleapis.com
     gcloud services enable run.googleapis.com
     gcloud services enable artifactregistry.googleapis.com
     ```

#### Windows Users:
- Use WSL (Windows Subsystem for Linux) for the best experience
- Alternatively, adapt the PowerShell commands manually:
  ```powershell
  # Install Docker Desktop for Windows
  # Install Terraform manually
  # Use docker-compose up instead of the setup script
  ```

### 🔄 Development Workflow
1. Write / adjust adapter in `data_pipeline/` for new source.
2. Generate merchants (`preset.py generate`) and ingest streams (`preset.py streams`).
3. Trigger risk evaluation job.
4. View progress (`/v1/risk-eval/jobs`) & metrics (`/v1/risk-eval/metrics`).
5. Iterate model weights via config override (env or file) → re-trigger windows.
6. Frontend queries summary & series endpoints; refine UI components.

### 🏗️ Manual Development Setup (Alternative)
If you prefer manual setup or encounter issues with the automated script:

```bash
# Backend setup
cd app/backend
pip install -r requirements.txt
# Start MongoDB locally
python main.py

# Frontend setup (separate terminal)
cd app/frontend
npm install
npm run dev
```

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
