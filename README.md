# NEPSE Pipeline

An end-to-end data engineering and ML pipeline for Nepal Stock Exchange (NEPSE) data.

## Tech Stack
- **Ingestion**: requests, BeautifulSoup
- **Storage**: PostgreSQL, SQLAlchemy
- **Transformation**: dbt, pandas
- **Orchestration**: Apache Airflow
- **ML**: scikit-learn, XGBoost
- **MLOps**: MLflow, FastAPI
- **Monitoring**: Grafana, Prometheus

## Architecture
raw scrape → PostgreSQL (raw) → dbt (clean) → ML model → FastAPI endpoint

## Setup
```bash
uv venv .venv
.venv\Scripts\Activate
uv pip install -r requirements.txt
```

## Data Sources
- Daily scraping: sharesansar.com
- Historical backfill (2024-2025): github.com/omitnomis/ShareSansarScraper

## Project Status
- [x] Ingestion — NEPSE scraper
- [x] Storage — PostgreSQL schema
- [x] Transformation — dbt models
- [ ] ML model
- [ ] API serving
- [ ] Monitoring