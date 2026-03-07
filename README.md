# ETF App

This repository is being rebuilt around a single goal: a reliable, maintainable UCITS ETF database for Singaporean investors.

## Current focus

The first fixed layer is ingestion and data hygiene:

- explicit source-run reconciliation for listings
- current product profile materialization
- UCITS evidence backfill from issuer metadata and instrument names
- a single CLI entrypoint for ingestion, taxonomy, and recommendations

## Commands

Install the package in editable mode for local development:

```powershell
python -m pip install -e .[dev]
```

Install the Streamlit UI dependencies:

```powershell
python -m pip install -e .[ui]
```

Rebuild derived data without fetching new listings:

```powershell
etf-pipeline rebuild-derived-data --db-path stage1_etf.db
```

Refresh listings and rebuild the MVP universe:

```powershell
etf-pipeline refresh-data --db-path stage1_etf.db
```

Build the normalized taxonomy layer:

```powershell
etf-pipeline build-taxonomy --db-path stage1_etf.db
```

Generate taxonomy-backed strategy recommendations:

```powershell
etf-pipeline recommend --db-path stage1_etf.db
```

Serve the thin JSON API for browse/filter/recommend UI work:

```powershell
etf-pipeline serve-api --db-path stage1_etf.db --host 127.0.0.1 --port 8000
```

Run the Streamlit UI:

```powershell
$env:ETF_APP_DB_PATH='stage1_etf.db'
streamlit run streamlit_app.py
```

Legacy command names (`patch-data`, `stage1-refresh`, `classify-taxonomy`, `recommend-strategies`) still work.

## API

The API is intentionally thin and SQLite-backed. It uses the existing `product_profile`, `instrument_taxonomy`, completeness, and predefined strategy logic without introducing a web framework dependency.

Available endpoints:

- `/health`
- `/api/funds`
- `/api/funds/{isin}`
- `/api/filters`
- `/api/strategies`
- `/api/completeness`

## Streamlit UI

The UI lives in `streamlit_app.py` and reads directly from the normalized SQLite data model via the same query helpers that back the API.

For Streamlit hosting, the repo now includes `requirements.txt`, which installs the package with the `ui` extra.

## Repo layout

Pipeline modules now live under `src/etf_app/` with descriptive names such as `listing_ingest.py`, `listing_hygiene.py`, `universe_refine.py`, and issuer-specific enrichers.

Tests live under `tests/`, generated outputs under `artifacts/`, and local caches under `kid_cache/`.

Artifacts are written under `artifacts/`.
