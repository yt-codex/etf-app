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

Legacy command names (`patch-data`, `stage1-refresh`, `classify-taxonomy`, `recommend-strategies`) still work.

## Repo layout

Pipeline modules now live under `src/etf_app/` with descriptive names such as `listing_ingest.py`, `listing_hygiene.py`, `universe_refine.py`, and issuer-specific enrichers.

Tests live under `tests/`, generated outputs under `artifacts/`, and local caches under `kid_cache/`.

Artifacts are written under `artifacts/`.
