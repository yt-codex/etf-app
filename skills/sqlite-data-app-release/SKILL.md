---
name: sqlite-data-app-release
description: Use when a repo maintains a large source SQLite database and publishes a smaller read-only SQLite artifact for an app. Covers refreshing the source DB, rebuilding derived read models, preserving data needed by search and filters, packaging a slim deploy DB, verifying parity, gzipping it, and preparing deployment settings such as db_url, db_version, and db_sha256.
metadata:
  short-description: Refresh and publish a slim SQLite app database
---

# SQLite Data App Release

Use this skill when a project has:
- a mutable source SQLite DB used by ingestion and enrichment
- derived read models such as normalized profile/taxonomy tables
- a smaller deploy DB for a read-only app
- deployment via a downloadable `.db` or `.db.gz` artifact

This pattern came from the UCITS ETF Atlas repo, but the workflow is reusable for any SQLite-backed data product.

## Core workflow

1. Identify the canonical refresh path.
   In this repo that is `etf-pipeline refresh-deploy-artifact`.
   In another repo, find the equivalent of:
   - raw/source ingestion
   - metadata enrichment
   - derived-table rebuild
   - deploy DB export
   - deploy packaging

2. Refresh the source DB first.
   Do not build a deploy artifact from stale source data.

3. Rebuild derived read models before export.
   Typical examples:
   - `product_profile`
   - `instrument_taxonomy`
   - current-cost views

4. Build the deploy DB from the source DB.
   Keep only what the app reads at runtime, but preserve any non-primary rows the app depends on for search or filtering.

5. Smoke-test the deploy DB against app queries.
   At minimum verify:
   - list/explorer query
   - detail query
   - one strategy/recommendation path
   - one completeness/coverage path

6. Gzip the deploy DB if the app bootstraps from `.gz`.

7. Compute and record SHA-256 of the decompressed `.db`, not the `.gz`, if the app verifies after unpacking.

8. Publish the artifact and update deployment settings.
   Typical settings:
   - `db_url`
   - `db_version`
   - `db_sha256`

## Guardrails

- Keep the full source DB out of git unless it is intentionally versioned.
- Treat the deploy DB as a build artifact.
- If the app shows one primary row per instrument but search must resolve alternate tickers or aliases, preserve the alias rows in the deploy DB.
- Do not assume “slim deploy DB” means “primary rows only”. Verify against real search/filter behavior.
- When a hosted app downloads `.gz`, make the artifact URL point to the archive but make the checksum match the uncompressed DB if that is what runtime validation uses.

## What to verify before publish

- Source DB and deploy DB return the same results for critical app reads.
- Search still works for alias identifiers, alternate tickers, or other non-primary keys.
- Filtering still works after slimming the DB.
- Artifact size is materially smaller than the source DB.
- Deployment metadata matches the actual uploaded artifact.

## Example commands from this repo

Refresh and package end to end:

```powershell
etf-pipeline refresh-deploy-artifact --db-path stage1_etf.db --version-label deploy-db-2026-03-10
```

Packaging-only rerun on an already refreshed source DB:

```powershell
etf-pipeline refresh-deploy-artifact --db-path stage1_etf.db --skip-refresh --skip-ft --skip-issuer-fees
```

Build only the slim deploy DB:

```powershell
etf-pipeline build-deploy-db --db-path stage1_etf.db --output-path deploy_stage1_etf.db
```

## Repo-specific mapping

In this repo, the equivalents are:
- source DB: `stage1_etf.db`
- slim deploy DB: `deploy_stage1_etf.db`
- packaged deploy artifact: `deploy_stage1_etf.db.gz`
- derived read models: `product_profile`, `instrument_taxonomy`
- runtime app: `streamlit_app.py`
- deploy bootstrap: `src/etf_app/db_bootstrap.py`

## Adaptation checklist for another repo

- Find the source DB path.
- Find the app’s runtime query surfaces.
- Find the derived tables/views that must be rebuilt before export.
- Confirm whether search/filter logic depends on secondary or alias rows.
- Build the smallest deploy DB that still preserves runtime behavior.
- Publish only after parity checks pass.
