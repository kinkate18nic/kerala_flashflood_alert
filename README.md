# Kerala Flash-Flood Watch

Kerala Flash-Flood Watch is a static-first decision-support dashboard and PWA for Kerala monsoon monitoring. It is designed to run on free hosting using GitHub Pages plus scheduled GitHub Actions, while keeping all operational state file-based.

## What is implemented

- Source register for IMD, KSDMA, CWC, NASA IMERG, and operator inputs
- Scheduled ingestion pipeline with raw snapshot archiving and derived JSON publishing
- Hybrid rules-plus-scoring risk model for Kerala districts and curated hotspots
- Manual review flow for severe alerts
- Static PWA frontend with district/hotspot map, source health, evidence drill-down, and offline shell cache
- Fixture-based tests and sample outputs for local development

## Project layout

- `config/`: source register, thresholds, and source policies
- `data/manual/`: operator review approvals and optional local overrides
- `docs/`: GitHub Pages output
- `fixtures/`: real-structure-like fixtures used for tests and local bootstrap
- `runtime/`: raw snapshots, metrics, and intermediate outputs
- `scripts/`: pipeline, publishing, and operator utilities
- `src/shared/`: shared district/hotspot definitions and risk metadata
- `src/site/`: static frontend shell copied into `docs/`
- `tests/`: parser, model, and pipeline tests

## Local usage

```bash
npm run build
npm test
```

`npm run build` uses fixtures so the dashboard can render locally without live source access.

## Live operation

1. Set GitHub Pages to publish from the `docs/` folder on the default branch.
2. Enable the scheduled workflow in `.github/workflows/refresh-data.yml`.
3. Add any required secrets for optional sources:
   - `NASA_EARTHDATA_BEARER`
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. Replace operator templates in `data/manual/` as real source adapters are stabilized.

`NASA_IMERG_DATA_URL` is optional. If omitted, the live adapter uses the PPS IMERG Early GIS directory configured in `config/sources.json`.

## Manual review flow

- Severe alerts are generated with `review_state: "pending_review"` by default.
- Approve an alert with:

```bash
node scripts/review-alert.js --id <alert-id>
```

- The next pipeline run promotes approved severe alerts to `Reviewed severe alert`.

## Telegram dispatch

- Reviewed severe alerts can be forwarded with:

```bash
node scripts/send-telegram.js
```

- Dispatch is skipped unless `config/telegram.json` is enabled and the bot credentials are present.

## Important operational note

The current "map" is a schematic Kerala control map built from district and hotspot anchors. It is production-usable for monitoring, but real district polygons and DEM-derived hotspot shapes should replace the schematic anchors once the geospatial preprocessing phase is completed.
