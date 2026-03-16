# Kerala Flash-Flood Watch

Kerala Flash-Flood Watch is a static-first decision-support dashboard and PWA for Kerala monsoon monitoring. It is designed to run on free hosting using GitHub Pages plus scheduled GitHub Actions, while keeping all operational state file-based.

## What is implemented

- Source register for IMD, KSDMA, CWC, NASA IMERG, and operator inputs
- Scheduled ingestion pipeline with raw snapshot archiving and derived JSON publishing
- Hybrid rules-plus-scoring risk model for Kerala districts and curated hotspots
- Manual review flow for severe alerts
- Kerala administrative boundary integration using `geohacker/kerala` district and taluk GeoJSON layers
- NASA IMERG district rainfall aggregation over district polygons with fallback to representative points if boundaries fail
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
   - `PPS_EMAIL`
   - `PPS_PASSWORD` (optional if your PPS account uses the email as both username and password)
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. Replace operator templates in `data/manual/` as real source adapters are stabilized.

## Manual review flow

- Severe alerts are generated with `review_state: "pending_review"` by default.
- Approve an alert with:

```bash
node scripts/review-alert.js --id <alert-id>
```

- The next pipeline run promotes approved severe alerts to `Reviewed severe alert`.

The NASA IMERG adapter now uses PPS near-real-time access via the documented `text/imerg/gis/early/` listing and downloads the latest 30-minute, 3-hour, and 1-day GeoTIFF products directly.
District rainfall is now computed against Kerala district polygons from `geohacker/kerala` rather than fixed district sample points, and the pipeline publishes boundary metadata in `docs/data/latest/admin-areas.json`.

## Telegram dispatch

- Reviewed severe alerts can be forwarded with:

```bash
node scripts/send-telegram.js
```

- Dispatch is skipped unless `config/telegram.json` is enabled and the bot credentials are present.

## Important operational note

The current "map" is a schematic Kerala control map built from district and hotspot anchors. It is production-usable for monitoring, but real district polygons and DEM-derived hotspot shapes should replace the schematic anchors once the geospatial preprocessing phase is completed.
