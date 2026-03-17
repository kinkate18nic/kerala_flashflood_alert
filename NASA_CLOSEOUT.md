# NASA IMERG Closeout

This document defines when the NASA IMERG rainfall source can be treated as complete for the Kerala Flash-Flood Watch system.

## Scope

NASA IMERG is the current rainfall backbone for:

- district rainfall
- taluk rainfall
- hotspot rainfall context through district and taluk linkage
- 1h, 3h, 6h, 24h, 3d, and 7d rainfall accumulations

The source is not considered fully closed just because downloads work. It is closed only when acquisition, aggregation, outputs, and operational monitoring all pass.

## Done Criteria

NASA IMERG is complete when all of the following are true:

1. Acquisition is stable for at least 14 consecutive days.
2. GitHub Actions success rate for the NASA adapter is at least 95%.
3. District polygon rainfall is working without reverting to point sampling under normal runs.
4. Taluk polygon rainfall is working under live runs.
5. `sources.json` shows correct freshness and status for NASA.
6. `observation-grid.json` contains district and taluk rainfall objects.
7. IMERG source metadata is stored per run:
   - latest 30min file
   - latest 3hr file
   - latest 1day file
8. District and taluk risk drivers explicitly mention rainfall evidence.
9. Manual review against 5-10 taluks shows no obvious spatial absurdities.
10. Failure and staleness history is recorded for the full 2-week observation window.

## Outputs To Monitor

Use these files during the closeout window:

- `docs/data/latest/sources.json`
- `docs/data/latest/observation-grid.json`
- `docs/data/latest/taluk-risk.json`
- `docs/data/latest/nasa-imerg-history.json`
- `runtime/metrics/nasa-imerg-history.json`

`nasa-imerg-history.json` is the primary operational log for this closeout. Each run records:

- generated time
- NASA issued time
- status
- parser status
- freshness
- notes
- district count
- taluk count
- latest 30min file
- latest 3hr file
- latest 1day file

## Manual Taluk Review

Review at least 5-10 taluks using the latest live IMERG outputs. Recommended starter set:

- `idukki--peerumade`
- `idukki--devikulam`
- `ernakulam--aluva`
- `pathanamthitta--ranni`
- `alappuzha--kuttanad`
- `wayanad--vythiri`
- `ernakulam--kochi`
- `thiruvananthapuram--thiruvananthapuram`

For each taluk, compare:

1. `observation-grid.json` taluk rainfall values
2. taluk risk drivers in `taluk-risk.json`
3. latest IMERG file names in `sources.json` and `nasa-imerg-history.json`
4. whether the values make geographic sense for the taluk and current weather situation

Record:

- review date/time
- taluk id
- 1h rainfall
- 24h rainfall
- peak 30min rainfall
- whether values look plausible
- notes on any mismatch

If a taluk looks obviously wrong, do not close NASA out yet. Investigate:

- taluk geometry coverage
- raster-cell intersection count
- stale PPS files
- district/taluk naming mismatch

## Two-Week Monitoring Procedure

For the next 14 days:

1. Let the scheduled workflow run normally.
2. Check `docs/data/latest/nasa-imerg-history.json` daily.
3. Count:
   - offline runs
   - stale runs
   - parser failures
4. Inspect whether the latest 30min, 3hr, and 1day files keep advancing as expected.

NASA should not be closed out if:

- frequent stale runs appear without explanation
- file windows stop advancing
- taluk counts drop unexpectedly
- parser status changes to failed
- rainfall evidence disappears from district or taluk outputs

## Closeout Decision

NASA can be marked complete after:

- 14 days of logged operation
- 95%+ successful live runs
- 5-10 taluks reviewed manually
- no major unexplained spatial or temporal anomalies

Once NASA is closed:

1. Treat it as the primary rainfall backbone.
2. Move next to IMD CAP hardening.
3. Then integrate CWC as the hydrology confirmation layer.
