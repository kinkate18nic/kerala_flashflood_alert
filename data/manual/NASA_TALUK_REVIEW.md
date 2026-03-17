# NASA Taluk Review

Use `nasa-taluks-review.csv` during the 2-week NASA observation window.

## When to fill it

You do not need to fill it every day.

Use it:

- on active rain days
- every 2-3 days during the observation window
- whenever a taluk value looks suspicious

Target:

- at least 5-10 review sessions over 2 weeks
- at least 5-10 important taluks checked

## Recommended taluks

Start with these:

- `idukki--peerumade`
- `idukki--devikulam`
- `wayanad--vythiri`
- `pathanamthitta--ranni`
- `alappuzha--kuttanad`
- `ernakulam--aluva`
- `ernakulam--kochi`
- `thiruvananthapuram--thiruvananthapuram`

## Where to read the values from

Check these files:

- `docs/data/latest/observation-grid.json`
- `docs/data/latest/taluk-risk.json`
- `docs/data/latest/sources.json`
- `docs/data/latest/nasa-imerg-history.json`

## What to record

Fill one row per taluk review.

Important columns:

- `taluk_id`
- `district_id`
- `rain_1h_mm`
- `rain_24h_mm`
- `peak_30m_mm`
- `risk_level`
- `nasa_status`
- `freshness_minutes`
- `latest_30min_file`
- `latest_3hr_file`
- `latest_1day_file`
- `plausible_yes_no`
- `issue_type`
- `notes`

## What counts as plausible

Examples:

- steep Ghats taluks often show stronger rain during active monsoon bursts
- low-lying basins may show moderate short rain but strong 24h accumulation concern
- urban taluks may spike during short bursts, but repeated unrealistic extremes are suspicious

## Allowed issue_type values

- `none`
- `stale_data`
- `unexpected_low_rain`
- `unexpected_high_rain`
- `risk_driver_mismatch`
- `file_progression_issue`
- `other`

## Goal at the end of 2 weeks

At the end of the observation window, use:

- `nasa-taluks-review.csv`
- `docs/data/latest/nasa-imerg-history.json`

to decide whether NASA IMERG can be treated as complete for operational rainfall use.
