# Kerala Flash-Flood Watch

Live dashboard: [https://kinkate18nic.github.io/kerala_flashflood_alert/](https://kinkate18nic.github.io/kerala_flashflood_alert/)

Kerala Flash-Flood Watch is a public, static-first decision-support dashboard for Kerala monsoon and flood monitoring. It combines official warnings, official hydrology, radar, satellite rainfall, reservoir and dam status, and a small number of curated local-risk corridors into one place.

This project is meant to be transparent enough that:
- a normal user can understand what they are seeing
- a field worker can understand why an area is being highlighted
- a technical reviewer can inspect the inputs, assumptions, and limits
- future methodology changes can be documented clearly

This README is the public methodology note for the project. When the scoring model, thresholds, review rules, source list, map behavior, or operational workflow changes, this file should be updated in the same change.

## What This Dashboard Is For

This dashboard is designed to answer a practical question:

**Where in Kerala should we pay closer attention right now for flash-flood, runoff, river-rise, reservoir-release, or dam-related flood consequences?**

It is not a single-source warning system. It is a decision-support layer that combines:
- official IMD warning products
- official hydrology and reservoir signals
- short-lead rainfall and radar context
- terrain and runoff susceptibility
- curated hotspot knowledge for known flood-sensitive corridors

The aim is not to replace official warnings. The aim is to make official and environmental signals easier to interpret together.

## What This Dashboard Is Not

This project is **not**:
- an official government warning system
- a guarantee that a flood or landslide will happen
- a replacement for district control rooms, IMD, CWC, KSDMA, or field reports
- a fully automated public-severity broadcaster for the highest-risk category

Use it as a monitoring and prioritization tool, not as a stand-alone authority.

## Who This Is For

This dashboard is useful for:
- disaster-management volunteers
- journalists and public-interest monitors
- local observers and community groups
- field teams who want one place to review multiple signals quickly
- people who want evidence-linked alerts rather than a black-box score

## What The Dashboard Shows

The dashboard currently shows:
- a single operational headline at the top
- active alerts at `Watch` and above
- district risk cards for all 14 Kerala districts
- taluk risk cards for 61 taluks
- hotspot risk cards for curated local corridors and low-lying pockets
- source-health cards showing whether each source is current, older than usual, or unavailable

The **map** is currently a **district-only Kerala map with hotspot overlays**.  
Taluks are still scored and shown in card form, but the taluk map toggle has been removed because it was not reliable enough.

## Area Types

The model works at three practical levels:

### 1. Districts
Districts are the main operational layer. They aggregate warning, rainfall, radar, hydrology, and reservoir context at district scale.

### 2. Taluks
Taluks are a finer risk-summary layer. They use localized rainfall context and district/hotspot membership, but are shown as cards rather than map polygons.

### 3. Hotspots
Hotspots are curated named places where local geography or infrastructure makes flood consequences more likely. These include:
- steep high-range catchments
- river floodplains
- river confluences
- below-sea-level basins
- dam-downstream pockets
- urban drainage pockets

These are not meant to represent every possible local hazard point in Kerala. They are selected known sensitive corridors and pockets.

## Data Sources Used

The dashboard currently uses the following sources.

### IMD warning sources
- **IMD CAP RSS**
  - official severe weather CAP feed for Kerala
  - strongest formal warning source when district mapping is available
  - CAP items only contribute while they are still unexpired

- **IMD District Warning (Kerala)**
  - district-level warning map for Kerala
  - used as official district warning support
  - treated as a same-day bulletin rather than a minute-precise source

- **IMD District Nowcast (Kerala)**
  - short-lead district warning/nowcast map
  - used as short-duration corroboration for current conditions

- **IMD Station Nowcast (Kerala)**
  - station-level nowcast
  - used cautiously and only as hotspot support
  - not used as a district-wide trigger by itself

- **IMD Flash Flood Bulletin**
  - bulletin-style expert guidance
  - currently used as corroborating guidance, not as a sole severe trigger

### Official hydrology and reservoir sources
- **India-WRIS CWC Rainfall**
  - official station-based rainfall
  - treated as higher-trust daily rainfall evidence than satellite-only estimates

- **India-WRIS CWC River Water Level**
  - river level context from official stations
  - used for river-rise and consequence context

- **CWC FFS Live River Levels**
  - live flood forecasting / warning-watch style river stage data
  - used to support river-linked district and hotspot risk

- **KSDMA Daily Dam Levels (KSEB)**
  - daily reservoir status context
  - used as a corroborating modifier

- **KSDMA Daily Dam Levels (Irrigation)**
  - daily dam and controlled outflow context
  - used for downstream caution modifiers

### Environmental context sources
- **RainViewer Radar Nowcast**
  - short-lead radar signal
  - used as current weather support, not as a stand-alone severe source

- **NASA IMERG Near-Real-Time**
  - satellite rainfall backbone
  - used for short-duration and spatially continuous rainfall estimation

### Manual source
- **Operator Observation Override**
  - a manual fallback/override path
  - meant for exceptional use, not as the primary normal workflow

## Why Multiple Sources Are Needed

No single source is enough.

Examples:
- IMD may issue a broad warning, but rainfall may still be low in a specific district
- radar may show active cells, but official warning products may still be quiet
- a river or dam situation may increase risk even without extreme short-duration rainfall
- terrain and wetness matter because the same rain does not produce the same consequence everywhere

The dashboard exists because operational risk in Kerala depends on **both hazard and vulnerability/consequence context**.

## How Scoring Works

The model produces a **composite score from 0 to 100** for districts, taluks, and hotspots.

That score is not magic. It is built from weighted components.

### Main score families

The district model currently distributes weight roughly as follows:

| Score family | Weight | What it means |
| --- | ---: | --- |
| Official warning (`CAP` / district warning) | 24% | Official warning support |
| Flash-flood bulletin | 10% | Expert bulletin corroboration |
| Recent rainfall | 24% | Short-duration and daily rain context |
| Antecedent wetness | 9% | Multi-day catchment wetness |
| Terrain susceptibility | 12% | How inherently flood/runoff-sensitive the area is |
| Hydrology | 8% | River-stage / flood forecasting support |
| Radar nowcast | 8% | Current weather support |
| Reservoir / dam context | 5% | Controlled storage and downstream caution |

The total is then adjusted by a small **agreement bonus** when multiple independent signals point in the same direction.

### Alert level thresholds

Current thresholds are:

| Level | Score |
| --- | --- |
| `Normal` | below 35 |
| `Watch` | 35 to 57.9 |
| `Alert` | 58 to 77.9 |
| `Severe - review required` | 78 and above |
| `Reviewed severe alert` | 78 and above, but manually approved |

### Important note about severity

The score is a **decision-support score**, not a probability.

A score of `58` does **not** mean “58% chance of flood.”  
It means the current combination of evidence crosses the dashboard’s `Alert` threshold.

## How District, Taluk, and Hotspot Logic Differ

### District logic
Districts combine:
- official warning products
- district nowcast
- official and satellite rainfall
- radar context
- river-stage and dam context
- terrain susceptibility

Districts are the most balanced layer.

### Taluk logic
Taluks use:
- localized rainfall estimates
- district background conditions
- hotspot membership/context
- local runoff susceptibility

Taluks are meant to be more local than districts, but still more cautious than hotspots.

### Hotspot logic
Hotspots are the most consequence-focused layer.

They combine:
- nearby rainfall
- nearby radar
- nearby station nowcast, when available
- catchment wetness
- hotspot category
- terrain and runoff susceptibility
- river or dam context where relevant

Hotspots are **not allowed** to escalate too easily from broad warning text alone.

For example:
- a generic district thunderstorm or wind warning should **not** create a hotspot `Watch` by itself
- hotspot `Watch` needs stronger support such as local rain, radar, hydrology, nearby official nowcast, or runoff readiness

This rule was tightened specifically to avoid false-positive hotspot alerts from broad statewide district warnings.

## How Source Freshness Works

The dashboard distinguishes between:
- **Data published**: when the source data itself appears to have been issued
- **Last checked by our system**: when the system last fetched or confirmed that source
- **This run**: whether the latest source refresh succeeded or left that source unavailable for this snapshot

### Source status meanings
- **Current / ok**
  - the source is within its expected freshness window
- **Degraded**
  - the source exists, but is weaker than normal for scoring purposes
- **Older than usual / stale**
  - the source is older than its normal freshness expectation
- **Unavailable / offline**
  - the source should not contribute to scoring

### How stale data is handled

Older data is not treated the same as fresh data.

Current rules:
- `ok` contributes fully
- `degraded` is down-weighted
- `stale` is heavily down-weighted
- `offline` and intentionally skipped sources do not contribute

This is important for transparency:
- each refresh workflow attempts a live fetch for the sources it owns
- if a source fails in that refresh, it drops out of scoring for that snapshot
- the next refresh can restore it as soon as the source succeeds again

## Important Rules Already Implemented

Some of the most important methodology safeguards are:

### CAP expiry is respected
CAP alerts only contribute while they are still within their official `expires` window.

This prevents yesterday’s warning from quietly continuing to push scores today.

### Same-day IMD district warning logic
The district warning page is treated like a **same-day bulletin**, not like a minute-precise feed.

That means:
- same-day district warnings can still be considered current
- yesterday’s district warning is treated as older/stale
- older than that is ignored

### Stale-source down-weighting
Signals from stale sources do not keep contributing at near-full strength.

This was added specifically to stop old hydrology or warning context from acting like fresh evidence.

### Manual review for highest severity
`Severe - review required` is not automatically treated the same as a published public severe alert.

The workflow is:
- the system can generate a severe candidate
- it stays pending review
- only after manual review can it become `Reviewed severe alert`

This is intentional and conservative.

## How The Dashboard Decides What To Show First

### Operational headline
The top headline is meant to summarize the current operational picture, not list every alert.

### Active alerts
The full alert list appears separately below the headline.

### District, taluk, and hotspot cards
Cards try to show the most local and most useful explanation first, rather than dumping a raw internal driver string.

Examples:
- district cards prefer district-average rain, radar, river, or official district context
- taluk cards prefer local rainfall and runoff context
- hotspot cards prefer hotspot-specific drivers such as nearby station nowcast, runoff context, radar, or local rainfall

## Maps And Visuals

The current Kerala map is intentionally simple:
- district boundaries
- hotspot overlays
- clickable evidence

At the moment:
- district map is the only map shown in the main map panel
- taluk and hotspot detail remain available through cards and evidence popups

This is a deliberate simplification after repeated reliability problems with the taluk map renderer.

## Refresh And Publication Workflow

The dashboard uses a multi-step workflow.

### Step 1: Data refresh
Different refresh workflows update different source groups:
- India fast sources
- India hydrology sources
- external sources

### Step 2: Publish dashboard snapshot
Once fresh source data is available, a publish workflow rebuilds the dashboard snapshot from the latest source cache.

### Step 3: GitHub Pages deployment
GitHub Pages then deploys the already-published snapshot.

This separation exists so that:
- source refreshes do not fight each other
- one workflow produces the final public snapshot
- the site is more internally consistent

## Telegram Operations And Review

Telegram is split into two different roles:

- **Private command chat**
  - used for operational controls and severe-alert review
  - commands include:
    - `/status`
    - `/pause_refresh`
    - `/resume_refresh`
    - `/pending_alerts`
    - `/approve <alert_id>`
    - `/reject <alert_id>`

- **Public alert group**
  - used only for reviewed severe alerts
  - normal command replies do not go here

### How the Telegram control path works now

The preferred control path is now:

1. Telegram sends a command to a Cloudflare Worker webhook
2. The Worker validates the command chat and webhook secret
3. The Worker either:
   - replies directly for simple read-only commands like `/status` and `/pending_alerts`, or
   - triggers a GitHub workflow for state-changing commands like pause, resume, approve, and reject
4. GitHub updates the repo state files
5. For alert-review commands, the normal publish workflow then picks up the change and, if approved, can send the severe alert to the public Telegram group

The older 5-minute polling workflow is kept only as a fallback path. If the Telegram bot is switched to webhook mode, polling now exits cleanly instead of failing.

### What pause means now

`/pause_refresh` now performs a **hard pause** for the three refresh workflows:

- `Refresh India fast sources (local)`
- `Refresh India hydrology (local)`
- `Refresh external sources`

They are disabled through the GitHub Actions API, which means:

- scheduled runs stop being created
- your local runner does not queue them overnight
- the dashboard should not get a newer refresh timestamp from a no-op run

`/resume_refresh` enables those same workflows again.

### Cloudflare Worker environment

The Worker should have these environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_COMMAND_CHAT_ID`
- `TELEGRAM_WEBHOOK_SECRET`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_REF`
  - usually `main`
- `GITHUB_DISPATCH_TOKEN`

`GITHUB_DISPATCH_TOKEN` should be a GitHub token that is allowed to trigger repository dispatch events for this repo.

### GitHub secrets used by the repo

- `TELEGRAM_COMMAND_CHAT_ID`
  - private command/review chat
- `TELEGRAM_CHAT_ID`
  - public alert group chat
- `TELEGRAM_BOT_TOKEN`
  - bot token used for both commands and alerts

### Telegram webhook setup

Once the Worker is deployed, the bot should be pointed to:

`https://<your-worker-domain>/telegram-webhook`

with the same `TELEGRAM_WEBHOOK_SECRET` set as Telegram’s webhook secret token.

This gives much faster command handling than the old poll-based approach and avoids waiting for the next 5-minute schedule.

## How Often Sources Are Checked

The system’s own check schedule is roughly:

### India fast sources
- about every 20 minutes

Includes:
- IMD CAP RSS
- IMD Flash Flood Bulletin
- IMD District Warning
- IMD District Nowcast
- IMD Station Nowcast
- Operator observations

### India hydrology sources
- about every 60 minutes

Includes:
- India-WRIS rainfall
- India-WRIS river level
- KSDMA reservoir status
- KSDMA dam management
- CWC FFS

### External sources
- about every 30 minutes

Includes:
- RainViewer radar
- NASA IMERG

Important: the source card should be read as “what data is being used now,” not “this source definitely published new data in this run.”

## Why Some Sources Are On Local Runner And Others Are Not

The system currently uses a mixed execution model because some official Indian sites do not behave reliably from standard GitHub-hosted runner networks.

In practice:
- several India government hydrology/warning sources perform better from the local self-hosted runner
- external sources like NASA IMERG and RainViewer are still handled on GitHub-hosted infrastructure

This is an operational reliability decision, not a methodological preference.

## What The Project Is Trying To Optimize For

The dashboard tries to balance:
- official warning integrity
- local consequence sensitivity
- freshness awareness
- transparency
- conservative escalation for the highest severity

That means it deliberately avoids some shortcuts:
- stale data should not quietly behave like fresh data
- broad warning text should not automatically become local hotspot `Watch`
- highest severity should not be auto-promoted without review

## Known Limitations

This project still has important limitations.

### 1. It is not an official warning authority
Always cross-check with official sources, especially for public action.

### 2. Source behavior changes
Government websites, PDFs, and embedded page data can change structure unexpectedly.

### 3. Mixed precision across sources
Some sources provide exact issue and validity times. Others only provide a date or a daily bulletin.

### 4. Curated hotspot coverage is selective
Hotspots cover known sensitive corridors, not every vulnerable place in Kerala.

### 5. Scores are heuristic
Even though they are structured and evidence-based, they are still human-designed heuristics and weights, not a trained probabilistic flood model.

## What Should Trigger A README Update

This file should be updated whenever any of the following changes:
- source list
- source role in scoring
- weights
- threshold levels
- manual review rules
- stale-data treatment
- CAP expiry handling
- district/taluk/hotspot logic
- map behavior
- publish workflow or operational architecture
- interpretation guidance for users

In short:

**If the meaning of the dashboard changes, this README should change too.**

## Feedback We Want

We especially welcome feedback from:
- IMD users
- hydrologists
- district disaster-management staff
- dam and river monitoring teams
- field volunteers who can compare dashboard outputs with local reality

Useful feedback looks like:
- “This alert is too aggressive for this kind of signal.”
- “This source should be weighted less.”
- “This district should rely more on river context than radar.”
- “This hotspot is missing.”
- “This official product should be treated as primary, not secondary.”
- “This explanation is too technical / not understandable.”

## For Reviewers And Contributors

If you want to review or improve the system, the most useful files are:
- [README.md](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\README.md)
- [config/sources.json](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\config\sources.json)
- [config/risk-thresholds.json](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\config\risk-thresholds.json)
- [scripts/lib/risk-model.js](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\scripts\lib\risk-model.js)
- [scripts/lib/pipeline.js](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\scripts\lib\pipeline.js)
- [src/shared/areas.js](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\src\shared\areas.js)
- [src/site/app.js](C:\Users\nisha\AndroidStudioProjects\Flashflood Alert\src\site\app.js)

## Local Build And Test

For local development:

```bash
npm run build
npm test
```

`npm run build` uses fixtures so the dashboard can render locally even without live source access.

## Final Operational Note

This dashboard is most useful when it is read the way it was designed:
- as a transparent evidence-combining monitor
- as an aid to prioritization
- as something that should invite verification and feedback

The goal is not to pretend uncertainty does not exist.  
The goal is to make the uncertainty, the evidence, and the operational judgment visible.
