import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { zipSync } from "fflate";
import thresholds from "../config/risk-thresholds.json" with { type: "json" };
import {
  parseImdCapRss,
  parseImdFlashFloodBulletin,
  parseKsdmaDamManagement,
  parseKsdmaReservoirs,
  parseCwcFfs,
  parseNasaImergNrt,
  parseRainviewerRadar,
  parseIndiaWrisRainfall,
  parseIndiaWrisRiverLevel
} from "../scripts/lib/parsers.js";
import {
  extractGeoTiffBuffer,
  parseImergTextListing,
  selectImergWindows
} from "../scripts/lib/imerg.js";
import { buildRainviewerPayload, parseRainviewerColorTable } from "../scripts/lib/rainviewer.js";
import { extractKsdmaIssuedAt } from "../scripts/lib/ksdma.js";
import {
  districtIdFromBoundaryName,
  parseDistrictBoundaries,
  parseTalukBoundaries,
  pointInGeometry,
  representativePointInGeometry,
  talukIdFromBoundaryNames
} from "../scripts/lib/boundaries.js";
import { summarizeRiverLevelSeries } from "../scripts/lib/indiawris.js";
import { buildHotspotFootprint } from "../src/shared/hotspot-footprints.js";
import { buildRiskOutputs } from "../scripts/lib/risk-model.js";
import { runPipeline } from "../scripts/lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

async function testParsers() {
  const capRaw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-rss.xml"), "utf8");
  const cap = await parseImdCapRss(capRaw);
  assert.equal(cap.item_count, 2);
  assert.ok(cap.kerala_district_ids.includes("idukki"));

  const capDetailRaw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-detail.xml"), "utf8");
  const capWithDetails = await parseImdCapRss(
    repoRoot,
    { active_window_hours: 48 },
    JSON.stringify({
      rss: `<?xml version="1.0" encoding="UTF-8"?><rss><channel><item><title>Localized alert</title><description>Regional alert</description><link>https://example.org/cap/imd-test-ernakulam</link></item></channel></rss>`,
      details: [
        {
          link: "https://example.org/cap/imd-test-ernakulam",
          xml: capDetailRaw
        }
      ]
    })
  );
  assert.ok(capWithDetails.kerala_district_ids.includes("ernakulam"));

  const capWithGeocodeOnly = await parseImdCapRss(
    repoRoot,
    { active_window_hours: 48 },
    JSON.stringify({
      rss: `<?xml version="1.0" encoding="UTF-8"?><rss><channel><item><title>Localized alert</title><description></description><link>https://example.org/cap/imd-test-geocode</link><pubDate>Mon, 16 Mar 2026 09:00:00 +0530</pubDate></item></channel></rss>`,
      details: [
        {
          link: "https://example.org/cap/imd-test-geocode",
          xml: `<?xml version="1.0" encoding="UTF-8"?>
          <cap:alert xmlns:cap="urn:oasis:names:tc:emergency:cap:1.2">
            <cap:identifier>imd-test-geocode</cap:identifier>
            <cap:sent>2026-03-16T09:00:00+05:30</cap:sent>
            <cap:info>
              <cap:category>Met</cap:category>
              <cap:severity>Severe</cap:severity>
              <cap:headline>മുന്നറിയിപ്പ്</cap:headline>
              <cap:area>
                <cap:areaDesc>4 districts of Kerala</cap:areaDesc>
                <cap:geocode>
                  <cap:valueName>LGD District Code</cap:valueName>
                  <cap:value>555</cap:value>
                </cap:geocode>
                <cap:geocode>
                  <cap:valueName>LGD District Code</cap:valueName>
                  <cap:value>560</cap:value>
                </cap:geocode>
              </cap:area>
            </cap:info>
          </cap:alert>`
        }
      ]
    })
  );
  assert.ok(capWithGeocodeOnly.kerala_district_ids.includes("ernakulam"));
  assert.ok(capWithGeocodeOnly.kerala_district_ids.includes("kottayam"));

  const filteredCap = await parseImdCapRss(
    `<?xml version="1.0" encoding="UTF-8"?><rss><channel>
      <item>
        <title>Fresh alert for Ernakulam</title>
        <description>Heavy rain expected</description>
        <pubDate>Tue, 18 Mar 2026 09:00:00 +0530</pubDate>
      </item>
      <item>
        <title>Old alert for Idukki</title>
        <description>Heavy rain expected</description>
        <pubDate>Fri, 21 Feb 2026 09:00:00 +0530</pubDate>
      </item>
    </channel></rss>`,
    { active_window_hours: 48 }
  );
  assert.equal(filteredCap.item_count, 1);
  assert.equal(filteredCap.filtered_item_count, 1);
  assert.ok(filteredCap.kerala_district_ids.includes("ernakulam"));
  assert.equal(filteredCap.kerala_district_ids.includes("idukki"), false);

  const bulletinRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-flash-flood-bulletin.html"),
    "utf8"
  );
  const bulletin = parseImdFlashFloodBulletin(bulletinRaw);
  assert.ok(bulletin.kerala_district_ids.includes("ernakulam"));

  const cwcRaw = await readFile(path.join(repoRoot, "fixtures", "cwc-ffs.json"), "utf8");
  const cwc = parseCwcFfs(cwcRaw);
  assert.equal(cwc.warning, true);
  assert.equal(cwc.station_count, 2);
  assert.equal(cwc.districts.length, 2);
  assert.equal(cwc.districts[0].above_danger_station_count, 1);
  assert.equal(cwc.forecast_warning_station_count, 1);
  assert.equal(cwc.forecast_danger_station_count, 1);
  assert.equal(cwc.districts[1].severity_basis, "threshold_forecast");

  const imergRaw = await readFile(path.join(repoRoot, "fixtures", "nasa-imerg-nrt.json"), "utf8");
  const imerg = parseNasaImergNrt(imergRaw);
  assert.equal(imerg.districts.length, 2);
  assert.equal(imerg.taluks.length, 2);
  assert.ok(imerg.source_files.half_hour[0].includes("30min"));

  const radarRaw = await readFile(path.join(repoRoot, "fixtures", "rainviewer-radar.json"), "utf8");
  const radar = parseRainviewerRadar(radarRaw);
  assert.equal(radar.districts.length, 2);
  assert.equal(radar.hotspots.length, 2);
  assert.ok(radar.frame_path.includes("/v2/radar/"));

  const ksdmaReservoirRaw = await readFile(path.join(repoRoot, "fixtures", "ksdma-reservoirs.json"), "utf8");
  const ksdmaReservoir = parseKsdmaReservoirs(ksdmaReservoirRaw);
  assert.equal(ksdmaReservoir.department, "kseb");
  assert.equal(ksdmaReservoir.districts.length, 2);
  assert.equal(ksdmaReservoir.alert_active, true);

  const ksdmaDamRaw = await readFile(path.join(repoRoot, "fixtures", "ksdma-dam-management.json"), "utf8");
  const ksdmaDam = parseKsdmaDamManagement(ksdmaDamRaw);
  assert.equal(ksdmaDam.department, "irrigation");
  assert.equal(ksdmaDam.districts.length, 3);
  assert.equal(ksdmaDam.release_preparedness, true);

  const indiaWrisRainfallRaw = await readFile(
    path.join(repoRoot, "fixtures", "indiawris-rainfall.json"),
    "utf8"
  );
  const indiaWrisRainfall = parseIndiaWrisRainfall(indiaWrisRainfallRaw);
  assert.equal(indiaWrisRainfall.districts.length, 2);
  assert.equal(indiaWrisRainfall.taluks.length, 2);
  assert.equal(indiaWrisRainfall.station_count, 5);
  assert.equal(indiaWrisRainfall.partial_failure_count, 0);

  const indiaWrisRiverLevelRaw = await readFile(
    path.join(repoRoot, "fixtures", "indiawris-river-level.json"),
    "utf8"
  );
  const indiaWrisRiverLevel = parseIndiaWrisRiverLevel(indiaWrisRiverLevelRaw);
  assert.equal(indiaWrisRiverLevel.districts.length, 2);
  assert.equal(indiaWrisRiverLevel.districts[0].max_rise_m, 0.62);
  assert.equal(indiaWrisRiverLevel.partial_failure_count, 0);
}

function testRiskModel() {
  const generatedAt = "2026-03-16T04:00:00.000Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-cap-rss", status: "ok" },
      { source_id: "imd-flash-flood-bulletin", status: "ok" },
      { source_id: "cwc-ffs", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" }
    ],
    capByDistrict: {
      idukki: { severity: 0.72, items: ["Orange warning"] }
    },
    bulletinByDistrict: {
      idukki: { severity: 0.45, notes: ["Flash flood bulletin references district"] }
    },
    reservoirByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Reservoir caution active"] }
    },
    damByDistrict: {},
    cwcByDistrict: {
      idukki: { active: true, severity: 0.4, notes: ["CWC watch"] }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 55,
        rain_3h_mm: 100,
        rain_6h_mm: 145,
        rain_24h_mm: 240,
        rain_3d_mm: 320,
        rain_7d_mm: 460
      }
    },
    taluks: [
      {
        taluk_id: "idukki--udumbanchola",
        district_id: "idukki",
        name: "Udumbanchola",
        hotspot_ids: ["h-peermade", "h-munnar-devikulam"]
      }
    ],
    approvals: [],
    hotspotOverrides: [],
    freshnessBySource: {
      "imd-cap-rss": 20,
      "imd-flash-flood-bulletin": 50,
      "cwc-ffs": 40
    },
    statusBySource: {
      "imd-cap-rss": "ok",
      "imd-flash-flood-bulletin": "ok",
      "cwc-ffs": "ok"
    }
  });

  const idukki = result.districtStates.find((district) => district.area_id === "idukki");
  assert.ok(idukki.score >= thresholds.thresholds.watch);
  assert.ok(idukki.runoff_potential);
  assert.equal(typeof idukki.runoff_potential.score, "number");
  assert.equal(result.talukStates.length, 1);
  assert.equal(result.talukStates[0].area_type, "taluk");
  assert.ok(result.talukStates[0].runoff_potential);
  assert.ok(result.alerts.every((alert) => alert.source_refs.length > 0));
}

function testHotspotWatchNeedsDynamicTrigger() {
  const generatedAt = "2026-03-25T04:00:00.000Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-cap-rss", status: "ok" },
      { source_id: "imd-flash-flood-bulletin", status: "ok" },
      { source_id: "cwc-ffs", status: "ok" },
      { source_id: "indiawris-rainfall", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    reservoirByDistrict: {
      pathanamthitta: { active: false, severity: 0.12, notes: ["KSEB high storage context"] }
    },
    damByDistrict: {
      pathanamthitta: { active: false, severity: 0.12, notes: ["Irrigation high storage context"] }
    },
    cwcByDistrict: {
      pathanamthitta: { active: false, severity: 0, notes: ["No river-stage warning for district"] }
    },
    radarByDistrict: {
      pathanamthitta: { severity: 0.25, intensity: "light", max_dbz: 18, notes: ["Light district radar echo"] }
    },
    radarByHotspot: {
      "h-pamba-corridor": {
        severity: 0.25,
        intensity: "light",
        max_dbz: 18,
        notes: ["Light hotspot radar echo"]
      }
    },
    rainfallByDistrict: {
      pathanamthitta: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 0.3,
        rain_3d_mm: 0.9,
        rain_7d_mm: 1.2,
        official_rain_24h_mm: 0.3,
        official_station_count: 4,
        official_peak_station_24h_mm: 2.4,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 0
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [],
    freshnessBySource: {
      "imd-cap-rss": 20,
      "imd-flash-flood-bulletin": 20,
      "cwc-ffs": 20,
      "indiawris-rainfall": 20,
      "rainviewer-radar": 20,
      "ksdma-reservoirs": 20,
      "ksdma-dam-management": 20
    },
    statusBySource: {
      "imd-cap-rss": "ok",
      "imd-flash-flood-bulletin": "ok",
      "cwc-ffs": "ok",
      "indiawris-rainfall": "ok",
      "rainviewer-radar": "ok",
      "ksdma-reservoirs": "ok",
      "ksdma-dam-management": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-pamba-corridor");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.ok(hotspot.runoff_potential);
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

async function testPipeline() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });
  const dashboardRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "dashboard.json"), "utf8");
  const dashboard = JSON.parse(dashboardRaw);
  const adminAreasRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "admin-areas.json"), "utf8");
  const adminAreas = JSON.parse(adminAreasRaw);
  const talukRiskRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "taluk-risk.json"), "utf8");
  const talukRisk = JSON.parse(talukRiskRaw);
  const radarNowcastRaw = await readFile(
    path.join(tempRoot, "docs", "data", "latest", "radar-nowcast.json"),
    "utf8"
  );
  const radarNowcast = JSON.parse(radarNowcastRaw);
  const observationGridRaw = await readFile(
    path.join(tempRoot, "docs", "data", "latest", "observation-grid.json"),
    "utf8"
  );
  const observationGrid = JSON.parse(observationGridRaw);
  const nasaHistoryRaw = await readFile(
    path.join(tempRoot, "docs", "data", "latest", "nasa-imerg-history.json"),
    "utf8"
  );
  const nasaHistory = JSON.parse(nasaHistoryRaw);
  const sourcesRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "sources.json"), "utf8");
  const sources = JSON.parse(sourcesRaw);
  assert.equal(dashboard.mode, "decision-support");
  assert.equal(adminAreas.boundaries.counts.district, 14);
  assert.ok(adminAreas.boundaries.counts.taluk >= 61);
  assert.ok(talukRisk.taluks.length >= 61);
  assert.equal(typeof observationGrid.observations.districts, "object");
  assert.equal(typeof observationGrid.observations.taluks, "object");
  assert.equal(Array.isArray(radarNowcast.districts), true);
  assert.equal(Array.isArray(radarNowcast.hotspots), true);
  assert.equal(
    observationGrid.source_metadata.nasa_imerg.latest_half_hour_file.includes("30min"),
    true
  );
  assert.equal(typeof observationGrid.source_metadata.rainviewer_radar.latest_frame_time, "string");
  assert.equal(observationGrid.source_metadata.indiawris_rainfall.station_count, 5);
  assert.equal(observationGrid.source_metadata.indiawris_river_level.district_count, 2);
  assert.equal(observationGrid.observations.taluks["idukki--peerumade"].peak_30m_mm, 25.9);
  assert.equal(observationGrid.observations.districts.idukki.official_rain_24h_mm, 5.7);
  assert.equal(nasaHistory.runs.length >= 1, true);
  assert.equal(nasaHistory.runs[0].latest_three_hour_file.includes("3hr"), true);
  assert.equal(
    sources.sources.find((source) => source.source_id === "imd-cap-rss")?.status,
    "ok"
  );
  assert.equal(
    sources.sources.find((source) => source.source_id === "imd-cap-rss")?.fetch_status,
    "ok"
  );
  assert.equal(
    sources.sources.find((source) => source.source_id === "imd-cap-rss")?.parser_status,
    "ok"
  );
  assert.equal(
    sources.sources.find((source) => source.source_id === "indiawris-rainfall")?.status,
    "ok"
  );
}

async function testPipelineDegradesPartialIndiaWrisCoverage() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-partial-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  const rainfallFixturePath = path.join(tempRoot, "fixtures", "indiawris-rainfall.json");
  const rainfallFixture = JSON.parse(await readFile(rainfallFixturePath, "utf8"));
  rainfallFixture.requested_district_count = 14;
  rainfallFixture.successful_district_count = 13;
  rainfallFixture.failed_districts = [
    {
      district_id: "kasaragod",
      district_name: "Kasaragod",
      status: 599,
      error: "fetch failed"
    }
  ];
  await writeFile(rainfallFixturePath, JSON.stringify(rainfallFixture, null, 2));

  await runPipeline(tempRoot, { useFixtures: true });
  const sourcesRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "sources.json"), "utf8");
  const sources = JSON.parse(sourcesRaw);
  const indiaWrisSource = sources.sources.find((source) => source.source_id === "indiawris-rainfall");

  assert.equal(indiaWrisSource?.status, "degraded");
  assert.equal(indiaWrisSource?.summary.failed_district_count, 1);
  assert.equal(indiaWrisSource?.summary.successful_district_count, 13);
}

async function testPipelineReusesSourcesWithinCadenceWindow() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-cache-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });
  await runPipeline(tempRoot, { useFixtures: false, enableCadenceReuse: true });

  const sourcesRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "sources.json"), "utf8");
  const latestRunRaw = await readFile(
    path.join(tempRoot, "runtime", "metrics", "latest-run.json"),
    "utf8"
  );
  const sources = JSON.parse(sourcesRaw);
  const latestRun = JSON.parse(latestRunRaw);

  const reusedSourceCount = sources.sources.filter((source) => source.reused_in_run === true).length;
  assert.equal(reusedSourceCount, sources.sources.length);
  assert.ok(
    sources.sources.every((source) =>
      String(source.notes ?? "").includes("Reused last successful fetch within")
    )
  );
  assert.equal(Array.isArray(latestRun.slowest_sources), true);
}

async function testPipelineFallsBackToLastSuccessfulPayloadOnFetchFailure() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-fallback-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });
  const sourcesConfigPath = path.join(tempRoot, "config", "sources.json");
  const sourcesConfig = JSON.parse(await readFile(sourcesConfigPath, "utf8"));
  const operatorConfig = sourcesConfig.find((source) => source.id === "operator-observations");
  operatorConfig.cadence_minutes = 0;
  await writeFile(sourcesConfigPath, JSON.stringify(sourcesConfig, null, 2));
  await rm(path.join(tempRoot, "data", "manual", "observations.json"));

  await runPipeline(tempRoot, { useFixtures: false, sourceIds: ["operator-observations"] });

  const sourcesRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "sources.json"), "utf8");
  const cacheRaw = await readFile(
    path.join(tempRoot, "runtime", "cache", "source-results.json"),
    "utf8"
  );
  const sources = JSON.parse(sourcesRaw);
  const cache = JSON.parse(cacheRaw);
  const operatorSource = sources.sources.find((source) => source.source_id === "operator-observations");
  const rainfallSource = sources.sources.find((source) => source.source_id === "indiawris-rainfall");

  assert.equal(operatorSource?.fetch_status, "failed_cached");
  assert.equal(operatorSource?.parser_status, "ok");
  assert.equal(operatorSource?.reused_in_run, true);
  assert.equal(operatorSource?.reuse_reason, "fetch_failure");
  assert.ok(String(operatorSource?.notes ?? "").includes("Reused last successful cached payload"));
  assert.equal(cache.sources["operator-observations"]?.snapshot.fetch_status, "ok");
  assert.equal(rainfallSource?.fetch_status, "skipped_cached");
  assert.equal(rainfallSource?.reused_in_run, true);
  assert.equal(rainfallSource?.reuse_reason, "source_selection");
}

async function testKsdmaIssuedAtExtractionPrefersCurrentLinkedDate() {
  const pageHtml = `
    <html>
      <body>
        <a href="https://sdma.kerala.gov.in/wp-content/uploads/2026/03/KSEB-SITE-20.pdf">Water Levels of Major Reservoirs (KSEB)</a> - 27/03/2026 11 AM
        <a href="https://sdma.kerala.gov.in/wp-content/uploads/2026/03/IRR-SITE-17.pdf">Water Levels of Major Reservoirs (IRRIGATION)</a> - 27/03/2026 11 AM
      </body>
    </html>
  `;
  assert.equal(
    extractKsdmaIssuedAt(
      pageHtml,
      "https://sdma.kerala.gov.in/wp-content/uploads/2026/03/KSEB-SITE-20.pdf"
    ),
    "2026-03-27T05:30:00.000Z"
  );
}

function testImergListingSelection() {
  const listing = [
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260316-S023000-E025959.0150.V07C.30min.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260316-S020000-E022959.0120.V07C.30min.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260316-S023000-E025959.0150.V07C.3hr.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260315-S233000-E025959.0150.V07C.3hr.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260316-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260315-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260314-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260313-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260312-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260311-S023000-E025959.0150.V07C.1day.tif",
    "/imerg/gis/early/3B-HHR-E.MS.MRG.3IMERG.20260310-S023000-E025959.0150.V07C.1day.tif"
  ].join("\n");

  const files = parseImergTextListing(listing);
  const selection = selectImergWindows(files);

  assert.equal(selection.halfHour.length, 2);
  assert.equal(selection.threeHourLatest.length, 1);
  assert.equal(selection.threeHourWindow.length, 2);
  assert.equal(selection.dailyWindow.length, 7);
  assert.ok(selection.dailyWindow.every((file) => file.slotCode === "0150"));
}

function testImergZipSelection() {
  const expectedAccumulation = Uint8Array.from([1, 2, 3, 4]);
  const archive = zipSync({
    "3B-HHR-E.MS.MRG.3IMERG.20260317-S233000-E235959.1410.V07C.1day.ice.tif": Uint8Array.from([
      8, 8, 8
    ]),
    "3B-HHR-E.MS.MRG.3IMERG.20260317-S233000-E235959.1410.V07C.1day.numPrecipHalfHour.tif":
      Uint8Array.from([9, 9, 9]),
    "3B-HHR-E.MS.MRG.3IMERG.20260317-S233000-E235959.1410.V07C.1day.tif": expectedAccumulation
  });

  const extracted = new Uint8Array(
    extractGeoTiffBuffer(
      archive.buffer.slice(archive.byteOffset, archive.byteOffset + archive.byteLength),
      "zip",
      "3B-HHR-E.MS.MRG.3IMERG.20260317-S233000-E235959.1410.V07C.1day.zip"
    )
  );

  assert.deepEqual(Array.from(extracted), Array.from(expectedAccumulation));
}

function testRainviewerHelpers() {
  const colorTable = parseRainviewerColorTable([
    "dBZ / RGBA,Black and White,Original,Universal Blue",
    "-32,#00000000,#00000000,#00000000",
    "10,#111111ff,#222222ff,#3366ccff",
    "40,#aaaaaaaa,#bbbbbbbb,#ff6600ff"
  ].join("\n"));

  assert.equal(colorTable.byColor["51,102,204,255"], 10);

  const payload = buildRainviewerPayload({
    metadata: {
      generated: 1773815132,
      host: "https://tilecache.rainviewer.com",
      radar: { past: [{ time: 1773814800, path: "/v2/radar/1773814800" }] }
    },
    districtResults: [
      {
        district_id: "idukki",
        name: "Idukki",
        location: { lat: 9.84, lon: 76.97 },
        max_dbz: 38,
        intensity: "heavy",
        severity: 0.75,
        detected: true
      }
    ],
    hotspotResults: [
      {
        hotspot_id: "h-peermade",
        district_id: "idukki",
        name: "Peermade high-range catchment",
        location: { lat: 9.574, lon: 76.967 },
        max_dbz: 42,
        intensity: "heavy",
        severity: 0.75,
        detected: true
      }
    ]
  });

  assert.equal(payload.districts.find((district) => district.district_id === "idukki").intensity, "heavy");
  assert.equal(payload.hotspots[0].hotspot_id, "h-peermade");
}

function testBoundaryHelpers() {
  assert.equal(districtIdFromBoundaryName("Thiruvananthapuram"), "thiruvananthapuram");
  assert.equal(districtIdFromBoundaryName("Thiruvanthapuram"), "thiruvananthapuram");
  assert.equal(districtIdFromBoundaryName("Pathanamthitta"), "pathanamthitta");
  assert.equal(
    talukIdFromBoundaryNames("Thiruvanthapuram", "Neyyattinkara"),
    "thiruvananthapuram--neyyattinkara"
  );
  assert.equal(
    pointInGeometry(
      [76.5, 9.5],
      {
        type: "Polygon",
        coordinates: [
          [
            [76.0, 9.0],
            [77.0, 9.0],
            [77.0, 10.0],
            [76.0, 10.0],
            [76.0, 9.0]
          ]
        ]
      }
    ),
    true
  );
  const representativePoint = representativePointInGeometry({
    type: "Polygon",
    coordinates: [
      [
        [76.0, 9.0],
        [77.0, 9.0],
        [77.0, 10.0],
        [76.0, 10.0],
        [76.0, 9.0]
      ]
    ]
  });
  assert.equal(pointInGeometry([representativePoint.lon, representativePoint.lat], {
    type: "Polygon",
    coordinates: [
      [
        [76.0, 9.0],
        [77.0, 9.0],
        [77.0, 10.0],
        [76.0, 10.0],
        [76.0, 9.0]
      ]
    ]
  }), true);
}

async function testIndiaWrisStationRegistry() {
  const registryRaw = await readFile(
    path.join(repoRoot, "data", "manual", "indiawris-stations.json"),
    "utf8"
  );
  const registry = JSON.parse(registryRaw);
  const districtLayer = JSON.parse(
    await readFile(path.join(repoRoot, "src", "site", "assets", "kerala-districts.geojson"), "utf8")
  );
  const talukLayer = JSON.parse(
    await readFile(path.join(repoRoot, "src", "site", "assets", "kerala-taluks.geojson"), "utf8")
  );
  const districtBoundaries = parseDistrictBoundaries(districtLayer);
  const talukBoundaries = parseTalukBoundaries(talukLayer);

  const vandiperiyar = registry.stations.find((station) => station.station_code === "016-SWRDKOCHI");
  assert.ok(vandiperiyar);

  const districtMatch = districtBoundaries.find((entry) =>
    pointInGeometry([vandiperiyar.lon, vandiperiyar.lat], entry.geometry)
  );
  const talukMatch = talukBoundaries.find((entry) =>
    pointInGeometry([vandiperiyar.lon, vandiperiyar.lat], entry.geometry)
  );

  assert.equal(districtMatch?.district_id, "idukki");
  assert.equal(talukMatch?.taluk_id, "idukki--peerumade");
}

function testIndiaWrisRiverThresholdSeverity() {
  const summary = summarizeRiverLevelSeries(
    [
      {
        stationName: "NEELEESWARAM",
        dataValue: "9.20",
        dataTime: "2026-03-18T06:00:00",
        district_id: "ernakulam"
      },
      {
        stationName: "NEELEESWARAM",
        dataValue: "10.10",
        dataTime: "2026-03-18T08:00:00",
        district_id: "ernakulam"
      }
    ],
    {
      registry: {
        byCode: new Map(),
        byName: new Map()
      },
      thresholds: {
        byCode: new Map(),
        byName: new Map([
          [
            "NEELEESWARAM",
            {
              station_name: "NEELEESWARAM",
              warning_level_m: 9,
              danger_level_m: 10,
              highest_flood_level_m: 12.4,
              confidence: "confirmed"
            }
          ]
        ])
      }
    }
  );

  assert.equal(summary.above_danger_station_count, 1);
  assert.equal(summary.severity_basis, "threshold");
  assert.equal(summary.severity, 1);
  assert.equal(summary.stations[0].level_status, "above_danger");
}

function testHotspotFootprints() {
  const footprint = buildHotspotFootprint(
    {
      id: "h-demo",
      district_id: "idukki",
      name: "Demo hotspot",
      category: "steep_catchment",
      location: { lat: 10, lon: 76.9 },
      buffer_km: 10
    },
    0.9
  );

  assert.equal(footprint.geometry.type, "Polygon");
  assert.ok(footprint.geometry.coordinates[0].length > 20);
  assert.equal(footprint.properties.category, "steep_catchment");
}

const tests = [
  ["parsers", testParsers],
  ["imerg-listing", testImergListingSelection],
  ["imerg-zip-selection", testImergZipSelection],
  ["rainviewer-helpers", testRainviewerHelpers],
  ["boundaries", testBoundaryHelpers],
  ["indiawris-registry", testIndiaWrisStationRegistry],
  ["indiawris-thresholds", testIndiaWrisRiverThresholdSeverity],
  ["hotspot-footprints", testHotspotFootprints],
  ["risk-model", testRiskModel],
  ["risk-model-hotspot-gating", testHotspotWatchNeedsDynamicTrigger],
  ["pipeline", testPipeline],
  ["pipeline-partial-indiawris", testPipelineDegradesPartialIndiaWrisCoverage],
  ["pipeline-cadence-reuse", testPipelineReusesSourcesWithinCadenceWindow],
  ["pipeline-fallback-cache", testPipelineFallsBackToLastSuccessfulPayloadOnFetchFailure],
  ["ksdma-issued-at", testKsdmaIssuedAtExtractionPrefersCurrentLinkedDate]
];

let failures = 0;

for (const [name, testFn] of tests) {
  try {
    await testFn();
    console.log(`PASS ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL ${name}`);
    console.error(error);
  }
}

if (failures > 0) {
  process.exit(1);
}
