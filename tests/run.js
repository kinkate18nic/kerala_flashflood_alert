import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { zipSync } from "fflate";
import thresholds from "../config/risk-thresholds.json" with { type: "json" };
import {
  parseImdCapRss,
  parseImdFlashFloodBulletin,
  parseImdDistrictWarning,
  parseImdDistrictNowcast,
  parseImdStationNowcast,
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
import { writeStableGeneratedJson } from "../scripts/lib/fs.js";
import { buildHotspotFootprint } from "../src/shared/hotspot-footprints.js";
import { buildRiskOutputs } from "../scripts/lib/risk-model.js";
import { runPipeline, statusFromFreshness } from "../scripts/lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

async function listFilesRecursively(rootDir) {
  const entries = await readdir(rootDir, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const entryPath = path.join(rootDir, entry.name);
      if (entry.isDirectory()) {
        return listFilesRecursively(entryPath);
      }
      return [entryPath];
    })
  );
  return nested.flat();
}

async function testTrackedOutputsHaveNoMergeMarkersOrBrokenJson() {
  const targets = [
    "README.md",
    path.join("src", "site", "app.js"),
    path.join("src", "site", "methodology.html"),
    path.join("docs", "app.js"),
    path.join("docs", "methodology.html"),
    path.join("docs", "data", "archive", "index.json"),
    path.join("docs", "data", "latest"),
    path.join("docs", "data", "static"),
    path.join("runtime", "metrics", "latest-run.json"),
    path.join("runtime", "metrics", "nasa-imerg-history.json"),
    path.join("scripts", "lib", "pipeline.js"),
    path.join("tests", "run.js")
  ];

  const files = (
    await Promise.all(
      targets.map(async (target) => {
        const absolutePath = path.join(repoRoot, target);
        if (path.extname(absolutePath)) {
          return [absolutePath];
        }
        return listFilesRecursively(absolutePath);
      })
    )
  ).flat();

  for (const filePath of files) {
    const contents = await readFile(filePath, "utf8");
    assert.equal(
      /^(<<<<<<<|=======|>>>>>>>) /m.test(contents) || /^(<<<<<<<|=======|>>>>>>>)$/m.test(contents),
      false,
      `merge marker found in ${path.relative(repoRoot, filePath)}`
    );

    if (path.extname(filePath) === ".json") {
      assert.doesNotThrow(
        () => JSON.parse(contents),
        `invalid JSON in ${path.relative(repoRoot, filePath)}`
      );
    }
  }
}

async function testParsers() {
  const capRaw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-rss.xml"), "utf8");
  const cap = await parseImdCapRss(capRaw);
  assert.equal(cap.item_count, 0);
  assert.equal(cap.raw_item_count, 2);
  assert.equal(cap.kerala_district_ids.length, 0);

  const capDetailRaw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-detail.xml"), "utf8");
  const capWithDetails = await parseImdCapRss(
    repoRoot,
    { reference_time: "2026-03-16T09:30:00+05:30" },
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
    { reference_time: "2026-03-16T09:30:00+05:30" },
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
              <cap:expires>2026-03-16T12:00:00+05:30</cap:expires>
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
    repoRoot,
    { reference_time: "2026-03-30T17:16:00+05:30" },
    JSON.stringify({
      rss: `<?xml version="1.0" encoding="UTF-8"?><rss><channel>
        <item>
          <title>Fresh alert for Ernakulam</title>
          <description>Heavy rain expected</description>
          <link>https://example.org/cap/fresh-ernakulam</link>
          <pubDate>Mon, 30 Mar 2026 16:51:53 +0530</pubDate>
        </item>
        <item>
          <title>Expired alert for Pathanamthitta</title>
          <description>Heavy rain expected</description>
          <link>https://example.org/cap/expired-pathanamthitta</link>
          <pubDate>Sun, 29 Mar 2026 17:14:55 +0530</pubDate>
        </item>
      </channel></rss>`,
      details: [
        {
          link: "https://example.org/cap/fresh-ernakulam",
          xml: `<?xml version="1.0" encoding="UTF-8"?>
          <cap:alert xmlns:cap="urn:oasis:names:tc:emergency:cap:1.2">
            <cap:identifier>fresh-ernakulam</cap:identifier>
            <cap:sent>2026-03-30T16:51:53+05:30</cap:sent>
            <cap:info>
              <cap:category>Met</cap:category>
              <cap:severity>Moderate</cap:severity>
              <cap:expires>2026-03-30T19:31:00+05:30</cap:expires>
              <cap:headline>Fresh alert for Ernakulam</cap:headline>
              <cap:area>
                <cap:areaDesc>Ernakulam district of Kerala</cap:areaDesc>
              </cap:area>
            </cap:info>
          </cap:alert>`
        },
        {
          link: "https://example.org/cap/expired-pathanamthitta",
          xml: `<?xml version="1.0" encoding="UTF-8"?>
          <cap:alert xmlns:cap="urn:oasis:names:tc:emergency:cap:1.2">
            <cap:identifier>expired-pathanamthitta</cap:identifier>
            <cap:sent>2026-03-29T17:14:55+05:30</cap:sent>
            <cap:info>
              <cap:category>Met</cap:category>
              <cap:severity>Moderate</cap:severity>
              <cap:expires>2026-03-29T20:00:00+05:30</cap:expires>
              <cap:headline>Expired alert for Pathanamthitta</cap:headline>
              <cap:area>
                <cap:areaDesc>Pathanamthitta district of Kerala</cap:areaDesc>
              </cap:area>
            </cap:info>
          </cap:alert>`
        }
      ]
    })
  );
  assert.equal(filteredCap.item_count, 1);
  assert.equal(filteredCap.filtered_item_count, 1);
  assert.ok(filteredCap.kerala_district_ids.includes("ernakulam"));
  assert.equal(filteredCap.kerala_district_ids.includes("pathanamthitta"), false);

  const bulletinRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-flash-flood-bulletin.html"),
    "utf8"
  );
  const bulletin = parseImdFlashFloodBulletin(bulletinRaw);
  assert.ok(bulletin.kerala_district_ids.includes("ernakulam"));

  const districtWarningRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-district-warning.html"),
    "utf8"
  );
  const districtWarning = parseImdDistrictWarning(districtWarningRaw);
  assert.equal(districtWarning.active_district_count, 1);
  assert.ok(districtWarning.districts.some((district) => district.district_id === "pathanamthitta"));
  assert.equal(
    districtWarning.districts.find((district) => district.district_id === "ernakulam")?.severity,
    0
  );

  const districtNowcastRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-district-nowcast.html"),
    "utf8"
  );
  const districtNowcast = parseImdDistrictNowcast(districtNowcastRaw, {
    reference_time: "2026-03-31T15:00:00+05:30"
  });
  assert.equal(districtNowcast.active_district_count, 1);
  assert.equal(districtNowcast.valid_until, "2026-03-31T10:30:00.000Z");
  assert.ok(districtNowcast.districts.some((district) => district.district_id === "pathanamthitta"));
  const expiredDistrictNowcast = parseImdDistrictNowcast(districtNowcastRaw, {
    reference_time: "2026-03-31T16:30:00+05:30"
  });
  assert.equal(expiredDistrictNowcast.active_district_count, 0);

  const stationNowcastRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-station-nowcast.html"),
    "utf8"
  );
  const stationNowcast = parseImdStationNowcast(stationNowcastRaw, {
    reference_time: "2026-04-01T15:00:00+05:30"
  });
  assert.equal(stationNowcast.active_station_count, 1);
  assert.equal(stationNowcast.valid_until, "2026-04-01T10:30:00.000Z");
  assert.ok(stationNowcast.hotspots.some((entry) => entry.hotspot_id === "h-munnar-devikulam"));
  const expiredStationNowcast = parseImdStationNowcast(stationNowcastRaw, {
    reference_time: "2026-04-01T16:30:00+05:30"
  });
  assert.equal(expiredStationNowcast.active_station_count, 0);

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

function testRiskModelDownweightsStaleSignals() {
  const baseContext = {
    generatedAt: "2026-03-30T12:00:00.000Z",
    thresholds,
    taluks: [],
    approvals: [],
    hotspotOverrides: [],
    bulletinByDistrict: {},
    rainfallByDistrict: {},
    rainfallByTaluk: {},
    capByDistrict: {
      pathanamthitta: { severity: 0.72, items: ["Orange warning"], source_ids: ["imd-cap-rss"] }
    },
    reservoirByDistrict: {
      pathanamthitta: {
        active: true,
        severity: 0.35,
        notes: ["Reservoir caution active"],
        source_ids: ["ksdma-reservoirs"]
      }
    },
    damByDistrict: {},
    cwcByDistrict: {
      pathanamthitta: {
        active: true,
        severity: 0.4,
        notes: ["CWC watch"],
        source_ids: ["cwc-ffs"]
      }
    },
    radarByDistrict: {
      pathanamthitta: {
        severity: 0.65,
        intensity: "moderate",
        max_dbz: 31,
        notes: ["RainViewer moderate cell near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    freshnessBySource: {
      "imd-cap-rss": 20,
      "rainviewer-radar": 20,
      "cwc-ffs": 20,
      "ksdma-reservoirs": 20
    }
  };

  const healthy = buildRiskOutputs({
    ...baseContext,
    sourceSnapshots: [
      { source_id: "imd-cap-rss", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "cwc-ffs", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" }
    ],
    statusBySource: {
      "imd-cap-rss": "ok",
      "rainviewer-radar": "ok",
      "cwc-ffs": "ok",
      "ksdma-reservoirs": "ok"
    }
  });

  const stale = buildRiskOutputs({
    ...baseContext,
    sourceSnapshots: [
      { source_id: "imd-cap-rss", status: "stale" },
      { source_id: "rainviewer-radar", status: "stale" },
      { source_id: "cwc-ffs", status: "stale" },
      { source_id: "ksdma-reservoirs", status: "stale" }
    ],
    statusBySource: {
      "imd-cap-rss": "stale",
      "rainviewer-radar": "stale",
      "cwc-ffs": "stale",
      "ksdma-reservoirs": "stale"
    }
  });

  const healthyDistrict = healthy.districtStates.find((district) => district.area_id === "pathanamthitta");
  const staleDistrict = stale.districtStates.find((district) => district.area_id === "pathanamthitta");

  assert.ok(healthyDistrict.score >= thresholds.thresholds.watch);
  assert.ok(staleDistrict.score < thresholds.thresholds.watch);
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

function testDistrictWarningAloneDoesNotPromoteHotspotWatch() {
  const generatedAt = "2026-03-31T11:02:39.908Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "nasa-imerg-nrt", status: "ok" },
      { source_id: "indiawris-rainfall", status: "stale" },
      { source_id: "indiawris-river-level", status: "ok" },
      { source_id: "cwc-ffs", status: "offline" },
      { source_id: "ksdma-reservoirs", status: "stale" },
      { source_id: "ksdma-dam-management", status: "stale" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.22,
        hazards: ["Thunderstorm & Lightning", "Squall etc", "Strong Surface Winds"],
        notes: ["IDUKKI : Thunderstorm & Lightning, Squall etc Strong Surface Winds Updated on:2026-03-31"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {
      idukki: {
        severity: 0.22,
        notes: ["IDUKKI Light rain:"],
        source_ids: ["imd-district-nowcast"]
      }
    },
    reservoirByDistrict: {
      idukki: {
        active: true,
        severity: 0.18,
        notes: ["KSEB high storage context"],
        source_ids: ["ksdma-reservoirs"]
      }
    },
    damByDistrict: {
      idukki: {
        active: true,
        severity: 0.18,
        notes: ["Irrigation high storage context"],
        source_ids: ["ksdma-dam-management"]
      }
    },
    cwcByDistrict: {
      idukki: {
        active: true,
        severity: 0.08,
        notes: ["CWC flood forecasting observed river rise 0.03 m across 4 stations"],
        source_ids: ["cwc-ffs", "indiawris-river-level"]
      }
    },
    radarByDistrict: {
      idukki: {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-munnar-devikulam": {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near hotspot"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 1.6,
        rain_3d_mm: 26.8,
        rain_7d_mm: 26.8,
        source: "nasa-imerg-pps+indiawris-cwc",
        source_ids: ["nasa-imerg-nrt", "indiawris-rainfall"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["indiawris-rainfall"],
        antecedent_source_ids: ["indiawris-rainfall"],
        official_rain_24h_mm: 1.6,
        official_rain_3d_mm: 26.8,
        official_rain_7d_mm: 26.8,
        official_station_count: 4,
        official_peak_station_24h_mm: 5.2,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 0
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [],
    freshnessBySource: {
      "imd-district-warning": 993,
      "imd-district-nowcast": 33,
      "rainviewer-radar": 3,
      "nasa-imerg-nrt": 303,
      "indiawris-rainfall": 1533,
      "indiawris-river-level": 1053,
      "cwc-ffs": 1653,
      "ksdma-reservoirs": 333,
      "ksdma-dam-management": 333
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "imd-district-nowcast": "ok",
      "rainviewer-radar": "ok",
      "nasa-imerg-nrt": "ok",
      "indiawris-rainfall": "stale",
      "indiawris-river-level": "ok",
      "cwc-ffs": "offline",
      "ksdma-reservoirs": "stale",
      "ksdma-dam-management": "stale"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-munnar-devikulam");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.ok(hotspot.score < thresholds.thresholds.watch);
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testModerateStormContextDoesNotPromoteFlashFloodHotspotWatch() {
  const generatedAt = "2026-04-29T12:12:04.503Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "indiawris-rainfall", status: "ok" },
      { source_id: "indiawris-river-level", status: "ok" },
      { source_id: "cwc-ffs", status: "stale" },
      { source_id: "ksdma-reservoirs", status: "stale" },
      { source_id: "ksdma-dam-management", status: "stale" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.22,
        hazards: ["Thunderstorm & Lightning", "Squall etc", "Strong Surface Winds"],
        notes: ["IDUKKI : Thunderstorm & Lightning, Squall etc Strong Surface Winds Updated on:2026-04-29"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {
      idukki: {
        severity: 0.35,
        notes: ["IDUKKI Light Thunderstorms with moderate rain: 5-15 mm/hr"],
        source_ids: ["imd-district-nowcast"]
      }
    },
    stationNowcastByHotspot: {
      "h-munnar-devikulam": {
        severity: 0.35,
        station_name: "Munnar",
        distance_km: 0.1,
        notes: ["Munnar: Light Thunderstorms with moderate rain: 5-15 mm/hr"],
        source_ids: ["imd-station-nowcast"]
      }
    },
    reservoirByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Reservoir caution active"], source_ids: ["ksdma-reservoirs"] }
    },
    damByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Dam downstream caution active"], source_ids: ["ksdma-dam-management"] }
    },
    cwcByDistrict: {
      idukki: {
        active: true,
        severity: 0.22,
        notes: ["CWC flood forecasting observed river rise 1.08 m across 4 stations"],
        source_ids: ["cwc-ffs"]
      }
    },
    radarByDistrict: {
      idukki: {
        severity: 0.5,
        intensity: "moderate",
        max_dbz: 23,
        notes: ["RainViewer moderate cell near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-munnar-devikulam": {
        severity: 0.5,
        intensity: "moderate",
        max_dbz: 23,
        notes: ["RainViewer moderate cell near hotspot"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0.6,
        rain_3h_mm: 1.6,
        rain_6h_mm: 2.8,
        rain_24h_mm: 6.5,
        rain_3d_mm: 6.5,
        rain_7d_mm: 6.5,
        official_rain_24h_mm: 6.5,
        official_station_count: 3,
        official_peak_station_24h_mm: 19.4,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 2.6,
        source_ids: ["nasa-imerg-nrt", "indiawris-rainfall"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["indiawris-rainfall"],
        antecedent_source_ids: ["indiawris-rainfall"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-munnar-devikulam", score_boost: 6 }],
    freshnessBySource: {
      "imd-district-warning": 1062,
      "imd-district-nowcast": 102,
      "imd-station-nowcast": 102,
      "rainviewer-radar": 2,
      "indiawris-rainfall": 552,
      "indiawris-river-level": 582,
      "cwc-ffs": 282,
      "ksdma-reservoirs": 402,
      "ksdma-dam-management": 402
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "imd-district-nowcast": "ok",
      "imd-station-nowcast": "ok",
      "rainviewer-radar": "ok",
      "indiawris-rainfall": "ok",
      "indiawris-river-level": "ok",
      "cwc-ffs": "stale",
      "ksdma-reservoirs": "stale",
      "ksdma-dam-management": "stale"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-munnar-devikulam");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.equal(hotspot.score, thresholds.thresholds.watch - 0.1);
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testShortDurationRainCanPromoteFlashFloodHotspotWatch() {
  const generatedAt = "2026-04-29T12:12:04.503Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "indiawris-rainfall", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    reservoirByDistrict: {},
    damByDistrict: {},
    cwcByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.22,
        hazards: ["Thunderstorm & Lightning"],
        notes: ["IDUKKI : Thunderstorm & Lightning Updated on:2026-04-29"],
        source_ids: ["imd-district-warning"]
      }
    },
    radarByDistrict: {
      idukki: {
        severity: 0.25,
        intensity: "light",
        max_dbz: 18,
        notes: ["Light district radar echo"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-munnar-devikulam": {
        severity: 0.25,
        intensity: "light",
        max_dbz: 18,
        notes: ["Light hotspot radar echo"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 16,
        rain_3h_mm: 18,
        rain_6h_mm: 20,
        rain_24h_mm: 22,
        rain_3d_mm: 22,
        rain_7d_mm: 22,
        official_rain_24h_mm: 22,
        official_station_count: 3,
        official_peak_station_24h_mm: 22,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 7,
        source_ids: ["nasa-imerg-nrt", "indiawris-rainfall"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["indiawris-rainfall"],
        antecedent_source_ids: ["indiawris-rainfall"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [],
    freshnessBySource: {
      "imd-district-warning": 60,
      "rainviewer-radar": 2,
      "indiawris-rainfall": 60
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "rainviewer-radar": "ok",
      "indiawris-rainfall": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-munnar-devikulam");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Watch");
  assert.ok(hotspot.score >= thresholds.thresholds.watch);
  assert.ok(
    hotspot.drivers.every((driver) =>
      !driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testWeakRiseOnlyHydrologyDoesNotPromoteDamDownstreamWatch() {
  const generatedAt = "2026-05-07T09:18:34.371Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "nasa-imerg-nrt", status: "ok" },
      { source_id: "cwc-ffs", status: "stale" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.35,
        hazards: ["Heavy Rain", "Thunderstorm & Lightning", "Squall etc", "Strong Surface Winds"],
        notes: ["IDUKKI : Heavy Rain Thunderstorm & Lightning, Squall etc Strong Surface Winds Updated on:2026-05-07"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {
      idukki: {
        severity: 0.22,
        notes: ["IDUKKI Light rain:"],
        source_ids: ["imd-district-nowcast"]
      }
    },
    stationNowcastByHotspot: {
      "h-vandiperiyar-mullaperiyar": {
        severity: 0.28,
        station_name: "Thekkady",
        distance_km: 3.1,
        notes: ["Thekkady: Light rain: less than 5 mm/hr"],
        source_ids: ["imd-station-nowcast"]
      }
    },
    reservoirByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Reservoir caution active"], source_ids: ["ksdma-reservoirs"] }
    },
    damByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Dam downstream caution active"], source_ids: ["ksdma-dam-management"] }
    },
    cwcByDistrict: {
      idukki: {
        active: true,
        severity: 0.4,
        above_warning_station_count: 0,
        above_danger_station_count: 0,
        forecast_warning_station_count: 0,
        forecast_danger_station_count: 0,
        notes: ["CWC flood forecasting observed river rise 1.08 m across 4 stations"],
        source_ids: ["cwc-ffs"]
      }
    },
    radarByDistrict: {
      idukki: {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-vandiperiyar-mullaperiyar": {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near hotspot"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0,
        rain_3h_mm: 0.3,
        rain_6h_mm: 0.6,
        rain_24h_mm: 9.5,
        rain_3d_mm: 48,
        rain_7d_mm: 95,
        official_rain_24h_mm: 9.5,
        official_station_count: 0,
        official_peak_station_24h_mm: 0,
        spatial_aggregation: "district_polygon_mean",
        peak_30m_mm: 0,
        source_ids: ["nasa-imerg-nrt"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["nasa-imerg-nrt"],
        antecedent_source_ids: ["nasa-imerg-nrt"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-vandiperiyar-mullaperiyar", score_boost: 12 }],
    freshnessBySource: {
      "imd-district-warning": 889,
      "imd-district-nowcast": 109,
      "imd-station-nowcast": 109,
      "rainviewer-radar": 19,
      "nasa-imerg-nrt": 259,
      "cwc-ffs": 409,
      "ksdma-reservoirs": 229,
      "ksdma-dam-management": 229
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "imd-district-nowcast": "ok",
      "imd-station-nowcast": "ok",
      "rainviewer-radar": "ok",
      "nasa-imerg-nrt": "ok",
      "cwc-ffs": "stale",
      "ksdma-reservoirs": "ok",
      "ksdma-dam-management": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-vandiperiyar-mullaperiyar");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testStaleThresholdHydrologyDoesNotPromoteDamDownstreamWatch() {
  const generatedAt = "2026-05-26T08:44:23.709Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "nasa-imerg-nrt", status: "ok" },
      { source_id: "cwc-ffs", status: "stale" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.22,
        hazards: ["Heavy Rain"],
        notes: ["IDUKKI : Heavy Rain Updated on:2026-05-26"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {
      idukki: {
        severity: 0.35,
        notes: ["IDUKKI Moderate rain likely"],
        source_ids: ["imd-district-nowcast"]
      }
    },
    stationNowcastByHotspot: {
      "h-vandiperiyar-mullaperiyar": {
        severity: 0.35,
        station_name: "Kumily",
        distance_km: 1.9,
        notes: ["Kumily: Moderate rain: 5-15 mm/hr"],
        source_ids: ["imd-station-nowcast"]
      }
    },
    reservoirByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Reservoir caution active"], source_ids: ["ksdma-reservoirs"] }
    },
    damByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Dam downstream caution active"], source_ids: ["ksdma-dam-management"] }
    },
    cwcByDistrict: {
      idukki: {
        active: true,
        severity: 0.8,
        watch: true,
        above_warning_station_count: 1,
        above_danger_station_count: 0,
        forecast_warning_station_count: 0,
        forecast_danger_station_count: 0,
        notes: ["CWC flood forecasting watch active at 1 station"],
        source_ids: ["cwc-ffs"]
      }
    },
    radarByDistrict: {
      idukki: {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-vandiperiyar-mullaperiyar": {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near hotspot"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 21.4,
        rain_3d_mm: 42,
        rain_7d_mm: 68,
        official_rain_24h_mm: 21.4,
        official_station_count: 3,
        official_peak_station_24h_mm: 56.8,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 0,
        source_ids: ["nasa-imerg-nrt"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["nasa-imerg-nrt"],
        antecedent_source_ids: ["nasa-imerg-nrt"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-vandiperiyar-mullaperiyar", score_boost: 12 }],
    freshnessBySource: {
      "imd-district-warning": 134,
      "imd-district-nowcast": 74,
      "imd-station-nowcast": 74,
      "rainviewer-radar": 14,
      "nasa-imerg-nrt": 284,
      "cwc-ffs": 374,
      "ksdma-reservoirs": 194,
      "ksdma-dam-management": 194
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "imd-district-nowcast": "ok",
      "imd-station-nowcast": "ok",
      "rainviewer-radar": "ok",
      "nasa-imerg-nrt": "ok",
      "cwc-ffs": "stale",
      "ksdma-reservoirs": "ok",
      "ksdma-dam-management": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-vandiperiyar-mullaperiyar");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testDistrictNowcastContextDoesNotCountAsLocalDamDownstreamTrigger() {
  const generatedAt = "2026-05-26T08:44:23.709Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "nasa-imerg-nrt", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      ernakulam: {
        severity: 0.35,
        hazards: ["Heavy Rain", "Thunderstorm & Lightning"],
        notes: ["ERNAKULAM : Heavy Rain Thunderstorm & Lightning Updated on:2026-05-26"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {
      ernakulam: {
        severity: 0.55,
        notes: ["ERNAKULAM Moderate rain likely in some parts"],
        source_ids: ["imd-district-nowcast"]
      }
    },
    stationNowcastByHotspot: {
      "h-aluva-periyar": {
        severity: 0.25,
        station_name: "Aluva",
        distance_km: 1.4,
        notes: ["Aluva: Light rain: less than 5 mm/hr"],
        source_ids: ["imd-station-nowcast"]
      }
    },
    reservoirByDistrict: {
      ernakulam: { active: true, severity: 0.35, notes: ["Reservoir caution active"], source_ids: ["ksdma-reservoirs"] }
    },
    damByDistrict: {
      ernakulam: { active: true, severity: 0.35, notes: ["Dam downstream caution active"], source_ids: ["ksdma-dam-management"] }
    },
    cwcByDistrict: {},
    radarByDistrict: {
      ernakulam: {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near district"],
        source_ids: ["rainviewer-radar"]
      }
    },
    radarByHotspot: {
      "h-aluva-periyar": {
        severity: 0,
        intensity: "none",
        max_dbz: 0,
        notes: ["No meaningful RainViewer radar echo near hotspot"],
        source_ids: ["rainviewer-radar"]
      }
    },
    rainfallByDistrict: {
      ernakulam: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 1,
        rain_24h_mm: 13.3,
        rain_3d_mm: 58,
        rain_7d_mm: 118,
        official_rain_24h_mm: 13.3,
        official_station_count: 3,
        official_peak_station_24h_mm: 19.4,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean",
        peak_30m_mm: 0,
        source_ids: ["nasa-imerg-nrt"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["nasa-imerg-nrt"],
        antecedent_source_ids: ["nasa-imerg-nrt"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-aluva-periyar", score_boost: 12 }],
    freshnessBySource: {
      "imd-district-warning": 134,
      "imd-district-nowcast": 74,
      "imd-station-nowcast": 74,
      "rainviewer-radar": 14,
      "nasa-imerg-nrt": 284,
      "ksdma-reservoirs": 194,
      "ksdma-dam-management": 194
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "imd-district-nowcast": "ok",
      "imd-station-nowcast": "ok",
      "rainviewer-radar": "ok",
      "nasa-imerg-nrt": "ok",
      "ksdma-reservoirs": "ok",
      "ksdma-dam-management": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-aluva-periyar");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.ok(
    hotspot.drivers.some((driver) =>
      driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testActionableHydrologyStillPromotesDamDownstreamWatch() {
  const generatedAt = "2026-05-07T09:18:34.371Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "cwc-ffs", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.35,
        hazards: ["Heavy Rain"],
        notes: ["IDUKKI : Heavy Rain Updated on:2026-05-07"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {},
    stationNowcastByHotspot: {},
    reservoirByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Reservoir caution active"], source_ids: ["ksdma-reservoirs"] }
    },
    damByDistrict: {
      idukki: { active: true, severity: 0.35, notes: ["Dam downstream caution active"], source_ids: ["ksdma-dam-management"] }
    },
    cwcByDistrict: {
      idukki: {
        active: true,
        severity: 0.4,
        watch: true,
        above_warning_station_count: 1,
        above_danger_station_count: 0,
        forecast_warning_station_count: 0,
        forecast_danger_station_count: 0,
        notes: ["CWC flood forecasting watch active at 1 station"],
        source_ids: ["cwc-ffs"]
      }
    },
    radarByDistrict: {},
    radarByHotspot: {},
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 8,
        rain_3d_mm: 18,
        rain_7d_mm: 35,
        official_rain_24h_mm: 8,
        official_station_count: 0,
        official_peak_station_24h_mm: 0,
        spatial_aggregation: "district_polygon_mean",
        peak_30m_mm: 0,
        source_ids: ["nasa-imerg-nrt"]
      }
    },
    taluks: [],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-vandiperiyar-mullaperiyar", score_boost: 12 }],
    freshnessBySource: {
      "imd-district-warning": 889,
      "cwc-ffs": 59,
      "ksdma-reservoirs": 229,
      "ksdma-dam-management": 229
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "cwc-ffs": "ok",
      "ksdma-reservoirs": "ok",
      "ksdma-dam-management": "ok"
    }
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-vandiperiyar-mullaperiyar");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Watch");
  assert.ok(
    hotspot.drivers.every((driver) =>
      !driver.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
    )
  );
}

function testTalukWatchUsesLocalTalukRainObservation() {
  const generatedAt = "2026-05-07T09:18:34.371Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "nasa-imerg-nrt", status: "ok" }
    ],
    capByDistrict: {},
    bulletinByDistrict: {},
    imdDistrictWarningByDistrict: {
      idukki: {
        severity: 0.22,
        hazards: ["Thunderstorm & Lightning"],
        notes: ["IDUKKI : Thunderstorm & Lightning Updated on:2026-05-07"],
        source_ids: ["imd-district-warning"]
      }
    },
    imdNowcastByDistrict: {},
    stationNowcastByHotspot: {},
    reservoirByDistrict: {},
    damByDistrict: {},
    cwcByDistrict: {},
    radarByDistrict: {},
    radarByHotspot: {},
    rainfallByDistrict: {
      idukki: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 3,
        rain_3d_mm: 6,
        rain_7d_mm: 10,
        official_rain_24h_mm: 3,
        official_station_count: 0,
        official_peak_station_24h_mm: 0,
        spatial_aggregation: "district_polygon_mean",
        peak_30m_mm: 0,
        source_ids: ["nasa-imerg-nrt"]
      }
    },
    rainfallByTaluk: {
      "t-devikulam-test": {
        rain_1h_mm: 22,
        rain_3h_mm: 50,
        rain_6h_mm: 70,
        rain_24h_mm: 90,
        rain_3d_mm: 100,
        rain_7d_mm: 140,
        official_rain_24h_mm: 90,
        official_station_count: 0,
        official_peak_station_24h_mm: 0,
        spatial_aggregation: "taluk_representative_point",
        peak_30m_mm: 11,
        source_ids: ["nasa-imerg-nrt"],
        short_duration_source_ids: ["nasa-imerg-nrt"],
        daily_source_ids: ["nasa-imerg-nrt"],
        antecedent_source_ids: ["nasa-imerg-nrt"]
      }
    },
    taluks: [
      {
        taluk_id: "t-devikulam-test",
        district_id: "idukki",
        name: "Devikulam Test",
        hotspot_ids: ["h-munnar-devikulam"]
      }
    ],
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-munnar-devikulam", score_boost: 6 }],
    freshnessBySource: {
      "imd-district-warning": 889,
      "nasa-imerg-nrt": 259
    },
    statusBySource: {
      "imd-district-warning": "ok",
      "nasa-imerg-nrt": "ok"
    }
  });

  const taluk = result.talukStates.find((entry) => entry.area_id === "t-devikulam-test");
  assert.ok(taluk);
  assert.equal(taluk.level, "Watch");
  assert.ok(
    taluk.drivers.every((driver) =>
      !driver.includes("No current rain, river-stage, or operational release trigger supporting taluk watch")
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

async function testPipelineKeepsLatestCachedStateForUnselectedSources() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-source-selection-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });
  await runPipeline(tempRoot, { useFixtures: false, sourceIds: ["operator-observations"] });

  const sourcesRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "sources.json"), "utf8");
  const sources = JSON.parse(sourcesRaw);
  const rainfallSource = sources.sources.find((source) => source.source_id === "indiawris-rainfall");
  const operatorSource = sources.sources.find((source) => source.source_id === "operator-observations");

  assert.equal(rainfallSource?.reused_in_run, true);
  assert.equal(rainfallSource?.reuse_reason, "source_selection");
  assert.equal(rainfallSource?.fetch_status, "ok");
  assert.equal(operatorSource?.fetch_status, "ok");
}

async function testPipelineMarksFailedSourceUnavailableInCache() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-failure-state-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });
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

  assert.equal(operatorSource?.fetch_status, "failed");
  assert.equal(operatorSource?.parser_status, "skipped");
  assert.equal(operatorSource?.status, "offline");
  assert.equal(operatorSource?.reused_in_run, undefined);
  assert.equal(cache.sources["operator-observations"]?.snapshot.fetch_status, "failed");
  assert.equal(cache.sources["operator-observations"]?.parsed, null);
  assert.equal(rainfallSource?.fetch_status, "ok");
  assert.equal(rainfallSource?.reused_in_run, true);
  assert.equal(rainfallSource?.reuse_reason, "source_selection");
}

async function testPipelinePrunesOldArchiveRuns() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-archive-prune-"));
  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  const archiveRoot = path.join(tempRoot, "docs", "data", "archive");
  const latestRoot = path.join(tempRoot, "docs", "data", "latest");
  await mkdir(archiveRoot, { recursive: true });
  await mkdir(latestRoot, { recursive: true });

  const oldRuns = Array.from({ length: 35 }, (_, index) => {
    const day = String(Math.floor(index / 5) + 1).padStart(2, "0");
    const minute = String(index % 60).padStart(2, "0");
    const stamp = `${String(index).padStart(6, "0")}`;
    return {
      generated_at: `2026-03-${day}T00:${minute}:00.000Z`,
      headline_level: "Normal",
      headline_message: "Synthetic archive entry",
      severe_pending_count: 0,
      path: `./data/archive/2026/03/${day}/${stamp}`
    };
  });

  for (const run of oldRuns) {
    const runPath = path.join(tempRoot, "docs", run.path.replace("./", "").replaceAll("/", path.sep));
    await mkdir(runPath, { recursive: true });
    await writeFile(path.join(runPath, "dashboard.json"), JSON.stringify({ generated_at: run.generated_at }));
  }

  await writeFile(path.join(archiveRoot, "index.json"), JSON.stringify({ runs: oldRuns }, null, 2));

  await runPipeline(tempRoot, { useFixtures: true });

  const archiveIndex = JSON.parse(await readFile(path.join(archiveRoot, "index.json"), "utf8"));
  const retainedMarchPaths = archiveIndex.runs
    .filter((run) => run.path.includes("./data/archive/2026/03/"))
    .map((run) => path.join(tempRoot, "docs", run.path.replace("./", "").replaceAll("/", path.sep)));
  const retainedRunPaths = new Set(archiveIndex.runs.map((run) => run.path));
  const removedRun = oldRuns.find((run) => !retainedRunPaths.has(run.path));
  const prunedRunPath = path.join(
    tempRoot,
    "docs",
    removedRun.path.replace("./", "").replaceAll("/", path.sep)
  );

  assert.equal(archiveIndex.runs.length, 30);
  assert.ok(removedRun);
  assert.equal(await fileExists(prunedRunPath), false);
  assert.equal(retainedMarchPaths.length > 0, true);
  assert.equal((await Promise.all(retainedMarchPaths.map((entry) => fileExists(entry)))).every(Boolean), true);
}

async function testStableGeneratedJsonPreservesTimestampWhenContentMatches() {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-static-json-"));
  const filePath = path.join(tempRoot, "areas.json");

  const firstValue = {
    generated_at: "2026-04-09T08:44:01.155Z",
    districts: [{ id: "idukki", name: "Idukki" }],
    hotspots: []
  };
  const secondValue = {
    generated_at: "2026-04-09T09:44:01.155Z",
    districts: [{ id: "idukki", name: "Idukki" }],
    hotspots: []
  };
  const changedValue = {
    generated_at: "2026-04-09T10:44:01.155Z",
    districts: [{ id: "idukki", name: "Idukki Hills" }],
    hotspots: []
  };

  assert.equal(await writeStableGeneratedJson(filePath, firstValue), true);
  const firstText = await readFile(filePath, "utf8");

  assert.equal(await writeStableGeneratedJson(filePath, secondValue), false);
  const secondText = await readFile(filePath, "utf8");
  const secondJson = JSON.parse(secondText);

  assert.equal(secondText, firstText);
  assert.equal(secondJson.generated_at, firstValue.generated_at);

  assert.equal(await writeStableGeneratedJson(filePath, changedValue), true);
  const thirdJson = JSON.parse(await readFile(filePath, "utf8"));
  assert.equal(thirdJson.generated_at, changedValue.generated_at);
  assert.equal(thirdJson.districts[0].name, "Idukki Hills");
}

async function fileExists(targetPath) {
  try {
    await stat(targetPath);
    return true;
  } catch {
    return false;
  }
}

function testCalendarDayDistrictWarningFreshness() {
  const source = {
    freshness_mode: "calendar_day_ist",
    freshness_sla_minutes: 720,
    offline_after_minutes: 2880
  };

  assert.equal(
    statusFromFreshness(900, source, true, true, "2026-03-31T00:00:00.000Z", "2026-03-31T10:40:38.758Z"),
    "ok"
  );
  assert.equal(
    statusFromFreshness(1500, source, true, true, "2026-03-30T00:00:00.000Z", "2026-03-31T10:40:38.758Z"),
    "stale"
  );
  assert.equal(
    statusFromFreshness(3000, source, true, true, "2026-03-29T00:00:00.000Z", "2026-03-31T10:40:38.758Z"),
    "offline"
  );
}

function testNowcastFreshnessUsesValidUntil() {
  const districtNowcastSource = {
    id: "imd-district-nowcast",
    freshness_sla_minutes: 120,
    offline_after_minutes: 360
  };
  const stationNowcastSource = {
    id: "imd-station-nowcast",
    freshness_sla_minutes: 120,
    offline_after_minutes: 360
  };

  assert.equal(
    statusFromFreshness(
      127,
      districtNowcastSource,
      true,
      true,
      "2026-04-17T01:30:00.000Z",
      "2026-04-17T03:36:48.347Z",
      "2026-04-17T04:00:00.000Z"
    ),
    "ok"
  );
  assert.equal(
    statusFromFreshness(
      127,
      stationNowcastSource,
      true,
      true,
      "2026-04-17T01:30:00.000Z",
      "2026-04-17T03:36:48.347Z",
      "2026-04-17T03:00:00.000Z"
    ),
    "offline"
  );
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
  ["risk-model-stale-weighting", testRiskModelDownweightsStaleSignals],
  ["risk-model-hotspot-gating", testHotspotWatchNeedsDynamicTrigger],
  ["risk-model-district-warning-hotspot-gating", testDistrictWarningAloneDoesNotPromoteHotspotWatch],
  ["risk-model-moderate-storm-context-gating", testModerateStormContextDoesNotPromoteFlashFloodHotspotWatch],
  ["risk-model-short-duration-rain-trigger", testShortDurationRainCanPromoteFlashFloodHotspotWatch],
  ["risk-model-weak-rise-only-hydrology-gating", testWeakRiseOnlyHydrologyDoesNotPromoteDamDownstreamWatch],
  ["risk-model-stale-threshold-hydrology-gating", testStaleThresholdHydrologyDoesNotPromoteDamDownstreamWatch],
  ["risk-model-district-nowcast-not-local-dam-trigger", testDistrictNowcastContextDoesNotCountAsLocalDamDownstreamTrigger],
  ["risk-model-actionable-hydrology-watch", testActionableHydrologyStillPromotesDamDownstreamWatch],
  ["risk-model-taluk-local-rain-gating", testTalukWatchUsesLocalTalukRainObservation],
  ["pipeline", testPipeline],
  ["pipeline-partial-indiawris", testPipelineDegradesPartialIndiaWrisCoverage],
  ["pipeline-source-selection-cache", testPipelineKeepsLatestCachedStateForUnselectedSources],
  ["pipeline-failure-state", testPipelineMarksFailedSourceUnavailableInCache],
  ["pipeline-archive-retention", testPipelinePrunesOldArchiveRuns],
  ["stable-generated-json", testStableGeneratedJsonPreservesTimestampWhenContentMatches],
  ["tracked-output-integrity", testTrackedOutputsHaveNoMergeMarkersOrBrokenJson],
  ["calendar-day-freshness", testCalendarDayDistrictWarningFreshness],
  ["nowcast-valid-until-freshness", testNowcastFreshnessUsesValidUntil],
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
