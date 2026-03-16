import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { cp, mkdtemp, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import thresholds from "../config/risk-thresholds.json" with { type: "json" };
import {
  parseImdCapRss,
  parseImdFlashFloodBulletin,
  parseCwcFfs
} from "../scripts/lib/parsers.js";
import { parseImergTextListing, selectImergWindows } from "../scripts/lib/imerg.js";
import { districtIdFromBoundaryName, pointInGeometry } from "../scripts/lib/boundaries.js";
import { buildHotspotFootprint } from "../src/shared/hotspot-footprints.js";
import { buildRiskOutputs } from "../scripts/lib/risk-model.js";
import { runPipeline } from "../scripts/lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

async function testParsers() {
  const capRaw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-rss.xml"), "utf8");
  const cap = parseImdCapRss(capRaw);
  assert.equal(cap.item_count, 2);
  assert.ok(cap.kerala_district_ids.includes("idukki"));

  const bulletinRaw = await readFile(
    path.join(repoRoot, "fixtures", "imd-flash-flood-bulletin.html"),
    "utf8"
  );
  const bulletin = parseImdFlashFloodBulletin(bulletinRaw);
  assert.ok(bulletin.kerala_district_ids.includes("ernakulam"));

  const cwcRaw = await readFile(path.join(repoRoot, "fixtures", "cwc-ffs.html"), "utf8");
  const cwc = parseCwcFfs(cwcRaw);
  assert.equal(cwc.warning, true);
  assert.equal(cwc.watch, true);
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
  assert.ok(result.alerts.every((alert) => alert.source_refs.length > 0));
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
  assert.equal(dashboard.mode, "decision-support");
  assert.equal(adminAreas.boundaries.counts.district, 14);
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

function testBoundaryHelpers() {
  assert.equal(districtIdFromBoundaryName("Thiruvananthapuram"), "thiruvananthapuram");
  assert.equal(districtIdFromBoundaryName("Pathanamthitta"), "pathanamthitta");
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
  ["boundaries", testBoundaryHelpers],
  ["hotspot-footprints", testHotspotFootprints],
  ["risk-model", testRiskModel],
  ["pipeline", testPipeline]
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
