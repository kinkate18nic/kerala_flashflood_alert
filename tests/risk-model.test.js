import test from "node:test";
import assert from "node:assert/strict";
import thresholds from "../config/risk-thresholds.json" with { type: "json" };
import { buildRiskOutputs } from "../scripts/lib/risk-model.js";

test("buildRiskOutputs creates reviewed or pending alerts with evidence", () => {
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
  assert.ok(idukki.drivers.length > 0);
  assert.ok(result.alerts.every((alert) => alert.source_refs.length > 0));
});

test("river floodplain hotspot is clamped below watch when only weak hydro and dam context exist", () => {
  const generatedAt = "2026-04-15T06:08:33.148Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "indiawris-river-level", status: "ok" },
      { source_id: "cwc-ffs", status: "offline" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    imdDistrictWarningByDistrict: {
      pathanamthitta: { severity: 0, hazards: ["Hot Day"], notes: ["PATHANAMTHITTA : Hot Day"] }
    },
    imdNowcastByDistrict: {
      pathanamthitta: {
        severity: 0.22,
        notes: ["PATHANAMTHITTA Light rain"],
        issued_at: "2026-04-15T04:30:00.000Z",
        valid_until: "2026-04-15T07:30:00.000Z"
      }
    },
    stationNowcastByHotspot: {
      "h-pamba-corridor": {
        severity: 0.18,
        station_name: "Pathanamthitta",
        distance_km: 11.9,
        notes: ["Pathanamthitta: Light rain"],
        issued_at: "2026-04-15T04:30:00.000Z",
        valid_until: "2026-04-15T07:30:00.000Z"
      }
    },
    radarByDistrict: {
      pathanamthitta: {
        severity: 0.23,
        intensity: "moderate",
        max_dbz: 23,
        notes: ["RainViewer moderate cell near district"]
      }
    },
    radarByHotspot: {
      "h-pamba-corridor": {
        severity: 0.05,
        intensity: "light",
        max_dbz: 5,
        notes: ["RainViewer light cell near hotspot"]
      }
    },
    reservoirByDistrict: {
      pathanamthitta: { active: true, severity: 0.85, notes: ["Reservoir caution active"] }
    },
    damByDistrict: {
      pathanamthitta: { active: true, severity: 0.85, notes: ["Dam downstream caution active"] }
    },
    cwcByDistrict: {
      pathanamthitta: {
        active: true,
        severity: 0.35,
        notes: [
          "India-WRIS river level above danger at MALAKKARA",
          "CWC flood forecasting live river level available from 4 stations"
        ],
        source_ids: ["indiawris-river-level", "cwc-ffs"]
      }
    },
    rainfallByDistrict: {
      pathanamthitta: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 0,
        rain_3d_mm: 0,
        rain_7d_mm: 0,
        peak_30m_mm: 0,
        official_rain_24h_mm: 0,
        official_station_count: 1,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean"
      }
    },
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-pamba-corridor", score_boost: 20 }]
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-pamba-corridor");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Normal");
  assert.equal(hotspot.score, 34.9);
  assert.ok(
    hotspot.drivers.includes("No current rain, river-stage, or operational release trigger supporting hotspot watch")
  );
});

test("river floodplain hotspot can still reach watch on strong hydrology alone", () => {
  const generatedAt = "2026-04-15T06:08:33.148Z";
  const result = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: [
      { source_id: "imd-district-warning", status: "ok" },
      { source_id: "imd-district-nowcast", status: "ok" },
      { source_id: "imd-station-nowcast", status: "ok" },
      { source_id: "rainviewer-radar", status: "ok" },
      { source_id: "indiawris-river-level", status: "ok" },
      { source_id: "cwc-ffs", status: "ok" },
      { source_id: "ksdma-reservoirs", status: "ok" },
      { source_id: "ksdma-dam-management", status: "ok" }
    ],
    imdDistrictWarningByDistrict: {
      pathanamthitta: { severity: 0, hazards: ["Hot Day"], notes: ["PATHANAMTHITTA : Hot Day"] }
    },
    imdNowcastByDistrict: {
      pathanamthitta: {
        severity: 0.18,
        notes: ["PATHANAMTHITTA Light rain"],
        issued_at: "2026-04-15T04:30:00.000Z",
        valid_until: "2026-04-15T07:30:00.000Z"
      }
    },
    stationNowcastByHotspot: {
      "h-pamba-corridor": {
        severity: 0.18,
        station_name: "Pathanamthitta",
        distance_km: 11.9,
        notes: ["Pathanamthitta: Light rain"],
        issued_at: "2026-04-15T04:30:00.000Z",
        valid_until: "2026-04-15T07:30:00.000Z"
      }
    },
    radarByDistrict: {
      pathanamthitta: {
        severity: 0.1,
        intensity: "light",
        max_dbz: 12,
        notes: ["RainViewer weak cell near district"]
      }
    },
    radarByHotspot: {
      "h-pamba-corridor": {
        severity: 0.05,
        intensity: "light",
        max_dbz: 5,
        notes: ["RainViewer light cell near hotspot"]
      }
    },
    reservoirByDistrict: {
      pathanamthitta: { active: true, severity: 0.2, notes: ["Reservoir caution active"] }
    },
    damByDistrict: {
      pathanamthitta: { active: true, severity: 0.2, notes: ["Dam downstream caution active"] }
    },
    cwcByDistrict: {
      pathanamthitta: {
        active: true,
        severity: 0.7,
        notes: [
          "India-WRIS river level above danger at MALAKKARA",
          "CWC flood forecasting live river level available from 4 stations"
        ],
        source_ids: ["indiawris-river-level", "cwc-ffs"]
      }
    },
    rainfallByDistrict: {
      pathanamthitta: {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        rain_24h_mm: 0,
        rain_3d_mm: 0,
        rain_7d_mm: 0,
        peak_30m_mm: 0,
        official_rain_24h_mm: 0,
        official_station_count: 1,
        spatial_aggregation: "district_polygon_mean+indiawris_station_mean"
      }
    },
    approvals: [],
    hotspotOverrides: [{ hotspot_id: "h-pamba-corridor", score_boost: 20 }]
  });

  const hotspot = result.hotspotStates.find((entry) => entry.area_id === "h-pamba-corridor");
  assert.ok(hotspot);
  assert.equal(hotspot.level, "Watch");
  assert.ok(hotspot.score >= thresholds.thresholds.watch);
});
