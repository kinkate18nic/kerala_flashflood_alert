import { districts, hotspots } from "../../src/shared/areas.js";
import { scoreToLevel } from "../../src/shared/risk.js";
import { buildDistrictTerrainLookup, clamp } from "../../src/shared/terrain.js";

function round(value) {
  return Math.round(value * 10) / 10;
}

function normalize(value, cap) {
  if (!value || !cap) {
    return 0;
  }
  return clamp(value / cap);
}

function severityToPoints(value) {
  return clamp(value ?? 0) * 100;
}

function computeRainfallScore(observation, rainfallCaps) {
  if (!observation) {
    return 0;
  }
  return (
    normalize(observation.rain_1h_mm, rainfallCaps.one_hour) * 0.28 +
    normalize(observation.rain_3h_mm, rainfallCaps.three_hour) * 0.24 +
    normalize(observation.rain_6h_mm, rainfallCaps.six_hour) * 0.24 +
    normalize(observation.rain_24h_mm, rainfallCaps.day) * 0.24
  ) * 100;
}

function computeAntecedentScore(observation, rainfallCaps) {
  if (!observation) {
    return 0;
  }
  return (
    normalize(observation.rain_3d_mm, rainfallCaps.three_day) * 0.55 +
    normalize(observation.rain_7d_mm, rainfallCaps.seven_day) * 0.45
  ) * 100;
}

function agreementBonus(activeSignals, agreementConfig) {
  if (activeSignals >= 4) {
    return agreementConfig.four_plus;
  }
  if (activeSignals === 3) {
    return agreementConfig.three;
  }
  if (activeSignals === 2) {
    return agreementConfig.two;
  }
  return agreementConfig.one;
}

function confidenceFromCoverage(onlineSources, totalSources) {
  if (!totalSources) {
    return 0.2;
  }
  return round(clamp(0.25 + (onlineSources / totalSources) * 0.75));
}

function sourceRef(sourceId, detail, freshnessMinutes, status) {
  return {
    source_id: sourceId,
    detail,
    freshness_minutes: freshnessMinutes,
    status
  };
}

function hotspotCategoryBoost(category) {
  switch (category) {
    case "steep_catchment":
      return 8;
    case "dam_downstream":
      return 7;
    case "river_floodplain":
      return 6;
    case "river_confluence":
      return 5;
    case "low_lying_basin":
      return 7;
    case "urban_flood_pocket":
      return 4;
    default:
      return 0;
  }
}

export function buildRiskOutputs(context) {
  const {
    thresholds,
    sourceSnapshots,
    capByDistrict,
    bulletinByDistrict,
    reservoirByDistrict,
    damByDistrict,
    cwcByDistrict,
    rainfallByDistrict,
    terrainStats,
    approvals,
    hotspotOverrides
  } = context;

  const totalSources = sourceSnapshots.length;
  const onlineSources = sourceSnapshots.filter((source) => source.status === "ok").length;
  const confidenceBase = confidenceFromCoverage(onlineSources, totalSources);
  const terrainByDistrict = buildDistrictTerrainLookup(districts, terrainStats);

  const hotspotOverrideLookup = Object.fromEntries(
    hotspotOverrides.map((override) => [override.hotspot_id, override])
  );

  const districtStates = districts.map((district) => {
    const cap = capByDistrict[district.id] ?? { severity: 0, items: [] };
    const bulletin = bulletinByDistrict[district.id] ?? { severity: 0, notes: [] };
    const reservoir = reservoirByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const dam = damByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const cwc = cwcByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const observation = rainfallByDistrict[district.id] ?? null;
    const terrain = terrainByDistrict[district.id];

    const componentScores = {
      cap_warning: severityToPoints(cap.severity) * thresholds.weights.cap_warning,
      flash_flood_bulletin:
        severityToPoints(bulletin.severity) * thresholds.weights.flash_flood_bulletin,
      rainfall: computeRainfallScore(observation, thresholds.rainfall_caps_mm) * thresholds.weights.rainfall,
      antecedent_wetness:
        computeAntecedentScore(observation, thresholds.rainfall_caps_mm) *
        thresholds.weights.antecedent_wetness,
      terrain: terrain.value * 100 * thresholds.weights.terrain,
      hydrology: severityToPoints(cwc.severity) * thresholds.weights.hydrology,
      reservoir_dam:
        Math.max(severityToPoints(reservoir.severity), severityToPoints(dam.severity)) *
        thresholds.weights.reservoir_dam
    };

    const activeSignals = [
      cap.severity > 0.2,
      bulletin.severity > 0.2,
      computeRainfallScore(observation, thresholds.rainfall_caps_mm) > 25,
      cwc.severity > 0.2 || reservoir.severity > 0.2 || dam.severity > 0.2
    ].filter(Boolean).length;

    const snapshotPenalty = sourceSnapshots.reduce((penalty, source) => {
      if (source.status === "offline") {
        return penalty + thresholds.freshness_penalties.offline / totalSources;
      }
      if (source.status === "stale") {
        return penalty + thresholds.freshness_penalties.stale / totalSources;
      }
      if (source.status === "degraded") {
        return penalty + thresholds.freshness_penalties.degraded / totalSources;
      }
      return penalty;
    }, 0);

    const rawScore =
      Object.values(componentScores).reduce((sum, value) => sum + value, 0) +
      agreementBonus(activeSignals, thresholds.agreement_bonus) -
      snapshotPenalty;

    const score = clamp(rawScore / 100, 0, 1) * 100;
    const level = scoreToLevel(score);

    const drivers = [
      cap.severity > 0 ? `IMD CAP severity ${round(cap.severity * 100)}%` : null,
      bulletin.severity > 0 ? "IMD flash-flood bulletin corroborates threat" : null,
      observation ? `Observed 24h rain ${observation.rain_24h_mm ?? 0} mm` : null,
      terrain.dem
        ? `Terrain susceptibility ${(terrain.value * 100).toFixed(0)}% from NASADEM + local baseline`
        : `Terrain susceptibility ${(terrain.value * 100).toFixed(0)}% from local baseline`,
      cwc.active ? "CWC river-stage warning/watch active" : null,
      reservoir.active ? "Reservoir caution active" : null,
      dam.active ? "Dam downstream caution active" : null
    ].filter(Boolean);

    return {
      area_id: district.id,
      area_type: "district",
      name: district.name,
      level,
      score: round(score),
      confidence: round(confidenceBase),
      region: district.region,
      susceptibility: terrain.value,
      terrain_model: terrain.dem ? "nasadem_blended" : "manual_baseline",
      terrain_metrics: terrain.dem
        ? {
            terrain_score_raw: terrain.dem.terrain_score_raw,
            elevation_mean_m: terrain.dem.elevation_mean_m,
            slope_mean_deg: terrain.dem.slope_mean_deg,
            roughness_mean: terrain.dem.roughness_mean,
            dem_normalized: round(terrain.dem_normalized ?? 0)
          }
        : null,
      valid_from: context.generatedAt,
      valid_to: new Date(new Date(context.generatedAt).getTime() + 6 * 3600 * 1000).toISOString(),
      drivers,
      source_refs: [
        sourceRef("imd-cap-rss", `${cap.items.length} CAP items`, context.freshnessBySource["imd-cap-rss"], context.statusBySource["imd-cap-rss"]),
        sourceRef(
          "imd-flash-flood-bulletin",
          bulletin.notes?.[0] ?? "No Kerala bulletin trigger",
          context.freshnessBySource["imd-flash-flood-bulletin"],
          context.statusBySource["imd-flash-flood-bulletin"]
        ),
        sourceRef(
          "cwc-ffs",
          cwc.notes?.[0] ?? "No river-stage warning for district",
          context.freshnessBySource["cwc-ffs"],
          context.statusBySource["cwc-ffs"]
        )
      ],
      rainfall: observation,
      review_state: level === "Severe - review required" ? "pending_review" : "auto_published"
    };
  });

  const hotspotStates = hotspots.map((hotspot) => {
    const districtState = districtStates.find((state) => state.area_id === hotspot.district_id);
    const override = hotspotOverrideLookup[hotspot.id];
    const manualBoost = override?.score_boost ?? 0;
    const districtTerrain = terrainByDistrict[hotspot.district_id];
    const hotspotSusceptibility = clamp(hotspot.susceptibility * 0.7 + districtTerrain.value * 0.3);
    const categoryBoost = hotspotCategoryBoost(hotspot.category);
    const score = round(
      clamp((districtState.score + hotspotSusceptibility * 20 + categoryBoost + manualBoost) / 100, 0, 1) * 100
    );
    const level = scoreToLevel(score);
    return {
      area_id: hotspot.id,
      area_type: "hotspot",
      district_id: hotspot.district_id,
      name: hotspot.name,
      tags: hotspot.tags,
      level,
      score,
      confidence: districtState.confidence,
      susceptibility: hotspotSusceptibility,
      category: hotspot.category,
      location: hotspot.location ?? null,
      buffer_km: hotspot.buffer_km ?? null,
      drivers: [
        ...districtState.drivers,
        `Hotspot susceptibility ${(hotspotSusceptibility * 100).toFixed(0)}%`,
        hotspot.category ? `Hotspot category ${hotspot.category.replaceAll("_", " ")}` : null,
        hotspot.buffer_km ? `Hotspot analysis radius ${hotspot.buffer_km} km` : null
      ].filter(Boolean),
      source_refs: districtState.source_refs,
      review_state: level === "Severe - review required" ? "pending_review" : "auto_published",
      valid_from: districtState.valid_from,
      valid_to: districtState.valid_to
    };
  });

  const alerts = [...districtStates, ...hotspotStates]
    .filter((state) => state.level !== "Normal")
    .map((state) => {
      const approval = approvals.find((entry) => entry.alert_id === `${state.area_id}:${state.valid_from}`);
      const approved = Boolean(approval) && state.level === "Severe - review required";
      const level = approved ? "Reviewed severe alert" : state.level;
      const review_state =
        level === "Reviewed severe alert"
          ? "approved"
          : state.level === "Severe - review required"
            ? "pending_review"
            : "auto_published";
      return {
        alert_id: `${state.area_id}:${state.valid_from}`,
        area_id: state.area_id,
        area_type: state.area_type,
        district_id: state.district_id ?? (state.area_type === "district" ? state.area_id : null),
        name: state.name,
        score: state.score,
        valid_from: state.valid_from,
        valid_to: state.valid_to,
        level,
        confidence: state.confidence,
        review_state,
        drivers: state.drivers,
        source_refs: state.source_refs,
        message_en: `${state.name}: ${level}. ${state.drivers[0] ?? "Multiple rainfall and hydrology signals are active."}`,
        recommended_actions:
          level === "Reviewed severe alert" || level === "Severe - review required"
            ? [
                "Confirm latest district administration advisories.",
                "Monitor low-lying roads, river crossings, and downstream release notices.",
                "Escalate operator review before broad public forwarding."
              ]
            : [
                "Continue district monitoring.",
                "Watch official IMD and district warnings for changes."
              ]
      };
    })
    .sort((left, right) => right.score - left.score);

  return { districtStates, hotspotStates, alerts };
}
