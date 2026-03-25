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
      return 5;
    case "dam_downstream":
      return 4;
    case "river_floodplain":
      return 3;
    case "river_confluence":
      return 3;
    case "low_lying_basin":
      return 4;
    case "urban_flood_pocket":
      return 3;
    default:
      return 0;
  }
}

function hotspotSusceptibilityWeight(category) {
  switch (category) {
    case "steep_catchment":
      return 14;
    case "low_lying_basin":
      return 12;
    case "dam_downstream":
      return 10;
    case "river_floodplain":
      return 9;
    case "river_confluence":
      return 8;
    case "urban_flood_pocket":
      return 9;
    default:
      return 10;
  }
}

function hasOfficialSupport(cap, bulletin) {
  return (cap?.severity ?? 0) > 0.2 || (bulletin?.severity ?? 0) > 0.2;
}

function hasRainSupport(observation) {
  if (!observation) {
    return false;
  }
  return (
    (observation.peak_30m_mm ?? 0) >= 4 ||
    (observation.rain_1h_mm ?? 0) >= 8 ||
    (observation.rain_24h_mm ?? 0) >= 20
  );
}

function hasAntecedentSupport(observation) {
  if (!observation) {
    return false;
  }
  return (observation.rain_3d_mm ?? 0) >= 45 || (observation.rain_7d_mm ?? 0) >= 90;
}

function hasRadarSupport(radar, { strongOnly = false } = {}) {
  const severity = radar?.severity ?? 0;
  const maxDbz = radar?.max_dbz ?? 0;
  if (strongOnly) {
    return severity >= 0.45 || maxDbz >= 25;
  }
  return severity >= 0.25 || maxDbz >= 18;
}

function hasHydrologySupport(cwc) {
  return (cwc?.severity ?? 0) >= 0.25;
}

function hasOperationalDamSupport(reservoir, dam) {
  return (reservoir?.severity ?? 0) >= 0.32 || (dam?.severity ?? 0) >= 0.32;
}

function hasWatchSupport({ category, cap, bulletin, observation, radar, cwc, reservoir, dam }) {
  const official = hasOfficialSupport(cap, bulletin);
  const rain = hasRainSupport(observation);
  const antecedent = hasAntecedentSupport(observation);
  const radarStrong = hasRadarSupport(radar, { strongOnly: true });
  const hydro = hasHydrologySupport(cwc);
  const damOps = hasOperationalDamSupport(reservoir, dam);

  switch (category) {
    case "river_floodplain":
    case "river_confluence":
      return official || hydro || damOps || (rain && radarStrong) || (antecedent && (rain || radarStrong));
    case "dam_downstream":
      return official || hydro || damOps || (rain && radarStrong);
    default:
      return official || rain || antecedent || radarStrong || hydro || damOps;
  }
}

export function buildRiskOutputs(context) {
  const {
    thresholds,
    sourceSnapshots,
    taluks = [],
    capByDistrict,
    bulletinByDistrict,
    reservoirByDistrict,
    damByDistrict,
    cwcByDistrict,
    radarByDistrict = {},
    radarByHotspot = {},
    rainfallByDistrict,
    rainfallByTaluk = {},
    terrainStats,
    approvals,
    hotspotOverrides
  } = context;

  const totalSources = sourceSnapshots.length;
  const onlineSources = sourceSnapshots.filter((source) => source.status === "ok").length;
  const confidenceBase = confidenceFromCoverage(onlineSources, totalSources);
  const terrainByDistrict = buildDistrictTerrainLookup(districts, terrainStats);
  const districtModelContextById = {};

  const hotspotOverrideLookup = Object.fromEntries(
    hotspotOverrides.map((override) => [override.hotspot_id, override])
  );

  const districtStates = districts.map((district) => {
    const cap = capByDistrict[district.id] ?? { severity: 0, items: [] };
    const bulletin = bulletinByDistrict[district.id] ?? { severity: 0, notes: [] };
    const reservoir = reservoirByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const dam = damByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const cwc = cwcByDistrict[district.id] ?? { active: false, severity: 0, notes: [] };
    const radar = radarByDistrict[district.id] ?? { severity: 0, intensity: "none", max_dbz: null, notes: [] };
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
      radar_nowcast:
        severityToPoints(radar.severity) * (thresholds.weights.radar_nowcast ?? 0.08),
      reservoir_dam:
        Math.max(severityToPoints(reservoir.severity), severityToPoints(dam.severity)) *
        thresholds.weights.reservoir_dam
    };

    const activeSignals = [
      cap.severity > 0.2,
      bulletin.severity > 0.2,
      computeRainfallScore(observation, thresholds.rainfall_caps_mm) > 25,
      radar.severity > 0.2,
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

    districtModelContextById[district.id] = {
      cap,
      bulletin,
      reservoir,
      dam,
      cwc,
      radar,
      observation,
      terrain,
      componentScores,
      rawScore,
      dynamicRawScore: Math.max(0, rawScore - componentScores.terrain)
    };

    const score = clamp(rawScore / 100, 0, 1) * 100;
    const level = scoreToLevel(score);

    const drivers = [
      cap.severity > 0 ? `IMD CAP severity ${round(cap.severity * 100)}%` : null,
      bulletin.severity > 0 ? "IMD flash-flood bulletin corroborates threat" : null,
      observation
        ? `Observed 24h rain ${observation.rain_24h_mm ?? 0} mm and 1h rain ${observation.rain_1h_mm ?? 0} mm`
        : null,
      observation?.official_rain_24h_mm !== undefined && observation?.official_rain_24h_mm !== null
        ? `India-WRIS official 24h rainfall ${observation.official_rain_24h_mm} mm from ${observation.official_station_count ?? 0} station${(observation.official_station_count ?? 0) === 1 ? "" : "s"}`
        : null,
      observation?.official_peak_station_24h_mm
        ? `Peak India-WRIS station 24h rainfall ${observation.official_peak_station_24h_mm} mm`
        : null,
      observation?.peak_30m_mm ? `Peak recent 30 min rain ${observation.peak_30m_mm} mm` : null,
      observation?.spatial_aggregation ? `Rainfall aggregation ${observation.spatial_aggregation}` : null,
      radar.severity > 0
        ? `RainViewer nowcast ${radar.intensity.replaceAll("_", " ")} (${radar.max_dbz ?? "n/a"} dBZ)`
        : null,
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
          "indiawris-rainfall",
          observation?.official_rain_24h_mm !== undefined && observation?.official_rain_24h_mm !== null
            ? `Official rainfall available from ${observation.official_station_count ?? 0} station${(observation.official_station_count ?? 0) === 1 ? "" : "s"}`
            : "No India-WRIS rainfall signal mapped for district",
          context.freshnessBySource["indiawris-rainfall"],
          context.statusBySource["indiawris-rainfall"]
        ),
        sourceRef(
          "rainviewer-radar",
          radar.notes?.[0] ?? "No short-lead radar cell near district",
          context.freshnessBySource["rainviewer-radar"],
          context.statusBySource["rainviewer-radar"]
        ),
        sourceRef(
          "indiawris-river-level",
          cwc.notes?.find((note) => note.startsWith("India-WRIS river level")) ?? "No India-WRIS river-level context for district",
          context.freshnessBySource["indiawris-river-level"],
          context.statusBySource["indiawris-river-level"]
        ),
        sourceRef(
          "cwc-ffs",
          cwc.notes?.find((note) => note.includes("CWC flood forecasting")) ?? "No river-stage warning for district",
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
    const districtModel = districtModelContextById[hotspot.district_id];
    const override = hotspotOverrideLookup[hotspot.id];
    const radar = radarByHotspot[hotspot.id] ?? {
      severity: radarByDistrict[hotspot.district_id]?.severity ?? 0,
      intensity: radarByDistrict[hotspot.district_id]?.intensity ?? "none",
      max_dbz: radarByDistrict[hotspot.district_id]?.max_dbz ?? null,
      notes: radarByDistrict[hotspot.district_id]?.notes ?? []
    };
    const manualBoost = override?.score_boost ?? 0;
    const districtTerrain = terrainByDistrict[hotspot.district_id];
    const hotspotSusceptibility = clamp(hotspot.susceptibility * 0.7 + districtTerrain.value * 0.3);
    const categoryBoost = hotspotCategoryBoost(hotspot.category);
    const supportForWatch = hasWatchSupport({
      category: hotspot.category,
      cap: districtModel?.cap,
      bulletin: districtModel?.bulletin,
      observation: districtModel?.observation,
      radar,
      cwc: districtModel?.cwc,
      reservoir: districtModel?.reservoir,
      dam: districtModel?.dam
    });
    const hotspotRawScore =
      (districtModel?.dynamicRawScore ?? Math.max(0, districtState.score - districtTerrain.value * 100 * thresholds.weights.terrain)) +
      hotspotSusceptibility * hotspotSusceptibilityWeight(hotspot.category) +
      categoryBoost +
      manualBoost +
      severityToPoints(radar.severity) * 0.08 +
      Math.max(
        severityToPoints(districtModel?.cwc?.severity),
        severityToPoints(districtModel?.reservoir?.severity),
        severityToPoints(districtModel?.dam?.severity)
      ) *
        (["river_floodplain", "river_confluence", "dam_downstream"].includes(hotspot.category) ? 0.06 : 0.02);
    let score = round(clamp(hotspotRawScore / 100, 0, 1) * 100);
    if (!supportForWatch && score >= thresholds.thresholds.watch) {
      score = thresholds.thresholds.watch - 0.1;
    }
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
        radar.severity > 0
          ? `Hotspot radar echo ${radar.intensity.replaceAll("_", " ")} (${radar.max_dbz ?? "n/a"} dBZ)`
          : null,
        !supportForWatch ? "No current rain, river-stage, or operational release trigger supporting hotspot watch" : null,
        `Hotspot susceptibility ${(hotspotSusceptibility * 100).toFixed(0)}%`,
        hotspot.category ? `Hotspot category ${hotspot.category.replaceAll("_", " ")}` : null,
        hotspot.buffer_km ? `Hotspot analysis radius ${hotspot.buffer_km} km` : null
      ].filter(Boolean),
      source_refs: districtState.source_refs.map((sourceRefEntry) =>
        sourceRefEntry.source_id === "rainviewer-radar"
          ? {
              ...sourceRefEntry,
              detail: radar.notes?.[0] ?? sourceRefEntry.detail
            }
          : sourceRefEntry
      ),
      review_state: level === "Severe - review required" ? "pending_review" : "auto_published",
      valid_from: districtState.valid_from,
      valid_to: districtState.valid_to
    };
  });

  const hotspotStateLookup = Object.fromEntries(
    hotspotStates.map((hotspotState) => [hotspotState.area_id, hotspotState])
  );
  const hotspotDefinitionLookup = Object.fromEntries(hotspots.map((hotspot) => [hotspot.id, hotspot]));

  const talukStates = taluks.map((taluk) => {
    const districtState = districtStates.find((state) => state.area_id === taluk.district_id);
    const cap = capByDistrict[taluk.district_id] ?? { severity: 0, items: [] };
    const bulletin = bulletinByDistrict[taluk.district_id] ?? { severity: 0, notes: [] };
    const reservoir = reservoirByDistrict[taluk.district_id] ?? { active: false, severity: 0, notes: [] };
    const dam = damByDistrict[taluk.district_id] ?? { active: false, severity: 0, notes: [] };
    const cwc = cwcByDistrict[taluk.district_id] ?? { active: false, severity: 0, notes: [] };
    const radarMatches = (taluk.hotspot_ids ?? [])
      .map((hotspotId) => radarByHotspot[hotspotId])
      .filter(Boolean);
    const radar = radarMatches.length
      ? radarMatches.reduce((best, current) =>
          (current.severity ?? 0) > (best.severity ?? 0) ? current : best
        )
      : radarByDistrict[taluk.district_id] ?? { severity: 0, intensity: "none", max_dbz: null, notes: [] };
    const observation = rainfallByTaluk[taluk.taluk_id] ?? rainfallByDistrict[taluk.district_id] ?? null;
    const talukHotspots = (taluk.hotspot_ids ?? [])
      .map((hotspotId) => hotspotStateLookup[hotspotId])
      .filter(Boolean);
    const hotspotDefinitions = (taluk.hotspot_ids ?? [])
      .map((hotspotId) => hotspotDefinitionLookup[hotspotId])
      .filter(Boolean);
    const hotspotScoreMax = talukHotspots.length
      ? Math.max(...talukHotspots.map((hotspot) => hotspot.score))
      : districtState.score;
    const hotspotExcess = Math.max(0, hotspotScoreMax - districtState.score);
    const hotspotSusceptibility = talukHotspots.length
      ? talukHotspots.reduce((sum, hotspot) => sum + hotspot.susceptibility, 0) / talukHotspots.length
      : districtState.susceptibility;
    const componentScores = {
      cap_warning: severityToPoints(cap.severity) * thresholds.weights.cap_warning,
      flash_flood_bulletin:
        severityToPoints(bulletin.severity) * thresholds.weights.flash_flood_bulletin,
      rainfall: computeRainfallScore(observation, thresholds.rainfall_caps_mm) * thresholds.weights.rainfall,
      antecedent_wetness:
        computeAntecedentScore(observation, thresholds.rainfall_caps_mm) *
        thresholds.weights.antecedent_wetness,
      terrain: hotspotSusceptibility * 100 * thresholds.weights.terrain,
      hydrology: severityToPoints(cwc.severity) * thresholds.weights.hydrology,
      radar_nowcast:
        severityToPoints(radar.severity) * (thresholds.weights.radar_nowcast ?? 0.08),
      reservoir_dam:
        Math.max(severityToPoints(reservoir.severity), severityToPoints(dam.severity)) *
        thresholds.weights.reservoir_dam
    };
    const activeSignals = [
      cap.severity > 0.2,
      bulletin.severity > 0.2,
      computeRainfallScore(observation, thresholds.rainfall_caps_mm) > 25,
      radar.severity > 0.2,
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
      agreementBonus(activeSignals, thresholds.agreement_bonus) +
      hotspotExcess * 0.35 +
      Math.min(8, talukHotspots.length * 2.5) -
      snapshotPenalty;
    const talukSupportsWatch = talukHotspots.length
      ? talukHotspots.some((hotspot) =>
          hasWatchSupport({
            category: hotspot.category,
            cap,
            bulletin,
            observation,
            radar,
            cwc,
            reservoir,
            dam
          })
        )
      : hasWatchSupport({
          category: null,
          cap,
          bulletin,
          observation,
          radar,
          cwc,
          reservoir,
          dam
        });
    let score = round(clamp(rawScore / 100, 0, 1) * 100);
    if (!talukSupportsWatch && score >= thresholds.thresholds.watch) {
      score = thresholds.thresholds.watch - 0.1;
    }
    const level = scoreToLevel(score);
    const hotspotNames = hotspotDefinitions.map((hotspot) => hotspot.name);

    return {
      area_id: taluk.taluk_id,
      area_type: "taluk",
      district_id: taluk.district_id,
      district_name: districtState.name,
      name: taluk.name,
      level,
      score,
      confidence: round(clamp(districtState.confidence * 0.96, 0, 1)),
      susceptibility: round(hotspotSusceptibility),
      hotspot_ids: taluk.hotspot_ids ?? [],
      hotspot_count: talukHotspots.length,
      centroid: taluk.centroid ?? null,
      rainfall: observation,
      valid_from: districtState.valid_from,
      valid_to: districtState.valid_to,
      drivers: [
        observation
          ? `Observed taluk 24h rain ${observation.rain_24h_mm ?? 0} mm and 1h rain ${observation.rain_1h_mm ?? 0} mm`
          : `Inherited rainfall and warning signal from ${districtState.name}`,
        observation?.peak_30m_mm ? `Peak recent 30 min rain ${observation.peak_30m_mm} mm` : null,
        observation?.spatial_aggregation ? `Rainfall aggregation ${observation.spatial_aggregation}` : null,
        radar.severity > 0
          ? `RainViewer nowcast ${radar.intensity.replaceAll("_", " ")} (${radar.max_dbz ?? "n/a"} dBZ)`
          : null,
        talukHotspots.length
          ? `${talukHotspots.length} mapped hotspot${talukHotspots.length > 1 ? "s" : ""}: ${hotspotNames.slice(0, 3).join(", ")}${hotspotNames.length > 3 ? ", ..." : ""}`
          : "No mapped hotspot override within taluk",
        `Taluk susceptibility ${(hotspotSusceptibility * 100).toFixed(0)}% from district terrain and hotspot context`,
        !talukSupportsWatch ? "No current rain, river-stage, or operational release trigger supporting taluk watch" : null,
        cwc.active ? "CWC river-stage warning/watch active" : null,
        reservoir.active ? "Reservoir caution active" : null,
        dam.active ? "Dam downstream caution active" : null
      ].filter(Boolean),
      source_refs: districtState.source_refs,
      review_state: level === "Severe - review required" ? "pending_review" : "auto_published"
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

  return { districtStates, talukStates, hotspotStates, alerts };
}
