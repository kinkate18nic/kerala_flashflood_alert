import path from "node:path";
import { readFile } from "node:fs/promises";
import { readJson, writeJson, writeText, ensureDir, pathExists } from "./fs.js";
import {
  buildBoundaryMetadata,
  boundaryLayerSources,
  loadTalukBoundaries,
  parseTalukBoundaries,
  pointInGeometry
} from "./boundaries.js";
import { fetchText } from "./http.js";
import { fetchCwcFfsPayload } from "./cwc-ffs.js";
import { fetchImdCapPayload } from "./imd-cap.js";
import { fetchNasaImergPayload } from "./imerg.js";
import { fetchIndiaWrisRainfallPayload, fetchIndiaWrisRiverLevelPayload } from "./indiawris.js";
import { fetchKsdmaDailyDamPayload } from "./ksdma.js";
import { parserRegistry } from "./parsers.js";
import { fetchRainviewerPayload } from "./rainviewer.js";
import { minutesBetween, nowIso, parseDate, toArchivePathParts } from "./time.js";
import { buildRiskOutputs } from "./risk-model.js";
import { districts, hotspots } from "../../src/shared/areas.js";

function statusFromFreshness(freshnessMinutes, source, fetchOk, parserOk) {
  if (!fetchOk || !parserOk) {
    return "offline";
  }
  if (freshnessMinutes === null) {
    return "degraded";
  }
  const staleThreshold = source.freshness_sla_minutes;
  const offlineThreshold = source.offline_after_minutes ?? staleThreshold * 2;

  if (freshnessMinutes > offlineThreshold) {
    return "offline";
  }
  if (freshnessMinutes > staleThreshold) {
    return "stale";
  }
  return "ok";
}

function rawExtension(format) {
  if (format === "xml") {
    return "xml";
  }
  if (format === "json") {
    return "json";
  }
  return "html";
}

function summarizeSource(parsed) {
  if (!parsed) {
    return {};
  }
  if ("item_count" in parsed) {
    return {
      item_count: parsed.item_count,
      raw_item_count: parsed.raw_item_count ?? parsed.item_count,
      filtered_item_count: parsed.filtered_item_count ?? 0,
      district_count: parsed.kerala_district_ids?.length ?? 0
    };
  }
  if ("districts" in parsed && Array.isArray(parsed.districts)) {
    if ("hotspots" in parsed && Array.isArray(parsed.hotspots)) {
      return {
        district_count: parsed.districts.length,
        hotspot_count: parsed.hotspots.length,
        latest_frame_time: parsed.frame_time ?? null
      };
    }
    const summary = {
      district_count: parsed.districts.length,
      taluk_count: Array.isArray(parsed.taluks) ? parsed.taluks.length : 0
    };
    if ("station_count" in parsed) {
      summary.station_count = parsed.station_count;
    }
    if ("requested_district_count" in parsed) {
      summary.requested_district_count = parsed.requested_district_count;
      summary.successful_district_count = parsed.successful_district_count ?? parsed.requested_district_count;
      summary.failed_district_count = parsed.partial_failure_count ?? parsed.failed_districts?.length ?? 0;
    }
    if ("requested_station_count" in parsed) {
      summary.requested_station_count = parsed.requested_station_count;
      summary.successful_station_count = parsed.successful_station_count ?? parsed.requested_station_count;
      summary.failed_station_count = parsed.partial_failure_count ?? parsed.failed_stations?.length ?? 0;
    }
    if ("above_warning_station_count" in parsed) {
      summary.above_warning_station_count = parsed.above_warning_station_count;
      summary.above_danger_station_count = parsed.above_danger_station_count ?? 0;
    }
    if (parsed.source_files) {
      summary.latest_half_hour_file = parsed.source_files.half_hour?.[0]?.split("/").pop() ?? null;
      summary.latest_three_hour_file = parsed.source_files.three_hour?.[0]?.split("/").pop() ?? null;
      summary.latest_daily_file = parsed.source_files.daily?.[0]?.split("/").pop() ?? null;
    }
    return summary;
  }
  if ("summary" in parsed) {
    return { excerpt: parsed.summary.slice(0, 160) };
  }
  return {};
}

function applyCoverageStatus(baseStatus, parsed) {
  const failedCount = parsed?.partial_failure_count ?? parsed?.failed_districts?.length ?? 0;
  if (failedCount > 0 && baseStatus === "ok") {
    return "degraded";
  }
  return baseStatus;
}

function parserStatusFromState(fetchOk, parserOk) {
  if (!fetchOk) {
    return "skipped";
  }
  return parserOk ? "ok" : "failed";
}

function failureStageFromState(fetchOk, parserOk, parsed) {
  if (!fetchOk) {
    return "fetch";
  }
  if (!parserOk) {
    return "parse";
  }
  const partialFailureCount =
    parsed?.partial_failure_count ??
    parsed?.failed_districts?.length ??
    parsed?.failed_stations?.length ??
    0;
  if (partialFailureCount > 0) {
    return "partial";
  }
  return null;
}

function mapWithConcurrency(items, worker, concurrency = 3) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runWorker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  return Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker())
  ).then(() => results);
}

function sourceCachePath(repoRoot) {
  return path.join(repoRoot, "runtime", "cache", "source-results.json");
}

function sourceFreshnessFromSnapshot(snapshot, source, parsed, generatedAt) {
  const freshnessMinutes = minutesBetween(parseDate(snapshot?.issued_at), new Date(generatedAt));
  const baseStatus = statusFromFreshness(freshnessMinutes, source, true, true);
  return {
    freshnessMinutes,
    status: applyCoverageStatus(baseStatus, parsed)
  };
}

function cacheEntryHasSuccessfulPayload(cacheEntry) {
  return Boolean(
    cacheEntry?.snapshot &&
      cacheEntry?.parsed !== undefined &&
      cacheEntry.snapshot.fetch_status === "ok" &&
      cacheEntry.snapshot.parser_status === "ok"
  );
}

function canReuseSource(source, cacheEntry, generatedAt) {
  if (!cacheEntryHasSuccessfulPayload(cacheEntry) || source.cadence_minutes === undefined) {
    return false;
  }

  const fetchedDate = parseDate(cacheEntry.snapshot.fetched_at);
  const ageMinutes = minutesBetween(fetchedDate, new Date(generatedAt));
  if (ageMinutes === null) {
    return false;
  }

  return ageMinutes < source.cadence_minutes;
}

function buildReusedSnapshot(source, cacheEntry, generatedAt, options = {}) {
  const previousSnapshot = cacheEntry.snapshot;
  const reusedAgeMinutes = minutesBetween(parseDate(previousSnapshot.fetched_at), new Date(generatedAt));
  const freshness = sourceFreshnessFromSnapshot(previousSnapshot, source, cacheEntry.parsed, generatedAt);
  const priorNote = previousSnapshot.notes ? `${previousSnapshot.notes}. ` : "";
  const notePrefix = options.notePrefix ? `${options.notePrefix} ` : "";
  return {
    ...previousSnapshot,
    fetch_status: options.fetchStatus ?? "ok",
    parser_status: options.parserStatus ?? previousSnapshot.parser_status ?? "ok",
    failure_stage: options.failureStage ?? null,
    status: freshness.status,
    freshness_minutes: freshness.freshnessMinutes,
    notes: `${notePrefix}${priorNote}${options.reuseMessage}`.trim(),
    reused_in_run: true,
    reuse_reason: options.reuseReason ?? "cadence_window",
    reused_age_minutes: reusedAgeMinutes,
    duration_ms: options.durationMs ?? 0,
    raw_url: options.rawUrl ?? previousSnapshot.raw_url,
    last_successful_fetched_at: previousSnapshot.fetched_at
  };
}

function buildSkippedSnapshot(source, generatedAt, note) {
  return {
    source_id: source.id,
    name: source.name,
    owner: source.owner,
    category: source.category,
    fetched_at: generatedAt,
    issued_at: null,
    raw_url: source.url ?? source.path ?? null,
    fetch_status: "skipped",
    parser_status: "skipped",
    failure_stage: "selection",
    status: "offline",
    freshness_minutes: null,
    duration_ms: 0,
    notes: note,
    auth: source.auth,
    summary: {}
  };
}

async function loadRawContent(repoRoot, source, options) {
  if (options.useFixtures) {
    const fixtureFile = path.join(repoRoot, "fixtures", `${source.id}.${rawExtension(source.format)}`);
    return {
      ok: true,
      status: 200,
      text: await readFile(fixtureFile, "utf8"),
      fetchedFrom: "fixture"
    };
  }

  if (source.path) {
    const sourcePath = path.join(repoRoot, source.path);
    if (!(await pathExists(sourcePath))) {
      return { ok: false, status: 404, text: "", fetchedFrom: "local-file" };
    }
    return {
      ok: true,
      status: 200,
      text: await readFile(sourcePath, "utf8"),
      fetchedFrom: "local-file"
    };
  }

  if (source.id === "nasa-imerg-nrt") {
    const response = await fetchNasaImergPayload(source);
    return {
      ...response,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "imd-cap-rss") {
    const response = await fetchImdCapPayload(source);
    return {
      ...response,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "rainviewer-radar") {
    const response = await fetchRainviewerPayload(source);
    return {
      ...response,
      resolvedUrl: source.url,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "indiawris-rainfall") {
    const response = await fetchIndiaWrisRainfallPayload(repoRoot, source);
    return {
      ...response,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "indiawris-river-level") {
    const response = await fetchIndiaWrisRiverLevelPayload(repoRoot, source);
    return {
      ...response,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "cwc-ffs") {
    const response = await fetchCwcFfsPayload(repoRoot, source);
    return {
      ...response,
      fetchedFrom: "remote"
    };
  }

  if (source.id === "ksdma-reservoirs" || source.id === "ksdma-dam-management") {
    const response = await fetchKsdmaDailyDamPayload(source);
    return {
      ...response,
      resolvedUrl: source.url,
      fetchedFrom: "remote"
    };
  }

  const candidateUrls = [source.url, ...(source.fallback_urls ?? [])].filter(Boolean);
  let lastResponse = {
    ok: false,
    status: 502,
    text: "",
    resolvedUrl: source.url
  };

  for (const candidateUrl of candidateUrls) {
    const response = await fetchText(candidateUrl, { timeoutMs: 20000 });
    lastResponse = {
      ...response,
      resolvedUrl: candidateUrl
    };
    if (response.ok && response.text?.trim()) {
      return {
        ...lastResponse,
        fetchedFrom: "remote"
      };
    }
  }

  return {
    ...lastResponse,
    fetchedFrom: "remote"
  };
}

function normalizeRainfall(parsedSources, taluks) {
  const observationSource = parsedSources["operator-observations"] ?? { active: false, districts: [] };
  const imergSource = parsedSources["nasa-imerg-nrt"] ?? { districts: [], taluks: [] };
  const indiaWrisSource = parsedSources["indiawris-rainfall"] ?? { districts: [], taluks: [] };
  const districtRainfallMap = {};
  const talukRainfallMap = {};

  for (const entry of [...(imergSource.districts ?? []), ...(observationSource.active ? observationSource.districts ?? [] : [])]) {
    districtRainfallMap[entry.district_id] = {
      rain_1h_mm: entry.rain_1h_mm ?? 0,
      rain_3h_mm: entry.rain_3h_mm ?? 0,
      rain_6h_mm: entry.rain_6h_mm ?? 0,
      rain_24h_mm: entry.rain_24h_mm ?? 0,
      rain_3d_mm: entry.rain_3d_mm ?? 0,
      rain_7d_mm: entry.rain_7d_mm ?? 0,
      source: entry.source ?? (observationSource.active ? "operator" : "nasa-imerg"),
      spatial_aggregation: entry.spatial_aggregation ?? null,
      cell_count: entry.cell_count ?? null,
      peak_30m_mm: entry.peak_30m_mm ?? null
    };
  }

  for (const entry of imergSource.taluks ?? []) {
    talukRainfallMap[entry.taluk_id] = {
      rain_1h_mm: entry.rain_1h_mm ?? 0,
      rain_3h_mm: entry.rain_3h_mm ?? 0,
      rain_6h_mm: entry.rain_6h_mm ?? 0,
      rain_24h_mm: entry.rain_24h_mm ?? 0,
      rain_3d_mm: entry.rain_3d_mm ?? 0,
      rain_7d_mm: entry.rain_7d_mm ?? 0,
      source: entry.source ?? "nasa-imerg",
      district_id: entry.district_id ?? null,
      spatial_aggregation: entry.spatial_aggregation ?? null,
      cell_count: entry.cell_count ?? null,
      peak_30m_mm: entry.peak_30m_mm ?? null
    };
  }

  for (const entry of indiaWrisSource.districts ?? []) {
    districtRainfallMap[entry.district_id] = {
      ...(districtRainfallMap[entry.district_id] ?? {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        source: "indiawris-cwc"
      }),
      rain_24h_mm: entry.rain_24h_mm ?? districtRainfallMap[entry.district_id]?.rain_24h_mm ?? 0,
      rain_3d_mm: entry.rain_3d_mm ?? districtRainfallMap[entry.district_id]?.rain_3d_mm ?? 0,
      rain_7d_mm: entry.rain_7d_mm ?? districtRainfallMap[entry.district_id]?.rain_7d_mm ?? 0,
      source: districtRainfallMap[entry.district_id]?.source
        ? `${districtRainfallMap[entry.district_id].source}+indiawris-cwc`
        : "indiawris-cwc",
      spatial_aggregation: districtRainfallMap[entry.district_id]?.spatial_aggregation
        ? `${districtRainfallMap[entry.district_id].spatial_aggregation}+indiawris_station_mean`
        : "indiawris_station_mean",
      official_rain_24h_mm: entry.rain_24h_mm ?? null,
      official_rain_3d_mm: entry.rain_3d_mm ?? null,
      official_rain_7d_mm: entry.rain_7d_mm ?? null,
      official_station_count: entry.station_count ?? 0,
      official_peak_station_24h_mm: entry.max_station_24h_mm ?? null
    };
  }

  for (const entry of indiaWrisSource.taluks ?? []) {
    talukRainfallMap[entry.taluk_id] = {
      ...(talukRainfallMap[entry.taluk_id] ?? {
        rain_1h_mm: 0,
        rain_3h_mm: 0,
        rain_6h_mm: 0,
        source: "indiawris-cwc",
        district_id: entry.district_id ?? null
      }),
      rain_24h_mm: entry.rain_24h_mm ?? talukRainfallMap[entry.taluk_id]?.rain_24h_mm ?? 0,
      rain_3d_mm: entry.rain_3d_mm ?? talukRainfallMap[entry.taluk_id]?.rain_3d_mm ?? 0,
      rain_7d_mm: entry.rain_7d_mm ?? talukRainfallMap[entry.taluk_id]?.rain_7d_mm ?? 0,
      source: talukRainfallMap[entry.taluk_id]?.source
        ? `${talukRainfallMap[entry.taluk_id].source}+indiawris-cwc`
        : "indiawris-cwc",
      spatial_aggregation: talukRainfallMap[entry.taluk_id]?.spatial_aggregation
        ? `${talukRainfallMap[entry.taluk_id].spatial_aggregation}+indiawris_station_mean`
        : "indiawris_station_mean",
      official_rain_24h_mm: entry.rain_24h_mm ?? null,
      official_rain_3d_mm: entry.rain_3d_mm ?? null,
      official_rain_7d_mm: entry.rain_7d_mm ?? null,
      official_station_count: entry.station_count ?? 0,
      official_peak_station_24h_mm: entry.max_station_24h_mm ?? null
    };
  }

  for (const district of districts) {
    districtRainfallMap[district.id] ??= null;
  }

  for (const taluk of taluks) {
    talukRainfallMap[taluk.taluk_id] ??= null;
  }

  return {
    districts: districtRainfallMap,
    taluks: talukRainfallMap
  };
}

function collapseSignals(parsedSources) {
  const capByDistrict = {};
  for (const item of parsedSources["imd-cap-rss"]?.items ?? []) {
    for (const districtId of item.districts) {
      capByDistrict[districtId] ??= { severity: 0, items: [] };
      capByDistrict[districtId].severity = Math.max(capByDistrict[districtId].severity, item.severity);
      capByDistrict[districtId].items.push(item.title);
    }
  }

  const bulletinByDistrict = {};
  for (const districtId of parsedSources["imd-flash-flood-bulletin"]?.kerala_district_ids ?? []) {
    bulletinByDistrict[districtId] = {
      severity: parsedSources["imd-flash-flood-bulletin"].severity,
      notes: ["Flash flood bulletin references district"]
    };
  }

  const reservoirByDistrict = {};
  const reservoirSource = parsedSources["ksdma-reservoirs"];
  if (Array.isArray(reservoirSource?.districts) && reservoirSource.districts[0] && typeof reservoirSource.districts[0] === "object") {
    for (const district of reservoirSource.districts) {
      reservoirByDistrict[district.district_id] = {
        active: district.active ?? reservoirSource.alert_active ?? false,
        severity: district.severity ?? 0.35,
        notes: [district.summary_note ?? "KSDMA KSEB dam level context"]
      };
    }
  } else {
    for (const districtId of reservoirSource?.districts ?? []) {
      reservoirByDistrict[districtId] = {
        active: reservoirSource.alert_active,
        severity: reservoirSource.severity || 0.35,
        notes: ["KSDMA reservoir caution context"]
      };
    }
  }

  const damByDistrict = {};
  const damSource = parsedSources["ksdma-dam-management"];
  if (Array.isArray(damSource?.districts) && damSource.districts[0] && typeof damSource.districts[0] === "object") {
    for (const district of damSource.districts) {
      damByDistrict[district.district_id] = {
        active: district.active ?? damSource.release_preparedness ?? false,
        severity: district.severity ?? 0.38,
        notes: [district.summary_note ?? "KSDMA irrigation dam release context"]
      };
    }
  } else {
    for (const districtId of damSource?.districts ?? []) {
      damByDistrict[districtId] = {
        active: damSource.release_preparedness,
        severity: damSource.severity || 0.38,
        notes: ["KSDMA dam downstream notice"]
      };
    }
  }

  const cwcByDistrict = {};
  for (const district of parsedSources["cwc-ffs"]?.districts ?? []) {
    if (typeof district === "string") {
      cwcByDistrict[district] = {
        active: Boolean(parsedSources["cwc-ffs"].warning || parsedSources["cwc-ffs"].watch),
        severity: parsedSources["cwc-ffs"].warning ? 0.7 : parsedSources["cwc-ffs"].watch ? 0.4 : 0,
        notes: ["CWC flood forecasting signal"]
      };
      continue;
    }
    cwcByDistrict[district.district_id] = {
      active: (district.severity ?? 0) > 0,
      severity: district.severity ?? 0,
      notes: [district.summary_note ?? "CWC flood forecasting live river-level signal"].filter(Boolean)
    };
  }

  for (const district of parsedSources["indiawris-river-level"]?.districts ?? []) {
    const existing = cwcByDistrict[district.district_id] ?? {
      active: false,
      severity: 0,
      notes: []
    };
    const riverLevelText = district.summary_note ??
      (district.max_rise_m > 0
        ? `India-WRIS river level rise ${district.max_rise_m} m across ${district.station_count} station${district.station_count === 1 ? "" : "s"}`
        : `India-WRIS river level available from ${district.station_count} station${district.station_count === 1 ? "" : "s"} with no notable rise`);
    cwcByDistrict[district.district_id] = {
      active: existing.active || (district.severity ?? 0) > 0,
      severity: Math.max(existing.severity, district.severity ?? 0),
      notes: [riverLevelText, ...existing.notes].filter(Boolean)
    };
  }

  const radarByDistrict = {};
  for (const district of parsedSources["rainviewer-radar"]?.districts ?? []) {
    radarByDistrict[district.district_id] = {
      severity: district.severity ?? 0,
      intensity: district.intensity ?? "none",
      max_dbz: district.max_dbz ?? null,
      notes: district.intensity && district.intensity !== "none"
        ? [`RainViewer ${district.intensity.replaceAll("_", " ")} cell near district`]
        : ["No meaningful RainViewer radar echo near district"]
    };
  }

  const radarByHotspot = {};
  for (const hotspot of parsedSources["rainviewer-radar"]?.hotspots ?? []) {
    radarByHotspot[hotspot.hotspot_id] = {
      severity: hotspot.severity ?? 0,
      intensity: hotspot.intensity ?? "none",
      max_dbz: hotspot.max_dbz ?? null,
      notes: hotspot.intensity && hotspot.intensity !== "none"
        ? [`RainViewer ${hotspot.intensity.replaceAll("_", " ")} cell near hotspot`]
        : ["No meaningful RainViewer radar echo near hotspot"]
    };
  }

  return {
    capByDistrict,
    bulletinByDistrict,
    reservoirByDistrict,
    damByDistrict,
    cwcByDistrict,
    radarByDistrict,
    radarByHotspot
  };
}

function buildNasaImergHistoryEntry(generatedAt, snapshot, parsedSource) {
  if (!snapshot) {
    return null;
  }

  const summary = snapshot.summary ?? {};
  return {
    generated_at: generatedAt,
    issued_at: snapshot.issued_at ?? parsedSource?.issued_at ?? null,
    status: snapshot.status,
    parser_status: snapshot.parser_status,
    freshness_minutes: snapshot.freshness_minutes,
    notes: snapshot.notes ?? "",
    district_count: summary.district_count ?? 0,
    taluk_count: summary.taluk_count ?? 0,
    latest_half_hour_file: summary.latest_half_hour_file ?? null,
    latest_three_hour_file: summary.latest_three_hour_file ?? null,
    latest_daily_file: summary.latest_daily_file ?? null
  };
}

async function loadTalukDefinitions(repoRoot, options) {
  const localTalukLayer = await readJson(
    path.join(repoRoot, "src", "site", "assets", "kerala-taluks.geojson"),
    { type: "FeatureCollection", features: [] }
  );

  let talukBoundaries = parseTalukBoundaries(localTalukLayer);

  if (!options.useFixtures) {
    try {
      talukBoundaries = await loadTalukBoundaries();
    } catch {
      talukBoundaries = parseTalukBoundaries(localTalukLayer);
    }
  }

  return talukBoundaries.map((taluk) => ({
    taluk_id: taluk.taluk_id,
    district_id: taluk.district_id,
    name: taluk.name,
    district_name: taluk.district_name,
    centroid: taluk.centroid,
    bbox: taluk.bbox,
    hotspot_ids: hotspots
      .filter((hotspot) =>
        hotspot.location
          ? pointInGeometry([hotspot.location.lon, hotspot.location.lat], taluk.geometry)
          : false
      )
      .map((hotspot) => hotspot.id)
  }));
}

export async function runPipeline(repoRoot, options = {}) {
  const generatedAt = nowIso();
  const sources = await readJson(path.join(repoRoot, "config", "sources.json"));
  const thresholds = await readJson(path.join(repoRoot, "config", "risk-thresholds.json"));
  const terrainStats = await readJson(path.join(repoRoot, "config", "terrain-stats.json"), {
    districts: [],
    normalization: {
      method: "blend_manual_and_dem",
      manual_weight: 0.4,
      dem_weight: 0.6,
      dem_reference_max: 100
    }
  });
  const approvalsDocument = await readJson(path.join(repoRoot, "data", "manual", "review-approvals.json"), {
    approvals: []
  });
  const hotspotOverridesDocument = await readJson(
    path.join(repoRoot, "data", "manual", "hotspot-overrides.json"),
    { overrides: [] }
  );
  const taluks = await loadTalukDefinitions(repoRoot, options);

  const archiveParts = toArchivePathParts(new Date(generatedAt));
  let boundaryMetadata = {
    sources: boundaryLayerSources,
    counts: {
      state: 0,
      district: districts.length,
      taluk: taluks.length
    },
    districts: districts.map((district) => ({
      district_id: district.id,
      name: district.name,
      centroid: null,
      bbox: null
    })),
    taluks: taluks.map((taluk) => ({
      taluk_id: taluk.taluk_id,
      district_id: taluk.district_id,
      name: taluk.name,
      centroid: taluk.centroid,
      bbox: taluk.bbox,
      hotspot_count: taluk.hotspot_ids.length
    }))
  };

  if (!options.useFixtures) {
    try {
      boundaryMetadata = await buildBoundaryMetadata();
    } catch {
      boundaryMetadata = {
        ...boundaryMetadata,
        note: "Boundary metadata fetch failed. Static district definitions remain available."
      };
    }
  }

  const rawDir = path.join(
    repoRoot,
    "runtime",
    "raw",
    `${archiveParts.year}${archiveParts.month}${archiveParts.day}`,
    archiveParts.stamp
  );
  await ensureDir(rawDir);

  const parsedSources = {};
  const priorSourceCache = options.useFixtures
    ? { sources: {} }
    : await readJson(sourceCachePath(repoRoot), { sources: {} });
  const selectedSourceIds = Array.isArray(options.sourceIds) && options.sourceIds.length
    ? new Set(options.sourceIds)
    : null;
  const enabledSources = sources.filter((entry) => entry.enabled);
  const sourceFetchConcurrency = options.sourceFetchConcurrency ?? 3;
  const sourceResults = await mapWithConcurrency(
    enabledSources,
    async (source) => {
      const cacheEntry = priorSourceCache.sources?.[source.id];
      const sourceSelected = !selectedSourceIds || selectedSourceIds.has(source.id);
      if (!sourceSelected && !options.useFixtures) {
        if (cacheEntryHasSuccessfulPayload(cacheEntry)) {
          return {
            sourceId: source.id,
            parsed: cacheEntry.parsed,
            snapshot: buildReusedSnapshot(source, cacheEntry, generatedAt, {
              fetchStatus: "skipped_cached",
              parserStatus: "ok",
              failureStage: null,
              reuseReason: "source_selection",
              reuseMessage: "Source was not selected for this run. Reused last successful cached payload."
            }),
            cacheUpdate: null
          };
        }
        return {
          sourceId: source.id,
          parsed: null,
          snapshot: buildSkippedSnapshot(
            source,
            generatedAt,
            "Source was not selected for this run and no successful cached payload was available."
          ),
          cacheUpdate: null
        };
      }

      if (!options.useFixtures && canReuseSource(source, cacheEntry, generatedAt)) {
        return {
          sourceId: source.id,
          parsed: cacheEntry.parsed,
          snapshot: buildReusedSnapshot(source, cacheEntry, generatedAt, {
            reuseMessage: `Reused last successful fetch within ${source.cadence_minutes}-minute cadence window.`
          }),
          cacheUpdate: null
        };
      }

      const parser = parserRegistry[source.parser];
      const fetchedAt = nowIso();
      const startedAtMs = Date.now();
      let raw = "";
      let parsed = null;
      let fetchOk = false;
      let parserOk = false;
      let issuedAt = null;
      let note = "";
      let resolvedUrl = source.url ?? source.path;

      try {
        const response = await loadRawContent(repoRoot, source, options);
        fetchOk = response.ok;
        raw = response.text ?? "";
        note = response.note ?? "";
        resolvedUrl = response.resolvedUrl ?? resolvedUrl;

        if ((source.fallback_urls?.length ?? 0) > 0 && resolvedUrl && resolvedUrl !== source.url) {
          note = note ? `${note} Using fallback feed ${resolvedUrl}` : `Using fallback feed ${resolvedUrl}`;
        }

        if (raw) {
          parsed =
            source.path || source.id === "imd-cap-rss"
              ? await parser(repoRoot, source, raw)
              : await parser(raw);
          parserOk = true;
          issuedAt = parsed?.issued_at ?? parsed?.published_at ?? parsed?.items?.[0]?.published_at ?? null;
          if (options.useFixtures) {
            issuedAt = new Date(
              new Date(generatedAt).getTime() - Math.max(5, source.cadence_minutes) * 60000
            ).toISOString();
          }
        }
      } catch (error) {
        note = error instanceof Error ? error.message : String(error);
      }

      const durationMs = Date.now() - startedAtMs;

      if ((!fetchOk || !parserOk) && cacheEntryHasSuccessfulPayload(cacheEntry)) {
        const reuseReason = fetchOk ? "parse_failure" : "fetch_failure";
        const reuseMessage = fetchOk
          ? "Parser failed in this run. Reused last successful cached payload."
          : "Fetch failed in this run. Reused last successful cached payload.";
        return {
          sourceId: source.id,
          parsed: cacheEntry.parsed,
          snapshot: buildReusedSnapshot(source, cacheEntry, generatedAt, {
            fetchStatus: fetchOk ? "ok" : "failed_cached",
            parserStatus: fetchOk ? "failed_cached" : "ok",
            failureStage: fetchOk ? "parse" : "fetch",
            reuseReason,
            reuseMessage,
            notePrefix: note,
            durationMs,
            rawUrl: resolvedUrl ?? source.url ?? source.path
          }),
          cacheUpdate: null
        };
      }

      const issuedDate = parseDate(issuedAt);
      const freshnessMinutes = minutesBetween(issuedDate, new Date(generatedAt));
      const baseStatus = statusFromFreshness(freshnessMinutes, source, fetchOk, parserOk);
      const status = applyCoverageStatus(baseStatus, parsed);
      const fetchStatus = fetchOk ? "ok" : "failed";
      const parserStatus = parserStatusFromState(fetchOk, parserOk);
      const failureStage = failureStageFromState(fetchOk, parserOk, parsed);

      if (raw) {
        const outputName = `${source.id}.${rawExtension(source.format)}`;
        await writeText(path.join(rawDir, outputName), raw);
      }

      return {
        sourceId: source.id,
        parsed,
        snapshot: {
          source_id: source.id,
          name: source.name,
          owner: source.owner,
          category: source.category,
          fetched_at: fetchedAt,
          issued_at: issuedDate?.toISOString() ?? null,
          raw_url: resolvedUrl ?? source.url ?? source.path,
          fetch_status: fetchStatus,
          parser_status: parserStatus,
          failure_stage: failureStage,
          status,
          freshness_minutes: freshnessMinutes,
          duration_ms: durationMs,
          notes: note,
          auth: source.auth,
          summary: summarizeSource(parsed)
        },
        cacheUpdate:
          fetchOk && parserOk
            ? {
                snapshot: {
                  source_id: source.id,
                  name: source.name,
                  owner: source.owner,
                  category: source.category,
                  fetched_at: fetchedAt,
                  issued_at: issuedDate?.toISOString() ?? null,
                  raw_url: resolvedUrl ?? source.url ?? source.path,
                  fetch_status: fetchStatus,
                  parser_status: parserStatus,
                  failure_stage: failureStage,
                  status,
                  freshness_minutes: freshnessMinutes,
                  duration_ms: durationMs,
                  notes: note,
                  auth: source.auth,
                  summary: summarizeSource(parsed)
                },
                parsed
              }
            : null
      };
    },
    sourceFetchConcurrency
  );

  const snapshots = sourceResults.map((result) => {
    parsedSources[result.sourceId] = result.parsed;
    return result.snapshot;
  });
  const sourceCache = {
    generated_at: generatedAt,
    sources: {
      ...(priorSourceCache.sources ?? {}),
      ...Object.fromEntries(
        sourceResults
          .filter((result) => result.cacheUpdate)
          .map((result) => [result.sourceId, result.cacheUpdate])
      )
    }
  };

  const signalMaps = collapseSignals(parsedSources);
  const rainfall = normalizeRainfall(parsedSources, taluks);
  const imergSummary = parsedSources["nasa-imerg-nrt"]?.source_files
    ? {
        issued_at: parsedSources["nasa-imerg-nrt"].issued_at ?? null,
        latest_half_hour_file:
          parsedSources["nasa-imerg-nrt"].source_files.half_hour?.[0]?.split("/").pop() ?? null,
        latest_three_hour_file:
          parsedSources["nasa-imerg-nrt"].source_files.three_hour?.[0]?.split("/").pop() ?? null,
        latest_daily_file:
          parsedSources["nasa-imerg-nrt"].source_files.daily?.[0]?.split("/").pop() ?? null
      }
    : null;
  const rainviewerSummary = parsedSources["rainviewer-radar"]
    ? {
        issued_at: parsedSources["rainviewer-radar"].issued_at ?? null,
        latest_frame_time: parsedSources["rainviewer-radar"].frame_time ?? null,
        hotspot_count: parsedSources["rainviewer-radar"].hotspots?.length ?? 0
      }
    : null;
  const indiaWrisRainfallSummary = parsedSources["indiawris-rainfall"]
    ? {
        issued_at: parsedSources["indiawris-rainfall"].issued_at ?? null,
        district_count: parsedSources["indiawris-rainfall"].districts?.length ?? 0,
        taluk_count: parsedSources["indiawris-rainfall"].taluks?.length ?? 0,
        station_count: parsedSources["indiawris-rainfall"].station_count ?? 0
      }
    : null;
  const indiaWrisRiverLevelSummary = parsedSources["indiawris-river-level"]
    ? {
        issued_at: parsedSources["indiawris-river-level"].issued_at ?? null,
        district_count: parsedSources["indiawris-river-level"].districts?.length ?? 0
      }
    : null;
  const freshnessBySource = Object.fromEntries(
    snapshots.map((source) => [source.source_id, source.freshness_minutes])
  );
  const statusBySource = Object.fromEntries(snapshots.map((source) => [source.source_id, source.status]));

  const { districtStates, talukStates, hotspotStates, alerts } = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: snapshots,
    taluks,
    ...signalMaps,
    rainfallByDistrict: rainfall.districts,
    rainfallByTaluk: rainfall.taluks,
    terrainStats,
    approvals: approvalsDocument.approvals ?? [],
    hotspotOverrides: hotspotOverridesDocument.overrides ?? [],
    freshnessBySource,
    statusBySource
  });

  const publicLatestDir = path.join(repoRoot, "docs", "data", "latest");
  const archiveRootDir = path.join(repoRoot, "docs", "data", "archive");
  const archiveDir = path.join(
    archiveRootDir,
    archiveParts.year,
    archiveParts.month,
    archiveParts.day,
    archiveParts.stamp
  );
  const runtimeDerivedDir = path.join(repoRoot, "runtime", "derived", "latest");
  await ensureDir(publicLatestDir);
  await ensureDir(archiveDir);
  await ensureDir(runtimeDerivedDir);

  const outputs = {
    "sources.json": {
      generated_at: generatedAt,
      sources: snapshots
    },
    "district-risk.json": {
      generated_at: generatedAt,
      districts: districtStates
    },
    "hotspot-risk.json": {
      generated_at: generatedAt,
      hotspots: hotspotStates
    },
    "taluk-risk.json": {
      generated_at: generatedAt,
      taluks: talukStates
    },
    "alerts.json": {
      generated_at: generatedAt,
      alerts
    },
    "observation-grid.json": {
      generated_at: generatedAt,
      spatial_aggregation: "district_and_taluk_polygon_mean_with_district_fallback_points",
      source_metadata: {
        nasa_imerg: imergSummary,
        rainviewer_radar: rainviewerSummary,
        indiawris_rainfall: indiaWrisRainfallSummary,
        indiawris_river_level: indiaWrisRiverLevelSummary
      },
      observations: {
        districts: rainfall.districts,
        taluks: rainfall.taluks
      }
    },
    "radar-nowcast.json": {
      generated_at: generatedAt,
      issued_at: parsedSources["rainviewer-radar"]?.issued_at ?? null,
      frame_time: parsedSources["rainviewer-radar"]?.frame_time ?? null,
      frame_path: parsedSources["rainviewer-radar"]?.frame_path ?? null,
      color_scheme: parsedSources["rainviewer-radar"]?.color_scheme ?? null,
      districts: parsedSources["rainviewer-radar"]?.districts ?? [],
      hotspots: parsedSources["rainviewer-radar"]?.hotspots ?? []
    },
    "admin-areas.json": {
      generated_at: generatedAt,
      boundaries: boundaryMetadata
    },
    "dashboard.json": {
      generated_at: generatedAt,
      headline_level: alerts[0]?.level ?? "Normal",
      headline_message:
        alerts[0]?.message_en ?? "No active Watch or higher alerts. Continue routine Kerala monsoon monitoring.",
      mode: "decision-support",
      severe_pending_count: alerts.filter((alert) => alert.review_state === "pending_review").length,
      terrain_model: terrainStats.source ?? "manual_baseline_only"
    }
  };

  const nasaSnapshot = snapshots.find((source) => source.source_id === "nasa-imerg-nrt");
  const nasaHistoryEntry = buildNasaImergHistoryEntry(
    generatedAt,
    nasaSnapshot,
    parsedSources["nasa-imerg-nrt"]
  );
  const nasaHistoryRuntimePath = path.join(repoRoot, "runtime", "metrics", "nasa-imerg-history.json");
  const nasaHistory = await readJson(nasaHistoryRuntimePath, { runs: [] });
  if (nasaHistoryEntry) {
    nasaHistory.runs = [nasaHistoryEntry, ...(nasaHistory.runs ?? [])]
      .filter(
        (run, index, allRuns) =>
          allRuns.findIndex((candidate) => candidate.generated_at === run.generated_at) === index
      )
      .slice(0, 2000);
  }

  const archiveIndexPath = path.join(archiveRootDir, "index.json");
  const archiveIndex = await readJson(archiveIndexPath, { runs: [] });
  archiveIndex.runs = [
    {
      generated_at: generatedAt,
      headline_level: outputs["dashboard.json"].headline_level,
      headline_message: outputs["dashboard.json"].headline_message,
      severe_pending_count: outputs["dashboard.json"].severe_pending_count,
      path: `./data/archive/${archiveParts.year}/${archiveParts.month}/${archiveParts.day}/${archiveParts.stamp}`
    },
    ...(archiveIndex.runs ?? [])
  ]
    .filter(
      (run, index, allRuns) => allRuns.findIndex((candidate) => candidate.generated_at === run.generated_at) === index
    )
    .slice(0, 120);

  for (const [fileName, data] of Object.entries(outputs)) {
    await writeJson(path.join(publicLatestDir, fileName), data);
    await writeJson(path.join(archiveDir, fileName), data);
    await writeJson(path.join(runtimeDerivedDir, fileName), data);
  }

  await writeJson(archiveIndexPath, archiveIndex);
  await writeJson(path.join(publicLatestDir, "archive-index.json"), archiveIndex);
  await writeJson(path.join(publicLatestDir, "nasa-imerg-history.json"), nasaHistory);
  await writeJson(path.join(runtimeDerivedDir, "archive-index.json"), archiveIndex);
  await writeJson(path.join(runtimeDerivedDir, "nasa-imerg-history.json"), nasaHistory);
  await writeJson(nasaHistoryRuntimePath, nasaHistory);

  await writeJson(path.join(repoRoot, "runtime", "metrics", "latest-run.json"), {
    generated_at: generatedAt,
    source_count: snapshots.length,
    online_sources: snapshots.filter((source) => source.status === "ok").length,
    severe_pending_count: outputs["dashboard.json"].severe_pending_count,
    total_source_duration_ms: snapshots.reduce((sum, source) => sum + (source.duration_ms ?? 0), 0),
    slowest_sources: [...snapshots]
      .sort((left, right) => (right.duration_ms ?? 0) - (left.duration_ms ?? 0))
      .slice(0, 5)
      .map((source) => ({
        source_id: source.source_id,
        duration_ms: source.duration_ms ?? 0,
        status: source.status,
        fetch_status: source.fetch_status,
        parser_status: source.parser_status
      }))
  });
  await writeJson(sourceCachePath(repoRoot), sourceCache);

  return outputs;
}
