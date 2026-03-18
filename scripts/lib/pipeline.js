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
import { fetchImdCapPayload } from "./imd-cap.js";
import { fetchNasaImergPayload } from "./imerg.js";
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
  for (const districtId of parsedSources["ksdma-reservoirs"]?.districts ?? []) {
    reservoirByDistrict[districtId] = {
      active: parsedSources["ksdma-reservoirs"].alert_active,
      severity: parsedSources["ksdma-reservoirs"].severity || 0.35,
      notes: ["KSDMA reservoir caution context"]
    };
  }

  const damByDistrict = {};
  for (const districtId of parsedSources["ksdma-dam-management"]?.districts ?? []) {
    damByDistrict[districtId] = {
      active: parsedSources["ksdma-dam-management"].release_preparedness,
      severity: parsedSources["ksdma-dam-management"].severity || 0.38,
      notes: ["KSDMA dam downstream notice"]
    };
  }

  const cwcByDistrict = {};
  for (const districtId of parsedSources["cwc-ffs"]?.districts ?? []) {
    cwcByDistrict[districtId] = {
      active: Boolean(parsedSources["cwc-ffs"].warning || parsedSources["cwc-ffs"].watch),
      severity: parsedSources["cwc-ffs"].warning ? 0.7 : parsedSources["cwc-ffs"].watch ? 0.4 : 0,
      notes: ["CWC flood forecasting signal"]
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
  const snapshots = [];

  for (const source of sources.filter((entry) => entry.enabled)) {
    const parser = parserRegistry[source.parser];
    const fetchedAt = nowIso();
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

    const issuedDate = parseDate(issuedAt);
    const freshnessMinutes = minutesBetween(issuedDate, new Date(generatedAt));
    const status = statusFromFreshness(freshnessMinutes, source, fetchOk, parserOk);

    if (raw) {
      const outputName = `${source.id}.${rawExtension(source.format)}`;
      await writeText(path.join(rawDir, outputName), raw);
    }

    parsedSources[source.id] = parsed;
    snapshots.push({
      source_id: source.id,
      name: source.name,
      owner: source.owner,
      category: source.category,
      fetched_at: fetchedAt,
      issued_at: issuedDate?.toISOString() ?? null,
      raw_url: resolvedUrl ?? source.url ?? source.path,
      parser_status: parserOk ? "ok" : "failed",
      status,
      freshness_minutes: freshnessMinutes,
      notes: note,
      auth: source.auth,
      summary: summarizeSource(parsed)
    });
  }

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
        rainviewer_radar: rainviewerSummary
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
    severe_pending_count: outputs["dashboard.json"].severe_pending_count
  });

  return outputs;
}
