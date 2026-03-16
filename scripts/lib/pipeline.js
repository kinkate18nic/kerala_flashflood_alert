import path from "node:path";
import { readFile } from "node:fs/promises";
import { readJson, writeJson, writeText, ensureDir, pathExists } from "./fs.js";
import { buildBoundaryMetadata, boundaryLayerSources } from "./boundaries.js";
import { fetchText } from "./http.js";
import { fetchNasaImergPayload } from "./imerg.js";
import { parserRegistry } from "./parsers.js";
import { minutesBetween, nowIso, parseDate, toArchivePathParts } from "./time.js";
import { buildRiskOutputs } from "./risk-model.js";
import { districts } from "../../src/shared/areas.js";

function statusFromFreshness(freshnessMinutes, slaMinutes, fetchOk, parserOk) {
  if (!fetchOk || !parserOk) {
    return "offline";
  }
  if (freshnessMinutes === null) {
    return "degraded";
  }
  if (freshnessMinutes > slaMinutes * 2) {
    return "offline";
  }
  if (freshnessMinutes > slaMinutes) {
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
    return { item_count: parsed.item_count, district_count: parsed.kerala_district_ids?.length ?? 0 };
  }
  if ("districts" in parsed && Array.isArray(parsed.districts)) {
    return { district_count: parsed.districts.length };
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

  const response = await fetchText(source.url, { timeoutMs: 20000 });
  return {
    ...response,
    fetchedFrom: "remote"
  };
}

function normalizeRainfall(parsedSources) {
  const observationSource = parsedSources["operator-observations"] ?? { active: false, districts: [] };
  const imergSource = parsedSources["nasa-imerg-nrt"] ?? { districts: [] };
  const rainfallMap = {};

  for (const entry of [...(imergSource.districts ?? []), ...(observationSource.active ? observationSource.districts ?? [] : [])]) {
    rainfallMap[entry.district_id] = {
      rain_1h_mm: entry.rain_1h_mm ?? 0,
      rain_3h_mm: entry.rain_3h_mm ?? 0,
      rain_6h_mm: entry.rain_6h_mm ?? 0,
      rain_24h_mm: entry.rain_24h_mm ?? 0,
      rain_3d_mm: entry.rain_3d_mm ?? 0,
      rain_7d_mm: entry.rain_7d_mm ?? 0,
      source: entry.source ?? (observationSource.active ? "operator" : "nasa-imerg")
    };
  }

  for (const district of districts) {
    rainfallMap[district.id] ??= null;
  }

  return rainfallMap;
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

  return {
    capByDistrict,
    bulletinByDistrict,
    reservoirByDistrict,
    damByDistrict,
    cwcByDistrict
  };
}

export async function runPipeline(repoRoot, options = {}) {
  const generatedAt = nowIso();
  const sources = await readJson(path.join(repoRoot, "config", "sources.json"));
  const thresholds = await readJson(path.join(repoRoot, "config", "risk-thresholds.json"));
  const approvalsDocument = await readJson(path.join(repoRoot, "data", "manual", "review-approvals.json"), {
    approvals: []
  });
  const hotspotOverridesDocument = await readJson(
    path.join(repoRoot, "data", "manual", "hotspot-overrides.json"),
    { overrides: [] }
  );

  const archiveParts = toArchivePathParts(new Date(generatedAt));
  let boundaryMetadata = {
    sources: boundaryLayerSources,
    counts: {
      state: 0,
      district: districts.length,
      taluk: 0
    },
    districts: districts.map((district) => ({
      district_id: district.id,
      name: district.name,
      centroid: null,
      bbox: null
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

    try {
      const response = await loadRawContent(repoRoot, source, options);
      fetchOk = response.ok;
      raw = response.text ?? "";
      note = response.note ?? "";

      if (raw) {
        parsed = source.path ? await parser(repoRoot, source, raw) : parser(raw);
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
    const status = statusFromFreshness(freshnessMinutes, source.freshness_sla_minutes, fetchOk, parserOk);

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
      raw_url: source.url ?? source.path,
      parser_status: parserOk ? "ok" : "failed",
      status,
      freshness_minutes: freshnessMinutes,
      notes: note,
      auth: source.auth,
      summary: summarizeSource(parsed)
    });
  }

  const signalMaps = collapseSignals(parsedSources);
  const rainfallByDistrict = normalizeRainfall(parsedSources);
  const freshnessBySource = Object.fromEntries(
    snapshots.map((source) => [source.source_id, source.freshness_minutes])
  );
  const statusBySource = Object.fromEntries(snapshots.map((source) => [source.source_id, source.status]));

  const { districtStates, hotspotStates, alerts } = buildRiskOutputs({
    generatedAt,
    thresholds,
    sourceSnapshots: snapshots,
    ...signalMaps,
    rainfallByDistrict,
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
    "alerts.json": {
      generated_at: generatedAt,
      alerts
    },
    "observation-grid.json": {
      generated_at: generatedAt,
      spatial_aggregation: "district_polygon_mean_or_fallback_point",
      observations: rainfallByDistrict
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
      severe_pending_count: alerts.filter((alert) => alert.review_state === "pending_review").length
    }
  };

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
  await writeJson(path.join(runtimeDerivedDir, "archive-index.json"), archiveIndex);

  await writeJson(path.join(repoRoot, "runtime", "metrics", "latest-run.json"), {
    generated_at: generatedAt,
    source_count: snapshots.length,
    online_sources: snapshots.filter((source) => source.status === "ok").length,
    severe_pending_count: outputs["dashboard.json"].severe_pending_count
  });

  return outputs;
}
