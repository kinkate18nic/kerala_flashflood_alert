import path from "node:path";
import { districts, districtLookup } from "../../src/shared/areas.js";
import {
  parseDistrictBoundaries,
  parseTalukBoundaries,
  pointInGeometry,
  talukIdFromBoundaryNames
} from "./boundaries.js";
import { readJson } from "./fs.js";

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function buildQueryUrl(baseUrl, parameters) {
  const outerUrl = new URL(baseUrl);
  const innerRaw = outerUrl.searchParams.get("url");

  // If this is a proxy URL (?url=...), append query params to the INNER target URL
  if (innerRaw) {
    const innerUrl = new URL(innerRaw);
    Object.entries(parameters).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") {
        innerUrl.searchParams.set(key, String(value));
      }
    });
    outerUrl.searchParams.set("url", innerUrl.toString());
    return outerUrl.toString();
  }

  // Otherwise, standard direct URL
  Object.entries(parameters).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      outerUrl.searchParams.set(key, String(value));
    }
  });
  return outerUrl.toString();
}

import { fetchText } from "./http.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, { timeoutMs = 60000, retries = 3 } = {}) {
  let attempt = 0;
  let lastFailure = null;

  while (attempt < retries) {
    try {
      const response = await fetchText(url, {
        method: "POST",
        timeoutMs,
        headers: {
          accept: "application/json, text/plain, */*",
          origin: "https://indiawris.gov.in",
          referer: "https://indiawris.gov.in/swagger-ui/index.html"
        }
      });

      const payload = response.text ? JSON.parse(response.text) : null;
      return {
        ...response,
        json: payload
      };
    } catch (error) {
      lastFailure = error;
      attempt += 1;
      if (attempt >= retries) {
        break;
      }
      await sleep(600 * attempt);
    }
  }

  throw lastFailure ?? new Error("India-WRIS request failed");
}

async function mapWithConcurrency(items, worker, concurrency = 1) {
  const results = new Array(items.length);
  let index = 0;

  async function runWorker() {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker()));
  return results;
}

function safeNumber(value) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeStationName(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

const stationContextCache = new Map();

async function loadStationContext(repoRoot) {
  if (!stationContextCache.has(repoRoot)) {
    stationContextCache.set(repoRoot, (async () => {
      const [registryDocument, thresholdDocument, districtLayer, talukLayer] = await Promise.all([
        readJson(path.join(repoRoot, "data", "manual", "indiawris-stations.json"), {
          stations: []
        }),
        readJson(path.join(repoRoot, "data", "manual", "indiawris-river-thresholds.json"), {
          stations: []
        }),
        readJson(path.join(repoRoot, "src", "site", "assets", "kerala-districts.geojson"), {
          type: "FeatureCollection",
          features: []
        }),
        readJson(path.join(repoRoot, "src", "site", "assets", "kerala-taluks.geojson"), {
          type: "FeatureCollection",
          features: []
        })
      ]);

      const byCode = new Map();
      const byName = new Map();
      for (const station of registryDocument.stations ?? []) {
        if (station.station_code) {
          byCode.set(normalizeStationName(station.station_code), station);
        }
        const names = [station.station_name, ...(station.aliases ?? [])].filter(Boolean);
        for (const name of names) {
          byName.set(normalizeStationName(name), station);
        }
      }

      const thresholdByCode = new Map();
      const thresholdByName = new Map();
      for (const station of thresholdDocument.stations ?? []) {
        if (station.station_code) {
          thresholdByCode.set(normalizeStationName(station.station_code), station);
        }
        const names = [station.station_name, ...(station.aliases ?? [])].filter(Boolean);
        for (const name of names) {
          thresholdByName.set(normalizeStationName(name), station);
        }
      }

      return {
        registry: {
          stations: registryDocument.stations ?? [],
          byCode,
          byName
        },
        thresholds: {
          stations: thresholdDocument.stations ?? [],
          byCode: thresholdByCode,
          byName: thresholdByName
        },
        districts: parseDistrictBoundaries(districtLayer),
        taluks: parseTalukBoundaries(talukLayer)
      };
    })());
  }

  return stationContextCache.get(repoRoot);
}

function findStationRegistration(registry, row) {
  const stationCode = normalizeStationName(row.stationCode);
  const stationName = normalizeStationName(row.stationName);
  return registry.byCode.get(stationCode) ?? registry.byName.get(stationName) ?? null;
}

function findStationThreshold(thresholds, row, registration = null) {
  const stationCode = normalizeStationName(row.stationCode ?? registration?.station_code);
  const stationName = normalizeStationName(row.stationName ?? registration?.station_name);
  return thresholds.byCode.get(stationCode) ?? thresholds.byName.get(stationName) ?? null;
}

function locateAdminAreaByPoint(context, lon, lat) {
  const point = [lon, lat];
  const district = context.districts.find((entry) => pointInGeometry(point, entry.geometry)) ?? null;
  const taluk = context.taluks.find((entry) => pointInGeometry(point, entry.geometry)) ?? null;
  return {
    district_id: district?.district_id ?? null,
    taluk_id: taluk?.taluk_id ?? null
  };
}

function enrichStationRow(row, fallbackDistrict, context) {
  const registration = findStationRegistration(context.registry, row);
  const lat = safeNumber(row.latitude ?? row.lat ?? registration?.lat);
  const lon = safeNumber(row.longitude ?? row.lon ?? registration?.lon);

  let districtId = registration?.district_id ?? null;
  let talukId = registration?.taluk_id ?? null;

  if (lat !== null && lon !== null) {
    const spatialMatch = locateAdminAreaByPoint(context, lon, lat);
    districtId = spatialMatch.district_id ?? districtId;
    talukId = spatialMatch.taluk_id ?? talukId;
  }

  districtId ??= fallbackDistrict.id;
  talukId ??= talukIdFromBoundaryNames(fallbackDistrict.name, row.tehsil ?? row.block ?? "");

  return {
    ...row,
    district_id: districtId,
    taluk_id: talukId,
    station_code: row.stationCode ?? registration?.station_code ?? null,
    station_name: row.stationName ?? registration?.station_name ?? null,
    station_lat: lat,
    station_lon: lon,
    station_mapping: registration ? "registry" : lat !== null && lon !== null ? "coordinates" : "tehsil_fallback"
  };
}

function computeRiseSeverity(riseMetres) {
  if (riseMetres >= 1) {
    return 0.7;
  }
  if (riseMetres >= 0.5) {
    return 0.45;
  }
  if (riseMetres >= 0.25) {
    return 0.25;
  }
  return 0;
}

function classifyThresholdSeverity(latestLevel, riseMetres, threshold) {
  const warning = safeNumber(threshold?.warning_level_m);
  const danger = safeNumber(threshold?.danger_level_m);
  if (warning === null || danger === null || danger <= warning) {
    return null;
  }

  const span = Math.max(danger - warning, 0.01);
  const ratio = (latestLevel - warning) / span;
  const clampedRatio = Math.max(-1, Math.min(2, ratio));
  let levelStatus = "below_warning";
  let severity = 0;

  if (latestLevel >= danger) {
    levelStatus = "above_danger";
    severity = 1;
  } else if (latestLevel >= warning) {
    levelStatus = "above_warning";
    severity = 0.65 + Math.max(0, ratio) * 0.25;
  } else if (latestLevel >= warning - 0.5 * span) {
    levelStatus = "approaching_warning";
    severity = 0.25;
  }

  if (levelStatus !== "below_warning" && riseMetres >= 0.5) {
    severity = Math.min(1, severity + 0.08);
  }

  return {
    severity: Number(Math.min(1, Math.max(0, severity)).toFixed(2)),
    threshold_ratio: Number(clampedRatio.toFixed(2)),
    level_status: levelStatus
  };
}

function latestTimestamp(records) {
  const timestamps = records
    .map((record) => new Date(record.dataTime).getTime())
    .filter((value) => Number.isFinite(value));
  if (!timestamps.length) {
    return null;
  }
  return new Date(Math.max(...timestamps)).toISOString();
}

async function fetchDistrictPages(source, district, queryBuilder, options = {}) {
  const {
    pageSize = 100,
    maxPages = 10
  } = options;

  const collectedRows = [];
  let page = 0;
  let latestResponse = null;
  let latestUrl = null;

  while (page < maxPages) {
    const parameters = queryBuilder(district, page, pageSize);
    latestUrl = buildQueryUrl(source.url, parameters);
    latestResponse = await fetchJson(latestUrl);

    if (!latestResponse.ok) {
      return {
        district,
        url: latestUrl,
        response: latestResponse
      };
    }

    const rows = Array.isArray(latestResponse.json?.data) ? latestResponse.json.data : [];
    collectedRows.push(...rows);

    if (rows.length < pageSize) {
      break;
    }

    page += 1;
    await sleep(250);
  }

  return {
    district,
    url: latestUrl,
    response: {
      ok: true,
      status: latestResponse?.status ?? 200,
      json: {
        data: collectedRows
      }
    }
  };
}

function aggregateRainfallSeries(records) {
  const byDate = new Map();

  for (const record of records) {
    const value = safeNumber(record.dataValue);
    if (value === null) {
      continue;
    }
    const dateKey = String(record.dataTime ?? "").slice(0, 10);
    if (!dateKey) {
      continue;
    }

    const bucket = byDate.get(dateKey) ?? {
      values: [],
      max: 0,
      stationNames: new Set()
    };
    bucket.values.push(value);
    bucket.max = Math.max(bucket.max, value);
    if (record.stationName) {
      bucket.stationNames.add(record.stationName);
    }
    byDate.set(dateKey, bucket);
  }

  const dailySeries = [...byDate.entries()]
    .map(([date, bucket]) => ({
      date,
      mean: bucket.values.length
        ? bucket.values.reduce((sum, value) => sum + value, 0) / bucket.values.length
        : 0,
      max: bucket.max,
      station_count: bucket.stationNames.size
    }))
    .sort((left, right) => right.date.localeCompare(left.date));

  const latest = dailySeries[0] ?? null;
  const latestCount = latest?.station_count ?? 0;

  return {
    rain_24h_mm: latest ? Number(latest.mean.toFixed(1)) : 0,
    rain_3d_mm: Number(dailySeries.slice(0, 3).reduce((sum, entry) => sum + entry.mean, 0).toFixed(1)),
    rain_7d_mm: Number(dailySeries.slice(0, 7).reduce((sum, entry) => sum + entry.mean, 0).toFixed(1)),
    max_station_24h_mm: latest ? Number(latest.max.toFixed(1)) : 0,
    station_count: latestCount,
    daily_series: dailySeries
  };
}

export function summarizeRiverLevelSeries(records, context = null) {
  const byStation = new Map();

  for (const record of records) {
    const value = safeNumber(record.dataValue);
    const time = record.dataTime ? new Date(record.dataTime).getTime() : NaN;
    if (value === null || !Number.isFinite(time)) {
      continue;
    }

    const stationCode = record.stationCode ?? record.stationName ?? "unknown";
    const bucket = byStation.get(stationCode) ?? [];
    bucket.push({
      ...record,
      numericValue: value,
      timestamp: time
    });
    byStation.set(stationCode, bucket);
  }

  const stationSummaries = [...byStation.values()].map((entries) => {
    const sorted = [...entries].sort((left, right) => left.timestamp - right.timestamp);
    const first = sorted[0];
    const latest = sorted[sorted.length - 1];
    const riseMetres = Number((latest.numericValue - first.numericValue).toFixed(2));
    const registration = context ? findStationRegistration(context.registry, latest) : null;
    const threshold = context ? findStationThreshold(context.thresholds, latest, registration) : null;
    const thresholdState = classifyThresholdSeverity(latest.numericValue, riseMetres, threshold);
    const fallbackSeverity = computeRiseSeverity(riseMetres);
    return {
      station_code: latest.stationCode ?? registration?.station_code ?? null,
      station_name: latest.stationName ?? registration?.station_name ?? null,
      latest_level_m: latest.numericValue,
      rise_m: riseMetres,
      data_time: latest.dataTime ?? null,
      tehsil: latest.tehsil ?? null,
      station_lat: safeNumber(latest.station_lat ?? registration?.lat),
      station_lon: safeNumber(latest.station_lon ?? registration?.lon),
      district_id: latest.district_id ?? registration?.district_id ?? null,
      taluk_id: latest.taluk_id ?? registration?.taluk_id ?? null,
      warning_level_m: threshold?.warning_level_m ?? null,
      danger_level_m: threshold?.danger_level_m ?? null,
      highest_flood_level_m: threshold?.highest_flood_level_m ?? null,
      threshold_confidence: threshold?.confidence ?? null,
      threshold_ratio: thresholdState?.threshold_ratio ?? null,
      level_status: thresholdState?.level_status ?? "unclassified",
      severity_basis: thresholdState ? "threshold" : fallbackSeverity > 0 ? "rise_fallback" : "none",
      severity: thresholdState?.severity ?? fallbackSeverity
    };
  });

  const maxRise = stationSummaries.reduce((best, station) => Math.max(best, station.rise_m), 0);
  const severity = stationSummaries.reduce((best, station) => Math.max(best, station.severity ?? 0), 0);
  const thresholdStations = stationSummaries.filter((station) => station.severity_basis === "threshold");
  const aboveWarning = thresholdStations.filter((station) =>
    station.level_status === "above_warning" || station.level_status === "above_danger"
  );
  const aboveDanger = thresholdStations.filter((station) => station.level_status === "above_danger");
  const maxThresholdRatio = thresholdStations.reduce(
    (best, station) => Math.max(best, station.threshold_ratio ?? Number.NEGATIVE_INFINITY),
    Number.NEGATIVE_INFINITY
  );
  const summaryNote =
    aboveDanger.length > 0
      ? `India-WRIS river level above danger at ${aboveDanger[0].station_name}`
      : aboveWarning.length > 0
        ? `India-WRIS river level above warning at ${aboveWarning[0].station_name}`
        : thresholdStations.length > 0
          ? `India-WRIS threshold-tracked river level available from ${thresholdStations.length} station${thresholdStations.length === 1 ? "" : "s"}`
          : maxRise > 0
            ? `India-WRIS river level rise ${Number(maxRise.toFixed(2))} m across ${stationSummaries.length} station${stationSummaries.length === 1 ? "" : "s"}`
            : `India-WRIS river level available from ${stationSummaries.length} station${stationSummaries.length === 1 ? "" : "s"} with no notable rise`;

  return {
    severity: Number(severity.toFixed(2)),
    station_count: stationSummaries.length,
    max_rise_m: Number(maxRise.toFixed(2)),
    threshold_station_count: thresholdStations.length,
    above_warning_station_count: aboveWarning.length,
    above_danger_station_count: aboveDanger.length,
    max_threshold_ratio: Number.isFinite(maxThresholdRatio) ? Number(maxThresholdRatio.toFixed(2)) : null,
    severity_basis: thresholdStations.length > 0 ? "threshold" : maxRise > 0 ? "rise_fallback" : "none",
    summary_note: summaryNote,
    stations: stationSummaries
  };
}

async function fetchDistrictDataset(repoRoot, source, queryBuilder) {
  const stationContext = await loadStationContext(repoRoot);
  const results = await mapWithConcurrency(districts, async (district) => {
    let response;
    try {
      response = await fetchDistrictPages(source, district, queryBuilder, source.fetch_options ?? {});
      if (response.response.ok) {
        const rows = Array.isArray(response.response.json?.data) ? response.response.json.data : [];
        response.response.json = {
          ...(response.response.json ?? {}),
          data: rows.map((row) => enrichStationRow(row, district, stationContext))
        };
      }
    } catch (error) {
      response = {
        district,
        url: buildQueryUrl(source.url, queryBuilder(district, 0, source.fetch_options?.pageSize ?? 100)),
        response: {
          ok: false,
          status: 599,
          text: "",
          json: null,
          error: error instanceof Error
            ? [error.message, error.cause?.message].filter(Boolean).join(" | ")
            : String(error)
        }
      };
    }
    return response;
  });

  const errors = results.filter((entry) => !entry.response.ok);
  if (errors.length === results.length) {
    const sample = errors[0];
    const endpointPath = new URL(sample.url).pathname;
    const cleanError = sample.response.error ? sample.response.error.split("|")[0].trim() : "Timeout or Network Issue";
    throw new Error(`India-WRIS fetch failed (${sample.response.status}): ${endpointPath} - ${cleanError}`);
  }

  return results;
}

function summarizeDistrictFetchCoverage(results) {
  const failedDistricts = results
    .filter((entry) => !entry.response.ok)
    .map((entry) => ({
      district_id: entry.district.id,
      district_name: entry.district.name,
      status: entry.response.status ?? 599,
      error: entry.response.error ?? "Request failed"
    }));

  return {
    requested_district_count: results.length,
    successful_district_count: results.length - failedDistricts.length,
    failed_districts: failedDistricts
  };
}

export async function fetchIndiaWrisRainfallPayload(repoRoot, source) {
  const endDate = new Date();
  const startDate = addDays(endDate, -6);

  const districtResults = await fetchDistrictDataset(repoRoot, source, (district, page, size) => ({
    stateName: "Kerala",
    districtName: district.name,
    agencyName: "CWC",
    startdate: formatDate(startDate),
    enddate: formatDate(endDate),
    download: false,
    page,
    size
  }));
  const coverage = summarizeDistrictFetchCoverage(districtResults);

  const districtBuckets = new Map();
  const talukBuckets = new Map();
  let latestIssuedAt = null;

  for (const result of districtResults) {
    if (!result.response.ok) {
      continue;
    }

    const rows = Array.isArray(result.response.json?.data) ? result.response.json.data : [];
    latestIssuedAt = [latestIssuedAt, latestTimestamp(rows)].filter(Boolean).sort().at(-1) ?? latestIssuedAt;

    for (const row of rows) {
      const districtId = row.district_id ?? result.district.id;
      const districtBucket = districtBuckets.get(districtId) ?? [];
      districtBucket.push(row);
      districtBuckets.set(districtId, districtBucket);

      const talukId = row.taluk_id;
      if (!talukId) {
        continue;
      }
      const bucket = talukBuckets.get(talukId) ?? [];
      bucket.push(row);
      talukBuckets.set(talukId, bucket);
    }
  }

  const districtRainfall = [...districtBuckets.entries()].map(([districtId, rows]) => {
    const aggregate = aggregateRainfallSeries(rows);
    return {
      district_id: districtId,
      district_name: districtLookup[districtId]?.name ?? districtId,
      source: "indiawris-cwc",
      station_count: aggregate.station_count,
      max_station_24h_mm: aggregate.max_station_24h_mm,
      rain_24h_mm: aggregate.rain_24h_mm,
      rain_3d_mm: aggregate.rain_3d_mm,
      rain_7d_mm: aggregate.rain_7d_mm,
      daily_series: aggregate.daily_series
    };
  });

  const taluks = [...talukBuckets.entries()].map(([talukId, rows]) => {
    const aggregate = aggregateRainfallSeries(rows);
    const [districtId] = talukId.split("--");
    return {
      taluk_id: talukId,
      district_id: districtId,
      source: "indiawris-cwc",
      station_count: aggregate.station_count,
      max_station_24h_mm: aggregate.max_station_24h_mm,
      rain_24h_mm: aggregate.rain_24h_mm,
      rain_3d_mm: aggregate.rain_3d_mm,
      rain_7d_mm: aggregate.rain_7d_mm,
      daily_series: aggregate.daily_series
    };
  });

  const totalStations = districtRainfall.reduce((sum, entry) => sum + (entry.station_count ?? 0), 0);

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: latestIssuedAt,
      districts: districtRainfall,
      taluks,
      station_count: totalStations,
      ...coverage
    }),
    note:
      coverage.failed_districts.length > 0
        ? `India-WRIS rainfall queried for ${coverage.successful_district_count}/${coverage.requested_district_count} Kerala districts`
        : `India-WRIS rainfall queried for ${districtRainfall.length} Kerala districts`,
    resolvedUrl: source.url
  };
}

export async function fetchIndiaWrisRiverLevelPayload(repoRoot, source) {
  const endDate = new Date();
  const startDate = addDays(endDate, -6);
  const stationContext = await loadStationContext(repoRoot);

  const districtResults = await fetchDistrictDataset(repoRoot, source, (district, page, size) => ({
    stateName: "Kerala",
    districtName: district.name,
    agencyName: "CWC",
    startdate: formatDate(startDate),
    enddate: formatDate(endDate),
    download: false,
    page,
    size
  }));
  const coverage = summarizeDistrictFetchCoverage(districtResults);

  const districtBuckets = new Map();
  let latestIssuedAt = null;

  for (const result of districtResults) {
    if (!result.response.ok) {
      continue;
    }

    const rows = Array.isArray(result.response.json?.data) ? result.response.json.data : [];
    latestIssuedAt = [latestIssuedAt, latestTimestamp(rows)].filter(Boolean).sort().at(-1) ?? latestIssuedAt;
    for (const row of rows) {
      const districtId = row.district_id ?? result.district.id;
      const bucket = districtBuckets.get(districtId) ?? [];
      bucket.push(row);
      districtBuckets.set(districtId, bucket);
    }
  }

  const districtLevels = [...districtBuckets.entries()].map(([districtId, rows]) => ({
    district_id: districtId,
    district_name: districtLookup[districtId]?.name ?? districtId,
    source: "indiawris-cwc",
    ...summarizeRiverLevelSeries(rows, stationContext)
  }));

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: latestIssuedAt,
      districts: districtLevels,
      ...coverage
    }),
    note:
      coverage.failed_districts.length > 0
        ? `India-WRIS river level queried for ${coverage.successful_district_count}/${coverage.requested_district_count} Kerala districts`
        : `India-WRIS river level queried for ${districtLevels.length} Kerala districts`,
    resolvedUrl: source.url
  };
}
