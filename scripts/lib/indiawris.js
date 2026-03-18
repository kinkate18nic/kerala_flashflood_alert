import { districts } from "../../src/shared/areas.js";
import { talukIdFromBoundaryNames } from "./boundaries.js";

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function buildQueryUrl(baseUrl, parameters) {
  const url = new URL(baseUrl);
  Object.entries(parameters).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, { timeoutMs = 60000, retries = 3 } = {}) {
  let attempt = 0;
  let lastFailure = null;

  while (attempt < retries) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          accept: "application/json, text/plain, */*",
          origin: "https://indiawris.gov.in",
          referer: "https://indiawris.gov.in/swagger-ui/index.html",
          "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/kinkate18nic/kerala_flashflood_alert)"
        },
        signal: controller.signal
      });

      const text = await response.text();
      const payload = text ? JSON.parse(text) : null;
      return {
        ok: response.ok,
        status: response.status,
        text,
        json: payload
      };
    } catch (error) {
      lastFailure = error;
      attempt += 1;
      if (attempt >= retries) {
        break;
      }
      await sleep(600 * attempt);
    } finally {
      clearTimeout(timeout);
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

function aggregateRiverLevelSeries(records) {
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
    return {
      station_code: latest.stationCode ?? null,
      station_name: latest.stationName ?? null,
      latest_level_m: latest.numericValue,
      rise_m: Number((latest.numericValue - first.numericValue).toFixed(2)),
      data_time: latest.dataTime ?? null,
      tehsil: latest.tehsil ?? null
    };
  });

  const maxRise = stationSummaries.reduce((best, station) => Math.max(best, station.rise_m), 0);
  let severity = 0;
  if (maxRise >= 1) {
    severity = 0.7;
  } else if (maxRise >= 0.5) {
    severity = 0.45;
  } else if (maxRise >= 0.25) {
    severity = 0.25;
  }

  return {
    severity,
    station_count: stationSummaries.length,
    max_rise_m: Number(maxRise.toFixed(2)),
    stations: stationSummaries
  };
}

async function fetchDistrictDataset(source, queryBuilder) {
  const results = await mapWithConcurrency(districts, async (district) => {
    let response;
    try {
      response = await fetchDistrictPages(source, district, queryBuilder, source.fetch_options ?? {});
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
    throw new Error(`India-WRIS fetch failed: ${sample.response.status} ${sample.url}${sample.response.error ? ` (${sample.response.error})` : ""}`);
  }

  return results;
}

export async function fetchIndiaWrisRainfallPayload(source) {
  const endDate = new Date();
  const startDate = addDays(endDate, -6);

  const districtResults = await fetchDistrictDataset(source, (district, page, size) => ({
    stateName: "Kerala",
    districtName: district.name,
    agencyName: "CWC",
    startdate: formatDate(startDate),
    enddate: formatDate(endDate),
    download: false,
    page,
    size
  }));

  const districtRainfall = [];
  const talukBuckets = new Map();
  let latestIssuedAt = null;
  let totalStations = 0;

  for (const result of districtResults) {
    if (!result.response.ok) {
      continue;
    }

    const rows = Array.isArray(result.response.json?.data) ? result.response.json.data : [];
    latestIssuedAt = [latestIssuedAt, latestTimestamp(rows)].filter(Boolean).sort().at(-1) ?? latestIssuedAt;

    const aggregate = aggregateRainfallSeries(rows);
    totalStations += aggregate.station_count;

    districtRainfall.push({
      district_id: result.district.id,
      district_name: result.district.name,
      source: "indiawris-cwc",
      station_count: aggregate.station_count,
      max_station_24h_mm: aggregate.max_station_24h_mm,
      rain_24h_mm: aggregate.rain_24h_mm,
      rain_3d_mm: aggregate.rain_3d_mm,
      rain_7d_mm: aggregate.rain_7d_mm,
      daily_series: aggregate.daily_series
    });

    for (const row of rows) {
      const talukId = talukIdFromBoundaryNames(result.district.name, row.tehsil ?? row.block ?? "");
      if (!talukId) {
        continue;
      }
      const bucket = talukBuckets.get(talukId) ?? [];
      bucket.push(row);
      talukBuckets.set(talukId, bucket);
    }
  }

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

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: latestIssuedAt,
      districts: districtRainfall,
      taluks,
      station_count: totalStations
    }),
    note: `India-WRIS rainfall queried for ${districtRainfall.length} Kerala districts`,
    resolvedUrl: source.url
  };
}

export async function fetchIndiaWrisRiverLevelPayload(source) {
  const endDate = new Date();
  const startDate = addDays(endDate, -1);

  const districtResults = await fetchDistrictDataset(source, (district, page, size) => ({
    stateName: "Kerala",
    districtName: district.name,
    agencyName: "CWC",
    startdate: formatDate(startDate),
    enddate: formatDate(endDate),
    download: false,
    page,
    size
  }));

  const districtLevels = [];
  let latestIssuedAt = null;

  for (const result of districtResults) {
    if (!result.response.ok) {
      continue;
    }

    const rows = Array.isArray(result.response.json?.data) ? result.response.json.data : [];
    latestIssuedAt = [latestIssuedAt, latestTimestamp(rows)].filter(Boolean).sort().at(-1) ?? latestIssuedAt;
    const aggregate = aggregateRiverLevelSeries(rows);

    districtLevels.push({
      district_id: result.district.id,
      district_name: result.district.name,
      source: "indiawris-cwc",
      ...aggregate
    });
  }

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: latestIssuedAt,
      districts: districtLevels
    }),
    note: `India-WRIS river level queried for ${districtLevels.length} Kerala districts`,
    resolvedUrl: source.url
  };
}
