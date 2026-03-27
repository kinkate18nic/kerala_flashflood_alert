import path from "node:path";
import { fetchText } from "./http.js";
import { readJson } from "./fs.js";
import { summarizeRiverLevelSeries } from "./indiawris.js";

const FFS_API_BASE = "https://ffs.india-water.gov.in/iam/api";
const FFS_DATATYPE_CODE = "HHS";
const FFS_TIMEZONE_SUFFIX = "+05:30";

let contextCache = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeStationName(value) {
  return String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function normalizeFfsTimestamp(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return null;
  }
  if (/[zZ]|[+-]\d{2}:?\d{2}$/.test(raw)) {
    return raw;
  }
  return `${raw}${FFS_TIMEZONE_SUFFIX}`;
}

function wrapTargetUrl(baseUrl, targetUrl) {
  const outerUrl = new URL(baseUrl);
  const innerRaw = outerUrl.searchParams.get("url");
  if (!innerRaw) {
    return targetUrl;
  }

  const innerUrl = new URL(targetUrl);
  outerUrl.searchParams.set("url", innerUrl.toString());
  return outerUrl.toString();
}

function buildLatestObservedUrl(stationCode, pageSize = 2) {
  const sortCriteria = {
    sortOrderDtos: [
      {
        sortDirection: "DESC",
        field: "id.dataTime"
      }
    ]
  };

  const specification = {
    where: {
      where: {
        expression: {
          valueIsRelationField: false,
          fieldName: "id.stationCode",
          operator: "eq",
          value: stationCode
        }
      },
      and: {
        expression: {
          valueIsRelationField: false,
          fieldName: "id.datatypeCode",
          operator: "eq",
          value: FFS_DATATYPE_CODE
        }
      }
    },
    and: {
      expression: {
        valueIsRelationField: false,
        fieldName: "dataValue",
        operator: "null",
        value: "false"
      }
    }
  };

  const url = new URL(`${FFS_API_BASE}/new-entry-data/specification/sorted-page`);
  url.searchParams.set("sort-criteria", JSON.stringify(sortCriteria));
  url.searchParams.set("page-number", "0");
  url.searchParams.set("page-size", String(pageSize));
  url.searchParams.set("specification", JSON.stringify(specification));
  return url.toString();
}

function buildForecastUrl(stationCode, pageSize = 8) {
  const sortCriteria = {
    sortOrderDtos: [
      {
        sortDirection: "ASC",
        field: "id.dataTime"
      }
    ]
  };

  const specification = {
    where: {
      where: {
        expression: {
          valueIsRelationField: false,
          fieldName: "id.stationCode",
          operator: "eq",
          value: stationCode
        }
      },
      and: {
        expression: {
          valueIsRelationField: false,
          fieldName: "id.datatypeCode",
          operator: "eq",
          value: FFS_DATATYPE_CODE
        }
      }
    }
  };

  const url = new URL(`${FFS_API_BASE}/new-forecasted-entry-data/specification/sorted`);
  url.searchParams.set("sort-criteria", JSON.stringify(sortCriteria));
  url.searchParams.set("specification", JSON.stringify(specification));
  url.searchParams.set("page-size", String(pageSize));
  return url.toString();
}

async function fetchJson(url, { timeoutMs = 25000, retries = 2 } = {}) {
  let lastFailure = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const response = await fetchText(url, {
        timeoutMs,
        headers: {
          accept: "application/json, text/plain, */*",
          referer: "https://ffs.india-water.gov.in/#/main/site"
        }
      });

      if (!response.ok) {
        return {
          ...response,
          json: null
        };
      }

      try {
        return {
          ...response,
          json: response.text ? JSON.parse(response.text) : null
        };
      } catch (error) {
        return {
          ok: false,
          status: response.status,
          text: response.text,
          json: null,
          error: error instanceof Error ? error.message : String(error)
        };
      }
    } catch (error) {
      lastFailure = error;
      if (attempt < retries) {
        await sleep(700 * attempt);
      }
    }
  }

  return {
    ok: false,
    status: 599,
    text: "",
    json: null,
    error: lastFailure instanceof Error ? lastFailure.message : String(lastFailure)
  };
}

async function fetchJsonWithFallback(urls, options = {}) {
  const failures = [];

  for (const url of [...new Set(urls.filter(Boolean))]) {
    const response = await fetchJson(url, options);
    if (response.ok && response.json !== null) {
      return {
        ...response,
        resolvedUrl: url,
        attemptedUrls: failures.map((entry) => entry.url)
      };
    }
    failures.push({
      url,
      status: response.status ?? 599,
      error: response.error ?? response.text?.slice(0, 200) ?? "fetch failed"
    });
  }

  const lastFailure = failures.at(-1) ?? { url: urls[0] ?? null, status: 599, error: "fetch failed" };
  return {
    ok: false,
    status: lastFailure.status,
    text: "",
    json: null,
    error: failures.map((entry) => `${entry.url}: ${entry.error}`).join(" | "),
    resolvedUrl: lastFailure.url,
    attemptedUrls: failures.map((entry) => entry.url)
  };
}

async function loadFfsContext(repoRoot) {
  if (!contextCache) {
    contextCache = (async () => {
      const [registryDocument, thresholdDocument] = await Promise.all([
        readJson(path.join(repoRoot, "data", "manual", "indiawris-stations.json"), {
          stations: []
        }),
        readJson(path.join(repoRoot, "data", "manual", "indiawris-river-thresholds.json"), {
          stations: []
        })
      ]);

      const registryByCode = new Map();
      const registryByName = new Map();
      for (const station of registryDocument.stations ?? []) {
        if (station.station_code) {
          registryByCode.set(normalizeStationName(station.station_code), station);
        }
        const names = [station.station_name, ...(station.aliases ?? [])].filter(Boolean);
        for (const name of names) {
          registryByName.set(normalizeStationName(name), station);
        }
      }

      const thresholdsByCode = new Map();
      const thresholdsByName = new Map();
      for (const station of thresholdDocument.stations ?? []) {
        if (station.station_code) {
          thresholdsByCode.set(normalizeStationName(station.station_code), station);
        }
        const names = [station.station_name, ...(station.aliases ?? [])].filter(Boolean);
        for (const name of names) {
          thresholdsByName.set(normalizeStationName(name), station);
        }
      }

      return {
        registry: {
          stations: registryDocument.stations ?? [],
          byCode: registryByCode,
          byName: registryByName
        },
        thresholds: {
          stations: thresholdDocument.stations ?? [],
          byCode: thresholdsByCode,
          byName: thresholdsByName
        }
      };
    })();
  }

  return contextCache;
}

function mapWithConcurrency(items, worker, concurrency = 4) {
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

function buildDistrictSummaryNote(districtSummary) {
  if ((districtSummary.above_danger_station_count ?? 0) > 0) {
    const station = districtSummary.stations.find((entry) => entry.level_status === "above_danger");
    return `CWC flood forecasting river level above danger at ${station?.station_name ?? "a district station"}`;
  }
  if ((districtSummary.above_warning_station_count ?? 0) > 0) {
    const station = districtSummary.stations.find((entry) =>
      entry.level_status === "above_warning" || entry.level_status === "above_danger"
    );
    return `CWC flood forecasting river level above warning at ${station?.station_name ?? "a district station"}`;
  }
  if ((districtSummary.forecast_danger_station_count ?? 0) > 0) {
    const station = districtSummary.stations.find((entry) => entry.forecast_status === "forecast_above_danger");
    return `CWC flood forecasting forecast to cross danger at ${station?.station_name ?? "a district station"} by ${station?.forecast_crossing_time ?? "the next forecast window"}`;
  }
  if ((districtSummary.forecast_warning_station_count ?? 0) > 0) {
    const station = districtSummary.stations.find((entry) =>
      entry.forecast_status === "forecast_above_warning" || entry.forecast_status === "forecast_above_danger"
    );
    return `CWC flood forecasting forecast to cross warning at ${station?.station_name ?? "a district station"} by ${station?.forecast_crossing_time ?? "the next forecast window"}`;
  }
  if ((districtSummary.station_count ?? 0) > 0 && (districtSummary.max_rise_m ?? 0) > 0) {
    return `CWC flood forecasting observed river rise ${districtSummary.max_rise_m} m across ${districtSummary.station_count} station${districtSummary.station_count === 1 ? "" : "s"}`;
  }
  return `CWC flood forecasting live river level available from ${districtSummary.station_count ?? 0} station${districtSummary.station_count === 1 ? "" : "s"}`;
}

function safeNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function classifyForecastRows(stationSummary, forecastRows) {
  if (!forecastRows.length) {
    return null;
  }
  if (stationSummary.warning_level_m === null || stationSummary.danger_level_m === null) {
    return null;
  }

  const normalizedRows = forecastRows
    .map((row) => ({
      value: safeNumber(row?.dataValue),
      dataTime: normalizeFfsTimestamp(row?.id?.dataTime ?? row?.dataTime ?? row?.forecastDate ?? row?.id?.forecastDate)
    }))
    .filter((row) => row.value !== null && row.dataTime)
    .sort((left, right) => new Date(left.dataTime).getTime() - new Date(right.dataTime).getTime());

  if (!normalizedRows.length) {
    return null;
  }

  const warning = stationSummary.warning_level_m;
  const danger = stationSummary.danger_level_m;
  const peak = normalizedRows.reduce((best, row) => Math.max(best, row.value), Number.NEGATIVE_INFINITY);
  const warningCrossing = normalizedRows.find((row) => row.value >= warning) ?? null;
  const dangerCrossing = normalizedRows.find((row) => row.value >= danger) ?? null;
  const latestObserved = stationSummary.latest_level_m ?? null;

  let forecastStatus = "forecast_below_warning";
  let severity = 0;
  let crossingTime = null;
  if (dangerCrossing) {
    forecastStatus = "forecast_above_danger";
    severity = latestObserved !== null && latestObserved >= warning ? 0.95 : 0.82;
    crossingTime = dangerCrossing.dataTime;
  } else if (warningCrossing) {
    forecastStatus = "forecast_above_warning";
    severity = latestObserved !== null && latestObserved >= warning ? 0.75 : 0.55;
    crossingTime = warningCrossing.dataTime;
  } else if (peak >= warning - Math.max(0.2, (danger - warning) * 0.4)) {
    forecastStatus = "forecast_near_warning";
    severity = 0.22;
  }

  return {
    forecast_peak_level_m: Number(peak.toFixed(2)),
    forecast_rows: normalizedRows.length,
    forecast_crossing_time: crossingTime,
    forecast_status: forecastStatus,
    forecast_severity: Number(severity.toFixed(2))
  };
}

function mergeForecastIntoSummary(summary, forecastByStation) {
  const stations = summary.stations.map((station) => {
    const forecastRows = forecastByStation.get(station.station_code) ?? [];
    const forecast = classifyForecastRows(station, forecastRows);
    return {
      ...station,
      forecast_peak_level_m: forecast?.forecast_peak_level_m ?? null,
      forecast_crossing_time: forecast?.forecast_crossing_time ?? null,
      forecast_status: forecast?.forecast_status ?? "forecast_unavailable",
      forecast_rows: forecast?.forecast_rows ?? 0,
      forecast_severity: forecast?.forecast_severity ?? 0
    };
  });

  const forecastWarningStations = stations.filter((station) =>
    station.forecast_status === "forecast_above_warning" || station.forecast_status === "forecast_above_danger"
  );
  const forecastDangerStations = stations.filter((station) => station.forecast_status === "forecast_above_danger");
  const forecastSeverity = stations.reduce((best, station) => Math.max(best, station.forecast_severity ?? 0), 0);

  return {
    ...summary,
    severity: Number(Math.max(summary.severity ?? 0, forecastSeverity).toFixed(2)),
    forecast_warning_station_count: forecastWarningStations.length,
    forecast_danger_station_count: forecastDangerStations.length,
    forecast_station_count: stations.filter((station) => station.forecast_rows > 0).length,
    forecast_severity: Number(forecastSeverity.toFixed(2)),
    severity_basis:
      forecastSeverity > (summary.severity ?? 0)
        ? "threshold_forecast"
        : summary.severity_basis,
    stations
  };
}

export async function fetchCwcFfsPayload(repoRoot, source) {
  const context = await loadFfsContext(repoRoot);
  const stations = context.registry.stations.filter(
    (station) => station.station_code && station.source_types?.includes("river_level")
  );

  const pageSize = source.fetch_options?.pageSize ?? 2;
  const concurrency = source.fetch_options?.concurrency ?? 4;
  const stationResponses = await mapWithConcurrency(
    stations,
    async (station) => {
      const observedUrl = buildLatestObservedUrl(station.station_code, pageSize);
      const observedUrls = [observedUrl, wrapTargetUrl(source.url, observedUrl)];
      const forecastUrl = buildForecastUrl(station.station_code, source.fetch_options?.forecastPageSize ?? 8);
      const forecastUrls = [forecastUrl, wrapTargetUrl(source.url, forecastUrl)];
      const [response, forecastResponse] = await Promise.all([
        fetchJsonWithFallback(observedUrls, {
          timeoutMs: source.fetch_options?.timeoutMs ?? 25000,
          retries: source.fetch_options?.retries ?? 2
        }),
        fetchJsonWithFallback(forecastUrls, {
          timeoutMs:
            source.fetch_options?.forecastTimeoutMs ??
            source.fetch_options?.timeoutMs ??
            25000,
          retries:
            source.fetch_options?.forecastRetries ??
            source.fetch_options?.retries ??
            2
        })
      ]);

      if (!response.ok) {
        return {
          station,
          ok: false,
          status: response.status ?? 599,
          error: response.error ?? response.text?.slice(0, 200) ?? "fetch failed"
        };
      }

      const rows = Array.isArray(response.json) ? response.json : [];
      const forecastRows = forecastResponse.ok && Array.isArray(forecastResponse.json) ? forecastResponse.json : [];
      return {
        station,
        ok: true,
        status: response.status ?? 200,
        forecast_ok: forecastResponse.ok,
        forecast_error: forecastResponse.ok ? null : forecastResponse.error ?? forecastResponse.text?.slice(0, 200) ?? "forecast fetch failed",
        rows: rows.map((row) => ({
          stationCode: row?.id?.stationCode ?? station.station_code,
          stationName: station.station_name,
          dataTime: normalizeFfsTimestamp(row?.id?.dataTime),
          dataValue: row?.dataValue,
          district_id: station.district_id,
          taluk_id: station.taluk_id ?? null,
          station_lat: station.lat ?? null,
          station_lon: station.lon ?? null
        })),
        forecast_rows: forecastRows
      };
    },
    concurrency
  );

  const failedStations = stationResponses
    .filter((entry) => !entry.ok)
    .map((entry) => ({
      station_code: entry.station.station_code,
      station_name: entry.station.station_name,
      district_id: entry.station.district_id,
      status: entry.status ?? 599,
      error: entry.error ?? "fetch failed"
    }));

  if (failedStations.length === stationResponses.length) {
    const sample = failedStations[0];
    throw new Error(
      `CWC FFS fetch failed (${sample.status}) for ${sample.station_code}: ${sample.error}`
    );
  }

  const successfulResponses = stationResponses.filter((entry) => entry.ok);
  const allRows = successfulResponses.flatMap((entry) => entry.rows ?? []);
  const forecastByStation = new Map(
    successfulResponses.map((entry) => [entry.station.station_code, entry.forecast_rows ?? []])
  );
  const districtBuckets = new Map();

  for (const row of allRows) {
    if (!row.district_id) {
      continue;
    }
    const bucket = districtBuckets.get(row.district_id) ?? [];
    bucket.push(row);
    districtBuckets.set(row.district_id, bucket);
  }

  const districts = [...districtBuckets.entries()].map(([districtId, rows]) => {
    const summary = summarizeRiverLevelSeries(rows, context);
    const enrichedSummary = mergeForecastIntoSummary(summary, forecastByStation);
    return {
      district_id: districtId,
      source: "cwc-ffs",
      ...enrichedSummary,
      summary_note: buildDistrictSummaryNote(enrichedSummary)
    };
  });

  const timestamps = allRows
    .map((row) => row.dataTime)
    .filter(Boolean)
    .sort();
  const totalAboveWarning = districts.reduce(
    (sum, district) => sum + (district.above_warning_station_count ?? 0),
    0
  );
  const totalAboveDanger = districts.reduce(
    (sum, district) => sum + (district.above_danger_station_count ?? 0),
    0
  );
  const totalForecastWarning = districts.reduce(
    (sum, district) => sum + (district.forecast_warning_station_count ?? 0),
    0
  );
  const totalForecastDanger = districts.reduce(
    (sum, district) => sum + (district.forecast_danger_station_count ?? 0),
    0
  );

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: timestamps.at(-1) ?? null,
      districts,
      station_count: allRows.length ? new Set(allRows.map((row) => row.stationCode)).size : 0,
      requested_station_count: stationResponses.length,
      successful_station_count: successfulResponses.length,
      failed_stations: failedStations,
      partial_failure_count: failedStations.length,
      above_warning_station_count: totalAboveWarning,
      above_danger_station_count: totalAboveDanger,
      forecast_warning_station_count: totalForecastWarning,
      forecast_danger_station_count: totalForecastDanger,
      warning: totalAboveWarning > 0,
      watch: districts.some((district) => (district.severity ?? 0) > 0) || totalForecastWarning > 0
    }),
    note:
      failedStations.length > 0
        ? `CWC FFS live river levels queried for ${successfulResponses.length}/${stationResponses.length} Kerala stations`
        : `CWC FFS live river levels queried for ${successfulResponses.length} Kerala stations`,
    resolvedUrl: source.url
  };
}
