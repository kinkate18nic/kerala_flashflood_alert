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
  if ((districtSummary.station_count ?? 0) > 0 && (districtSummary.max_rise_m ?? 0) > 0) {
    return `CWC flood forecasting observed river rise ${districtSummary.max_rise_m} m across ${districtSummary.station_count} station${districtSummary.station_count === 1 ? "" : "s"}`;
  }
  return `CWC flood forecasting live river level available from ${districtSummary.station_count ?? 0} station${districtSummary.station_count === 1 ? "" : "s"}`;
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
      const url = buildLatestObservedUrl(station.station_code, pageSize);
      const response = await fetchJson(wrapTargetUrl(source.url, url), {
        timeoutMs: source.fetch_options?.timeoutMs ?? 25000,
        retries: source.fetch_options?.retries ?? 2
      });

      if (!response.ok) {
        return {
          station,
          ok: false,
          status: response.status ?? 599,
          error: response.error ?? response.text?.slice(0, 200) ?? "fetch failed"
        };
      }

      const rows = Array.isArray(response.json) ? response.json : [];
      return {
        station,
        ok: true,
        status: response.status ?? 200,
        rows: rows.map((row) => ({
          stationCode: row?.id?.stationCode ?? station.station_code,
          stationName: station.station_name,
          dataTime: normalizeFfsTimestamp(row?.id?.dataTime),
          dataValue: row?.dataValue,
          district_id: station.district_id,
          taluk_id: station.taluk_id ?? null,
          station_lat: station.lat ?? null,
          station_lon: station.lon ?? null
        }))
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
    return {
      district_id: districtId,
      source: "cwc-ffs",
      ...summary,
      summary_note: buildDistrictSummaryNote(summary)
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
      warning: totalAboveWarning > 0,
      watch: districts.some((district) => (district.severity ?? 0) > 0)
    }),
    note:
      failedStations.length > 0
        ? `CWC FFS live river levels queried for ${successfulResponses.length}/${stationResponses.length} Kerala stations`
        : `CWC FFS live river levels queried for ${successfulResponses.length} Kerala stations`,
    resolvedUrl: source.url
  };
}
