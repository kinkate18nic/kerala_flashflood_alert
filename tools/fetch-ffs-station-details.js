import path from "node:path";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const DEFAULT_REGISTRY_PATH = path.join(repoRoot, "data", "manual", "indiawris-stations.json");
const DEFAULT_OUTPUT_PATH = path.join(repoRoot, "runtime", "metrics", "ffs-station-details.json");

function parseArgs(argv) {
  const options = {
    registryPath: DEFAULT_REGISTRY_PATH,
    outputPath: DEFAULT_OUTPUT_PATH,
    codes: [],
    stateCode: "11",
    agencyId: "41",
    pageSize: 100
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--registry") {
      options.registryPath = path.resolve(argv[++index]);
      continue;
    }
    if (arg === "--output") {
      options.outputPath = path.resolve(argv[++index]);
      continue;
    }
    if (arg === "--station-code") {
      options.codes.push(String(argv[++index]).trim());
      continue;
    }
    if (arg === "--state-code") {
      options.stateCode = String(argv[++index]).trim();
      continue;
    }
    if (arg === "--agency-id") {
      options.agencyId = String(argv[++index]).trim();
      continue;
    }
    if (arg === "--page-size") {
      options.pageSize = Number.parseInt(argv[++index], 10);
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      options.help = true;
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function printHelp() {
  console.log(`Usage:
  node tools/fetch-ffs-station-details.js [--station-code CODE]... [--state-code 11] [--agency-id 41] [--page-size 100] [--output PATH] [--registry PATH]

Behavior:
  - Without --station-code, discovers all flood-forecast stations for the given state from the live FFS list endpoint.
  - Calls FFS endpoints:
      /iam/api/layer-station/specification/sorted-page
      /iam/api/layer-station/<code>
      /iam/api/flood-forecast-static/<code>
  - Writes a normalized JSON document to runtime/metrics/ffs-station-details.json by default.`);
}

function safeNumber(value) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeName(value) {
  return String(value ?? "").trim().toUpperCase();
}

async function readRegistry(registryPath) {
  const raw = await readFile(registryPath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      accept: "application/json, text/plain, */*",
      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      referer: "https://ffs.india-water.gov.in/#/main/site"
    }
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}: ${text.slice(0, 200)}`);
  }

  if (!text.trim()) {
    return null;
  }

  return JSON.parse(text);
}

function buildSortedPageUrl({ pageNumber = 0, pageSize = 100, stateCode = "11", agencyId = "41" } = {}) {
  const sortCriteria = {
    sortOrderDtos: [
      {
        sortDirection: "ASC",
        field: "name"
      }
    ]
  };

  const specification = {
    where: {
      where: {
        expression: {
          valueIsRelationField: false,
          fieldName:
            "subdivisionalOfficeId.divisionalOfficeId.circleOfficeId.regionalOfficeId.agencyId.agencyId",
          operator: "eq",
          value: String(agencyId)
        }
      },
      and: {
        expression: {
          valueIsRelationField: false,
          fieldName: "floodForecastStaticStationCode.stationCode",
          operator: "null",
          value: "false"
        }
      }
    },
    and: {
      expression: {
        valueIsRelationField: false,
        fieldName: "tahsilId.districtId.stateCode.stateCode",
        operator: "eq",
        value: String(stateCode)
      }
    },
    unique: true
  };

  const url = new URL("https://ffs.india-water.gov.in/iam/api/layer-station/specification/sorted-page");
  url.searchParams.set("sort-criteria", JSON.stringify(sortCriteria));
  url.searchParams.set("page-number", String(pageNumber));
  url.searchParams.set("page-size", String(pageSize));
  url.searchParams.set("specification", JSON.stringify(specification));
  return url.toString();
}

async function discoverStationList({ stateCode = "11", agencyId = "41", pageSize = 100 } = {}) {
  const url = buildSortedPageUrl({ pageNumber: 0, pageSize, stateCode, agencyId });
  const payload = await fetchJson(url);
  if (!Array.isArray(payload)) {
    throw new Error(`Unexpected station-list payload shape from ${url}`);
  }
  return payload;
}

async function fetchStationDetail(stationCode) {
  const encoded = encodeURIComponent(stationCode);
  const [station, forecastStatic] = await Promise.all([
    fetchJson(`https://ffs.india-water.gov.in/iam/api/layer-station/${encoded}`),
    fetchJson(`https://ffs.india-water.gov.in/iam/api/flood-forecast-static/${encoded}`)
  ]);

  return { station, forecastStatic };
}

function normalizeStationPayload({ station, forecastStatic }) {
  return {
    station_code: station?.stationCode ?? forecastStatic?.stationCode ?? null,
    station_name: station?.name ?? null,
    lat: safeNumber(station?.lat),
    lon: safeNumber(station?.lon),
    reduced_level_of_zero_gauge_m: safeNumber(
      station?.reducedLevelOfZeroGauge ?? station?.zeroRl
    ),
    warning_level_m: safeNumber(forecastStatic?.warningLevel),
    danger_level_m: safeNumber(forecastStatic?.dangerLevel),
    highest_flood_level_m: safeNumber(forecastStatic?.highestFlowLevel),
    hfl_attained_date: forecastStatic?.highestFlowLevelDate ?? null,
    source: "ffs_api",
    fetched_at: new Date().toISOString()
  };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const registry = await readRegistry(options.registryPath);
  const liveStations = options.codes.length
    ? []
    : await discoverStationList({
        stateCode: options.stateCode,
        agencyId: options.agencyId,
        pageSize: options.pageSize
      });
  const registryCodes = (registry.stations ?? []).map((station) => station.station_code).filter(Boolean);
  const discoveredCodes = liveStations.map((station) => station.stationCode).filter(Boolean);
  const targetCodes = [...new Set(options.codes.length ? options.codes : discoveredCodes.length ? discoveredCodes : registryCodes)];

  if (!targetCodes.length) {
    throw new Error("No station codes available. Pass --station-code or add station_code values to the registry.");
  }

  const byCode = new Map(
    (registry.stations ?? [])
      .filter((station) => station.station_code)
      .map((station) => [normalizeName(station.station_code), station])
  );

  const results = [];
  for (const code of targetCodes) {
    const payload = await fetchStationDetail(code);
    const normalized = normalizeStationPayload(payload);
    const registryStation = byCode.get(normalizeName(code)) ?? null;
    const liveStation =
      liveStations.find((station) => normalizeName(station.stationCode) === normalizeName(code)) ?? null;

    results.push({
      ...normalized,
      live_station_name: liveStation?.name ?? null,
      live_district_name: liveStation?.districtId?.name ?? null,
      live_river: liveStation?.river?.name ?? null,
      live_basin: liveStation?.basin?.name ?? null,
      live_divisional_office: liveStation?.divisionalOffice?.name ?? null,
      registry_station_name: registryStation?.station_name ?? null,
      registry_district_id: registryStation?.district_id ?? null,
      registry_river: registryStation?.river ?? null,
      registry_basin: registryStation?.basin ?? null
    });
  }

  const document = {
    schema_version: 1,
    fetched_at: new Date().toISOString(),
    discovery: options.codes.length
      ? {
          mode: "explicit_codes"
        }
      : {
          mode: "state_query",
          state_code: options.stateCode,
          agency_id: options.agencyId,
          discovered_station_count: discoveredCodes.length
        },
    station_count: results.length,
    stations: results
  };

  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, `${JSON.stringify(document, null, 2)}\n`);
  console.log(`Wrote ${results.length} FFS station detail records to ${options.outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
