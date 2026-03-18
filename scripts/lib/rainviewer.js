import { PNG } from "pngjs";
import { districts, hotspots } from "../../src/shared/areas.js";
import { fetchText } from "./http.js";

const RAINVIEWER_MAPS_URL = "https://api.rainviewer.com/public/weather-maps.json";
const RAINVIEWER_COLORS_URL = "https://www.rainviewer.com/files/rainviewer_api_colors_table.csv";
const RAINVIEWER_SIZE = 256;
const RAINVIEWER_ZOOM = 7;
const RAINVIEWER_COLOR_SCHEME = 2;
const RAINVIEWER_OPTIONS = "0_0";

const districtRadarLocations = {
  kasaragod: { lat: 12.4996, lon: 74.9869 },
  kannur: { lat: 11.8745, lon: 75.3704 },
  wayanad: { lat: 11.6854, lon: 76.132 },
  kozhikode: { lat: 11.2588, lon: 75.7804 },
  malappuram: { lat: 11.073, lon: 76.074 },
  palakkad: { lat: 10.7867, lon: 76.6548 },
  thrissur: { lat: 10.5276, lon: 76.2144 },
  ernakulam: { lat: 9.9816, lon: 76.2999 },
  idukki: { lat: 9.8494, lon: 76.972 },
  kottayam: { lat: 9.5916, lon: 76.5222 },
  alappuzha: { lat: 9.4981, lon: 76.3388 },
  pathanamthitta: { lat: 9.2648, lon: 76.787 },
  kollam: { lat: 8.8932, lon: 76.6141 },
  thiruvananthapuram: { lat: 8.5241, lon: 76.9366 }
};

function rgbaKey(r, g, b, a) {
  return `${r},${g},${b},${a}`;
}

function parseHexRgba(hex) {
  const normalized = hex.replace("#", "").trim();
  if (normalized.length !== 8) {
    return null;
  }
  return {
    r: Number.parseInt(normalized.slice(0, 2), 16),
    g: Number.parseInt(normalized.slice(2, 4), 16),
    b: Number.parseInt(normalized.slice(4, 6), 16),
    a: Number.parseInt(normalized.slice(6, 8), 16)
  };
}

export function parseRainviewerColorTable(csv) {
  const lines = csv.trim().split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    return { byColor: {}, entries: [] };
  }

  const header = lines[0].split(",").map((value) => value.trim());
  const universalBlueIndex = header.findIndex((value) => value === "Universal Blue");
  if (universalBlueIndex === -1) {
    return { byColor: {}, entries: [] };
  }

  const entries = lines
    .slice(1)
    .map((line) => line.split(",").map((value) => value.trim()))
    .filter((columns) => columns.length > universalBlueIndex)
    .map((columns) => {
      const dbz = Number.parseFloat(columns[0]);
      const rgba = parseHexRgba(columns[universalBlueIndex]);
      if (!Number.isFinite(dbz) || !rgba) {
        return null;
      }
      return {
        dbz,
        ...rgba,
        key: rgbaKey(rgba.r, rgba.g, rgba.b, rgba.a)
      };
    })
    .filter(Boolean);

  return {
    byColor: Object.fromEntries(entries.map((entry) => [entry.key, entry.dbz])),
    entries
  };
}

function dbzToIntensity(dbz) {
  if (!Number.isFinite(dbz) || dbz < 5) {
    return { label: "none", severity: 0 };
  }
  if (dbz < 20) {
    return { label: "light", severity: 0.25 };
  }
  if (dbz < 35) {
    return { label: "moderate", severity: 0.5 };
  }
  if (dbz < 45) {
    return { label: "heavy", severity: 0.75 };
  }
  return { label: "very_heavy", severity: 1 };
}

function nearestDbzForColor(r, g, b, a, entries) {
  if (a === 0) {
    return null;
  }

  let nearest = null;
  let nearestDistance = Number.POSITIVE_INFINITY;
  for (const entry of entries) {
    const distance =
      Math.abs(entry.r - r) +
      Math.abs(entry.g - g) +
      Math.abs(entry.b - b) +
      Math.abs(entry.a - a);
    if (distance < nearestDistance) {
      nearest = entry.dbz;
      nearestDistance = distance;
    }
  }

  if (nearestDistance > 80) {
    return null;
  }
  return nearest;
}

function sampleTileDbz(png, colorTable, radius = 48) {
  const centerX = Math.floor(png.width / 2);
  const centerY = Math.floor(png.height / 2);
  let maxDbz = null;
  let activePixels = 0;

  for (let y = Math.max(0, centerY - radius); y <= Math.min(png.height - 1, centerY + radius); y += 1) {
    for (let x = Math.max(0, centerX - radius); x <= Math.min(png.width - 1, centerX + radius); x += 1) {
      const offset = (y * png.width + x) * 4;
      const r = png.data[offset];
      const g = png.data[offset + 1];
      const b = png.data[offset + 2];
      const a = png.data[offset + 3];
      const exactDbz = colorTable.byColor[rgbaKey(r, g, b, a)];
      const dbz =
        exactDbz ??
        nearestDbzForColor(r, g, b, a, colorTable.entries);
      if (!Number.isFinite(dbz)) {
        continue;
      }
      if (dbz >= 5) {
        activePixels += 1;
      }
      maxDbz = maxDbz === null ? dbz : Math.max(maxDbz, dbz);
    }
  }

  return {
    maxDbz,
    activePixels
  };
}

function tileUrl(host, framePath, location) {
  return `${host}${framePath}/${RAINVIEWER_SIZE}/${RAINVIEWER_ZOOM}/${location.lat}/${location.lon}/${RAINVIEWER_COLOR_SCHEME}/${RAINVIEWER_OPTIONS}.png`;
}

async function fetchArrayBuffer(url, timeoutMs = 20000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/)"
      },
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`RainViewer download failed: ${response.status} ${url}`);
    }
    return response.arrayBuffer();
  } finally {
    clearTimeout(timeout);
  }
}

async function sampleRadarPoint(host, framePath, sample, colorTable) {
  const url = tileUrl(host, framePath, sample.location);
  const arrayBuffer = await fetchArrayBuffer(url);
  const png = PNG.sync.read(Buffer.from(arrayBuffer));
  const { maxDbz, activePixels } = sampleTileDbz(
    png,
    colorTable,
    sample.sample_radius_pixels ?? 48
  );
  const intensity = dbzToIntensity(maxDbz);

  return {
    ...sample,
    frame_url: url,
    active_pixels: activePixels,
    max_dbz: Number.isFinite(maxDbz) ? Math.round(maxDbz * 10) / 10 : null,
    intensity: intensity.label,
    severity: intensity.severity,
    detected: intensity.severity > 0 && activePixels > 0
  };
}

function districtSamples() {
  return districts.map((district) => ({
    id: district.id,
    district_id: district.id,
    name: district.name,
    location: districtRadarLocations[district.id],
    sample_radius_pixels: 56
  }));
}

function hotspotSamples() {
  return hotspots
    .filter((hotspot) => hotspot.location?.lat && hotspot.location?.lon)
    .map((hotspot) => ({
      id: hotspot.id,
      hotspot_id: hotspot.id,
      district_id: hotspot.district_id,
      name: hotspot.name,
      location: hotspot.location,
      sample_radius_pixels: 40
    }));
}

export function buildRainviewerPayload({ metadata, districtResults, hotspotResults }) {
  const frame = metadata.radar?.past?.at(-1) ?? null;
  const issuedAt = frame ? new Date(frame.time * 1000).toISOString() : null;
  const districtLookup = Object.fromEntries(districtResults.map((result) => [result.district_id, result]));

  const districtEntries = districts.map((district) => {
    const direct = districtLookup[district.id] ?? {
      max_dbz: null,
      intensity: "none",
      severity: 0,
      detected: false
    };
    const hotspotMatches = hotspotResults.filter((result) => result.district_id === district.id);
    const hotspotDbz = hotspotMatches
      .map((result) => result.max_dbz)
      .filter((value) => Number.isFinite(value));
    const maxDbz = [direct.max_dbz, ...hotspotDbz]
      .filter((value) => Number.isFinite(value))
      .reduce((max, value) => Math.max(max, value), Number.NEGATIVE_INFINITY);
    const intensity = dbzToIntensity(Number.isFinite(maxDbz) ? maxDbz : null);

    return {
      district_id: district.id,
      name: district.name,
      intensity: intensity.label,
      severity: intensity.severity,
      max_dbz: Number.isFinite(maxDbz) ? Math.round(maxDbz * 10) / 10 : null,
      sample_location: direct.location ?? districtRadarLocations[district.id],
      active_pixels: direct.active_pixels ?? 0,
      hotspot_detection_count: hotspotMatches.filter((result) => result.detected).length
    };
  });

  return {
    issued_at: issuedAt,
    generated_at: new Date((metadata.generated ?? frame?.time ?? Date.now() / 1000) * 1000).toISOString(),
    host: metadata.host ?? null,
    frame_time: issuedAt,
    frame_path: frame?.path ?? null,
    color_scheme: "universal_blue",
    districts: districtEntries,
    hotspots: hotspotResults.map((result) => ({
      hotspot_id: result.hotspot_id,
      district_id: result.district_id,
      name: result.name,
      intensity: result.intensity,
      severity: result.severity,
      max_dbz: result.max_dbz,
      active_pixels: result.active_pixels ?? 0,
      location: result.location
    }))
  };
}

export async function fetchRainviewerPayload() {
  const [metadataResponse, colorsResponse] = await Promise.all([
    fetchText(RAINVIEWER_MAPS_URL, { timeoutMs: 20000 }),
    fetchText(RAINVIEWER_COLORS_URL, { timeoutMs: 20000 })
  ]);

  if (!metadataResponse.ok) {
    return {
      ok: false,
      status: metadataResponse.status,
      text: "",
      note: "RainViewer metadata fetch failed."
    };
  }

  if (!colorsResponse.ok) {
    return {
      ok: false,
      status: colorsResponse.status,
      text: "",
      note: "RainViewer color table fetch failed."
    };
  }

  const metadata = JSON.parse(metadataResponse.text);
  const frame = metadata.radar?.past?.at(-1) ?? null;
  if (!frame?.path || !metadata.host) {
    return {
      ok: false,
      status: 424,
      text: "",
      note: "RainViewer did not provide a usable radar frame."
    };
  }

  const colorTable = parseRainviewerColorTable(colorsResponse.text);
  if (!colorTable.entries.length) {
    return {
      ok: false,
      status: 424,
      text: "",
      note: "RainViewer color table could not be parsed."
    };
  }

  const [districtResults, hotspotResults] = await Promise.all([
    Promise.all(districtSamples().map((sample) => sampleRadarPoint(metadata.host, frame.path, sample, colorTable))),
    Promise.all(hotspotSamples().map((sample) => sampleRadarPoint(metadata.host, frame.path, sample, colorTable)))
  ]);

  const payload = buildRainviewerPayload({ metadata, districtResults, hotspotResults });
  return {
    ok: true,
    status: 200,
    text: JSON.stringify(payload),
    note: `RainViewer latest radar frame ${frame.path}`
  };
}
