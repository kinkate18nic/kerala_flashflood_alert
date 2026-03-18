import path from "node:path";
import { readJson } from "./fs.js";
import { districts } from "../../src/shared/areas.js";
import { parseDate } from "./time.js";
import { severityKeywords } from "../../src/shared/risk.js";
import { parseDistrictBoundaries, pointInGeometry } from "./boundaries.js";

function stripHtml(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function readTag(fragment, tagName) {
  const match = fragment.match(new RegExp(`<(?:(?:\\w+):)?${tagName}[^>]*>([\\s\\S]*?)<\\/(?:(?:\\w+):)?${tagName}>`, "i"));
  return match?.[1]?.trim() ?? null;
}

function readTagAttribute(fragment, tagName, attributeName) {
  const match = fragment.match(
    new RegExp(`<(?:(?:\\w+):)?${tagName}[^>]*\\b${attributeName}="([^"]+)"[^>]*\\/?>`, "i")
  );
  return match?.[1]?.trim() ?? null;
}

function readFirstTag(fragment, tagNames) {
  for (const tagName of tagNames) {
    const value = readTag(fragment, tagName);
    if (value) {
      return value;
    }
  }
  return null;
}

function readLink(fragment) {
  return (
    readTag(fragment, "link") ??
    readTagAttribute(fragment, "link", "href") ??
    readTag(fragment, "id")
  );
}

function readCategoryValues(fragment) {
  const directValues = [...fragment.matchAll(/<(?:(?:\w+):)?category[^>]*>([\s\S]*?)<\/(?:(?:\w+):)?category>/gi)]
    .map((match) => match[1]?.trim())
    .filter(Boolean);
  const termValues = [...fragment.matchAll(/<(?:(?:\w+):)?category[^>]*\bterm="([^"]+)"/gi)]
    .map((match) => match[1]?.trim())
    .filter(Boolean);
  return [...new Set([...directValues, ...termValues])];
}

function parseCapPolygon(value) {
  const coordinates = String(value ?? "")
    .trim()
    .split(/\s+/)
    .map((pair) => pair.split(",").map((part) => Number.parseFloat(part)))
    .filter((pair) => pair.length === 2 && Number.isFinite(pair[0]) && Number.isFinite(pair[1]))
    .map(([lat, lon]) => [lon, lat]);

  if (coordinates.length < 4) {
    return null;
  }

  const [firstLon, firstLat] = coordinates[0];
  const [lastLon, lastLat] = coordinates[coordinates.length - 1];
  if (firstLon !== lastLon || firstLat !== lastLat) {
    coordinates.push([firstLon, firstLat]);
  }

  return {
    type: "Polygon",
    coordinates: [coordinates]
  };
}

function parseCapGeocodes(fragment) {
  return [...fragment.matchAll(/<(?:(?:\w+):)?geocode\b[^>]*>([\s\S]*?)<\/(?:(?:\w+):)?geocode>/gi)].map((match) => {
    const geocodeText = match[1];
    return {
      value_name: readFirstTag(geocodeText, ["valueName"]) ?? null,
      value: readFirstTag(geocodeText, ["value"]) ?? null
    };
  });
}

let districtBoundaryCache = null;

async function loadLocalDistrictBoundaries(repoRoot) {
  if (districtBoundaryCache) {
    return districtBoundaryCache;
  }

  const layer = await readJson(
    path.join(repoRoot, "src", "site", "assets", "kerala-districts.geojson"),
    { type: "FeatureCollection", features: [] }
  );
  districtBoundaryCache = parseDistrictBoundaries(layer);
  return districtBoundaryCache;
}

function districtIdsFromPolygons(polygons, districtBoundaries) {
  const districtIds = new Set();

  for (const polygon of polygons) {
    for (const districtBoundary of districtBoundaries) {
      const samplePoints = [
        districtBoundary.representative_point,
        districtBoundary.centroid
      ].filter(Boolean);
      if (samplePoints.some((point) => pointInGeometry([point.lon, point.lat], polygon))) {
        districtIds.add(districtBoundary.district_id);
      }
    }
  }

  return [...districtIds];
}

function parseCapXmlDetail(detailXml, districtBoundaries) {
  const title = readFirstTag(detailXml, ["headline", "title"]) ?? "";
  const description = readFirstTag(detailXml, ["description"]) ?? "";
  const instruction = readFirstTag(detailXml, ["instruction"]) ?? "";
  const areaDesc = readFirstTag(detailXml, ["areaDesc"]) ?? "";
  const severityText = readFirstTag(detailXml, ["severity"]) ?? "";
  const categoryValues = readCategoryValues(detailXml);
  const geocodes = parseCapGeocodes(detailXml);
  const polygons = [...detailXml.matchAll(/<(?:(?:\w+):)?polygon[^>]*>([\s\S]*?)<\/(?:(?:\w+):)?polygon>/gi)]
    .map((match) => parseCapPolygon(match[1]))
    .filter(Boolean);

  const geocodeText = geocodes
    .map((geocode) => `${geocode.value_name ?? ""} ${geocode.value ?? ""}`.trim())
    .join(" ");
  const text = `${title} ${description} ${instruction} ${areaDesc} ${geocodeText} ${severityText}`.trim();
  const polygonDistricts = districtIdsFromPolygons(polygons, districtBoundaries);
  const textDistricts = findDistrictIds(text);

  return {
    identifier: readFirstTag(detailXml, ["identifier"]) ?? null,
    sent: parseDate(readFirstTag(detailXml, ["sent"]))?.toISOString() ?? null,
    title,
    description,
    instruction: instruction || null,
    area_desc: areaDesc || null,
    severity_text: severityText || null,
    categories: categoryValues,
    geocodes,
    polygons,
    severity: inferSeverity(text),
    districts: [...new Set([...polygonDistricts, ...textDistricts])]
  };
}

function findDistrictIds(text) {
  const lower = text.toLowerCase();
  return districts
    .filter((district) => lower.includes(district.name.toLowerCase()))
    .map((district) => district.id);
}

function inferSeverity(text) {
  const matches = severityKeywords
    .filter(({ pattern }) => pattern.test(text))
    .map(({ value }) => value);
  return matches.length ? Math.max(...matches) : 0;
}

function latestPublishedAt(items) {
  const timestamps = items
    .map((item) => parseDate(item.published_at))
    .filter(Boolean)
    .map((date) => date.getTime());

  if (!timestamps.length) {
    return null;
  }

  return new Date(Math.max(...timestamps));
}

function filterRecentCapItems(items, activeWindowHours = 48) {
  const latest = latestPublishedAt(items);
  if (!latest) {
    return {
      activeItems: items,
      filteredCount: 0,
      latestPublishedAt: null
    };
  }

  const threshold = new Date(latest.getTime() - activeWindowHours * 60 * 60 * 1000);
  const activeItems = items.filter((item) => {
    const publishedAt = parseDate(item.published_at);
    return publishedAt ? publishedAt.getTime() >= threshold.getTime() : false;
  });

  return {
    activeItems,
    filteredCount: items.length - activeItems.length,
    latestPublishedAt: latest.toISOString()
  };
}

async function parseImdCapItems(raw) {
  const rawItems = [...raw.matchAll(/<(item|entry)\b[^>]*>([\s\S]*?)<\/\1>/gi)];
  const items = rawItems.map((match) => {
    const itemText = match[2];
    const title = readFirstTag(itemText, ["title", "headline"]) ?? "";
    const description = readFirstTag(itemText, ["description", "summary", "content", "instruction"]) ?? "";
    const areaDesc = readFirstTag(itemText, ["areaDesc"]) ?? "";
    const pubDate = readFirstTag(itemText, ["pubDate", "published", "updated", "sent"]);
    const link = readLink(itemText);
    const categories = readCategoryValues(itemText);
    const severityText = readFirstTag(itemText, ["severity"]) ?? "";
    const text = `${title} ${description} ${areaDesc} ${severityText}`.trim();
    return {
      title,
      description,
      area_desc: areaDesc || null,
      categories,
      link,
      published_at: parseDate(pubDate)?.toISOString() ?? null,
      severity: inferSeverity(text),
      districts: findDistrictIds(text)
    };
  });

  const filteredItems = items.filter((item) => {
    if (item.categories.length === 0) {
      return true;
    }
    return item.categories.some((category) => /met/i.test(category));
  });

  return {
    items: filteredItems,
    issued_at: filteredItems[0]?.published_at ?? items[0]?.published_at ?? null
  };
}

export async function parseImdCapRss(repoRootOrRaw, source = null, rawInput = null) {
  const repoRoot = rawInput ? repoRootOrRaw : null;
  const raw = rawInput ?? repoRootOrRaw;

  let payload = null;
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = null;
  }

  const rssRaw = payload?.rss ?? raw;
  const base = await parseImdCapItems(rssRaw);
  const activeWindowHours = Number(source?.active_window_hours ?? 48);

  if (!payload?.details?.length || !repoRoot) {
    const filtered = filterRecentCapItems(base.items, activeWindowHours);
    return {
      issued_at: filtered.latestPublishedAt ?? base.issued_at,
      item_count: filtered.activeItems.length,
      raw_item_count: base.items.length,
      filtered_item_count: filtered.filteredCount,
      max_severity: filtered.activeItems.length ? Math.max(...filtered.activeItems.map((item) => item.severity)) : 0,
      kerala_district_ids: [...new Set(filtered.activeItems.flatMap((item) => item.districts))],
      items: filtered.activeItems
    };
  }

  const districtBoundaries = await loadLocalDistrictBoundaries(repoRoot);
  const detailByLink = new Map();
  const detailByIdentifier = new Map();

  for (const detail of payload.details) {
    const parsedDetail = parseCapXmlDetail(detail.xml, districtBoundaries);
    if (detail.link) {
      detailByLink.set(detail.link, parsedDetail);
    }
    if (detail.identifier || parsedDetail.identifier) {
      detailByIdentifier.set(detail.identifier ?? parsedDetail.identifier, parsedDetail);
    }
  }

  const mergedItems = base.items.map((item) => {
    const identifier = item.link?.match(/[?&]identifier=([^&]+)/i)?.[1] ?? null;
    const detail =
      detailByLink.get(item.link) ??
      (identifier ? detailByIdentifier.get(identifier) : null) ??
      null;

    if (!detail) {
      return item;
    }

    const combinedText = `${detail.title} ${detail.description} ${detail.instruction ?? ""} ${detail.area_desc ?? ""}`.trim();
    const severity = Math.max(item.severity, detail.severity);

    return {
      ...item,
      title: detail.title || item.title,
      description: detail.description || item.description,
      instruction: detail.instruction,
      area_desc: detail.area_desc,
      categories: detail.categories.length ? detail.categories : item.categories,
      published_at: detail.sent ?? item.published_at,
      severity,
      districts: detail.districts.length ? detail.districts : [...new Set([...item.districts, ...findDistrictIds(combinedText)])],
      geocodes: detail.geocodes,
      polygons: detail.polygons
    };
  });

  const filtered = filterRecentCapItems(mergedItems, activeWindowHours);

  return {
    issued_at: filtered.latestPublishedAt ?? base.issued_at ?? null,
    item_count: filtered.activeItems.length,
    raw_item_count: mergedItems.length,
    filtered_item_count: filtered.filteredCount,
    max_severity: filtered.activeItems.length ? Math.max(...filtered.activeItems.map((item) => item.severity)) : 0,
    kerala_district_ids: [...new Set(filtered.activeItems.flatMap((item) => item.districts))],
    items: filtered.activeItems
  };
}

export function parseImdFlashFloodBulletin(raw) {
  const text = stripHtml(raw);
  return {
    summary: text,
    severity: inferSeverity(text),
    kerala_district_ids: findDistrictIds(text),
    issued_at: parseDate(text.match(/Issued on ([^.]*)/i)?.[1])?.toISOString() ?? null
  };
}

function keywordHit(text, patterns) {
  return patterns.some((pattern) => pattern.test(text));
}

export function parseKsdmaReservoirs(raw) {
  const text = stripHtml(raw);
  return {
    summary: text,
    alert_active: keywordHit(text, [/\balert\b/i, /\bcaution\b/i]),
    districts: findDistrictIds(text),
    severity: inferSeverity(text)
  };
}

export function parseKsdmaDamManagement(raw) {
  const text = stripHtml(raw);
  return {
    summary: text,
    release_preparedness: keywordHit(text, [/\bspillway\b/i, /\brelease\b/i, /\bdownstream\b/i]),
    districts: findDistrictIds(text),
    severity: inferSeverity(text)
  };
}

export function parseCwcFfs(raw) {
  const text = stripHtml(raw);
  return {
    summary: text,
    warning: keywordHit(text, [/\bwarning\b/i]),
    watch: keywordHit(text, [/\bwatch\b/i]),
    districts: findDistrictIds(text),
    severity: inferSeverity(text)
  };
}

export function parseNasaImergNrt(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { districts: [], taluks: [] };
  }

  return {
    issued_at: payload.issued_at ?? null,
    districts: Array.isArray(payload.districts) ? payload.districts : [],
    taluks: Array.isArray(payload.taluks) ? payload.taluks : [],
    source_files: payload.source_files ?? null
  };
}

export function parseRainviewerRadar(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { districts: [], hotspots: [] };
  }

  return {
    issued_at: payload.issued_at ?? payload.frame_time ?? null,
    generated_at: payload.generated_at ?? null,
    frame_time: payload.frame_time ?? null,
    frame_path: payload.frame_path ?? null,
    color_scheme: payload.color_scheme ?? null,
    districts: Array.isArray(payload.districts) ? payload.districts : [],
    hotspots: Array.isArray(payload.hotspots) ? payload.hotspots : []
  };
}

export async function parseOperatorObservations(repoRoot, source, raw = null) {
  if (raw) {
    try {
      return JSON.parse(raw);
    } catch {
      return { active: false, districts: [] };
    }
  }

  return readJson(path.join(repoRoot, source.path), {
    active: false,
    districts: []
  });
}

export const parserRegistry = {
  imdCapRss: parseImdCapRss,
  imdFlashFloodBulletin: parseImdFlashFloodBulletin,
  ksdmaReservoirs: parseKsdmaReservoirs,
  ksdmaDamManagement: parseKsdmaDamManagement,
  cwcFfs: parseCwcFfs,
  nasaImergNrt: parseNasaImergNrt,
  rainviewerRadar: parseRainviewerRadar,
  operatorObservations: parseOperatorObservations
};
