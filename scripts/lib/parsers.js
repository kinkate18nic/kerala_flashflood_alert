import path from "node:path";
import { readJson } from "./fs.js";
import { districts, hotspots } from "../../src/shared/areas.js";
import { parseDate } from "./time.js";
import { severityKeywords } from "../../src/shared/risk.js";
import { parseDistrictBoundaries, pointInGeometry } from "./boundaries.js";
import capGeocodeMappings from "../../config/imd-cap-geocodes.json" with { type: "json" };

function stripHtml(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const districtNameLookup = new Map(
  districts.map((district) => [district.name.trim().toUpperCase(), district.id])
);

function districtIdFromImdTitle(title) {
  return districtNameLookup.get(String(title ?? "").trim().toUpperCase()) ?? null;
}

function decodeEmbeddedHtml(value) {
  return String(value ?? "")
    .replace(/<\s*(\d+(?:\.\d+)?)\s*mm\/hr/gi, "less than $1 mm/hr")
    .replace(/\\\//g, "/")
    .replace(/\\"/g, "\"")
    .replace(/\\r/g, "\r")
    .replace(/\\n/g, "\n")
    .replace(/<\/?img[^>]*>/gi, " ")
    .replace(/<\/?br\s*\/?>/gi, "\n")
    .replace(/&nbsp;/gi, " ")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'");
}

function parseImdLocalDateTime(dateText, timeText) {
  const normalizedDate = String(dateText ?? "").trim();
  const normalizedTime = String(timeText ?? "")
    .trim()
    .match(/(\d{1,2})(\d{2})/) ?? null;

  if (!normalizedDate || !normalizedTime) {
    return null;
  }

  const [, hourPart, minutePart] = normalizedTime;
  return parseDate(`${normalizedDate}T${hourPart.padStart(2, "0")}:${minutePart}:00+05:30`)?.toISOString() ?? null;
}

function haversineDistanceKm(left, right) {
  const toRadians = (value) => (value * Math.PI) / 180;
  const earthRadiusKm = 6371;
  const dLat = toRadians(right.lat - left.lat);
  const dLon = toRadians(right.lon - left.lon);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(left.lat)) *
      Math.cos(toRadians(right.lat)) *
      Math.sin(dLon / 2) ** 2;
  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function colorSeverity(color, fallback = 0) {
  switch (String(color ?? "").trim().toUpperCase()) {
    case "#008000":
      return 0;
    case "#FFFF00":
      return Math.max(fallback, 0.22);
    case "#FFA500":
      return Math.max(fallback, 0.45);
    case "#FF0000":
      return Math.max(fallback, 0.7);
    default:
      return fallback;
  }
}

function extractImdHazards(balloonText) {
  return [...decodeEmbeddedHtml(balloonText).matchAll(/<p>(.*?)<\/p>/gi)]
    .map((match) => stripHtml(match[1]))
    .map((entry) => entry.replace(/^Updated on:\s*/i, "").trim())
    .filter(
      (entry) =>
        entry &&
        !/^\d{4}-\d{2}-\d{2}$/.test(entry) &&
        !/^Updated on:/i.test(entry) &&
        !/^Time of issue:/i.test(entry) &&
        !/^Valid upto:/i.test(entry)
    );
}

function parseEmbeddedDistrictAreas(raw) {
  return districts
    .map((district) => {
      const title = district.name.toUpperCase();
      const pattern =
        `\\"title\\":\\s*\\"${title}\\"[\\s\\S]*?\\"color\\":\\s*\\"(?<color>#[0-9A-Fa-f]{6})\\"[\\s\\S]*?\\"balloonText\\":\\s*\\"(?<balloon>[\\s\\S]*?)\\"\\s*\\}`;
      const match = raw.match(new RegExp(pattern, "i"));
      if (!match?.groups) {
        return null;
      }
      return {
        district_id: district.id,
        district_name: district.name,
        color: match.groups.color,
        balloon_text: match.groups.balloon
      };
    })
    .filter(Boolean);
}

function warningSeverityFromText(text) {
  const normalized = String(text ?? "").toLowerCase();
  if (!normalized || /no warning/.test(normalized)) {
    return 0;
  }
  let severity = 0;
  if (/very heavy rain|extremely heavy rain/.test(normalized)) {
    severity = Math.max(severity, 0.6);
  }
  if (/heavy rain/.test(normalized)) {
    severity = Math.max(severity, 0.35);
  }
  if (/thunderstorm|lightning|squall|strong surface winds|hailstorm/.test(normalized)) {
    severity = Math.max(severity, 0.22);
  }
  return severity;
}

function nowcastSeverityFromText(text) {
  const normalized = String(text ?? "").toLowerCase();
  if (!normalized || /no warning/.test(normalized)) {
    return 0;
  }
  let severity = 0;
  if (/very heavy rain|extremely heavy rain/.test(normalized)) {
    severity = Math.max(severity, 0.75);
  }
  if (/heavy rain/.test(normalized)) {
    severity = Math.max(severity, 0.55);
  }
  if (/moderate rain/.test(normalized)) {
    severity = Math.max(severity, 0.35);
  }
  if (/light rain/.test(normalized)) {
    severity = Math.max(severity, 0.18);
  }
  if (/thunderstorm|lightning|squall/.test(normalized)) {
    severity = Math.max(severity, 0.28);
  }
  return severity;
}

function parseImdDistrictWarningDate(raw) {
  const checkedMatch = raw.match(/value="Day_1"\s+checked="true"\s*>([^<]+)/i);
  if (!checkedMatch?.[1]) {
    return null;
  }
  return parseDate(`${checkedMatch[1].trim()} 00:00:00 +05:30`)?.toISOString() ?? null;
}

function parseImdDistrictWarningPage(raw) {
  const entries = parseEmbeddedDistrictAreas(raw).map((entry) => {
    const cleanedBalloon = decodeEmbeddedHtml(entry.balloon_text);
    const cleanedText = stripHtml(cleanedBalloon);
    const hazards = extractImdHazards(entry.balloon_text);
    const updatedMatch = cleanedText.match(/Updated on:\s*(\d{4}-\d{2}-\d{2})/i);
    const updatedAt = updatedMatch?.[1]
      ? parseDate(`${updatedMatch[1]}T00:00:00+05:30`)?.toISOString() ?? null
      : null;
    const severity = colorSeverity(entry.color, warningSeverityFromText(cleanedText));
    return {
      district_id: entry.district_id,
      district_name: entry.district_name,
      color: entry.color,
      severity,
      warning_text: cleanedText,
      hazards,
      updated_at: updatedAt
    };
  });

  const issuedAt = entries
    .map((entry) => parseDate(entry.updated_at))
    .filter(Boolean)
    .sort((left, right) => right.getTime() - left.getTime())[0]
    ?.toISOString() ?? parseImdDistrictWarningDate(raw);

  return {
    issued_at: issuedAt,
    forecast_date: parseImdDistrictWarningDate(raw),
    district_count: entries.length,
    active_district_count: entries.filter((entry) => entry.severity > 0).length,
    districts: entries
  };
}

function parseImdDistrictNowcastPage(raw, source = null) {
  const referenceTime = parseDate(source?.reference_time) ?? new Date();
  const entries = parseEmbeddedDistrictAreas(raw).map((entry) => {
    const cleanedBalloon = decodeEmbeddedHtml(entry.balloon_text);
    const cleanedText = stripHtml(cleanedBalloon);
    const issueMatch = cleanedText.match(/Time of issue:\s*(\d{4}-\d{2}-\d{2})\s*(\d{3,4})\s*Hrs/i);
    const validMatch = cleanedText.match(/Valid upto:\s*(\d{3,4})\s*Hrs/i);
    const issuedAt = issueMatch ? parseImdLocalDateTime(issueMatch[1], issueMatch[2]) : null;
    const validUntil = issueMatch && validMatch
      ? parseImdLocalDateTime(issueMatch[1], validMatch[1])
      : null;
    const severity = colorSeverity(entry.color, nowcastSeverityFromText(cleanedText));
    return {
      district_id: entry.district_id,
      district_name: entry.district_name,
      color: entry.color,
      severity,
      nowcast_text: cleanedText,
      issued_at: issuedAt,
      valid_until: validUntil,
      active: validUntil ? parseDate(validUntil)?.getTime() > referenceTime.getTime() : severity > 0
    };
  });

  const activeEntries = entries.filter((entry) => entry.active);
  const issuedAt = entries
    .map((entry) => parseDate(entry.issued_at))
    .filter(Boolean)
    .sort((left, right) => right.getTime() - left.getTime())[0]
    ?.toISOString() ?? null;

  return {
    issued_at: issuedAt,
    district_count: entries.length,
    active_district_count: activeEntries.filter((entry) => entry.severity > 0).length,
    filtered_item_count: entries.length - activeEntries.length,
    districts: activeEntries
  };
}

function parseEmbeddedStationMarkers(raw) {
  const quotedImagesMatch = raw.match(
    /"images"\s*:\s*(\[[\s\S]*?\])\s*(?:,\s*"areas"\s*:|,\s*"legend"\s*:|\})/i
  );
  if (quotedImagesMatch?.[1]) {
    try {
      const images = JSON.parse(quotedImagesMatch[1]);
      const markers = images
        .map((entry) => {
          const latitude = Number.parseFloat(entry.latitude);
          const longitude = Number.parseFloat(entry.longitude);
          if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
            return null;
          }
          return {
            title: decodeEmbeddedHtml(entry.title).trim(),
            latitude,
            longitude,
            image_url: decodeEmbeddedHtml(entry.imageURL).trim(),
            description: decodeEmbeddedHtml(entry.description).trim()
          };
        })
        .filter(Boolean);
      if (markers.length > 0) {
        return markers;
      }
    } catch {
      // Fall through to the legacy parser for older IMD page shapes.
    }
  }

  const markers = [];
  const markerPattern =
    /(?:^|[,{])\s*title\s*:\s*"(?<title>[^"]+)"[\s\S]*?latitude\s*:\s*"?(?<latitude>-?\d+(?:\.\d+)?)"?[\s\S]*?longitude\s*:\s*"?(?<longitude>-?\d+(?:\.\d+)?)"?[\s\S]*?imageURL\s*:\s*"(?<imageURL>[^"]+)"[\s\S]*?description\s*:\s*"(?<description>[\s\S]*?)"\s*(?:[,}])/gi;

  for (const match of raw.matchAll(markerPattern)) {
    if (!match.groups) {
      continue;
    }
    const latitude = Number.parseFloat(match.groups.latitude);
    const longitude = Number.parseFloat(match.groups.longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      continue;
    }
    markers.push({
      title: decodeEmbeddedHtml(match.groups.title).trim(),
      latitude,
      longitude,
      image_url: decodeEmbeddedHtml(match.groups.imageURL).trim(),
      description: decodeEmbeddedHtml(match.groups.description).trim()
    });
  }

  return markers;
}

function parseImdStationNowcastPage(raw, source = null) {
  const referenceTime = parseDate(source?.reference_time) ?? new Date();
  const stations = parseEmbeddedStationMarkers(raw).map((entry) => {
    const cleanedText = stripHtml(entry.description);
    const issueMatch = cleanedText.match(/Time of issue:\s*(\d{4}-\d{2}-\d{2})\s*(\d{3,4})\s*Hrs/i);
    const validMatch = cleanedText.match(/Valid upto:\s*(\d{3,4})\s*Hrs/i);
    const issuedAt = issueMatch ? parseImdLocalDateTime(issueMatch[1], issueMatch[2]) : null;
    const validUntil =
      issueMatch && validMatch ? parseImdLocalDateTime(issueMatch[1], validMatch[1]) : null;
    const severity = nowcastSeverityFromText(cleanedText);
    const active = validUntil ? parseDate(validUntil)?.getTime() > referenceTime.getTime() : severity > 0;
    return {
      station_name: entry.title,
      latitude: entry.latitude,
      longitude: entry.longitude,
      image_url: entry.image_url,
      nowcast_text: cleanedText,
      severity,
      issued_at: issuedAt,
      valid_until: validUntil,
      active
    };
  });

  const activeStations = stations.filter((entry) => entry.active && entry.severity > 0);
  const hotspotMatches = new Map();

  for (const station of activeStations) {
    for (const hotspot of hotspots) {
      if (!hotspot.location) {
        continue;
      }
      const distanceKm = haversineDistanceKm(
        { lat: station.latitude, lon: station.longitude },
        { lat: hotspot.location.lat, lon: hotspot.location.lon }
      );
      if (distanceKm > 20) {
        continue;
      }
      const current = hotspotMatches.get(hotspot.id);
      const candidate = {
        hotspot_id: hotspot.id,
        hotspot_name: hotspot.name,
        district_id: hotspot.district_id,
        station_name: station.station_name,
        distance_km: Math.round(distanceKm * 10) / 10,
        severity: station.severity,
        issued_at: station.issued_at,
        valid_until: station.valid_until,
        nowcast_text: station.nowcast_text
      };
      if (
        !current ||
        candidate.severity > current.severity ||
        (candidate.severity === current.severity && candidate.distance_km < current.distance_km)
      ) {
        hotspotMatches.set(hotspot.id, candidate);
      }
    }
  }

  const issuedAt = stations
    .map((entry) => parseDate(entry.issued_at))
    .filter(Boolean)
    .sort((left, right) => right.getTime() - left.getTime())[0]
    ?.toISOString() ?? null;

  return {
    issued_at: issuedAt,
    station_count: stations.length,
    active_station_count: activeStations.length,
    hotspot_count: hotspotMatches.size,
    stations: activeStations,
    hotspots: [...hotspotMatches.values()]
  };
}

function readTag(fragment, tagName) {
  const match = fragment.match(
    new RegExp(`<(?:(?:\\w+):)?${tagName}\\b[^>]*>([\\s\\S]*?)<\\/(?:(?:\\w+):)?${tagName}>`, "i")
  );
  return match?.[1]?.trim() ?? null;
}

function readTagAttribute(fragment, tagName, attributeName) {
  const match = fragment.match(
    new RegExp(`<(?:(?:\\w+):)?${tagName}\\b[^>]*\\b${attributeName}="([^"]+)"[^>]*\\/?>`, "i")
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

function normalizeGeocodeValueName(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function districtIdsFromGeocodes(geocodes) {
  const districtIds = new Set();

  for (const geocode of geocodes) {
    const valueName = normalizeGeocodeValueName(geocode.value_name);
    const mappingTable = capGeocodeMappings.value_name_mappings?.[valueName];
    if (!mappingTable) {
      continue;
    }

    const districtId = mappingTable[String(geocode.value ?? "").trim()];
    if (districtId) {
      districtIds.add(districtId);
    }
  }

  return [...districtIds];
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
  const effectiveAt = parseDate(readFirstTag(detailXml, ["effective"]))?.toISOString() ?? null;
  const onsetAt = parseDate(readFirstTag(detailXml, ["onset"]))?.toISOString() ?? null;
  const expiresAt = parseDate(readFirstTag(detailXml, ["expires"]))?.toISOString() ?? null;
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
  const geocodeDistricts = districtIdsFromGeocodes(geocodes);
  const textDistricts = findDistrictIds(text);

  return {
    identifier: readFirstTag(detailXml, ["identifier"]) ?? null,
    sent: parseDate(readFirstTag(detailXml, ["sent"]))?.toISOString() ?? null,
    effective_at: effectiveAt,
    onset_at: onsetAt,
    expires_at: expiresAt,
    title,
    description,
    instruction: instruction || null,
    area_desc: areaDesc || null,
    severity_text: severityText || null,
    categories: categoryValues,
    geocodes,
    polygons,
    severity: inferSeverity(text),
    districts: [...new Set([...polygonDistricts, ...geocodeDistricts, ...textDistricts])]
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

function filterRecentCapItems(items, referenceTime = null) {
  const latest = latestPublishedAt(items);
  if (!items.length) {
    return {
      activeItems: [],
      filteredCount: 0,
      latestPublishedAt: null
    };
  }

  const effectiveReference = parseDate(referenceTime) ?? new Date();
  const activeItems = items.filter((item) => {
    const expiresAt = parseDate(item.expires_at);
    return expiresAt ? expiresAt.getTime() > effectiveReference.getTime() : false;
  });

  return {
    activeItems,
    filteredCount: items.length - activeItems.length,
    latestPublishedAt: latest?.toISOString() ?? null
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
  const referenceTime = source?.reference_time ?? null;

  if (!payload?.details?.length || !repoRoot) {
    const filtered = filterRecentCapItems(base.items, referenceTime);
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
      effective_at: detail.effective_at,
      onset_at: detail.onset_at,
      expires_at: detail.expires_at,
      severity,
      districts: detail.districts.length ? detail.districts : [...new Set([...item.districts, ...findDistrictIds(combinedText)])],
      geocodes: detail.geocodes,
      polygons: detail.polygons
    };
  });

  const filtered = filterRecentCapItems(mergedItems, referenceTime);

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

export function parseImdDistrictWarning(raw) {
  return parseImdDistrictWarningPage(raw);
}

export function parseImdDistrictNowcast(raw, source = null) {
  return parseImdDistrictNowcastPage(raw, source);
}

export function parseImdStationNowcast(raw, source = null) {
  return parseImdStationNowcastPage(raw, source);
}

function keywordHit(text, patterns) {
  return patterns.some((pattern) => pattern.test(text));
}

export function parseKsdmaReservoirs(raw) {
  try {
    const payload = JSON.parse(raw);
    return {
      issued_at: payload.issued_at ?? null,
      department: payload.department ?? "kseb",
      pdf_url: payload.pdf_url ?? null,
      pdf_label: payload.pdf_label ?? null,
      dam_count: payload.dam_count ?? 0,
      release_dam_count: payload.release_dam_count ?? 0,
      alert_active: Boolean(payload.alert_active),
      release_preparedness: Boolean(payload.release_preparedness),
      districts: Array.isArray(payload.districts) ? payload.districts : [],
      dams: Array.isArray(payload.dams) ? payload.dams : []
    };
  } catch {
    const text = stripHtml(raw);
    return {
      summary: text,
      alert_active: keywordHit(text, [/\balert\b/i, /\bcaution\b/i]),
      districts: findDistrictIds(text),
      severity: inferSeverity(text)
    };
  }
}

export function parseKsdmaDamManagement(raw) {
  try {
    const payload = JSON.parse(raw);
    return {
      issued_at: payload.issued_at ?? null,
      department: payload.department ?? "irrigation",
      pdf_url: payload.pdf_url ?? null,
      pdf_label: payload.pdf_label ?? null,
      dam_count: payload.dam_count ?? 0,
      release_dam_count: payload.release_dam_count ?? 0,
      alert_active: Boolean(payload.alert_active),
      release_preparedness: Boolean(payload.release_preparedness),
      districts: Array.isArray(payload.districts) ? payload.districts : [],
      dams: Array.isArray(payload.dams) ? payload.dams : []
    };
  } catch {
    const text = stripHtml(raw);
    return {
      summary: text,
      release_preparedness: keywordHit(text, [/\bspillway\b/i, /\brelease\b/i, /\bdownstream\b/i]),
      districts: findDistrictIds(text),
      severity: inferSeverity(text)
    };
  }
}

export function parseCwcFfs(raw) {
  try {
    const payload = JSON.parse(raw);
    return {
      issued_at: payload.issued_at ?? null,
      districts: Array.isArray(payload.districts) ? payload.districts : [],
      station_count: payload.station_count ?? 0,
      requested_station_count: payload.requested_station_count ?? 0,
      successful_station_count: payload.successful_station_count ?? 0,
      failed_stations: Array.isArray(payload.failed_stations) ? payload.failed_stations : [],
      partial_failure_count:
        payload.partial_failure_count ??
        (Array.isArray(payload.failed_stations) ? payload.failed_stations.length : 0),
      above_warning_station_count: payload.above_warning_station_count ?? 0,
      above_danger_station_count: payload.above_danger_station_count ?? 0,
      forecast_warning_station_count: payload.forecast_warning_station_count ?? 0,
      forecast_danger_station_count: payload.forecast_danger_station_count ?? 0,
      warning: Boolean(payload.warning),
      watch: Boolean(payload.watch)
    };
  } catch {
    const text = stripHtml(raw);
    return {
      summary: text,
      warning: keywordHit(text, [/\bwarning\b/i]),
      watch: keywordHit(text, [/\bwatch\b/i]),
      districts: findDistrictIds(text),
      severity: inferSeverity(text)
    };
  }
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

export function parseIndiaWrisRainfall(raw) {
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
    station_count: payload.station_count ?? 0,
    requested_district_count: payload.requested_district_count ?? 0,
    successful_district_count: payload.successful_district_count ?? 0,
    failed_districts: Array.isArray(payload.failed_districts) ? payload.failed_districts : [],
    partial_failure_count: Array.isArray(payload.failed_districts) ? payload.failed_districts.length : 0
  };
}

export function parseIndiaWrisRiverLevel(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { districts: [] };
  }

  return {
    issued_at: payload.issued_at ?? null,
    districts: Array.isArray(payload.districts) ? payload.districts : [],
    requested_district_count: payload.requested_district_count ?? 0,
    successful_district_count: payload.successful_district_count ?? 0,
    failed_districts: Array.isArray(payload.failed_districts) ? payload.failed_districts : [],
    partial_failure_count: Array.isArray(payload.failed_districts) ? payload.failed_districts.length : 0
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
  imdDistrictWarning: parseImdDistrictWarning,
  imdDistrictNowcast: parseImdDistrictNowcast,
  imdStationNowcast: parseImdStationNowcast,
  ksdmaReservoirs: parseKsdmaReservoirs,
  ksdmaDamManagement: parseKsdmaDamManagement,
  cwcFfs: parseCwcFfs,
  nasaImergNrt: parseNasaImergNrt,
  rainviewerRadar: parseRainviewerRadar,
  indiaWrisRainfall: parseIndiaWrisRainfall,
  indiaWrisRiverLevel: parseIndiaWrisRiverLevel,
  operatorObservations: parseOperatorObservations
};
