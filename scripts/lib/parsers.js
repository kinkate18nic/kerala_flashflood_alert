import path from "node:path";
import { readJson } from "./fs.js";
import { districts } from "../../src/shared/areas.js";
import { parseDate } from "./time.js";
import { severityKeywords } from "../../src/shared/risk.js";

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

export function parseImdCapRss(raw) {
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
    issued_at: filteredItems[0]?.published_at ?? items[0]?.published_at ?? null,
    item_count: filteredItems.length,
    max_severity: filteredItems.length ? Math.max(...filteredItems.map((item) => item.severity)) : 0,
    kerala_district_ids: [...new Set(filteredItems.flatMap((item) => item.districts))],
    items: filteredItems
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
