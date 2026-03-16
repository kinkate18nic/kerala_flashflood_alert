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
  const match = fragment.match(new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`, "i"));
  return match?.[1]?.trim() ?? null;
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
  const items = [...raw.matchAll(/<item>([\s\S]*?)<\/item>/gi)].map((match) => {
    const itemText = match[1];
    const title = readTag(itemText, "title") ?? "";
    const description = readTag(itemText, "description") ?? "";
    const pubDate = readTag(itemText, "pubDate");
    const link = readTag(itemText, "link");
    const text = `${title} ${description}`.trim();
    return {
      title,
      description,
      link,
      published_at: parseDate(pubDate)?.toISOString() ?? null,
      severity: inferSeverity(text),
      districts: findDistrictIds(text)
    };
  });

  return {
    item_count: items.length,
    max_severity: items.length ? Math.max(...items.map((item) => item.severity)) : 0,
    kerala_district_ids: [...new Set(items.flatMap((item) => item.districts))],
    items
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
    payload = { districts: [] };
  }

  return {
    issued_at: payload.issued_at ?? null,
    districts: Array.isArray(payload.districts) ? payload.districts : []
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
  operatorObservations: parseOperatorObservations
};
