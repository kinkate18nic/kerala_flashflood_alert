export function nowIso() {
  return new Date().toISOString();
}

export function parseDate(value, fallback = null) {
  if (!value) {
    return fallback;
  }
  const normalized = String(value)
    .replace(/\bIST\b/i, "+0530")
    .replace(/\s+UTC\b/i, "Z");
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? fallback : date;
}

export function minutesBetween(older, newer = new Date()) {
  if (!older) {
    return null;
  }
  return Math.max(0, Math.round((newer.getTime() - older.getTime()) / 60000));
}

export function toArchivePathParts(date = new Date()) {
  const year = date.getUTCFullYear().toString().padStart(4, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = String(date.getUTCHours()).padStart(2, "0");
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  const second = String(date.getUTCSeconds()).padStart(2, "0");
  return { year, month, day, stamp: `${hour}${minute}${second}` };
}
