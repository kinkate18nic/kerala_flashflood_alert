import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseImdCapRss,
  parseImdFlashFloodBulletin,
  parseCwcFfs,
  parseImdStationNowcast
} from "../scripts/lib/parsers.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

test("parseImdCapRss extracts district-targeted warning items", async () => {
  const raw = await readFile(path.join(repoRoot, "fixtures", "imd-cap-rss.xml"), "utf8");
  const parsed = parseImdCapRss(raw);

  assert.equal(parsed.item_count, 2);
  assert.ok(parsed.kerala_district_ids.includes("idukki"));
  assert.ok(parsed.items[0].severity > parsed.items[1].severity);
});

test("parseImdFlashFloodBulletin finds referenced districts", async () => {
  const raw = await readFile(path.join(repoRoot, "fixtures", "imd-flash-flood-bulletin.html"), "utf8");
  const parsed = parseImdFlashFloodBulletin(raw);

  assert.ok(parsed.kerala_district_ids.includes("ernakulam"));
  assert.ok(parsed.summary.includes("Kerala"));
});

test("parseCwcFfs classifies watch and warning language", async () => {
  const raw = await readFile(path.join(repoRoot, "fixtures", "cwc-ffs.html"), "utf8");
  const parsed = parseCwcFfs(raw);

  assert.equal(parsed.warning, true);
  assert.equal(parsed.watch, true);
  assert.ok(parsed.districts.includes("ernakulam"));
});

test("parseImdStationNowcast supports the current quoted IMD map format", async () => {
  const raw = await readFile(path.join(repoRoot, "fixtures", "imd-station-nowcast-live.html"), "utf8");
  const parsed = parseImdStationNowcast(raw, {
    reference_time: "2026-04-09T09:30:00+05:30"
  });

  assert.equal(parsed.station_count, 3);
  assert.equal(parsed.active_station_count, 1);
  assert.equal(parsed.issued_at, "2026-04-09T09:30:00.000Z");
  assert.ok(parsed.stations.some((station) => station.station_name === "Pathanamthitta"));
});
