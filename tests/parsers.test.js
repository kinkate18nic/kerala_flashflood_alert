import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseImdCapRss,
  parseImdFlashFloodBulletin,
  parseCwcFfs
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
