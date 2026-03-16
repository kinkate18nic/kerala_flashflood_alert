import path from "node:path";
import { fileURLToPath } from "node:url";
import { copyTree, ensureDir, readJson, writeJson, writeText } from "./lib/fs.js";
import { districts, hotspots } from "../src/shared/areas.js";
import { buildHotspotFootprint } from "../src/shared/hotspot-footprints.js";
import { alertLevels } from "../src/shared/risk.js";
import { buildDistrictTerrainLookup } from "../src/shared/terrain.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const sourceSiteDir = path.join(repoRoot, "src", "site");
const targetSiteDir = path.join(repoRoot, "docs");
await ensureDir(targetSiteDir);
await copyTree(sourceSiteDir, targetSiteDir);
const terrainStats = await readJson(path.join(repoRoot, "config", "terrain-stats.json"), {
  districts: []
});
const terrainByDistrict = buildDistrictTerrainLookup(districts, terrainStats);
const hotspotsWithFootprints = hotspots.map((hotspot) => ({
  ...hotspot,
  terrain_value: terrainByDistrict[hotspot.district_id]?.value ?? hotspot.susceptibility,
  footprint: buildHotspotFootprint(
    hotspot,
    terrainByDistrict[hotspot.district_id]?.value ?? hotspot.susceptibility
  )
}));

await writeJson(path.join(targetSiteDir, "data", "static", "areas.json"), {
  generated_at: new Date().toISOString(),
  districts,
  hotspots: hotspotsWithFootprints
});

await writeJson(path.join(targetSiteDir, "data", "static", "risk-metadata.json"), {
  generated_at: new Date().toISOString(),
  alert_levels: alertLevels
});

await writeText(path.join(targetSiteDir, ".nojekyll"), "\n");
