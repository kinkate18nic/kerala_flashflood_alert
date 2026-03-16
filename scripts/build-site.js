import path from "node:path";
import { fileURLToPath } from "node:url";
import { copyTree, ensureDir, writeJson, writeText } from "./lib/fs.js";
import { districts, hotspots } from "../src/shared/areas.js";
import { alertLevels } from "../src/shared/risk.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const sourceSiteDir = path.join(repoRoot, "src", "site");
const targetSiteDir = path.join(repoRoot, "docs");
await ensureDir(targetSiteDir);
await copyTree(sourceSiteDir, targetSiteDir);

await writeJson(path.join(targetSiteDir, "data", "static", "areas.json"), {
  generated_at: new Date().toISOString(),
  districts,
  hotspots
});

await writeJson(path.join(targetSiteDir, "data", "static", "risk-metadata.json"), {
  generated_at: new Date().toISOString(),
  alert_levels: alertLevels
});

await writeText(path.join(targetSiteDir, ".nojekyll"), "\n");
