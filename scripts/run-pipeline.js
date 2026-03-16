import path from "node:path";
import { fileURLToPath } from "node:url";
import { runPipeline } from "./lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const useFixtures = process.argv.includes("--fixtures");

await runPipeline(repoRoot, { useFixtures });
