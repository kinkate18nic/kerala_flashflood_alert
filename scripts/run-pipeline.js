import path from "node:path";
import { fileURLToPath } from "node:url";
import { runPipeline } from "./lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const useFixtures = process.argv.includes("--fixtures");
const sourceArg = process.argv.find((argument) => argument.startsWith("--sources="));
const sourceIds = (sourceArg?.split("=")[1] ?? process.env.SOURCE_IDS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

await runPipeline(repoRoot, {
  useFixtures,
  sourceIds: sourceIds.length ? sourceIds : null
});
