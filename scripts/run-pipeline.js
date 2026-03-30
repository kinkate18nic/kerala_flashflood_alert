import path from "node:path";
import { fileURLToPath } from "node:url";
import { runPipeline } from "./lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const useFixtures = process.argv.includes("--fixtures");
const sourceArg = process.argv.find((argument) => argument.startsWith("--sources="));
const cadenceReuseArg = process.argv.find((argument) => argument.startsWith("--enable-cadence-reuse="));
const sourceIds = (sourceArg?.split("=")[1] ?? process.env.SOURCE_IDS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const enableCadenceReuseRaw =
  cadenceReuseArg?.split("=")[1] ??
  process.env.ENABLE_CADENCE_REUSE ??
  "";
const enableCadenceReuse = ["1", "true", "yes", "on"].includes(
  String(enableCadenceReuseRaw).trim().toLowerCase()
);

await runPipeline(repoRoot, {
  useFixtures,
  sourceIds: sourceIds.length ? sourceIds : null,
  enableCadenceReuse
});
