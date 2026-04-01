import path from "node:path";
import { fileURLToPath } from "node:url";
import { runPipeline } from "./lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const useFixtures = process.argv.includes("--fixtures");
const sourceArg = process.argv.find((argument) => argument.startsWith("--sources="));
const cadenceReuseArg = process.argv.find((argument) => argument.startsWith("--enable-cadence-reuse="));
const cacheOnlyArg = process.argv.find((argument) => argument.startsWith("--cache-only="));
const writePublicArg = process.argv.find((argument) => argument.startsWith("--write-public-outputs="));
const writeArchiveArg = process.argv.find((argument) => argument.startsWith("--write-archive-outputs="));
const writeRuntimeDerivedArg = process.argv.find((argument) => argument.startsWith("--write-runtime-derived="));
const writeMetricsArg = process.argv.find((argument) => argument.startsWith("--write-metrics="));
const writeRawArg = process.argv.find((argument) => argument.startsWith("--write-raw-outputs="));
const sourceIds = (sourceArg?.split("=")[1] ?? process.env.SOURCE_IDS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

function readBooleanFlag(cliArg, envValue, fallback = false) {
  const rawValue = cliArg?.split("=")[1] ?? envValue ?? "";
  if (rawValue === "") {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(String(rawValue).trim().toLowerCase());
}

const enableCadenceReuse = readBooleanFlag(cadenceReuseArg, process.env.ENABLE_CADENCE_REUSE, false);
const cacheOnly = readBooleanFlag(cacheOnlyArg, process.env.CACHE_ONLY, false);
const writePublicOutputs = readBooleanFlag(writePublicArg, process.env.WRITE_PUBLIC_OUTPUTS, true);
const writeArchiveOutputs = readBooleanFlag(writeArchiveArg, process.env.WRITE_ARCHIVE_OUTPUTS, true);
const writeRuntimeDerived = readBooleanFlag(
  writeRuntimeDerivedArg,
  process.env.WRITE_RUNTIME_DERIVED,
  true
);
const writeMetrics = readBooleanFlag(writeMetricsArg, process.env.WRITE_METRICS, true);
const writeRawOutputs = readBooleanFlag(writeRawArg, process.env.WRITE_RAW_OUTPUTS, true);

await runPipeline(repoRoot, {
  useFixtures,
  sourceIds: sourceIds.length ? sourceIds : null,
  enableCadenceReuse,
  cacheOnly,
  writePublicOutputs,
  writeArchiveOutputs,
  writeRuntimeDerived,
  writeMetrics,
  writeRawOutputs
});
