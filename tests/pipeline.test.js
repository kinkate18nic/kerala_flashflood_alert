import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { cp, mkdtemp, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { runPipeline } from "../scripts/lib/pipeline.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

test("runPipeline with fixtures publishes latest dashboard outputs", async () => {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kerala-flood-watch-"));

  await cp(path.join(repoRoot, "config"), path.join(tempRoot, "config"), { recursive: true });
  await cp(path.join(repoRoot, "data"), path.join(tempRoot, "data"), { recursive: true });
  await cp(path.join(repoRoot, "fixtures"), path.join(tempRoot, "fixtures"), { recursive: true });
  await cp(path.join(repoRoot, "src"), path.join(tempRoot, "src"), { recursive: true });

  await runPipeline(tempRoot, { useFixtures: true });

  const dashboardRaw = await readFile(path.join(tempRoot, "docs", "data", "latest", "dashboard.json"), "utf8");
  const dashboard = JSON.parse(dashboardRaw);
  assert.equal(dashboard.mode, "decision-support");
  assert.ok(typeof dashboard.headline_message === "string");
});
