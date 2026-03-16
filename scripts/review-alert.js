import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson, writeJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const idFlagIndex = process.argv.indexOf("--id");
if (idFlagIndex === -1 || !process.argv[idFlagIndex + 1]) {
  console.error("Usage: node scripts/review-alert.js --id <alert-id>");
  process.exit(1);
}

const alertId = process.argv[idFlagIndex + 1];
const approvalsPath = path.join(repoRoot, "data", "manual", "review-approvals.json");
const approvalsDocument = await readJson(approvalsPath, { approvals: [] });

if (approvalsDocument.approvals.some((approval) => approval.alert_id === alertId)) {
  console.log(`Alert ${alertId} is already approved.`);
  process.exit(0);
}

approvalsDocument.approvals.push({
  alert_id: alertId,
  approved_at: new Date().toISOString(),
  approved_by: process.env.USERNAME ?? "operator"
});

await writeJson(approvalsPath, approvalsDocument);
console.log(`Approved ${alertId}. Re-run the pipeline to publish the reviewed alert.`);
