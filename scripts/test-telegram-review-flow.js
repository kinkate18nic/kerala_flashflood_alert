import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson, writeJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const alertsPath = path.join(repoRoot, "docs", "data", "latest", "alerts.json");
const approvalsPath = path.join(repoRoot, "data", "manual", "review-approvals.json");
const rejectionsPath = path.join(repoRoot, "data", "manual", "review-rejections.json");
const dispatchLogPath = path.join(repoRoot, "runtime", "metrics", "telegram-dispatch-log.json");

const alertsDocument = await readJson(alertsPath, { generated_at: new Date().toISOString(), alerts: [] });
const approvalsDocument = await readJson(approvalsPath, { approvals: [] });
const rejectionsDocument = await readJson(rejectionsPath, { rejections: [] });
const dispatchLog = await readJson(dispatchLogPath, {
  sent_alert_ids: [],
  pending_review_notified_alert_ids: []
});

const testAlertId = `test-review-alert:${new Date().toISOString()}`;

alertsDocument.generated_at = new Date().toISOString();
alertsDocument.alerts = [
  {
    alert_id: testAlertId,
    area_id: "test-review-alert",
    area_type: "hotspot",
    district_id: "idukki",
    name: "Telegram review flow test",
    score: 91.2,
    valid_from: new Date().toISOString(),
    valid_to: new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString(),
    level: "Severe - review required",
    confidence: 0.74,
    review_state: "pending_review",
    drivers: [
      "Manual Telegram review flow verification"
    ],
    source_refs: [
      {
        source_id: "manual-test",
        detail: "Synthetic pending severe alert for Telegram review testing"
      }
    ],
    message_en: "Telegram review flow test: Severe - review required. Manual Telegram review flow verification",
    recommended_actions: [
      "Do not forward publicly. This is a test alert.",
      "Use Telegram approve/reject commands to verify review flow."
    ]
  }
];

approvalsDocument.approvals = (approvalsDocument.approvals ?? []).filter((entry) => entry.alert_id !== testAlertId);
rejectionsDocument.rejections = (rejectionsDocument.rejections ?? []).filter((entry) => entry.alert_id !== testAlertId);
dispatchLog.sent_alert_ids = (dispatchLog.sent_alert_ids ?? []).filter((entry) => entry !== testAlertId);
dispatchLog.pending_review_notified_alert_ids = (dispatchLog.pending_review_notified_alert_ids ?? []).filter(
  (entry) => entry !== testAlertId
);

await writeJson(alertsPath, alertsDocument);
await writeJson(approvalsPath, approvalsDocument);
await writeJson(rejectionsPath, rejectionsDocument);
await writeJson(dispatchLogPath, dispatchLog);

console.log(`Prepared Telegram review-flow test alert: ${testAlertId}`);
