import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson, writeJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const telegramConfig = await readJson(path.join(repoRoot, "config", "telegram.json"));
if (!telegramConfig.enabled) {
  console.log("Telegram dispatch disabled in config/telegram.json");
  process.exit(0);
}

const botToken = process.env[telegramConfig.bot_token_env];
const chatId = process.env[telegramConfig.chat_id_env];
const commandChatId =
  process.env[telegramConfig.command_chat_id_env ?? "TELEGRAM_COMMAND_CHAT_ID"] || chatId;
if (!botToken || !chatId) {
  console.log("Telegram credentials not configured.");
  process.exit(0);
}

const alertsDocument = await readJson(path.join(repoRoot, "docs", "data", "latest", "alerts.json"), {
  alerts: []
});
const dispatchLogPath = path.join(repoRoot, "runtime", "metrics", "telegram-dispatch-log.json");
const dispatchLog = await readJson(dispatchLogPath, {
  sent_alert_ids: [],
  pending_review_notified_alert_ids: []
});
const rejectionsPath = path.join(repoRoot, "data", "manual", "review-rejections.json");
const rejectionsDocument = await readJson(rejectionsPath, { rejections: [] });
const rejectedIds = new Set((rejectionsDocument.rejections ?? []).map((entry) => entry.alert_id));

const reviewedCandidates = alertsDocument.alerts.filter(
  (alert) =>
    telegramConfig.send_levels.includes(alert.level) &&
    !dispatchLog.sent_alert_ids.includes(alert.alert_id)
);

const pendingCandidates = alertsDocument.alerts.filter(
  (alert) =>
    alert.level === "Severe - review required" &&
    alert.review_state === "pending_review" &&
    !dispatchLog.pending_review_notified_alert_ids.includes(alert.alert_id) &&
    !rejectedIds.has(alert.alert_id)
);

async function sendMessage(targetChatId, text) {
  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: targetChatId,
      text
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Telegram send failed to ${targetChatId}: ${response.status} ${body}`);
  }
}

for (const alert of pendingCandidates) {
  const text = [
    "Kerala Flash-Flood Watch",
    "Pending severe alert review",
    `${alert.name}: ${alert.level}`,
    `Alert ID: ${alert.alert_id}`,
    `Score: ${alert.score.toFixed(1)}`,
    `Confidence: ${(alert.confidence * 100).toFixed(0)}%`,
    `Primary driver: ${alert.drivers[0] ?? "Multiple rainfall and hydrology signals are active."}`,
    "",
    `Approve: /approve ${alert.alert_id}`,
    `Reject: /reject ${alert.alert_id}`,
    "Use /pending_alerts to see all alerts awaiting review."
  ].join("\n");

  await sendMessage(commandChatId, text);
  dispatchLog.pending_review_notified_alert_ids.push(alert.alert_id);
}

for (const alert of reviewedCandidates) {
  const text = [
    `Kerala Flash-Flood Watch`,
    `${alert.name}: ${alert.level}`,
    alert.message_en,
    `Confidence: ${(alert.confidence * 100).toFixed(0)}%`,
    `Actions: ${alert.recommended_actions.join(" ")}`
  ].join("\n");

  await sendMessage(chatId, text);
  dispatchLog.sent_alert_ids.push(alert.alert_id);
}

if (!pendingCandidates.length && !reviewedCandidates.length) {
  console.log("No Telegram review notices or reviewed severe alerts to send.");
  process.exit(0);
}

await writeJson(dispatchLogPath, dispatchLog);
console.log(
  `Sent ${pendingCandidates.length} pending review notice(s) and ${reviewedCandidates.length} reviewed severe alert(s).`
);
