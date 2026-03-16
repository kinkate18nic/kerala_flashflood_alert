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
if (!botToken || !chatId) {
  console.log("Telegram credentials not configured.");
  process.exit(0);
}

const alertsDocument = await readJson(path.join(repoRoot, "docs", "data", "latest", "alerts.json"), {
  alerts: []
});
const dispatchLogPath = path.join(repoRoot, "runtime", "metrics", "telegram-dispatch-log.json");
const dispatchLog = await readJson(dispatchLogPath, { sent_alert_ids: [] });

const candidates = alertsDocument.alerts.filter(
  (alert) =>
    telegramConfig.send_levels.includes(alert.level) &&
    !dispatchLog.sent_alert_ids.includes(alert.alert_id)
);

if (!candidates.length) {
  console.log("No new reviewed severe alerts to send.");
  process.exit(0);
}

for (const alert of candidates) {
  const text = [
    `Kerala Flash-Flood Watch`,
    `${alert.name}: ${alert.level}`,
    alert.message_en,
    `Confidence: ${(alert.confidence * 100).toFixed(0)}%`,
    `Actions: ${alert.recommended_actions.join(" ")}`
  ].join("\n");

  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Telegram send failed for ${alert.alert_id}: ${response.status} ${body}`);
  }

  dispatchLog.sent_alert_ids.push(alert.alert_id);
}

await writeJson(dispatchLogPath, dispatchLog);
console.log(`Sent ${candidates.length} Telegram alert(s).`);
