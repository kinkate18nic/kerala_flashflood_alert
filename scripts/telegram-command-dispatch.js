import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson, writeJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const telegramConfig = await readJson(path.join(repoRoot, "config", "telegram.json"), {
  enabled: false,
  bot_token_env: "TELEGRAM_BOT_TOKEN",
  chat_id_env: "TELEGRAM_CHAT_ID",
  command_chat_id_env: "TELEGRAM_COMMAND_CHAT_ID"
});

if (!telegramConfig.enabled) {
  console.log("Telegram dispatch disabled in config/telegram.json");
  process.exit(0);
}

const commandText = String(process.env.TELEGRAM_COMMAND_TEXT ?? "").trim();
const actor = String(process.env.TELEGRAM_COMMAND_ACTOR ?? "Telegram user").trim() || "Telegram user";
const now = process.env.TELEGRAM_COMMAND_AT?.trim() || new Date().toISOString();
const replyChatId = String(process.env.TELEGRAM_REPLY_CHAT_ID ?? "").trim();
const botToken = process.env[telegramConfig.bot_token_env];

if (!commandText) {
  console.log("No Telegram command text provided.");
  process.exit(0);
}

const opsStatePath = path.join(repoRoot, "data", "manual", "ops-state.json");
const approvalsPath = path.join(repoRoot, "data", "manual", "review-approvals.json");
const rejectionsPath = path.join(repoRoot, "data", "manual", "review-rejections.json");
const alertsPath = path.join(repoRoot, "docs", "data", "latest", "alerts.json");

const opsState = await readJson(opsStatePath, {
  refresh_paused: false,
  changed_at: null,
  changed_by: null,
  source: "default",
  note: null
});
const approvalsDocument = await readJson(approvalsPath, { approvals: [] });
const rejectionsDocument = await readJson(rejectionsPath, { rejections: [] });
const alertsDocument = await readJson(alertsPath, { alerts: [] });

function getPendingAlerts() {
  const rejectedIds = new Set((rejectionsDocument.rejections ?? []).map((entry) => entry.alert_id));
  return alertsDocument.alerts.filter(
    (alert) => alert.review_state === "pending_review" && !rejectedIds.has(alert.alert_id)
  );
}

const normalizedCommand = commandText.split(/\s+/)[0]?.split("@")[0] ?? "";
const commandParts = commandText.split(/\s+/).filter(Boolean);
const commandArg = commandParts[1] ?? null;

let changed = false;
let reply = "No action taken.";

if (normalizedCommand === "/pause_refresh") {
  opsState.refresh_paused = true;
  opsState.changed_at = now;
  opsState.changed_by = actor;
  opsState.source = "telegram-webhook";
  opsState.note = "Paused from Telegram webhook";
  changed = true;
  reply = "All scheduled refresh workflows are now paused.";
} else if (normalizedCommand === "/resume_refresh") {
  opsState.refresh_paused = false;
  opsState.changed_at = now;
  opsState.changed_by = actor;
  opsState.source = "telegram-webhook";
  opsState.note = "Resumed from Telegram webhook";
  changed = true;
  reply = "All scheduled refresh workflows are now active again.";
} else if (normalizedCommand === "/approve") {
  if (!commandArg) {
    reply = "Usage: /approve <alert_id>";
  } else {
    const pendingAlert = getPendingAlerts().find((alert) => alert.alert_id === commandArg);
    if (!pendingAlert) {
      reply = `No pending severe alert found for ${commandArg}.`;
    } else {
      if (!approvalsDocument.approvals.some((entry) => entry.alert_id === commandArg)) {
        approvalsDocument.approvals.push({
          alert_id: commandArg,
          approved_at: now,
          approved_by: actor
        });
        changed = true;
      }
      const rejectionCountBefore = (rejectionsDocument.rejections ?? []).length;
      rejectionsDocument.rejections = (rejectionsDocument.rejections ?? []).filter(
        (entry) => entry.alert_id !== commandArg
      );
      if (rejectionsDocument.rejections.length !== rejectionCountBefore) {
        changed = true;
      }
      reply = `Approved ${pendingAlert.name}. The next publish run will send it to the alert group.`;
    }
  }
} else if (normalizedCommand === "/reject") {
  if (!commandArg) {
    reply = "Usage: /reject <alert_id>";
  } else {
    const pendingAlert = getPendingAlerts().find((alert) => alert.alert_id === commandArg);
    if (!pendingAlert) {
      reply = `No pending severe alert found for ${commandArg}.`;
    } else {
      const approvalCountBefore = (approvalsDocument.approvals ?? []).length;
      approvalsDocument.approvals = (approvalsDocument.approvals ?? []).filter(
        (entry) => entry.alert_id !== commandArg
      );
      if (approvalsDocument.approvals.length !== approvalCountBefore) {
        changed = true;
      }
      if (!rejectionsDocument.rejections.some((entry) => entry.alert_id === commandArg)) {
        rejectionsDocument.rejections.push({
          alert_id: commandArg,
          rejected_at: now,
          rejected_by: actor
        });
        changed = true;
      }
      reply = `Rejected ${pendingAlert.name}. It will stay out of the public alert group.`;
    }
  }
} else {
  console.log(`Telegram webhook command ${JSON.stringify(commandText)} does not require repo mutation.`);
  reply = "No action taken.";
}

if (changed) {
  await writeJson(opsStatePath, opsState);
  await writeJson(approvalsPath, approvalsDocument);
  await writeJson(rejectionsPath, rejectionsDocument);
}

if (botToken && replyChatId && reply) {
  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: replyChatId,
      text: reply
    })
  });

  if (!response.ok) {
    throw new Error(`Telegram send failed: ${response.status} ${await response.text()}`);
  }
}

console.log(reply);
