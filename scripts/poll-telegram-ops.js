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

const botToken = process.env[telegramConfig.bot_token_env];
const chatId = process.env[telegramConfig.chat_id_env];
const commandChatId =
  process.env[telegramConfig.command_chat_id_env ?? "TELEGRAM_COMMAND_CHAT_ID"] || chatId;

if (!botToken || !chatId) {
  console.log("Telegram credentials not configured.");
  process.exit(0);
}

const opsStatePath = path.join(repoRoot, "data", "manual", "ops-state.json");
const telegramStatePath = path.join(repoRoot, "runtime", "cache", "telegram-command-state.json");
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

const telegramState = await readJson(telegramStatePath, {
  last_update_id: 0
});
const approvalsDocument = await readJson(approvalsPath, { approvals: [] });
const rejectionsDocument = await readJson(rejectionsPath, { rejections: [] });
const alertsDocument = await readJson(alertsPath, { alerts: [] });

const allowedChatId = String(commandChatId);
console.log(
  `Telegram ops env: alert chat=${maskChatId(chatId)} command chat env=${maskChatId(process.env[telegramConfig.command_chat_id_env ?? "TELEGRAM_COMMAND_CHAT_ID"] || "unset")} effective command chat=${maskChatId(allowedChatId)}`
);

function maskChatId(value) {
  const text = String(value ?? "");
  if (text.length <= 4) {
    return text || "none";
  }
  return `${"*".repeat(text.length - 4)}${text.slice(-4)}`;
}

console.log(`Telegram ops listener active. Allowed command chat: ${maskChatId(allowedChatId)}`);

function commandHelp() {
  return [
    "Kerala Flash-Flood Watch bot commands:",
    "/pause_refresh - pause all scheduled refresh work",
    "/resume_refresh - resume all scheduled refresh work",
    "/status - show current refresh pause status",
    "/pending_alerts - list severe alerts awaiting review",
    "/approve <alert_id> - approve a pending severe alert for group delivery",
    "/reject <alert_id> - mark a pending severe alert as rejected"
  ].join("\n");
}

function buildStatusText(state) {
  const pendingCount = getPendingAlerts().length;
  if (state.refresh_paused) {
    return [
      "Refresh status: paused",
      `Pending severe reviews: ${pendingCount}`,
      state.changed_at ? `Changed: ${state.changed_at}` : null,
      state.changed_by ? `By: ${state.changed_by}` : null,
      state.note ? `Note: ${state.note}` : null
    ]
      .filter(Boolean)
      .join("\n");
  }

  return [
    "Refresh status: active",
    `Pending severe reviews: ${pendingCount}`,
    state.changed_at ? `Last change: ${state.changed_at}` : null,
    state.changed_by ? `By: ${state.changed_by}` : null
  ]
    .filter(Boolean)
    .join("\n");
}

function getPendingAlerts() {
  const rejectedIds = new Set((rejectionsDocument.rejections ?? []).map((entry) => entry.alert_id));
  return alertsDocument.alerts.filter(
    (alert) => alert.review_state === "pending_review" && !rejectedIds.has(alert.alert_id)
  );
}

function buildPendingAlertsText() {
  const pending = getPendingAlerts();
  if (!pending.length) {
    return "No severe alerts are currently waiting for review.";
  }

  return [
    `Pending severe alerts: ${pending.length}`,
    ...pending.slice(0, 10).map(
      (alert, index) =>
        `${index + 1}. ${alert.name} | score ${alert.score.toFixed(1)} | ${alert.alert_id}`
    ),
    pending.length > 10 ? `...and ${pending.length - 10} more` : null,
    "",
    "Approve one: /approve <alert_id>",
    "Reject one: /reject <alert_id>"
  ]
    .filter(Boolean)
    .join("\n");
}

async function sendMessage(text, targetChatId = commandChatId) {
  console.log(`Sending Telegram reply to chat ${maskChatId(targetChatId)}: ${text.split("\n")[0]}`);
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
    throw new Error(`Telegram send failed: ${response.status} ${body}`);
  }

  console.log(`Telegram reply sent to chat ${maskChatId(targetChatId)}.`);
}

const params = new URLSearchParams({
  timeout: "0",
  allowed_updates: JSON.stringify(["message"])
});

if (telegramState.last_update_id) {
  params.set("offset", String(telegramState.last_update_id + 1));
}

const updatesResponse = await fetch(`https://api.telegram.org/bot${botToken}/getUpdates?${params.toString()}`);
if (!updatesResponse.ok) {
  const body = await updatesResponse.text();
  if (updatesResponse.status === 409) {
    console.log("Telegram webhook is active; polling fallback skipped.");
    process.exit(0);
  }
  throw new Error(`Telegram getUpdates failed: ${updatesResponse.status} ${body}`);
}

const updatesPayload = await updatesResponse.json();
const updates = updatesPayload.result ?? [];

let changed = false;
let latestUpdateId = telegramState.last_update_id ?? 0;

for (const update of updates) {
  latestUpdateId = Math.max(latestUpdateId, update.update_id ?? 0);

  const message = update.message;
  const incomingChatId = message?.chat?.id != null ? String(message.chat.id) : null;
  const text = String(message?.text ?? "").trim();
  const normalizedCommand = text.split(/\s+/)[0]?.split("@")[0] ?? "";

  console.log(
    `Telegram update ${update.update_id}: chat=${maskChatId(incomingChatId)} text=${JSON.stringify(text)} normalized=${JSON.stringify(normalizedCommand)}`
  );

  if (!incomingChatId) {
    console.log(`Skipping update ${update.update_id}: no incoming chat id.`);
    continue;
  }

  if (incomingChatId !== allowedChatId) {
    console.log(
      `Skipping update ${update.update_id}: chat ${maskChatId(incomingChatId)} does not match allowed chat ${maskChatId(allowedChatId)}.`
    );
    continue;
  }

  if (!normalizedCommand.startsWith("/")) {
    console.log(`Skipping update ${update.update_id}: not a slash command.`);
    continue;
  }

  const actor =
    [message.from?.first_name, message.from?.last_name].filter(Boolean).join(" ").trim() ||
    message.from?.username ||
    "Telegram user";
  const now = new Date().toISOString();
  const commandParts = text.split(/\s+/).filter(Boolean);
  const commandArg = commandParts[1] ?? null;

  if (normalizedCommand === "/pause_refresh") {
    opsState.refresh_paused = true;
    opsState.changed_at = now;
    opsState.changed_by = actor;
    opsState.source = "telegram";
    opsState.note = "Paused from Telegram";
    changed = true;
    await sendMessage("All scheduled refresh workflows are now paused.", incomingChatId);
    continue;
  }

  if (normalizedCommand === "/resume_refresh") {
    opsState.refresh_paused = false;
    opsState.changed_at = now;
    opsState.changed_by = actor;
    opsState.source = "telegram";
    opsState.note = "Resumed from Telegram";
    changed = true;
    await sendMessage("All scheduled refresh workflows are now active again.", incomingChatId);
    continue;
  }

  if (normalizedCommand === "/status") {
    await sendMessage(buildStatusText(opsState), incomingChatId);
    continue;
  }

  if (normalizedCommand === "/pending_alerts") {
    await sendMessage(buildPendingAlertsText(), incomingChatId);
    continue;
  }

  if (normalizedCommand === "/approve") {
    if (!commandArg) {
      await sendMessage("Usage: /approve <alert_id>", incomingChatId);
      continue;
    }

    const pendingAlert = getPendingAlerts().find((alert) => alert.alert_id === commandArg);
    if (!pendingAlert) {
      await sendMessage(`No pending severe alert found for ${commandArg}.`, incomingChatId);
      continue;
    }

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
    await sendMessage(
      `Approved ${pendingAlert.name}.\nThe next publish run will mark it as Reviewed severe alert and send it to the alert group.`,
      incomingChatId
    );
    continue;
  }

  if (normalizedCommand === "/reject") {
    if (!commandArg) {
      await sendMessage("Usage: /reject <alert_id>", incomingChatId);
      continue;
    }

    const pendingAlert = getPendingAlerts().find((alert) => alert.alert_id === commandArg);
    if (!pendingAlert) {
      await sendMessage(`No pending severe alert found for ${commandArg}.`, incomingChatId);
      continue;
    }

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

    await sendMessage(
      `Rejected ${pendingAlert.name}.\nIt will not be sent to the public alert group unless you later approve it.`,
      incomingChatId
    );
    continue;
  }

  await sendMessage(commandHelp(), incomingChatId);
}

telegramState.last_update_id = latestUpdateId;

await writeJson(telegramStatePath, telegramState);
if (changed) {
  await writeJson(opsStatePath, opsState);
  await writeJson(approvalsPath, approvalsDocument);
  await writeJson(rejectionsPath, rejectionsDocument);
}

console.log(`Processed ${updates.length} Telegram update(s).`);
