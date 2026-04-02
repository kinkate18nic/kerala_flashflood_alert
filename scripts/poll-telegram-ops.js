import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson, writeJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const telegramConfig = await readJson(path.join(repoRoot, "config", "telegram.json"), {
  enabled: false,
  bot_token_env: "TELEGRAM_BOT_TOKEN",
  chat_id_env: "TELEGRAM_CHAT_ID"
});

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

const opsStatePath = path.join(repoRoot, "data", "manual", "ops-state.json");
const telegramStatePath = path.join(repoRoot, "runtime", "cache", "telegram-command-state.json");

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

const allowedChatId = String(chatId);

function commandHelp() {
  return [
    "Kerala Flash-Flood Watch bot commands:",
    "/pause_refresh - pause all scheduled refresh work",
    "/resume_refresh - resume all scheduled refresh work",
    "/status - show current refresh pause status"
  ].join("\n");
}

function buildStatusText(state) {
  if (state.refresh_paused) {
    return [
      "Refresh status: paused",
      state.changed_at ? `Changed: ${state.changed_at}` : null,
      state.changed_by ? `By: ${state.changed_by}` : null,
      state.note ? `Note: ${state.note}` : null
    ]
      .filter(Boolean)
      .join("\n");
  }

  return [
    "Refresh status: active",
    state.changed_at ? `Last change: ${state.changed_at}` : null,
    state.changed_by ? `By: ${state.changed_by}` : null
  ]
    .filter(Boolean)
    .join("\n");
}

async function sendMessage(text) {
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
    throw new Error(`Telegram send failed: ${response.status} ${body}`);
  }
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

  if (!incomingChatId || incomingChatId !== allowedChatId || !text.startsWith("/")) {
    continue;
  }

  const actor =
    [message.from?.first_name, message.from?.last_name].filter(Boolean).join(" ").trim() ||
    message.from?.username ||
    "Telegram user";
  const now = new Date().toISOString();

  if (text === "/pause_refresh") {
    opsState.refresh_paused = true;
    opsState.changed_at = now;
    opsState.changed_by = actor;
    opsState.source = "telegram";
    opsState.note = "Paused from Telegram";
    changed = true;
    await sendMessage("All scheduled refresh workflows are now paused.");
    continue;
  }

  if (text === "/resume_refresh") {
    opsState.refresh_paused = false;
    opsState.changed_at = now;
    opsState.changed_by = actor;
    opsState.source = "telegram";
    opsState.note = "Resumed from Telegram";
    changed = true;
    await sendMessage("All scheduled refresh workflows are now active again.");
    continue;
  }

  if (text === "/status") {
    await sendMessage(buildStatusText(opsState));
    continue;
  }

  await sendMessage(commandHelp());
}

telegramState.last_update_id = latestUpdateId;

await writeJson(telegramStatePath, telegramState);
if (changed) {
  await writeJson(opsStatePath, opsState);
}

console.log(`Processed ${updates.length} Telegram update(s).`);
