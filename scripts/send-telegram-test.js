import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson } from "./lib/fs.js";

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
const extraMessage = process.env.EXTRA_MESSAGE?.trim();

if (!botToken || !chatId) {
  console.error("Telegram credentials not configured.");
  process.exit(1);
}

const lines = [
  "Kerala Flash-Flood Watch",
  "Telegram test message",
  "If you can read this, group delivery is working.",
  `Sent at: ${new Date().toISOString()}`
];

if (extraMessage) {
  lines.push(`Note: ${extraMessage}`);
}

const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    chat_id: chatId,
    text: lines.join("\n")
  })
});

if (!response.ok) {
  const body = await response.text();
  throw new Error(`Telegram send failed: ${response.status} ${body}`);
}

console.log("Telegram test message sent successfully.");
