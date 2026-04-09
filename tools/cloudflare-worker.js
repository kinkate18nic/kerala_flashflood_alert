export default {
  async fetch(request, env) {
    const targetUrl = new URL(request.url);

    if (targetUrl.pathname === "/telegram-webhook") {
      return handleTelegramWebhook(request, env);
    }

    if (targetUrl.pathname === "/healthz") {
      return new Response("ok", { status: 200 });
    }

    return jsonResponse({ ok: false, error: "not_found" }, 404);
  }
};

async function handleTelegramWebhook(request, env) {
  if (request.method !== "POST") {
    return jsonResponse({ ok: false, error: "method_not_allowed" }, 405);
  }

  const expectedSecret = String(env.TELEGRAM_WEBHOOK_SECRET ?? "").trim();
  const providedSecret = request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "";
  if (expectedSecret && providedSecret !== expectedSecret) {
    return jsonResponse({ ok: false, error: "invalid_webhook_secret" }, 401);
  }

  const botToken = String(env.TELEGRAM_BOT_TOKEN ?? "").trim();
  const commandChatId = String(env.TELEGRAM_COMMAND_CHAT_ID ?? env.TELEGRAM_CHAT_ID ?? "").trim();
  const githubOwner = String(env.GITHUB_OWNER ?? "").trim();
  const githubRepo = String(env.GITHUB_REPO ?? "").trim();
  const githubDispatchToken = String(env.GITHUB_DISPATCH_TOKEN ?? "").trim();
  const githubRef = String(env.GITHUB_REF ?? "main").trim() || "main";
  const rawBase =
    String(env.GITHUB_RAW_BASE ?? "").trim() ||
    `https://raw.githubusercontent.com/${githubOwner}/${githubRepo}/${githubRef}`;

  const update = await request.json().catch(() => null);
  const message = update?.message;
  const incomingChatId = message?.chat?.id != null ? String(message.chat.id) : "";
  const commandText = String(message?.text ?? "").trim();
  const normalizedCommand = commandText.split(/\s+/)[0]?.split("@")[0] ?? "";

  if (!botToken || !commandChatId) {
    return jsonResponse({ ok: false, error: "telegram_not_configured" }, 500);
  }

  if (!incomingChatId || incomingChatId !== commandChatId || !normalizedCommand.startsWith("/")) {
    return jsonResponse({ ok: true, ignored: true }, 200);
  }

  const actor =
    [message.from?.first_name, message.from?.last_name].filter(Boolean).join(" ").trim() ||
    message.from?.username ||
    "Telegram user";
  const commandAt = new Date().toISOString();
  const commandParts = commandText.split(/\s+/).filter(Boolean);
  const commandArg = commandParts[1] ?? null;

  if (normalizedCommand === "/status") {
    const [opsState, alertsDocument, rejectionsDocument] = await Promise.all([
      fetchJson(`${rawBase}/data/manual/ops-state.json`, {
        refresh_paused: false,
        changed_at: null,
        changed_by: null,
        source: "default",
        note: null
      }),
      fetchJson(`${rawBase}/docs/data/latest/alerts.json`, { alerts: [] }),
      fetchJson(`${rawBase}/data/manual/review-rejections.json`, { rejections: [] })
    ]);

    await sendTelegramMessage(botToken, incomingChatId, buildStatusText(opsState, alertsDocument, rejectionsDocument));
    return jsonResponse({ ok: true, handled: "status" }, 200);
  }

  if (normalizedCommand === "/pending_alerts") {
    const [alertsDocument, rejectionsDocument] = await Promise.all([
      fetchJson(`${rawBase}/docs/data/latest/alerts.json`, { alerts: [] }),
      fetchJson(`${rawBase}/data/manual/review-rejections.json`, { rejections: [] })
    ]);

    await sendTelegramMessage(
      botToken,
      incomingChatId,
      buildPendingAlertsText(alertsDocument, rejectionsDocument)
    );
    return jsonResponse({ ok: true, handled: "pending_alerts" }, 200);
  }

  if (normalizedCommand === "/help") {
    await sendTelegramMessage(botToken, incomingChatId, commandHelp());
    return jsonResponse({ ok: true, handled: "help" }, 200);
  }

  if (normalizedCommand === "/pause_refresh" || normalizedCommand === "/resume_refresh") {
    await sendGithubDispatch(githubOwner, githubRepo, githubDispatchToken, "telegram_refresh_command", {
      command_text: commandText,
      actor,
      command_at: commandAt,
      reply_chat_id: incomingChatId
    });
    await sendTelegramMessage(
      botToken,
      incomingChatId,
      `${normalizedCommand} received. GitHub is applying it now.`
    );
    return jsonResponse({ ok: true, handled: "refresh_command" }, 200);
  }

  if (normalizedCommand === "/approve" || normalizedCommand === "/reject") {
    if (!commandArg) {
      await sendTelegramMessage(
        botToken,
        incomingChatId,
        `Usage: ${normalizedCommand} <alert_id>`
      );
      return jsonResponse({ ok: true, handled: "review_usage" }, 200);
    }

    const [alertsDocument, rejectionsDocument] = await Promise.all([
      fetchJson(`${rawBase}/docs/data/latest/alerts.json`, { alerts: [] }),
      fetchJson(`${rawBase}/data/manual/review-rejections.json`, { rejections: [] })
    ]);
    const pendingAlert = getPendingAlerts(alertsDocument, rejectionsDocument).find(
      (alert) => alert.alert_id === commandArg
    );

    if (!pendingAlert) {
      await sendTelegramMessage(
        botToken,
        incomingChatId,
        `No pending severe alert found for ${commandArg}.`
      );
      return jsonResponse({ ok: true, handled: "review_missing" }, 200);
    }

    await sendGithubDispatch(githubOwner, githubRepo, githubDispatchToken, "telegram_review_command", {
      command_text: commandText,
      actor,
      command_at: commandAt,
      reply_chat_id: incomingChatId
    });
    await sendTelegramMessage(
      botToken,
      incomingChatId,
      `${normalizedCommand} received. GitHub is applying it now.`
    );
    return jsonResponse({ ok: true, handled: "review_command" }, 200);
  }

  await sendTelegramMessage(botToken, incomingChatId, commandHelp());
  return jsonResponse({ ok: true, handled: "help_fallback" }, 200);
}

async function sendGithubDispatch(owner, repo, token, eventType, clientPayload) {
  if (!owner || !repo || !token) {
    throw new Error("GitHub dispatch is not fully configured in the Worker environment.");
  }

  const response = await fetch(`https://api.github.com/repos/${owner}/${repo}/dispatches`, {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "kerala-flash-flood-watch-telegram-webhook"
    },
    body: JSON.stringify({
      event_type: eventType,
      client_payload: clientPayload
    })
  });

  if (!response.ok) {
    throw new Error(`GitHub dispatch failed: ${response.status} ${await response.text()}`);
  }
}

async function fetchJson(url, fallbackValue) {
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "kerala-flash-flood-watch-telegram-webhook"
      }
    });
    if (!response.ok) {
      return fallbackValue;
    }
    return await response.json();
  } catch {
    return fallbackValue;
  }
}

function getPendingAlerts(alertsDocument, rejectionsDocument) {
  const rejectedIds = new Set((rejectionsDocument.rejections ?? []).map((entry) => entry.alert_id));
  return (alertsDocument.alerts ?? []).filter(
    (alert) => alert.review_state === "pending_review" && !rejectedIds.has(alert.alert_id)
  );
}

function buildStatusText(opsState, alertsDocument, rejectionsDocument) {
  const pendingCount = getPendingAlerts(alertsDocument, rejectionsDocument).length;
  if (opsState.refresh_paused) {
    return [
      "Refresh status: paused",
      `Pending severe reviews: ${pendingCount}`,
      opsState.changed_at ? `Changed: ${opsState.changed_at}` : null,
      opsState.changed_by ? `By: ${opsState.changed_by}` : null,
      opsState.note ? `Note: ${opsState.note}` : null
    ]
      .filter(Boolean)
      .join("\n");
  }

  return [
    "Refresh status: active",
    `Pending severe reviews: ${pendingCount}`,
    opsState.changed_at ? `Last change: ${opsState.changed_at}` : null,
    opsState.changed_by ? `By: ${opsState.changed_by}` : null
  ]
    .filter(Boolean)
    .join("\n");
}

function buildPendingAlertsText(alertsDocument, rejectionsDocument) {
  const pending = getPendingAlerts(alertsDocument, rejectionsDocument);
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

async function sendTelegramMessage(botToken, chatId, text) {
  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text
    })
  });

  if (!response.ok) {
    throw new Error(`Telegram send failed: ${response.status} ${await response.text()}`);
  }
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8"
    }
  });
}
