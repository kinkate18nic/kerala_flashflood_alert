const DEFAULT_HEADERS = {
  "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/kinkate18nic/kerala_flashflood_alert)"
};

export async function fetchText(url, options = {}) {
  const timeoutMs = options.timeoutMs ?? 60000;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: options.method ?? "GET",
      headers: { ...DEFAULT_HEADERS, ...(options.headers ?? {}) },
      body: options.body,
      signal: controller.signal
    });

    const text = await response.text();
    return {
      ok: response.ok,
      status: response.status,
      text,
      headers: response.headers
    };
  } finally {
    clearTimeout(timeout);
  }
}

export async function fetchBuffer(url, options = {}) {
  const timeoutMs = options.timeoutMs ?? 60000;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: options.method ?? "GET",
      headers: { ...DEFAULT_HEADERS, ...(options.headers ?? {}) },
      body: options.body,
      signal: controller.signal
    });

    return {
      ok: response.ok,
      status: response.status,
      buffer: Buffer.from(await response.arrayBuffer()),
      headers: response.headers
    };
  } finally {
    clearTimeout(timeout);
  }
}
