const DEFAULT_HEADERS = {
  "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/)"
};

export async function fetchText(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? 15000);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: { ...DEFAULT_HEADERS, ...(options.headers ?? {}) },
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
