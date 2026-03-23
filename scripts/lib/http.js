import { execFileSync } from "node:child_process";

const DEFAULT_HEADERS = {
  "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/kinkate18nic/kerala_flashflood_alert)"
};

export async function fetchText(url, options = {}) {
  const timeoutMs = options.timeoutMs ?? 15000;
  
  if (url.includes("workers.dev")) {
    const method = options.method || "GET";
    const args = [
      "-s", "-k", "-L", "-X", method,
      url,
      "-H", "Accept: text/html,application/xhtml+xml,application/json,application/xml;q=0.9,*/*;q=0.8",
      "-H", "User-Agent: curl/8.4.0",
      "--max-time", String(Math.floor(timeoutMs / 1000))
    ];
    
    if (options.headers) {
      for (const [key, value] of Object.entries(options.headers)) {
        if (key.toLowerCase() !== "user-agent" && key.toLowerCase() !== "accept") {
           args.push("-H", `${key}: ${value}`);
        }
      }
    }
    
    if (method === "POST") {
       args.push("-d", options.body ? String(options.body) : "");
    }

    try {
      // Execute curl directly without relying on a shell (fixes nested quote issues entirely)
      const stdout = execFileSync("curl", args, { timeout: timeoutMs }).toString();
      
      if (stdout.includes("<html") && stdout.includes("Cloudflare") && stdout.includes("challenge")) {
        throw new Error("Cloudflare Bot Fight Mode intercepted request");
      }
      return { ok: true, status: 200, text: stdout, headers: new Headers() };
    } catch (e) {
      throw new Error(`Proxy / curl fetch failed: ${e.message}`);
    }
  }

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
