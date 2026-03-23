export default {
  async fetch(request, env, ctx) {
    const targetUrl = new URL(request.url);
    const explicitTarget = targetUrl.searchParams.get("url");
    const upstreamUrl = explicitTarget
      ? new URL(explicitTarget)
      : new URL("https://indiawris.gov.in" + targetUrl.pathname + targetUrl.search);

    const headers = new Headers(request.headers);
    if (upstreamUrl.hostname.includes("indiawris.gov.in")) {
      headers.set("Origin", "https://indiawris.gov.in");
      headers.set("Referer", "https://indiawris.gov.in/swagger-ui/index.html");
    } else if (upstreamUrl.hostname.includes("ffs.india-water.gov.in")) {
      headers.set("Origin", "https://ffs.india-water.gov.in");
      headers.set("Referer", "https://ffs.india-water.gov.in/#/main/site");
    }
    headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");

    try {
      const response = await fetch(upstreamUrl, {
        method: request.method,
        headers: headers,
        body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body
      });

      const responseHeaders = new Headers(response.headers);
      responseHeaders.set("Access-Control-Allow-Origin", "*");

      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: responseHeaders
      });
    } catch (e) {
      return new Response("Proxy Error: " + e.message, { status: 502 });
    }
  }
};
