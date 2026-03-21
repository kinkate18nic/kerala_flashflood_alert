export default {
  async fetch(request, env, ctx) {
    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    const targetUrl = new URL(request.url);
    const wrisUrl = new URL("https://indiawris.gov.in" + targetUrl.pathname + targetUrl.search);

    const headers = new Headers(request.headers);
    headers.set("Origin", "https://indiawris.gov.in");
    headers.set("Referer", "https://indiawris.gov.in/swagger-ui/index.html");
    headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");

    try {
      const response = await fetch(wrisUrl, {
        method: "POST",
        headers: headers,
        body: request.body
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
