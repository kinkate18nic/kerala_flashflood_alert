import { fetchText } from "./http.js";

function readTag(fragment, tagName) {
  const match = fragment.match(
    new RegExp(`<(?:(?:\\w+):)?${tagName}[^>]*>([\\s\\S]*?)<\\/(?:(?:\\w+):)?${tagName}>`, "i")
  );
  return match?.[1]?.trim() ?? null;
}

function readTagAttribute(fragment, tagName, attributeName) {
  const match = fragment.match(
    new RegExp(`<(?:(?:\\w+):)?${tagName}[^>]*\\b${attributeName}="([^"]+)"[^>]*\\/?>`, "i")
  );
  return match?.[1]?.trim() ?? null;
}

function readFirstTag(fragment, tagNames) {
  for (const tagName of tagNames) {
    const value = readTag(fragment, tagName);
    if (value) {
      return value;
    }
  }
  return null;
}

function readLink(fragment) {
  return readTag(fragment, "link") ?? readTagAttribute(fragment, "link", "href") ?? readTag(fragment, "id");
}

function extractRssItems(raw) {
  return [...raw.matchAll(/<(item|entry)\b[^>]*>([\s\S]*?)<\/\1>/gi)].map((match) => {
    const itemText = match[2];
    const link = readLink(itemText);
    const identifier =
      readFirstTag(itemText, ["identifier", "guid"]) ??
      link?.match(/[?&]identifier=([^&]+)/i)?.[1] ??
      null;

    return {
      title: readFirstTag(itemText, ["title", "headline"]) ?? "",
      link,
      identifier
    };
  });
}

function buildDetailUrl(link, identifier) {
  if (link && /^https?:/i.test(link)) {
    return link;
  }
  if (identifier) {
    return `https://sachet.ndma.gov.in/cap_public_website/FetchXMLFile?identifier=${encodeURIComponent(identifier)}`;
  }
  return null;
}

export async function fetchImdCapPayload(source) {
  const candidateUrls = [source.url, ...(source.fallback_urls ?? [])].filter(Boolean);
  let rssResponse = null;
  let resolvedUrl = source.url;

  for (const candidateUrl of candidateUrls) {
    const response = await fetchText(candidateUrl, { timeoutMs: 20000 });
    rssResponse = response;
    resolvedUrl = candidateUrl;
    if (response.ok && response.text?.trim()) {
      break;
    }
  }

  if (!rssResponse?.ok || !rssResponse.text?.trim()) {
    return {
      ok: false,
      status: rssResponse?.status ?? 502,
      text: "",
      note: "IMD CAP RSS fetch failed.",
      resolvedUrl
    };
  }

  const items = extractRssItems(rssResponse.text).slice(0, 25);
  const details = [];

  for (const item of items) {
    const detailUrl = buildDetailUrl(item.link, item.identifier);
    if (!detailUrl) {
      continue;
    }

    try {
      const detailResponse = await fetchText(detailUrl, { timeoutMs: 20000 });
      if (detailResponse.ok && detailResponse.text?.trim()) {
        details.push({
          identifier: item.identifier ?? null,
          link: item.link ?? null,
          detail_url: detailUrl,
          xml: detailResponse.text
        });
      }
    } catch {
      // Ignore single-detail failures; RSS still provides a usable fallback.
    }
  }

  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      feed_url: resolvedUrl,
      rss: rssResponse.text,
      details
    }),
    note: details.length
      ? `IMD CAP RSS with ${details.length} detail XML messages`
      : "IMD CAP RSS without detail XML enrichment",
    resolvedUrl
  };
}
