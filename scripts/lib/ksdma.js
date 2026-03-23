import { PDFParse } from "pdf-parse";
import { districts } from "../../src/shared/areas.js";
import { fetchBuffer, fetchText } from "./http.js";
import { parseDate } from "./time.js";

const DISTRICT_ALIASES = new Map([
  ["kasaragod", "kasaragod"],
  ["kannur", "kannur"],
  ["wayanad", "wayanad"],
  ["kozhikode", "kozhikode"],
  ["kozhikkode", "kozhikode"],
  ["malappuram", "malappuram"],
  ["palakkad", "palakkad"],
  ["thrissur", "thrissur"],
  ["trichur", "thrissur"],
  ["ernakulam", "ernakulam"],
  ["idukki", "idukki"],
  ["kottayam", "kottayam"],
  ["alappuzha", "alappuzha"],
  ["alleppey", "alappuzha"],
  ["pathanamthitta", "pathanamthitta"],
  ["kollam", "kollam"],
  ["thiruvananthapuram", "thiruvananthapuram"],
  ["trivandrum", "thiruvananthapuram"]
]);

function normalizeText(value) {
  return String(value ?? "")
    .replace(/\r/g, "")
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+/g, " ")
    .trim();
}

function normalizeKey(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
}

function wrapTargetUrl(baseUrl, targetUrl) {
  const outerUrl = new URL(baseUrl);
  const innerRaw = outerUrl.searchParams.get("url");
  if (!innerRaw) {
    return targetUrl;
  }

  outerUrl.searchParams.set("url", targetUrl);
  return outerUrl.toString();
}

function extractPdfLink(pageHtml, department) {
  const matches = [...pageHtml.matchAll(/href="([^"]+\.pdf[^"]*)"[^>]*>([^<]*)<\/a>/gi)];
  const expectedToken = department === "kseb" ? "KSEB" : "IRR";
  const hit = matches.find((match) =>
    match[1].toUpperCase().includes(expectedToken) ||
    match[2].toUpperCase().includes(department === "kseb" ? "KSEB" : "IRRIGATION")
  );

  if (!hit) {
    throw new Error(`KSDMA ${department.toUpperCase()} PDF link not found on dam-water-level page`);
  }

  return {
    href: hit[1],
    label: normalizeText(hit[2] || "")
  };
}

function extractIssuedAt(pageHtml, pdfHref) {
  const escapedHref = pdfHref.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = pageHtml.match(new RegExp(`${escapedHref}"[^>]*>[^<]*<\\/a>\\s*[–-]\\s*(\\d{2}\\/\\d{2}\\/\\d{4})`, "i"));
  if (!match) {
    return null;
  }

  const [day, month, year] = match[1].split("/").map(Number);
  const issuedAt = new Date(Date.UTC(year, month - 1, day, 5, 30, 0));
  return issuedAt.toISOString();
}

function splitRowChunks(text, headerMarker) {
  const prelude = text.split(headerMarker)[0] ?? text;
  return prelude
    .split(/(?=^\s*\d+\s)/m)
    .map((chunk) => chunk.trim())
    .filter((chunk) => /^\d+\s/.test(chunk));
}

function extractDistrictId(chunk) {
  const normalized = normalizeText(chunk);
  for (const district of districts) {
    if (new RegExp(`\\b${district.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(normalized)) {
      return district.id;
    }
  }

  for (const [alias, districtId] of DISTRICT_ALIASES.entries()) {
    if (normalizeKey(normalized).includes(alias)) {
      return districtId;
    }
  }

  return null;
}

function extractDamName(chunk, districtId) {
  const lines = chunk.split("\n").map((line) => normalizeText(line)).filter(Boolean);
  for (const line of lines) {
    const candidate = line.replace(/^\d+\s*/, "").trim();
    if (!/[A-Za-z]{3,}/.test(candidate)) {
      continue;
    }
    if (districtId && DISTRICT_ALIASES.get(normalizeKey(candidate)) === districtId) {
      continue;
    }
    if (/daily water levels|irrigation reservoirs statistics|colour code|alert levels/i.test(candidate)) {
      continue;
    }
    return candidate;
  }
  return lines[0]?.replace(/^\d+\s*/, "").trim() ?? null;
}

function extractNumbers(chunk) {
  return [...chunk.matchAll(/-?\d+(?:\.\d+)?/g)].map((match) => Number.parseFloat(match[0]));
}

function buildDamRow(chunk, department) {
  const districtId = extractDistrictId(chunk);
  const damName = extractDamName(chunk, districtId);
  const numbers = extractNumbers(chunk);
  const storagePercentMatch = chunk.match(/(\d+(?:\.\d+)?)%/);
  const storagePercent = storagePercentMatch ? Number.parseFloat(storagePercentMatch[1]) : null;
  const lines = chunk.split("\n").map((line) => normalizeText(line)).filter(Boolean);
  const remarks = normalizeText(lines.slice(Math.min(lines.length - 2, 4)).join(" "));
  const releaseActive =
    /\b(outflow|spillway|sluice|shutter|discharge|opened)\b/i.test(remarks) ||
    (department === "kseb" ? (numbers[9] ?? 0) > 0 : (numbers[8] ?? 0) > 0);

  const data =
    department === "kseb"
      ? {
          frl: numbers[0] ?? null,
          current_level: numbers[1] ?? null,
          rule_level: numbers[2] ?? null,
          blue_level: numbers[3] ?? null,
          orange_level: numbers[4] ?? null,
          red_level: numbers[5] ?? null,
          storage_capacity_mcm: numbers[6] ?? null,
          storage_today_mcm: numbers[7] ?? null,
          storage_percent: storagePercent ?? numbers[8] ?? null,
          outflow_value: numbers[9] ?? null
        }
      : {
          frl: numbers[0] ?? null,
          current_level: numbers[1] ?? null,
          blue_level: numbers[2] ?? null,
          orange_level: numbers[3] ?? null,
          red_level: numbers[4] ?? null,
          storage_capacity_mcm: numbers[5] ?? null,
          storage_today_mcm: numbers[6] ?? null,
          storage_percent: storagePercent ?? numbers[7] ?? null,
          outflow_value: numbers[8] ?? null
        };

  let alertStage = "none";
  let severity = 0;
  if (data.current_level !== null && data.red_level !== null && data.current_level >= data.red_level) {
    alertStage = "red";
    severity = 0.85;
  } else if (data.current_level !== null && data.orange_level !== null && data.current_level >= data.orange_level) {
    alertStage = "orange";
    severity = 0.65;
  } else if (data.current_level !== null && data.blue_level !== null && data.current_level >= data.blue_level) {
    alertStage = "blue";
    severity = 0.45;
  } else if (releaseActive) {
    alertStage = "release";
    severity = 0.32;
  } else if ((data.storage_percent ?? 0) >= 90) {
    severity = 0.18;
  }

  return {
    dam_name: damName,
    district_id: districtId,
    department,
    remarks,
    release_active: releaseActive,
    alert_stage: alertStage,
    severity,
    ...data
  };
}

function summarizeDistrictRows(rows, department) {
  const grouped = new Map();
  for (const row of rows) {
    if (!row.district_id) {
      continue;
    }
    const bucket = grouped.get(row.district_id) ?? [];
    bucket.push(row);
    grouped.set(row.district_id, bucket);
  }

  return [...grouped.entries()].map(([districtId, districtRows]) => {
    const stagePriority = { none: 0, release: 1, blue: 2, orange: 3, red: 4 };
    const topRow = districtRows.reduce((best, row) => {
      if (!best) {
        return row;
      }
      if ((stagePriority[row.alert_stage] ?? 0) > (stagePriority[best.alert_stage] ?? 0)) {
        return row;
      }
      if ((row.severity ?? 0) > (best.severity ?? 0)) {
        return row;
      }
      return best;
    }, null);

    const activeDamCount = districtRows.filter((row) => (row.severity ?? 0) > 0).length;
    const releaseDamCount = districtRows.filter((row) => row.release_active).length;
    let summaryNote = `${department.toUpperCase()} daily dam table available from ${districtRows.length} dam${districtRows.length === 1 ? "" : "s"}`;
    if (topRow?.alert_stage === "red") {
      summaryNote = `${department.toUpperCase()} dam level at red alert for ${topRow.dam_name}`;
    } else if (topRow?.alert_stage === "orange") {
      summaryNote = `${department.toUpperCase()} dam level at orange alert for ${topRow.dam_name}`;
    } else if (topRow?.alert_stage === "blue") {
      summaryNote = `${department.toUpperCase()} dam level at blue alert for ${topRow.dam_name}`;
    } else if (releaseDamCount > 0) {
      summaryNote = `${department.toUpperCase()} controlled outflow/release active at ${releaseDamCount} dam${releaseDamCount === 1 ? "" : "s"}`;
    }

    return {
      district_id: districtId,
      severity: topRow?.severity ?? 0,
      active: activeDamCount > 0,
      alert_stage: topRow?.alert_stage ?? "none",
      dam_count: districtRows.length,
      active_dam_count: activeDamCount,
      release_dam_count: releaseDamCount,
      summary_note: summaryNote,
      dams: districtRows
    };
  });
}

async function parsePdfText(buffer) {
  const parser = new PDFParse({ data: buffer });
  try {
    const result = await parser.getText();
    return result.text;
  } finally {
    await parser.destroy();
  }
}

export async function fetchKsdmaDailyDamPayload(source) {
  const department = source.fetch_options?.department ?? "kseb";
  const pageResponse = await fetchText(source.url, { timeoutMs: source.fetch_options?.pageTimeoutMs ?? 45000 });
  if (!pageResponse.ok) {
    return {
      ok: false,
      status: pageResponse.status,
      text: "",
      note: `KSDMA dam-water-level page fetch failed for ${department.toUpperCase()}.`
    };
  }

  const pdfLink = extractPdfLink(pageResponse.text, department);
  const absolutePdfUrl = new URL(pdfLink.href, "https://sdma.kerala.gov.in/dam-water-level/").toString();
  const pdfResponse = await fetchBuffer(wrapTargetUrl(source.url, absolutePdfUrl), {
    timeoutMs: source.fetch_options?.pdfTimeoutMs ?? 60000
  });
  if (!pdfResponse.ok) {
    return {
      ok: false,
      status: pdfResponse.status,
      text: "",
      note: `KSDMA ${department.toUpperCase()} PDF fetch failed.`
    };
  }

  const pdfText = await parsePdfText(pdfResponse.buffer);
  const rows = splitRowChunks(
    pdfText,
    department === "kseb" ? "Daily Water Levels Details of Major Power Generation Dams" : "IRRIGATION RESERVOIRS STATISTICS"
  ).map((chunk) => buildDamRow(chunk, department)).filter((row) => row.dam_name && row.district_id);

  const districts = summarizeDistrictRows(rows, department);
  return {
    ok: true,
    status: 200,
    text: JSON.stringify({
      issued_at: extractIssuedAt(pageResponse.text, pdfLink.href) ?? parseDate(pageResponse.text.match(/(\d{2}\/\d{2}\/\d{4})/)?.[1]?.split("/").reverse().join("-"))?.toISOString() ?? null,
      department,
      pdf_url: absolutePdfUrl,
      pdf_label: pdfLink.label,
      district_count: districts.length,
      dam_count: rows.length,
      release_dam_count: rows.filter((row) => row.release_active).length,
      alert_active: districts.some((district) => ["blue", "orange", "red"].includes(district.alert_stage)),
      release_preparedness: rows.some((row) => row.release_active),
      districts,
      dams: rows
    }),
    note: `KSDMA ${department.toUpperCase()} daily dam PDF ${pdfLink.label || absolutePdfUrl}`
  };
}
