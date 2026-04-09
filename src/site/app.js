const state = {
  horizon: "now",
  mapScope: "district",
  payload: null,
  archiveIndex: null,
  districtGeometry: null
};

const levelColors = {
  Normal: "var(--normal)",
  Watch: "var(--watch)",
  Alert: "var(--alert)",
  "Severe - review required": "var(--severe)",
  "Reviewed severe alert": "var(--severe-reviewed)"
};

const references = {
  headlineText: document.querySelector("#headline-text"),
  headlineCard: document.querySelector("#headline-card"),
  generatedChip: document.querySelector("#generated-chip"),
  modeChip: document.querySelector("#mode-chip"),
  reviewCount: document.querySelector("#review-count"),
  districtLayer: document.querySelector("#district-layer"),
  hotspotFootprintLayer: document.querySelector("#hotspot-footprint-layer"),
  districtLabelLayer: document.querySelector("#district-label-layer"),
  alertsList: document.querySelector("#alerts-list"),
  districtGrid: document.querySelector("#district-grid"),
  talukGrid: document.querySelector("#taluk-grid"),
  hotspotGrid: document.querySelector("#hotspot-grid"),
  sourceGrid: document.querySelector("#source-grid"),
  dialog: document.querySelector("#evidence-dialog"),
  dialogContent: document.querySelector("#dialog-content"),
  timeframeToggle: document.querySelector("#timeframe-toggle"),
  dialogClose: document.querySelector("#dialog-close"),
  archiveSelect: document.querySelector("#archive-select")
};

const mapViewBox = { width: 420, height: 720, padding: 34 };
const districtNameLookup = {
  alappuzha: "alappuzha",
  ernakulam: "ernakulam",
  idukki: "idukki",
  kannur: "kannur",
  kasaragod: "kasaragod",
  kollam: "kollam",
  kottayam: "kottayam",
  kozhikode: "kozhikode",
  malappuram: "malappuram",
  palakkad: "palakkad",
  pathanamthitta: "pathanamthitta",
  thiruvanthapuram: "thiruvananthapuram",
  thiruvananthapuram: "thiruvananthapuram",
  thrissur: "thrissur",
  wayanad: "wayanad"
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function safeStatusClass(status) {
  switch (status) {
    case "ok":
    case "stale":
    case "offline":
    case "degraded":
      return status;
    default:
      return "degraded";
  }
}

function renderTextList(items = [], className = "evidence-list") {
  return `<ul class="${escapeAttr(className)}">${items
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("")}</ul>`;
}

function levelPill(level) {
  return `<span class="level-pill" style="background:${escapeAttr(levelColors[level] ?? "var(--accent)")}">${escapeHtml(level)}</span>`;
}

async function fetchJson(url, fallback) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    if (fallback !== undefined) {
      return fallback;
    }
    throw new Error(`${response.status} ${url}`);
  }
  return response.json();
}

function formatTime(value) {
  if (!value) {
    return "Unknown";
  }
  return new Date(value).toLocaleString("en-IN", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Kolkata"
  });
}

function horizonAdjustedScore(item) {
  if (state.horizon === "now") {
    return item.score;
  }
  if (state.horizon === "6h") {
    return Math.min(100, Math.round(item.score + (item.level === "Watch" ? 4 : 7)));
  }
  return Math.max(0, Math.round(item.score * 0.88 + (item.susceptibility ?? 0.5) * 18));
}

function horizonLabel() {
  if (state.horizon === "6h") {
    return "Next 6 hours";
  }
  if (state.horizon === "72h") {
    return "Next 72 hours";
  }
  return "Now";
}

function sortByHorizon(items) {
  return [...items].sort((left, right) => horizonAdjustedScore(right) - horizonAdjustedScore(left));
}

function firstMatchingDriver(drivers = [], patterns = []) {
  return drivers.find((driver) => patterns.some((pattern) => pattern.test(driver))) ?? null;
}

function humanizeCardSummary(item, summary) {
  if (!summary) {
    return "No current local drivers beyond baseline terrain and runoff context.";
  }

  if (item.area_type === "district") {
    return summary
      .replace(/^Observed 24h rain /i, "District-average rain in the last 24h: ")
      .replace(/ and 1h rain /i, "; last 1h: ")
      .replace(/^India-WRIS official 24h rainfall /i, "Official district rain gauges recorded ")
      .replace(/^IMD district nowcast /i, "District nowcast signal ")
      .replace(/^IMD district warning /i, "District warning in force ")
      .replace(/^RainViewer nowcast /i, "Radar nowcast signal ")
      .replace(/^Runoff potential /i, "Runoff context ");
  }

  if (item.area_type === "taluk") {
    return summary
      .replace(/^Observed taluk 24h rain /i, "Local taluk rain estimate in the last 24h: ")
      .replace(/ and 1h rain /i, "; last 1h: ")
      .replace(/^[0-9]+ mapped hotspot/i, (match) => `Mapped hotspot context: ${match.toLowerCase()}`)
      .replace(/^Runoff potential /i, "Runoff context ")
      .replace(/^IMD district warning /i, "District warning context ");
  }

  if (item.area_type === "hotspot") {
    return summary
      .replace(/^Hotspot radar echo /i, "Nearby radar echo ")
      .replace(/^Hotspot runoff potential /i, "Hotspot runoff context ")
      .replace(/^IMD station nowcast /i, "Nearby station nowcast ")
      .replace(/^Observed 24h rain /i, "Nearby rain context in the last 24h: ")
      .replace(/ and 1h rain /i, "; last 1h: ")
      .replace(/^Terrain susceptibility /i, "Terrain susceptibility ")
      .replace(/^Hotspot susceptibility /i, "Hotspot susceptibility ")
      .replace(/^IMD district warning /i, "District warning context ");
  }

  return summary;
}

function cardSummary(item) {
  const drivers = item.drivers ?? [];
  const areaType = item.area_type ?? "";

  if (areaType === "district") {
    return humanizeCardSummary(item, (
      firstMatchingDriver(drivers, [
        /^Observed 24h rain/i,
        /^India-WRIS official 24h rainfall/i,
        /^RainViewer nowcast/i,
        /^IMD district nowcast/i,
        /^CWC river-stage/i,
        /^Reservoir caution active/i,
        /^Dam downstream caution active/i,
        /^Runoff potential/i
      ]) ??
      firstMatchingDriver(drivers, [/^IMD district warning/i]) ??
      "No active district drivers beyond baseline terrain and runoff context."
    ));
  }

  if (areaType === "taluk") {
    return humanizeCardSummary(item, (
      firstMatchingDriver(drivers, [
        /^Observed taluk 24h rain/i,
        /^[0-9]+ mapped hotspot/i,
        /^Runoff potential/i,
        /^Reservoir caution active/i,
        /^Dam downstream caution active/i
      ]) ??
      firstMatchingDriver(drivers, [/^IMD district warning/i]) ??
      "No active taluk drivers beyond baseline terrain and runoff context."
    ));
  }

  if (areaType === "hotspot") {
    return humanizeCardSummary(item, (
      firstMatchingDriver(drivers, [
        /^IMD station nowcast/i,
        /^Hotspot radar echo/i,
        /^Hotspot runoff potential/i,
        /^Observed 24h rain/i,
        /^India-WRIS official 24h rainfall/i,
        /^Peak India-WRIS station 24h rainfall/i,
        /^Reservoir caution active/i,
        /^Dam downstream caution active/i,
        /^Terrain susceptibility/i,
        /^Hotspot susceptibility/i
      ]) ??
      firstMatchingDriver(drivers, [/^IMD district warning/i]) ??
      "No active hotspot drivers beyond baseline terrain and runoff context."
    ));
  }

  return humanizeCardSummary(
    item,
    drivers[0] ?? "No active drivers beyond baseline susceptibility."
  );
}

function cardScopeLabel(item, suffix = "") {
  if (suffix) {
    return suffix;
  }
  return item.region ?? item.district_name ?? item.district_id ?? "";
}

function openEvidence(title, body) {
  references.dialogContent.innerHTML = `<h2>${escapeHtml(title)}</h2>${body}`;
  references.dialog.showModal();
}

function publicSourceUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") {
    return null;
  }
  if (!/^https?:/i.test(rawUrl)) {
    return null;
  }
  try {
    const parsed = new URL(rawUrl);
    const proxiedUrl = parsed.searchParams.get("url");
    return proxiedUrl || rawUrl;
  } catch {
    return rawUrl;
  }
}

function sourceLinkMarkup(sourceId) {
  const source = state.payload?.sources?.sources?.find((item) => item.source_id === sourceId);
  const meta = SOURCE_META[sourceId] ?? {};
  const link = meta.source_url ?? publicSourceUrl(source?.raw_url);
  if (!link) {
    return "";
  }
  return ` <a class="source-link" href="${escapeAttr(link)}" target="_blank" rel="noopener noreferrer">Open source</a>`;
}

function normalizeBoundaryName(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
}

function getDistrictIdFromFeature(feature) {
  const name =
    feature?.properties?.DISTRICT ??
    feature?.properties?.district ??
    feature?.properties?.name ??
    feature?.properties?.NAME_2 ??
    "";
  return districtNameLookup[normalizeBoundaryName(name)] ?? null;
}

function collectCoordinates(coordinates, visitor) {
  if (!Array.isArray(coordinates)) {
    return;
  }

  if (
    coordinates.length >= 2 &&
    Number.isFinite(coordinates[0]) &&
    Number.isFinite(coordinates[1])
  ) {
    visitor(coordinates[0], coordinates[1]);
    return;
  }

  coordinates.forEach((entry) => collectCoordinates(entry, visitor));
}

function geometryBounds(geometry) {
  const bounds = {
    minLon: Number.POSITIVE_INFINITY,
    maxLon: Number.NEGATIVE_INFINITY,
    minLat: Number.POSITIVE_INFINITY,
    maxLat: Number.NEGATIVE_INFINITY
  };

  collectCoordinates(geometry?.coordinates, (lon, lat) => {
    bounds.minLon = Math.min(bounds.minLon, lon);
    bounds.maxLon = Math.max(bounds.maxLon, lon);
    bounds.minLat = Math.min(bounds.minLat, lat);
    bounds.maxLat = Math.max(bounds.maxLat, lat);
  });

  return bounds;
}

function geometryCollectionBounds(features) {
  return features.reduce(
    (accumulator, feature) => {
      const bounds = geometryBounds(feature.geometry);
      return {
        minLon: Math.min(accumulator.minLon, bounds.minLon),
        maxLon: Math.max(accumulator.maxLon, bounds.maxLon),
        minLat: Math.min(accumulator.minLat, bounds.minLat),
        maxLat: Math.max(accumulator.maxLat, bounds.maxLat)
      };
    },
    {
      minLon: Number.POSITIVE_INFINITY,
      maxLon: Number.NEGATIVE_INFINITY,
      minLat: Number.POSITIVE_INFINITY,
      maxLat: Number.NEGATIVE_INFINITY
    }
  );
}

function buildProjector(bounds) {
  const width = mapViewBox.width - mapViewBox.padding * 2;
  const height = mapViewBox.height - mapViewBox.padding * 2;
  const lonSpan = bounds.maxLon - bounds.minLon || 1;
  const latSpan = bounds.maxLat - bounds.minLat || 1;

  return (lon, lat) => ({
    x: mapViewBox.padding + ((lon - bounds.minLon) / lonSpan) * width,
    y: mapViewBox.height - mapViewBox.padding - ((lat - bounds.minLat) / latSpan) * height
  });
}

function ringToPath(ring, project) {
  return ring
    .map(([lon, lat], index) => {
      const point = project(lon, lat);
      return `${index === 0 ? "M" : "L"}${point.x.toFixed(2)} ${point.y.toFixed(2)}`;
    })
    .join(" ");
}

function geometryToPath(geometry, project) {
  if (geometry.type === "Polygon") {
    return geometry.coordinates.map((ring) => `${ringToPath(ring, project)} Z`).join(" ");
  }

  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates
      .map((polygon) => polygon.map((ring) => `${ringToPath(ring, project)} Z`).join(" "))
      .join(" ");
  }

  return "";
}

function featureToPath(feature, project) {
  return geometryToPath(feature.geometry, project);
}

function centroidFromBounds(bounds, project) {
  return project((bounds.minLon + bounds.maxLon) / 2, (bounds.minLat + bounds.maxLat) / 2);
}

function areaItemFromEvent(areaId, areaType) {
  const collection =
    areaType === "district"
      ? state.payload.districtRisk.districts
      : areaType === "taluk"
        ? state.payload.talukRisk.taluks
        : state.payload.hotspotRisk.hotspots;
  return collection.find((entry) => entry.area_id === areaId);
}

function hotspotPosition(hotspot, projectedCentroids, districtAnchorsById, project) {
  if (hotspot.location?.lon && hotspot.location?.lat) {
    return project(hotspot.location.lon, hotspot.location.lat);
  }

  const districtCentroid = projectedCentroids[hotspot.district_id];
  const districtAnchor = districtAnchorsById[hotspot.district_id] ?? { x: 50, y: 50 };
  const dx = ((hotspot.anchor.x - districtAnchor.x) / 100) * mapViewBox.width * 0.42;
  const dy = ((hotspot.anchor.y - districtAnchor.y) / 100) * mapViewBox.height * 0.42;
  return {
    x: districtCentroid.x + dx,
    y: districtCentroid.y + dy
  };
}

function bindMapInteractions() {
  document.querySelectorAll("[data-area-id][data-area-type]").forEach((element) => {
    element.addEventListener("click", () => {
      const item = areaItemFromEvent(element.dataset.areaId, element.dataset.areaType);
      if (!item) {
        return;
      }
      openEvidence(
        item.name,
        `
          ${levelPill(item.level)}
          <p><strong>Composite score:</strong> ${horizonAdjustedScore(item).toFixed(0)} / 100</p>
          <p><strong>Confidence:</strong> ${(item.confidence * 100).toFixed(0)}%</p>
          <h3>Drivers</h3>
          ${renderTextList(item.drivers)}
          <h3>Source evidence</h3>
          <ul class="evidence-list">
            ${item.source_refs
              .map(
                (source) =>
                  `<li><strong>${escapeHtml(source.source_id)}</strong>: ${escapeHtml(source.detail)} (${escapeHtml(source.status)}, freshness ${escapeHtml(source.freshness_minutes ?? "n/a")} min)${sourceLinkMarkup(source.source_id)}</li>`
              )
              .join("")}
          </ul>
        `
      );
    });
  });
}

function renderHeadline() {
  const topAlert = state.payload.alerts.alerts[0];
  const alerts = state.payload.alerts.alerts;
  const affectedDistricts = new Set(
    alerts
      .map((alert) => alert.district_id ?? (alert.area_type === "district" ? alert.area_id : null))
      .filter(Boolean)
  );
  references.headlineText.textContent = topAlert
    ? `${alerts.length} active alert${alerts.length === 1 ? "" : "s"} across ${affectedDistricts.size || 1} district${affectedDistricts.size === 1 ? "" : "s"}`
    : "No active Watch-or-higher alerts";
  references.generatedChip.textContent = `Updated ${formatTime(state.payload.dashboard.generated_at)}`;
  references.modeChip.textContent = `${state.payload.dashboard.mode} mode`;
  references.reviewCount.textContent = String(state.payload.dashboard.severe_pending_count);

  references.headlineCard.innerHTML = topAlert
    ? `
      ${levelPill(topAlert.level)}
      <h3>Top concern: ${escapeHtml(topAlert.name)}</h3>
      <p>${alerts.length === 1 ? "Current top alert" : `${alerts.length} active alerts are listed below`}. Highest current concern: ${escapeHtml(topAlert.message_en)}</p>
      <div class="meta">
        <span>${horizonLabel()}</span>
        <span>${affectedDistricts.size || 1} district${affectedDistricts.size === 1 ? "" : "s"} affected</span>
        <span>${escapeHtml(topAlert.review_state.replaceAll("_", " "))}</span>
      </div>
    `
    : `
      ${levelPill("Normal")}
      <h3>Routine monitoring</h3>
      <p>No active Watch-or-higher alerts. Continue routine observation and source-health checks.</p>
      <div class="meta">
        <span>${horizonLabel()}</span>
        <span>${state.payload.sources.sources.filter((source) => source.status === "ok").length} sources currently healthy</span>
      </div>
    `;
}

function renderMap() {
  const { areas, districtRisk, hotspotRisk } = state.payload;
  const districtById = Object.fromEntries(districtRisk.districts.map((item) => [item.area_id, item]));
  const hotspotById = Object.fromEntries(hotspotRisk.hotspots.map((item) => [item.area_id, item]));

  if (!state.districtGeometry?.features?.length) {
    references.districtLayer.innerHTML = "";
    references.hotspotFootprintLayer.innerHTML = "";
    references.districtLabelLayer.innerHTML = "";
    bindMapInteractions();
    return;
  }

  const visibleFeatures = state.districtGeometry.features
    .map((feature) => ({ ...feature, district_id: getDistrictIdFromFeature(feature) }))
    .filter((feature) => feature.district_id && districtById[feature.district_id]);

  if (!visibleFeatures.length) {
    references.districtLayer.innerHTML = "";
    references.districtLabelLayer.innerHTML = "";
    references.hotspotFootprintLayer.innerHTML = "";
    bindMapInteractions();
    return;
  }

  const bounds = geometryCollectionBounds(visibleFeatures);
  const project = buildProjector(bounds);
  const districtAnchorsById = Object.fromEntries(areas.districts.map((district) => [district.id, district.anchor]));
  const projectedCentroids = {};

  references.districtLayer.innerHTML = visibleFeatures
    .map((feature) => {
      const areaId = feature.district_id;
      const item = districtById[feature.district_id];
      const pathData = geometryToPath(feature.geometry, project);
      const level = item?.level ?? "Normal";
      const centroid = centroidFromBounds(geometryBounds(feature.geometry), project);
      projectedCentroids[areaId] = centroid;
      return `
        <path
          class="district-shape"
          data-area-id="${escapeAttr(areaId)}"
          data-area-type="district"
          d="${pathData}"
          fill="${escapeAttr(levelColors[level] ?? "var(--normal)")}"
          title="${escapeAttr(item?.name ?? areaId)}"
        ></path>
      `;
    })
    .join("");

  references.districtLabelLayer.innerHTML = visibleFeatures
    .map((feature) => {
      const areaId = feature.district_id;
      const centroid = projectedCentroids[areaId];
      const item = districtById[feature.district_id];
      return `
        <text class="district-label" x="${centroid.x.toFixed(1)}" y="${(centroid.y + 4).toFixed(1)}">
          ${escapeHtml(item?.name ?? feature.district_id)}
        </text>
      `;
    })
    .join("");

  references.hotspotFootprintLayer.innerHTML = [
    ...areas.hotspots
      .filter((hotspot) => hotspot.footprint?.geometry)
      .map((hotspot) => {
        const item = hotspotById[hotspot.id];
        const level = item?.level ?? "Normal";
        return `
          <path
            class="hotspot-footprint"
            data-area-id="${escapeAttr(hotspot.id)}"
            data-area-type="hotspot"
            d="${featureToPath(hotspot.footprint, project)}"
            fill="${escapeAttr(levelColors[level] ?? "var(--normal)")}"
          ></path>
        `;
      }),
    ...areas.hotspots.map((hotspot) => {
      const item = hotspotById[hotspot.id];
      const level = item?.level ?? "Normal";
      const position = hotspotPosition(hotspot, projectedCentroids, districtAnchorsById, project);
      return `
        <circle
          class="hotspot-marker"
          data-area-id="${escapeAttr(hotspot.id)}"
          data-area-type="hotspot"
          cx="${position.x.toFixed(1)}"
          cy="${position.y.toFixed(1)}"
          r="6"
          fill="${escapeAttr(levelColors[level] ?? "var(--normal)")}"
        ></circle>
      `;
    })
  ].join("");

  bindMapInteractions();
}

function renderAlerts() {
  references.alertsList.innerHTML = sortByHorizon(state.payload.alerts.alerts)
    .map(
      (alert) => `
        <article class="alert-row" data-alert-id="${escapeAttr(alert.alert_id)}">
          <div>
            ${levelPill(alert.level)}
            <h3>${escapeHtml(alert.name)}</h3>
            <p>${escapeHtml(alert.message_en)}</p>
          </div>
          <div class="meta">
            <span>${horizonLabel()}</span>
            <span>Score ${horizonAdjustedScore(alert)}</span>
            <span>${escapeHtml(alert.review_state.replaceAll("_", " "))}</span>
          </div>
          <button class="chip subtle" type="button">Evidence</button>
        </article>
      `
    )
    .join("");

  references.alertsList.querySelectorAll(".alert-row").forEach((row) => {
    row.addEventListener("click", () => {
      const alert = state.payload.alerts.alerts.find((item) => item.alert_id === row.dataset.alertId);
      openEvidence(
        `${alert.name} alert`,
        `
          ${levelPill(alert.level)}
          <p>${escapeHtml(alert.message_en)}</p>
          <h3>Drivers</h3>
          ${renderTextList(alert.drivers)}
          <h3>Recommended actions</h3>
          ${renderTextList(alert.recommended_actions, "actions-list")}
        `
      );
    });
  });
}

function renderRiskCards(target, items, suffix = "") {
  target.innerHTML = sortByHorizon(items)
    .map(
      (item) => `
        <article class="risk-card" data-id="${escapeAttr(item.area_id)}">
          ${levelPill(item.level)}
          <h3>${escapeHtml(item.name)}</h3>
          <div class="score">${horizonAdjustedScore(item)}</div>
          <p>${escapeHtml(cardSummary(item))}</p>
          <div class="meta">
            <span>${escapeHtml(cardScopeLabel(item, suffix))}</span>
          </div>
        </article>
      `
    )
    .join("");
}

const SOURCE_META = {
  "imd-cap-rss": {
    description: "Official severe weather warnings for Kerala",
    method: "XML RSS feed from NDMA/IMD",
    cadence: "Every 15 min",
    impact: "No official warning data. Scores rely on satellite and ground observations only."
  },
  "imd-flash-flood-bulletin": {
    description: "IMD meteorologist flash flood risk guidance",
    method: "HTML scraper from mausam.imd.gov.in",
    cadence: "Every 3 hrs",
    impact: "No expert meteorological guidance. Automated data sources still active."
  },
  "imd-district-warning": {
    description: "Official district-level warning map for Kerala.",
    method: "HTML page parser from mausam.imd.gov.in",
    cadence: "About every 60 min",
    impact: "No district-level IMD warning support. Scores rely more on CAP, nowcast, radar, and rainfall context.",
    source_url: "https://mausam.imd.gov.in/imd_latest/contents/districtwise-warning_mc.php?id=4"
  },
  "imd-district-nowcast": {
    description: "Official district-level nowcast map for Kerala.",
    method: "HTML page parser from mausam.imd.gov.in",
    cadence: "About every 20 min",
    impact: "No district-level IMD nowcast support. Short-lead weather context relies more on radar and rainfall.",
    source_url: "https://mausam.imd.gov.in/imd_latest/contents/districtwisewarnings_mc.php?id=4"
  },
  "imd-station-nowcast": {
    description: "Station-level IMD nowcast mapped cautiously to nearby curated hotspots.",
    method: "HTML page parser from mausam.imd.gov.in",
    cadence: "About every 20 min",
    impact: "No station-level IMD nowcast support. Hotspots rely more on district warnings, radar, rainfall, and hydrology context.",
    source_url: "https://mausam.imd.gov.in/imd_latest/contents/stationwise-nowcast-warning_mc.php?id=4"
  },
  "indiawris-rainfall": {
    description: "Ground rain gauge readings across Kerala",
    method: "JSON API from India-WRIS",
    cadence: "Every 3 hrs",
    impact: "No ground-truth rainfall. Satellite-only estimates (NASA IMERG) used instead.",
    source_url: "https://indiawris.gov.in/swagger-ui/index.html"
  },
  "indiawris-river-level": {
    description: "River water level from CWC gauge stations",
    method: "JSON API from India-WRIS",
    cadence: "Every 3 hrs",
    impact: "No India-WRIS river level context. Live CWC FFS river-stage evidence may still be available.",
    source_url: "https://indiawris.gov.in/swagger-ui/index.html"
  },
  "ksdma-reservoirs": {
    description: "KSEB daily major dam and reservoir levels from KSDMA PDF",
    method: "Page scraper + PDF parser from KSDMA",
    cadence: "Every 1 hr",
    impact: "No KSEB daily dam-level context. Reservoir-related modifiers inactive."
  },
  "ksdma-dam-management": {
    description: "Irrigation daily dam levels and outflow remarks from KSDMA PDF",
    method: "Page scraper + PDF parser from KSDMA",
    cadence: "Every 1 hr",
    impact: "No irrigation daily dam context. Downstream consequence modifiers inactive."
  },
  "cwc-ffs": {
    description: "Live river levels from CWC Flood Forecasting Service stations",
    method: "JSON API from CWC FFS",
    cadence: "Every 1 hr",
    impact: "No live CWC FFS river-stage data. River flood scoring falls back to India-WRIS water level where available."
  },
  "rainviewer-radar": {
    description: "Real-time Doppler radar rain imagery",
    method: "JSON API from RainViewer (public)",
    cadence: "Every 10 min",
    impact: "No short-range radar nowcasting. 0-2 hour storm tracking unavailable."
  },
  "nasa-imerg-nrt": {
    description: "Satellite-estimated rainfall (half-hourly)",
    method: "GeoTIFF raster download from NASA PPS",
    cadence: "Every 30 min",
    impact: "Primary rainfall source offline. Scores depend entirely on ground gauges."
  },
  "operator-observations": {
    description: "Manual human observation input",
    method: "Local JSON file (data/manual/observations.json)",
    cadence: "On demand",
    impact: "No manual overrides active. Fully automated scoring in effect."
  }
};

function sourceStatusMessage(source) {
  if (source.published_from_cache) {
    return source.status === "offline"
      ? "This published snapshot reflects the latest refresh state, and this source is currently unavailable."
      : "This published snapshot was rebuilt from the latest refresh state for this source.";
  }
  if (source.fetch_status === "failed_cached") {
    return "This source is unavailable in the latest refresh state.";
  }
  if (source.fetch_status === "skipped_cached") {
    return "This source was not part of the latest refresh flow.";
  }
  if (source.fetch_status === "skipped") {
    return "This source was not part of the latest refresh flow.";
  }
  if (source.fetch_status === "failed") {
    return "This source was unavailable in the latest refresh state, so current scoring is proceeding without it.";
  }
  if (source.parser_status === "failed") {
    return "This source is unavailable in the latest refresh state, so current scoring is proceeding without it.";
  }
  if (source.status === "offline") {
    return "This source is currently unavailable, so current scoring is proceeding without it.";
  }
  if (source.status === "stale") {
    if (source.category === "official-warning") {
      return "This warning source is older than usual. It may describe the last valid warning rather than a brand-new one.";
    }
    return "This source data is older than the normal freshness window, so it is being treated cautiously.";
  }
  if (source.status === "degraded") {
    return "This source is only partly usable right now. Some fields or mappings may be incomplete.";
  }
  return "This source is current for this run.";
}

function sourceHealthLabel(source) {
  switch (source.status) {
    case "ok":
      return "Current";
    case "stale":
      return "Older than usual";
    case "offline":
      return "Unavailable";
    case "degraded":
      return "Partly available";
    default:
      return source.status;
  }
}

function sourceDataPublishedLabel(source) {
  if (source.issued_at) {
    return formatTime(source.issued_at);
  }
  if (source.fetch_status === "failed" || source.parser_status === "failed" || source.status === "offline") {
    return "No usable source timestamp";
  }
  return "Source did not publish a timestamp";
}

function sourceLastCheckedLabel(source) {
  const checkedAt = source.fetched_at;
  return checkedAt ? formatTime(checkedAt) : "Unknown";
}

function sourceCurrentRunLabel(source) {
  if (source.published_from_cache) {
    return source.status === "offline"
      ? "Published using latest refresh result: unavailable"
      : "Published using latest refresh result";
  }
  if (source.fetch_status === "ok" && source.parser_status === "ok") {
    return "Live refresh succeeded";
  }
  if (source.fetch_status === "failed_cached") {
    return "Latest refresh result: unavailable";
  }
  if (source.fetch_status === "skipped_cached") {
    return "Not checked in this refresh flow";
  }
  if (source.fetch_status === "skipped") {
    return "Not checked in this refresh flow";
  }
  if (source.fetch_status === "failed") {
    return "Latest refresh failed";
  }
  if (source.parser_status === "failed") {
    return "Latest refresh parse failed";
  }
  return "Checked in this run";
}

function formatFreshness(minutes) {
  if (minutes === null || minutes === undefined || isNaN(minutes)) return "n/a";
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes} min${minutes === 1 ? "" : "s"} ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hr${hours === 1 ? "" : "s"} ago`;
  const days = (minutes / 1440).toFixed(1);
  const cleanlyFormattedDays = days.endsWith(".0") ? days.slice(0, -2) : days;
  return `${cleanlyFormattedDays} day${cleanlyFormattedDays === "1" ? "" : "s"} ago`;
}

function formatCadence(minutes) {
  if (!minutes) return "Unknown";
  if (minutes < 60) return `Every ${minutes} min`;
  const hours = Math.floor(minutes / 60);
  return `Every ${hours} hr${hours === 1 ? "" : "s"}`;
}

function dataUsageSummary(source) {
  const dataPublishedLabel = source.issued_at ? formatTime(source.issued_at) : "an unknown publication time";
  const refreshLabel = source.fetched_at
    ? formatTime(source.fetched_at)
    : "the latest refresh attempt";

  if (source.published_from_cache) {
    if (source.status === "offline") {
      return `This page was rebuilt from the latest saved refresh result. We last checked this source at ${refreshLabel}, but it was unavailable then, so it is not contributing to scoring.`;
    }
    return `Showing source data published at ${dataPublishedLabel}. This page was rebuilt from the latest saved refresh result, and our system last checked this source at ${refreshLabel}.`;
  }

  if (source.fetch_status === "failed_cached") {
    return `This source was last checked at ${refreshLabel} and was unavailable in the latest refresh result, so it is not contributing to scoring right now.`;
  }
  if (source.fetch_status === "skipped_cached") {
    return `Showing source data published at ${dataPublishedLabel}. This source was not checked in the latest refresh flow.`;
  }
  if (source.fetch_status === "skipped") {
    return "This source was not checked in this refresh flow.";
  }
  if (source.fetch_status === "failed") {
    return `We last checked this source at ${refreshLabel}, but the latest refresh failed, so it is not contributing to scoring right now.`;
  }
  if (source.parser_status === "failed") {
    return `We last checked this source at ${refreshLabel}, but the latest refresh could not parse it, so it is not contributing to scoring right now.`;
  }
  return `Showing source data published at ${dataPublishedLabel}.`;
}

function openSourceDetails(source) {
  const meta = SOURCE_META[source.source_id] ?? {};
  const freshLabel = formatFreshness(source.freshness_minutes);
  const cadenceLabel = meta.cadence ?? "Unknown";
  const fetchNote = source.notes || source.summary?.excerpt || "None";
  const sourceLink = meta.source_url ?? publicSourceUrl(source.raw_url);
  const fetchFailed = source.fetch_status === "failed";
  const fetchFallback = source.fetch_status === "failed_cached";
  const parserFailed = source.parser_status === "failed";
  const parserFallback = source.parser_status === "failed_cached";
  const parserStateClass =
    parserFailed ? "status-offline" : parserFallback ? "status-degraded" : source.parser_status === "ok" ? "status-ok" : "status-degraded";
  const fetchStateClass =
    fetchFailed ? "status-offline" : fetchFallback ? "status-degraded" : source.fetch_status === "ok" ? "status-ok" : "status-degraded";
  const sourceStatusClass = safeStatusClass(source.status);

  openEvidence(
    source.name,
    `
      <p class="source-detail-desc">${escapeHtml(meta.description ?? source.name)}</p>
      <p class="source-detail-desc"><strong>What this page is using:</strong> ${escapeHtml(dataUsageSummary(source))}</p>
      ${
        sourceLink
          ? `<p class="source-detail-desc"><strong>Source link:</strong> <a class="source-link" href="${escapeAttr(sourceLink)}" target="_blank" rel="noopener noreferrer">Open original source</a></p>`
          : ""
      }
      <div class="source-detail-grid">
        <div class="source-detail-row">
          <span class="source-detail-label">Status</span>
          <span class="status-${sourceStatusClass} source-detail-value">${escapeHtml(sourceHealthLabel(source))}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Data age</span>
          <span class="source-detail-value">${escapeHtml(freshLabel)}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Source data time</span>
          <span class="source-detail-value">${escapeHtml(sourceDataPublishedLabel(source))}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Last checked by our system</span>
          <span class="source-detail-value">${escapeHtml(sourceLastCheckedLabel(source))}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Latest refresh result</span>
          <span class="source-detail-value">${escapeHtml(sourceCurrentRunLabel(source))}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Expected cadence</span>
          <span class="source-detail-value">${escapeHtml(cadenceLabel)}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Collection method</span>
          <span class="source-detail-value">${escapeHtml(meta.method ?? "Unknown")}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Fetch</span>
          <span class="source-detail-value ${fetchStateClass}">${escapeHtml(source.fetch_status ?? "unknown")}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Parser</span>
          <span class="source-detail-value ${parserStateClass}">${escapeHtml(source.parser_status)}</span>
        </div>
      </div>
      ${fetchNote !== "None" ? `
        <h3>${fetchFailed || fetchFallback ? "Fetch Notes" : parserFailed || parserFallback ? "Parser Notes" : "Notes"}</h3>
        <p class="source-detail-fetch-note">${escapeHtml(fetchNote)}</p>
      ` : ""}
      ${source.status === "offline" || source.status === "degraded" ? `
        <h3>Impact</h3>
        <p class="source-detail-impact">${escapeHtml(meta.impact ?? sourceStatusMessage(source))}</p>
      ` : ""}
    `
  );
}

function renderSources() {
  references.sourceGrid.innerHTML = state.payload.sources.sources
    .map(
      (source) => {
        const statusClass = safeStatusClass(source.status);
        return `
          <article class="source-card" data-source-id="${escapeAttr(source.source_id)}">
            <button class="source-info-btn" title="View details" type="button">i</button>
            <div class="label">${escapeHtml(source.owner)}</div>
            <h3>${escapeHtml(source.name)}</h3>
            <div class="score status-${statusClass}">${escapeHtml(sourceHealthLabel(source))}</div>
            <div class="source-facts">
              <div class="source-fact-row">
                <span class="source-fact-label">Data age</span>
                <span class="source-fact-value">${escapeHtml(formatFreshness(source.freshness_minutes))}</span>
              </div>
              <div class="source-fact-row">
                <span class="source-fact-label">Source data time</span>
                <span class="source-fact-value">${escapeHtml(sourceDataPublishedLabel(source))}</span>
              </div>
              <div class="source-fact-row">
                <span class="source-fact-label">Last checked by our system</span>
                <span class="source-fact-value">${escapeHtml(sourceLastCheckedLabel(source))}</span>
              </div>
              <div class="source-fact-row">
                <span class="source-fact-label">Latest refresh result</span>
                <span class="source-fact-value">${escapeHtml(sourceCurrentRunLabel(source))}</span>
              </div>
            </div>
          </article>
        `;
      }
    )
    .join("");

  references.sourceGrid.querySelectorAll(".source-info-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const card = btn.closest(".source-card");
      const source = state.payload.sources.sources.find(
        (s) => s.source_id === card.dataset.sourceId
      );
      if (source) openSourceDetails(source);
    });
  });
}

function renderAll() {
  renderHeadline();
  renderMap();
  renderAlerts();
  renderRiskCards(references.districtGrid, state.payload.districtRisk.districts);
  renderRiskCards(references.talukGrid, state.payload.talukRisk.taluks);
  renderRiskCards(references.hotspotGrid, state.payload.hotspotRisk.hotspots, "Hotspot");
  renderSources();
}

async function loadPayload() {
  const fresh = `t=${Date.now()}`;
  const [areas, dashboard, sources, districtRisk, talukRisk, hotspotRisk, alerts, archiveIndex, districtGeometry] = await Promise.all([
    fetchJson("./data/static/areas.json"),
    fetchJson(`./data/latest/dashboard.json?${fresh}`),
    fetchJson(`./data/latest/sources.json?${fresh}`),
    fetchJson(`./data/latest/district-risk.json?${fresh}`),
    fetchJson(`./data/latest/taluk-risk.json?${fresh}`, { generated_at: null, taluks: [] }),
    fetchJson(`./data/latest/hotspot-risk.json?${fresh}`),
    fetchJson(`./data/latest/alerts.json?${fresh}`),
    fetchJson(`./data/archive/index.json?${fresh}`),
    fetchJson("./assets/kerala-districts.geojson")
  ]);

  state.archiveIndex = archiveIndex;
  state.districtGeometry = districtGeometry;
  state.payload = { areas, dashboard, sources, districtRisk, talukRisk, hotspotRisk, alerts };
  references.archiveSelect.innerHTML = [
    `<option value="latest">Latest run</option>`,
    ...(archiveIndex.runs ?? [])
      .slice(0, 20)
      .map(
        (run) =>
          `<option value="${escapeAttr(run.path)}">${escapeHtml(
            `${new Date(run.generated_at).toLocaleString("en-IN", {
              dateStyle: "medium",
              timeStyle: "short",
              timeZone: "Asia/Kolkata"
            })} - ${run.headline_level}`
          )}</option>`
      )
  ].join("");
  renderAll();
}

async function loadArchive(pathPrefix) {
  if (pathPrefix === "latest") {
    return loadPayload();
  }

  const [dashboard, sources, districtRisk, talukRisk, hotspotRisk, alerts] = await Promise.all([
    fetchJson(`${pathPrefix}/dashboard.json`),
    fetchJson(`${pathPrefix}/sources.json`),
    fetchJson(`${pathPrefix}/district-risk.json`),
    fetchJson(`${pathPrefix}/taluk-risk.json`, { generated_at: null, taluks: [] }),
    fetchJson(`${pathPrefix}/hotspot-risk.json`),
    fetchJson(`${pathPrefix}/alerts.json`)
  ]);

  state.payload = {
    areas: state.payload.areas,
    dashboard,
    sources,
    districtRisk,
    talukRisk,
    hotspotRisk,
    alerts
  };
  renderAll();
}

references.timeframeToggle.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-horizon]");
  if (!button) {
    return;
  }
  state.horizon = button.dataset.horizon;
  references.timeframeToggle.querySelectorAll("button").forEach((candidate) => {
    candidate.classList.toggle("active", candidate === button);
  });
  renderAll();
});

references.dialogClose.addEventListener("click", () => references.dialog.close());
references.dialog.addEventListener("click", (event) => {
  if (event.target === references.dialog) {
    references.dialog.close();
  }
});

references.archiveSelect.addEventListener("change", (event) => {
  loadArchive(event.target.value).catch((error) => {
    references.headlineText.textContent = `Unable to load archive run: ${error.message}`;
  });
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("./sw.js?v=20260401-map").then((registration) => {
      registration.update().catch(() => {});
    }).catch(() => {});
  });
}

loadPayload().catch((error) => {
  references.headlineText.textContent = `Unable to load dashboard data: ${error.message}`;
});
