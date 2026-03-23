const state = {
  horizon: "now",
  mapScope: "district",
  payload: null,
  archiveIndex: null,
  districtGeometry: null,
  talukGeometry: null
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
  mapOverlay: document.querySelector("#map-overlay"),
  alertsList: document.querySelector("#alerts-list"),
  districtGrid: document.querySelector("#district-grid"),
  talukGrid: document.querySelector("#taluk-grid"),
  hotspotGrid: document.querySelector("#hotspot-grid"),
  sourceGrid: document.querySelector("#source-grid"),
  dialog: document.querySelector("#evidence-dialog"),
  dialogContent: document.querySelector("#dialog-content"),
  timeframeToggle: document.querySelector("#timeframe-toggle"),
  mapScopeToggle: document.querySelector("#map-scope-toggle"),
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

function levelPill(level) {
  return `<span class="level-pill" style="background:${levelColors[level] ?? "var(--accent)"}">${level}</span>`;
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

function openEvidence(title, body) {
  references.dialogContent.innerHTML = `<h2>${title}</h2>${body}`;
  references.dialog.showModal();
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
    coordinates.length === 2 &&
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
    const point = project(hotspot.location.lon, hotspot.location.lat);
    return {
      left: (point.x / mapViewBox.width) * 100,
      top: (point.y / mapViewBox.height) * 100
    };
  }

  const districtCentroid = projectedCentroids[hotspot.district_id];
  const districtAnchor = districtAnchorsById[hotspot.district_id] ?? { x: 50, y: 50 };
  const dx = ((hotspot.anchor.x - districtAnchor.x) / 100) * mapViewBox.width * 0.42;
  const dy = ((hotspot.anchor.y - districtAnchor.y) / 100) * mapViewBox.height * 0.42;
  return {
    left: ((districtCentroid.x + dx) / mapViewBox.width) * 100,
    top: ((districtCentroid.y + dy) / mapViewBox.height) * 100
  };
}

function getTalukIdFromFeature(feature, talukLookup) {
  const districtName =
    feature?.properties?.DISTRICT ??
    feature?.properties?.district ??
    feature?.properties?.DIST_NAME ??
    "";
  const talukName =
    feature?.properties?.TALUK ??
    feature?.properties?.taluk ??
    feature?.properties?.name ??
    "";
  const districtId = districtNameLookup[normalizeBoundaryName(districtName)] ?? null;
  if (!districtId) {
    return null;
  }
  return talukLookup[`${districtId}--${normalizeBoundaryName(talukName)}`] ?? null;
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
          <ul class="evidence-list">${item.drivers.map((driver) => `<li>${driver}</li>`).join("")}</ul>
          <h3>Source evidence</h3>
          <ul class="evidence-list">
            ${item.source_refs
              .map(
                (source) =>
                  `<li><strong>${source.source_id}</strong>: ${source.detail} (${source.status}, freshness ${source.freshness_minutes ?? "n/a"} min)</li>`
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
  references.headlineText.textContent = state.payload.dashboard.headline_message;
  references.generatedChip.textContent = `Updated ${formatTime(state.payload.dashboard.generated_at)}`;
  references.modeChip.textContent = `${state.payload.dashboard.mode} mode`;
  references.reviewCount.textContent = String(state.payload.dashboard.severe_pending_count);

  references.headlineCard.innerHTML = topAlert
    ? `
      ${levelPill(topAlert.level)}
      <h3>${topAlert.name}</h3>
      <p>${topAlert.message_en}</p>
      <div class="meta">
        <span>${horizonLabel()}</span>
        <span>Confidence ${(topAlert.confidence * 100).toFixed(0)}%</span>
        <span>${topAlert.review_state.replaceAll("_", " ")}</span>
      </div>
    `
    : `
      ${levelPill("Normal")}
      <h3>Routine monitoring</h3>
      <p>No active Watch-or-higher alerts. Continue observation and source-health checks.</p>
      <div class="meta">
        <span>${horizonLabel()}</span>
        <span>Confidence ${(state.payload.sources.sources.length ? 0.7 * 100 : 0).toFixed(0)}%</span>
      </div>
    `;
}

function renderMap() {
  const { areas, districtRisk, talukRisk, hotspotRisk } = state.payload;
  const districtById = Object.fromEntries(districtRisk.districts.map((item) => [item.area_id, item]));
  const talukById = Object.fromEntries(talukRisk.taluks.map((item) => [item.area_id, item]));
  const hotspotById = Object.fromEntries(hotspotRisk.hotspots.map((item) => [item.area_id, item]));
  const talukLookup = Object.fromEntries(
    (areas.taluks ?? []).map((taluk) => [`${taluk.district_id}--${normalizeBoundaryName(taluk.name)}`, taluk.id])
  );
  const showTaluks =
    state.mapScope === "taluk" &&
    state.talukGeometry?.features?.length &&
    talukRisk.taluks.length > 0;

  if (!state.districtGeometry?.features?.length) {
    references.districtLayer.innerHTML = "";
    references.hotspotFootprintLayer.innerHTML = "";
    references.districtLabelLayer.innerHTML = "";
    references.mapOverlay.innerHTML = [
      ...areas.districts.map((district) => {
        const item = districtById[district.id];
        const level = item?.level ?? "Normal";
        return `
          <button
            class="map-point district"
            data-area-id="${district.id}"
            data-area-type="district"
            style="left:${district.anchor.x}%; top:${district.anchor.y}%; background:${levelColors[level]}"
            title="${district.name}"
          ></button>
          <span class="map-label" style="left:${district.anchor.x}%; top:${district.anchor.y}%">${district.name}</span>
        `;
      })
    ].join("");
    bindMapInteractions();
    return;
  }

  const visibleFeatures = showTaluks
    ? state.talukGeometry.features
        .map((feature) => ({ ...feature, taluk_id: getTalukIdFromFeature(feature, talukLookup) }))
        .filter((feature) => feature.taluk_id && talukById[feature.taluk_id])
    : state.districtGeometry.features
        .map((feature) => ({ ...feature, district_id: getDistrictIdFromFeature(feature) }))
        .filter((feature) => feature.district_id && districtById[feature.district_id]);

  const bounds = geometryCollectionBounds(visibleFeatures);
  const project = buildProjector(bounds);
  const districtAnchorsById = Object.fromEntries(areas.districts.map((district) => [district.id, district.anchor]));
  const projectedCentroids = {};

  references.districtLayer.innerHTML = visibleFeatures
    .map((feature) => {
      const areaId = showTaluks ? feature.taluk_id : feature.district_id;
      const item = showTaluks ? talukById[feature.taluk_id] : districtById[feature.district_id];
      const pathData = geometryToPath(feature.geometry, project);
      const level = item?.level ?? "Normal";
      const centroid = centroidFromBounds(geometryBounds(feature.geometry), project);
      projectedCentroids[areaId] = centroid;
      return `
        <path
          class="district-shape"
          data-area-id="${areaId}"
          data-area-type="${showTaluks ? "taluk" : "district"}"
          d="${pathData}"
          fill="${levelColors[level] ?? "var(--normal)"}"
          title="${item?.name ?? areaId}"
        ></path>
      `;
    })
    .join("");

  references.districtLabelLayer.innerHTML = visibleFeatures
    .map((feature) => {
      const areaId = showTaluks ? feature.taluk_id : feature.district_id;
      const centroid = projectedCentroids[areaId];
      const item = showTaluks ? talukById[feature.taluk_id] : districtById[feature.district_id];
      return `
        <text class="district-label" x="${centroid.x.toFixed(1)}" y="${(centroid.y + 4).toFixed(1)}">
          ${item?.name ?? feature.district_id}
        </text>
      `;
    })
    .join("");

  references.hotspotFootprintLayer.innerHTML = areas.hotspots
    .filter((hotspot) => hotspot.footprint?.geometry)
    .map((hotspot) => {
      const item = hotspotById[hotspot.id];
      const level = item?.level ?? "Normal";
      return `
        <path
          class="hotspot-footprint"
          data-area-id="${hotspot.id}"
          data-area-type="hotspot"
          d="${featureToPath(hotspot.footprint, project)}"
          fill="${levelColors[level] ?? "var(--normal)"}"
        ></path>
      `;
    })
    .join("");

  references.mapOverlay.innerHTML = areas.hotspots
    .map((hotspot) => {
      const item = hotspotById[hotspot.id];
      const level = item?.level ?? "Normal";
      const position = hotspotPosition(hotspot, projectedCentroids, districtAnchorsById, project);
      return `
        <button
          class="map-point hotspot"
          data-area-id="${hotspot.id}"
          data-area-type="hotspot"
          style="left:${position.left}%; top:${position.top}%; background:${levelColors[level]}"
          title="${hotspot.name}"
        ></button>
      `;
    })
    .join("");

  bindMapInteractions();
}

function renderAlerts() {
  references.alertsList.innerHTML = sortByHorizon(state.payload.alerts.alerts)
    .map(
      (alert) => `
        <article class="alert-row" data-alert-id="${alert.alert_id}">
          <div>
            ${levelPill(alert.level)}
            <h3>${alert.name}</h3>
            <p>${alert.message_en}</p>
          </div>
          <div class="meta">
            <span>${horizonLabel()}</span>
            <span>Score ${horizonAdjustedScore(alert)}</span>
            <span>${alert.review_state.replaceAll("_", " ")}</span>
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
          <p>${alert.message_en}</p>
          <h3>Drivers</h3>
          <ul class="evidence-list">${alert.drivers.map((driver) => `<li>${driver}</li>`).join("")}</ul>
          <h3>Recommended actions</h3>
          <ul class="actions-list">${alert.recommended_actions.map((action) => `<li>${action}</li>`).join("")}</ul>
        `
      );
    });
  });
}

function renderRiskCards(target, items, suffix = "") {
  target.innerHTML = sortByHorizon(items)
    .map(
      (item) => `
        <article class="risk-card" data-id="${item.area_id}">
          ${levelPill(item.level)}
          <h3>${item.name}</h3>
          <div class="score">${horizonAdjustedScore(item)}</div>
          <p>${item.drivers[0] ?? "No active drivers beyond baseline susceptibility."}</p>
          <div class="meta">
            <span>Confidence ${(item.confidence * 100).toFixed(0)}%</span>
            <span>${suffix ? suffix : item.region ?? item.district_name ?? item.district_id ?? ""}</span>
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
  "indiawris-rainfall": {
    description: "Ground rain gauge readings across Kerala",
    method: "JSON API via Cloudflare Proxy → India-WRIS",
    cadence: "Every 3 hrs",
    impact: "No ground-truth rainfall. Satellite-only estimates (NASA IMERG) used instead."
  },
  "indiawris-river-level": {
    description: "River water level from CWC gauge stations",
    method: "JSON API via Cloudflare Proxy → India-WRIS",
    cadence: "Every 3 hrs",
    impact: "No river level context. CWC flood forecasting used as fallback."
  },
  "ksdma-reservoirs": {
    description: "Kerala dam reservoir storage levels",
    method: "HTML scraper via Cloudflare Proxy → KSDMA",
    cadence: "Every 1 hr",
    impact: "No reservoir data. Dam-related risk modifiers inactive."
  },
  "ksdma-dam-management": {
    description: "Dam spillway release bulletins",
    method: "HTML scraper via Cloudflare Proxy → KSDMA",
    cadence: "Every 1 hr",
    impact: "No spillway alerts. Downstream consequence modifiers inactive."
  },
  "cwc-ffs": {
    description: "Central Water Commission river flood status",
    method: "HTML scraper (direct fetch)",
    cadence: "Every 1 hr",
    impact: "No river flood warnings. River-stage scoring relies on WRIS water level."
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
  if (source.status === "offline") {
    return "Unavailable in this run. Current scores are being generated without this source.";
  }
  if (source.status === "stale") {
    if (source.category === "official-warning") {
      return "Older event-driven alert data. It may describe the last valid warning, not a fresh new one.";
    }
    return "Older than the normal freshness window. Use with caution.";
  }
  if (source.status === "degraded") {
    return "Partially usable. Some fields or mappings may be incomplete in this run.";
  }
  return "Current for this run.";
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

function openSourceDetails(source) {
  const meta = SOURCE_META[source.source_id] ?? {};
  const freshLabel = formatFreshness(source.freshness_minutes);
  const cadenceLabel = meta.cadence ?? "Unknown";
  const fetchNote = source.notes || source.summary?.excerpt || "None";
  const parserFailed = source.parser_status !== "ok";

  openEvidence(
    source.name,
    `
      <p class="source-detail-desc">${meta.description ?? source.name}</p>
      <div class="source-detail-grid">
        <div class="source-detail-row">
          <span class="source-detail-label">Status</span>
          <span class="status-${source.status} source-detail-value">${source.status.toUpperCase()}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Last updated</span>
          <span class="source-detail-value">${freshLabel}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Expected cadence</span>
          <span class="source-detail-value">${cadenceLabel}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Collection method</span>
          <span class="source-detail-value">${meta.method ?? "Unknown"}</span>
        </div>
        <div class="source-detail-row">
          <span class="source-detail-label">Parser</span>
          <span class="source-detail-value ${parserFailed ? "status-offline" : "status-ok"}">${source.parser_status}</span>
        </div>
      </div>
      ${fetchNote !== "None" ? `
        <h3>Fetch Notes</h3>
        <p class="source-detail-fetch-note">${fetchNote}</p>
      ` : ""}
      ${source.status === "offline" || source.status === "degraded" ? `
        <h3>Impact</h3>
        <p class="source-detail-impact">${meta.impact ?? sourceStatusMessage(source)}</p>
      ` : ""}
    `
  );
}

function renderSources() {
  references.sourceGrid.innerHTML = state.payload.sources.sources
    .map(
      (source) => {
        const meta = SOURCE_META[source.source_id] ?? {};
        const freshLabel = formatFreshness(source.freshness_minutes);
        return `
          <article class="source-card" data-source-id="${source.source_id}">
            <button class="source-info-btn" title="View details" type="button">i</button>
            <div class="label">${source.owner}</div>
            <h3>${source.name}</h3>
            <p class="source-desc">${meta.description ?? ""}</p>
            <div class="score status-${source.status}">${source.status}</div>
            <div class="meta">
              <span>Updated ${freshLabel}</span>
              <span>Parser ${source.parser_status}</span>
            </div>
            <p class="source-status-note status-${source.status}">${sourceStatusMessage(source)}</p>
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
  const [areas, dashboard, sources, districtRisk, talukRisk, hotspotRisk, alerts, archiveIndex, districtGeometry, talukGeometry] = await Promise.all([
    fetchJson("./data/static/areas.json"),
    fetchJson(`./data/latest/dashboard.json?${fresh}`),
    fetchJson(`./data/latest/sources.json?${fresh}`),
    fetchJson(`./data/latest/district-risk.json?${fresh}`),
    fetchJson(`./data/latest/taluk-risk.json?${fresh}`, { generated_at: null, taluks: [] }),
    fetchJson(`./data/latest/hotspot-risk.json?${fresh}`),
    fetchJson(`./data/latest/alerts.json?${fresh}`),
    fetchJson(`./data/latest/archive-index.json?${fresh}`),
    fetchJson("./assets/kerala-districts.geojson"),
    fetchJson("./assets/kerala-taluks.geojson")
  ]);

  state.archiveIndex = archiveIndex;
  state.districtGeometry = districtGeometry;
  state.talukGeometry = talukGeometry;
  state.payload = { areas, dashboard, sources, districtRisk, talukRisk, hotspotRisk, alerts };
  references.archiveSelect.innerHTML = [
    `<option value="latest">Latest run</option>`,
    ...(archiveIndex.runs ?? [])
      .slice(0, 20)
      .map(
        (run) =>
          `<option value="${run.path}">${new Date(run.generated_at).toLocaleString("en-IN", {
            dateStyle: "medium",
            timeStyle: "short",
            timeZone: "Asia/Kolkata"
          })} - ${run.headline_level}</option>`
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

references.mapScopeToggle.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-scope]");
  if (!button) {
    return;
  }
  state.mapScope = button.dataset.scope;
  references.mapScopeToggle.querySelectorAll("button").forEach((candidate) => {
    candidate.classList.toggle("active", candidate === button);
  });
  renderMap();
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
    navigator.serviceWorker.register("./sw.js").then((registration) => {
      registration.update().catch(() => {});
    }).catch(() => {});
  });
}

loadPayload().catch((error) => {
  references.headlineText.textContent = `Unable to load dashboard data: ${error.message}`;
});
