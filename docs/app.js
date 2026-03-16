const state = {
  horizon: "now",
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
  districtLabelLayer: document.querySelector("#district-label-layer"),
  mapOverlay: document.querySelector("#map-overlay"),
  alertsList: document.querySelector("#alerts-list"),
  districtGrid: document.querySelector("#district-grid"),
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
  thiruvananthapuram: "thiruvananthapuram",
  thrissur: "thrissur",
  wayanad: "wayanad"
};

function levelPill(level) {
  return `<span class="level-pill" style="background:${levelColors[level] ?? "var(--accent)"}">${level}</span>`;
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

function centroidFromBounds(bounds, project) {
  return project((bounds.minLon + bounds.maxLon) / 2, (bounds.minLat + bounds.maxLat) / 2);
}

function districtItemFromEvent(areaId, areaType) {
  const collection = areaType === "district" ? state.payload.districtRisk.districts : state.payload.hotspotRisk.hotspots;
  return collection.find((entry) => entry.area_id === areaId);
}

function bindMapInteractions() {
  document.querySelectorAll("[data-area-id][data-area-type]").forEach((element) => {
    element.addEventListener("click", () => {
      const item = districtItemFromEvent(element.dataset.areaId, element.dataset.areaType);
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
  const { areas, districtRisk, hotspotRisk } = state.payload;
  const districtById = Object.fromEntries(districtRisk.districts.map((item) => [item.area_id, item]));
  const hotspotById = Object.fromEntries(hotspotRisk.hotspots.map((item) => [item.area_id, item]));

  if (!state.districtGeometry?.features?.length) {
    references.districtLayer.innerHTML = "";
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

  const districtFeatures = state.districtGeometry.features
    .map((feature) => ({ ...feature, district_id: getDistrictIdFromFeature(feature) }))
    .filter((feature) => feature.district_id && districtById[feature.district_id]);

  const bounds = geometryCollectionBounds(districtFeatures);
  const project = buildProjector(bounds);
  const districtAnchorsById = Object.fromEntries(areas.districts.map((district) => [district.id, district.anchor]));
  const projectedCentroids = {};

  references.districtLayer.innerHTML = districtFeatures
    .map((feature) => {
      const item = districtById[feature.district_id];
      const pathData = geometryToPath(feature.geometry, project);
      const level = item?.level ?? "Normal";
      const centroid = centroidFromBounds(geometryBounds(feature.geometry), project);
      projectedCentroids[feature.district_id] = centroid;
      return `
        <path
          class="district-shape"
          data-area-id="${feature.district_id}"
          data-area-type="district"
          d="${pathData}"
          fill="${levelColors[level] ?? "var(--normal)"}"
          title="${item?.name ?? feature.district_id}"
        ></path>
      `;
    })
    .join("");

  references.districtLabelLayer.innerHTML = districtFeatures
    .map((feature) => {
      const centroid = projectedCentroids[feature.district_id];
      const item = districtById[feature.district_id];
      return `
        <text class="district-label" x="${centroid.x.toFixed(1)}" y="${(centroid.y + 4).toFixed(1)}">
          ${item?.name ?? feature.district_id}
        </text>
      `;
    })
    .join("");

  references.mapOverlay.innerHTML = areas.hotspots
    .map((hotspot) => {
      const item = hotspotById[hotspot.id];
      const level = item?.level ?? "Normal";
      const districtCentroid = projectedCentroids[hotspot.district_id];
      const districtAnchor = districtAnchorsById[hotspot.district_id] ?? { x: 50, y: 50 };
      const dx = ((hotspot.anchor.x - districtAnchor.x) / 100) * mapViewBox.width * 0.42;
      const dy = ((hotspot.anchor.y - districtAnchor.y) / 100) * mapViewBox.height * 0.42;
      const left = ((districtCentroid.x + dx) / mapViewBox.width) * 100;
      const top = ((districtCentroid.y + dy) / mapViewBox.height) * 100;
      return `
        <button
          class="map-point hotspot"
          data-area-id="${hotspot.id}"
          data-area-type="hotspot"
          style="left:${left}%; top:${top}%; background:${levelColors[level]}"
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
            <span>${suffix ? suffix : item.region ?? item.district_id ?? ""}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderSources() {
  references.sourceGrid.innerHTML = state.payload.sources.sources
    .map(
      (source) => `
        <article class="source-card">
          <div class="label">${source.owner}</div>
          <h3>${source.name}</h3>
          <div class="score status-${source.status}">${source.status}</div>
          <div class="meta">
            <span>Freshness ${source.freshness_minutes ?? "n/a"} min</span>
            <span>Parser ${source.parser_status}</span>
          </div>
          <p>${source.notes || source.summary.excerpt || "No parser notes."}</p>
        </article>
      `
    )
    .join("");
}

function renderAll() {
  renderHeadline();
  renderMap();
  renderAlerts();
  renderRiskCards(references.districtGrid, state.payload.districtRisk.districts);
  renderRiskCards(references.hotspotGrid, state.payload.hotspotRisk.hotspots, "Hotspot");
  renderSources();
}

async function loadPayload() {
  const fresh = `t=${Date.now()}`;
  const [areas, dashboard, sources, districtRisk, hotspotRisk, alerts, archiveIndex, districtGeometry] = await Promise.all([
    fetch("./data/static/areas.json").then((response) => response.json()),
    fetch(`./data/latest/dashboard.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`./data/latest/sources.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`./data/latest/district-risk.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`./data/latest/hotspot-risk.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`./data/latest/alerts.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`./data/latest/archive-index.json?${fresh}`, { cache: "no-store" }).then((response) => response.json()),
    fetch("./assets/kerala-districts.geojson").then((response) => response.json())
  ]);

  state.archiveIndex = archiveIndex;
  state.districtGeometry = districtGeometry;
  state.payload = { areas, dashboard, sources, districtRisk, hotspotRisk, alerts };
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

  const [dashboard, sources, districtRisk, hotspotRisk, alerts] = await Promise.all([
    fetch(`${pathPrefix}/dashboard.json`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`${pathPrefix}/sources.json`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`${pathPrefix}/district-risk.json`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`${pathPrefix}/hotspot-risk.json`, { cache: "no-store" }).then((response) => response.json()),
    fetch(`${pathPrefix}/alerts.json`, { cache: "no-store" }).then((response) => response.json())
  ]);

  state.payload = {
    areas: state.payload.areas,
    dashboard,
    sources,
    districtRisk,
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
    navigator.serviceWorker.register("./sw.js").then((registration) => {
      registration.update().catch(() => {});
    }).catch(() => {});
  });
}

loadPayload().catch((error) => {
  references.headlineText.textContent = `Unable to load dashboard data: ${error.message}`;
});
