import { districts } from "../../src/shared/areas.js";
import { fetchText } from "./http.js";

export const boundaryLayerSources = {
  state: {
    id: "kerala-state",
    url: "https://raw.githubusercontent.com/geohacker/kerala/master/geojsons/state.geojson"
  },
  district: {
    id: "kerala-district",
    url: "https://raw.githubusercontent.com/geohacker/kerala/master/geojsons/district.geojson"
  },
  taluk: {
    id: "kerala-taluk",
    url: "https://raw.githubusercontent.com/geohacker/kerala/master/geojsons/taluk.geojson"
  },
  village: {
    id: "kerala-village",
    url: "https://raw.githubusercontent.com/geohacker/kerala/master/geojsons/village.geojson"
  }
};

const layerCache = new Map();
const districtIdsByName = Object.fromEntries(
  districts.map((district) => [normalizeBoundaryName(district.name), district.id])
);

export function normalizeBoundaryName(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
}

export function districtIdFromBoundaryName(name) {
  return districtIdsByName[normalizeBoundaryName(name)] ?? null;
}

async function fetchGeoJson(url) {
  const response = await fetchText(url, { timeoutMs: 45000 });
  if (!response.ok) {
    throw new Error(`Boundary fetch failed: ${response.status} ${url}`);
  }

  return JSON.parse(response.text);
}

export async function loadBoundaryLayer(layer) {
  const source = boundaryLayerSources[layer];
  if (!source) {
    throw new Error(`Unknown boundary layer: ${layer}`);
  }

  if (!layerCache.has(layer)) {
    layerCache.set(layer, fetchGeoJson(source.url));
  }

  return layerCache.get(layer);
}

function updateBounds(bounds, longitude, latitude) {
  bounds.minLon = Math.min(bounds.minLon, longitude);
  bounds.minLat = Math.min(bounds.minLat, latitude);
  bounds.maxLon = Math.max(bounds.maxLon, longitude);
  bounds.maxLat = Math.max(bounds.maxLat, latitude);
}

function walkCoordinates(coordinates, visitor) {
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

  for (const entry of coordinates) {
    walkCoordinates(entry, visitor);
  }
}

export function geometryBounds(geometry) {
  const bounds = {
    minLon: Number.POSITIVE_INFINITY,
    minLat: Number.POSITIVE_INFINITY,
    maxLon: Number.NEGATIVE_INFINITY,
    maxLat: Number.NEGATIVE_INFINITY
  };

  if (!geometry?.coordinates) {
    return null;
  }

  walkCoordinates(geometry.coordinates, (longitude, latitude) => {
    updateBounds(bounds, longitude, latitude);
  });

  if (!Number.isFinite(bounds.minLon)) {
    return null;
  }

  return bounds;
}

export function geometryCentroid(geometry) {
  const bounds = geometryBounds(geometry);
  if (!bounds) {
    return null;
  }

  return {
    lon: Number(((bounds.minLon + bounds.maxLon) / 2).toFixed(5)),
    lat: Number(((bounds.minLat + bounds.maxLat) / 2).toFixed(5))
  };
}

function pointInRing(point, ring) {
  let inside = false;

  for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index, index += 1) {
    const [xi, yi] = ring[index];
    const [xj, yj] = ring[previous];
    const intersects =
      yi > point[1] !== yj > point[1] &&
      point[0] < ((xj - xi) * (point[1] - yi)) / ((yj - yi) || Number.EPSILON) + xi;
    if (intersects) {
      inside = !inside;
    }
  }

  return inside;
}

function pointInPolygon(point, polygon) {
  if (!polygon.length || !pointInRing(point, polygon[0])) {
    return false;
  }

  for (let index = 1; index < polygon.length; index += 1) {
    if (pointInRing(point, polygon[index])) {
      return false;
    }
  }

  return true;
}

export function pointInGeometry(point, geometry) {
  if (!geometry) {
    return false;
  }

  if (geometry.type === "Polygon") {
    return pointInPolygon(point, geometry.coordinates);
  }

  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates.some((polygon) => pointInPolygon(point, polygon));
  }

  return false;
}

export async function loadDistrictBoundaries() {
  const layer = await loadBoundaryLayer("district");
  return (layer.features ?? [])
    .map((feature) => {
      const rawName =
        feature.properties?.DISTRICT ??
        feature.properties?.district ??
        feature.properties?.name ??
        feature.properties?.NAME_2 ??
        null;
      const districtId = districtIdFromBoundaryName(rawName);

      if (!districtId) {
        return null;
      }

      return {
        district_id: districtId,
        name: rawName,
        geometry: feature.geometry,
        bbox: geometryBounds(feature.geometry),
        centroid: geometryCentroid(feature.geometry)
      };
    })
    .filter(Boolean);
}

export async function buildBoundaryMetadata() {
  const [stateLayer, districtLayer, talukLayer] = await Promise.all([
    loadBoundaryLayer("state"),
    loadBoundaryLayer("district"),
    loadBoundaryLayer("taluk")
  ]);
  const districtBoundaries = await loadDistrictBoundaries();

  return {
    sources: boundaryLayerSources,
    counts: {
      state: stateLayer.features?.length ?? 0,
      district: districtLayer.features?.length ?? 0,
      taluk: talukLayer.features?.length ?? 0
    },
    districts: districtBoundaries.map((district) => ({
      district_id: district.district_id,
      name: district.name,
      centroid: district.centroid,
      bbox: district.bbox
    }))
  };
}
