function toRadians(value) {
  return (value * Math.PI) / 180;
}

function categoryShape(category) {
  switch (category) {
    case "steep_catchment":
      return { major: 1.2, minor: 0.68, rotation: 28 };
    case "dam_downstream":
      return { major: 1.6, minor: 0.62, rotation: 18 };
    case "river_floodplain":
      return { major: 1.55, minor: 0.78, rotation: 10 };
    case "river_confluence":
      return { major: 1.2, minor: 0.95, rotation: 0 };
    case "low_lying_basin":
      return { major: 1.35, minor: 1.08, rotation: -10 };
    case "urban_flood_pocket":
      return { major: 0.82, minor: 0.82, rotation: 0 };
    default:
      return { major: 1, minor: 1, rotation: 0 };
  }
}

function terrainScale(terrainValue = 0.5) {
  return 0.82 + terrainValue * 0.45;
}

function translateKilometers(lon, lat, xKm, yKm) {
  const latRadians = toRadians(lat);
  const deltaLat = yKm / 110.574;
  const deltaLon = xKm / (111.320 * Math.cos(latRadians || Number.EPSILON));
  return [lon + deltaLon, lat + deltaLat];
}

function buildEllipseCoordinates(center, radiusKm, shape, terrainValue, points = 40) {
  const scale = terrainScale(terrainValue);
  const majorKm = radiusKm * shape.major * scale;
  const minorKm = radiusKm * shape.minor * scale;
  const rotation = toRadians(shape.rotation);
  const coordinates = [];

  for (let index = 0; index < points; index += 1) {
    const theta = (index / points) * Math.PI * 2;
    const localX = Math.cos(theta) * majorKm;
    const localY = Math.sin(theta) * minorKm;
    const rotatedX = localX * Math.cos(rotation) - localY * Math.sin(rotation);
    const rotatedY = localX * Math.sin(rotation) + localY * Math.cos(rotation);
    coordinates.push(translateKilometers(center.lon, center.lat, rotatedX, rotatedY));
  }

  coordinates.push(coordinates[0]);
  return coordinates;
}

export function buildHotspotFootprint(hotspot, terrainValue = 0.5) {
  if (!hotspot.location?.lat || !hotspot.location?.lon || !hotspot.buffer_km) {
    return null;
  }

  const shape = categoryShape(hotspot.category);
  return {
    type: "Feature",
    properties: {
      hotspot_id: hotspot.id,
      district_id: hotspot.district_id,
      name: hotspot.name,
      category: hotspot.category,
      buffer_km: hotspot.buffer_km,
      terrain_value: terrainValue
    },
    geometry: {
      type: "Polygon",
      coordinates: [buildEllipseCoordinates(hotspot.location, hotspot.buffer_km, shape, terrainValue)]
    }
  };
}
