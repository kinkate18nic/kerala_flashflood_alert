export function clamp(value, min = 0, max = 1) {
  return Math.min(max, Math.max(min, value));
}

export function buildDistrictTerrainLookup(districts, terrainStats) {
  const manualWeight = terrainStats?.normalization?.manual_weight ?? 0.4;
  const demWeight = terrainStats?.normalization?.dem_weight ?? 0.6;
  const demReferenceMax = terrainStats?.normalization?.dem_reference_max ?? 100;
  const demLookup = Object.fromEntries(
    (terrainStats?.districts ?? []).map((entry) => [entry.district_id, entry])
  );

  return Object.fromEntries(
    districts.map((district) => {
      const dem = demLookup[district.id];
      const demNormalized = dem ? clamp((dem.terrain_score_raw ?? 0) / demReferenceMax) : null;
      const blended = demNormalized === null
        ? district.susceptibility
        : clamp(district.susceptibility * manualWeight + demNormalized * demWeight);

      return [
        district.id,
        {
          value: blended,
          manual_baseline: district.susceptibility,
          dem_normalized: demNormalized,
          dem
        }
      ];
    })
  );
}
