import { fromArrayBuffer } from "geotiff";
import { unzipSync } from "fflate";
import { fetchText } from "./http.js";
import { districts } from "../../src/shared/areas.js";

const districtLocations = {
  kasaragod: { lat: 12.4996, lon: 74.9869 },
  kannur: { lat: 11.8745, lon: 75.3704 },
  wayanad: { lat: 11.6854, lon: 76.132 },
  kozhikode: { lat: 11.2588, lon: 75.7804 },
  malappuram: { lat: 11.073, lon: 76.074 },
  palakkad: { lat: 10.7867, lon: 76.6548 },
  thrissur: { lat: 10.5276, lon: 76.2144 },
  ernakulam: { lat: 9.9816, lon: 76.2999 },
  idukki: { lat: 9.8494, lon: 76.972 },
  kottayam: { lat: 9.5916, lon: 76.5222 },
  alappuzha: { lat: 9.4981, lon: 76.3388 },
  pathanamthitta: { lat: 9.2648, lon: 76.787 },
  kollam: { lat: 8.8932, lon: 76.6141 },
  thiruvananthapuram: { lat: 8.5241, lon: 76.9366 }
};

function pad2(value) {
  return String(value).padStart(2, "0");
}

function currentYearMonths() {
  const now = new Date();
  const current = `${now.getUTCFullYear()}${pad2(now.getUTCMonth() + 1)}`;
  const previousDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));
  const previous = `${previousDate.getUTCFullYear()}${pad2(previousDate.getUTCMonth() + 1)}`;
  return [current, previous];
}

function directoryUrls(source) {
  if (process.env[source.data_url_env]) {
    return [process.env[source.data_url_env]];
  }

  return currentYearMonths().map((yearMonth) => `${source.directory_url}${yearMonth}/`);
}

function parseListing(html, baseUrl) {
  return [...html.matchAll(/href="([^"]+)"/gi)]
    .map((match) => match[1])
    .filter((href) => /\.(tif|zip)$/i.test(href))
    .map((href) => ({
      href,
      url: href.startsWith("http") ? href : new URL(href, baseUrl).toString(),
      fileName: href.split("/").pop()
    }))
    .map((file) => ({
      ...file,
      issuedAt: parseImergTimestamp(file.fileName)
    }))
    .filter((file) => file.issuedAt)
    .sort((left, right) => right.issuedAt.localeCompare(left.issuedAt));
}

function parseImergTimestamp(fileName) {
  const match = fileName.match(/3IMERG\.(\d{8})-S(\d{2})(\d{2})(\d{2})/i);
  if (!match) {
    return null;
  }

  const [, datePart, hour, minute, second] = match;
  return `${datePart.slice(0, 4)}-${datePart.slice(4, 6)}-${datePart.slice(6, 8)}T${hour}:${minute}:${second}.000Z`;
}

function selectLatest(files, pattern, count) {
  return files.filter((file) => pattern.test(file.fileName)).slice(0, count);
}

async function fetchArrayBuffer(url, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 45000);
  try {
    const response = await fetch(url, {
      headers: {
        authorization: `Bearer ${token}`,
        "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/)"
      },
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`IMERG download failed: ${response.status} ${url}`);
    }
    return response.arrayBuffer();
  } finally {
    clearTimeout(timeout);
  }
}

function extractTiffBuffer(buffer, url) {
  if (!url.endsWith(".zip")) {
    return buffer;
  }

  const entries = unzipSync(new Uint8Array(buffer));
  const tiffName = Object.keys(entries).find((entry) => entry.toLowerCase().endsWith(".tif"));
  if (!tiffName) {
    throw new Error(`No GeoTIFF found in IMERG zip ${url}`);
  }

  const tiffData = entries[tiffName];
  return tiffData.buffer.slice(tiffData.byteOffset, tiffData.byteOffset + tiffData.byteLength);
}

function sampleRasterValue(data, image, latitude, longitude) {
  const [originX, originY] = image.getOrigin();
  const [resolutionX, resolutionY] = image.getResolution();
  const width = image.getWidth();
  const height = image.getHeight();
  const x = Math.floor((longitude - originX) / resolutionX);
  const y = Math.floor((latitude - originY) / resolutionY);

  if (x < 0 || y < 0 || x >= width || y >= height) {
    return 0;
  }

  const value = data[y * width + x];
  if (!Number.isFinite(value) || value < 0) {
    return 0;
  }

  return value / 10;
}

async function sampleFileAtDistricts(file, token) {
  const arrayBuffer = await fetchArrayBuffer(file.url, token);
  const tiffBuffer = extractTiffBuffer(arrayBuffer, file.url);
  const tiff = await fromArrayBuffer(tiffBuffer);
  const image = await tiff.getImage();
  const raster = await image.readRasters({ interleave: true });

  return Object.fromEntries(
    districts.map((district) => {
      const location = districtLocations[district.id];
      const value = location ? sampleRasterValue(raster, image, location.lat, location.lon) : 0;
      return [district.id, value];
    })
  );
}

function sumByDistrict(valuesList) {
  const totals = Object.fromEntries(districts.map((district) => [district.id, 0]));
  for (const values of valuesList) {
    for (const [districtId, value] of Object.entries(values)) {
      totals[districtId] += value ?? 0;
    }
  }
  return totals;
}

export async function fetchNasaImergPayload(source) {
  const token = process.env.NASA_EARTHDATA_BEARER;
  if (!token) {
    return {
      ok: false,
      status: 401,
      text: "",
      note: "NASA_EARTHDATA_BEARER not configured."
    };
  }

  let files = [];
  for (const url of directoryUrls(source)) {
    const listing = await fetchText(url, {
      timeoutMs: 30000,
      headers: { authorization: `Bearer ${token}` }
    });
    if (!listing.ok) {
      continue;
    }
    files = parseListing(listing.text, url);
    if (files.length) {
      break;
    }
  }

  if (!files.length) {
    return {
      ok: false,
      status: 404,
      text: "",
      note: "No IMERG GIS files found in the configured directory."
    };
  }

  const halfHourFiles = selectLatest(files, /\.30min\.(tif|zip)$/i, 2);
  const threeHourFiles = selectLatest(files, /\.3hr\.(tif|zip)$/i, 2);
  const dailyFiles = selectLatest(files, /\.(1day|1d)\.(tif|zip)$/i, 7);

  if (!halfHourFiles.length || !threeHourFiles.length || !dailyFiles.length) {
    return {
      ok: false,
      status: 424,
      text: "",
      note: "IMERG listing did not contain the expected 30min/3hr/1day GIS files."
    };
  }

  const [halfHourSamples, threeHourSamples, dailySamples] = await Promise.all([
    Promise.all(halfHourFiles.map((file) => sampleFileAtDistricts(file, token))),
    Promise.all(threeHourFiles.map((file) => sampleFileAtDistricts(file, token))),
    Promise.all(dailyFiles.map((file) => sampleFileAtDistricts(file, token)))
  ]);

  const oneHour = sumByDistrict(halfHourSamples);
  const threeHour = threeHourSamples[0];
  const sixHour = sumByDistrict(threeHourSamples);
  const oneDay = dailySamples[0];
  const threeDay = sumByDistrict(dailySamples.slice(0, 3));
  const sevenDay = sumByDistrict(dailySamples.slice(0, 7));

  const payload = {
    issued_at: halfHourFiles[0].issuedAt,
    source_files: {
      half_hour: halfHourFiles.map((file) => file.url),
      three_hour: threeHourFiles.map((file) => file.url),
      daily: dailyFiles.map((file) => file.url)
    },
    districts: districts.map((district) => ({
      district_id: district.id,
      rain_1h_mm: Number(oneHour[district.id].toFixed(1)),
      rain_3h_mm: Number((threeHour[district.id] ?? 0).toFixed(1)),
      rain_6h_mm: Number((sixHour[district.id] ?? 0).toFixed(1)),
      rain_24h_mm: Number((oneDay[district.id] ?? 0).toFixed(1)),
      rain_3d_mm: Number((threeDay[district.id] ?? 0).toFixed(1)),
      rain_7d_mm: Number((sevenDay[district.id] ?? 0).toFixed(1)),
      source: "nasa-imerg-pps"
    }))
  };

  return {
    ok: true,
    status: 200,
    text: JSON.stringify(payload),
    note: `IMERG sampled from ${halfHourFiles[0].fileName}`
  };
}
