import { unzipSync } from "fflate";
import { fromArrayBuffer } from "geotiff";
import { districts } from "../../src/shared/areas.js";
import { fetchText } from "./http.js";

const PPS_ORIGIN = "https://jsimpsonhttps.pps.eosdis.nasa.gov";

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

function previousMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() - 1, 1));
}

function buildListingUrls(now = new Date()) {
  const current = `${now.getUTCFullYear()}/${pad2(now.getUTCMonth() + 1)}`;
  const previous = previousMonth(now);
  const previousPath = `${previous.getUTCFullYear()}/${pad2(previous.getUTCMonth() + 1)}`;
  return [
    `${PPS_ORIGIN}/text/imerg/gis/early/`,
    `${PPS_ORIGIN}/text/imerg/gis/early/${current}/`,
    `${PPS_ORIGIN}/text/imerg/gis/early/${previousPath}/`
  ];
}

function getPpsCredentials() {
  const email = process.env.PPS_EMAIL;
  const password = process.env.PPS_PASSWORD || email;
  if (!email || !password) {
    return null;
  }
  return { email, password };
}

function basicAuthHeader(credentials) {
  const token = Buffer.from(`${credentials.email}:${credentials.password}`).toString("base64");
  return `Basic ${token}`;
}

function parseTimestamp(datePart, timePart) {
  return `${datePart.slice(0, 4)}-${datePart.slice(4, 6)}-${datePart.slice(6, 8)}T${timePart.slice(0, 2)}:${timePart.slice(2, 4)}:${timePart.slice(4, 6)}.000Z`;
}

export function parseImergTextListing(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /\.(tif|zip)$/i.test(line))
    .map((line) => {
      const fileName = line.split("/").pop();
      const match = fileName.match(
        /3IMERG\.(\d{8})-S(\d{6})-E(\d{6})\.(\d{4})\.V([0-9]{2}[A-Z])\.(30min|3hr|1day)\.(tif|zip)$/i
      );
      if (!match) {
        return null;
      }
      const [, datePart, startTime, endTime, slotCode, version, product, extension] = match;
      return {
        path: line,
        url: `${PPS_ORIGIN}${line}`,
        fileName,
        extension: extension.toLowerCase(),
        fileKey: fileName.replace(/\.(tif|zip)$/i, ""),
        product,
        slotCode,
        version,
        start_at: parseTimestamp(datePart, startTime),
        issuedAt: parseTimestamp(datePart, endTime)
      };
    })
    .filter(Boolean);
}

function dedupeByFileName(files) {
  const byStem = files.reduce((accumulator, file) => {
    const existing = accumulator[file.fileKey];
    if (!existing || (existing.extension !== "zip" && file.extension === "zip")) {
      accumulator[file.fileKey] = file;
    }
    return accumulator;
  }, {});
  return Object.values(byStem);
}

function sortNewestFirst(files) {
  return [...files].sort((left, right) => right.issuedAt.localeCompare(left.issuedAt));
}

export function selectImergWindows(files) {
  const latest30Min = sortNewestFirst(files.filter((file) => file.product === "30min")).slice(0, 2);
  const latest3Hr = sortNewestFirst(files.filter((file) => file.product === "3hr"));
  const latestDaily = sortNewestFirst(files.filter((file) => file.product === "1day"));

  const latestDailySlot = latestDaily[0]?.slotCode ?? null;
  const spacedDaily = latestDailySlot
    ? latestDaily.filter((file) => file.slotCode === latestDailySlot).slice(0, 7)
    : [];

  return {
    halfHour: latest30Min,
    threeHourLatest: latest3Hr.slice(0, 1),
    threeHourWindow: latest3Hr.slice(0, 2),
    dailyWindow: spacedDaily
  };
}

async function fetchListing(url, credentials) {
  return fetchText(url, {
    timeoutMs: 45000,
    headers: { authorization: basicAuthHeader(credentials) }
  });
}

async function fetchArrayBuffer(url, credentials, attempts = 3) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 60000);
    try {
      const response = await fetch(url, {
        headers: {
          authorization: basicAuthHeader(credentials),
          "user-agent": "KeralaFlashFloodWatch/0.1 (+https://github.com/)"
        },
        signal: controller.signal
      });
      if (!response.ok) {
        throw new Error(`IMERG download failed: ${response.status} ${url}`);
      }
      return await response.arrayBuffer();
    } catch (error) {
      if (attempt === attempts) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, attempt * 1500));
    } finally {
      clearTimeout(timeout);
    }
  }
}

function extractGeoTiffBuffer(arrayBuffer, extension) {
  if (extension !== "zip") {
    return arrayBuffer;
  }

  const entries = unzipSync(new Uint8Array(arrayBuffer));
  const tifName = Object.keys(entries).find((entry) => entry.toLowerCase().endsWith(".tif"));
  if (!tifName) {
    throw new Error("IMERG zip did not contain a GeoTIFF.");
  }

  const tif = entries[tifName];
  return tif.buffer.slice(tif.byteOffset, tif.byteOffset + tif.byteLength);
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

async function sampleFileAtDistricts(file, credentials) {
  const arrayBuffer = await fetchArrayBuffer(file.url, credentials);
  const tiffBuffer = extractGeoTiffBuffer(arrayBuffer, file.extension);
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

async function sampleFilesSequentially(files, credentials) {
  const samples = [];
  for (const file of files) {
    samples.push(await sampleFileAtDistricts(file, credentials));
  }
  return samples;
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

export async function fetchNasaImergPayload() {
  const credentials = getPpsCredentials();
  if (!credentials) {
    return {
      ok: false,
      status: 401,
      text: "",
      note: "PPS_EMAIL not configured."
    };
  }

  const listingResponses = await Promise.all(buildListingUrls().map((url) => fetchListing(url, credentials)));
  const allFiles = dedupeByFileName(
    listingResponses.filter((response) => response.ok).flatMap((response) => parseImergTextListing(response.text))
  );

  if (!allFiles.length) {
    return {
      ok: false,
      status: 404,
      text: "",
      note: "No IMERG GIS files found in the PPS text listings."
    };
  }

  const selection = selectImergWindows(allFiles);
  if (
    selection.halfHour.length < 2 ||
    selection.threeHourLatest.length < 1 ||
    selection.threeHourWindow.length < 2 ||
    selection.dailyWindow.length < 7
  ) {
    return {
      ok: false,
      status: 424,
      text: "",
      note: "IMERG listings did not provide enough 30min, 3hr, and 1day files for the current windows."
    };
  }

  const [halfHourSamples, threeHourLatestSamples, threeHourWindowSamples, dailySamples] = await Promise.all([
    sampleFilesSequentially(selection.halfHour, credentials),
    sampleFilesSequentially(selection.threeHourLatest, credentials),
    sampleFilesSequentially(selection.threeHourWindow, credentials),
    sampleFilesSequentially(selection.dailyWindow, credentials)
  ]);

  const oneHour = sumByDistrict(halfHourSamples);
  const threeHour = threeHourLatestSamples[0];
  const sixHour = sumByDistrict(threeHourWindowSamples);
  const oneDay = dailySamples[0];
  const threeDay = sumByDistrict(dailySamples.slice(0, 3));
  const sevenDay = sumByDistrict(dailySamples.slice(0, 7));

  const payload = {
    issued_at: selection.halfHour[0].issuedAt,
    source_files: {
      half_hour: selection.halfHour.map((file) => file.url),
      three_hour: selection.threeHourWindow.map((file) => file.url),
      daily: selection.dailyWindow.map((file) => file.url)
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
    note: `IMERG sampled from ${selection.halfHour[0].fileName}`
  };
}
