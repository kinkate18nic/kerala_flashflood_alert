import { cp, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";

export async function ensureDir(targetDir) {
  await mkdir(targetDir, { recursive: true });
}

export async function readJson(filePath, fallback = null) {
  try {
    const content = await readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch (error) {
    if (fallback !== null) {
      return fallback;
    }
    throw error;
  }
}

export async function writeJson(filePath, value) {
  await ensureDir(path.dirname(filePath));
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function stripGeneratedAt(value) {
  if (Array.isArray(value)) {
    return value.map(stripGeneratedAt);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => key !== "generated_at")
        .map(([key, nestedValue]) => [key, stripGeneratedAt(nestedValue)])
    );
  }
  return value;
}

export async function writeStableGeneratedJson(filePath, value) {
  await ensureDir(path.dirname(filePath));

  let existingText = null;
  let existingJson = null;
  try {
    existingText = await readFile(filePath, "utf8");
    existingJson = JSON.parse(existingText);
  } catch {
    existingText = null;
    existingJson = null;
  }

  let nextValue = value;
  if (
    existingJson &&
    JSON.stringify(stripGeneratedAt(existingJson)) === JSON.stringify(stripGeneratedAt(value)) &&
    existingJson.generated_at
  ) {
    nextValue = {
      ...value,
      generated_at: existingJson.generated_at
    };
  }

  const nextText = `${JSON.stringify(nextValue, null, 2)}\n`;
  if (existingText === nextText) {
    return false;
  }

  await writeFile(filePath, nextText, "utf8");
  return true;
}

export async function writeText(filePath, value) {
  await ensureDir(path.dirname(filePath));
  await writeFile(filePath, value, "utf8");
}

export async function pathExists(filePath) {
  try {
    await stat(filePath);
    return true;
  } catch {
    return false;
  }
}

export async function copyTree(sourceDir, targetDir) {
  await ensureDir(targetDir);
  await cp(sourceDir, targetDir, { recursive: true });
}
