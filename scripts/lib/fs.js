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
