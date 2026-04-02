import path from "node:path";
import { fileURLToPath } from "node:url";
import { readJson } from "./lib/fs.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const githubToken = String(process.env.GITHUB_TOKEN ?? "").trim();
const githubRepository = String(process.env.GITHUB_REPOSITORY ?? "").trim();

if (!githubToken || !githubRepository) {
  throw new Error("GITHUB_TOKEN and GITHUB_REPOSITORY are required.");
}

const [owner, repo] = githubRepository.split("/");
if (!owner || !repo) {
  throw new Error(`Invalid GITHUB_REPOSITORY value: ${githubRepository}`);
}

const opsState = await readJson(path.join(repoRoot, "data", "manual", "ops-state.json"), {
  refresh_paused: false
});

const desiredAction = opsState.refresh_paused ? "disable" : "enable";
const workflowFiles = ["refresh-fast-data.yml", "refresh-slow-data.yml", "refresh-data.yml"];

for (const workflowFile of workflowFiles) {
  const response = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowFile}/${desiredAction}`,
    {
      method: "PUT",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${githubToken}`,
        "User-Agent": "kerala-flash-flood-watch-refresh-control"
      }
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed to ${desiredAction} ${workflowFile}: ${response.status} ${await response.text()}`
    );
  }

  console.log(`${desiredAction === "disable" ? "Disabled" : "Enabled"} ${workflowFile}`);
}
