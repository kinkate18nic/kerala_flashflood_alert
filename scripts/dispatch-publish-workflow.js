const githubToken = String(process.env.GITHUB_TOKEN ?? "").trim();
const githubRepository = String(process.env.GITHUB_REPOSITORY ?? "").trim();
const workflowId = String(process.env.PUBLISH_WORKFLOW_ID ?? "publish-site.yml").trim();
const workflowRef = String(process.env.PUBLISH_WORKFLOW_REF ?? "main").trim();
const triggerSource = String(process.env.PUBLISH_TRIGGER_SOURCE ?? "unknown").trim();

if (!githubToken || !githubRepository) {
  throw new Error("GITHUB_TOKEN and GITHUB_REPOSITORY are required.");
}

const [owner, repo] = githubRepository.split("/");
if (!owner || !repo) {
  throw new Error(`Invalid GITHUB_REPOSITORY value: ${githubRepository}`);
}

const response = await fetch(
  `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowId}/dispatches`,
  {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${githubToken}`,
      "User-Agent": "kerala-flash-flood-watch-publish-dispatch"
    },
    body: JSON.stringify({
      ref: workflowRef,
      inputs: {
        trigger_source: triggerSource
      }
    })
  }
);

if (!response.ok) {
  throw new Error(
    `Failed to dispatch ${workflowId}: ${response.status} ${await response.text()}`
  );
}

console.log(`Dispatched ${workflowId} for ${workflowRef} (source: ${triggerSource}).`);
