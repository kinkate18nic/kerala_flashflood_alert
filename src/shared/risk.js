export const alertLevels = [
  { id: "Normal", minScore: 0, color: "#3a544b" },
  { id: "Watch", minScore: 35, color: "#a87d14" },
  { id: "Alert", minScore: 58, color: "#b4541e" },
  { id: "Severe - review required", minScore: 78, color: "#9d2a17" },
  { id: "Reviewed severe alert", minScore: 78, color: "#6d1a10" }
];

export const severityKeywords = [
  { pattern: /\bred\b/i, value: 1 },
  { pattern: /\borange\b/i, value: 0.72 },
  { pattern: /\byellow\b/i, value: 0.42 },
  { pattern: /\bmoderate\b/i, value: 0.45 },
  { pattern: /\bwatch\b/i, value: 0.35 },
  { pattern: /\balert\b/i, value: 0.6 },
  { pattern: /\bwarning\b/i, value: 0.7 }
];

export function scoreToLevel(score) {
  if (score >= 78) {
    return "Severe - review required";
  }
  if (score >= 58) {
    return "Alert";
  }
  if (score >= 35) {
    return "Watch";
  }
  return "Normal";
}
