import { isCodexBrowser } from "./runtime-environment.js";

const LOCAL_PREVIEW_HOSTNAMES = new Set(["localhost", "127.0.0.1", "::1", "[::1]", "terminal.local"]);

function currentUserAgent() {
  return globalThis.navigator?.userAgent;
}

function currentHostname() {
  return globalThis.window?.location?.hostname;
}

function isLocalPreviewHostname(hostname) {
  const normalized = typeof hostname === "string" ? hostname.toLowerCase() : "";
  return LOCAL_PREVIEW_HOSTNAMES.has(normalized) || normalized.endsWith(".localhost");
}

export function canUseDashboardAsk({ mode, userAgent = currentUserAgent(), hostname = currentHostname() }) {
  return mode === "view" && !isCodexBrowser(userAgent) && !isLocalPreviewHostname(hostname);
}

function sitesProjectReference(title, projectId) {
  if (typeof projectId !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_-]*$/u.test(projectId)) return null;
  // The public handoff accepts canonical Site references; Web applies composer escaping.
  const escapedTitle = String(title)
    .replace(/\r\n?|\n/g, " ")
    .replaceAll("\\", "\\\\")
    .replaceAll("[", "\\[")
    .replaceAll("](", "]\\(")
    .replaceAll("]", "\\]");
  return `[${escapedTitle}](sites-project://${encodeURIComponent(projectId).replaceAll(")", "\\)")})`;
}

export function dashboardAskPrompt({
  question,
  dashboardTitle,
  dashboardUrl,
  dashboardProjectId,
  dashboardProjectRoot,
  componentKind,
  componentTitle,
  selectedContext,
  selectedContextLabel = "Selected context",
}) {
  const trimmedQuestion = question.trim();
  if (!trimmedQuestion) return null;
  const siteReference = sitesProjectReference(dashboardTitle, dashboardProjectId);

  return [
    siteReference ? `Answer this question about ${siteReference}:` : `Dashboard: ${dashboardTitle}`,
    !siteReference && /^https?:/u.test(dashboardUrl ?? "") ? `Dashboard URL: ${dashboardUrl}` : null,
    dashboardProjectRoot ? `Dashboard project directory: ${dashboardProjectRoot}` : null,
    componentTitle ? `${componentKind === "chart" ? "Chart" : "Component"}: ${componentTitle}` : null,
    `${selectedContextLabel}: ${selectedContext}`,
    `Question: ${trimmedQuestion}`,
  ]
    .filter((entry) => entry !== null)
    .join("\n");
}
