import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { dataAppActionRequest, printDataApp } from "../src/data-app-actions.js";

const templateRoot = new URL("../", import.meta.url);

test("host actions preserve presentation state without embedding reviewed rows", () => {
  const context = {
    title: "Decision dashboard",
    snapshot: {
      generatedAt: "2026-07-27T12:00:00Z",
      queries: { reviewed: { rows: [{ secretMarker: 42 }] } },
    },
    presentation: {
      theme: "scientific-blue",
      title: "Edited decision dashboard",
      description: "Edited dashboard description",
      filters: { segment: "A" },
      assumptions: { activationLift: 12 },
      chartOverrides: { trend: { type: "line" } },
      hiddenBlocks: ["note"],
      componentTitles: { trend: "Edited trend" },
      textEdits: { "p:2": "Edited analysis" },
    },
  };
  const refresh = dataAppActionRequest("refresh", context);
  const publish = dataAppActionRequest("sites", context);
  const duplicate = dataAppActionRequest("duplicate", context);

  assert.match(refresh.prompt, /"segment": "A"/);
  assert.match(refresh.prompt, /"activationLift": 12/);
  assert.match(refresh.prompt, /"hiddenBlocks": \[/);
  assert.match(refresh.prompt, /"theme": "scientific-blue"/);
  assert.match(refresh.prompt, /"title": "Edited decision dashboard"/);
  assert.match(refresh.prompt, /"description": "Edited dashboard description"/);
  assert.doesNotMatch(refresh.prompt, /secretMarker/);
  assert.equal(duplicate.title, "Duplicate dashboard");
  assert.match(duplicate.prompt, /\$build-dashboard/);
  assert.match(duplicate.prompt, /new, separate dashboard/i);
  assert.match(duplicate.prompt, /view-only dashboard/i);
  assert.match(duplicate.prompt, /Never modify or overwrite the original dashboard/i);
  assert.match(duplicate.prompt, /"theme": "scientific-blue"/);
  assert.doesNotMatch(duplicate.prompt, /secretMarker/);
  assert.throws(
    () => dataAppActionRequest("duplicate", { ...context, surface: "report" }),
    /Duplication is available only for dashboards/,
  );
  assert.match(publish.prompt, /explicit action authorizes publication/);
  assert.match(publish.prompt, /Preserve the dashboard’s existing Sites access settings/);
  assert.match(publish.prompt, /normal hosted editing for the verified current Site owner/);
  assert.match(publish.prompt, /do not silently choose read-only when a supported owner-authorization path is available/);
  assert.match(
    dataAppActionRequest("sites", { ...context, accessMode: "custom" }).prompt,
    /access mode to custom \(Only those invited\)/,
  );
  assert.match(
    dataAppActionRequest("sites", { ...context, accessMode: "workspace_all" }).prompt,
    /access mode to workspace_all \(Anyone in this workspace with the link\)/,
  );
  for (const surface of ["dashboard", "report"]) {
    for (const accessMode of [undefined, "custom", "workspace_all"]) {
      const prompt = dataAppActionRequest("sites", { ...context, surface, accessMode }).prompt;
      assert.match(prompt, /normal hosted editing for the verified current Site owner/);
      assert.doesNotMatch(prompt, /legacy-owner-email|Product Security|PSEC-6681/);
    }
  }
  assert.throws(
    () => dataAppActionRequest("pdf", context),
    /PDF export prints this dashboard directly from the browser/,
  );
  for (const action of ["word", "google-docs"]) {
    assert.match(
      dataAppActionRequest(action, context).prompt,
      /Use \[@Data\]\(plugin:\/\/data-analytics@openai-curated-remote\) and invoke \$data-analytics:convert-to-doc with \[@Documents\]\(plugin:\/\/documents@openai-primary-runtime\)/u,
    );
  }
  for (const action of ["powerpoint", "google-slides"]) {
    assert.match(
      dataAppActionRequest(action, context).prompt,
      /Use \[@Data\]\(plugin:\/\/data-analytics@openai-curated-remote\) and invoke \$data-analytics:convert-to-slides with \[@Presentations\]\(plugin:\/\/presentations@openai-primary-runtime\)/u,
    );
  }
  assert.match(dataAppActionRequest("google-docs", context).prompt, /Import the verified DOCX as a native Google Doc/);
  assert.match(
    dataAppActionRequest("google-slides", context).prompt,
    /Import the verified PPTX as native Google Slides/,
  );
  assert.match(dataAppActionRequest("word", context).prompt, /rather than importing it into Google Drive/);
  assert.throws(() => dataAppActionRequest("unknown", context), /Unsupported dashboard action/);
});

test("PDF export prints directly without constructing a host action", () => {
  let printCalls = 0;
  assert.equal(
    printDataApp({
      print: () => {
        printCalls += 1;
      },
    }),
    true,
  );
  assert.equal(printCalls, 1);
  assert.throws(() => printDataApp({}), /browser cannot print the Data app/);
});

test("published host actions include personal exploration without writing it to shared presentation", async () => {
  const app = await readFile(new URL("../src/DataAppShell.jsx", import.meta.url), "utf8");
  assert.match(
    app,
    /\.\.\.\(!hosted \? \{ filters, assumptions \} : \{\}\)/,
    "Published exploration must remain outside shared persisted presentation",
  );
  assert.match(
    app,
    /presentation:\s*\{\s*\.\.\.presentation,\s*filters,\s*assumptions,/s,
    "Refresh and export requests still need the viewer's current device-local exploration",
  );
});

test("the starter keeps its shared runtime and hosted data contract", async () => {
  const [indexHtml, main, app, runtime, dataComponent, worker, workerFactory, hosting, packageJson] = await Promise.all(
    [
      readFile(new URL("index.html", templateRoot), "utf8"),
      readFile(new URL("src/main.jsx", templateRoot), "utf8"),
      readFile(new URL("src/App.jsx", templateRoot), "utf8"),
      readFile(new URL("src/DataAppRuntime.jsx", templateRoot), "utf8"),
      readFile(new URL("src/components/DataComponent.jsx", templateRoot), "utf8"),
      readFile(new URL("src/worker.js", templateRoot), "utf8"),
      readFile(new URL("src/data-app-worker.js", templateRoot), "utf8"),
      readFile(new URL(".openai/hosting.json", templateRoot), "utf8"),
      readFile(new URL("package.json", templateRoot), "utf8"),
    ],
  );
  const packageMetadata = JSON.parse(packageJson);

  assert.match(indexHtml, /src="\/src\/main\.jsx"/u);
  assert.match(main, /export function DataAppRuntime/u);
  assert.match(main, /createRoot\(root\)\.render\(/u);
  assert.match(main, /createRoot\(root\)\.render\([\s\S]*?<DataAppRuntime\s*\/>/u);
  assert.match(main, /from ["']\.\/App\.jsx["']/u);
  assert.doesNotMatch(app, /createRoot\(/u);
  assert.match(main, /return <App hosted=\{hosted\} \/>/u);
  assert.match(app, /from ["']\.\/DataAppRuntime\.jsx["']/u);
  assert.match(app, /<DataAppRuntime[\s\S]*?reviewedSnapshot=\{reviewedSnapshot\}/u);
  assert.match(app, /DashboardContent=\{DashboardContent\}/u);
  assert.match(app, /ReportContent=\{ReportContent\}/u);
  assert.match(runtime, /export function DataAppRuntime\(/u);
  assert.match(runtime, /snapshot\.surface === "report" \? ReportContent : DashboardContent/u);
  assert.match(runtime, /<DataAppShell\s+snapshot=\{snapshot\}/u);
  assert.doesNotMatch(runtime, /from ["']\.\/(?:data\.json|content\/)/u);
  assert.match(dataComponent, /requires a non-empty stable id/u);
  assert.match(dataComponent, /data-component-id=\{componentId\}/u);
  assert.match(hosting, /"d1"\s*:\s*"DB"/u);
  assert.match(worker, /from ["']\.\/data-app-worker\.js["']/u);
  assert.match(worker, /export default createDataAppWorker\(\{/u);
  assert.match(worker, /html: dataAppHtml/u);
  assert.match(worker, /ownerUserIdSha256: dataAppOwnerUserIdSha256/u);
  assert.match(worker, /ownerEmailSha256: dataAppOwnerEmailSha256/u);
  assert.match(workerFactory, /export function createDataAppWorker\(/u);
  assert.match(workerFactory, /ownerUserIdSha256: dataAppOwnerUserIdSha256 = ""/u);
  assert.match(workerFactory, /ownerEmailSha256: dataAppOwnerEmailSha256 = ""/u);
  assert.match(workerFactory, /pathname === "\/api\/snapshot"/u);
  assert.match(workerFactory, /storedSnapshot\(environment\.DB, seedSnapshot\)/u);
  assert.doesNotMatch(workerFactory, /from ["'][^"']*(?:data\.json|index\.html\?raw|data-app-owner\.js)["']/u);
  assert.equal(packageMetadata.scripts.build, "node scripts/verify-protected-runtime.mjs && vite build");
  assert.match(dataComponent, /useOptionalDataAppShell/u);
  assert.match(dataComponent, /requires a stable reviewed query id/u);
  assert.doesNotMatch(dataComponent, /requiredReportSummary|componentId === "report-executive-summary"/u,
    "Report headings and visibility belong to authored content");
});

test("report and dashboard content inherit protected chrome and source-backed component actions", async () => {
  const [shell, dashboard, report, reportStyles, manifest] = await Promise.all([
    readFile(new URL("../src/DataAppShell.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/content/dashboard/DashboardContent.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/content/report/ReportContent.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/content/report/report.css", import.meta.url), "utf8"),
    readFile(new URL("../protected-runtime.json", import.meta.url), "utf8"),
  ]);
  const protectedRuntime = JSON.parse(manifest);
  assert.match(shell, /<DataAppContext\.Provider value=\{shellContext\}/u);
  assert.match(shell, /<DataAppTopbar/u);
  assert.match(shell, /<DataAppThemeDrawer/u);
  assert.match(shell, /<SourceSidebar/u);
  assert.match(shell, /<ChartExplorer/u);
  assert.match(dashboard, /useDataApp\(\)/u);
  assert.match(report, /useDataApp\(\)/u);
  assert.doesNotMatch(dashboard, /\.\.\.actions/u);
  assert.doesNotMatch(report, /\.\.\.actions|RichMarkdown|>Save<|>Cancel</u);
  assert.match(report, /<ReportSection id="report-summary"/u);
  assert.match(report, /<MetricCard id="report-metric-active"/u);
  assert.match(
    report,
    /<RichNarrative id="report-summary:body"/u,
    "Independently editable report blocks require stable semantic identities",
  );
  assert.match(report, /<h1 data-data-app-title/u, "Report headings must persist only as the shared artifact title");
  assert.doesNotMatch(
    dashboard,
    /<h1 data-data-app-title/u,
    "Dashboard content must not duplicate the title already shown and persisted by the top bar",
  );
  assert.match(
    reportStyles,
    /\.report-page\s*\{[^}]*--data-app-content-width:\s*var\(--data-app-report-evidence-width\)[^}]*--data-app-report-prose-width:\s*var\(--data-app-content-width\)[^}]*max-width:\s*calc\(var\(--data-app-content-width\)\s*\+\s*2\s*\*\s*var\(--data-app-layout-gutter\)\)/su,
  );
  assert.match(reportStyles, /\.report-page \.dashboard-component:not\(\[data-component-kind="metric"\]\):not\(\[data-component-variant="card"\]\)\s*\{[^}]*border:\s*0/su,
    "The starter's editorial treatment preserves shared metric-card styling");
  for (const path of [
    "AGENTS.md",
    "src/App.jsx",
    "src/DataAppRuntime.jsx",
    "src/DataAppShell.jsx",
    "src/DataAppContext.jsx",
    "src/data-app-public.jsx",
    "src/components/DataComponent.jsx",
    "src/data-app-actions.js",
    "src/data-app-worker.js",
    "src/worker.js",
    "src/chrome-contrast.js",
    "src/chrome-layout.js",
  ]) {
    assert.ok(protectedRuntime.files[path], `${path} must be protected`);
  }
  for (const path of ["src/content/", "src/theme.css", "src/data.json"]) {
    assert.ok(protectedRuntime.editablePaths.includes(path), `${path} must remain model-authored`);
    assert.equal(protectedRuntime.files[path], undefined);
  }
});

test("authored report and dashboard code uses the protected public component API", async () => {
  const [publicApi, editableText, agents] = await Promise.all([
    readFile(new URL("../src/data-app-public.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/components/EditableText.jsx", import.meta.url), "utf8"),
    readFile(new URL("../AGENTS.md", import.meta.url), "utf8"),
  ]);
  assert.match(publicApi, /ChartRenderer as Chart/u);
  assert.match(publicApi, /DataTable as Table/u);
  assert.match(publicApi, /useDataAppShell as useDataApp/u);
  assert.match(publicApi, /EditableText/u);
  assert.match(editableText, /data-editable-narrative/u);
  assert.match(agents, /explicit user request is authorization/u);
  assert.match(agents, /Only ask for permission when the agent proposes/u);
  assert.doesNotMatch(agents, /obtain one clear confirmation|stop and ask for one clear confirmation/u);
  assert.match(agents, /src\/content\/assets\//u);
  assert.match(agents, /--data-app-chrome-background/u);
  assert.match(agents, /integrity:authorize/u);
});

test("rich Markdown editor caps pill-shaped control radii", async () => {
  const styles = await readFile(new URL("../src/styles.css", import.meta.url), "utf8");
  const editorRule = styles.match(/\.markdown-editable\s*\{(?<body>[^}]*)\}/u)?.groups.body;
  assert.ok(editorRule, "The rich Markdown editor must keep its protected styling");
  assert.match(
    editorRule,
    /border-radius:\s*min\(var\(--control-radius\),\s*var\(--radius-md\)\)/u,
    "Multiline text editors must not inherit pill-shaped control radii",
  );
});
