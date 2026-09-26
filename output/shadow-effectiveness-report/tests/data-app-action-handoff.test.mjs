import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createServer } from "vite";

import { dataAppActionHref, dataAppActionRequest, submitDataAppAction } from "../src/data-app-actions.js";
import {
  codexDataAppActionUrl,
  currentDataAppReference,
  launchCodexPromptFallback,
} from "../src/runtime-environment.js";

test("Data apps explicitly invoke their protected artifact-sharing skill", async () => {
  const chrome = await readFile(new URL("../src/components/DataAppChrome.jsx", import.meta.url), "utf8");
  const promptActions = chrome.match(/const promptActionDetails = \{[\s\S]*?\n\};/u)?.[0] ?? "";
  const askSuggestions =
    chrome.match(/function dataAppAskSuggestions\([\s\S]*?\n\}\n\nfunction useCompactAskMenu/u)?.[0] ?? "";
  const overflowMenu =
    chrome.match(/function DataAppOverflowMenu\([\s\S]*?\n\}\n\nconst promptActionDetails/u)?.[0] ?? "";
  const convertItems =
    chrome.match(/function DataAppConvertItems\([\s\S]*?\n\}\n\nfunction DataAppPublishButton/u)?.[0] ?? "";
  const convertOptions = chrome.match(/const convertOptionGroups = \[[\s\S]*?\n\];/u)?.[0] ?? "";
  const promptActionRows = [
    ...promptActions.matchAll(/"([^"]+)": \{\s*label: "([^"]+)",\s*subtext: "([^"]+)",\s*icon: "([^"]+)"/gu),
  ].map(([, action, label, subtext, icon]) => ({ action, label, subtext, icon }));

  assert.deepEqual(
    promptActionRows.map(({ action, label }) => ({ action, label })),
    [
      { action: "share-summary", label: "Draft a team update" },
      { action: "refresh-document", label: "Refresh a document" },
      { action: "alert-changes", label: "Set up an alert" },
    ],
  );
  assert.deepEqual(
    promptActionRows.map(({ action, subtext }) => ({ action, subtext })),
    [
      { action: "share-summary", subtext: "Share the highlights in a message" },
      { action: "refresh-document", subtext: "Update a file with fresh data" },
      { action: "alert-changes", subtext: "Notify you of important changes" },
    ],
  );
  for (const { icon } of promptActionRows) assert.match(icon, /^[A-Za-z][A-Za-z0-9]*$/u);
  assert.doesNotMatch(
    chrome,
    /useDialKit|data-app-topbar-actions|data-app-ask-chatgpt-action|data-app-action-previews|actionVariant|dashboard-overflow|DataAppPromptAction|dashboard-action-preview|onCopyPrompt/u,
    "The dashboard Ask composer must replace DialKit, the old experiment, overflow, and hover previews",
  );
  assert.match(
    chrome,
    /<HeaderActionButton\s+ref=\{askButtonRef\}[\s\S]*?onClick=\{\(\) => openAskComposer\(\)\}[\s\S]*?<DataAppPublishButton[\s\S]*?<DataAppOverflowMenu/u,
    "The shared Ask composer must be available in every top-bar state",
  );
  assert.doesNotMatch(
    askSuggestions,
    /<Dropdown\.(?:Group|Label)\b/u,
    "The Ask zero state must not keep the removed Automate and Customize headers",
  );
  assert.match(
    askSuggestions,
    /Object\.entries\(promptActionDetails\)[\s\S]*?Remix this dashboard/u,
    "The Ask zero state must keep prompt actions first and its remaining utilities in the same list",
  );
  assert.doesNotMatch(askSuggestions, /hasUtilityActions|<MenuSeparator \/>/u);
  assert.doesNotMatch(askSuggestions, /(?:Customize|Edit) theme|Convert to\.\.\./u);
  assert.match(
    overflowMenu,
    /icon="link"[\s\S]*?Copy link[\s\S]*?icon="palette"[\s\S]*?Edit theme[\s\S]*?icon="eye"[\s\S]*?Restore hidden \(\{hiddenCount\}\)[\s\S]*?hasUtilityActions && <MenuSeparator \/>[\s\S]*?<MenuGroup label="Export">[\s\S]*?<DataAppConvertItems/u,
    "The overflow must keep the newer icons while restoring utility order, one divider, and the inline Export group",
  );
  assert.doesNotMatch(
    overflowMenu,
    /\bDuplicate\b|dataAppActionLink\(getActionHref, "duplicate"\)|MenuSub|Convert to\.\.\./u,
    "The More menu must not repeat the dashboard remix action",
  );
  assert.doesNotMatch(askSuggestions, /size=\{20\}/u, "Ask suggestion icons must use the standard icon frame");
  const convertRows = [
    ...convertOptions.matchAll(/\{\s*action:\s*"([^"]+)",\s*label:\s*"([^"]+)",\s*iconSrc:\s*(\w+),?\s*\}/gu),
  ].map(([, action, label, iconSrc]) => ({ action, label, iconSrc }));
  assert.deepEqual(convertRows, [
    { action: "pdf", label: "PDF", iconSrc: "convertPdfIcon" },
    { action: "word", label: "Word document", iconSrc: "convertWordIcon" },
    { action: "powerpoint", label: "PowerPoint", iconSrc: "convertPowerpointIcon" },
    { action: "google-docs", label: "Google Docs", iconSrc: "convertGoogleDocsIcon" },
    { action: "google-slides", label: "Google Slides", iconSrc: "convertGoogleSlidesIcon" },
    { action: "jupyter-notebook", label: "Jupyter Notebook", iconSrc: "convertJupyterIcon" },
    { action: "html", label: "HTML", iconSrc: "convertHtmlIcon" },
  ]);
  assert.match(
    convertItems,
    /convertOptionGroups\.flat\(\)\.map\([\s\S]*?action === "pdf"[\s\S]*?onSelect: \(\) => onAction\?\.\(action\)[\s\S]*?dataAppActionLink\(getActionHref, action\)/u,
    "Export rows must stay inline and use genuine action links while keeping PDF export local",
  );
  assert.doesNotMatch(convertItems, /MenuSeparator/u, "The inline Export group must not add internal dividers");
  assert.match(
    askSuggestions,
    /Object\.entries\(promptActionDetails\)\.map\([\s\S]*?\.\.\.dataAppActionLink\(getActionHref, action\)/u,
    "Prompt suggestions must keep real navigation links instead of synthetic callback clicks",
  );
  assert.match(askSuggestions, /dataAppActionLink\(getActionHref, "duplicate"\)/u);
  assert.match(chrome, /dataAppActionLink\(getActionHref, "refresh"\)/u);
  assert.match(chrome, /dataAppActionLink\(getActionHref, "schedule-refresh", \{ schedule \}\)/u);

  for (const surface of ["dashboard", "report"]) {
    const request = dataAppActionRequest("share-summary", {
      surface,
      title: "Sensitive reviewed app",
      presentation: { filters: { account: "private" } },
    });
    assert.deepEqual(request, {
      title: "Share a summary",
      prompt: `Use @Data and invoke $share-artifact-summary to share a summary of this ${surface}.`,
    });
    assert.doesNotMatch(
      request.prompt,
      /Slack|myself|Sensitive reviewed app|private|connectors|structured|bounded lookup/u,
    );
  }

  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  const location = new URL(`file:///Users/example/Dashboard/dist/index.html#codexThreadId=${threadId}`);
  const href = new URL(dataAppActionHref("share-summary", { surface: "dashboard" }, location));
  assert.equal(`${href.protocol}//${href.host}${href.pathname}`, `codex://threads/${threadId}`);
  assert.equal(
    href.searchParams.get("prompt"),
    "Use @Data and invoke $share-artifact-summary to share a summary of this dashboard.\n\n" +
      "dashboard project directory: /Users/example/Dashboard\n" +
      "dashboard HTML file: /Users/example/Dashboard/dist/index.html",
  );
  assert.doesNotMatch(href.searchParams.get("prompt"), new RegExp(threadId, "u"));
});

test("sharing identifies the exact published or local app without exposing private context", () => {
  const published = dataAppActionRequest("share-summary", {
    surface: "report",
    title: "Sensitive reviewed app",
    presentation: { filters: { account: "private" } },
    dataAppReference: {
      sourceUrl: "https://metrics.openai.chatgpt.site/growth?token=secret#private",
    },
  });
  assert.equal(
    published.prompt,
    "Use @Data and invoke $share-artifact-summary to share a summary of this report.\n\n" +
      "Published report URL: https://metrics.openai.chatgpt.site/growth",
  );
  assert.doesNotMatch(published.prompt, /Sensitive reviewed app|token|secret|private/u);

  const local = dataAppActionRequest("share-summary", {
    surface: "dashboard",
    dataAppReference: {
      root: "/Users/example/Dashboard",
      htmlPath: "/Users/example/Dashboard/dist/index.html",
      sourceUrl: "http://terminal.local:4173/?token=secret",
    },
  });
  assert.equal(
    local.prompt,
    "Use @Data and invoke $share-artifact-summary to share a summary of this dashboard.\n\n" +
      "dashboard project directory: /Users/example/Dashboard\n" +
      "dashboard HTML file: /Users/example/Dashboard/dist/index.html",
  );
  assert.doesNotMatch(local.prompt, /terminal\.local|token|secret/u);

  for (const dataAppReference of [
    { sourceUrl: "https://user:password@reports.example.com/private" },
    { sourceUrl: "http://localhost:5173/preview" },
    { sourceUrl: "https://terminal.local/preview" },
    { sourceUrl: "https://dashboard.local/preview" },
    { sourceUrl: "not a URL" },
  ]) {
    assert.deepEqual(
      dataAppActionRequest("share-summary", {
        surface: "dashboard",
        dataAppReference,
      }),
      {
        title: "Share a summary",
        prompt: "Use @Data and invoke $share-artifact-summary to share a summary of this dashboard.",
      },
    );
  }
});

test("HTML export requests a sanitized copy without changing the reviewed preview", () => {
  for (const surface of ["dashboard", "report"]) {
    const { prompt } = dataAppActionRequest("html", { surface });

    assert.match(prompt, /`dist\/shareable\.html`[^\n]*`dist\/index\.html`/u);
    assert.match(prompt, /removing `data-app-local-thread` metadata and every originating task ID/u);
    assert.match(prompt, /Leave the original HTML and preview unchanged/u);
    assert.doesNotMatch(prompt, /export-data-app-html(?:\.mjs)?/u);
  }
});

test("Data app actions identify their exact local HTML and project for ChatGPT handoffs", () => {
  const reference = currentDataAppReference({
    href: "file:///Users/example/My%20Dashboard/dist/index.html?preview=1#chart",
  });
  assert.deepEqual(reference, {
    htmlPath: "/Users/example/My Dashboard/dist/index.html",
    root: "/Users/example/My Dashboard",
    sourceUrl: "file:///Users/example/My%20Dashboard/dist/index.html",
  });
  assert.deepEqual(
    currentDataAppReference({
      href: "https://dashboard.chatgpt.site/path?token=private#title",
    }),
    {
      sourceUrl: "https://dashboard.chatgpt.site/path",
    },
  );

  const request = dataAppActionRequest("sites", {
    title: "Rollout review",
    accessMode: "custom",
    dataAppReference: reference,
  });
  assert.match(request.prompt, /Dashboard project directory: \/Users\/example\/My Dashboard/);
  assert.match(request.prompt, /Dashboard HTML file: \/Users\/example\/My Dashboard\/dist\/index\.html/);
  assert.match(request.prompt, /package the exact existing dashboard HTML/i);
  assert.doesNotMatch(request.prompt, /stop and ask/);

  const reportRequest = dataAppActionRequest("sites", {
    surface: "report",
    title: "Executive review",
    dataAppReference: reference,
  });
  assert.match(reportRequest.prompt, /Report project directory:/);
  assert.match(reportRequest.prompt, /exact existing report HTML/i);
});

test("local Data app actions return to their existing task without leaking its identifier", () => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  const location = new URL(`file:///Users/example/Dashboard/dist/index.html#codexThreadId=${threadId}`);
  const prompt = "Refresh the dashboard & preserve the reviewed source";
  const action = codexDataAppActionUrl(prompt, location);

  assert.equal(
    action.toString(),
    `codex://threads/${threadId}?prompt=Refresh+the+dashboard+%26+preserve+the+reviewed+source`,
  );
  assert.equal(action.searchParams.get("prompt"), prompt);
  assert.equal(action.searchParams.has("originUrl"), false);
  assert.equal(action.searchParams.has("path"), false);
  assert.equal(action.searchParams.get("prompt").includes(threadId), false);
  assert.deepEqual(currentDataAppReference(location), {
    htmlPath: "/Users/example/Dashboard/dist/index.html",
    root: "/Users/example/Dashboard",
    sourceUrl: "file:///Users/example/Dashboard/dist/index.html",
  });
});

test("hosted Data app actions recognize current and rollout Codex browser identities", (context) => {
  const originalNavigator = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  context.after(() => {
    if (originalNavigator) Object.defineProperty(globalThis, "navigator", originalNavigator);
    else delete globalThis.navigator;
  });
  const location = new URL("https://dashboard.chatgpt.site/published?token=secret#private-section");

  for (const userAgent of ["CodexBrowser/1.0", "ChatGPTBrowser/1.0"]) {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: { userAgent },
    });
    const action = new URL(dataAppActionHref("share-summary", { surface: "dashboard" }, location));

    assert.equal(`${action.protocol}//${action.host}${action.pathname}`, "codex://threads/new");
    assert.equal(action.searchParams.get("browserUrl"), "https://dashboard.chatgpt.site/published");
    assert.doesNotMatch(action.toString(), /secret|private-section/u);
  }
});

test("localhost previews use a same-frame Codex prompt fallback", () => {
  const previousWindow = globalThis.window;
  const previousDocument = globalThis.document;
  let openedHref = "";
  const link = {
    href: "",
    target: "",
    rel: "",
    style: {},
    click() {
      openedHref = this.href;
    },
    remove() {},
  };
  try {
    globalThis.window = { location: new URL("http://127.0.0.1:5365/") };
    globalThis.document = {
      createElement() {
        return link;
      },
      body: { appendChild() {} },
    };
    assert.equal(launchCodexPromptFallback("Use @Data to refresh this dashboard."), true);
    const opened = new URL(openedHref);
    assert.equal(opened.protocol + "//" + opened.host + opened.pathname, "codex://threads/new");
    assert.equal(link.target, "_self");
    assert.equal(opened.searchParams.has("browserUrl"), false);
  } finally {
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
    if (previousDocument === undefined) delete globalThis.document;
    else globalThis.document = previousDocument;
  }
});

test("localhost previews without an app reference omit automation links but still reject submission", async (t) => {
  const previousWindow = globalThis.window;
  let sends = 0;
  const location = new URL("http://127.0.0.1:8766/");
  globalThis.window = {
    location,
    openai: { sendFollowUpMessage() { sends += 1; return {}; } },
  };
  t.after(() => {
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
  });

  for (const surface of ["dashboard", "report"]) {
    const context = { surface, title: "Local preview", snapshot: { id: "local-preview" } };
    const actions = surface === "dashboard" ? ["alert-changes", "schedule-refresh"] : ["alert-changes"];
    for (const action of actions) {
      assert.equal(dataAppActionHref(action, context, location), null);
      await assert.rejects(submitDataAppAction(action, context), /requires its exact project path/u);
    }
    assert.ok(dataAppActionHref("share-summary", context, location), "Unrelated links remain available");
  }
  assert.equal(sends, 0, "An unidentified automation must never reach the host transport");
});

test("dashboard and report chrome render on localhost without project metadata", async () => {
  const server = await createServer({
    root: fileURLToPath(new URL("../", import.meta.url)),
    configFile: false,
    server: { middlewareMode: true, watch: null, hmr: false },
    appType: "custom",
  });
  try {
    const { DataAppTopbar } = await server.ssrLoadModule("/src/components/DataAppChrome.jsx");
    const location = new URL("http://127.0.0.1:8766/");
    for (const surface of ["dashboard", "report"]) {
      const context = { surface, title: "Local preview" };
      const html = renderToStaticMarkup(React.createElement(DataAppTopbar, {
        ...context,
        generatedAt: "2026-08-24T12:00:00Z",
        mode: "view",
        getActionHref: (action, options) => dataAppActionHref(action, { ...context, ...options }, location),
      }));
      assert.ok(html.includes("Local preview"), "The title renders instead of a blank app");
      assert.ok(html.includes('aria-label="Ask ChatGPT"'), "The Ask control remains available");
    }
  } finally {
    await server.close();
  }
});

test("automation links retain exact identity for file, identified localhost, and hosted previews", () => {
  const localReference = {
    root: "/Users/example/Dashboard",
    htmlPath: "/Users/example/Dashboard/dist/index.html",
  };
  for (const [location, dataAppReference, expectedIdentity] of [
    [new URL("file:///Users/example/Dashboard/dist/index.html"), undefined, localReference.htmlPath],
    [new URL("http://localhost:5173/"), localReference, localReference.htmlPath],
    [new URL("https://dashboard.chatgpt.site/"), undefined, "https://dashboard.chatgpt.site/"],
  ]) {
    for (const action of ["alert-changes", "schedule-refresh"]) {
      const href = new URL(dataAppActionHref(action, { surface: "dashboard", dataAppReference }, location));
      const prompt = href.searchParams.get("prompt") ?? href.searchParams.get("q");
      assert.ok(prompt.includes(expectedIdentity), "Automation links must retain their exact target");
    }
  }
});

test("unavailable automation links do not mask invalid references or unsupported actions", () => {
  const location = new URL("http://localhost:5173/");
  assert.throws(() => dataAppActionHref("alert-changes", {
    surface: "dashboard", dataAppReference: { sourceUrl: "not a URL" },
  }, location), /valid, credential-free published URL/u);
  assert.throws(() => dataAppActionHref("unknown", { surface: "dashboard" }, location), /Unsupported dashboard action/u);
});

test("local publication links use the Codex deep link and retain the selected access policy", () => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  const location = new URL(`file:///Users/example/My%20Dashboard/dist/index.html#codexThreadId=${threadId}`);
  const href = new URL(
    dataAppActionHref(
      "sites",
      {
        surface: "dashboard",
        title: "Rollout review",
        accessMode: "workspace_all",
      },
      location,
    ),
  );

  assert.equal(`${href.protocol}//${href.host}${href.pathname}`, `codex://threads/${threadId}`);
  assert.match(href.searchParams.get("prompt"), /Publish this dashboard through Sites/);
  assert.match(href.searchParams.get("prompt"), /Anyone in this workspace with the link/);
  assert.match(href.searchParams.get("prompt"), /Dashboard project directory: \/Users\/example\/My Dashboard/);
  assert.equal(href.searchParams.has("originUrl"), false);
  assert.equal(href.searchParams.has("path"), false);
  assert.equal(href.searchParams.get("prompt").includes(threadId), false);
});

test("Data app actions select the appropriate deep link without exposing task context", () => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";

  for (const href of [
    "file:///Users/example/Dashboard/dist/index.html",
    "file:///Users/example/Dashboard/dist/index.html#codexThreadId=../../settings",
  ]) {
    const action = codexDataAppActionUrl("Export as PDF", new URL(href));
    assert.equal(`${action.protocol}//${action.host}${action.pathname}`, "codex://threads/new");
    assert.equal(action.searchParams.get("prompt"), "Export as PDF");
    assert.equal(action.searchParams.has("originUrl"), false);
    assert.equal(action.searchParams.has("path"), false);
    assert.equal(action.searchParams.has("browserUrl"), false);
  }

  const hosted = codexDataAppActionUrl(
    "Refresh dashboard",
    new URL(`https://user:password@dashboard.chatgpt.site/path?token=secret#codexThreadId=${threadId}`),
  );
  assert.equal(`${hosted.origin}${hosted.pathname}`, "https://chatgpt.com/");
  assert.equal(hosted.searchParams.get("q"), "Refresh dashboard");
  assert.equal(hosted.searchParams.has("originUrl"), false);
  assert.equal(hosted.searchParams.has("path"), false);
  assert.doesNotMatch(hosted.toString(), /secret|user|password|codexThreadId/);
});
