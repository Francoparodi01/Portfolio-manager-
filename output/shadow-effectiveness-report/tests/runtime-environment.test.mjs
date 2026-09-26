import assert from "node:assert/strict";
import test from "node:test";

import { dataAppActionHref, submitDataAppAction } from "../src/data-app-actions.js";
import {
  chatGPTPromptUrl,
  chatGPTWorkTaskUrl,
  codexDataAppNewTaskUrl,
  currentDataAppReference,
  dataAppPromptTarget,
  isCodexBrowser,
} from "../src/runtime-environment.js";

test("Ask ChatGPT uses the public Work handoff with an encoded clean browser URL", () => {
  const prompt = "Why did Enterprise grow 12%?\n\nSelected context: Enterprise — 12% & rising";
  const browserUrl = "https://dashboard.chatgpt.site/_data/charts/abc-123";
  const handoff = chatGPTWorkTaskUrl(prompt, browserUrl);

  assert.equal(`${handoff.origin}${handoff.pathname}`, "https://chatgpt.com/codex/open-app");
  assert.deepEqual(
    [...handoff.searchParams.keys()],
    ["app_brand", "fallback", "q", "browserUrl"],
  );
  assert.equal(handoff.searchParams.get("app_brand"), "chatgpt");
  assert.equal(handoff.searchParams.get("fallback"), "work");
  assert.equal(handoff.searchParams.get("q"), prompt);
  assert.equal(handoff.searchParams.get("browserUrl"), browserUrl);
  assert.match(handoff.toString(), /q=Why\+did\+Enterprise\+grow\+12%25%3F/u);
});

test("Ask ChatGPT omits unsafe browser context without weakening the Work fallback", () => {
  for (const browserUrl of [
    "javascript:alert(1)",
    "https://viewer:secret@dashboard.chatgpt.site/",
    "https://dashboard.chatgpt.site/?token=private",
    "https://dashboard.chatgpt.site/#private",
    "https://dashboard.chatgpt.site/tokens/private",
  ]) {
    const handoff = chatGPTWorkTaskUrl("Explain this chart", browserUrl);
    assert.equal(handoff.searchParams.get("fallback"), "work");
    assert.equal(handoff.searchParams.has("browserUrl"), false, browserUrl);
  }
  assert.throws(() => chatGPTWorkTaskUrl(" ", "https://dashboard.chatgpt.site/"), /requires a prompt/u);
});

test("Ask ChatGPT retains a non-Sites dashboard path without leaking private URL state", () => {
  const reference = currentDataAppReference(new URL("https://viewer:secret@example.com/reports/dashboard.html?token=private#mark"));
  const handoff = chatGPTWorkTaskUrl("Explain this chart", reference.sourceUrl);
  assert.equal(handoff.searchParams.get("browserUrl"), "https://example.com/reports/dashboard.html");
  assert.equal(handoff.searchParams.get("fallback"), "work");
  assert.doesNotMatch(handoff.toString(), /viewer|secret|private|_data/u);
});

function useLocalThreadMetadata(context, threadId) {
  const originalDocument = globalThis.document;
  globalThis.document = {
    querySelector: (selector) =>
      selector === 'meta[name="data-app-local-thread"]'
        ? {
            getAttribute: (attribute) => (attribute === "content" ? threadId : null),
          }
        : null,
  };
  context.after(() => {
    if (originalDocument === undefined) delete globalThis.document;
    else globalThis.document = originalDocument;
  });
}

function useLocalReferenceMetadata(context, entries) {
  const originalDocument = globalThis.document;
  let contents = entries;
  globalThis.document = {
    querySelectorAll: (selector) =>
      selector === 'meta[name="data-app-local-reference"]'
        ? contents.map((content) => ({
            getAttribute: (attribute) => (attribute === "content" ? content : null),
          }))
        : [],
  };
  context.after(() => {
    if (originalDocument === undefined) delete globalThis.document;
    else globalThis.document = originalDocument;
  });
  return (next) => {
    contents = next;
  };
}

function useSitesProjectMetadata(context, entries) {
  const originalDocument = globalThis.document;
  globalThis.document = {
    querySelectorAll: (selector) =>
      selector === 'meta[name="data-app-sites-project"]'
        ? entries.map((content) => ({
            getAttribute: (attribute) => (attribute === "content" ? content : null),
          }))
        : [],
  };
  context.after(() => {
    if (originalDocument === undefined) delete globalThis.document;
    else globalThis.document = originalDocument;
  });
}

test("native prompt links use the current tab while hosted web links use a new tab", () => {
  const hosted = new URL("https://growth.openai.chatgpt.site/");
  const local = new URL("file:///Users/example/dashboard/dist/index.html");

  assert.equal(dataAppPromptTarget(chatGPTPromptUrl("Publish", local)), "_self");
  assert.equal(dataAppPromptTarget(chatGPTPromptUrl("Publish", hosted, "CodexBrowser Chrome/140")), "_self");
  assert.equal(dataAppPromptTarget(chatGPTPromptUrl("Publish", hosted, "ChatGPTBrowser Chrome/140")), "_self");
  assert.equal(dataAppPromptTarget(chatGPTPromptUrl("Publish", hosted, "Mozilla/5.0 Chrome/140")), "_blank");
  assert.equal(dataAppPromptTarget(undefined), "_blank");
});

test("Codex browser detection accepts current and rollout user agents", () => {
  assert.equal(isCodexBrowser("CodexBrowser Chrome/140"), true);
  assert.equal(isCodexBrowser("ChatGPTBrowser Chrome/140"), true);
  assert.equal(isCodexBrowser("Mozilla/5.0 Chrome/140"), false);
  assert.equal(isCodexBrowser("CodexBrowserSpoof Chrome/140"), false);
  assert.equal(isCodexBrowser(""), false);
  assert.equal(isCodexBrowser(null), false);
});

test("published apps always open a new task without sharing a prior task identifier", (context) => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  useLocalThreadMetadata(context, threadId);
  const location = new URL(`https://viewer:secret@growth.openai.chatgpt.site/?token=private#codexThreadId=${threadId}`);
  const desktop = chatGPTPromptUrl("Refresh the dashboard", location, "CodexBrowser Chrome/140");

  assert.equal(`${desktop.protocol}//${desktop.host}${desktop.pathname}`, "codex://threads/new");
  assert.equal(desktop.searchParams.get("prompt"), "Refresh the dashboard");
  assert.equal(desktop.searchParams.get("browserUrl"), "https://growth.openai.chatgpt.site/");
  assert.doesNotMatch(desktop.toString(), /viewer|secret|private|codexThreadId|550e8400/u);

  const browser = chatGPTPromptUrl("Refresh the dashboard", location, "Mozilla/5.0 Chrome/140");
  assert.equal(browser.toString(), "https://chatgpt.com/?q=Refresh+the+dashboard");
});

test("local HTML and Sites previews recover their task from build metadata without a URL fragment", (context) => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  useLocalThreadMetadata(context, threadId);
  for (const href of [
    "file:///Users/example/dashboard/dist/index.html",
    "http://terminal.local:4173/",
    "http://localhost:5173/",
    "http://127.0.0.1:5173/",
    "http://[::1]:5173/",
    "http://dashboard.localhost:5173/",
  ]) {
    const action = chatGPTPromptUrl("Refresh the dashboard", new URL(href), "Mozilla/5.0");
    assert.equal(`${action.protocol}//${action.host}${action.pathname}`, `codex://threads/${threadId}`);
    assert.deepEqual([...action.searchParams.keys()], ["prompt"]);
  }

  const explicitThreadId = "de305d54-75b4-431b-adb2-eb6b9e546014";
  const explicit = chatGPTPromptUrl(
    "Refresh the dashboard",
    new URL(`file:///Users/example/dashboard/dist/index.html#codexThreadId=${explicitThreadId}`),
  );
  assert.equal(
    `${explicit.protocol}//${explicit.host}${explicit.pathname}`,
    `codex://threads/${threadId}`,
    "Protected local build metadata must take precedence over a stale or untrusted URL fragment",
  );

  const invalidFragment = chatGPTPromptUrl(
    "Refresh the dashboard",
    new URL("file:///Users/example/dashboard/dist/index.html#codexThreadId=not-a-uuid"),
  );
  assert.equal(
    `${invalidFragment.protocol}//${invalidFragment.host}${invalidFragment.pathname}`,
    `codex://threads/${threadId}`,
  );
});

test("local handoffs reject missing or invalid task identifiers", (context) => {
  useLocalThreadMetadata(context, "../../settings");
  for (const fragment of ["", "draft", "codexThreadId=../../settings", "codexThreadId=not-a-uuid"]) {
    const action = chatGPTPromptUrl(
      "Refresh the dashboard",
      new URL(`file:///Users/example/dashboard/dist/index.html#${fragment}`),
    );
    assert.equal(`${action.protocol}//${action.host}${action.pathname}`, "codex://threads/new");
    assert.equal(action.searchParams.get("prompt"), "Refresh the dashboard");
  }

  const legacyThreadId = "550e8400-e29b-41d4-a716-446655440000";
  const legacy = chatGPTPromptUrl(
    "Refresh the dashboard",
    new URL(`file:///Users/example/dashboard/dist/index.html#codexThreadId=${legacyThreadId}`),
  );
  assert.equal(
    `${legacy.protocol}//${legacy.host}${legacy.pathname}`,
    `codex://threads/${legacyThreadId}`,
    "Valid fragments remain a legacy fallback only when trusted build metadata is absent",
  );
});

test("development previews retain their project identity without leaking internal URLs", () => {
  for (const href of [
    "http://terminal.local:4173/?token=private#draft",
    "http://localhost:5173/",
    "http://127.0.0.1:5173/",
    "http://[::1]:5173/",
    "http://dashboard.localhost:5173/",
  ]) {
    assert.deepEqual(currentDataAppReference(new URL(href), "/workspace/revenue-dashboard"), {
      root: "/workspace/revenue-dashboard",
      htmlPath: "/workspace/revenue-dashboard/dist/index.html",
    });
    assert.deepEqual(currentDataAppReference(new URL(href)), {});
  }
});

test("prebuilt development previews use their exact server-supplied local HTML identity", (context) => {
  const reference = {
    root: "/workspace/My Dashboard",
    htmlPath: "/workspace/My Dashboard/build/review.html",
  };
  useLocalReferenceMetadata(context, [JSON.stringify(reference)]);
  for (const href of [
    "http://terminal.local:4173/?token=private#draft",
    "https://localhost:5173/",
    "http://127.0.0.1:5173/",
    "http://[::1]:5173/",
    "http://dashboard.localhost:5173/",
  ]) {
    assert.deepEqual(currentDataAppReference(new URL(href)), reference);
    assert.deepEqual(currentDataAppReference(new URL(href), "/workspace/old-source-build"), reference);
  }

  const action = new URL(
    dataAppActionHref(
      "sites",
      {
        surface: "dashboard",
        title: "Revenue overview",
      },
      new URL("http://localhost:5173/?token=private#draft"),
    ),
  );
  assert.match(action.searchParams.get("prompt"), /Dashboard project directory: \/workspace\/My Dashboard/u);
  assert.match(
    action.searchParams.get("prompt"),
    /Dashboard HTML file: \/workspace\/My Dashboard\/build\/review\.html/u,
  );
  assert.doesNotMatch(action.searchParams.get("prompt"), /localhost|token=private|#draft/u);
});

test("local-preview metadata supports normalized POSIX, drive-letter, and UNC paths", (context) => {
  const setMetadata = useLocalReferenceMetadata(context, []);
  const location = new URL("http://localhost:4173/");
  for (const [reference, expected] of [
    [
      { root: "/workspace/app/", htmlPath: "/workspace/app/dist/index.html" },
      { root: "/workspace/app", htmlPath: "/workspace/app/dist/index.html" },
    ],
    [
      { root: "/", htmlPath: "/dashboard.htm" },
      { root: "/", htmlPath: "/dashboard.htm" },
    ],
    [
      { root: "C:\\Users\\Example\\Dashboard", htmlPath: "c:\\users\\example\\dashboard\\dist\\index.HTML" },
      { root: "C:/Users/Example/Dashboard", htmlPath: "c:/users/example/dashboard/dist/index.HTML" },
    ],
    [
      { root: "C:\\", htmlPath: "C:\\Dashboard\\dist\\index.html" },
      { root: "C:/", htmlPath: "C:/Dashboard/dist/index.html" },
    ],
    [
      { root: "\\\\server\\share\\Dashboard\\", htmlPath: "\\\\SERVER\\share\\Dashboard\\dist\\index.html" },
      { root: "//server/share/Dashboard", htmlPath: "//SERVER/share/Dashboard/dist/index.html" },
    ],
  ]) {
    setMetadata([JSON.stringify(reference)]);
    assert.deepEqual(currentDataAppReference(location), expected);
  }
});

test("local-preview metadata rejects ambiguous, malformed, or escaping paths", (context) => {
  const setMetadata = useLocalReferenceMetadata(context, []);
  const location = new URL("http://localhost:4173/");
  const fallback = { root: "/workspace/fallback", htmlPath: "/workspace/fallback/dist/index.html" };
  const valid = { root: "/workspace/app", htmlPath: "/workspace/app/dist/index.html" };
  const invalid = [
    null,
    [],
    {},
    { ...valid, sourceUrl: "https://example.com/" },
    { ...valid, root: "workspace/app" },
    { ...valid, htmlPath: "dist/index.html" },
    { ...valid, htmlPath: "/workspace/app-other/index.html" },
    { ...valid, htmlPath: "/workspace/app/../outside.html" },
    { ...valid, htmlPath: "/workspace/app/nested\\..\\..\\outside.html" },
    { ...valid, htmlPath: "/workspace/app/./dist/index.html" },
    { ...valid, htmlPath: "/workspace/app//dist/index.html" },
    { ...valid, root: "/workspace/./app" },
    { ...valid, root: "/workspace/app\t" },
    { ...valid, htmlPath: "/workspace/app/dist/index\u0000.html" },
    { ...valid, htmlPath: "/workspace/app/dist/index\u0085.html" },
    { ...valid, htmlPath: "/workspace/app/dist/index\u2028.html" },
    { ...valid, htmlPath: "https://localhost/index.html" },
    { ...valid, htmlPath: "/workspace/app/dist/index.js" },
    { ...valid, htmlPath: "/workspace/app/dist/index.html/" },
    { root: "/workspace/app.html", htmlPath: "/workspace/app.html" },
    { root: "C:Dashboard", htmlPath: "C:Dashboard/index.html" },
    { root: "C:\\Dashboard", htmlPath: "D:\\Dashboard\\index.html" },
    { root: "C:\\Dashboard", htmlPath: "C:\\Dashboard\\.. \\outside.html" },
    { root: "C:\\Dashboard", htmlPath: "C:\\Dashboard\\index.html:stream" },
    { root: "\\\\?\\C:\\Dashboard", htmlPath: "\\\\?\\C:\\Dashboard\\index.html" },
    { root: "\\\\server\\share\\Dashboard", htmlPath: "\\\\server\\other\\Dashboard\\index.html" },
  ];
  for (const value of invalid) {
    setMetadata([JSON.stringify(value)]);
    assert.deepEqual(currentDataAppReference(location), {}, JSON.stringify(value));
    assert.deepEqual(currentDataAppReference(location, fallback.root), fallback, JSON.stringify(value));
  }
  for (const entries of [[], ["{"], [JSON.stringify(valid), JSON.stringify(valid)], ["null", JSON.stringify(valid)]]) {
    setMetadata(entries);
    assert.deepEqual(currentDataAppReference(location), {});
    assert.deepEqual(currentDataAppReference(location, fallback.root), fallback);
  }
});

test("hosted and saved-file views never read local-preview metadata", (context) => {
  const reference = { root: "/workspace/private", htmlPath: "/workspace/private/dist/index.html" };
  useLocalReferenceMetadata(context, [JSON.stringify(reference)]);
  for (const href of [
    "https://growth.openai.chatgpt.site/?token=private#draft",
    "https://localhost.example.com/?token=private#draft",
    "https://example.local/?token=private#draft",
  ]) {
    const expected = new URL(href);
    expected.search = "";
    expected.hash = "";
    assert.deepEqual(currentDataAppReference(new URL(href)), { sourceUrl: expected.toString() });
  }
  assert.deepEqual(currentDataAppReference(new URL("file:///workspace/actual/dist/index.html")), {
    root: "/workspace/actual",
    htmlPath: "/workspace/actual/dist/index.html",
    sourceUrl: "file:///workspace/actual/dist/index.html",
  });
  assert.deepEqual(currentDataAppReference(new URL("ftp://localhost/index.html")), {});
});

test("hosted Sites recover their packaged project identity without trusting URLs or foreign pages", (context) => {
  useSitesProjectMetadata(context, ["appgprj_123"]);
  for (const hostname of ["growth.openai.chatgpt.site", "growth.openai.chatgpt-team.site"]) {
    assert.deepEqual(
      currentDataAppReference(new URL(`https://${hostname}/_data/charts/abc?token=private#mark`)),
      { sourceUrl: `https://${hostname}/`, projectId: "appgprj_123" },
    );
  }
  for (const href of [
    "https://example.com/dashboard",
    "https://growth.chatgpt-team.site.example.com/dashboard",
    "https://growth.notchatgpt-team.site/dashboard",
    "http://growth.openai.chatgpt-team.site/dashboard",
  ]) {
    assert.deepEqual(currentDataAppReference(new URL(href)), { sourceUrl: href });
  }
});

test("hosted Sites reject missing, duplicate, or malformed packaged project identities", (context) => {
  const originalDocument = globalThis.document;
  let entries = [];
  globalThis.document = {
    querySelectorAll: () => entries.map((content) => ({ getAttribute: () => content })),
  };
  context.after(() => {
    if (originalDocument === undefined) delete globalThis.document;
    else globalThis.document = originalDocument;
  });
  const location = new URL("https://growth.openai.chatgpt.site/");
  for (entries of [[], ["appgprj_123", "appgprj_123"], ["../private"], ["site id"], [""]]) {
    assert.deepEqual(currentDataAppReference(location), { sourceUrl: location.href });
  }
});

test("local file URLs retain decoded POSIX and Windows paths in native task links", () => {
  for (const [href, root] of [
    ["file:///C:/work/report/dist/index.html", "C:/work/report"],
    ["file://localhost/d:/work/Quarterly%20report%20%E2%82%AC/dist/index.html?preview=1#draft", "d:/work/Quarterly report €"],
    ["file:///Users/example/Quarterly%20report%20%E2%82%AC/dist/index.html", "/Users/example/Quarterly report €"],
    ["file://localhost/Users/example/report/dist/index.html", "/Users/example/report"],
    ["file:///C:/work/100%25%20report/dist/index.html", "C:/work/100% report"],
  ]) {
    const location = new URL(href);
    const source = new URL(href);
    source.search = "";
    source.hash = "";
    assert.deepEqual(currentDataAppReference(location), {
      root, htmlPath: `${root}/dist/index.html`, sourceUrl: source.href,
    });
    const task = codexDataAppNewTaskUrl("Investigate", undefined, location, "Mozilla/5.0");
    assert.equal(`${task.protocol}//${task.host}${task.pathname}`, "codex://new");
    assert.equal(task.searchParams.get("path"), root);
  }
});

test("filesystem-root reports retain absolute working directories in native task links", () => {
  for (const [href, root, htmlPath] of [
    ["file:///C:/dist/index.html", "C:/", "C:/dist/index.html"],
    ["file:///d:/index.html", "d:/", "d:/index.html"],
    ["file:///dist/index.html", "/", "/dist/index.html"],
    ["file:///index.html", "/", "/index.html"],
  ]) {
    const location = new URL(href);
    assert.deepEqual(currentDataAppReference(location), { root, htmlPath, sourceUrl: href });
    const task = codexDataAppNewTaskUrl("Investigate", undefined, location, "Mozilla/5.0");
    assert.equal(`${task.protocol}//${task.host}${task.pathname}`, "codex://new");
    assert.equal(task.searchParams.get("path"), root);
  }
});

test("network and unsafe file URLs cannot become local report identities or task paths", (context) => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  useLocalThreadMetadata(context, threadId);
  for (const href of [
    "file://server/share/report/dist/index.html",
    "file://127.0.0.1/share/report/dist/index.html",
    "file://report.localhost/share/report/dist/index.html",
    "file:////server/share/report/dist/index.html",
    "file:///%2Fserver/share/report/dist/index.html",
    "file:///C:/work%2Freport/dist/index.html",
    "file:///C:/work%5Creport/dist/index.html",
    "file:///C:/work/report%00/dist/index.html",
    "file:///C:/work/report%7F/dist/index.html",
    "file:///C:/work/report%FF/dist/index.html",
  ]) {
    const location = new URL(`${href}#codexThreadId=${threadId}`);
    assert.deepEqual(currentDataAppReference(location, "C:/other/report"), {}, href);
    const task = codexDataAppNewTaskUrl("Investigate", { root: "C:/other/report" }, location, "CodexBrowser");
    assert.deepEqual([...task.searchParams.keys()], ["prompt"], href);
    const ordinary = chatGPTPromptUrl("Investigate", location, "CodexBrowser");
    assert.equal(`${ordinary.protocol}//${ordinary.host}${ordinary.pathname}`, "codex://threads/new", href);
    assert.equal(codexDataAppNewTaskUrl("Investigate", undefined, location, "Mozilla/5.0").href,
      "https://chatgpt.com/?q=Investigate", href);
  }
});

test("development-preview publication handoffs retain the exact project without leaking internal URLs", () => {
  const threadId = "550e8400-e29b-41d4-a716-446655440000";
  const location = new URL(`http://terminal.local:4173/?token=private#codexThreadId=${threadId}`);
  const dataAppReference = currentDataAppReference(location, "/workspace/revenue-dashboard");
  const action = new URL(
    dataAppActionHref(
      "sites",
      {
        surface: "dashboard",
        title: "Revenue overview",
        dataAppReference,
      },
      location,
    ),
  );
  const prompt = action.searchParams.get("prompt");

  assert.equal(`${action.protocol}//${action.host}${action.pathname}`, `codex://threads/${threadId}`);
  assert.match(prompt, /Dashboard project directory: \/workspace\/revenue-dashboard/u);
  assert.match(prompt, /Dashboard HTML file: \/workspace\/revenue-dashboard\/dist\/index\.html/u);
  assert.doesNotMatch(prompt, /terminal\.local|token=private|codexThreadId|550e8400/u);
});

test("published actions bypass host follow-ups and start a private new conversation", async (context) => {
  const originalWindow = globalThis.window;
  const originalDocument = globalThis.document;
  const links = [];
  let hostMessages = 0;
  globalThis.window = {
    location: new URL("https://growth.openai.chatgpt.site/"),
    openai: {
      sendFollowUpMessage: () => {
        hostMessages += 1;
      },
    },
  };
  globalThis.document = {
    createElement: () => ({
      style: {},
      click() {
        links.push(this.href);
      },
      remove() {},
    }),
    body: { appendChild() {} },
  };
  context.after(() => {
    if (originalWindow === undefined) delete globalThis.window;
    else globalThis.window = originalWindow;
    if (originalDocument === undefined) delete globalThis.document;
    else globalThis.document = originalDocument;
  });

  assert.equal(
    await submitDataAppAction("refresh", {
      surface: "dashboard",
      title: "Revenue overview",
      presentation: {
        filters: { customer: "Confidential Enterprise" },
        assumptions: { renewal: "Restricted forecast" },
        rows: [{ account: "Private customer" }],
      },
    }),
    true,
  );

  assert.equal(hostMessages, 0);
  assert.equal(links.length, 1);
  const action = new URL(links[0]);
  assert.equal(`${action.origin}${action.pathname}`, "https://chatgpt.com/");
  assert.match(action.searchParams.get("q"), /Published dashboard URL: https:\/\/growth\.openai\.chatgpt\.site\//u);
  assert.doesNotMatch(action.toString(), /Confidential|Restricted|Private\+customer/u);
});
