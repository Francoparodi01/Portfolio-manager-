import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { dataAppActionHref, dataAppActionRequest, submitDataAppAction } from "../src/data-app-actions.js";
import { chatGPTPromptUrl, codexDataAppNewTaskUrl, currentDataAppReference, dataAppPromptTarget } from "../src/runtime-environment.js";

const originThread = "550e8400-e29b-41d4-a716-446655440000";
const local = new URL(`file:///private/tmp/report/dist/index.html#codexThreadId=${originThread}`);
const reference = { root: "/private/tmp/report", htmlPath: "/private/tmp/report/dist/index.html" };

function context(extra = {}) {
  return {
    snapshot: { surface: "report", id: "test-report", queries: {
      evidence: { rows: [{ private: "PRIVATE_ROW" }], source: { sql: "PRIVATE_SQL", url: "https://example.com/evidence" } },
    } },
    canEdit: true,
    dataAppReference: reference,
    followUp: { id: "question", narrativeId: "question:body", text: "What changed?", queryId: "evidence" },
    ...extra,
  };
}

function replaceGlobal(t, key, value) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, key);
  Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
  t.after(() => {
    if (previous) Object.defineProperty(globalThis, key, previous);
    else delete globalThis[key];
  });
}

test("reader investigations prefill a new local task without reading or sending to the originating task", (t) => {
  let sends = 0;
  replaceGlobal(t, "document", { querySelector() { throw new Error("Must not read origin metadata"); } });
  replaceGlobal(t, "window", { location: local, openai: { sendFollowUpMessage() { sends += 1; }, sendMessage() { sends += 1; } } });
  const href = new URL(dataAppActionHref("report-investigate", context(), local));
  assert.equal(`${href.protocol}//${href.host}${href.pathname}`, "codex://new");
  assert.equal(href.searchParams.get("path"), reference.root);
  assert.match(href.searchParams.get("prompt"), /What changed\?/u);
  assert.deepEqual([...href.searchParams.keys()], ["prompt", "path"]);
  assert.doesNotMatch(href.toString(), new RegExp(`${originThread}|codexThreadId|PRIVATE_|auto.?send|auto.?submit`, "iu"));
  assert.equal(dataAppPromptTarget(href), "_self");
  assert.equal(sends, 0);
});

test("prepare links preserve their specific draft intent and can never use the submit transport", async (t) => {
  let sends = 0;
  replaceGlobal(t, "window", { location: local, openai: { sendFollowUpMessage() { sends += 1; }, sendMessage() { sends += 1; } } });
  replaceGlobal(t, "document", { querySelector() { throw new Error("Must not read origin metadata"); },
    createElement() { throw new Error("Must not launch a submitted prepare request"); } });
  const draft = context({ canEdit: false, followUp: { ...context().followUp, text: "Current edited recommendation",
    intent: "prepare", deliverable: "recovery flow" } });
  const href = new URL(dataAppActionHref("report-investigate", draft, local));
  assert.equal(`${href.protocol}//${href.host}${href.pathname}`, "codex://new");
  const prompt = href.searchParams.get("prompt");
  const payload = JSON.parse(prompt.slice(prompt.indexOf("\n\n{") + 2));
  assert.equal(payload.text, "Current edited recommendation");
  assert.deepEqual(payload.request, { intent: "prepare", deliverable: "recovery flow" });
  assert.doesNotMatch(prompt, /PRIVATE_/u);
  await assert.rejects(submitDataAppAction("report-investigate", draft), /unsent new-task link/u);
  await assert.rejects(submitDataAppAction("word", draft), /unsent new-task link/u,
    "Passing prepare context to another submitted action must not silently escalate its scope");
  assert.equal(sends, 0);
});

test("Windows preview investigations and draft links retain the exact local project", () => {
  const preview = new URL("http://localhost:5173/?token=private");
  for (const root of ["C:\\work\\quarterly report", "d:/work/quarterly report"]) {
    const dataAppReference = currentDataAppReference(preview, root);
    for (const intent of ["investigate", "prepare"]) {
      const followUp = { ...context().followUp, intent, ...(intent === "prepare" ? { deliverable: "recovery flow" } : {}) };
      const href = new URL(dataAppActionHref("report-investigate", context({ canEdit: false, dataAppReference, followUp }), preview));
      assert.equal(`${href.protocol}//${href.host}${href.pathname}`, "codex://new");
      assert.equal(href.searchParams.get("path"), root);
      const prompt = href.searchParams.get("prompt");
      const report = JSON.parse(prompt.slice(prompt.indexOf("\n\n{") + 2)).report;
      assert.equal(report.projectDirectory, root);
      assert.equal(report.htmlPath, `${root}/dist/index.html`);
    }
  }
});

test("new-task links keep only safe hosted or local context and retain ordinary web routing", () => {
  const hosted = new URL(`https://viewer:secret@report.openai.chatgpt.site/_data/components/question?token=private#codexThreadId=${originThread}`);
  const desktop = codexDataAppNewTaskUrl("Investigate", reference, hosted, "ChatGPTBrowser Chrome/140");
  assert.equal(`${desktop.protocol}//${desktop.host}${desktop.pathname}`, "codex://new");
  assert.equal(desktop.searchParams.get("browserUrl"), "https://report.openai.chatgpt.site/");
  assert.deepEqual([...desktop.searchParams.keys()], ["prompt", "browserUrl"]);
  assert.doesNotMatch(desktop.toString(), /viewer|secret|private|codexThreadId|550e8400/u);
  const web = codexDataAppNewTaskUrl("Investigate", reference, hosted, "Mozilla/5.0");
  assert.equal(web.href, "https://chatgpt.com/?q=Investigate");
  assert.equal(dataAppPromptTarget(web), "_blank");
  for (const root of ["relative/path", "C:work\\report", "\\work\\report", "//network/path", "\\\\server\\share",
    "\\\\?\\C:\\work\\report", "/\\server/share", "/tmp/report\nsecret", "C:\\work\\report\nsecret", "C:/work/report\u0000",
    "/tmp/report\u007f", `C:/${"x".repeat(2046)}`, "x".repeat(2050)]) {
    assert.equal(codexDataAppNewTaskUrl("Investigate", { root }, local).searchParams.has("path"), false);
  }
  const unsafe = codexDataAppNewTaskUrl("Investigate", reference,
    new URL("https://report.openai.chatgpt.site/tokens/private-value"), "ChatGPTBrowser");
  assert.equal(unsafe.searchParams.has("browserUrl"), false);
  assert.throws(() => codexDataAppNewTaskUrl(" ", reference, local), /requires a prompt/u);
});

test("report task links use the shared current and legacy Codex browser detection", () => {
  const hosted = new URL("https://report.openai.chatgpt.site/_data/components/question?token=private");
  for (const userAgent of ["CodexBrowser", "CodexBrowser/1.0", "CodexBrowser Chrome/140", "ChatGPTBrowser/1.0"]) {
    const task = codexDataAppNewTaskUrl("Investigate", reference, hosted, userAgent);
    assert.equal(`${task.protocol}//${task.host}${task.pathname}`, "codex://new");
    assert.equal(task.searchParams.get("browserUrl"), "https://report.openai.chatgpt.site/");
    assert.deepEqual([...task.searchParams.keys()], ["prompt", "browserUrl"]);
    assert.equal(dataAppPromptTarget(task), "_self");
  }
  for (const userAgent of ["Mozilla/5.0", "CodexBrowserFake", "ChatGPTBrowserFake", "", null, 42]) {
    const task = codexDataAppNewTaskUrl("Investigate", reference, hosted, userAgent);
    assert.equal(task.href, "https://chatgpt.com/?q=Investigate");
    assert.equal(dataAppPromptTarget(task), "_blank");
  }
});

test("new reader href keeps permission checks while existing editor transport remains host-first", async (t) => {
  const editorOnly = { ...context().followUp, editorOnly: true };
  assert.throws(() => dataAppActionHref("report-investigate", context({ canEdit: false, followUp: editorOnly }), local), /Editing permission/u);
  for (const action of ["report-investigate-update", "report-correct"]) {
    assert.throws(() => dataAppActionHref(action, context({ canEdit: false }), local), /Editing permission/u);
  }
  let sends = 0;
  replaceGlobal(t, "window", { location: local, openai: { async sendFollowUpMessage() { sends += 1; return { isError: true }; } } });
  replaceGlobal(t, "document", {
    querySelector: () => ({ getAttribute: () => originThread }),
    createElement: () => { throw new Error("A rejected host send must not retry"); },
  });
  assert.equal(await submitDataAppAction("report-correct", context()), false);
  assert.equal(sends, 1);
  const ordinary = chatGPTPromptUrl("Existing action", local);
  assert.equal(`${ordinary.protocol}//${ordinary.host}${ordinary.pathname}`, `codex://threads/${originThread}`);
  const update = new URL(dataAppActionHref("report-investigate-update", context(), local));
  assert.equal(`${update.protocol}//${update.host}${update.pathname}`, `codex://threads/${originThread}`);
});

test("shell href derives current saved narrative and authority without invoking the submit path", async () => {
  const shell = await readFile(new URL("../src/DataAppShell.jsx", import.meta.url), "utf8");
  const helper = shell.match(/function reportFollowUpHref\(followUp\) \{[\s\S]*?\n  \}/u)?.[0];
  assert.ok(helper);
  assert.match(helper, /narrativeEdits\[followUp\?\.narrativeId\] \?\? followUp\?\.text/u);
  assert.match(helper, /const current = \{ \.\.\.followUp,/u,
    "The saved text override must retain the separate intent and deliverable");
  assert.match(helper, /dataAppActionHref\("report-investigate", actionContext\(\{ followUp: current \}\)\)/u);
  assert.match(helper, /return \{ href, target: dataAppPromptTarget\(href\) \}/u);
  assert.doesNotMatch(helper, /runAction|submitDataAppAction|sendPromptToHost/u);
});

test("static report exports include reader-visible disclosure contents, not controls or private notes", () => {
  for (const action of ["word", "powerpoint", "google-docs", "google-slides"]) {
    const report = dataAppActionRequest(action, { ...context(), surface: "report" }).prompt;
    assert.match(report, /reader-visible collapsed evidence, methods, and the full follow-up text/u);
    assert.match(report, /Omit action controls, editor-only content, and hidden content/u);
    assert.match(report, /not transient disclosure open\/closed state/u);
    const dashboard = dataAppActionRequest(action, { ...context(), surface: "dashboard" }).prompt;
    assert.doesNotMatch(dashboard, /reader-visible collapsed evidence/u);
  }
  for (const action of ["html", "jupyter-notebook", "sites"]) {
    assert.doesNotMatch(dataAppActionRequest(action, { ...context(), surface: "report" }).prompt, /reader-visible collapsed evidence/u);
  }
});
