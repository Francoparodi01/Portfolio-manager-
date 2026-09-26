import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { componentPermalinkId, componentPermalinkShortId } from "../src/chart-permalink.js";
import { semanticColor } from "../src/charting/chart-theme.js";
import { createDataAppWorker, validatePresentation as validateWorkerPresentation } from "../src/data-app-worker.js";
import {
  mergePresentationChanges, normalizeAppearance, normalizePresentation, presentationStorageKey,
  presentationVersion, readLocalPresentation, readViewerAppearance, validatePresentation,
  writeLocalPresentation, writeViewerAppearance,
  editHistoryValue, recordPresentationEdit,
} from "../src/presentation-state.js";

test("edit history restores deletion/layout changes, branches after undo and excludes reviewed state", () => {
  const original = editHistoryValue({ title: "Test", hiddenBlocks: [], queries: { secret: 1 }, filters: { plan: "A" } });
  const deleted = editHistoryValue({ title: "Test", hiddenBlocks: ["kpi"] });
  let history = { entries: [original], index: 0 };
  history = recordPresentationEdit(history, deleted);
  assert.deepEqual(JSON.parse(history.entries[0]), { title: "Test", hiddenBlocks: [] });
  assert.equal(recordPresentationEdit(history, deleted), history);
  const undone = { ...history, index: 0 };
  assert.equal(JSON.parse(undone.entries[undone.index]).hiddenBlocks.length, 0);
  const moved = editHistoryValue({ title: "Test", blockLayouts: { canvas: { order: ["b", "a"] } } });
  history = recordPresentationEdit(undone, moved);
  assert.deepEqual(history.entries, [original, moved], "New edits discard the redo branch");
  for (let i = 0; i < 120; i++) history = recordPresentationEdit(history, editHistoryValue({ title: `Edit ${i}` }));
  assert.equal(history.entries.length, 101);
  assert.deepEqual(recordPresentationEdit(history, original, false), { entries: [original], index: 0 });
});

function memoryStorage() {
  const values = new Map();
  return { getItem: (key) => values.get(key) ?? null, setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key), values };
}

let presentationHookHarnessIndex = 0;

async function presentationPersistenceHarness(initialPresentation, { deferResponses = false } = {}) {
  const binding = `__dataAppPresentationHookHarness${process.pid}_${presentationHookHarnessIndex += 1}`;
  const hooks = [];
  const effects = [];
  const timers = new Map();
  const requests = [];
  const pendingResponses = [];
  const snapshot = { id: `presentation-hook-${presentationHookHarnessIndex}` };
  const server = { presentation: structuredClone(initialPresentation), revision: 0 };
  let cursor = 0;
  let nextTimer = 0;
  let pendingEffects = [];
  let deferred = deferResponses;

  function response(status, value) {
    return { status, ok: status >= 200 && status < 300, json: async () => structuredClone(value) };
  }

  globalThis[binding] = {
    useState(initial) {
      const index = cursor++;
      if (!Object.hasOwn(hooks, index)) hooks[index] = typeof initial === "function" ? initial() : initial;
      return [hooks[index], (next) => {
        hooks[index] = typeof next === "function" ? next(hooks[index]) : next;
      }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!Object.hasOwn(hooks, index)) hooks[index] = { current: initial };
      return hooks[index];
    },
    useEffect(effect, dependencies) {
      const index = cursor++;
      if (!effects[index] || dependencies.some((dependency, position) =>
        !Object.is(dependency, effects[index].dependencies[position]))) {
        pendingEffects.push({ index, effect, dependencies });
      }
    },
    setTimeout(callback, delay) {
      assert.equal(delay, 300);
      const id = ++nextTimer;
      timers.set(id, callback);
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    async fetch(endpoint, options) {
      assert.equal(endpoint, "/api/presentation");
      const request = JSON.parse(options.body);
      requests.push(structuredClone(request));
      if (deferred) {
        return new Promise((resolve, reject) => pendingResponses.push({ request, resolve, reject }));
      }
      if (request.revision !== server.revision) {
        return response(409, { presentation: server.presentation, revision: server.revision });
      }
      const existingVerification = server.presentation.verification;
      server.presentation = structuredClone(request.presentation);
      if (request.verificationAction === "verify") {
        server.presentation.verification = existingVerification ?? {
          verifiedBy: "server-owner@example.com", verifiedAt: "2026-08-17T19:34:56.789Z",
        };
      } else if (request.verificationAction === "remove") {
        delete server.presentation.verification;
      }
      server.revision += 1;
      return response(200, { presentation: server.presentation, revision: server.revision });
    },
  };

  const source = (await readFile(new URL("../src/use-presentation.js", import.meta.url), "utf8"))
    .replace('import { useEffect, useLayoutEffect, useRef, useState } from "react";',
      `const { useEffect, useRef, useState, setTimeout, clearTimeout, fetch } = globalThis[${JSON.stringify(binding)}];`)
    .replace('import { editHistoryValue, recordPresentationEdit, mergePresentationChanges, normalizePresentation, writeLocalPresentation } from "./presentation-state.js";',
      `import { editHistoryValue, recordPresentationEdit, mergePresentationChanges, normalizePresentation, writeLocalPresentation } from "${
        new URL("../src/presentation-state.js", import.meta.url).href
      }";`);
  const directory = await mkdtemp(join(tmpdir(), "data-app-presentation-hook-"));
  const modulePath = join(directory, "use-presentation.mjs");
  let usePresentationPersistence;
  try {
    await writeFile(modulePath, source);
    ({ usePresentationPersistence } = await import(pathToFileURL(modulePath).href));
  } catch (error) {
    delete globalThis[binding];
    throw error;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }

  return {
    requests,
    server,
    timers,
    pendingResponses,
    render(presentation, options = {}) {
      cursor = 0;
      pendingEffects = [];
      usePresentationPersistence({ snapshot, hosted: true, presentation, initialPresentation, ...options });
      for (const { index, effect, dependencies } of pendingEffects) {
        effects[index]?.cleanup?.();
        effects[index] = { dependencies, cleanup: effect() };
      }
    },
    async flush() {
      const scheduled = [...timers.values()];
      timers.clear();
      await Promise.all(scheduled.map((callback) => callback()));
    },
    start() {
      const scheduled = [...timers.values()];
      timers.clear();
      assert.equal(scheduled.length, 1);
      return scheduled[0]();
    },
    respond(index, { status = 200, presentation, revision, error } = {}) {
      const pending = pendingResponses[index];
      assert.ok(pending, `Deferred response ${index} does not exist`);
      if (status >= 200 && status < 300 && revision >= server.revision) {
        server.presentation = structuredClone(presentation);
        server.revision = revision;
      }
      pending.resolve(response(status, status >= 200 && status < 300
        ? { presentation, revision } : { error: error ?? "A stale request failed." }));
    },
    resumeResponses() {
      deferred = false;
    },
    status() {
      return hooks[0];
    },
    dispose() {
      for (const effect of effects) effect?.cleanup?.();
      delete globalThis[binding];
    },
  };
}

test("dashboard appearance validates author defaults and keeps viewer overrides local", () => {
  const snapshot = { id: "appearance-dashboard" };
  const storage = memoryStorage();
  assert.equal(normalizeAppearance("unknown"), "system");
  assert.deepEqual(validatePresentation({ appearance: "dark" }), { appearance: "dark" });
  assert.throws(() => validatePresentation({ appearance: "unknown" }), /Appearance must be/);
  assert.equal(writeViewerAppearance(snapshot, "light", storage), true);
  assert.equal(readViewerAppearance(snapshot, storage), "light");
  assert.equal(readViewerAppearance({ id: "other-dashboard" }, storage), "");
  assert.equal(writeViewerAppearance(snapshot, "", storage), true);
  assert.equal(readViewerAppearance(snapshot, storage), "");
});

test("semantic chart colors preserve dimension identity across reviewed queries and reordering", () => {
  assert.equal(semanticColor({ dimension: "plan", value: "Pro" }),
    semanticColor({ dimension: "plan", value: "Pro", index: 7 }));
  assert.notEqual(semanticColor({ dimension: "plan", value: "Pro" }),
    semanticColor({ dimension: "product", value: "Pro" }));
  assert.equal(semanticColor({ field: "revenue", explicitColor: "#123456" }), "#123456");
});

test("presentation rejects removed chart annotations rather than persisting nested reviewed data", () => {
  assert.throws(() => validatePresentation({ chartOverrides: {
    trend: { referenceLines: [{ axis: "y", value: 1, rows: [{ amount: 1 }] }] },
  } }), /annotations are not supported/);
});

test("presentation preserves bounded authored annotation references without accepting data payloads", () => {
  const spec = { type: "line", x: "date", y: "value", annotations: [
    { id: "target", kind: "benchmark", label: "Reviewed target", field: "target", measure: "value" },
  ] };
  assert.deepEqual(validatePresentation({ chartOverrides: { trend: spec } }).chartOverrides.trend, spec);
  for (const extra of [{ rows: [{ value: 2 }] }, { value: 42 }, { sql: "select 1" }, { source: { secret: true } }]) {
    assert.throws(() => validatePresentation({ chartOverrides: { trend: {
      ...spec, annotations: [{ ...spec.annotations[0], ...extra }],
    } } }), /unsupported fields/u);
  }
});

test("annotation visibility is an optional boolean and never bypasses annotation validation", () => {
  const spec = { type: "line", x: "date", y: "value", annotations: [
    { id: "target", kind: "benchmark", label: "Reviewed target", field: "target", measure: "value" },
  ] };
  const validate = (chart) => validatePresentation({ chartOverrides: { trend: chart } }).chartOverrides.trend;
  assert.equal(Object.hasOwn(validate(spec), "showAnnotations"), false);
  for (const showAnnotations of [false, true]) {
    assert.deepEqual(validate({ ...spec, showAnnotations }), { ...spec, showAnnotations });
  }
  for (const showAnnotations of [null, undefined, "false", "true", 0, 1, [], {}]) {
    assert.throws(() => validate({ ...spec, showAnnotations }), /boolean/i);
  }
  assert.throws(() => validate({ ...spec, showAnnotations: false,
    annotations: [{ ...spec.annotations[0], rows: [{ value: 42 }] }],
  }), /unsupported fields/u);
});

test("local annotation visibility round trips without changing reviewed data or authored annotations", () => {
  const snapshot = { id: "annotation-visibility", queries: { reviewed: { rows: [{ value: 42, target: 50 }] } } };
  const before = structuredClone(snapshot);
  const spec = { type: "line", x: "date", y: "value", annotations: [
    { id: "target", kind: "benchmark", label: "Reviewed target", field: "target", measure: "value" },
  ] };
  const storage = memoryStorage();
  for (const setting of [{}, { showAnnotations: false }, { showAnnotations: true }]) {
    const presentation = { chartOverrides: { trend: { ...spec, ...setting } } };
    assert.equal(writeLocalPresentation(snapshot, presentation, storage), true);
    assert.deepEqual(readLocalPresentation(snapshot, storage), presentation);
    assert.deepEqual(readLocalPresentation(snapshot, storage).chartOverrides.trend.annotations, spec.annotations);
  }
  assert.deepEqual(snapshot, before);
});

test("presentation text edits and component titles accept only bounded authored strings", () => {
  for (const key of ["componentTitles", "textEdits"]) {
    assert.deepEqual(validatePresentation({ [key]: { "component:label": "Reviewed label" } }), {
      [key]: { "component:label": "Reviewed label" },
    });
    for (const invalid of [{ value: { rows: [{ amount: 42 }] } }, { value: 42 },
      { value: "   " }, { value: "x".repeat(20_001) }, { " ": "Visible copy" },
      { ["x".repeat(201)]: "Visible copy" }]) {
      assert.throws(() => validatePresentation({ [key]: invalid }), /bounded text values/u);
    }
    assert.deepEqual(normalizePresentation({ [key]: {
      keep: "Visible copy", nested: { rows: [{ amount: 42 }] }, empty: " ",
    } }), { [key]: { keep: "Visible copy" } });
  }
});

test("cleared narrative text survives validation and local storage without allowing empty titles", () => {
  const presentation = { textEdits: { "finding:body": "" } };
  assert.deepEqual(validatePresentation(presentation), presentation);
  const snapshot = { id: "cleared-narrative" };
  const storage = memoryStorage();
  assert.equal(writeLocalPresentation(snapshot, presentation, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), presentation);
  assert.throws(() => validatePresentation({ componentTitles: { finding: "" } }), /nonempty titles/u);
  assert.deepEqual(normalizePresentation({ componentTitles: { finding: "" } }), { componentTitles: {} });
});

test("dashboard verification accepts only a bounded creator email and canonical UTC timestamp", () => {
  const verification = { verifiedBy: "creator@example.com", verifiedAt: "2026-08-17T19:34:56.789Z" };
  assert.deepEqual(validatePresentation({ verification }), { verification });
  assert.deepEqual(normalizePresentation({ verification: {
    verifiedBy: "  CREATOR@EXAMPLE.COM  ", verifiedAt: verification.verifiedAt,
  } }), { verification });

  for (const invalid of [
    null, "verified", true, [], {},
    { verifiedBy: verification.verifiedBy },
    { verifiedAt: verification.verifiedAt },
    { ...verification, rows: [{ amount: 42 }] },
    { ...verification, provenance: "fabricated" },
    { ...verification, verifiedBy: "" },
    { ...verification, verifiedBy: "   " },
    { ...verification, verifiedBy: "creator" },
    { ...verification, verifiedBy: "creator@example" },
    { ...verification, verifiedBy: `${"x".repeat(245)}@example.com` },
    { ...verification, verifiedBy: 42 },
    { ...verification, verifiedAt: "" },
    { ...verification, verifiedAt: "2026-08-17" },
    { ...verification, verifiedAt: "2026-08-17T19:34:56Z" },
    { ...verification, verifiedAt: "2026-08-17T19:34:56.789+00:00" },
    { ...verification, verifiedAt: "2026-02-30T19:34:56.789Z" },
    { ...verification, verifiedAt: 1_787_017_896_789 },
  ]) {
    assert.throws(() => validatePresentation({ verification: invalid }), /Verification must contain/u);
    assert.deepEqual(normalizePresentation({ verification: invalid }), {});
  }
});

test("verified dashboard metadata persists and merges as one protected presentation field", () => {
  const snapshot = { id: "verified-dashboard" };
  const verification = { verifiedBy: "creator@example.com", verifiedAt: "2026-08-17T19:34:56.789Z" };
  const original = { title: "Reviewed dashboard", verification };
  const storage = memoryStorage();

  assert.equal(writeLocalPresentation(snapshot, original, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), original);
  assert.deepEqual(mergePresentationChanges(original, { ...original, title: "Updated dashboard" }, original), {
    title: "Updated dashboard", verification,
  });
  assert.deepEqual(mergePresentationChanges(original, { title: original.title }, original), {
    title: original.title,
  });
  assert.deepEqual(mergePresentationChanges({ title: original.title }, original, {
    title: "Concurrent update",
  }), { title: "Concurrent update", verification });
});

test("debounced hosted saves retain rapid verification changes and unrelated creator edits", async () => {
  const verification = { verifiedBy: "creator@example.com", verifiedAt: "2026-08-17T19:34:56.789Z" };
  for (const { label, initial, toggled, edited } of [
    {
      label: "verify",
      initial: { title: "Reviewed dashboard" },
      toggled: { title: "Reviewed dashboard", verification },
      edited: { title: "Updated dashboard", verification },
    },
    {
      label: "unverify",
      initial: { title: "Reviewed dashboard", verification },
      toggled: { title: "Reviewed dashboard" },
      edited: { title: "Updated dashboard" },
    },
  ]) {
    const harness = await presentationPersistenceHarness(initial);
    try {
      harness.render(initial);
      harness.render(toggled);
      assert.equal(harness.timers.size, 1, `${label}: toggling should schedule one hosted save`);
      harness.render(edited);
      assert.equal(harness.timers.size, 1, `${label}: a rapid edit should replace the earlier debounce timer`);
      await harness.flush();

      assert.equal(harness.requests.length, 1, `${label}: both creator changes should use one hosted write`);
      assert.deepEqual(harness.requests[0], { presentation: edited, revision: 0 },
        `${label}: the later edit must not discard a verification toggle from its canceled timer`);
      assert.deepEqual(harness.server.presentation, edited);
    } finally {
      harness.dispose();
    }
  }
});

test("debounced verification writes preserve conflict merges and later acknowledged creator edits", async () => {
  const initial = { title: "Reviewed dashboard", theme: "original" };
  const verification = { verifiedBy: "creator@example.com", verifiedAt: "2026-08-17T19:34:56.789Z" };
  const toggled = { ...initial, verification };
  const edited = { ...toggled, title: "Updated dashboard" };
  const harness = await presentationPersistenceHarness(initial);
  try {
    harness.render(initial);
    harness.render(toggled);
    harness.render(edited);
    harness.server.presentation = { ...initial, theme: "scientific-blue" };
    harness.server.revision = 1;
    await harness.flush();

    assert.equal(harness.requests.length, 2, "A stale verification write must retry its server conflict once");
    assert.deepEqual(harness.requests.map(({ revision }) => revision), [0, 1]);
    assert.deepEqual(harness.server.presentation, {
      title: "Updated dashboard", theme: "scientific-blue", verification,
    }, "A debounced verification and title change must retain the concurrent server-side theme");

    const subsequent = { ...edited, componentTitles: { revenue: "Recognized revenue" } };
    harness.render(subsequent);
    await harness.flush();
    assert.deepEqual(harness.server.presentation, {
      title: "Updated dashboard", theme: "scientific-blue", verification,
      componentTitles: { revenue: "Recognized revenue" },
    }, "Later acknowledged edits must remain rebased on the earlier conflict merge");
  } finally {
    harness.dispose();
  }
});

test("verification intents expose only server-acknowledged badge metadata and preserve immediate creator edits", async () => {
  const initial = { title: "Reviewed dashboard" };
  const acknowledgements = [];
  const onAcknowledged = (presentation, action) => acknowledgements.push({
    presentation: structuredClone(presentation), action,
  });
  const harness = await presentationPersistenceHarness(initial);
  try {
    harness.render(initial, { onAcknowledged });
    harness.render(initial, { verificationAction: "verify", onAcknowledged });
    assert.equal(harness.timers.size, 1,
      "A server-side verification intent must schedule a save without fabricated local badge metadata");

    const edited = { title: "Updated dashboard" };
    harness.render(edited, { verificationAction: "verify", onAcknowledged });
    assert.equal(harness.timers.size, 1,
      "An immediate creator edit must preserve the pending verification action in the same debounced save");
    assert.equal(acknowledgements.length, 0,
      "The creator must not see verified metadata before the trusted Worker acknowledges its stamp");
    await harness.flush();

    assert.deepEqual(harness.requests[0], {
      presentation: edited, revision: 0, verificationAction: "verify",
    }, "Verification requests must carry only an explicit action, never a client-fabricated identity");
    assert.deepEqual(acknowledgements, [{
      action: "verify", presentation: { ...edited, verification: {
        verifiedBy: "server-owner@example.com", verifiedAt: "2026-08-17T19:34:56.789Z",
      } },
    }], "The displayed badge must use only the Worker-returned authoritative verification record");

    const acknowledged = acknowledgements[0].presentation;
    harness.render(acknowledged, { onAcknowledged });
    assert.equal(harness.timers.size, 0,
      "Applying the server-stamped verification acknowledgement must not schedule a redundant save");
    harness.render(acknowledged, { verificationAction: "remove", onAcknowledged });
    const removalEdit = { ...acknowledged, title: "Unverified dashboard" };
    harness.render(removalEdit, { verificationAction: "remove", onAcknowledged });
    assert.equal(acknowledgements.length, 1,
      "A persisted badge must remain displayed until server-side removal is acknowledged");
    await harness.flush();

    assert.deepEqual(harness.requests[1], {
      presentation: removalEdit, revision: 1, verificationAction: "remove",
    }, "Removal must remain explicit and preserve creator edits while the old badge is still acknowledged");
    assert.deepEqual(acknowledgements[1], {
      action: "remove", presentation: { title: "Unverified dashboard" },
    }, "Only the trusted Worker may acknowledge that shared dashboard verification was removed");
  } finally {
    harness.dispose();
  }
});

test("overlapping stale autosave acknowledgements cannot roll back a newer conflict-preserving baseline", async () => {
  for (const staleResult of ["success", "failure"]) {
    const initial = { title: "Reviewed dashboard", theme: "original" };
    const older = { title: "Older title", theme: "original" };
    const newer = { title: "Newer title", theme: "local-brand" };
    const acknowledgements = [];
    const errors = [];
    const options = {
      onAcknowledged: (presentation) => acknowledgements.push(structuredClone(presentation)),
      onError: (error) => errors.push(error),
    };
    const harness = await presentationPersistenceHarness(initial, { deferResponses: true });
    try {
      harness.render(initial, options);
      harness.render(older, options);
      const olderSave = harness.start();
      harness.render(newer, options);
      const newerSave = harness.start();
      assert.equal(harness.pendingResponses.length, 2,
        `${staleResult}: both in-flight autosaves must overlap`);

      harness.respond(1, { presentation: newer, revision: 2 });
      await newerSave;
      assert.deepEqual(acknowledgements, [newer]);
      assert.equal(harness.status(), "saved");

      if (staleResult === "success") {
        harness.respond(0, { presentation: older, revision: 1 });
      } else {
        harness.respond(0, { status: 500, error: "A stale request failed." });
      }
      await olderSave;
      assert.deepEqual(acknowledgements, [newer],
        `A stale ${staleResult} must not replace a newer authoritative acknowledgement`);
      assert.deepEqual(errors, [], `A stale ${staleResult} must not surface an obsolete autosave error`);
      assert.equal(harness.status(), "saved",
        `A stale ${staleResult} must not roll back a successfully acknowledged save status`);

      harness.resumeResponses();
      harness.server.presentation = { ...newer, theme: "concurrent-remote-brand" };
      harness.server.revision = 3;
      const subsequent = { ...newer, title: "Latest title" };
      harness.render(subsequent, options);
      await harness.flush();

      assert.deepEqual(harness.requests.slice(-2).map(({ revision }) => revision), [2, 3],
        `A stale ${staleResult} must not roll back the next autosave's acknowledged revision`);
      assert.deepEqual(harness.server.presentation, {
        title: "Latest title", theme: "concurrent-remote-brand",
      }, `A stale ${staleResult} must not replay old local changes over a newer concurrent server edit`);
    } finally {
      harness.dispose();
    }
  }
});

test("dashboard descriptions remain optional while previously authored descriptions persist and reload", () => {
  const snapshot = { id: "optional-description-dashboard", title: "Product adoption" };
  const storage = memoryStorage();

  assert.deepEqual(normalizePresentation({ title: "Product adoption" }), { title: "Product adoption" });
  assert.equal(writeLocalPresentation(snapshot, { title: "Product adoption" }, storage), true);
  assert.equal(Object.hasOwn(readLocalPresentation(snapshot, storage), "description"), false);

  const authored = { title: "Product adoption", description: "Paid workspaces; excludes trials" };
  assert.equal(writeLocalPresentation(snapshot, authored, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), authored);
});

test("disabled chart zero baselines persist and reload without becoming enabled", () => {
  const snapshot = { id: "focused-waterfall-dashboard", title: "Product adoption" };
  const presentation = { chartOverrides: {
    "growth-drivers": { type: "waterfall", x: "driver", y: "change", startAtZero: false },
  } };
  const storage = memoryStorage();

  assert.equal(writeLocalPresentation(snapshot, presentation, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), presentation);
});

test("presentation stores only bounded presentation changes and isolates dashboard identities", () => {
  const snapshot = { id: "reviewed-dashboard", title: "Original dashboard title" };
  const presentation = {
    theme: "scientific-blue", title: "Edited title", description: "Edited description",
    hiddenBlocks: ["forecast", "forecast"], componentTitles: { revenue: "Recognized revenue" },
    textEdits: { "p:2": "An edited analysis." }, chartOverrides: { trend: { type: "bar" } },
    filters: { region: "West" }, assumptions: { conversion: 12 }, notes: "## Reviewed notes",
    tabs: [{ id: "overview", label: "Overview" }, { id: "details", label: "Details" }],
  };
  const storage = memoryStorage();
  assert.equal(writeLocalPresentation(snapshot, presentation, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), { ...presentation, hiddenBlocks: ["forecast"] });
  assert.deepEqual(readLocalPresentation({ ...snapshot, id: "another-dashboard" }, storage), {});
  const record = JSON.parse([...storage.values.values()][0]);
  assert.equal(record.version, presentationVersion);
  assert.equal(presentationStorageKey(snapshot, "/one") === presentationStorageKey(snapshot, "/two"), false);
  assert.throws(() => validatePresentation({ rows: [{ amount: 42 }] }), /Unsupported presentation fields/);
  assert.throws(() => validatePresentation({ chartOverrides: { trend: { type: "line", rows: [{ amount: 42 }] } } }),
    /reviewed data or provenance/);
  assert.throws(() => validatePresentation({ hiddenBlocks: [4] }), /Hidden blocks/);
  assert.throws(() => validatePresentation({ notes: "x".repeat(128_001) }), /size limit/);
  assert.throws(() => validatePresentation({ tabs: [{ id: "overview", label: "Overview" },
    { id: "overview", label: "Duplicate" }] }), /Tabs must be a bounded list/);
  assert.throws(() => validatePresentation({ tabs: [{ id: "overview", label: "" }] }),
    /Tabs must be a bounded list/);
  assert.deepEqual(normalizePresentation({ theme: "dark-pixel", queries: { secret: true } }), {
    theme: "dark-pixel",
  });
});

test("an explicit legacy title migrates the complete valid presentation without deleting its old record", () => {
  const legacy = { surface: "report", title: "Original report question?" };
  const snapshot = { surface: "report", id: "report-stable-123", title: "A clearer conclusion",
    legacyPresentationTitle: legacy.title };
  const presentation = {
    theme: "scientific-blue", appearance: "dark", title: "My edited title", description: "My description",
    hiddenBlocks: ["risk"], componentTitles: { revenue: "Recognized revenue" },
    textEdits: { "finding:body": "My complete edited recommendation." },
    chartOverrides: { trend: { type: "bar" } }, filters: { region: "West" },
    assumptions: { conversion: 12 }, notes: "## Notes",
    refreshSchedule: { frequency: "daily", time: "09:00" },
    tabs: [{ id: "overview", label: "Overview" }],
    blockLayouts: { story: { order: ["finding", "risk"] } },
  };
  const storage = memoryStorage();
  assert.equal(writeLocalPresentation(legacy, presentation, storage), true);
  assert.equal(writeViewerAppearance(legacy, "light", storage), true);
  const oldKey = presentationStorageKey(legacy);
  const stableKey = presentationStorageKey(snapshot);
  const originalRecord = storage.getItem(oldKey);

  assert.deepEqual(readLocalPresentation(snapshot, storage), presentation);
  assert.equal(storage.getItem(stableKey), originalRecord);
  assert.equal(storage.getItem(oldKey), originalRecord);
  assert.equal(readViewerAppearance(snapshot, storage), "light");
  assert.equal(storage.getItem(`${stableKey}:viewer-appearance`), "light");
  assert.equal(storage.getItem(`${oldKey}:viewer-appearance`), "light");
  assert.deepEqual(readLocalPresentation({ ...snapshot, title: "Renamed again" }, storage), presentation);
  assert.equal(presentationStorageKey({ ...snapshot, title: "Renamed again" }), stableKey);
});

test("stable presentation and appearance records win independently, including explicit resets and corrupt records", () => {
  const legacy = { title: "Old report" };
  const snapshot = { id: "stable-report", title: "New report", legacyPresentationTitle: legacy.title };
  const storage = memoryStorage();
  writeLocalPresentation(legacy, { title: "Legacy title", textEdits: { body: "Legacy text" } }, storage);
  writeViewerAppearance(legacy, "dark", storage);
  writeLocalPresentation(snapshot, { title: "Current title" }, storage);
  assert.deepEqual(readLocalPresentation(snapshot, storage), { title: "Current title" });
  assert.equal(readViewerAppearance(snapshot, storage), "dark");
  assert.equal(writeViewerAppearance(snapshot, "", storage), true);
  assert.equal(readViewerAppearance(snapshot, storage), "");
  assert.equal(storage.getItem(`${presentationStorageKey(snapshot)}:viewer-appearance`), "");
  assert.equal(storage.getItem(`${presentationStorageKey(legacy)}:viewer-appearance`), "dark");

  const key = presentationStorageKey(snapshot);
  storage.setItem(key, "{corrupt current record");
  assert.deepEqual(readLocalPresentation(snapshot, storage), {});
  assert.equal(storage.getItem(key), "{corrupt current record");
  storage.setItem(`${key}:viewer-appearance`, "invalid-current-value");
  assert.equal(readViewerAppearance(snapshot, storage), "");
  assert.equal(storage.getItem(`${key}:viewer-appearance`), "invalid-current-value");
  writeLocalPresentation(snapshot, {}, storage);
  assert.deepEqual(readLocalPresentation(snapshot, storage), {});
});

test("legacy lookup requires one bounded exact alias and a safe stable ID", () => {
  const legacy = { title: "Old report" };
  const stored = memoryStorage();
  writeLocalPresentation(legacy, { title: "Legacy title" }, stored);
  const invalid = [
    { title: "New report", legacyPresentationTitle: legacy.title },
    { id: "stable-report", title: "New report" },
    ...["", " ", "../report", "report/id", "report:id", "x".repeat(201), 4]
      .map((id) => ({ id, title: "New report", legacyPresentationTitle: legacy.title })),
    ...["", " ", "x".repeat(301), "Old\nreport", "Old\u007freport", [legacy.title], 4]
      .map((legacyPresentationTitle) => ({ id: "stable-report", title: "New report", legacyPresentationTitle })),
  ];
  for (const snapshot of invalid) {
    const reads = [];
    const storage = { getItem(key) { reads.push(key); return stored.getItem(key); },
      setItem() { assert.fail("Invalid identity must not migrate"); } };
    assert.deepEqual(readLocalPresentation(snapshot, storage), {});
    assert.deepEqual(reads, [presentationStorageKey(snapshot)]);
  }
  const exactTitle = "  Original title with spaces  ";
  const storage = memoryStorage();
  writeLocalPresentation({ title: exactTitle }, { title: "Keep the exact old key" }, storage);
  assert.deepEqual(readLocalPresentation({ id: "stable-exact", legacyPresentationTitle: exactTitle }, storage),
    { title: "Keep the exact old key" });
});

test("legacy migration stays at the same canonical artifact path and never enumerates storage", () => {
  const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
  const legacy = { title: "Shared title" };
  const snapshot = { id: "specific-report", legacyPresentationTitle: legacy.title };
  const values = new Map([
    [presentationStorageKey(legacy, "/one/index.html"), JSON.stringify({
      version: presentationVersion, presentation: { title: "Report one" },
    })],
    [presentationStorageKey(legacy, "/two/index.html"), JSON.stringify({
      version: presentationVersion, presentation: { title: "Report two" },
    })],
  ]);
  const reads = [];
  const storage = { getItem(key) { reads.push(key); return values.get(key) ?? null; },
    setItem: (key, value) => values.set(key, value),
    key() { assert.fail("Must not enumerate browser storage"); },
    get length() { assert.fail("Must not enumerate browser storage"); } };
  try {
    Object.defineProperty(globalThis, "location", { configurable: true, value: { pathname: "/one/index.html" } });
    assert.deepEqual(readLocalPresentation(snapshot, storage), { title: "Report one" });
    assert.ok(reads.every((key) => key.includes(":/one/index.html:")));
    assert.equal(values.has(presentationStorageKey(snapshot, "/two/index.html")), false);
  } finally {
    if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
    else delete globalThis.location;
  }
});

test("invalid legacy records are not copied and unavailable storage fails safely", () => {
  const legacy = { title: "Old report" };
  const snapshot = { id: "stable-report", legacyPresentationTitle: legacy.title };
  const oldKey = presentationStorageKey(legacy);
  const key = presentationStorageKey(snapshot);
  for (const value of ["not json", "null", JSON.stringify({ version: 2, presentation: {} }),
    JSON.stringify({ version: presentationVersion, presentation: { rows: [{ secret: true }] } }),
    JSON.stringify({ version: presentationVersion, presentation: { textEdits: { body: "x".repeat(20_001) } } }),
    " ".repeat(132_097)]) {
    const storage = memoryStorage();
    storage.setItem(oldKey, value);
    assert.deepEqual(readLocalPresentation(snapshot, storage), {});
    assert.equal(storage.getItem(key), null);
    assert.equal(storage.getItem(oldKey), value);
  }
  const legacyValue = JSON.stringify({ version: presentationVersion, presentation: { title: "Keep readable edits" } });
  const readonly = { getItem: (candidate) => candidate === oldKey ? legacyValue : null,
    setItem() { throw new Error("Quota or write denied"); } };
  assert.deepEqual(readLocalPresentation(snapshot, readonly), { title: "Keep readable edits" });
  const unavailable = { getItem() { throw new Error("Storage denied"); }, setItem() { throw new Error("Storage denied"); } };
  assert.deepEqual(readLocalPresentation(snapshot, unavailable), {});
  assert.equal(readViewerAppearance(snapshot, unavailable), "");
  assert.equal(writeLocalPresentation(snapshot, {}, unavailable), false);
  assert.equal(writeViewerAppearance(snapshot, "dark", unavailable), false);
});

test("a stable record arriving during legacy lookup is never overwritten", () => {
  const legacy = { title: "Old report" };
  const snapshot = { id: "stable-report", legacyPresentationTitle: legacy.title };
  const key = presentationStorageKey(snapshot);
  const oldKey = presentationStorageKey(legacy);
  const encode = (title) => JSON.stringify({ version: presentationVersion, presentation: { title } });
  let stableReads = 0;
  const storage = { getItem(candidate) {
    if (candidate === oldKey) return encode("Legacy");
    if (candidate === key) return ++stableReads === 1 ? null : encode("Current");
    return null;
  }, setItem() { assert.fail("Must not replace the current stable record"); } };
  assert.deepEqual(readLocalPresentation(snapshot, storage), { title: "Current" });
});

test("valid widget and chart permalinks share their dashboard presentation without merging unrelated paths", () => {
  const snapshot = { id: "reviewed-dashboard" };
  const dashboardKey = presentationStorageKey(snapshot, "/");
  const componentUuid = componentPermalinkId("https://dashboard.chatgpt.site", "active-users");
  const chartUuid = componentPermalinkId("https://dashboard.chatgpt.site", "usage-trend");
  const componentShortId = componentPermalinkShortId("https://dashboard.chatgpt.site", "active-users");
  const chartShortId = componentPermalinkShortId("https://dashboard.chatgpt.site", "usage-trend");

  assert.equal(presentationStorageKey(snapshot, `/_data/components/${componentShortId}`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, `/_data/charts/${chartShortId}`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, `/_data/charts/${chartShortId}/detail`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, `/_data/components/${componentUuid}`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, `/_data/charts/${chartUuid}`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, `/_data/charts/${chartUuid}/detail`), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/components/active-users"), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/components/usage-details"), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/components/Revenue%20growth"), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/charts/usage-trend"), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/charts/usage-trend/detail"), dashboardKey);
  assert.equal(presentationStorageKey(snapshot, "/_data/charts/Revenue%20growth"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/_data/components/parent%2Fchild"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/_data/components/active-users/detail"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/_data/components/active-users/"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/_data/charts/parent%2Fchild"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/_data/charts/usage-trend/"), dashboardKey);
  assert.notEqual(presentationStorageKey(snapshot, "/one"), presentationStorageKey(snapshot, "/two"));
  assert.notEqual(presentationStorageKey({ id: "another-dashboard" }, "/_data/components/active-users"),
    dashboardKey);
  assert.notEqual(presentationStorageKey({ id: "another-dashboard" }, "/_data/charts/usage-trend"), dashboardKey);
});

test("tab order merges as one shared presentation change", () => {
  const previous = { tabs: [{ id: "overview", label: "Overview" }, { id: "details", label: "Details" }] };
  const next = { tabs: [{ id: "details", label: "Details" }, { id: "overview", label: "Overview" }] };
  assert.deepEqual(mergePresentationChanges(previous, next, previous), next);
});

test("creator refresh schedules persist separately from reviewed data and reject invalid settings", () => {
  const snapshot = { id: "source-backed-dashboard" };
  const schedule = { frequency: "custom", time: "09:17", days: ["SU", "WE", "MO"] };
  const storage = memoryStorage();

  assert.equal(writeLocalPresentation(snapshot, { refreshSchedule: schedule }, storage), true);
  assert.deepEqual(readLocalPresentation(snapshot, storage), {
    refreshSchedule: { frequency: "custom", time: "09:17", days: ["MO", "WE", "SU"] },
  });
  assert.deepEqual(mergePresentationChanges({}, { refreshSchedule: schedule }, { title: "Reviewed title" }), {
    title: "Reviewed title", refreshSchedule: { frequency: "custom", time: "09:17", days: ["MO", "WE", "SU"] },
  });
  assert.throws(() => validatePresentation({ refreshSchedule: { frequency: "daily", time: "29:00" } }),
    /Refresh schedule must include/);
  assert.throws(() => validatePresentation({ refreshSchedule: { frequency: "custom", time: "09:00", days: [] } }),
    /Refresh schedule must include/);
});

test("concurrent presentation edits merge changed fields without discarding another editor", () => {
  const previous = { theme: "original", componentTitles: { revenue: "Revenue" }, filters: { region: "all" } };
  const mine = { theme: "scientific-blue", componentTitles: { revenue: "Recognized revenue" }, filters: { region: "all" } };
  const theirs = { theme: "original", componentTitles: { revenue: "Revenue", costs: "Operating costs" },
    filters: { region: "West" }, hiddenBlocks: ["forecast"] };
  assert.deepEqual(mergePresentationChanges(previous, mine, theirs), {
    theme: "scientific-blue", hiddenBlocks: ["forecast"],
    componentTitles: { revenue: "Recognized revenue", costs: "Operating costs" }, filters: { region: "West" },
  });
});

test("unrelated hosted owner edits preserve an existing verified refresh schedule", () => {
  const schedule = { frequency: "weekdays", time: "09:00" };
  const original = { theme: "original", title: "Reviewed dashboard", refreshSchedule: schedule };
  const edited = { ...original, title: "Edited dashboard" };

  assert.deepEqual(mergePresentationChanges(original, edited, original), {
    theme: "original", title: "Edited dashboard", refreshSchedule: schedule,
  });
});

test("later owner edits remain rebased on an earlier cross-session conflict merge", async () => {
  const original = { theme: "original", title: "Reviewed dashboard", componentTitles: { revenue: "Revenue" } };
  const firstLocal = { ...original, title: "Edited dashboard" };
  const concurrentRemote = {
    ...original, theme: "dark-pixel", componentTitles: { revenue: "Revenue", costs: "Operating costs" },
  };
  const firstMerged = mergePresentationChanges(original, firstLocal, concurrentRemote);
  const secondLocal = { ...firstLocal, componentTitles: { revenue: "Recognized revenue" } };
  const secondMerged = mergePresentationChanges(firstLocal, secondLocal, firstMerged);
  assert.deepEqual(secondMerged, {
    theme: "dark-pixel", title: "Edited dashboard",
    componentTitles: { revenue: "Recognized revenue", costs: "Operating costs" },
  }, "A second local edit cannot silently overwrite fields preserved during the earlier conflict");

  const persistence = await readFile(new URL("../src/use-presentation.js", import.meta.url), "utf8");
  assert.match(persistence, /const baseline = JSON\.parse\(previous\.current\)/);
  assert.match(persistence, /let next = mergePresentationChanges\(baseline, current, latest\.current\)/,
    "Every subsequent owner save must rebase its local delta onto the latest acknowledged presentation");
  assert.match(persistence, /mergePresentationChanges\(baseline, current, result\.presentation\)/,
    "A 409 must preserve unrelated changes from the current server presentation");
});

function fakeDatabase() {
  const state = { snapshot: null, queries: new Map(), rows: new Map(), presentation: null, maxBoundParameters: 0 };
  return {
    state,
    prepare(sql) {
      const statement = {
        values: [],
        bind(...values) {
          this.values = values;
          state.maxBoundParameters = Math.max(state.maxBoundParameters, values.length);
          return this;
        },
        async run() {
          if (this.values.length > 100) throw new Error("D1 statements accept at most 100 parameters.");
          if (sql.startsWith("DELETE FROM data_app_query_rows")) {
            if (this.values.length) {
              for (const [key, row] of state.rows) {
                if (row.query_id === this.values[0]) state.rows.delete(key);
              }
            } else {
              state.rows.clear();
            }
          }
          if (sql.startsWith("DELETE FROM data_app_queries")) state.queries.clear();
          if (sql.startsWith("INSERT INTO data_app_snapshots")) {
            const [, metadata_json, seed_sha256] = this.values;
            state.snapshot = { metadata_json, seed_sha256 };
          }
          if (sql.startsWith("INSERT INTO data_app_queries")) {
            const [id, position, query_json] = this.values;
            state.queries.set(id, { id, position, query_json });
          }
          if (sql.startsWith("INSERT INTO data_app_query_rows")) {
            for (let index = 0; index < this.values.length; index += 3) {
              const [query_id, position, row_json] = this.values.slice(index, index + 3);
              state.rows.set(`${query_id}:${position}`, { query_id, position, row_json });
            }
          }
          if (sql.startsWith("UPDATE data_app_snapshots")) {
            state.snapshot.metadata_json = JSON.stringify({
              ...JSON.parse(state.snapshot.metadata_json), generatedAt: this.values[0],
            });
          }
          if (sql.startsWith("INSERT INTO data_app_presentation_v1")) {
            if (!state.presentation) {
              const [, presentation, revision, updatedAt] = this.values;
              state.presentation = { presentation_json: presentation, revision, updated_at: updatedAt };
            }
          }
          if (sql.startsWith("UPDATE data_app_presentation_v1")) {
            const [presentation, updatedAt, , revision] = this.values;
            if (state.presentation.revision !== revision) return { meta: { changes: 0 } };
            state.presentation = { presentation_json: presentation, revision: revision + 1, updated_at: updatedAt };
            return { meta: { changes: 1 } };
          }
          return { meta: { changes: 0 } };
        },
        async first() {
          if (sql.includes("SELECT presentation_json")) return state.presentation;
          if (sql.includes("SELECT seed_sha256")) {
            return state.snapshot && { seed_sha256: state.snapshot.seed_sha256 };
          }
          if (sql.includes("SELECT metadata_json")) {
            return state.snapshot && { metadata_json: state.snapshot.metadata_json };
          }
          return null;
        },
        async all() {
          if (sql.includes("FROM data_app_queries")) {
            return { results: [...state.queries.values()].sort((a, b) => a.position - b.position) };
          }
          if (sql.includes("FROM data_app_query_rows")) {
            return { results: [...state.rows.values()].sort((a, b) =>
              a.query_id.localeCompare(b.query_id) || a.position - b.position) };
          }
          return { results: [] };
        },
      };
      return statement;
    },
    async batch(statements) {
      const previous = {
        snapshot: state.snapshot && { ...state.snapshot },
        queries: new Map(state.queries),
        rows: new Map(state.rows),
        presentation: state.presentation && { ...state.presentation },
      };
      try {
        const results = [];
        for (const statement of statements) results.push(await statement.run());
        return results;
      } catch (error) {
        Object.assign(state, previous);
        throw error;
      }
    },
  };
}

async function loadDashboardWorker(seed, ownerHash = "", ownerEmailHash = "", initialPresentation = {}) {
  return createDataAppWorker({
    html: "<main>Dashboard</main>",
    seedSnapshot: seed,
    ownerUserIdSha256: ownerHash,
    ownerEmailSha256: ownerEmailHash,
    initialPresentation,
  });
}

test("the hosted Worker adds one trusted Sites project reference to dashboard HTML", async () => {
  const worker = createDataAppWorker({
    html: "<!doctype html><html><head><title>Dashboard</title></head><body></body></html>",
    projectId: "appgprj_123",
    seedSnapshot: { queries: {} },
  });
  const response = await worker.fetch(new Request("https://dashboard.chatgpt.site/_data/charts/active-users"), {});
  const html = await response.text();
  assert.equal(html.match(/data-app-sites-project/gu)?.length, 1);
  assert.match(html, /<head><meta name="data-app-sites-project" content="appgprj_123">/u);
  assert.throws(
    () => createDataAppWorker({
      html: '<head><meta name="data-app-sites-project" content="forged"></head>',
      projectId: "appgprj_123",
      seedSnapshot: { queries: {} },
    }),
    /must not define its Sites project identity/u,
  );
  assert.throws(
    () => createDataAppWorker({ html: "<head></head>", projectId: "../private", seedSnapshot: { queries: {} } }),
    /Sites project ID is invalid/u,
  );
});

test("the hosted Worker ignores Sites marker text in scripts and comments", async () => {
  const reviewedHtml = `<!doctype html><html><head>
    <!-- <meta name="data-app-sites-project" content="comment"> -->
    <script>const example = '</head><meta name="data-app-sites-project" content="script">';</script>
    <style>head::after { content: 'meta[name="data-app-sites-project"]'; }</style>
    <meta name="description" content='Example: name="data-app-sites-project"'>
    </head><body>
    <script>document.querySelectorAll('meta[name="data-app-sites-project"]');</script>
    </body></html>`;
  const worker = createDataAppWorker({
    html: reviewedHtml,
    projectId: "appgprj_123",
    seedSnapshot: { queries: {} },
  });
  const html = await (await worker.fetch(new Request("https://dashboard.chatgpt.site/"), {})).text();
  assert.equal(html, reviewedHtml.replace("<head>", '<head><meta name="data-app-sites-project" content="appgprj_123">'));
  assert.throws(
    () => createDataAppWorker({
      html: '<head><META content="forged > identity" NAME = data-app-sites-project></head>',
      projectId: "appgprj_123",
      seedSnapshot: { queries: {} },
    }),
    /must not define its Sites project identity/u,
  );
});

test("the reusable Worker validates and seeds presentation without replacing later D1 edits", async () => {
  assert.equal(validateWorkerPresentation, validatePresentation);
  assert.throws(() => createDataAppWorker({
    html: "<main>Dashboard</main>", seedSnapshot: { queries: {} },
    initialPresentation: { appearance: "invalid" },
  }), /Appearance must be/u);
  const database = fakeDatabase();
  const first = await loadDashboardWorker({ queries: {} }, "", "", { title: "Reviewed initial title",
    verification: { verifiedBy: "forged@example.com", verifiedAt: "2026-08-17T19:34:56.789Z" } });
  const request = new Request("https://dashboard.chatgpt.site/api/presentation");
  const initial = await (await first.fetch(request, { DB: database })).json();
  assert.equal(initial.presentation.title, "Reviewed initial title");
  assert.equal(Object.hasOwn(initial.presentation, "verification"), false);
  const redeployed = await loadDashboardWorker({ queries: {} }, "", "", { title: "New source default" });
  const preserved = await (await redeployed.fetch(request, { DB: database })).json();
  assert.deepEqual(preserved.presentation, initial.presentation);
  assert.equal(preserved.revision, 0);
});

test("hosted edit authorization rejects missing, malformed, and overlong Site user IDs", async () => {
  const seed = { title: "Reviewed dashboard", queries: {} };
  const environment = {
    DB: { prepare() { assert.fail("Invalid caller identity must not touch dashboard storage."); } },
  };

  for (const userId of [null, "", " \t ", "Site User", "Site\tUser", "Site\0User", "Site\x7fUser", "x".repeat(513)]) {
    const ownerHash = createHash("sha256").update(userId?.trim() ?? "").digest("hex");
    const worker = await loadDashboardWorker(seed, ownerHash);
    // A request-shaped object lets us exercise even control characters rejected
    // by the Fetch API before they can reach a real Worker.
    const request = (path, method = "GET") => ({
      url: `https://dashboard.chatgpt.site${path}`,
      method,
      headers: { get(name) {
        if (name === "oai-authenticated-user-id") return userId;
        if (name === "oai-authenticated-user-email") return "publisher@example.com";
        return null;
      } },
    });
    assert.equal((await worker.fetch(request("/api/presentation", "PUT"), environment)).status, 403);
    assert.equal((await worker.fetch(request("/api/queries/reviewed", "PUT"), environment)).status, 403);
  }
});

test("legacy email authorization rejects missing, malformed, and overlong trusted emails", async () => {
  const seed = { title: "Reviewed dashboard", queries: {} };
  const environment = {
    DB: { prepare() { assert.fail("Invalid caller identity must not touch dashboard storage."); } },
  };

  for (const email of [null, "", " \t ", "not-an-email", "owner@@example.com", "owner@example",
    "ow ner@example.com", "owner\0@example.com", "owner\x7f@example.com", `${"x".repeat(245)}@example.com`]) {
    const emailHash = createHash("sha256").update(email?.trim().toLowerCase() ?? "").digest("hex");
    const worker = await loadDashboardWorker(seed, "", emailHash);
    const request = (path, method = "GET") => ({
      url: `https://dashboard.chatgpt.site${path}`,
      method,
      headers: { get(name) {
        if (name === "oai-authenticated-user-email") return email;
        if (name === "oai-authenticated-user-id") return "SiteUser_CurrentOwner";
        return null;
      } },
    });
    assert.equal((await worker.fetch(request("/api/presentation", "PUT"), environment)).status, 403);
    assert.equal((await worker.fetch(request("/api/queries/reviewed", "PUT"), environment)).status, 403);
  }
});

test("mixed or malformed owner seeds fail closed without trying another identity", async () => {
  const userId = "SiteUser_CurrentOwner";
  const email = "owner@example.com";
  const userIdHash = createHash("sha256").update(userId).digest("hex");
  const emailHash = createHash("sha256").update(email).digest("hex");
  const environment = {
    DB: { prepare() { assert.fail("An invalid owner configuration must not touch dashboard storage."); } },
  };
  const headers = {
    "oai-authenticated-user-id": userId,
    "oai-authenticated-user-email": email,
  };

  for (const [idSeed, emailSeed] of [
    [userIdHash, emailHash], ["invalid", emailHash], [userIdHash, "invalid"],
    [null, emailHash], [42, emailHash], ["", "invalid"],
    ["", emailHash.toUpperCase()], ["", null], ["", ""],
  ]) {
    const worker = await loadDashboardWorker({ queries: {} }, idSeed, emailSeed);
    for (const path of ["/api/presentation", "/api/queries/reviewed"]) {
      assert.equal((await worker.fetch(new Request(`https://dashboard.chatgpt.site${path}`, {
        method: "PUT", headers, body: "{}",
      }), environment)).status, 403);
    }
  }
});

test("explicit legacy email mode restores editing and server-stamped verification on older Sites", async () => {
  const seed = { title: "Reviewed dashboard", queries: { reviewed: { rows: [{ amount: 42 }] } } };
  const ownerEmail = "publisher@example.com";
  const ownerEmailHash = createHash("sha256").update(ownerEmail).digest("hex");
  const worker = await loadDashboardWorker(seed, "", ownerEmailHash);
  const database = fakeDatabase(seed);
  const fetch = (path, options = {}) => worker.fetch(new Request(`https://dashboard.chatgpt.site${path}`, options), {
    DB: database,
  });
  const headers = {
    "content-type": "application/json",
    "oai-authenticated-user-email": "  PUBLISHER@EXAMPLE.COM  ",
  };
  const viewerHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-id": "SiteUser_Viewer",
    "oai-authenticated-user-email": "viewer@example.com",
  };
  const body = JSON.stringify({
    presentation: { title: "Owner's saved title" }, revision: 0, verificationAction: "verify",
  });
  for (const untrustedHeaders of [undefined, viewerHeaders,
    { "oai-authenticated-user-id": "SiteUser_CurrentOwner" },
    { "x-owner-email": ownerEmail, "cf-access-authenticated-user-email": ownerEmail }]) {
    assert.equal((await (await fetch("/api/presentation", { headers: untrustedHeaders })).json()).canEdit, false);
    assert.equal((await fetch("/api/presentation", {
      method: "PUT", headers: untrustedHeaders, body,
    })).status, 403);
    assert.equal((await fetch("/api/queries/reviewed", {
      method: "PUT", headers: untrustedHeaders, body: JSON.stringify({ rows: [] }),
    })).status, 403);
  }

  const ownerResponse = await fetch("/api/presentation", { headers });
  assert.equal(ownerResponse.headers.get("cache-control"), "private, no-store");
  const owner = await ownerResponse.json();
  assert.equal(owner.canEdit, true);
  assert.equal(Object.hasOwn(owner, "viewerEmail"), false);
  assert.equal(JSON.stringify(owner).includes(ownerEmail), false,
    "Legacy authorization must not expose the owner email before explicit verification");
  const beforeVerification = Date.now();
  const savedResponse = await fetch("/api/presentation", { method: "PUT", headers, body });
  assert.equal(savedResponse.status, 200);
  const saved = await savedResponse.json();
  assert.equal(saved.revision, 1);
  assert.equal(saved.presentation.verification.verifiedBy, ownerEmail);
  assert.equal(saved.presentation.verification.verifiedAt, saved.updatedAt);
  assert.ok(Date.parse(saved.updatedAt) >= beforeVerification);
  assert.equal((await fetch("/api/presentation", { method: "PUT", headers, body })).status, 409);
  assert.equal((await fetch("/api/queries/reviewed", {
    method: "PUT", headers, body: JSON.stringify({ rows: [{ amount: 84 }] }),
  })).status, 200);
  assert.deepEqual((await (await fetch("/api/snapshot")).json()).queries.reviewed.rows, [{ amount: 84 }]);

  const reloadedWorker = await loadDashboardWorker(seed, "", ownerEmailHash);
  const reloaded = await (await reloadedWorker.fetch(new Request("https://dashboard.chatgpt.site/api/presentation", {
    headers,
  }), { DB: database })).json();
  assert.equal(reloaded.canEdit, true);
  assert.deepEqual(reloaded.presentation, saved.presentation);
  const removal = JSON.stringify({ presentation: saved.presentation, revision: 1, verificationAction: "remove" });
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: viewerHeaders, body: removal,
  })).status, 403);
  const removed = await fetch("/api/presentation", { method: "PUT", headers, body: removal });
  assert.equal(removed.status, 200);
  assert.equal(Object.hasOwn((await removed.json()).presentation, "verification"), false);
});

test("replacing legacy email authorization revokes the old identity without resetting D1", async () => {
  const seed = { title: "Existing dashboard", queries: { reviewed: { rows: [{ amount: 42 }] } } };
  const previousEmail = "previous-owner@example.com";
  const nextEmail = "current-owner@example.com";
  const nextUserId = "SiteUser_CurrentOwner";
  const hash = (value) => createHash("sha256").update(value).digest("hex");
  const database = fakeDatabase(seed);
  const fetch = (worker, path, email, options = {}, userId = undefined) => worker.fetch(
    new Request(`https://dashboard.chatgpt.site${path}`, {
      ...options,
      headers: {
        "content-type": "application/json",
        ...(email === undefined ? {} : { "oai-authenticated-user-email": email }),
        ...(userId === undefined ? {} : { "oai-authenticated-user-id": userId }),
      },
    }), { DB: database },
  );
  const originalWorker = await loadDashboardWorker(seed, "", hash(previousEmail));
  await fetch(originalWorker, "/api/snapshot", previousEmail);
  const saved = await (await fetch(originalWorker, "/api/presentation", previousEmail, {
    method: "PUT", body: JSON.stringify({
      presentation: { title: "Saved before transfer" }, revision: 0, verificationAction: "verify",
    }),
  })).json();
  assert.equal((await fetch(originalWorker, "/api/queries/reviewed", previousEmail, {
    method: "PUT", body: JSON.stringify({ rows: [{ amount: 84 }] }),
  })).status, 200);

  for (const [worker, expectedEmail, expectedUserId] of [
    [await loadDashboardWorker(seed, "", hash(nextEmail)), nextEmail, undefined],
    [await loadDashboardWorker(seed, hash(nextUserId)), undefined, nextUserId],
  ]) {
    const oldOwner = await (await fetch(worker, "/api/presentation", previousEmail)).json();
    assert.equal(oldOwner.canEdit, false);
    assert.deepEqual(oldOwner.presentation, saved.presentation,
      "Historical verification is persisted presentation, never proof of current ownership");
    const nextOwner = await (await fetch(worker, "/api/presentation", expectedEmail, {}, expectedUserId)).json();
    assert.equal(nextOwner.canEdit, true);
    assert.equal(nextOwner.revision, saved.revision);
    assert.deepEqual(nextOwner.presentation, saved.presentation);
    assert.deepEqual((await (await fetch(worker, "/api/snapshot", expectedEmail)).json())
      .queries.reviewed.rows, [{ amount: 84 }]);
    assert.equal((await fetch(worker, "/api/presentation", previousEmail, {
      method: "PUT", body: JSON.stringify({ presentation: {}, revision: saved.revision }),
    })).status, 403);
    assert.equal((await fetch(worker, "/api/queries/reviewed", previousEmail, {
      method: "PUT", body: JSON.stringify({ rows: [] }),
    })).status, 403);
  }

  const stableIdWorker = await loadDashboardWorker(seed, hash(nextUserId));
  assert.equal((await (await fetch(stableIdWorker, "/api/presentation", nextEmail)).json()).canEdit, false,
    "Switching to stable ID must disable legacy email authorization completely");
});

test("hosted widget, chart, and detail permalinks serve only safe GET and bodyless HEAD dashboard routes", async () => {
  const worker = await loadDashboardWorker({ title: "Reviewed dashboard", queries: {} });
  const componentUuid = componentPermalinkId("https://dashboard.chatgpt.site", "active-users");
  const chartUuid = componentPermalinkId("https://dashboard.chatgpt.site", "usage-trend");
  const componentShortId = componentPermalinkShortId("https://dashboard.chatgpt.site", "active-users");
  const chartShortId = componentPermalinkShortId("https://dashboard.chatgpt.site", "usage-trend");
  const environment = {
    DB: { prepare() { assert.fail("Component permalink HTML must not read or change dashboard data."); } },
  };
  const fetch = (path, options = {}) => worker.fetch(
    new Request(`https://dashboard.chatgpt.site${path}`, options), environment,
  );
  const root = await fetch("/");
  const expectedHtml = await root.text();

  for (const path of [
    `/_data/components/${componentShortId}`, `/_data/charts/${chartShortId}`, `/_data/charts/${chartShortId}/detail`,
    `/_data/components/${componentUuid}`, `/_data/charts/${chartUuid}`, `/_data/charts/${chartUuid}/detail`,
    "/_data/components/active-users", "/_data/components/notes", "/_data/components/usage-details",
    "/_data/components/detail", "/_data/components/Revenue%20%26%20growth",
    "/_data/components/%F0%9F%93%88", "/_data/components/%252F", "/_data/components/missing-widget",
    `/_data/components/${"x".repeat(200)}`, "/_data/components/active-users?token=private#draft",
    "/_data/charts/usage-trend", "/_data/charts/usage-trend/detail",
    "/_data/charts/Revenue%20%26%20growth", "/_data/charts/%F0%9F%93%88/detail",
    "/_data/charts/%252F", "/_data/charts/missing-chart", `/_data/charts/${"x".repeat(200)}`,
    "/_data/charts/usage-trend?token=private#draft",
  ]) {
    const response = await fetch(path);
    assert.equal(response.status, 200, path);
    assert.equal(response.headers.get("content-type"), "text/html; charset=utf-8", path);
    assert.equal(await response.text(), expectedHtml, path);
  }

  for (const path of [
    `/_data/components/${componentShortId}`, `/_data/charts/${chartShortId}`, `/_data/charts/${chartShortId}/detail`,
    `/_data/components/${componentUuid}`, `/_data/charts/${chartUuid}`, `/_data/charts/${chartUuid}/detail`,
    "/_data/components/active-users", "/_data/components/notes",
    "/_data/charts/usage-trend", "/_data/charts/usage-trend/detail",
  ]) {
    const response = await fetch(path, { method: "HEAD" });
    assert.equal(response.status, 200, path);
    assert.equal(response.headers.get("content-type"), "text/html; charset=utf-8", path);
    assert.equal(await response.text(), "", path);
  }

  for (const path of [
    `/_data/components/${componentShortId}/detail`, `/_data/charts/${chartShortId}/detail/extra`,
    `/_data/components/${componentUuid}/detail`, `/_data/charts/${chartUuid}/detail/extra`,
    "/_data/components", "/_data/components/", "/_data/components//detail",
    "/_data/components/active-users/", "/_data/components/active-users/detail",
    "/_data/components/active-users/detail/", "/_data/components/active-users/extra",
    "/_data/components/.", "/_data/components/..", "/_data/components/%2e",
    "/_data/components/%2E%2e", "/_data/components/%2F", "/_data/components/%2f",
    "/_data/components/parent%2Fchild", "/_data/components/%5C", "/_data/components/parent%5cchild",
    "/_data/components/%00", "/_data/components/widget%00name", "/_data/components/%20",
    "/_data/components/%", "/_data/components/%E0%A4%A", `/_data/components/${"x".repeat(201)}`,
    "/nested/_data/components/active-users", "/_DATA/components/active-users",
    "/_data/component/active-users", "/_data/COMPONENTS/active-users",
    "/_data/charts", "/_data/charts/", "/_data/charts//detail", "/_data/charts/usage-trend/",
    "/_data/charts/usage-trend/details", "/_data/charts/usage-trend/detail/",
    "/_data/charts/usage-trend/detail/extra", "/_data/charts/.", "/_data/charts/..",
    "/_data/charts/%2e", "/_data/charts/%2E%2e", "/_data/charts/%2F", "/_data/charts/%2f",
    "/_data/charts/parent%2Fchild", "/_data/charts/%5C", "/_data/charts/parent%5cchild",
    "/_data/charts/%00", "/_data/charts/trend%00chart", "/_data/charts/%20",
    "/_data/charts/%", "/_data/charts/%E0%A4%A", `/_data/charts/${"x".repeat(201)}`,
    "/nested/_data/charts/usage-trend", "/_DATA/charts/usage-trend", "/_data/chart/usage-trend",
  ]) {
    assert.equal((await fetch(path)).status, 404, path);
  }

  for (const method of ["POST", "PUT", "PATCH", "DELETE", "OPTIONS"]) {
    assert.equal((await fetch(`/_data/components/${componentShortId}`, { method })).status, 404, method);
    assert.equal((await fetch(`/_data/charts/${chartShortId}`, { method })).status, 404, method);
    assert.equal((await fetch(`/_data/components/${componentUuid}`, { method })).status, 404, method);
    assert.equal((await fetch(`/_data/charts/${chartUuid}`, { method })).status, 404, method);
    assert.equal((await fetch("/_data/components/active-users", { method })).status, 404, method);
    assert.equal((await fetch("/_data/components/active-users/detail", { method })).status, 404, method);
    assert.equal((await fetch("/_data/charts/usage-trend", { method })).status, 404, method);
    assert.equal((await fetch("/_data/charts/usage-trend/detail", { method })).status, 404, method);
  }
});

test("hosted presentation allows only its seeded Site owner and keeps reviewed rows separate", async () => {
  const seed = { title: "Reviewed dashboard", queries: { reviewed: { rows: [{ amount: 42 }] } } };
  const ownerUserId = "SiteUser_Publisher123";
  const ownerEmail = "new-address@example.com";
  const ownerHash = createHash("sha256").update(ownerUserId).digest("hex");
  const worker = await loadDashboardWorker(seed, ownerHash);
  const database = fakeDatabase(seed);
  const fetch = (path, options = {}) => worker.fetch(new Request(`https://dashboard.chatgpt.site${path}`, options), {
    DB: database,
  });
  assert.deepEqual((await (await fetch("/api/snapshot")).json()).queries.reviewed.rows, [{ amount: 42 }]);
  const firstResponse = await fetch("/api/presentation");
  assert.equal(firstResponse.headers.get("cache-control"), "private, no-store");
  const first = await firstResponse.json();
  assert.deepEqual(first.presentation, {});
  assert.equal(first.revision, 0);
  assert.equal(first.canEdit, false);
  assert.equal(Object.hasOwn(first, "viewerEmail"), false);

  const viewerHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-id": "SiteUser_Viewer456",
    "oai-authenticated-user-email": "publisher@example.com",
  };
  const viewerResponse = await fetch("/api/presentation", { headers: viewerHeaders });
  assert.equal(viewerResponse.headers.get("cache-control"), "private, no-store");
  const viewer = await viewerResponse.json();
  assert.equal(viewer.canEdit, false);
  assert.equal(Object.hasOwn(viewer, "viewerEmail"), false);

  const body = JSON.stringify({ presentation: { theme: "dark-pixel", title: "Edited title" }, revision: 0 });
  assert.equal((await fetch("/api/presentation", { method: "PUT", body })).status, 403);
  assert.equal((await fetch("/api/presentation", { method: "PUT", headers: viewerHeaders, body })).status, 403);
  assert.equal((await fetch("/api/queries/reviewed", {
    method: "PUT", headers: viewerHeaders, body: JSON.stringify({ rows: [] }),
  })).status, 403);
  const emailOnlyHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-email": "publisher@example.com",
  };
  assert.equal((await (await fetch("/api/presentation", { headers: emailOnlyHeaders })).json()).canEdit, false);
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: emailOnlyHeaders, body,
  })).status, 403);
  const differentlyCasedOwnerHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-id": ownerUserId.toLowerCase(),
  };
  assert.equal((await (await fetch("/api/presentation", {
    headers: differentlyCasedOwnerHeaders,
  })).json()).canEdit, false);
  const headers = {
    "content-type": "application/json",
    "oai-authenticated-user-id": ` ${ownerUserId} `,
    "oai-authenticated-user-email": ownerEmail,
  };
  const ownerResponse = await fetch("/api/presentation", { headers });
  assert.equal(ownerResponse.headers.get("cache-control"), "private, no-store");
  const owner = await ownerResponse.json();
  assert.equal(owner.canEdit, true);
  assert.equal(Object.hasOwn(owner, "viewerEmail"), false,
    "Authenticated owner email must never be disclosed to dashboard-authored browser code");
  assert.equal(JSON.stringify(owner).includes(ownerEmail), false,
    "An unverified dashboard must not reveal its authenticated owner's identity");
  const saved = await fetch("/api/presentation", { method: "PUT", headers, body });
  assert.equal(saved.status, 200);
  assert.equal(saved.headers.get("cache-control"), "private, no-store");
  assert.equal((await saved.json()).revision, 1);
  assert.equal((await fetch("/api/presentation", { method: "PUT", headers, body })).status, 409);
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers, body: JSON.stringify({ presentation: { rows: [{ amount: 1 }] }, revision: 1 }),
  })).status, 400);
  assert.deepEqual((await (await fetch("/api/presentation")).json()).presentation, {
    theme: "dark-pixel", title: "Edited title",
  });
  assert.deepEqual((await (await fetch("/api/snapshot")).json()).queries.reviewed.rows, [{ amount: 42 }]);
});

test("hosted annotation visibility round trips without dropping authored evidence or changing reviewed rows", async () => {
  const seed = { title: "Annotation visibility", queries: { reviewed: { rows: [{ amount: 42, target: 50 }] } } };
  const ownerUserId = "SiteUser_AnnotationOwner";
  const worker = await loadDashboardWorker(seed, createHash("sha256").update(ownerUserId).digest("hex"));
  const database = fakeDatabase();
  const fetch = (path, options = {}) => worker.fetch(new Request(`https://dashboard.chatgpt.site${path}`, options), {
    DB: database,
  });
  const headers = { "content-type": "application/json", "oai-authenticated-user-id": ownerUserId };
  const spec = { type: "line", x: "date", y: "amount", annotations: [
    { id: "target", kind: "benchmark", label: "Reviewed target", field: "target", measure: "amount" },
  ] };
  let revision = 0;
  let lastPresentation;
  for (const setting of [{}, { showAnnotations: false }, { showAnnotations: true }]) {
    const presentation = { chartOverrides: { trend: { ...spec, ...setting } } };
    const saved = await fetch("/api/presentation", {
      method: "PUT", headers, body: JSON.stringify({ presentation, revision }),
    });
    assert.equal(saved.status, 200);
    assert.equal((await saved.json()).revision, ++revision);
    const readback = await (await fetch("/api/presentation")).json();
    assert.deepEqual(readback.presentation, presentation,
      "Readers receive the saved visibility and all authored annotation references");
    assert.equal(readback.revision, revision);
    assert.equal(readback.canEdit, false);
    lastPresentation = presentation;
  }
  for (const chart of [
    { ...spec, showAnnotations: "false" },
    { ...spec, showAnnotations: false, annotations: [{ ...spec.annotations[0], rows: [{ amount: 1 }] }] },
  ]) {
    const response = await fetch("/api/presentation", {
      method: "PUT", headers,
      body: JSON.stringify({ presentation: { chartOverrides: { trend: chart } }, revision }),
    });
    assert.equal(response.status, 400);
  }
  const readback = await (await fetch("/api/presentation")).json();
  assert.deepEqual(readback.presentation, lastPresentation);
  assert.equal(readback.revision, revision, "Rejected settings do not create a new revision");
  assert.deepEqual((await (await fetch("/api/snapshot")).json()).queries.reviewed.rows, seed.queries.reviewed.rows);
});

test("Site ownership transfer immediately replaces the dashboard editor after redeployment", async () => {
  const seed = { title: "Transferred dashboard", queries: { reviewed: { rows: [{ amount: 42 }] } } };
  const originalOwnerId = "SiteUser_OriginalOwner";
  const originalOwnerEmail = "original-owner@example.com";
  const nextOwnerId = "SiteUser_NewOwner";
  const nextOwnerEmail = "next-owner@example.com";
  const ownerHash = (userId) => createHash("sha256").update(userId).digest("hex");
  const database = fakeDatabase(seed);
  const fetch = (worker, path, userId, options = {}, email = userId === originalOwnerId
    ? originalOwnerEmail : nextOwnerEmail) => worker.fetch(
    new Request(`https://dashboard.chatgpt.site${path}`, {
      ...options,
      headers: {
        "content-type": "application/json",
        "oai-authenticated-user-id": userId,
        "oai-authenticated-user-email": email,
      },
    }), { DB: database },
  );
  const originalWorker = await loadDashboardWorker(seed, ownerHash(originalOwnerId));
  const originalOwner = await (await fetch(originalWorker, "/api/presentation", originalOwnerId)).json();
  assert.equal(originalOwner.canEdit, true);
  assert.equal(Object.hasOwn(originalOwner, "viewerEmail"), false);
  assert.equal((await (await fetch(originalWorker, "/api/presentation", nextOwnerId)).json()).canEdit, false);

  const requestedVerification = {
    verifiedBy: "spoofed@example.com", verifiedAt: "2000-01-01T00:00:00.000Z",
  };
  const originalVerification = await (await fetch(originalWorker, "/api/presentation", originalOwnerId, {
    method: "PUT", body: JSON.stringify({
      presentation: { title: "Originally verified dashboard" }, revision: 0, verificationAction: "verify",
    }),
  })).json();
  assert.equal(originalVerification.presentation.verification.verifiedBy, originalOwnerEmail);

  const transferredWorker = await loadDashboardWorker(seed, ownerHash(nextOwnerId));
  assert.equal((await (await fetch(transferredWorker, "/api/presentation", originalOwnerId)).json()).canEdit, false);
  assert.equal((await (await fetch(transferredWorker, "/api/presentation", nextOwnerId)).json()).canEdit, true);
  assert.equal((await (await fetch(transferredWorker, "/api/presentation", originalOwnerId, {},
    nextOwnerEmail)).json()).canEdit, false,
  "The previous owner's ID cannot regain edit access by presenting the new owner's email");

  const body = JSON.stringify({ presentation: {
    title: "Transferred ownership", verification: requestedVerification,
  }, revision: 1 });
  assert.equal((await fetch(transferredWorker, "/api/presentation", originalOwnerId, {
    method: "PUT", body,
  })).status, 403);
  assert.equal((await fetch(transferredWorker, "/api/presentation", originalOwnerId, {
    method: "PUT", body,
  }, nextOwnerEmail)).status, 403,
  "Spoofing the new owner's email must not authorize the previous owner to edit or verify");
  assert.equal((await fetch(transferredWorker, "/api/queries/reviewed", originalOwnerId, {
    method: "PUT", body: JSON.stringify({ rows: [] }),
  })).status, 403);
  const transferredPresentation = await fetch(transferredWorker, "/api/presentation", nextOwnerId, {
    method: "PUT", body,
  });
  assert.equal(transferredPresentation.status, 200);
  assert.deepEqual((await transferredPresentation.json()).presentation.verification,
    originalVerification.presentation.verification,
    "The new owner's ordinary edits must preserve the original trusted verification record");
  assert.equal((await fetch(transferredWorker, "/api/queries/reviewed", nextOwnerId, {
    method: "PUT", body: JSON.stringify({ rows: [{ amount: 84 }] }),
  })).status, 200);

  const removal = JSON.stringify({ presentation: {
    title: "Transferred ownership", verification: requestedVerification,
  }, revision: 2, verificationAction: "remove" });
  assert.equal((await fetch(transferredWorker, "/api/presentation", originalOwnerId, {
    method: "PUT", body: removal,
  }, nextOwnerEmail)).status, 403,
  "A transferred-away owner cannot remove the existing verified dashboard badge");
  assert.equal((await fetch(transferredWorker, "/api/presentation", nextOwnerId, {
    method: "PUT", body: removal,
  })).status, 200, "Only the current owner may remove the transferred dashboard badge");

  const reverify = JSON.stringify({ presentation: {
    title: "Transferred ownership",
  }, revision: 3, verificationAction: "verify" });
  assert.equal((await fetch(transferredWorker, "/api/presentation", originalOwnerId, {
    method: "PUT", body: reverify,
  }, nextOwnerEmail)).status, 403,
  "The previous owner cannot verify the transferred dashboard with the new owner's email");
  const nextVerification = await fetch(transferredWorker, "/api/presentation", nextOwnerId, {
    method: "PUT", body: reverify,
  });
  assert.equal(nextVerification.status, 200);
  assert.equal((await nextVerification.json()).presentation.verification.verifiedBy, nextOwnerEmail,
    "A transferred dashboard's new badge must be stamped with the current owner's authenticated email");
});

test("private owner bootstrap preserves existing D1 state across the owner-enabled redeployment", async () => {
  const seed = { title: "Existing dashboard", queries: { reviewed: { rows: [{ amount: 42 }] } } };
  const oldOwnerId = "SiteUser_PreviousOwner";
  const nextOwnerId = "SiteUser_CurrentOwner";
  const oldOwnerHash = createHash("sha256").update(oldOwnerId).digest("hex");
  const database = fakeDatabase(seed);
  const fetch = (worker, path, userId, options = {}) => worker.fetch(
    new Request(`https://dashboard.chatgpt.site${path}`, {
      ...options,
      headers: {
        "content-type": "application/json",
        "oai-authenticated-user-id": userId,
        "oai-authenticated-user-email": userId === oldOwnerId
          ? "previous-owner@example.com" : "current-owner@example.com",
      },
    }), { DB: database },
  );

  const originalWorker = await loadDashboardWorker(seed, oldOwnerHash);
  await fetch(originalWorker, "/api/snapshot", oldOwnerId);
  const originalPresentation = await (await fetch(originalWorker, "/api/presentation", oldOwnerId, {
    method: "PUT",
    body: JSON.stringify({
      presentation: { title: "Saved presentation", filters: { week: "latest" } },
      revision: 0,
      verificationAction: "verify",
    }),
  })).json();
  assert.equal((await fetch(originalWorker, "/api/queries/reviewed", oldOwnerId, {
    method: "PUT", body: JSON.stringify({ rows: [{ amount: 84 }] }),
  })).status, 200);
  const stateBeforeBootstrap = structuredClone(database.state);

  const bootstrapWorker = await loadDashboardWorker(seed);
  assert.deepEqual(database.state, stateBeforeBootstrap,
    "An owner-setup deployment must neither reset persisted data nor claim ownership");
  for (const userId of [oldOwnerId, nextOwnerId]) {
    const presentation = await (await fetch(bootstrapWorker, "/api/presentation", userId)).json();
    assert.equal(presentation.canEdit, false);
    assert.deepEqual(presentation.presentation, originalPresentation.presentation);
    assert.equal(presentation.revision, originalPresentation.revision);
    assert.equal((await fetch(bootstrapWorker, "/api/presentation", userId, {
      method: "PUT", body: JSON.stringify({ presentation: {}, revision: presentation.revision }),
    })).status, 403);
  }

  // Publication obtains this hash from the authoritative Sites control plane,
  // never from whichever browser visitor arrives first.
  const nextOwnerHash = createHash("sha256").update(nextOwnerId).digest("hex");
  const repairedWorker = await loadDashboardWorker(seed, nextOwnerHash);
  const repaired = await (await fetch(repairedWorker, "/api/presentation", nextOwnerId)).json();
  assert.equal(repaired.canEdit, true);
  assert.deepEqual(repaired.presentation, originalPresentation.presentation);
  assert.equal(repaired.revision, originalPresentation.revision);
  assert.deepEqual((await (await fetch(repairedWorker, "/api/snapshot", nextOwnerId)).json())
    .queries.reviewed.rows, [{ amount: 84 }]);
  assert.equal((await (await fetch(repairedWorker, "/api/presentation", oldOwnerId)).json()).canEdit, false);
  assert.equal((await fetch(repairedWorker, "/api/queries/reviewed", oldOwnerId, {
    method: "PUT", body: JSON.stringify({ rows: [] }),
  })).status, 403);
  assert.equal((await fetch(repairedWorker, "/api/presentation", nextOwnerId, {
    method: "PUT",
    body: JSON.stringify({
      presentation: repaired.presentation,
      revision: repaired.revision,
      verificationAction: "remove",
    }),
  })).status, 200);
});

test("hosted dashboard verification is creator-only, server-stamped, stable, and removable", async () => {
  const ownerEmail = "publisher@example.com";
  const changedOwnerEmail = "new-address@example.com";
  const ownerUserId = "SiteUser_Publisher123";
  const ownerHash = createHash("sha256").update(ownerUserId).digest("hex");
  const worker = await loadDashboardWorker({ title: "Reviewed dashboard", queries: {} }, ownerHash);
  const database = fakeDatabase();
  const ownerHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-id": ownerUserId,
    "oai-authenticated-user-email": "  PUBLISHER@EXAMPLE.COM  ",
  };
  const viewerHeaders = {
    "content-type": "application/json",
    "oai-authenticated-user-id": "SiteUser_Viewer456",
    "oai-authenticated-user-email": "viewer@example.com",
  };
  const spoofedOwnerEmailHeaders = {
    ...viewerHeaders, "oai-authenticated-user-email": ownerEmail,
  };
  const emailOnlyHeaders = {
    "content-type": "application/json", "oai-authenticated-user-email": ownerEmail,
  };
  const ownerIdOnlyHeaders = {
    "content-type": "application/json", "oai-authenticated-user-id": ownerUserId,
  };
  const changedOwnerEmailHeaders = {
    ...ownerIdOnlyHeaders, "oai-authenticated-user-email": `  ${changedOwnerEmail.toUpperCase()}  `,
  };
  const fetch = (path, options = {}) => worker.fetch(new Request(`https://dashboard.chatgpt.site${path}`, options), {
    DB: database,
  });
  const spoofed = { verifiedBy: "impostor@example.com", verifiedAt: "2000-01-01T00:00:00.000Z" };
  const body = JSON.stringify({
    presentation: { title: "Reviewed dashboard" }, revision: 0, verificationAction: "verify",
  });

  assert.equal((await fetch("/api/presentation", { method: "PUT", body })).status, 403);
  assert.equal((await fetch("/api/presentation", { method: "PUT", headers: viewerHeaders, body })).status, 403);
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: spoofedOwnerEmailHeaders, body,
  })).status, 403, "An ID-configured deployment must not let the owner email authorize a different user ID");
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: emailOnlyHeaders, body,
  })).status, 403, "The owner email alone must not authorize verification in stable-ID mode");
  const spoofedOwner = await (await fetch("/api/presentation", { headers: spoofedOwnerEmailHeaders })).json();
  assert.equal(spoofedOwner.canEdit, false);
  assert.equal(Object.hasOwn(spoofedOwner, "viewerEmail"), false);
  const actualOwner = await (await fetch("/api/presentation", { headers: ownerHeaders })).json();
  assert.equal(actualOwner.canEdit, true);
  assert.equal(Object.hasOwn(actualOwner, "viewerEmail"), false,
    "Even the authenticated owner must not receive their private identity before opting into verification");
  assert.equal(JSON.stringify(actualOwner).includes(ownerEmail), false);
  const ownerWithoutEmail = await (await fetch("/api/presentation", { headers: ownerIdOnlyHeaders })).json();
  assert.equal(ownerWithoutEmail.canEdit, true,
    "The stable owner ID must remain authorized even when its authenticated email is unavailable");
  assert.equal(Object.hasOwn(ownerWithoutEmail, "viewerEmail"), false);
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: ownerIdOnlyHeaders, body,
  })).status, 400, "A new badge must not be verified without a trusted authenticated email");
  for (const invalidOwnerEmail of ["not-an-email", `${"x".repeat(245)}@example.com`]) {
    assert.equal((await fetch("/api/presentation", {
      method: "PUT", headers: { ...ownerIdOnlyHeaders, "oai-authenticated-user-email": invalidOwnerEmail }, body,
    })).status, 400, `A new badge must reject invalid trusted attribution: ${invalidOwnerEmail}`);
  }
  for (const invalidAction of [null, true, false, 0, {}, [], "", "VERIFY", "delete"]) {
    assert.equal((await fetch("/api/presentation", {
      method: "PUT", headers: ownerHeaders, body: JSON.stringify({
        presentation: { title: "Reviewed dashboard" }, revision: 0, verificationAction: invalidAction,
      }),
    })).status, 400, `Malformed verification action must fail closed: ${JSON.stringify(invalidAction)}`);
  }
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: ownerHeaders,
    body: JSON.stringify({ presentation: { title: "Reviewed dashboard", verification: spoofed }, revision: 0 }),
  })).status, 400, "An authored dashboard cannot fabricate a verified badge without an explicit server action");
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: ownerHeaders,
    body: JSON.stringify({ presentation: {
      verification: { ...spoofed, rows: [{ secret: true }] },
    }, revision: 0, verificationAction: "verify" }),
  })).status, 400, "Malformed verification metadata must be rejected before creator stamping");

  const beforeVerification = Date.now();
  const verifiedResponse = await fetch("/api/presentation", { method: "PUT", headers: ownerHeaders, body });
  assert.equal(verifiedResponse.status, 200);
  const verified = await verifiedResponse.json();
  assert.equal(verified.revision, 1);
  assert.equal(verified.presentation.verification.verifiedBy, ownerEmail);
  assert.notEqual(verified.presentation.verification.verifiedAt, spoofed.verifiedAt);
  assert.ok(Date.parse(verified.presentation.verification.verifiedAt) >= beforeVerification);
  assert.equal(verified.presentation.verification.verifiedAt, verified.updatedAt,
    "The verification timestamp must be generated by the trusted Worker");

  const publicVerification = await (await fetch("/api/presentation")).json();
  assert.equal(publicVerification.canEdit, false);
  assert.equal(Object.hasOwn(publicVerification, "viewerEmail"), false);
  assert.deepEqual(publicVerification.presentation.verification, verified.presentation.verification,
    "Every authorized dashboard viewer must see the persisted verifier identity and timestamp");
  const viewer = await (await fetch("/api/presentation", { headers: viewerHeaders })).json();
  assert.equal(Object.hasOwn(viewer, "viewerEmail"), false);
  assert.deepEqual(viewer.presentation.verification, verified.presentation.verification);
  const changedOwner = await (await fetch("/api/presentation", { headers: changedOwnerEmailHeaders })).json();
  assert.equal(changedOwner.canEdit, true,
    "Changing the owner's email must not revoke access granted by the stable Site-scoped user ID");
  assert.equal(Object.hasOwn(changedOwner, "viewerEmail"), false,
    "Changing the owner email must never expose the new authenticated identity to dashboard code");
  assert.equal(JSON.stringify(changedOwner).includes(changedOwnerEmail), false);
  const reloadedWorker = await loadDashboardWorker({ title: "Reviewed dashboard", queries: {} }, ownerHash);
  const reloaded = await reloadedWorker.fetch(new Request("https://dashboard.chatgpt.site/api/presentation"), {
    DB: database,
  });
  assert.deepEqual((await reloaded.json()).presentation.verification, verified.presentation.verification,
    "Verification must persist across Worker reloads and deployments");

  const outdated = await fetch("/api/presentation", {
    method: "PUT", headers: ownerHeaders,
    body: JSON.stringify({ presentation: { verification: spoofed }, revision: 0 }),
  });
  assert.equal(outdated.status, 409);
  assert.deepEqual((await outdated.json()).presentation.verification, verified.presentation.verification,
    "Conflicting writes must return the authoritative verification record without replacing it");

  const unchanged = await fetch("/api/presentation", {
    method: "PUT", headers: changedOwnerEmailHeaders,
    body: JSON.stringify({ presentation: { title: "Updated dashboard", verification: spoofed }, revision: 1 }),
  });
  assert.equal(unchanged.status, 200);
  const updated = await unchanged.json();
  assert.equal(updated.revision, 2);
  assert.equal(updated.presentation.title, "Updated dashboard");
  assert.deepEqual(updated.presentation.verification, verified.presentation.verification,
    "Unrelated edits and spoofed client metadata must preserve the original server verification exactly");

  const removal = JSON.stringify({ presentation: {
    title: "Updated dashboard", verification: spoofed,
  }, revision: 2, verificationAction: "remove" });
  assert.equal((await fetch("/api/presentation", {
    method: "PUT", headers: viewerHeaders, body: removal,
  })).status, 403, "A read-only viewer must never remove dashboard verification");
  const cleared = await fetch("/api/presentation", { method: "PUT", headers: ownerHeaders, body: removal });
  assert.equal(cleared.status, 200);
  assert.equal((await cleared.json()).revision, 3);
  assert.equal(Object.hasOwn((await (await fetch("/api/presentation")).json()).presentation, "verification"), false,
    "Removing verification must clear the shared dashboard badge for every viewer");

  const reverified = await fetch("/api/presentation", {
    method: "PUT", headers: changedOwnerEmailHeaders,
    body: JSON.stringify({
      presentation: { title: "Updated dashboard" }, revision: 3, verificationAction: "verify",
    }),
  });
  assert.equal(reverified.status, 200);
  const changedVerification = (await reverified.json()).presentation.verification;
  assert.equal(changedVerification.verifiedBy, changedOwnerEmail,
    "A stable owner ID must stamp a newly verified badge with its current authenticated email");

  const idOnlyEdit = await fetch("/api/presentation", {
    method: "PUT", headers: ownerIdOnlyHeaders,
    body: JSON.stringify({ presentation: {
      title: "ID-only owner edit", verification: spoofed,
    }, revision: 4 }),
  });
  assert.equal(idOnlyEdit.status, 200,
    "The owner may edit an already verified dashboard when their authenticated email is unavailable");
  assert.deepEqual((await idOnlyEdit.json()).presentation.verification, changedVerification,
    "ID-only owner edits must preserve the original trusted verification without stamping missing attribution");
});

test("hosted reviewed data stores each result separately and stays within D1 statement limits", async () => {
  const rows = Array.from({ length: 35 }, (_, index) => ({ index, evidence: "reviewed ".repeat(4_000) }));
  const seed = {
    title: "Large reviewed dashboard",
    generatedAt: "2026-08-11T12:00:00.000Z",
    queries: {
      reviewed: { source: { label: "Reviewed evidence" }, rows },
      summary: { source: { label: "Summary" }, rows: [{ total: rows.length }] },
    },
  };
  assert.ok(JSON.stringify(seed).length * 2 > 2_000_000,
    "The previous duplicated snapshot record would exceed D1's maximum row size");

  const worker = await loadDashboardWorker(seed);
  const database = fakeDatabase();
  const snapshot = await (await worker.fetch(new Request("https://dashboard.chatgpt.site/api/snapshot"), {
    DB: database,
  })).json();

  assert.deepEqual(snapshot, seed);
  assert.equal(database.state.rows.size, rows.length + 1);
  assert.equal(database.state.queries.size, 2);
  assert.equal(Object.hasOwn(JSON.parse(database.state.snapshot.metadata_json), "queries"), false);
  assert.match(database.state.snapshot.seed_sha256, /^[a-f\d]{64}$/u);
  assert.equal(database.state.maxBoundParameters, 90);
  assert.ok([...database.state.rows.values()].every(({ row_json }) => row_json.length < 2_000_000));
});

test("hosted query updates preserve other results until a newly reviewed seed replaces them", async () => {
  const ownerUserId = "SiteUser_Publisher123";
  const ownerHash = createHash("sha256").update(ownerUserId).digest("hex");
  const seed = {
    title: "Reviewed dashboard",
    generatedAt: "2026-08-11T12:00:00.000Z",
    queries: {
      reviewed: { source: { label: "Reviewed evidence" }, rows: [{ amount: 42 }] },
      unchanged: { source: { label: "Unchanged evidence" }, rows: [{ amount: 7 }] },
    },
  };
  const database = fakeDatabase();
  const headers = { "content-type": "application/json", "oai-authenticated-user-id": ownerUserId };
  const fetch = (worker, path, options = {}) => worker.fetch(
    new Request(`https://dashboard.chatgpt.site${path}`, options), { DB: database },
  );
  const worker = await loadDashboardWorker(seed, ownerHash);
  const reviewedRows = Array.from({ length: 32 }, (_, amount) => ({ amount }));
  const result = await (await fetch(worker, "/api/queries/reviewed", {
    method: "PUT", headers, body: JSON.stringify({ rows: reviewedRows }),
  })).json();

  assert.deepEqual(result.rows, reviewedRows);
  const saved = await (await fetch(worker, "/api/snapshot")).json();
  assert.deepEqual(saved.queries.reviewed.rows, reviewedRows);
  assert.deepEqual(saved.queries.unchanged.rows, seed.queries.unchanged.rows);
  assert.equal(saved.generatedAt, result.generatedAt);

  const reloaded = await loadDashboardWorker(seed, ownerHash);
  assert.deepEqual((await (await fetch(reloaded, "/api/snapshot")).json()).queries.reviewed.rows, reviewedRows);

  const nextSeed = {
    ...seed,
    queries: { reviewed: { source: { label: "New reviewed evidence" }, rows: [{ amount: 100 }] } },
  };
  const redeployed = await loadDashboardWorker(nextSeed, ownerHash);
  assert.deepEqual(await (await fetch(redeployed, "/api/snapshot")).json(), nextSeed);
});

test("an unseeded dashboard remains read-only for every authenticated viewer", async () => {
  const seed = { title: "Unseeded dashboard", queries: {} };
  const worker = await loadDashboardWorker(seed);
  const database = fakeDatabase(seed);
  const headers = {
    "content-type": "application/json",
    "oai-authenticated-user-id": "SiteUser_FirstViewer",
    "oai-authenticated-user-email": "first-viewer@example.com",
  };
  const response = await worker.fetch(new Request("https://dashboard.chatgpt.site/api/presentation", { headers }), {
    DB: database,
  });
  assert.equal((await response.json()).canEdit, false);
  const denied = await worker.fetch(new Request("https://dashboard.chatgpt.site/api/presentation", {
    method: "PUT", headers, body: JSON.stringify({ presentation: { title: "Taken over" }, revision: 0 }),
  }), { DB: database });
  assert.equal(denied.status, 403);
});
