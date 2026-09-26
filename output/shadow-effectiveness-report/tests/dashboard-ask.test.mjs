import assert from "node:assert/strict";
import test from "node:test";

import { canUseDashboardAsk, dashboardAskPrompt } from "../src/dashboard-ask.js";

test("shows inline Ask ChatGPT only outside packaged Codex and local previews", () => {
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "example.com",
    }),
    true,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: true,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "example.com",
    }),
    true,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "edit",
      userAgent: "Mozilla/5.0",
      hostname: "example.com",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "CodexBrowser Mozilla/5.0",
      hostname: "example.com",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "ChatGPTBrowser Mozilla/5.0",
      hostname: "example.com",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "127.0.0.1",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "localhost",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "::1",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "dashboard.localhost",
    }),
    false,
  );
  assert.equal(
    canUseDashboardAsk({
      canEdit: false,
      mode: "view",
      userAgent: "Mozilla/5.0",
      hostname: "terminal.local",
    }),
    false,
  );
});

test("formats compact dashboard context before the labeled question", () => {
  const prompt = dashboardAskPrompt({
    question: "  Why did this decline?  ",
    dashboardTitle: "Product adoption and engagement",
    dashboardUrl: "https://example.com/dashboard",
    componentKind: "chart",
    componentTitle: "Weekly growth drivers",
    selectedContext: "Churn · -165",
  });

  assert.equal(
    prompt,
    [
      "Dashboard: Product adoption and engagement",
      "Dashboard URL: https://example.com/dashboard",
      "Chart: Weekly growth drivers",
      "Selected context: Churn · -165",
      "Question: Why did this decline?",
    ].join("\n"),
  );
});

test("uses the rich Sites project reference without repeating the raw dashboard URL", () => {
  const prompt = dashboardAskPrompt({
    question: "Why did this number go up?",
    dashboardTitle: "Data `quality` [review]\nDashboard",
    dashboardUrl: "https://dashboard.chatgpt.site/?token=private#selection",
    dashboardProjectId: "appgprj_123",
    componentKind: "chart",
    componentTitle: "Active accounts over time",
    selectedContext: "2026-03-30 · Active Users · 6K",
    selectedContextLabel: "Selected point",
  });

  assert.equal(
    prompt,
    [
      "Answer this question about [Data `quality` \\[review\\] Dashboard](sites-project://appgprj_123):",
      "Chart: Active accounts over time",
      "Selected point: 2026-03-30 · Active Users · 6K",
      "Question: Why did this number go up?",
    ].join("\n"),
  );
  assert.doesNotMatch(prompt, /Dashboard URL:|token=private|#selection/u);
});

test("Sites handoff references preserve punctuation with canonical title escaping", () => {
  for (const [title, escapedTitle] of [
    ["Revenue: *growth* + 12%!", "Revenue: *growth* + 12%!"],
    ["Data `quality`", "Data `quality`"],
    ["[Overview](quarterly)", "\\[Overview\\]\\(quarterly)"],
    ["North \\ South", "North \\\\ South"],
    ["Weekly\r\nactivity", "Weekly activity"],
  ]) {
    const prompt = dashboardAskPrompt({
      question: "What changed?",
      dashboardTitle: title,
      dashboardProjectId: "appgprj_123",
      selectedContext: "Entire dashboard",
    });
    assert.equal(
      prompt.split("\n")[0],
      `Answer this question about [${escapedTitle}](sites-project://appgprj_123):`,
      title,
    );
  }
});

test("omits unavailable or unsafe optional context", () => {
  assert.equal(
    dashboardAskPrompt({
      question: "What changed?",
      dashboardTitle: "Engagement",
      dashboardUrl: "file:///tmp/dashboard.html",
      dashboardProjectRoot: "/workspace/revenue-dashboard",
      selectedContext: "Selected text · “Retention”",
    }),
    [
      "Dashboard: Engagement",
      "Dashboard project directory: /workspace/revenue-dashboard",
      "Selected context: Selected text · “Retention”",
      "Question: What changed?",
    ].join("\n"),
  );
  assert.equal(
    dashboardAskPrompt({
      question: "   ",
      dashboardTitle: "Engagement",
      selectedContext: "Retention",
    }),
    null,
  );
});
