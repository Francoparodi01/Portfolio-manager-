import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  chromeContrastRatio, compositeChromeColor, dataAppChromeColors, parseChromeColor,
} from "../src/chrome-contrast.js";

function assertReadable(colors, surface) {
  for (const variable of [
    "--data-app-safe-chrome-foreground",
    "--data-app-safe-chrome-secondary",
    "--data-app-safe-chrome-positive",
  ]) {
    const contrast = chromeContrastRatio(parseChromeColor(colors[variable]), surface);
    assert.ok(contrast >= 4.5, `${variable} contrast is only ${contrast.toFixed(2)}:1`);
  }
  const publishContrast = chromeContrastRatio(
    parseChromeColor(colors["--data-app-safe-chrome-publish-background"]),
    parseChromeColor(colors["--data-app-safe-chrome-publish-text"]),
  );
  assert.ok(publishContrast >= 4.5, `Publish contrast is only ${publishContrast.toFixed(2)}:1`);
}

test("protected chrome corrects unreadable custom foregrounds, secondary labels, and publishing", () => {
  for (const [background, foreground, secondary] of [
    ["rgb(21, 56, 212)", "rgb(70, 81, 138)", "rgb(70, 81, 138)"],
    ["rgb(23, 62, 47)", "rgb(21, 59, 45)", "rgb(93, 111, 97)"],
    ["rgb(25, 51, 73)", "rgb(25, 51, 73)", "rgb(83, 100, 119)"],
    ["rgb(229, 236, 220)", "rgb(215, 222, 207)", "rgb(192, 196, 183)"],
  ]) {
    const colors = dataAppChromeColors({ background, foreground, secondary });
    assertReadable(colors, parseChromeColor(background));
  }
});

test("protected chrome preserves readable authored brand colors and stable segmented-control ink", () => {
  const colors = dataAppChromeColors({
    background: "rgb(23, 62, 47)",
    foreground: "rgb(248, 241, 222)",
    secondary: "rgb(93, 111, 97)",
  });
  assert.equal(colors["--data-app-safe-chrome-foreground"], "rgb(248, 241, 222)");
  assert.equal(colors["--data-app-safe-chrome-indicator"], "rgb(248, 241, 222)");
  assert.ok(chromeContrastRatio(
    parseChromeColor(colors["--data-app-safe-chrome-indicator-text"]),
    parseChromeColor(colors["--data-app-safe-chrome-indicator"]),
  ) >= 4.5);
});

test("translucent theme colors are composited before protected chrome contrast is selected", () => {
  const underlay = parseChromeColor("rgb(6, 8, 17)");
  const translucent = parseChromeColor("color(srgb 0.0235294 0.0313726 0.0666667 / 0.95)");
  const surface = compositeChromeColor(translucent, underlay);
  assert.deepEqual(surface, [6, 8, 17, 1]);
  const colors = dataAppChromeColors({
    background: "color(srgb 0.0235294 0.0313726 0.0666667 / 0.95)",
    underlay: "rgb(6, 8, 17)",
    foreground: "rgb(237, 247, 255)",
    secondary: "rgb(145, 164, 189)",
  });
  assertReadable(colors, surface);
});

test("verified badges preserve readable brand colors and reject hostile authored positive tokens", async () => {
  const hostile = dataAppChromeColors({
    background: "rgb(23, 62, 47)",
    foreground: "rgb(248, 241, 222)",
    positive: "rgb(23, 62, 47)",
  });
  assert.equal(hostile["--data-app-safe-chrome-positive"], hostile["--data-app-safe-chrome-foreground"],
    "A hostile positive token must fall back to readable protected chrome foreground");
  assertReadable(hostile, parseChromeColor("rgb(23, 62, 47)"));

  const branded = dataAppChromeColors({
    background: "rgb(255, 255, 255)",
    foreground: "rgb(23, 24, 26)",
    positive: "rgb(0, 100, 45)",
  });
  assert.equal(branded["--data-app-safe-chrome-positive"], "rgb(0, 100, 45)",
    "A readable authored positive brand color should remain green");
  assertReadable(branded, parseChromeColor("rgb(255, 255, 255)"));

  const translucent = dataAppChromeColors({
    background: "rgb(23, 62, 47)",
    foreground: "rgb(248, 241, 222)",
    positive: "rgba(248, 241, 222, 0.02)",
  });
  assert.equal(translucent["--data-app-safe-chrome-positive"], translucent["--data-app-safe-chrome-foreground"],
    "An almost transparent positive token cannot bypass protected badge contrast");

  const styles = await readFile(new URL("../src/styles.css", import.meta.url), "utf8");
  assert.match(styles,
    /\.dashboard-verification\.is-verified\s*\{[^}]*color:\s*var\(--data-app-safe-chrome-positive/u,
    "The verified badge must consume the contrast-checked positive chrome token");
  assert.match(styles,
    /button\.dashboard-verification-trigger:hover\s*\{[^}]*color:\s*var\(--data-app-safe-chrome-positive/u,
    "The verification badge hover state must also consume the contrast-checked positive chrome token");
});
