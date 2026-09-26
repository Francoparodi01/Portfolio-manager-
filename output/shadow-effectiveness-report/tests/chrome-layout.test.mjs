import assert from "node:assert/strict";
import test from "node:test";

import { dataAppChromeLayout } from "../src/chrome-layout.js";

test("protected chrome spans the viewport using ordinary dashboard gutters", () => {
  assert.deepEqual(dataAppChromeLayout({
    viewportWidth: 1440, paddingLeft: 40, paddingRight: 40,
  }), {
    "--data-app-safe-chrome-width": "100%",
    "--data-app-safe-chrome-margin": "0px",
    "--data-app-safe-chrome-inset-start": "40px",
    "--data-app-safe-chrome-inset-end": "40px",
  });
});

test("protected chrome keeps viewport gutters for full-bleed authored content", () => {
  const layout = dataAppChromeLayout({ viewportWidth: 1920 });
  assert.equal(layout["--data-app-safe-chrome-inset-start"], "32px");
  assert.equal(layout["--data-app-safe-chrome-inset-end"], "32px");
});

test("protected chrome remains full-width outside narrow editorial reports", () => {
  const layout = dataAppChromeLayout({
    viewportWidth: 1440, paddingLeft: 36, paddingRight: 36,
  });
  assert.equal(layout["--data-app-safe-chrome-inset-start"], "36px");
  assert.equal(layout["--data-app-safe-chrome-inset-end"], "36px");
});

test("protected chrome preserves usable actions on narrow and asymmetric layouts", () => {
  const narrow = dataAppChromeLayout({ viewportWidth: 320 });
  assert.equal(narrow["--data-app-safe-chrome-inset-start"], "16px");
  assert.equal(narrow["--data-app-safe-chrome-inset-end"], "16px");
  const asymmetric = dataAppChromeLayout({
    viewportWidth: 1440, paddingLeft: 24, paddingRight: 40,
  });
  assert.equal(asymmetric["--data-app-safe-chrome-inset-start"], "24px");
  assert.equal(asymmetric["--data-app-safe-chrome-inset-end"], "40px");
});
