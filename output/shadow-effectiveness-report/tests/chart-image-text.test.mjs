import assert from "node:assert/strict";
import test from "node:test";
import { wrapChartImageText } from "../src/chart-image.js";

test("copied chart notes wrap long unbroken labels without clipping", () => {
  const context = { measureText: (text) => ({ width: [...text].length * 8 }) };
  for (const text of ["A".repeat(160), "Evidence " + "界".repeat(200), "Short readable text"]) {
    const lines = wrapChartImageText(context, text, 120);
    assert.ok(lines.every((line) => context.measureText(line).width <= 120));
    assert.equal(lines.join("").replace(/\s/gu, ""), text.replace(/\s/gu, ""));
  }
});
