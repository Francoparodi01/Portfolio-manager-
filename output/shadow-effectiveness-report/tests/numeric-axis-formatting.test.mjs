import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  compact,
  displayValue,
  mergeNumericAxisTicks,
  numericAxisFormatter,
  percentage,
  ratioMetric,
} from "../src/charting/chart-theme.js";

const labels = (values, options = {}) => values.map(numericAxisFormatter(values, { locale: "en-US", ...options }));

test("ordinary numeric axes keep grouped integers distinct across compact boundaries", () => {
  assert.deepEqual(labels([960, 980, 1000, 1020, 1040]), ["960", "980", "1,000", "1,020", "1,040"]);
  assert.deepEqual(labels([-1040, -1020, -1000, -980, -960]), ["-1,040", "-1,020", "-1,000", "-980", "-960"]);
  assert.deepEqual(labels([-0.0002, 0, 0.0002]), ["-0.0002", "0", "0.0002"]);
  assert.deepEqual(labels([-0.0002, 0]), ["-0.0002", "0"], "Rounded negative zero must not count as distinct");
});

test("large-value axes retain compact labels unless a narrow range needs exact grouped values", () => {
  assert.deepEqual(labels([960e6, 980e6, 1e9, 1.02e9, 1.04e9]), ["960M", "980M", "1B", "1.02B", "1.04B"]);
  assert.deepEqual(labels([1e9, 1e9 + 25, 1e9 + 50]), ["1,000,000,000", "1,000,000,025", "1,000,000,050"]);
  assert.deepEqual(labels([999950, 1e6, 1000050]), ["999,950", "1,000,000", "1,000,050"]);
  const extreme = labels([1e100, 1.0000000000000002e100, 1.0000000000000004e100]);
  assert.equal(new Set(extreme).size, 3);
  assert.ok(extreme.every((value) => value.includes("E")));
});

test("percentage axes increase precision without changing percentage scale or signs", () => {
  assert.deepEqual(labels([0.5001, 0.5002, 0.5003], { percent: true }), ["50.01%", "50.02%", "50.03%"]);
  assert.deepEqual(labels([0.0995, 0.1, 0.1005], { percent: true }), ["9.95%", "10%", "10.05%"]);
  assert.deepEqual(labels([-0.0002, 0, 0.0002], { percent: true }), ["-0.02%", "0%", "0.02%"]);
  assert.deepEqual(labels([0, 0.25, 0.5, 0.75, 1], { percent: true }), ["0%", "25%", "50%", "75%", "100%"]);
  assert.equal(ratioMetric("conversion", [0.0995, 0.1005]), true);
  assert.equal(ratioMetric("conversion", [-0.0002, 0.0002]), false, "Metric inference is unchanged");
});

test("unknown or degenerate tick sets use a non-collapsing fallback", () => {
  for (const values of [[], [1e9], [null, NaN, Infinity, -Infinity, -0, 0, 0]]) {
    const format = numericAxisFormatter(values, { locale: "en-US" });
    assert.notEqual(format(1e9), format(1e9 + 25));
    assert.equal(format(-0), "0");
    assert.equal(format(null), "");
    assert.equal(format(NaN), "");
    assert.equal(format(Infinity), "");
  }
  const tiny = labels([-1e-100, 0, 1e-100]);
  assert.equal(new Set(tiny).size, 3);
  assert.equal(tiny[1], "0");
});

test("axis tick history is monotone within a domain and resets for a different axis or mode", () => {
  const key = JSON.stringify(["y", 0, [960, 1040], false]);
  const initial = mergeNumericAxisTicks(null, key, [1040, 960, 1000, 980, 1020, 1000, NaN]);
  assert.deepEqual(initial.values, [960, 980, 1000, 1020, 1040]);
  assert.equal(
    mergeNumericAxisTicks(initial, key, [1040, 960]),
    initial,
    "Collision filtering must not reduce precision or churn formatter identity",
  );
  const expanded = mergeNumericAxisTicks(initial, key, [1010]);
  assert.deepEqual(expanded.values, [960, 980, 1000, 1010, 1020, 1040]);
  const otherAxis = mergeNumericAxisTicks(expanded, "weekly-change", [1e9, 1e9 + 25]);
  assert.deepEqual(otherAxis.values, [1e9, 1e9 + 25]);
  assert.deepEqual(initial.values, [960, 980, 1000, 1020, 1040], "History inputs are immutable");
});

test("legacy KPI and tooltip formatters are unchanged", () => {
  assert.equal(compact(1020), "1K");
  assert.equal(displayValue(1020), "1,020");
  assert.equal(percentage(0.5001), "+50%");
});

test("all canonical numeric axes use public tick/domain hooks without changing line styling", async () => {
  const renderer = await readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8");
  assert.match(renderer, /function NumericXAxis[\s\S]*?useXAxisDomain\(xAxisId\),\s*useXAxisTicks\(xAxisId\)/u);
  assert.match(renderer, /function NumericYAxis[\s\S]*?useYAxisDomain\(yAxisId\),\s*useYAxisTicks\(yAxisId\)/u);
  assert.match(renderer, /getNiceTickValues\(bounds, tickCount, allowDecimals\)/u);
  assert.match(renderer, /mergeNumericAxisTicks\(history, key, candidates\.values\)/u);
  assert.match(renderer, /\[signature, percent\]/u);
  assert.match(renderer, /const ValueYAxis = horizontal \? YAxis : NumericYAxis/u);
  assert.match(renderer, /const ValueXAxis = horizontal \|\| quantitativeXAxis \? NumericXAxis : temporalValues\.length \? TemporalXAxis : XAxis/u);
  assert.match(renderer, /numericXDomain\[0\],[\s\S]*?getNiceTickValues\(numericXDomain, 5, !integerXAxis\)/u,
    "Quantitative x-axes should retain reviewed endpoints and readable domain-aware intermediate ticks");
  assert.match(renderer, /const ScatterXAxis = numeric\.includes\(x\) \? NumericXAxis : XAxis/u);
  assert.match(renderer, /<NumericYAxis\s+allowDecimals=\{false\}/u);
  assert.match(renderer, /<NumericYAxis\s+domain=\{\[0, whiskerMaximum \* 1\.12\]\}/u);
  assert.match(renderer, /<NumericYAxis\s+yAxisId="weekly-change"\s+orientation="right"\s+width="auto"/u);
  assert.match(renderer, /const formatNumericAxisTick = numericAxisFormatter/u);
  assert.match(renderer, /type="monotone"\s+dataKey=\{field\}/u);
  assert.match(renderer, /connectNulls=\{false\}/u);
  assert.match(renderer, /<IsolatedLineDot indexes=\{isolatedPoints\.get\(field\)\} \/> : false/u);
});
