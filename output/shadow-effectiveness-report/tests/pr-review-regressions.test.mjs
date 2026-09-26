import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { filterReviewedRows } from "../src/use-data-app.js";
import {
  aggregateSessions,
  chronologicalRows,
  progressPercent,
  regionalTotals,
} from "../src/content/dashboard/dashboard-data.js";
import { compareTableValues, distributionBuckets, distributionPercentiles, statusTone } from "../src/charting/table-data.js";
const data = JSON.parse(readFileSync(new URL("../src/data.json", import.meta.url)));

test("table sorting compares decimal percentages numerically and keeps missing cells last", () => {
  const values = [0.004321, .75, .06, .04, -.02, null];
  assert.deepEqual([...values].sort((a, b) => compareTableValues(a, b, true)), [.75, .06, .04, .004321, -.02, null]);
  assert.deepEqual([...values].sort(compareTableValues), [-.02, .004321, .04, .06, .75, null]);
  assert.deepEqual(["6%", "75%", "0%", "-2%"].sort(compareTableValues), ["-2%", "0%", "6%", "75%"]);
  assert.deepEqual(["$1,200.50", "$9.75", "−$20"].sort(compareTableValues), ["−$20", "$9.75", "$1,200.50"]);
  assert.deepEqual(["Plan 10", "Plan 2"].sort(compareTableValues), ["Plan 2", "Plan 10"]);
});

test("heatmap sessions preserve all product slices", () => {
  const rows = data.queries.engagement_intensity.rows;
  const cells = aggregateSessions(rows, ["hour", "day"]);
  assert.equal(cells.length, 84);
  assert.equal(
    cells.reduce((sum, row) => sum + row.sessions, 0),
    rows.reduce((sum, row) => sum + row.sessions, 0),
  );
  const first = cells[0];
  assert.equal(
    first.sessions,
    rows.filter((row) => row.hour === first.hour && row.day === first.day).reduce((sum, row) => sum + row.sessions, 0),
  );
});
test("scenario history is chronologically ordered without mutation", () => {
  const rows = [{ week: "2026-07-27" }, { week: "2026-07-13" }, { week: "2026-07-20" }];
  assert.deepEqual(
    chronologicalRows(rows).map((row) => row.week),
    ["2026-07-13", "2026-07-20", "2026-07-27"],
  );
  assert.equal(rows[0].week, "2026-07-27");
});
test("product history reconciles with reviewed totals", () => {
  const products = data.queries.product_history.rows;
  for (const row of data.queries.usage_summary.rows) {
    const matches = products.filter(
      (item) =>
        item.week === row.week &&
        (row.segment === "all" || item.segment === row.segment) &&
        (row.region === "all" || item.region === row.region),
    );
    assert.equal(
      matches.reduce((sum, item) => sum + item.activeUsers, 0),
      row.activeUsers,
      JSON.stringify(row),
    );
  }
});
test("target progress uses its full 125 percent scale", () => {
  assert.equal(progressPercent(1.25), 100);
  assert.ok(Math.abs(progressPercent(1.1) - 88) < 1e-10);
  assert.equal(progressPercent(2), 100);
  assert.equal(progressPercent(-1), 0);
});
test("range endpoint follows sparse dimension and aggregate selection", () => {
  const rows = [
    { week: "2026-07-20", segment: "A" },
    { week: "2026-07-27", segment: "B" },
  ];
  const defs = [
    { id: "date", field: "week", mode: "through" },
    { id: "segment", field: "segment" },
  ];
  const filters = { date: "2026-07-01..2026-07-31", segment: "A" };
  assert.deepEqual(filterReviewedRows(rows, defs, filters, "q"), [rows[0]]);
  assert.deepEqual(filterReviewedRows(rows, [...defs].reverse(), filters, "q"), [rows[0]]);
  assert.deepEqual(filterReviewedRows(rows, defs, filters, "q", ["week"]), [rows[0]]);
  const aggregates = [...rows, { week: "2026-07-13", segment: "all" }];
  assert.deepEqual(filterReviewedRows(aggregates, defs, { ...filters, segment: "all" }, "q"), [aggregates[2]]);
});
test("unknown map regions retain reviewed totals without invented coordinates", () => {
  const [region] = regionalTotals([{ region: "Other", activeUsers: 42, riskTier: "Elevated" }], {});
  assert.equal(region.activeUsers, 42);
  assert.equal(region.elevated, 1);
  assert.equal(region.color, "var(--secondary)");
  assert.equal(region.x, undefined);
});
test("dashboard table classifies risk labels semantically", () => {
  for (const label of ["Low", "Low risk", "No risk"]) assert.equal(statusTone(label), "positive");
  assert.equal(statusTone("Moderate risk"), "warning");
  for (const label of ["High risk", "Elevated"]) assert.equal(statusTone(label), "negative");
});
test("distribution scans source rows once and includes maximum endpoint", () => {
  let reads = 0;
  const rows = Array.from({ length: 10000 }, (_, index) => ({
    get value() {
      reads++;
      return index % 101;
    },
  }));
  assert.equal(distributionBuckets(rows, "value").length, 12);
  assert.ok(reads <= rows.length * 2);
  assert.equal(distributionBuckets([{ value: 100 }], "value")[11], 100);
});
test("distribution ranks exclude missing cells while preserving zero and tied reviewed values", () => {
  const missing = [null, undefined, "", " ", "unknown", NaN, Infinity, "9".repeat(400), false];
  const valid = [75, "75", 90, 0, 30];
  const rows = [...valid, ...missing].map((value) => ({ value }));
  assert.deepEqual(distributionPercentiles(rows, "value"), new Map([
    [0, 20], [30, 40], [75, 80], ["75", 80], [90, 100],
  ]));
  assert.deepEqual(distributionBuckets(rows, "value"),
    distributionBuckets(valid.map((value) => ({ value })), "value"));
  assert.equal(distributionPercentiles(missing.map((value) => ({ value })), "value").size, 0);
});
test("heatmap axis-title switches remain independent", () => {
  const source = readFileSync(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8");
  assert.match(source, /const showXAxisTitle = spec.showXAxisLabel !== false;/);
  assert.match(source, /const heatmapYAxisTitle = spec.yLabel \?\? label\(heatmapGroup\);/);
  const editor = readFileSync(new URL("../src/components/ChartExplorer.jsx", import.meta.url), "utf8");
  assert.doesNotMatch(editor.match(/const chartsWithoutAxes = .*;/)?.[0] ?? "", /"heatmap"/);
});
