import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { funnelLayout, funnelRibbonSegments, funnelStages } from "../src/charting/chart-transforms.js";

import {
  categoryAxisLayout,
  chartDataShape,
  chartSpecKeys,
  groupAdditiveCategories,
  groupAdditiveSeries,
  isTemporalCategory,
  orderCalendarRows,
  secondaryAxisFields,
  orderedDistribution,
  projectChartSpec,
  rankedListCapacity,
  resolvedChartType,
  scatterTooltipIdentityField,
  temporalAxisTicks,
  visibleGroupedCategories,
} from "../src/charting/chart-data-shape.js";
import {
  boxPlots,
  boxPlotSummaryFields,
  heatmap,
  histogram,
  pivot,
  sankeyGraph,
  waterfall,
} from "../src/charting/chart-transforms.js";
import { funnelStageColor, tick } from "../src/charting/chart-theme.js";

test("calendar categories use weekday and month chronology without rewriting reviewed data", () => {
  const rows = ["Fri", "Mon", "Sat", "Sun", "Thu", "Tue", "Wed"].map((day) => ({ day, value: 1 }));
  assert.deepEqual(orderCalendarRows(rows, "day").map((row) => row.day), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]);
  assert.equal(rows[0].day, "Fri");
  assert.equal(orderCalendarRows(rows, "day", { sortOrder: "original" }), rows);
  assert.equal(orderCalendarRows(rows, "day", { sortOrder: "descending" }), rows);
  assert.equal(orderCalendarRows(rows, "day", { categoryOrder: ["Sun", "Mon"] })[0].day, "Sun");
  const repeated = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "Mon", "Tue"]
    .map((day, index) => ({ day, id: index + 1 }));
  assert.equal(orderCalendarRows(repeated, "day"), repeated,
    "Repeated weekdays can span multiple weeks and must retain their reviewed chronology");
  assert.equal(orderCalendarRows(repeated, "day", { sortOrder: "original" }), repeated);
  assert.equal(orderCalendarRows([{ day: "Monday" }, { day: "Mon" }], "day")[0].day, "Monday",
    "Weekday aliases do not establish unique calendar grain");
  assert.deepEqual(orderCalendarRows([{ m: "2026-01" }, { m: "2025-12" }], "m").map((row) => row.m), ["2025-12", "2026-01"]);
  assert.equal(isTemporalCategory("2026-08"), true);
  assert.equal(isTemporalCategory("2026-13"), false);
  assert.equal(tick("2026-08"), "Aug 2026");
  assert.equal(tick("2026-08-21", { includeYear: true }), "Aug 21, 2026");
});

test("different-scale measures get separate axes but category series and stacks do not", () => {
  const rows = [{ energy: 3000, cost: 30, rate: .75 }, { energy: -1000, cost: -10, rate: .5 }];
  const spec = { type: "bar", x: "city", y: "energy", fields: ["energy", "cost"] };
  assert.deepEqual(secondaryAxisFields(spec, rows, spec.fields), ["cost"]);
  assert.deepEqual(secondaryAxisFields({ ...spec, rightAxisFields: [] }, rows, spec.fields), []);
  assert.deepEqual(secondaryAxisFields({ ...spec, rightAxisFields: ["energy"] }, rows, spec.fields), ["energy"]);
  assert.deepEqual(secondaryAxisFields({ ...spec, series: "plan" }, rows, spec.fields), []);
  assert.deepEqual(secondaryAxisFields({ ...spec, type: "stackedBar" }, rows, spec.fields), []);
  assert.deepEqual(secondaryAxisFields(spec, [{ energy: 30, cost: 20 }], spec.fields), []);
  assert.deepEqual(secondaryAxisFields(spec, [{ energy: 0, cost: 0 }], spec.fields), []);
  assert.deepEqual(secondaryAxisFields(spec, [{ energy: null, cost: 20 }], spec.fields), []);
  assert.deepEqual(secondaryAxisFields(spec, [{ energy: 3, rate: .75 }], ["energy", "rate"]), ["rate"]);
});

test("line chart renderer and editor cannot enable an area fill", async () => {
  const renderer = await readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8");
  const lineBranch = renderer.split('} else if (["line", "sparkline"].includes(type)) {')[1].split('} else if (["area", "stackedArea"].includes(type)) {')[0];
  assert.doesNotMatch(lineBranch, /<Area\b|showArea/u);
  const editor = await readFile(new URL("../src/components/ChartExplorer.jsx", import.meta.url), "utf8");
  assert.doesNotMatch(editor, /Fill area|showArea/u);
});

function project(spec, rows) {
  const shape = chartDataShape(spec, rows);
  const projected = rows.map((row) =>
    Object.fromEntries(
      shape.rowFields.filter((field) => Object.hasOwn(row, field)).map((field) => [field, row[field]]),
    ),
  );
  return { shape, rows: projected };
}

test("crowded additive categories retain reviewed leaders and reconcile the exact Other total", () => {
  const rows = [
    { plan: "Plus", users: 4791905, reviewedMetadata: "preserved" },
    { plan: "Pro Lite", users: 410586 },
    { plan: "Pro", users: 389654 },
    { plan: "Team", users: 313530 },
    { plan: "Free", users: 181026 },
    { plan: "Business", users: 96976 },
    { plan: "Enterprise usage", users: 5173 },
    { plan: "Education", users: 3594 },
    { plan: "Enterprise", users: 2587 },
    { plan: "Self-serve usage", users: 1559 },
    { plan: "Education Plus", users: 251 },
    { plan: "Education Pro", users: 58 },
  ];
  const result = groupAdditiveCategories(rows, { categoryField: "plan", valueField: "users", enabled: true });
  assert.equal(result.length, 7);
  assert.equal(result[0], rows[0], "Reviewed leading rows retain their original fields");
  assert.deepEqual(result.at(-1), { plan: "Other", users: 13222 });
  assert.equal(result.reduce((total, row) => total + row.users, 0), rows.reduce((total, row) => total + row.users, 0));
  assert.equal(rows.length, 12, "Grouping must not modify the reviewed source rows");

  const preserved = groupAdditiveCategories(rows, {
    categoryField: "plan",
    valueField: "users",
    preserveCategories: ["Education Pro"],
    enabled: true,
  });
  assert.ok(preserved.some((row) => row.plan === "Education Pro"));
  assert.equal(preserved.reduce((total, row) => total + row.users, 0), rows.reduce((total, row) => total + row.users, 0));
});

test("Other grouping declines non-additive, ambiguous, and explicitly preserved source data", () => {
  const rows = Array.from({ length: 8 }, (_, index) => ({ category: `Category ${index}`, count: index + 1 }));
  const options = { categoryField: "category", valueField: "count", enabled: true };
  assert.equal(groupAdditiveCategories(rows, { categoryField: "category", valueField: "count" }), rows,
    "Unknown category populations must never be combined without explicit reviewed additivity");
  const overlappingProducts = rows.map(({ category, count }) => ({ product: category, users: count }));
  assert.equal(groupAdditiveCategories(overlappingProducts, { categoryField: "product", valueField: "users" }),
    overlappingProducts, "Overlapping product audiences must not be inferred as mutually exclusive");
  assert.equal(groupAdditiveCategories(rows, { ...options, enabled: false }), rows);
  assert.equal(groupAdditiveCategories(rows.slice(0, 7), options).length, 7);
  assert.equal(groupAdditiveCategories(rows, { ...options, maxCategories: 8 }), rows);
  assert.equal(groupAdditiveCategories(rows, { ...options, preserveCategories: rows.map((row) => row.category) }), rows);

  for (const field of ["adoptionRate", "percentage", "averageUsers", "users_per_account", "audienceShare"]) {
    const nonadditive = rows.map(({ category, count }) => ({ category, [field]: count / 10 }));
    assert.equal(groupAdditiveCategories(nonadditive, { categoryField: "category", valueField: field, enabled: true }),
      nonadditive);
  }
  for (const field of ["feature", "productExperience", "surface", "funnelStage"]) {
    const overlapping = rows.map(({ category, count }) => ({ [field]: category, count }));
    assert.equal(groupAdditiveCategories(overlapping, { categoryField: field, valueField: "count" }),
      overlapping);
  }
  const reviewedFeatureObservations = rows.map(({ category, count }) => ({ feature: category, observations: count }));
  assert.equal(groupAdditiveCategories(reviewedFeatureObservations, {
    categoryField: "feature", valueField: "observations", enabled: true,
  }).length, 7, "Explicitly verified additive observations must not be rejected because of a dimension's name");

  for (const invalid of [
    [...rows.slice(0, -1), rows[0]],
    [...rows.slice(0, -1), { category: "Other", count: 8 }],
    [...rows.slice(0, -1), { category: "Invalid", count: -1 }],
    [...rows.slice(0, -1), { category: "Invalid", count: Number.NaN }],
  ]) assert.equal(groupAdditiveCategories(invalid, options), invalid);
});

test("crowded additive plan histories preserve stable leaders and reconcile Other for every reviewed period", () => {
  const plans = ["Plus", "Pro", "Team", "Free", "Business", "Enterprise", "Education", "Education Pro"];
  const rows = ["2026-08-18", "2026-08-19"].flatMap((period, dateIndex) => plans.map((plan, index) => ({
    period,
    plan,
    users: (plans.length - index) * (dateIndex + 1),
    reviewedMetadata: `${period}:${plan}`,
  })));
  const options = { groupField: "period", categoryField: "plan", valueField: "users", enabled: true };
  assert.equal(groupAdditiveSeries(rows, { groupField: "period", categoryField: "plan", valueField: "users" }), rows,
    "Even familiar category names cannot prove that reviewed audiences are mutually exclusive");
  const grouped = groupAdditiveSeries(rows, options);
  assert.equal(grouped.length, 14);
  for (const period of ["2026-08-18", "2026-08-19"]) {
    const reviewed = rows.filter((row) => row.period === period);
    const displayed = grouped.filter((row) => row.period === period);
    assert.equal(displayed.length, 7);
    assert.equal(displayed.reduce((total, row) => total + row.users, 0),
      reviewed.reduce((total, row) => total + row.users, 0));
    assert.deepEqual(displayed.at(-1), {
      period,
      plan: "Other",
      users: reviewed.slice(6).reduce((total, row) => total + row.users, 0),
    });
    assert.equal(displayed[0], reviewed[0], "Reviewed leading rows retain their source identity");
  }
  assert.equal(rows.length, 16, "Grouping does not modify reviewed source rows");

  const preserved = groupAdditiveSeries(rows, { ...options, preserveCategories: ["Education Pro"] });
  assert.ok(preserved.some((row) => row.plan === "Education Pro"));
  assert.equal(groupAdditiveSeries(rows, { ...options, enabled: false }), rows);
  assert.equal(groupAdditiveSeries(rows, { ...options, maxCategories: 8 }), rows);
  assert.equal(groupAdditiveSeries(rows, { ...options, valueField: "adoptionRate" }), rows);

  for (const categoryField of ["feature", "experience", "surface", "product", "category"]) {
    const unverified = rows.map(({ plan, ...row }) => ({ ...row, [categoryField]: plan }));
    assert.equal(groupAdditiveSeries(unverified, { ...options, categoryField, enabled: false }), unverified);
  }
  for (const invalid of [
    [...rows, rows[0]],
    rows.map((row, index) => index ? row : { ...row, plan: "Other" }),
    rows.map((row, index) => index ? row : { ...row, users: -1 }),
  ]) assert.equal(groupAdditiveSeries(invalid, options), invalid);
});

test("grouped totals exclude filtered source categories without changing reviewed leaders", () => {
  const categories = ["Plus", "Pro", "Team", "Free", "Business", "Enterprise", "Education", "Education Pro"];
  const categoryRows = categories.map((plan, index) => ({ plan, users: categories.length - index }));
  const groupedCategories = groupAdditiveCategories(categoryRows, {
    categoryField: "plan", valueField: "users", enabled: true,
  });
  const visibleCategories = new Set(categories.filter((plan) => plan !== "Education"));
  const filteredCategories = visibleGroupedCategories(categoryRows, groupedCategories, {
    categoryField: "plan", valueField: "users", visibleCategories,
  });
  assert.deepEqual(filteredCategories.at(-1), { plan: "Other", users: 1 },
    "A hidden constituent must not remain counted in the visible donut slice");
  assert.deepEqual(filteredCategories.slice(0, -1), groupedCategories.slice(0, -1),
    "Filtering an Other constituent must preserve the reviewed leader identities");

  const seriesRows = ["2026-08-18", "2026-08-19"].flatMap((period, index) =>
    categoryRows.map((row) => ({ period, ...row, users: row.users * (index + 1) })));
  const groupedSeries = groupAdditiveSeries(seriesRows, {
    groupField: "period", categoryField: "plan", valueField: "users", enabled: true,
  });
  const filteredSeries = visibleGroupedCategories(seriesRows, groupedSeries, {
    groupField: "period", categoryField: "plan", valueField: "users", visibleCategories,
  });
  assert.deepEqual(filteredSeries.filter((row) => row.plan === "Other"), [
    { period: "2026-08-18", plan: "Other", users: 1 },
    { period: "2026-08-19", plan: "Other", users: 2 },
  ], "Each reviewed period must independently exclude hidden constituent categories");
});

test("explicitly reviewed additive series group regardless of dimension names", () => {
  const channels = ["Direct", "Organic", "Referral", "Paid", "Email", "Partner", "Community", "Events"];
  const rows = ["2026-08-18", "2026-08-19"].flatMap((period) => channels.map((channel, index) => ({
    period, channel, signups: channels.length - index,
  })));
  const grouped = groupAdditiveSeries(rows, {
    groupField: "period", categoryField: "channel", valueField: "signups", enabled: true,
  });
  for (const period of ["2026-08-18", "2026-08-19"]) {
    const reviewed = rows.filter((row) => row.period === period);
    const displayed = grouped.filter((row) => row.period === period);
    assert.equal(displayed.length, 7);
    assert.equal(displayed.at(-1).channel, "Other");
    assert.equal(displayed.reduce((total, row) => total + row.signups, 0),
      reviewed.reduce((total, row) => total + row.signups, 0));
  }
});

test("reviewed quarter labels choose current leaders without lexicographic period sorting", () => {
  const plans = ["Legacy", "Standard", "Business", "Growth", "Current", "Emerging", "Pilot", "New"];
  const rows = ["Q4 2025", "Q1 2026"].flatMap((period, periodIndex) => plans.map((plan, index) => ({
    period,
    plan,
    users: periodIndex ? (index + 1) * 10 : plans.length - index,
  })));
  const grouped = groupAdditiveSeries(rows, {
    groupField: "period", categoryField: "plan", valueField: "users", maxCategories: 3, enabled: true,
  });
  assert.deepEqual(grouped.filter((row) => row.period === "Q1 2026").map((row) => row.plan),
    ["Pilot", "New", "Other"],
    "Latest-quarter leaders must follow reviewed chronological order rather than sorting quarter names");

  const numericRows = [20, 10].flatMap((period, periodIndex) => plans.map((plan, index) => ({
    period,
    plan,
    users: periodIndex ? index + 1 : (plans.length - index) * 10,
  })));
  const numeric = groupAdditiveSeries(numericRows, {
    groupField: "period", categoryField: "plan", valueField: "users", maxCategories: 3, enabled: true,
  });
  assert.deepEqual(numeric.filter((row) => row.period === 20).map((row) => row.plan),
    ["Legacy", "Standard", "Other"], "Numeric periods must continue to select their highest reviewed value");
});

test("stable grouped leaders preserve major historical spikes across the reviewed window", () => {
  const channels = ["Earlier leader", "Current leader", "Steady", "Small A", "Small B", "Small C", "Small D", "Small E"];
  const rows = ["Q4 2025", "Q1 2026"].flatMap((period, periodIndex) => channels.map((channel, index) => ({
    period,
    channel,
    signups: channel === "Earlier leader" ? periodIndex ? 1 : 1_000
      : channel === "Current leader" ? periodIndex ? 900 : 2
        : 50 - index,
  })));
  const grouped = groupAdditiveSeries(rows, {
    groupField: "period", categoryField: "channel", valueField: "signups", maxCategories: 3, enabled: true,
  });
  for (const period of ["Q4 2025", "Q1 2026"]) {
    const reviewed = rows.filter((row) => row.period === period);
    const displayed = grouped.filter((row) => row.period === period);
    assert.deepEqual(displayed.map((row) => row.channel), ["Earlier leader", "Current leader", "Other"],
      "One stable leader set must preserve both earlier spikes and currently important categories");
    assert.equal(displayed.reduce((total, row) => total + row.signups, 0),
      reviewed.reduce((total, row) => total + row.signups, 0));
  }
});

test("shareable chart specs retain safe grouping and distribution options", () => {
  assert.deepEqual(projectChartSpec({
    type: "pie",
    x: "plan",
    y: "users",
    groupOther: false,
    maxCategories: 9,
    distribution: true,
    preserveCategories: ["Enterprise", { privateMetadata: true }],
  }), {
    type: "pie",
    x: "plan",
    y: "users",
    groupOther: false,
    maxCategories: 9,
    distribution: true,
    preserveCategories: ["Enterprise"],
  });
});

test("reviewed grouping is an explicit chart authoring decision", async () => {
  const renderer = await readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8");
  assert.equal((renderer.match(/enabled:\s*spec\.groupOther\s*===\s*true/gu) ?? []).length, 2,
    "Donuts and long-form series must require an explicit reviewed grouping decision");
  assert.match(renderer, /groupedCategories\.some\(\(category\)\s*=>\s*controlledVisible\.has\(category\)\)/u,
    "A synthetic Other category must inherit visibility from its reviewed source categories");
  assert.match(renderer, /String\(value\)\s*===\s*"Other"\s*&&\s*groupedCategories\.length\s*\?\s*groupedCategories/u,
    "Persisted grouping visibility must expand Other back into its reviewed source identities");
  assert.match(renderer, /onVisibleSeriesChange\(visibleCategories\)/u,
    "Controlled chart visibility must never persist a synthetic Other identity");
  assert.match(renderer,
    /const data\s*=\s*numericTrend\s*\?\s*\[\.\.\.sourceData\]\.sort\(\(left,\s*right\)\s*=>\s*left\[x\]\s*-\s*right\[x\]\)/u,
    "Quantitative line and area charts must connect their reviewed rows in ascending x order");
});

test("chart controls preserve reviewed identities, complete labels, and accessible selections", async () => {
  const [renderer, explorer, frame, sortable] = await Promise.all([
    readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/components/ChartExplorer.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/charting/ChartFrame.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/content/shared/SortableDashboardLayout.jsx", import.meta.url), "utf8"),
  ]);
  const categoryTick = renderer.slice(renderer.indexOf("function CategoryAxisTick"),
    renderer.indexOf("function ", renderer.indexOf("function CategoryAxisTick") + 1));
  assert.doesNotMatch(categoryTick, /return null/u,
    "Categorical axes must abbreviate reviewed labels without silently dropping categories");
  assert.match(explorer, /aria-pressed=\{!customSelected\s*&&\s*color\s*===\s*token\}/u,
    "Theme colors sharing the same readable label must still have only one selected token");
  assert.match(explorer, /const current = draft \?\? localSpec;\s*const next = \{\s*\.\.\.current,/u,
    "Unrelated editor changes must preserve the authored rather than inferred chart type");
  assert.match(frame, /legend\.clientHeight\s*<\s*legend\.scrollHeight\s*-\s*1/u,
    "The legend expansion control must remain available after scrolling");
  assert.doesNotMatch(frame, /legend\.scrollTop\s*\+\s*legend\.clientHeight/u,
    "Legend overflow is independent of the current scroll position");
  assert.match(sortable, /const stableIdentity = attributes\.id \?\? authored\[0\]\.metadata\.id/u,
    "Conditional sections must not replace stable authored dashboard-region identities");
  assert.match(sortable, /element\.props\.className \?\? ""\)\.split\(\/\\s\+\/u\)\.filter\(Boolean\)/u,
    "Every authored placement class must remain available while measuring custom grid proportions");
  assert.doesNotMatch(sortable, /placementClass\.test/u,
    "Nonstandard authored layout classes must not be discarded by a fixed placement allowlist");
  assert.match(sortable, /row\.items\.sort\(\(left,\s*right\)\s*=>\s*left\.bounds\.left\s*-\s*right\.bounds\.left\)/u,
    "Canvas promotion must preserve measured visual placement rather than authored DOM order");
});

test("ordered numeric and temporal buckets inherit distribution spacing without affecting categories", () => {
  assert.equal(orderedDistribution({ x: "ageBand" }, [
    { ageBand: "< 5 min" }, { ageBand: "5–9 min" }, { ageBand: "10–19 min" }, { ageBand: "30+ min" },
  ]), true);
  assert.equal(orderedDistribution({ x: "interval" }, [
    { interval: "22:55" }, { interval: "23:00" }, { interval: "23:05" },
  ]), true);
  assert.equal(orderedDistribution({ x: "bucket" }, [{ bucket: 0 }, { bucket: 1 }, { bucket: 2 }]), true);
  assert.equal(orderedDistribution({ x: "accountBand" }, [
    { accountBand: "Free" }, { accountBand: "Plus" }, { accountBand: "Enterprise" },
  ]), false);
  assert.equal(orderedDistribution({ x: "plan" }, [
    { plan: "Free" }, { plan: "Plus" }, { plan: "Enterprise" },
  ]), false);
  assert.equal(orderedDistribution({ x: "ageBand", distribution: false }, [
    { ageBand: "0–5" }, { ageBand: "5–10" }, { ageBand: "10–15" },
  ]), false);
  assert.equal(orderedDistribution({ x: "category", distribution: true }, [{ category: "Only" }]), true);
});

test("daily temporal ticks keep their calendar cadence without adding an off-cadence endpoint", () => {
  const dates = Array.from({ length: 28 }, (_, index) =>
    new Date(Date.UTC(2026, 6, 13 + index)).toISOString().slice(0, 10));
  const before = [...dates];
  assert.deepEqual(temporalAxisTicks(dates), ["2026-07-13", "2026-07-20", "2026-07-27", "2026-08-03"]);
  assert.deepEqual(temporalAxisTicks(dates.slice(0, 7)),
    ["2026-07-13", "2026-07-15", "2026-07-17", "2026-07-19"],
    "The final date remains when it naturally falls on the cadence");
  assert.deepEqual(temporalAxisTicks(dates.slice(0, 6)), dates.slice(0, 6),
    "Every reviewed period is labeled when the available budget fits");
  assert.deepEqual(temporalAxisTicks(dates, { maxTicks: 2 }), [dates[0], dates.at(-1)],
    "A deliberate two-label comparison retains both endpoints");
  assert.deepEqual(dates, before, "Tick selection must not remove the final period from the source data");
});

test("month-grain tick budgets balance readable coverage and both boundary months", () => {
  const months = Array.from({ length: 26 }, (_, index) =>
    new Date(Date.UTC(2024, 6 + index, 1)).toISOString().slice(0, 7));
  for (const maxTicks of [2, 3, 6, 13, 25, 26]) {
    const ticks = temporalAxisTicks(months, { maxTicks });
    assert.equal(ticks[0], "2024-07");
    assert.equal(ticks.at(-1), "2026-08");
    assert.ok(ticks.length <= maxTicks);
    const steps = ticks.slice(1).map((value, index) => months.indexOf(value) - months.indexOf(ticks[index]));
    assert.ok(Math.max(...steps) - Math.min(...steps) <= 1,
      "Endpoint preservation distributes rounding across the axis instead of leaving one short interval");
  }
  assert.deepEqual(temporalAxisTicks(months, { maxTicks: 13 }),
    ["2024-07", "2024-09", "2024-11", "2025-01", "2025-03", "2025-05", "2025-08",
      "2025-10", "2025-12", "2026-02", "2026-04", "2026-06", "2026-08"]);
  for (const count of [12, 14, 18, 24]) {
    const series = months.slice(0, count);
    const ticks = temporalAxisTicks(series, { maxTicks: 6 });
    assert.equal(ticks.length, 6, `${count} months must use the readable budget even when its span is prime`);
    assert.equal(ticks[0], series[0]);
    assert.equal(ticks.at(-1), series.at(-1));
    const steps = ticks.slice(1).map((value, index) => series.indexOf(value) - series.indexOf(ticks[index]));
    assert.ok(Math.max(...steps) - Math.min(...steps) <= 1);
  }
});

test("weekly and monthly temporal ticks use a consistent source-period stride", () => {
  const weekly = Array.from({ length: 12 }, (_, index) =>
    new Date(Date.UTC(2026, 5, 1 + index * 7)).toISOString().slice(0, 10));
  const monthly = Array.from({ length: 12 }, (_, index) =>
    new Date(Date.UTC(2026, index, 1)).toISOString().slice(0, 10));
  for (const dates of [weekly, monthly]) {
    const before = [...dates];
    assert.deepEqual(temporalAxisTicks(dates), [dates[0], dates[2], dates[4], dates[6], dates[8], dates[10]],
      "Regular series must not alternate two- and three-period gaps to fill the budget");
    assert.deepEqual(temporalAxisTicks(dates, { maxTicks: 4 }), [dates[0], dates[3], dates[6], dates[9]]);
    assert.deepEqual(temporalAxisTicks(dates, { maxTicks: 2 }), [dates[0], dates.at(-1)]);
    assert.deepEqual(dates, before);
  }
  const eightWeeks = weekly.slice(0, 8);
  assert.deepEqual(temporalAxisTicks(eightWeeks, { maxTicks: 8 }), eightWeeks);
  assert.deepEqual(temporalAxisTicks(eightWeeks, { maxTicks: 4 }),
    [eightWeeks[0], eightWeeks[2], eightWeeks[4], eightWeeks[6]]);
  assert.deepEqual(temporalAxisTicks(eightWeeks, { maxTicks: 3 }), [eightWeeks[0], eightWeeks[3], eightWeeks[6]]);
});

test("temporal tick selection deduplicates reviewed values without inventing or reordering periods", () => {
  const dates = ["2026-07-13", "2026-07-17", "2026-07-23", "2026-08-01",
    "2026-08-07", "2026-08-09", "2026-08-12"];
  assert.deepEqual(temporalAxisTicks([dates[0], dates[0], dates[1]]), [dates[0], dates[1]]);
  assert.deepEqual(temporalAxisTicks(dates, { maxTicks: 4 }),
    ["2026-07-13", "2026-07-23", "2026-08-07", "2026-08-12"]);
  assert.deepEqual(temporalAxisTicks([], { maxTicks: 1 }), []);
  assert.deepEqual(temporalAxisTicks([null, dates[0], undefined, dates[1]]), dates.slice(0, 2));
});

test("temporal axes budget measured labels once and keep edge labels inside the SVG", async () => {
  const renderer = await readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8");
  const axis = renderer.slice(renderer.indexOf("function TemporalXAxis"), renderer.indexOf("function compactAxisCategory"));
  assert.match(axis, /usePlotArea\(\)/u, "Available plot width determines the temporal tick budget");
  assert.match(axis, /useChartText\(\)/u);
  assert.match(axis, /measureText\(tick\(value, \{ includeYear \}\)\)/u,
    "Measure the same year-aware date labels that the temporal axis renders");
  assert.match(axis, /plot\?\.width/u);
  assert.match(axis, /ticks=\{temporalAxisTicks\(values,\s*\{\s*maxTicks\s*\}\)\}\s*interval=\{0\}/u,
    "Recharts must not apply a second, uneven thinning pass to the selected cadence");
  const tick = renderer.slice(renderer.indexOf("function TemporalAxisTick"), renderer.indexOf("function TemporalXAxis"));
  assert.match(tick, /tick\(payload\.value, \{ includeYear \}\)/u);
  assert.match(tick, /useChartWidth\(\)/u, "Endpoint labels use the SVG bounds rather than changing the plotted domain");
  assert.match(tick, /Math\.min\(width - halfLabel, x\)/u);
  assert.match(tick, /x=\{textX\}[\s\S]*?textAnchor="middle"/u,
    "Date labels stay centered while their visible text is kept inside the SVG");
});

test("axis layouts rotate only dense named categories and honor explicit presentation choices", () => {
  const countries = ["United States", "Japan", "Germany", "South Korea", "Brazil", "United Kingdom",
    "Canada", "Australia", "France", "India", "Netherlands", "Mexico"];
  assert.equal(categoryAxisLayout(countries), "angled");
  assert.equal(categoryAxisLayout(countries.slice(0, 7)), "wrapped");
  assert.equal(categoryAxisLayout(Array.from({ length: 12 }, (_, index) => String(index + 1))), "horizontal");
  assert.equal(categoryAxisLayout(Array.from({ length: 12 }, (_, index) =>
    new Date(Date.UTC(2026, 7, index + 1)).toISOString().slice(0, 10))), "horizontal");
  assert.equal(categoryAxisLayout(["2026-08-01", "2026-08-02"], { preference: "angled" }), "horizontal");
  assert.equal(categoryAxisLayout(["08:00", "09:00"], { preference: "angled" }), "horizontal");
  assert.equal(categoryAxisLayout(countries, { preference: "horizontal" }), "horizontal");
  assert.equal(categoryAxisLayout(countries, { preference: "wrapped" }), "wrapped");
  assert.equal(categoryAxisLayout(countries.slice(0, 3), { preference: "angled" }), "angled");
  assert.deepEqual(projectChartSpec({ type: "bar", x: "country", y: "users", xTickLabelLayout: "angled" }),
    { type: "bar", x: "country", y: "users", xTickLabelLayout: "angled" });
});

test("Dark Pixel keeps clean semantic chart fills without decorative dithering", async () => {
  const [shell, styles, dashboard, dashboardStyles, theme] = await Promise.all([
    readFile(new URL("../src/DataAppShell.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/styles.css", import.meta.url), "utf8"),
    readFile(new URL("../src/content/dashboard/DashboardContent.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/content/dashboard/dashboard.css", import.meta.url), "utf8"),
    readFile(new URL("../../themes/dark-pixel/theme.css", import.meta.url), "utf8"),
  ]);
  for (const [name, source] of Object.entries({ shell, styles, dashboard, dashboardStyles, theme })) {
    assert.doesNotMatch(source, /PixelChartShaders|pixel-chart-dither|data-app-chart-effect|data-chart-effect/u,
      `${name} must not add decorative Dark Pixel shaders or activation metadata`);
  }
  assert.doesNotMatch(dashboard, /regional-map-dots|regional-map-dot-field/u,
    "Regional maps should not retain hidden theme-only dot-pattern assets");
  assert.match(theme, /--card-radius:\s*0/u, "Dark Pixel must retain its distinctive square geometry");
  assert.match(theme, /--font-sans:\s*"Courier New"/u, "Dark Pixel must retain its monospace typography");
  assert.match(theme, /--fixed-theme-scheme:\s*dark/u, "Authored Dark Pixel artifacts must remain genuinely dark");
});

const pick = (row, fields) => Object.fromEntries(fields.map((field) => [field, row[field]]));
const bridgeValues = (rows, y, x) =>
  waterfall(rows, y, { categoryField: x, includeEnding: true }).map((row) =>
    pick(
      row,
      [
        x,
        "baseline",
        "magnitude",
        "range",
        "change",
        "balance",
        "runningTotal",
        "isTotal",
        "totalType",
        "waterfallRole",
      ].filter(Boolean),
    ),
  );

test("simple nonnegative category rankings use the compact leaderboard without losing explicit bar intent", () => {
  const rows = [
    { category: "Projects", activeUsers: 1800, targetUsers: 1600 },
    { category: "Canvas", activeUsers: 1700, targetUsers: 1550 },
  ];
  const ranking = { type: "horizontalBar", x: "category", y: "activeUsers" };
  assert.equal(resolvedChartType(ranking, rows), "rankedList");
  assert.equal(resolvedChartType({ ...ranking, annotations: [] }, rows), "rankedList");
  const annotations = [{ id: "launch", kind: "point", at: "Projects", field: "activeUsers", label: "Launch cohort" }];
  assert.equal(resolvedChartType({ ...ranking, annotations }, rows), "horizontalBar");
  for (const showAnnotations of [false, true]) {
    const chart = { ...ranking, annotations, showAnnotations };
    assert.equal(resolvedChartType(chart, rows), "horizontalBar",
      "Changing annotation visibility must not switch the chart to ranked-list geometry");
    assert.deepEqual(projectChartSpec(chart), chart);
  }
  assert.equal(resolvedChartType({ ...ranking, annotations: [], showAnnotations: false }, rows), "rankedList",
    "An empty annotation list does not reserve bar geometry");
  assert.equal(resolvedChartType({ ...ranking, annotations }, rows.slice(1)), "horizontalBar",
    "Filtering an annotation anchor must not change the chart's geometry");
  assert.equal(resolvedChartType({ ...ranking, type: "leaderboard" }, rows), "rankedList");
  assert.equal(resolvedChartType({ ...ranking, preserveBarChart: true }, rows), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, xLabel: "Active accounts" }, rows), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, showXAxisLabel: true }, rows), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, fields: ["activeUsers", "targetUsers"] }, rows), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, series: "segment" }, rows), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, colorBySign: true }, rows), "horizontalBar");
  assert.equal(resolvedChartType(ranking, [...rows, { category: "Search", activeUsers: -200 }]), "horizontalBar");
  assert.equal(resolvedChartType(ranking, [...rows, rows[0]]), "horizontalBar");
  assert.equal(resolvedChartType(ranking, [{ category: "2026-08-19", activeUsers: 1800 }]), "horizontalBar");
  assert.equal(resolvedChartType({ ...ranking, type: "stackedBar" }, rows), "stackedBar");
  assert.deepEqual(projectChartSpec({ ...ranking, preserveBarChart: true }), { ...ranking, preserveBarChart: true });
});

test("ranked leaderboards start at five rows and use only genuinely available adjacent height", () => {
  assert.equal(rankedListCapacity({ availableHeight: 196, rowHeight: 36, rowGap: 4, totalCount: 12 }), 5);
  assert.equal(rankedListCapacity({ availableHeight: 316, rowHeight: 36, rowGap: 4, totalCount: 12 }), 8);
  assert.equal(rankedListCapacity({ availableHeight: 800, rowHeight: 36, rowGap: 4, totalCount: 12 }), 12);
  assert.equal(rankedListCapacity({ availableHeight: 20, rowHeight: 36, rowGap: 4, totalCount: 12 }), 5);
  assert.equal(rankedListCapacity({ availableHeight: 316, rowHeight: 36, rowGap: 4, minimumCount: 3, totalCount: 12 }), 8);
});

test("waterfall projection preserves explicit balances and only used total metadata", () => {
  const spec = { type: "waterfall", x: "period", y: "amount" };
  const rows = [
    { period: "FY25", amount: 100, isTotal: true, privateNote: "not chart data" },
    { period: "Expansion", amount: 20 },
    { period: "Interim", amount: 120, isTotal: true },
    { period: "Churn", amount: -10 },
    { period: "FY26", amount: 110, isTotal: true },
  ];
  const projected = project(spec, rows);
  assert.deepEqual(projected.shape.requiredRowFields, ["period", "amount"]);
  assert.deepEqual(projected.shape.optionalRowFields, ["isTotal"]);
  assert.deepEqual(bridgeValues(projected.rows, "amount", "period"), bridgeValues(rows, "amount", "period"));
  assert.deepEqual(
    bridgeValues(projected.rows, "amount", "period").map((row) => row.balance),
    [100, 120, 120, 110, 110],
  );
  assert.ok(projected.rows.every((row) => !Object.hasOwn(row, "privateNote")));

  for (const field of ["totalType", "waterfallRole", "role", "type", "isTotal", "total"]) {
    const flagged = [
      { period: "A", amount: 100, [field]: field.endsWith("Total") || field === "total" ? true : "total" },
      { period: "B", amount: 20 },
    ];
    const result = project(spec, flagged);
    assert.ok(result.shape.rowFields.includes(field), field);
    assert.deepEqual(bridgeValues(result.rows, "amount", "period"), bridgeValues(flagged, "amount", "period"));
  }
  const precedence = [
    { period: "A", amount: 100, totalType: "", role: "beginning", undefined: "ending" },
    { period: "B", amount: 20 },
  ];
  const result = project(spec, precedence);
  assert.ok(result.shape.rowFields.includes("totalType"));
  assert.ok(!result.shape.rowFields.includes("role"));
  assert.ok(!result.shape.rowFields.includes("undefined"));
  assert.deepEqual(bridgeValues(result.rows, "amount", "period"), bridgeValues(precedence, "amount", "period"));
});

test("box plot projection keeps reviewed quartiles or raw observations as appropriate", () => {
  const rows = [
    {
      segment: "A",
      minimum: 1,
      lowerQuartile: 2,
      median: 3,
      upperQuartile: 4,
      maximum: 5,
      privateNote: "not chart data",
    },
  ];
  for (const y of ["median", "latency"]) {
    const spec = { type: "boxPlot", x: "segment", y };
    const result = project(spec, rows);
    assert.equal(result.shape.reviewedBoxPlot, true);
    assert.deepEqual(result.shape.requiredRowFields, ["segment", ...boxPlotSummaryFields]);
    assert.deepEqual(
      boxPlots(result.rows, "segment", y).map((row) => pick(row, ["segment", ...boxPlotSummaryFields, "spread"])),
      boxPlots(rows, "segment", y).map((row) => pick(row, ["segment", ...boxPlotSummaryFields, "spread"])),
    );
    assert.equal(boxPlots(result.rows, "segment", y)[0].spread, 2);
    assert.ok(!result.shape.rowFields.includes("privateNote"));
  }
  const raw = [
    { segment: "A", latency: 1, minimum: -99 },
    { segment: "A", latency: 5, minimum: -99 },
  ];
  const spec = { type: "boxPlot", x: "segment", y: "latency" };
  const result = project(spec, raw);
  assert.equal(result.shape.reviewedBoxPlot, false);
  assert.deepEqual(result.shape.rowFields, ["segment", "latency"]);
  assert.deepEqual(boxPlots(result.rows, "segment", "latency"), boxPlots(raw, "segment", "latency"));
});

test("heatmap projection preserves its inferred dimension and observed intensities", () => {
  const spec = { type: "heatmap", x: "day", y: "count" };
  const rows = [
    { day: "Mon", hour: "09:00", count: 4, privateNote: "first" },
    { day: "Tue", hour: "10:00", count: 7, privateNote: "second" },
  ];
  const result = project(spec, rows);
  assert.equal(result.shape.heatmapGroup, "hour");
  assert.equal(chartDataShape(spec, result.rows).heatmapGroup, "hour");
  assert.deepEqual(result.shape.rowFields, ["day", "hour", "count"]);
  const values = (data) => {
    const value = heatmap(data, "day", "hour", "count");
    return {
      ...value,
      rows: value.rows.map((row) => pick(row, ["day", "hour", "count", "xIndex", "yIndex", "intensity", "__missing"])),
    };
  };
  assert.deepEqual(values(result.rows), values(rows));
  const stringMeasures = [{ day: "Mon", hour: "09:00", count: "4" }];
  const stringResult = project(spec, stringMeasures);
  assert.equal(chartDataShape(spec, stringResult.rows).heatmapGroup, "hour", "Column order survives projection");
});

test("histograms and explicit-stage Sankey charts do not require a nominal x column", () => {
  const histogramSpec = { type: "histogram", y: "latency" };
  const samples = [
    { latency: 12, secret: "A" },
    { latency: 29, secret: "B" },
    { latency: 15, secret: "C" },
  ];
  const histogramInput = project(histogramSpec, samples);
  assert.equal(histogramInput.shape.requiresX, false);
  assert.deepEqual(histogramInput.shape.requiredRowFields, ["latency"]);
  assert.deepEqual(histogram(histogramInput.rows, "latency"), histogram(samples, "latency"));

  const rows = [
    { from: "A", via: "B", to: "C", count: 3, secret: "not chart data" },
    { from: "A", via: "D", to: "C", count: 2, secret: "not chart data" },
  ];
  for (const spec of [
    { type: "sankey", source: "from", target: "to", y: "count" },
    { type: "sankey", stages: ["from", "via", "to"], y: "count" },
  ]) {
    const result = project(spec, rows);
    assert.equal(result.shape.requiresX, false);
    assert.deepEqual(result.shape.requiredRowFields, ["count", ...result.shape.sankeyStages]);
    assert.deepEqual(
      sankeyGraph(result.rows, result.shape.sankeyStages, "count"),
      sankeyGraph(rows, result.shape.sankeyStages, "count"),
    );
    assert.ok(!result.shape.rowFields.includes("secret"));
  }
});

test("long-form line barFields name pivoted series; wide-form barFields name raw columns", () => {
  const spec = { type: "line", x: "date", y: "value", series: "metric", barFields: ["Revenue"] };
  const rows = [
    { date: "2026-01-01", metric: "Revenue", value: 12, secret: "A" },
    { date: "2026-01-01", metric: "Target", value: 14, secret: "B" },
    { date: "2026-01-02", metric: "Revenue", value: 15, secret: "C" },
  ];
  const result = project(spec, rows);
  assert.equal(result.shape.longForm, true);
  assert.deepEqual(result.shape.fields, ["Revenue", "Target"]);
  assert.deepEqual(result.shape.barFields, ["Revenue"]);
  assert.deepEqual(result.shape.requiredRowFields, ["date", "value", "metric"]);
  assert.ok(!result.shape.rowFields.includes("Revenue"));
  assert.deepEqual(pivot(result.rows, "date", "metric", "value"), pivot(rows, "date", "metric", "value"));

  const wide = { type: "line", x: "date", y: "Revenue", fields: ["Revenue", "Target"], barFields: ["Growth"] };
  const wideRows = [{ date: "2026-01-01", Revenue: 12, Target: 14, Growth: 3, secret: "A" }];
  const wideResult = project(wide, wideRows);
  assert.equal(wideResult.shape.longForm, false);
  assert.deepEqual(wideResult.shape.requiredRowFields, ["date", "Revenue", "Target", "Growth"]);
  assert.deepEqual(wideResult.rows, [{ date: "2026-01-01", Revenue: 12, Target: 14, Growth: 3 }]);
});

test("scatter projection retains the actual tooltip identities without arbitrary extra columns", () => {
  const spec = { type: "scatter", x: "cost", y: "latency" };
  const rows = [
    { cost: 1, latency: 2, featureName: "Search", name: "unused", secret: "A" },
    { cost: 2, latency: 3, name: "Export", secret: "B" },
  ];
  const result = project(spec, rows);
  assert.deepEqual(
    result.rows.map((row) => row[scatterTooltipIdentityField(row)]),
    ["Search", "Export"],
  );
  assert.ok(!result.shape.rowFields.includes("secret"));
});

test("shareable chart specs retain canonical options and drop extension metadata", async () => {
  const spec = {
    type: "line",
    x: "week",
    y: "activeUsers",
    fields: ["activeUsers", "targetUsers"],
    barFields: ["growth"],
    showArea: true,
    stackable: false,
    colors: { activeUsers: "var(--chart-1)", privateMetadata: { sql: "secret" } },
    colorDomain: [0, 12, "secret"],
    legend: { position: "right", privateMetadata: "secret" },
    privateMetadata: "secret",
    sql: "secret",
    url: "secret",
  };
  assert.deepEqual(projectChartSpec(spec), {
    type: "line",
    x: "week",
    y: "activeUsers",
    stackable: false,
    fields: ["activeUsers", "targetUsers"],
    barFields: ["growth"],
    colors: { activeUsers: "var(--chart-1)" },
    colorDomain: [0, 12],
    legend: { position: "right" },
  });
  const [renderer, editor, shape] = await Promise.all([
    readFile(new URL("../src/charting/ChartRenderer.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/components/ChartExplorer.jsx", import.meta.url), "utf8"),
    readFile(new URL("../src/charting/chart-data-shape.js", import.meta.url), "utf8"),
  ]);
  const consumed = new Set([
    "type",
    "x",
    "y",
    "series",
    ...[renderer, editor, shape].flatMap((source) =>
      [...source.matchAll(/\b(?:spec|component\.chart)(?:\?\.|\.)([A-Za-z_$][\w$]*)/gu)].map((match) => match[1]),
    ),
  ]);
  for (const key of consumed) assert.ok(chartSpecKeys.includes(key), `Missing canonical option ${key}`);
  assert.match(renderer, /chartDataShape\(spec, seriesRows\)/u,
    "Long-form chart shapes must reflect the same safely grouped rows used for rendering");
  assert.match(renderer, /sankeyGraph\(rows, sankeyStages, y\)/u);
});

test("funnel stages preserve source order and calculate only valid prior-stage conversion", () => {
  const stages = funnelStages(
    [
      { stage: "Visited", accounts: 100 },
      { stage: "Signed up", accounts: 60 },
      { stage: "Activated", accounts: 30 },
    ],
    "stage",
    "accounts",
  );
  assert.deepEqual(
    stages.map(({ __funnelStage, __funnelConversion }) => [__funnelStage, __funnelConversion]),
    [
      ["Visited", undefined],
      ["Signed up", 0.6],
      ["Activated", 0.5],
    ],
  );
  assert.deepEqual(stages.map(({ __funnelShare, __funnelDropoff }) => [__funnelShare, __funnelDropoff]),
    [[1, undefined], [.6, 40], [.3, 30]], "Overall conversion and step conversion must stay distinct");
  assert.deepEqual(stages.map(({ __funnelChange }) => __funnelChange), [undefined, -.4, -.5]);
  const zero = funnelStages(
    [
      { stage: "Empty", accounts: 0 },
      { stage: "Later", accounts: 2 },
    ],
    "stage",
    "accounts",
  );
  assert.equal(zero[1].__funnelConversion, undefined);
  assert.equal(zero[1].__funnelShare, undefined);
  assert.equal(zero[1].__funnelChange, undefined, "A zero denominator cannot imply a percent change");
  assert.equal(funnelStages([{ count: 20 }, { count: 0 }], "stage", "count")[1].__funnelChange, -1);
  const missing = funnelStages(
    [
      { stage: "Known", accounts: 100 },
      { stage: "Unavailable", accounts: null },
      { stage: "Blank", accounts: "" },
      { stage: "Resumed", accounts: 30 },
    ],
    "stage",
    "accounts",
  );
  assert.deepEqual(
    missing.map(({ __funnelValue, __funnelConversion }) => [__funnelValue, __funnelConversion]),
    [
      [100, undefined],
      [undefined, undefined],
      [undefined, undefined],
      [30, undefined],
    ],
  );
});

test("horizontal funnel geometry preserves proportional values, flat endings, gaps, and increases", () => {
  const stages = funnelStages([
    { stage: "First", count: "100" }, { stage: "Second", count: 60 }, { stage: "Third", count: 30 },
  ], "stage", "count");
  const [segment] = funnelRibbonSegments(stages);
  assert.deepEqual(segment.points, [{ x: 50, height: 180 }, { x: 150, height: 108 }, { x: 250, height: 54 }]);
  assert.match(segment.path, /L 300 73 L 300 127/u, "The last stage must not taper to an invented zero");
  const invalid = [null, "", -1, true, {}, Infinity, NaN];
  for (const count of invalid) {
    const rows = funnelStages([{ stage: "First", count: 100 }, { stage: "Missing", count },
      { stage: "Last", count: 20 }], "stage", "count");
    assert.equal(rows[1].__funnelValue, undefined);
    assert.equal(rows[2].__funnelConversion, undefined);
    assert.equal(funnelRibbonSegments(rows).length, 2, "Missing stages must break the ribbon, not become zero");
  }
  const growing = funnelStages([{ stage: "First", count: 50 }, { stage: "Last", count: 100 }], "stage", "count");
  assert.equal(growing[1].__funnelShare, 2);
  assert.equal(growing[1].__funnelDropoff, -50);
  assert.equal(growing[1].__funnelChange, 1, "An increasing stage has a positive change, not a drop-off percentage");
  assert.deepEqual(funnelRibbonSegments(growing)[0].points.map(({ height }) => height), [90, 180]);
  assert.deepEqual(funnelRibbonSegments([]), []);
  assert.deepEqual(funnelRibbonSegments(funnelStages([{ stage: "Empty", count: 0 }], "stage", "count"))[0].points,
    [{ x: 50, height: 0 }], "Zero must remain zero thickness");
});

test("funnel shades start at the unchanged core hue, including single-stage funnels", () => {
  for (const count of [1, 4, 8]) assert.equal(funnelStageColor("var(--chart-2)", 0, count), "var(--chart-2)");
  assert.equal(funnelStageColor("var(--chart-2)", 3, 4), "color-mix(in srgb, var(--chart-2) 24%, var(--surface))");
  assert.notEqual(funnelStageColor("var(--chart-2)", 0, 4, true), funnelStageColor("var(--chart-2)", 0, 4),
    "A fully saturated first stage must still have an active treatment");
});

test("funnel layout follows its own width and stage count at both density boundaries", () => {
  for (const count of [1, 4, 8]) {
    assert.equal(funnelLayout(count * 144, count), "horizontal");
    assert.equal(funnelLayout(count * 144 - 1, count), "compact");
    assert.equal(funnelLayout(count * 120, count), "compact");
    assert.equal(funnelLayout(count * 120 - 1, count), "vertical");
  }
});
