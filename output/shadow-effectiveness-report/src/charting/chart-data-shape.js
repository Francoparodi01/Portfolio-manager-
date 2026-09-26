import { boxPlotSummaryFields, hasReviewedBoxPlotSummary, waterfallRowFields } from "./chart-transforms.js";
import { ratioMetric } from "./chart-theme.js";
import { chartAnnotationFields, normalizeChartAnnotations } from "./chart-annotations.js";

const scalarSpecKeys = [
  "type",
  "x",
  "y",
  "series",
  "source",
  "target",
  "xLabel",
  "yLabel",
  "rightYAxisLabel",
  "xTickLabelLayout",
  "showXAxisLabel",
  "showYAxisLabel",
  "startAtZero",
  "showValues",
  "showLegend",
  "showAnnotations",
  "baseColor",
  "colorScaleStartAtZero",
  "colorBySign",
  "sortOrder",
  "centerLabel",
  "centerValue",
  "beginning",
  "ending",
  "rowHeight",
  "initialVisibleCount",
  "preserveBarChart",
  "groupOther",
  "maxCategories",
  "distribution",
  "variant",
  "stackable",
];
const listSpecKeys = ["fields", "barFields", "stages", "preserveCategories", "categoryOrder", "rightAxisFields"];

export function isTemporalCategory(value) {
  return typeof value === "string" && (/^\d{4}-(?:0[1-9]|1[0-2])$/u.test(value)
    || /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value) && Number.isFinite(Date.parse(value)));
}

/** Calendar order is a display transform, never a mutation of reviewed rows. */
export function orderCalendarRows(rows, field, spec = {}) {
  if (!rows.length || ["ascending", "descending"].includes(spec.sortOrder)) return rows;
  if (spec.categoryOrder?.length) {
    const order = new Map(spec.categoryOrder.map((value, index) => [value, index]));
    return [...rows].sort((a, b) => (order.get(a[field]) ?? order.size) - (order.get(b[field]) ?? order.size));
  }
  if (spec.sortOrder === "original") return rows;
  const weekdays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
  const weekday = (value) => typeof value === "string"
    && /^(?:mon(?:day)?|tue(?:s(?:day)?)?|wed(?:nesday)?|thu(?:rs?(?:day)?)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\.?$/iu.test(value.trim())
    ? weekdays.indexOf(value.trim().slice(0, 3).toLowerCase()) : -1;
  const weekdayIndices = rows.map((row) => weekday(row[field]));
  // Repeated labels may represent successive weeks, not one weekday breakdown.
  if (weekdayIndices.every((day) => day >= 0) && new Set(weekdayIndices).size === rows.length) {
    return [...rows].sort((a, b) => weekday(a[field]) - weekday(b[field]));
  }
  if (rows.every((row) => isTemporalCategory(row[field]))) {
    return [...rows].sort((a, b) => Date.parse(a[field]) - Date.parse(b[field]));
  }
  return rows;
}

/** Separate two measures, not two categories of the same measure. Empty explicit fields opt out. */
export function secondaryAxisFields(spec, rows, fields) {
  if (spec.series || spec.barFields?.length || !["line", "bar", "area", "horizontalBar"].includes(spec.type)
    || fields.length < 2) return [];
  if (Array.isArray(spec.rightAxisFields)) {
    const right = fields.filter((field) => spec.rightAxisFields.includes(field));
    return right.length < fields.length ? right : [];
  }
  if (fields.length !== 2) return [];
  const values = fields.map((field) => rows.map((row) => row[field]).filter(Number.isFinite));
  if (values.some((series) => !series.length)) return [];
  const magnitude = values.map((series) => Math.max(...series.map(Math.abs)));
  const differentUnits = ratioMetric(fields[0], values[0]) !== ratioMetric(fields[1], values[1]);
  const differentScales = Math.min(...magnitude) > 0 && Math.max(...magnitude) / Math.min(...magnitude) >= 20;
  return differentUnits || differentScales ? [fields[1]] : [];
}

/** Options consumed by the canonical chart renderer and editor. */
export const chartSpecKeys = Object.freeze([...scalarSpecKeys, ...listSpecKeys, "colors", "colorDomain", "legend", "annotations"]);

const scalar = (value) =>
  value == null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  (typeof value === "number" && Number.isFinite(value));
const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

/** Project a separately shareable chart spec without carrying arbitrary extension metadata. */
export function projectChartSpec(spec = {}) {
  const result = {};
  for (const key of scalarSpecKeys) {
    if (Object.hasOwn(spec, key) && scalar(spec[key])) result[key] = spec[key];
  }
  for (const key of listSpecKeys) {
    if (Array.isArray(spec[key])) result[key] = spec[key].filter((field) => typeof field === "string");
  }
  if (record(spec.colors)) {
    result.colors = Object.fromEntries(Object.entries(spec.colors).filter(([, color]) => typeof color === "string"));
  }
  if (Array.isArray(spec.colorDomain)) {
    result.colorDomain = spec.colorDomain.slice(0, 2).map((value) => (Number.isFinite(value) ? value : null));
  }
  if (record(spec.legend) && typeof spec.legend.position === "string") {
    result.legend = { position: spec.legend.position };
  }
  if (Object.hasOwn(spec, "annotations")) result.annotations = normalizeChartAnnotations(spec.annotations);
  return result;
}

/** Collapse only a single reviewed, nonnegative additive category measure. */
export function groupAdditiveCategories(
  rows = [],
  { categoryField, valueField, maxCategories = 7, preserveCategories = [], enabled = false } = {},
) {
  const limit = Number(maxCategories);
  if (!enabled || !categoryField || !valueField || !Number.isSafeInteger(limit) || limit < 2 || rows.length <= limit) {
    return rows;
  }

  const measureWords = String(valueField)
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ");
  if (/\b(?:rate|ratio|percent(?:age)?|pct|avg|average|mean|median|share|per)\b/iu.test(measureWords)) return rows;
  const categories = new Set();
  for (const row of rows) {
    const category = row?.[categoryField];
    const value = row?.[valueField];
    if (typeof category !== "string" || !category.trim() || categories.has(category)
      || /^other$/iu.test(category.trim()) || !Number.isFinite(value) || value < 0) return rows;
    categories.add(category);
  }

  const requested = Array.isArray(preserveCategories) ? preserveCategories : [];
  const preserved = new Set(requested.map(String).filter((category) => categories.has(category)));
  if (preserved.size >= limit) return rows;

  const sorted = [...rows].sort((left, right) => right[valueField] - left[valueField]);
  const leading = new Set([
    ...sorted.filter((row) => preserved.has(row[categoryField])),
    ...sorted.filter((row) => !preserved.has(row[categoryField])),
  ].slice(0, limit - 1).map((row) => row[categoryField]));
  const other = sorted.filter((row) => !leading.has(row[categoryField]));
  if (!other.length) return rows;

  return [
    ...sorted.filter((row) => leading.has(row[categoryField])),
    { [categoryField]: "Other", [valueField]: other.reduce((total, row) => total + row[valueField], 0) },
  ];
}

/** Group only independently additive, mutually exclusive series at each reviewed x value. */
export function groupAdditiveSeries(
  rows = [],
  { groupField, categoryField, valueField, maxCategories = 7, preserveCategories = [], enabled = false } = {},
) {
  if (!enabled || !groupField || !categoryField || !valueField || !rows.length) return rows;

  const groups = new Map();
  const categories = new Set();
  for (const row of rows) {
    const group = row?.[groupField];
    const category = row?.[categoryField];
    const value = row?.[valueField];
    if (group == null || typeof category !== "string" || !category.trim()
      || /^other$/iu.test(category.trim()) || !Number.isFinite(value) || value < 0) return rows;
    if (!groups.has(group)) groups.set(group, new Map());
    const current = groups.get(group);
    if (current.has(category)) return rows;
    current.set(category, row);
    categories.add(category);
  }

  const reference = [...categories].map((category) => ({
    [categoryField]: category,
    [valueField]: [...groups.values()]
      .reduce((peak, period) => Math.max(peak, period.get(category)?.[valueField] ?? 0), 0),
  }));
  const grouped = groupAdditiveCategories(reference, {
    categoryField,
    valueField,
    maxCategories,
    preserveCategories,
    enabled: true,
  });
  if (grouped === reference) return rows;

  const leading = new Set(grouped.filter((row) => row[categoryField] !== "Other")
    .map((row) => row[categoryField]));
  return [...groups].flatMap(([group, current]) => {
    const prominent = [...current.values()].filter((row) => leading.has(row[categoryField]));
    const remaining = [...current.values()].filter((row) => !leading.has(row[categoryField]));
    if (!remaining.length) return prominent;
    return [
      ...prominent,
      {
        [groupField]: group,
        [categoryField]: "Other",
        [valueField]: remaining.reduce((total, row) => total + row[valueField], 0),
      },
    ];
  });
}

/** Reconcile grouped totals with the reviewed source categories that remain visible. */
export function visibleGroupedCategories(
  rows = [],
  grouped = [],
  { categoryField, valueField, groupField, visibleCategories } = {},
) {
  if (!visibleCategories || !grouped.some((row) => row[categoryField] === "Other")) return grouped;
  const visible = new Set([...visibleCategories].map(String));
  const leading = new Set(grouped.filter((row) => row[categoryField] !== "Other")
    .map((row) => String(row[categoryField])));
  return grouped.map((row) => row[categoryField] === "Other"
    ? {
        ...row,
        [valueField]: rows.reduce((total, source) => {
          const category = String(source[categoryField]);
          return !leading.has(category) && visible.has(category)
            && (!groupField || source[groupField] === row[groupField])
            ? total + source[valueField]
            : total;
        }, 0),
      }
    : row);
}

/** Recognize ordered numeric or time buckets without collapsing ordinary categories. */
export function orderedDistribution(spec = {}, rows = []) {
  if (typeof spec.distribution === "boolean") return spec.distribution;
  const words = String(spec.x ?? "")
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ");
  if (!/\b(?:bands?|buckets?|bins?|intervals?)\b/iu.test(words)) return false;
  const values = [...new Set(rows.map((row) => row?.[spec.x]).filter((value) => value != null))];
  return values.length >= 3 && values.every((value) => typeof value === "number" && Number.isFinite(value)
    || typeof value === "string" && (/^\d{1,2}:\d{2}(?::\d{2})?$/u.test(value.trim())
      || /^(?:[<>≤≥]\s*)?\d+(?:[.,]\d+)?(?:\s*(?:-|–|—|to)\s*\d+(?:[.,]\d+)?)?\+?(?:\s*(?:m(?:in(?:ute)?s?)?|h(?:ours?)?|days?|weeks?|months?|years?))?$/iu.test(value.trim())));
}

/** Select reviewed dates at a consistent cadence; two-label comparisons retain both endpoints. */
export function temporalAxisTicks(values = [], { maxTicks = 6 } = {}) {
  const reviewed = [...new Set(values.filter((value) => value != null))];
  const limit = Math.max(2, Number.isSafeInteger(maxTicks) ? maxTicks : 6);
  if (reviewed.length <= limit) return reviewed;
  if (limit === 2) return [reviewed[0], reviewed.at(-1)];

  // Keep both boundary months and distribute rounding across the available
  // labels. Requiring a stride that divides the span collapses prime spans
  // (including an ordinary 12-month year) to just their two endpoints.
  if (reviewed.every((value) => typeof value === "string" && /^\d{4}-(?:0[1-9]|1[0-2])$/u.test(value))) {
    const span = reviewed.length - 1;
    return Array.from({ length: limit }, (_, index) => reviewed[Math.round(index * span / (limit - 1))]);
  }

  const timestamps = reviewed.map((value) => typeof value === "string"
    && /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value) ? Date.parse(value) : Number.NaN);
  const daily = timestamps.every((value, index) => Number.isFinite(value)
    && (index === 0 || value - timestamps[index - 1] === 86_400_000));
  if (!daily) {
    const stride = Math.ceil(reviewed.length / limit);
    return reviewed.filter((_, index) => index % stride === 0);
  }

  const last = reviewed.length - 1;
  const intervals = [1, 2, 3, 5, 7, 10, 14, 21, 28, 30, 60, 90, 180, 365];
  const interval = intervals.find((candidate) => Math.ceil(last / candidate) + 1 <= limit)
    ?? Math.ceil(last / (limit - 1));
  const positions = Array.from({ length: Math.floor(last / interval) + 1 }, (_, index) => index * interval);
  return positions.map((index) => reviewed[index]);
}

/** Prefer horizontal labels; reserve diagonal treatment for genuinely crowded named categories. */
export function categoryAxisLayout(values = [], { preference = "auto", minimumAngledCount = 8 } = {}) {
  const labels = [...new Set(values.filter((value) => value != null).map(String))];
  if (labels.length && labels.every((value) => /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value)
    || /^\d{1,2}:\d{2}(?::\d{2})?$/u.test(value)
    || /^-?\d+(?:\.\d+)?$/u.test(value))) return "horizontal";
  if (["horizontal", "wrapped", "angled"].includes(preference)) return preference;
  if (labels.length < minimumAngledCount) return "wrapped";
  return labels.some((value) => value.length > 8) ? "angled" : "wrapped";
}

/** Recognize compact, single-measure category rankings without changing explicit comparisons. */
export function resolvedChartType(spec = {}, rows = []) {
  if (!["leaderboard", "horizontalBar"].includes(spec.type)) return spec.type;

  const fallback = "horizontalBar";
  const explicitAxis = [spec.xLabel, spec.yLabel]
    .some((value) => typeof value === "string" && value.trim().length > 0)
    || spec.showXAxisLabel === true
    || spec.showYAxisLabel === true;
  const measures = [...new Set((spec.fields ?? [spec.y]).filter(Boolean))];
  if (spec.preserveBarChart || spec.series || measures.length !== 1 || explicitAxis
    || (Array.isArray(spec.annotations) && spec.annotations.length > 0)
    || spec.colorBySign || spec.startAtZero === false || !rows.length) return fallback;

  const categories = new Set();
  for (const row of rows) {
    const category = row?.[spec.x];
    const value = row?.[spec.y];
    if (typeof category !== "string" || !category.trim()
      || /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(category)
      || categories.has(category) || !Number.isFinite(value) || value < 0) return fallback;
    categories.add(category);
  }

  return "rankedList";
}

/** Keep the compact baseline while revealing additional reviewed rows that genuinely fit. */
export function rankedListCapacity({ availableHeight, rowHeight, rowGap = 0, minimumCount = 5, totalCount }) {
  const minimum = Math.max(1, Number(minimumCount) || 5);
  const total = Math.max(minimum, Number(totalCount) || minimum);
  if (!(availableHeight > 0) || !(rowHeight > 0)) return Math.min(minimum, total);
  const visible = Math.floor((availableHeight + rowGap) / (rowHeight + rowGap));
  return Math.min(total, Math.max(minimum, visible));
}

const scatterIdentityFields = [
  "featureName",
  "feature",
  "name",
  "label",
  "account",
  "plan",
  "country",
  "action",
  "product",
  "surface",
  "segment",
  "status",
];

export function scatterTooltipIdentityField(row) {
  return scatterIdentityFields.find((field) => typeof row?.[field] === "string" && row[field]);
}

const fieldNames = (values) => [...new Set(values.filter((field) => typeof field === "string" && field.length > 0))];

/**
 * Resolve the renderer's real data shape before projecting raw rows. Series values
 * become output columns after pivoting; they are not raw field dependencies.
 */
export function chartDataShape(spec, rows) {
  const { type, x, y, series = "" } = spec;
  const columns = [...new Set(rows.flatMap(Object.keys))];
  const numeric = columns.filter((column) => rows.some((row) => Number.isFinite(row[column])));
  const seriesValues = series ? [...new Set(rows.map((row) => row[series]).filter((value) => value != null))] : [];
  const fields = seriesValues.length ? seriesValues : spec.fields ?? [y];
  const barFields = type === "line" && Array.isArray(spec.barFields) ? spec.barFields : [];
  const heatmapGroup = series || columns.find((column) => column !== x && !numeric.includes(column)) || x;
  const sankeyStages =
    type === "sankey" ? (spec.stages ?? [spec.source ?? x, spec.target ?? series]).filter(Boolean) : [];
  const longForm = seriesValues.length > 0;
  const reviewedBoxPlot = type === "boxPlot" && hasReviewedBoxPlotSummary(rows);
  const requiresX = type !== "histogram" && type !== "sankey";
  const wideFields = longForm ? [] : [...fields, ...barFields];

  let required = [requiresX ? x : null, y, ...(longForm ? [series] : wideFields)];
  if (type === "histogram") required = [y];
  if (type === "sankey") required = [y, ...sankeyStages];
  if (type === "boxPlot") required = reviewedBoxPlot ? [x, ...boxPlotSummaryFields] : [x, y];
  if (type === "heatmap") required = [x, y, heatmapGroup];

  const requiredRowFields = fieldNames(required);
  const optional = [x, y, series, ...wideFields];
  const annotations = normalizeChartAnnotations(spec.annotations);
  optional.push(...chartAnnotationFields(annotations.filter((entry) => entry.kind !== "point" || !longForm)));
  if (type === "waterfall") optional.push(...waterfallRowFields(rows, x));
  if (type === "scatter") optional.push(...rows.map(scatterTooltipIdentityField));
  const requiredSet = new Set(requiredRowFields);
  const optionalRowFields = fieldNames(optional).filter((field) => columns.includes(field) && !requiredSet.has(field));
  const retained = new Set([...requiredRowFields, ...optionalRowFields]);

  return {
    columns,
    numeric,
    seriesValues,
    fields,
    barFields,
    heatmapGroup,
    sankeyStages,
    longForm,
    reviewedBoxPlot,
    requiresX,
    requiredRowFields,
    optionalRowFields,
    // Preserve source order so a second shape inference selects the same heatmap dimension.
    rowFields: columns.filter((field) => retained.has(field)),
  };
}
