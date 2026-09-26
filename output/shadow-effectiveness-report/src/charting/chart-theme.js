export const chartTypes = [
  "line",
  "area",
  "stackedArea",
  "bar",
  "horizontalBar",
  "stackedBar",
  "stackedBar100",
  "horizontalStackedBar",
  "horizontalStackedBar100",
  "histogram",
  "scatter",
  "heatmap",
  "pie",
  "leaderboard",
  "rankedList",
  "sparkline",
  "funnel",
  "waterfall",
  "boxPlot",
  "sankey",
];

export const colors = Array.from({ length: 8 }, (_, index) => `var(--chart-${index + 1})`);
const categoricalPaletteOrder = [0, 12, 6, 18, 3, 15, 9, 21, 1, 13, 7, 19, 4, 16, 10, 22, 2, 14, 8, 20, 5, 17, 11, 23];
const categoryColors = Array.from(
  { length: categoricalPaletteOrder.length },
  (_, index) => `oklch(from var(--chart-1) l max(c, 0.16) calc(h + ${categoricalPaletteOrder[index] * 15}))`,
);

function reservedConceptColor(value = "") {
  const words = String(value)
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ")
    .trim()
    .toLocaleLowerCase();
  if (/^active\s+(?:users?|accounts?)$/u.test(words)) return colors[0];
  if (/\b(?:resurrected|reactivated|returning)\b/u.test(words)) return colors[2];
  if (/\bretained\b/u.test(words)) return colors[0];
  if (/\bnew(?:\s+users?)?\b/u.test(words)) return colors[4];
  if (/\b(?:churned|churn)\b/u.test(words)) return "var(--negative)";
  if (/^(?:dau|daily active users?)$/u.test(words)) return categoryColors[0];
  if (/^(?:wau|weekly active users?)$/u.test(words)) return categoryColors[1];
  if (/^(?:mau|monthly active users?)$/u.test(words)) return categoryColors[2];
  if (/\bapproved\b/u.test(words)) return "var(--positive)";
  if (/\b(?:denied|rejected)\b/u.test(words)) return "var(--negative)";
  if (/\bfailed\b/u.test(words)) return "color-mix(in oklch, var(--negative) 72%, var(--chart-7))";
  if (/\btimed?\s*out\b/u.test(words)) return "color-mix(in oklch, var(--chart-6) 72%, var(--chart-5))";
  if (/\bdraft\b/u.test(words)) return "var(--secondary)";
  if (/\bopen\b/u.test(words)) return colors[0];
  if (/\bclosed\b/u.test(words)) return colors[2];
  return undefined;
}

/** Keep a concept on the same palette token even when its local series order changes. */
export function semanticColor({ field, dimension, value, index = 0, explicitColor } = {}) {
  if (typeof explicitColor === "string" && explicitColor.trim()) return explicitColor;
  const identity =
    value == null || value === "" ? String(field ?? "") : `${String(dimension ?? field ?? "")}:${String(value)}`;
  if (!identity) return colors[Math.abs(Number(index) || 0) % colors.length];
  let hash = 0;
  for (const character of identity.toLocaleLowerCase()) {
    hash = (Math.imul(31, hash) + character.codePointAt(0)) | 0;
  }
  return colors[(hash >>> 0) % colors.length];
}

/** Allocate distinct palette colors from complete reviewed dimensions before filtering. */
export function semanticColorResolver(queries = {}) {
  const dimensions = new Map();
  const dimensionGroups = [];
  const measureGroups = [];
  for (const query of Object.values(queries)) {
    const queryDimensions = new Map();
    const queryMeasures = new Set();
    for (const row of query?.rows ?? []) {
      for (const [field, value] of Object.entries(row)) {
        if (typeof value === "number" && Number.isFinite(value)) {
          queryMeasures.add(field);
          continue;
        }
        if (
          typeof value !== "string" ||
          !value ||
          value.toLowerCase() === "all" ||
          /^(?:\d{4}-\d{2}-\d{2}|https?:)/u.test(value)
        )
          continue;
        if (!dimensions.has(field)) dimensions.set(field, new Set());
        dimensions.get(field).add(value);
        if (!queryDimensions.has(field)) queryDimensions.set(field, new Set());
        queryDimensions.get(field).add(value);
      }
    }
    dimensionGroups.push(queryDimensions);
    measureGroups.push(queryMeasures);
  }
  const assignments = new Map();
  for (const [dimension] of dimensions) {
    const containsCountries = [...dimensions.get(dimension)].filter((value) => /^[A-Z]{2}$/u.test(value)).length >= 3;
    const palette = semanticCategoryDimension(dimension) || containsCountries ? categoryColors : colors;
    for (const group of dimensionGroups) {
      const values = [...(group.get(dimension) ?? [])].sort((left, right) => left.localeCompare(right));
      const used = new Set(
        values.map((value) => palette.indexOf(assignments.get(`${dimension}:${value}`))).filter((slot) => slot >= 0),
      );
      for (const value of values) {
        const identity = `${dimension}:${value}`;
        if (assignments.has(identity)) continue;
        let slot = colors.indexOf(semanticColor({ dimension, value }));
        if (used.size < palette.length) {
          while (used.has(slot)) slot = (slot + 1) % palette.length;
          used.add(slot);
        }
        assignments.set(identity, palette[slot]);
      }
    }
  }
  const measureAssignments = new Map();
  for (const group of measureGroups) {
    const fields = [...group].sort((left, right) => left.localeCompare(right));
    const used = new Set(
      fields.map((field) => categoryColors.indexOf(measureAssignments.get(field))).filter((slot) => slot >= 0),
    );
    for (const field of fields) {
      if (measureAssignments.has(field)) continue;
      let slot = colors.indexOf(semanticColor({ field }));
      if (used.size < categoryColors.length) {
        while (used.has(slot)) slot = (slot + 1) % categoryColors.length;
        used.add(slot);
      }
      measureAssignments.set(field, categoryColors[slot]);
    }
  }
  const categoryForField = (field) => {
    const matches = [...dimensions].filter(([, values]) => values.has(field));
    return matches.length === 1 ? assignments.get(`${matches[0][0]}:${field}`) : undefined;
  };
  const dynamicAssignments = new Map();
  const dynamicColor = (dimension, value) => {
    const key = `${dimension}:${value}`;
    if (assignments.has(key)) return assignments.get(key);
    if (dynamicAssignments.has(key)) return dynamicAssignments.get(key);
    const palette = semanticCategoryDimension(dimension) ? categoryColors : colors;
    const used = new Set(
      [...assignments, ...dynamicAssignments]
        .filter(([identity]) => identity.startsWith(`${dimension}:`))
        .map(([, color]) => palette.indexOf(color))
        .filter((index) => index >= 0),
    );
    let slot = colors.indexOf(semanticColor({ dimension, value }));
    while (used.has(slot) && used.size < palette.length) slot = (slot + 1) % palette.length;
    const selected = palette[slot];
    dynamicAssignments.set(key, selected);
    return selected;
  };
  return (descriptor = {}) => {
    if (descriptor.explicitColor) return descriptor.explicitColor;
    const reserved = reservedConceptColor(descriptor.value ?? descriptor.field);
    if (reserved) return reserved;
    if (descriptor.value != null && descriptor.value !== "") {
      return dynamicColor(descriptor.dimension ?? descriptor.field ?? "category", descriptor.value);
    }
    return descriptor.field
      ? categoryForField(descriptor.field) ??
          measureAssignments.get(descriptor.field) ??
          dynamicColor("category", descriptor.field)
      : semanticColor(descriptor);
  };
}

export function semanticCategoryDimension(field = "") {
  const words = String(field)
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ");
  return /\b(?:plans?|products?|segments?|features?|experiences?|cohorts?|channels?|categor(?:y|ies)|dimensions?|buckets?|status(?:es)?|states?|stages?|countr(?:y|ies)|geograph(?:y|ies)|actions?|surfaces?|lifecycles?|drivers?)\b/iu.test(
    words,
  );
}

export function ratioMetric(field = "", values = []) {
  return (
    /(?:ratio|rate|conversion|retention|percent|percentage|share|penetration|adoption)$/iu.test(String(field)) &&
    values.some((value) => Number.isFinite(value)) &&
    values.every((value) => !Number.isFinite(value) || (value >= 0 && value <= 1))
  );
}

export function deltaDirection(field, value) {
  const words = String(field)
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ");
  if (!/\b(?:delta|change|variance|growth|difference|diff|movement)(?:\b|\d)/iu.test(words)) return "";
  const normalized =
    typeof value === "number"
      ? value
      : Number(
          String(value)
            .trim()
            .replace(/[,$€£%\s]/gu, "")
            .replace(/^[−–]/u, "-")
            .replace(/[KMBT]$/iu, ""),
        );
  return Number.isFinite(normalized) && normalized !== 0 ? (normalized < 0 ? "negative" : "positive") : "";
}

export const compact = (value) =>
  new Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);

const numericTickValues = (values) =>
  [...new Set(values.filter(Number.isFinite).map((value) => (value === 0 ? 0 : value)))].sort(
    (left, right) => left - right,
  );

/** Keep all observed tick values until the axis domain or formatting mode changes. */
export function mergeNumericAxisTicks(previous, key, values) {
  const merged = numericTickValues([...(previous?.key === key ? previous.values : []), ...values]);
  const signature = JSON.stringify(merged);
  return previous?.key === key && previous.signature === signature ? previous : { key, values: merged, signature };
}

/** Axis-only precision; KPI, mark, and tooltip formatting remains independent. */
export function numericAxisFormatter(tickValues = [], { percent = false, locale } = {}) {
  const values = numericTickValues(tickValues);
  const style = percent ? { style: "percent" } : {};
  const zero = new Intl.NumberFormat(locale, { ...style, maximumFractionDigits: 0 }).format(0);
  const formatter = (options) => {
    const number = new Intl.NumberFormat(locale, { ...style, signDisplay: "exceptZero", ...options });
    return (value) =>
      !Number.isFinite(value)
        ? ""
        : value === 0
          ? zero
          : number
              .formatToParts(value)
              .filter((part) => part.type !== "plusSign")
              .map((part) => part.value)
              .join("");
  };
  const distinct = (format) => new Set(values.map(format)).size === values.length;
  const readable = (format) => values.every((value) => format(value).length <= 24);
  const exact = formatter({ maximumSignificantDigits: 21 });
  const scientific = formatter({ notation: "scientific", maximumSignificantDigits: 21 });
  // Before Recharts reports its ticks, do not round unknown nearby values together.
  if (values.length < 2)
    return (value) => {
      const label = exact(value);
      return label.length <= 24 ? label : scientific(value);
    };

  const maxAbs = Math.max(...values.map(Math.abs));
  const notation = percent || maxAbs < 10_000 ? "standard" : "compact";
  const maximum = notation === "compact" ? 3 : 12;
  for (let digits = 1; digits <= maximum; digits += 1) {
    const format = formatter({ notation, maximumFractionDigits: digits });
    if (distinct(format) && readable(format)) return format;
  }
  // Narrow large-value domains are clearer as grouped numbers than long compact decimals.
  if (notation === "compact") {
    for (let digits = 0; digits <= 12; digits += 1) {
      const format = formatter({ maximumFractionDigits: digits });
      if (distinct(format) && readable(format)) return format;
    }
  }
  if (distinct(exact) && readable(exact)) return exact;
  for (let digits = 3; digits <= 21; digits += 1) {
    const format = formatter({ notation: "scientific", maximumSignificantDigits: digits });
    if (distinct(format)) return format;
  }
  return scientific;
}

export function displayValue(value) {
  if (value == null || value === "") return "—";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value)) {
    const date = new Date(value.length === 10 ? `${value}T00:00:00Z` : value);
    if (!Number.isNaN(date.valueOf()))
      return new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        year: "numeric",
        timeZone: "UTC",
      }).format(date);
  }
  if (typeof value !== "number" || !Number.isFinite(value)) return String(value);
  if (Math.abs(value) >= 10_000) return compact(value);
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
    minimumFractionDigits: Number.isInteger(value) ? 0 : Math.abs(value) < 1 ? 2 : 0,
  }).format(value);
}

export const percentage = (value) =>
  new Intl.NumberFormat(undefined, {
    style: "percent",
    maximumFractionDigits: 1,
    signDisplay: "exceptZero",
  }).format(value);

export function periodComparison(
  current,
  previous,
  { percentagePoints = false, lowerIsBetter = false, previousPeriod = "previous reporting period" } = {},
) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || (!percentagePoints && previous === 0)) return {};
  const change = percentagePoints ? current - previous : (current - previous) / Math.abs(previous);
  const formatted = percentagePoints
    ? `${new Intl.NumberFormat(undefined, {
        maximumFractionDigits: 1,
        signDisplay: "exceptZero",
      }).format(change * 100)} pp`
    : percentage(change);
  return {
    comparison: `${formatted} vs. ${dateValue(previousPeriod) ? shortDate(previousPeriod) : previousPeriod}`,
    negative: lowerIsBetter ? change > 0 : change < 0,
  };
}

function dateValue(value) {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value);
}

export const shortDate = (value) =>
  new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(new Date(value));

export const label = (field = "") =>
  String(field)
    .replace(/([a-z\d])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ")
    .replace(/\b(?:arr|mau|wau|dau|api|sql|kpi|cli|ide|mcp|p50|p95|p99)\b/giu, (abbreviation) =>
      abbreviation.toUpperCase(),
    )
    .replace(/^./u, (character) => character.toUpperCase());

export function categoryLabel(field = "", value = "") {
  const raw = String(value);
  const dimension = String(field)
    .replace(/([a-z])([A-Z])/gu, "$1 $2")
    .replaceAll("_", " ");
  if (/\b(?:country|countries|geography)\b/iu.test(dimension) && /^[A-Z]{2}$/u.test(raw)) {
    try {
      return new Intl.DisplayNames(undefined, { type: "region" }).of(raw) ?? raw;
    } catch {
      return raw;
    }
  }
  return label(raw)
    .replace(/\bSelf serve\b/iu, "Self-serve")
    .replace(/\busage based\b/iu, "usage-based")
    .replace(/\bEdu\b/iu, "Education")
    .replace(/\bCbp\s+/iu, "");
}

export const tick = (value, { includeYear = false } = {}) => {
  if (typeof value !== "string") return value;
  const month = /^\d{4}-(?:0[1-9]|1[0-2])$/u.test(value);
  if (!month && !/^\d{4}-\d{2}-\d{2}(?:T|$)/u.test(value)) return value;
  const date = new Date(month ? `${value}-01T00:00:00Z` : value);
  if (!Number.isFinite(date.getTime())) return value;
  return date.toLocaleDateString("en-US", { month: "short", ...(month ? {} : { day: "numeric" }),
    ...(month || includeYear ? { year: "numeric" } : {}), timeZone: "UTC" });
};
// Anchor the first stage to the unmodified core hue; tint only later stages.
export function funnelStageColor(color, index, count, active = false) {
  if (index === 0) return active ? `color-mix(in srgb, ${color} 92%, var(--text))` : color;
  const strength = Math.min(100, Math.round(100 - index / Math.max(1, count - 1) * 76) + (active ? 8 : 0));
  return `color-mix(in srgb, ${color} ${strength}%, var(--surface))`;
}
