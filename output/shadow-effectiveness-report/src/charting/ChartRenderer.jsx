import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Dot,
  Label,
  LabelList,
  Line,
  LineChart,
  Pie,
  PieChart,
  Rectangle,
  ReferenceArea,
  Sankey,
  Scatter,
  ScatterChart,
  Tooltip,
  getNiceTickValues,
  useChartWidth,
  usePlotArea,
  useXAxisDomain,
  useXAxisTicks,
  useYAxisDomain,
  useYAxisTicks,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

import { ChartFrame } from "./ChartFrame.jsx";
import { chartAnnotationMarks, ChartAnnotationLayer, ChartAnnotationNotes, useChartText } from "./ChartAnnotations.jsx";
import { benchmarkAnnotationDomain, bindChartAnnotationAxes, resolveChartAnnotations } from "./chart-annotations.js";
import { ChartTooltip } from "./ChartTooltip.jsx";
import { FunnelRenderer } from "./FunnelRenderer.jsx";
import {
  categoryAxisLayout,
  chartDataShape,
  groupAdditiveCategories,
  groupAdditiveSeries,
  isTemporalCategory,
  orderCalendarRows,
  orderedDistribution,
  rankedListCapacity,
  resolvedChartType,
  secondaryAxisFields,
  temporalAxisTicks,
  visibleGroupedCategories,
} from "./chart-data-shape.js";
import { useDashboardAsk } from "../components/DashboardAsk.jsx";
import {
  boxPlots,
  heatmap,
  histogram,
  isolatedPointIndexes,
  normalizeZoomRange,
  pivot,
  sankeyGraph,
  stackedMarkBounds,
  waterfall,
  waterfallValueDomain,
} from "./chart-transforms.js";
import {
  categoryLabel,
  colors,
  compact,
  displayValue,
  label,
  mergeNumericAxisTicks,
  numericAxisFormatter,
  percentage,
  ratioMetric,
  semanticCategoryDimension,
  semanticColor,
  tick,
} from "./chart-theme.js";

const grid = <CartesianGrid stroke="var(--border)" strokeWidth={0.5} vertical={false} />;
const margin = { top: 10, right: 14, bottom: 8, left: 9 };
// Recharts shallow-compares label settings. A fresh nested style resets the
// measured auto axis width on each annotation update and can oscillate layout.
const valueAxisLabelStyle = { fill: "var(--secondary)", fontSize: 12, textAnchor: "middle" };

function useBenchmarkAnnotationDomain(annotations, data, fields, axisId, height, startAtZero, ratio) {
  const benchmarks = annotations.filter((entry) => entry.kind === "benchmark" && entry.valueAxisId === axisId)
    .map(({ y }) => y);
  const values = benchmarks.length ? data.flatMap((row) => fields.map((field) => row[field])).filter(Number.isFinite) : [];
  const benchmarkKey = JSON.stringify(benchmarks);
  const domainLow = Math.min(...values, ...benchmarks), domainHigh = Math.max(...values, ...benchmarks);
  return useMemo(() => {
    const domain = benchmarkAnnotationDomain(JSON.parse(benchmarkKey), [domainLow, domainHigh], height, startAtZero, ratio);
    if (!domain) return undefined;
    const ticks = getNiceTickValues(domain, 6), step = ticks[1] - ticks[0];
    const rounded = domain.map((value) => value < domainLow ? Math.floor(value / step) * step
      : value > domainHigh ? Math.ceil(value / step) * step : value);
    return rounded.every(Number.isFinite) ? rounded : domain;
  }, [benchmarkKey, domainLow, domainHigh, height, startAtZero, ratio]);
}

function tooltipLabel(value) {
  return tick(value);
}

function useNumericAxisFormatter(axis, axisId, domain, ticks, percent, tickCount, allowDecimals) {
  const numericDomain =
    Array.isArray(domain) && domain.length === 2 && domain.every(Number.isFinite)
      ? [...domain].sort((left, right) => left - right)
      : null;
  const domainSignature = JSON.stringify(numericDomain);
  const key = JSON.stringify([axis, axisId, domainSignature, percent, tickCount, allowDecimals]);
  const seed = useMemo(() => {
    const bounds = JSON.parse(domainSignature);
    return bounds ? getNiceTickValues(bounds, tickCount, allowDecimals).filter(Number.isFinite) : [];
  }, [domainSignature, tickCount, allowDecimals]);
  const bounds = numericDomain ? [...numericDomain, ...seed] : null;
  const minimum = bounds ? Math.min(...bounds) : -Infinity;
  const maximum = bounds ? Math.max(...bounds) : Infinity;
  const tolerance = Math.max(1, Math.abs(minimum), Math.abs(maximum)) * Number.EPSILON * 16;
  const reported = (ticks ?? [])
    .map((entry) => entry.value)
    .filter((value) => Number.isFinite(value) && value >= minimum - tolerance && value <= maximum + tolerance);
  const candidates = mergeNumericAxisTicks(null, key, [...seed, ...reported]);
  const [history, setHistory] = useState(candidates);
  const current = mergeNumericAxisTicks(history, key, candidates.values);
  const signature = current.signature;
  const candidateSignature = candidates.signature;
  useEffect(() => {
    setHistory((previous) => mergeNumericAxisTicks(previous, key, JSON.parse(candidateSignature)));
  }, [key, candidateSignature]);
  return useMemo(() => numericAxisFormatter(JSON.parse(signature), { percent }), [signature, percent]);
}

function NumericXAxis({ percent = false, xAxisId = 0, tickCount = 5, allowDecimals = true, ...props }) {
  const format = useNumericAxisFormatter(
    "x",
    xAxisId,
    useXAxisDomain(xAxisId),
    useXAxisTicks(xAxisId),
    percent,
    tickCount,
    allowDecimals,
  );
  return (
    <XAxis {...props} xAxisId={xAxisId} tickCount={tickCount} allowDecimals={allowDecimals} tickFormatter={format} />
  );
}

function NumericYAxis({ percent = false, yAxisId = 0, tickCount = 5, allowDecimals = true, ...props }) {
  const format = useNumericAxisFormatter(
    "y",
    yAxisId,
    useYAxisDomain(yAxisId),
    useYAxisTicks(yAxisId),
    percent,
    tickCount,
    allowDecimals,
  );
  return (
    <YAxis {...props} yAxisId={yAxisId} tickCount={tickCount} allowDecimals={allowDecimals} tickFormatter={format} />
  );
}

function HeatCell({ cx, cy, payload, columnCount, rowCount, radius, baseColor = "var(--chart-1)" }) {
  const plot = usePlotArea();
  const width = Math.max(8, (plot?.width ?? 240) / Math.max(1, columnCount) - 5);
  const height = Math.max(8, (plot?.height ?? 160) / Math.max(1, rowCount) - 5);
  return (
    <rect
      className="chart-heatmap-cell"
      x={cx - width / 2}
      y={cy - height / 2}
      width={width}
      height={height}
      rx={Math.min(radius, width / 5, height / 5)}
      data-structural-zero={payload.__missing ? "true" : undefined}
      fill={`color-mix(in srgb, ${baseColor} ${Math.max(12, payload.intensity * 100)}%, var(--surface))`}
    />
  );
}

function SankeyNode({ x, y, width, height, payload, colorFor }) {
  const color = colorFor({ field: payload.field, dimension: payload.field, value: payload.name, index: payload.stage });
  const right = payload.targetNodes?.length === 0;
  const textX = right ? x - 8 : x + width + 8;
  return (
    <g className="chart-sankey-node">
      <rect x={x} y={y} width={width} height={height} rx={Math.min(4, height / 3)} fill={color} />
      <text
        x={textX}
        y={y + Math.min(height / 2 + 4, 17)}
        textAnchor={right ? "end" : "start"}
        fill="var(--text)"
        fontSize={12}
      >
        {categoryLabel(payload.field, payload.name)}
      </text>
      {height >= 32 && (
        <text
          x={textX}
          y={y + Math.min(height / 2 + 19, 32)}
          textAnchor={right ? "end" : "start"}
          fill="var(--secondary)"
          fontSize={11}
        >
          {compact(payload.value)}
        </text>
      )}
      <title>{`${categoryLabel(payload.field, payload.name)}: ${compact(payload.value)}`}</title>
    </g>
  );
}

function HeatmapTick({ x, y, payload, values, angled = false }) {
  const display = (raw) => {
    const time = typeof raw === "string" && /^(?:[01]\d|2[0-3]):[0-5]\d$/u.test(raw)
      ? `${Number(raw.slice(0, 2)) % 12 || 12}${raw.slice(3) === "00" ? "" : `:${raw.slice(3)}`} ${Number(raw.slice(0, 2)) < 12 ? "AM" : "PM"}`
      : tick(raw);
    return categoryLabel("", time);
  };
  const value = display(values[payload.value] ?? "");
  const plot = usePlotArea();
  const spacing = (plot?.width ?? values.length * 82) / Math.max(1, values.length);
  const angle = angled && spacing < 50 ? -60 : -45;
  const projection = angled ? Math.cos(Math.abs(angle) * Math.PI / 180) : 1;
  const widest = Math.max(42, ...values.map((raw) => Math.min(104, display(raw).length * 7 + 12)));
  const interval = angled ? 1 : Math.max(
    1,
    Math.ceil((values.length * widest) / Math.max(1, plot?.width ?? values.length * 82)),
  );
  const index = Number(payload.value);
  if (index % interval !== 0 && index !== values.length - 1) return null;
  if (index !== 0 && index !== values.length - 1 && values.length - 1 - index < interval) return null;
  const capacity = Math.max(
    angled ? 4 : 5,
    Math.min(angled ? spacing < 35 ? 4 : angle === -60 ? 8 : 12 : 24,
      Math.floor((spacing * interval - (angled ? 12 : 8)) / (7 * projection))),
  );
  const visible = value.length > capacity ? `${value.slice(0, capacity - 1).trimEnd()}…` : value;
  const labelY = y + 11;
  return (
    <text x={x} y={labelY} fill="var(--secondary)" textAnchor={angled ? "end" : "middle"} fontSize={12}
      transform={angled ? `rotate(${angle} ${x} ${labelY})` : undefined}
      data-axis-layout={angled ? "angled" : "horizontal"}>
      <title>{value}</title>
      {visible}
    </text>
  );
}

function TemporalAxisTick({ x, y, payload, measureFont, measureText, includeYear }) {
  const width = useChartWidth();
  const label = tick(payload.value, { includeYear });
  const halfLabel = measureText(label) / 2 + 2;
  const textX = width ? Math.max(halfLabel, Math.min(width - halfLabel, x)) : x;
  return (
    <text ref={measureFont} x={textX} y={y + 11} fill="var(--secondary)" textAnchor="middle" fontSize={12}>
      {label}
    </text>
  );
}

function TemporalXAxis({ values, ...props }) {
  const plot = usePlotArea();
  const { measureFont, measureText } = useChartText();
  const includeYear = new Set(values.map((value) => value.slice(0, 4))).size > 1;
  const labelWidth = Math.max(1, ...values.map((value) => measureText(tick(value, { includeYear }))));
  const maxTicks = Math.max(2, Math.floor((plot?.width ?? 240) / (labelWidth + 16)));
  return <XAxis {...props} ticks={temporalAxisTicks(values, { maxTicks })} interval={0}
    tick={<TemporalAxisTick includeYear={includeYear} measureFont={measureFont} measureText={measureText} />} />;
}

function compactAxisCategory(value, capacity) {
  if (value.length <= capacity) return value;
  const words = value.split(/\s+/u);
  if (words.length > 1) {
    const prefix = words.slice(0, -1).map((word) => `${word[0]}.`).join(" ") + " ";
    const suffix = words.at(-1);
    const combined = prefix + suffix;
    if (combined.length <= capacity) return combined;
    if (capacity - prefix.length >= 3) return `${prefix}${suffix.slice(0, capacity - prefix.length - 1)}…`;
  }
  return `${value.slice(0, capacity - 1).trimEnd()}…`;
}

function CategoryAxisTick({ x, y, payload, field, horizontal = false, angled = false, width = 150, count = 1 }) {
  const value = categoryLabel(field, payload.value);
  const plot = usePlotArea();
  const spacing = (plot?.width ?? count * 80) / Math.max(1, count);
  const angle = angled && spacing < 50 ? -60 : -45;
  const projection = angled ? Math.cos(Math.abs(angle) * Math.PI / 180) : 1;
  const capacity = horizontal
    ? Math.max(6, Math.floor((width - 22) / 8))
    : Math.max(angled ? 4 : 1, Math.min(angled ? spacing < 35 ? 4 : angle === -60 ? 8 : 12 : 18,
      Math.floor((spacing - (angled ? 12 : 6))
      / (angled ? 7 * projection : 8))));
  const renderedTicks = useXAxisTicks();
  const { measureText: measure, measureFont } = useChartText();
  const available = horizontal ? width - 14
    : (plot?.width ?? count * 80) / Math.max(1, renderedTicks?.length ?? count) - 12;
  const fit = (text) => {
    if (measure(text) <= available) return text;
    if (measure("…") > available) return "";
    let head = [...text];
    while (head.length && measure(`${head.join("")}…`) > available) head.pop();
    return `${head.join("").trimEnd()}…`;
  };
  if (horizontal) {
    return (
      <text ref={measureFont} x={x} y={y + 4} fill="var(--secondary)" textAnchor="end" fontSize={12}>
        <title>{value}</title>
        {fit(value)}
      </text>
    );
  }
  if (angled) {
    const visible = compactAxisCategory(value, capacity);
    const labelY = y + 11;
    return (
      <text x={x} y={labelY} fill="var(--secondary)" textAnchor="end" fontSize={12}
        transform={`rotate(${angle} ${x} ${labelY})`} data-axis-layout="angled">
        <title>{value}</title>
        {visible}
      </text>
    );
  }
  const lines = [];
  for (const word of value.split(/\s+/u)) {
    const current = lines.at(-1);
    if (current && `${current} ${word}`.length <= capacity) lines[lines.length - 1] = `${current} ${word}`;
    else lines.push(word);
  }
  const visible = lines.slice(0, 2);
  if (lines.length > 2) visible[1] += "…";
  return (
    <text ref={measureFont} x={x} y={y + 11} fill="var(--secondary)" textAnchor="middle" fontSize={12}
      data-axis-layout={visible.length > 1 ? "wrapped" : "horizontal"}>
      <title>{value}</title>
      {visible.map((line, index) => (
        <tspan key={`${line}-${index}`} x={x} dy={index ? 13 : 0}>
          {fit(line)}
        </tspan>
      ))}
    </text>
  );
}

function StackedMarkShape({ x, y, width, height, fill, payload, fields, field, horizontal, radius }) {
  const clipId = useId().replaceAll(":", "");
  const bounds = stackedMarkBounds(payload, fields, field, { x, y, width, height, horizontal });
  if (!bounds) return null;
  const rounded = Math.min(radius, bounds.width / 2, bounds.height / 2);
  return (
    <g data-stack-sign={Number(payload[field]) < 0 ? "negative" : "positive"}>
      <defs>
        <clipPath id={clipId}>
          <Rectangle {...bounds} radius={[rounded, rounded, rounded, rounded]} />
        </clipPath>
      </defs>
      <Rectangle x={x} y={y} width={width} height={height} fill={fill} clipPath={`url(#${clipId})`} />
    </g>
  );
}

function BoxShape({ x, y, width, height, payload, fill }) {
  const ratio = height / Math.max(1, payload.upperQuartile - payload.lowerQuartile);
  const center = x + width / 2;
  const cap = Math.min(12, width * 0.28);
  const top = y - (payload.maximum - payload.upperQuartile) * ratio;
  const bottom = y + height + (payload.lowerQuartile - payload.minimum) * ratio;
  const median = y + (payload.upperQuartile - payload.median) * ratio;
  const color = fill ?? colors[0];
  return (
    <g className="chart-box-plot" stroke={color} strokeWidth={1.5}>
      <line x1={center} x2={center} y1={top} y2={bottom} strokeOpacity={0.75} />
      <line x1={center - cap} x2={center + cap} y1={top} y2={top} />
      <line x1={center - cap} x2={center + cap} y1={bottom} y2={bottom} />
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        rx={Math.min(6, width / 8)}
        fill={"color-mix(in srgb, " + color + " 16%, var(--surface))"}
      />
      <line x1={x + 1} x2={x + width - 1} y1={median} y2={median} strokeWidth={2.5} />
    </g>
  );
}

function WaterfallShape({ x, y, width, height, fill, radius }) {
  return <Rectangle x={x} y={y} width={width} height={height} radius={radius} fill={fill} />;
}

function SparseValueLabel({ x, y, width = 0, value, index, count }) {
  const interval = Math.max(1, Math.ceil(count / 8));
  if (index % interval !== 0 && index !== count - 1) return null;
  if (!Number.isFinite(Number(value))) return null;
  return (
    <text x={Number(x) + Number(width) / 2} y={Number(y) - 8} fill="var(--secondary)" fontSize={12} textAnchor="middle">
      {compact(Number(value))}
    </text>
  );
}

function IsolatedLineDot({ indexes, index, cx, cy, stroke, fill, strokeWidth, strokeOpacity, className }) {
  if (!indexes.has(index)) return null;
  return (
    <Dot
      cx={cx}
      cy={cy}
      r={3}
      stroke={stroke}
      fill={stroke ?? fill}
      strokeWidth={strokeWidth}
      strokeOpacity={strokeOpacity}
      fillOpacity={strokeOpacity}
      className={["recharts-line-dot", className].filter(Boolean).join(" ")}
      data-isolated-point="true"
    />
  );
}

const dateValue = isTemporalCategory;

export function ChartRenderer({
  spec,
  rows,
  accessibleLabel,
  height = 240,
  chartId,
  resolveColor,
  themeRoot,
  visibleSeries,
  onVisibleSeriesChange,
  zoomRange,
  onZoomChange,
}) {
  const { selectChartMark, selectChartSection, selectionEnabled } = useDashboardAsk();
  const [hiddenSeries, setHiddenSeries] = useState(() => new Set());
  const [localZoom, setLocalZoom] = useState(null);
  const [selection, setSelection] = useState(null);
  const [rankingExpanded, setRankingExpanded] = useState(false);
  const [placedAnnotationIds, setPlacedAnnotationIds] = useState([]);
  const rankingListRef = useRef(null);
  const minimumRankingRows = Math.max(1, Number(spec.initialVisibleCount) || 5);
  const [fittedRankingRows, setFittedRankingRows] = useState(minimumRankingRows);
  const type = resolvedChartType(spec, rows);
  const { x, y, series = "" } = spec;
  const controlledVisible = visibleSeries == null ? null : new Set([...visibleSeries].map(String));
  const groupedField = type === "pie" ? x : series;
  const visibleSourceCategories = controlledVisible
    ?? new Set(rows.map((row) => String(row[groupedField])).filter((category) => !hiddenSeries.has(category)));
  const groupedSeriesRows = series && ["line", "area", "stackedArea", "bar", "stackedBar", "stackedBar100",
    "horizontalStackedBar", "horizontalStackedBar100"].includes(type)
    ? groupAdditiveSeries(rows, {
        groupField: x,
        categoryField: series,
        valueField: y,
        maxCategories: spec.maxCategories ?? 7,
        preserveCategories: spec.preserveCategories ?? [],
        enabled: spec.groupOther === true,
      })
    : rows;
  const seriesRows = visibleGroupedCategories(rows, groupedSeriesRows, {
    categoryField: series, valueField: y, groupField: x, visibleCategories: visibleSourceCategories,
  });
  const groupedPieRows = type === "pie"
    ? groupAdditiveCategories(rows, {
        categoryField: x,
        valueField: y,
        maxCategories: spec.maxCategories ?? 7,
        preserveCategories: spec.preserveCategories ?? [],
        enabled: spec.groupOther === true,
      })
    : rows;
  const pieRows = visibleGroupedCategories(rows, groupedPieRows, {
    categoryField: x, valueField: y, visibleCategories: visibleSourceCategories,
  });
  const { numeric, seriesValues, fields, barFields, heatmapGroup, sankeyStages } = chartDataShape(spec, seriesRows);

  useEffect(() => {
    if (type !== "rankedList" || rankingExpanded) return undefined;
    const ranking = rankingListRef.current;
    const component = ranking?.closest("[data-component-id]");
    const firstRow = ranking?.querySelector(".chart-ranked-list-row");
    if (!ranking || !component || !firstRow) return undefined;
    const layoutItem = component.closest(".sortable-item") ?? component;
    const layout = layoutItem.parentElement;

    let frame;
    const updateCapacity = () => {
      const itemBounds = layoutItem.getBoundingClientRect();
      const adjacent = [...(layout?.children ?? [])].some((candidate) => {
        if (candidate === layoutItem || candidate.querySelector(".chart-ranked-list")) return false;
        const bounds = candidate.getBoundingClientRect();
        return bounds.width > 0 && bounds.height > 0 && Math.abs(bounds.top - itemBounds.top) < 2;
      });
      if (!adjacent) {
        setFittedRankingRows((current) => current === minimumRankingRows ? current : minimumRankingRows);
        return;
      }
      const componentStyle = getComputedStyle(component);
      const rankingStyle = getComputedStyle(ranking);
      const toggle = component.querySelector(".chart-ranked-list-toggle");
      const toggleStyle = toggle ? getComputedStyle(toggle) : null;
      const toggleHeight = toggle
        ? toggle.getBoundingClientRect().height + Number.parseFloat(toggleStyle.marginTop || "0")
        : 0;
      const availableHeight = component.getBoundingClientRect().bottom
        - Number.parseFloat(componentStyle.paddingBottom || "0")
        - ranking.getBoundingClientRect().top
        - toggleHeight;
      const capacity = rankedListCapacity({
        availableHeight,
        rowHeight: firstRow.getBoundingClientRect().height,
        rowGap: Number.parseFloat(rankingStyle.rowGap || "0"),
        minimumCount: minimumRankingRows,
        totalCount: rows.length,
      });
      setFittedRankingRows((current) => current === capacity ? current : capacity);
    };
    const scheduleCapacity = () => {
      if (frame) cancelAnimationFrame(frame);
      frame = requestAnimationFrame(updateCapacity);
    };
    scheduleCapacity();
    if (typeof ResizeObserver !== "function") return () => cancelAnimationFrame(frame);
    const observer = new ResizeObserver(scheduleCapacity);
    observer.observe(component);
    observer.observe(firstRow);
    if (layout) observer.observe(layout);
    return () => {
      observer.disconnect();
      if (frame) cancelAnimationFrame(frame);
    };
  }, [minimumRankingRows, rankingExpanded, rows.length, type]);
  const groupedRows = type === "pie" ? pieRows : seriesRows;
  const displayedCategories = new Set(groupedRows.map((row) => String(row[groupedField])));
  const groupedCategories = rows.map((row) => String(row[groupedField]))
    .filter((category) => !displayedCategories.has(category));
  const isVisible = (value) => {
    if (!controlledVisible) {
      if (String(value) === "Other" && groupedCategories.length) {
        return !hiddenSeries.has("Other") && groupedCategories.some((category) => !hiddenSeries.has(category));
      }
      return !hiddenSeries.has(String(value));
    }
    return controlledVisible.has(String(value))
      || String(value) === "Other" && groupedCategories.some((category) => controlledVisible.has(category));
  };
  const visibleFields = fields.filter(isVisible);
  const activeZoom = zoomRange ?? localZoom;
  const zoomable = ["line", "area", "stackedArea"].includes(type) && rows.some((row) => dateValue(row[x]));
  const plottedRows =
    zoomable && activeZoom?.start && activeZoom?.end
      ? seriesRows.filter((row) => String(row[x]) >= String(activeZoom.start) && String(row[x]) <= String(activeZoom.end))
      : seriesRows;
  const visibleRows = type === "pie" ? pieRows.filter((row) => isVisible(row[x])) : plottedRows;
  const sourceData = seriesValues.length ? pivot(plottedRows, x, series, y) : plottedRows;
  const numericTrend = ["line", "area", "stackedArea"].includes(type)
    && sourceData.length > 1 && sourceData.every((row) => typeof row[x] === "number" && Number.isFinite(row[x]));
  const calendarChart = ["line", "area", "stackedArea", "bar", "stackedBar", "stackedBar100",
    "horizontalBar", "horizontalStackedBar", "horizontalStackedBar100"].includes(type);
  const data = numericTrend ? [...sourceData].sort((left, right) => left[x] - right[x])
    : calendarChart ? orderCalendarRows(sourceData, x, spec) : sourceData;
  const rightFields = secondaryAxisFields({ ...spec, type }, data, fields);
  const annotations = bindChartAnnotationAxes(resolveChartAnnotations(spec, plottedRows, { data, visibleFields }), rightFields);
  const sorted = (source) => {
    if (!["ascending", "descending"].includes(spec.sortOrder)) return source;
    const value = (row) =>
      Number.isFinite(row[y]) ? row[y] : visibleFields.reduce((total, field) => total + (Number(row[field]) || 0), 0);
    const direction = spec.sortOrder === "descending" ? -1 : 1;
    return [...source].sort((left, right) => direction * (value(left) - value(right)));
  };
  const horizontal = type === "horizontalBar" || type === "leaderboard" || type.startsWith("horizontalStacked");
  const stacked = type.toLowerCase().includes("stacked");
  const proportional = type.endsWith("100");
  const barPoints = type === "bar" || type === "horizontalBar";
  const annotationRanges = chartAnnotationMarks(annotations, { horizontal, layer: "ranges" });
  const annotationMarks = chartAnnotationMarks(annotations, { horizontal, barPoints });
  const annotationLabels = annotations.length
    ? <ChartAnnotationLayer annotations={annotations} horizontal={horizontal} barPoints={barPoints}
      onPlacedChange={setPlacedAnnotationIds} /> : null;
  const ratio = ratioMetric(
    y,
    rows.map((row) => row[y]),
  );
  const leftFields = fields.filter((field) => !rightFields.includes(field));
  const separateAxes = rightFields.length > 0;
  const fieldAxis = (field) => rightFields.includes(field) ? "secondary" : 0;
  const axisRatio = (axisFields) => axisFields.every((field) => ratioMetric(field, data.map((row) => row[field])));
  const valueIsRatio = (field) => seriesValues.length ? ratio : ratioMetric(field, data.map((row) => row[field]));
  const formatMarkValue = (value, field = y) =>
    valueIsRatio(field) ? percentage(Number(value)).replace("+", "") : compact(Number(value));
  const formatTooltipValue = (value, field = y) =>
    valueIsRatio(field) ? percentage(Number(value)).replace("+", "") : displayValue(value);
  const yTitle = spec.yLabel ?? (separateAxes ? leftFields.map(label).join(" / ") : label(y));
  const rightTitle = spec.rightYAxisLabel ?? rightFields.map(label).join(" / ");
  const xTitle = spec.xLabel ?? label(x);
  const showXAxisTitle = spec.showXAxisLabel !== false;
  const showYAxisTitle = spec.showYAxisLabel !== false;
  const chartMargin = {
    ...margin,
    left: showYAxisTitle ? margin.left : 0,
    bottom: showXAxisTitle ? margin.bottom : 0,
  };
  const colorFor = ({ field, value, index = 0, dimension, explicitColor } = {}) => {
    const descriptor = { field: field ?? y, dimension, value, index, explicitColor };
    return explicitColor ?? resolveColor?.(descriptor) ?? semanticColor(descriptor);
  };
  const pieColor = (row) => {
    const index = rows.findIndex((candidate) => candidate[x] === row[x]);
    return colorFor({
      field: y,
      dimension: x,
      value: row[x],
      index: Math.max(index, 0),
      explicitColor: spec.colors?.[row[x]] ?? (index < 0 ? "var(--secondary)" : undefined),
    });
  };
  const fieldColor = (field) =>
    seriesValues.length
      ? colorFor({
          field: y,
          dimension: series,
          value: field,
          index: fields.indexOf(field),
          explicitColor: spec.colors?.[field] ?? (field === "Other" ? "var(--secondary)" : undefined),
        })
      : colorFor({ field, index: fields.indexOf(field), explicitColor: spec.colors?.[field] });
  const numericAxisValues = data
    .flatMap((row) =>
      stacked
        ? [visibleFields.reduce((total, field) => total + (Number(row[field]) || 0), 0)]
        : visibleFields.map((field) => Number(row[field])),
    )
    .filter(Number.isFinite);
  const numericAxisMaximum = Math.max(0, ...numericAxisValues.map(Math.abs));
  const annotationFields = visibleFields.filter((field) => type !== "line" || !spec.barFields?.includes(field));
  const benchmarkDomain = useBenchmarkAnnotationDomain(annotations, data,
    annotationFields.filter((field) => fieldAxis(field) === 0), 0, height, spec.startAtZero !== false,
    separateAxes ? axisRatio(leftFields) : ratio);
  const secondaryBenchmarkDomain = useBenchmarkAnnotationDomain(annotations, data,
    annotationFields.filter((field) => fieldAxis(field) === "secondary"), "secondary", height,
    spec.startAtZero !== false, axisRatio(rightFields));
  const formatNumericAxisTick = numericAxisFormatter([...numericAxisValues, numericAxisMaximum * 1.25], {
    percent: proportional || ratio,
  });
  const numericAxisTickLength = Math.max(
    proportional ? 4 : 1,
    ...[...numericAxisValues, numericAxisMaximum * 1.25].map((value) => String(formatNumericAxisTick(value)).length),
  );
  const numericAxisWidth = Math.max(
    34,
    Math.min(82, Math.ceil(numericAxisTickLength * 7 + 14 + (showYAxisTitle && yTitle ? 20 : 0))),
  );
  const categoryWidth = horizontal
    ? Math.max(54, Math.min(126, Math.max(0, ...rows.map((row) => categoryLabel(x, row[x] ?? "").length * 6.4 + 18))))
    : numericAxisWidth;
  const heatmapYAxisTitle = spec.yLabel ?? label(heatmapGroup);
  const heatmapCategoryWidth =
    type === "heatmap"
      ? Math.max(
          38,
          Math.min(126, Math.max(0, ...rows.map((row) => String(row[heatmapGroup] ?? "").length * 6.1 + 10))),
        ) + (showYAxisTitle && heatmapYAxisTitle ? 18 : 0)
      : 86;
  const referenceField = (field) => /(?:target|plan|forecast|projected|benchmark)/iu.test(String(field));
  const primaryValueField = visibleFields.find((field) => !referenceField(field)) ?? visibleFields[0];
  const themeElement =
    themeRoot?.host ?? themeRoot ?? (typeof document === "undefined" ? null : document.documentElement);
  const themeView = themeElement?.ownerDocument?.defaultView;
  const readThemeStyle =
    themeView?.getComputedStyle?.bind(themeView) ?? (typeof getComputedStyle === "function" ? getComputedStyle : null);
  const markRadius =
    !themeElement || !readThemeStyle
      ? 0
      : Number.parseFloat(readThemeStyle(themeElement).getPropertyValue("--mark-radius")) || 0;
  const markCorners = (index, row) => {
    if (!stacked || visibleFields.length === 1) {
      return [markRadius, markRadius, markRadius, markRadius];
    }
    const present = row
      ? visibleFields
          .map((field, position) => ({ position, value: Number(row[field]) }))
          .filter(({ value }) => Number.isFinite(value) && value !== 0)
      : visibleFields.map((_, position) => ({ position }));
    const first = present[0]?.position;
    const last = present.at(-1)?.position;
    if (index !== first && index !== last) return 0;
    if (horizontal) {
      if (first === last) return [markRadius, markRadius, markRadius, markRadius];
      return index === first ? [markRadius, 0, 0, markRadius] : [0, markRadius, markRadius, 0];
    }
    return index === last ? [markRadius, markRadius, 0, 0] : 0;
  };
  const selectMark =
    (field, sourceRows = data) =>
    (entry, index, event) => {
      const row = entry?.payload ?? sourceRows[index] ?? {};
      selectChartMark(
        {
          kind: "chart",
          chartType: type,
          label: String(row[x] ?? entry?.name ?? label(field)),
          series: label(field),
          value: type === "waterfall" && row.isTotal ? row.runningTotal : row[field] ?? entry?.value,
          row,
        },
        event?.nativeEvent ?? event,
      );
    };
  const suppressSelectionAfterZoom = useRef(false);
  const selectLinePoint = (chartState, event) => {
    if (suppressSelectionAfterZoom.current) {
      suppressSelectionAfterZoom.current = false;
      return;
    }
    const activeEntry =
      chartState?.activePayload?.find((entry) => visibleFields.includes(entry.dataKey)) ??
      chartState?.activePayload?.[0];
    const activeIndex = Number(chartState?.activeTooltipIndex);
    const row = activeEntry?.payload ?? (Number.isInteger(activeIndex) ? data[activeIndex] : null);
    const field = activeEntry?.dataKey ?? visibleFields[0];
    if (!row || !field) return;
    selectChartMark(
      {
        kind: "chart",
        chartType: type,
        label: String(row[x] ?? chartState?.activeLabel ?? label(field)),
        series: label(field),
        value: row[field] ?? activeEntry?.value,
        row,
      },
      event?.nativeEvent ?? event,
    );
  };
  const ValueYAxis = horizontal ? YAxis : NumericYAxis;
  const yAxis = (
    <ValueYAxis
      type={horizontal ? "category" : "number"}
      dataKey={horizontal ? x : undefined}
      {...(horizontal ? {} : { percent: proportional || (separateAxes ? axisRatio(leftFields) : ratio) })}
      domain={!horizontal ? benchmarkDomain ?? (spec.startAtZero === false ? ["dataMin", "auto"] : undefined) : undefined}
      tickCount={!horizontal && benchmarkDomain ? 6 : undefined}
      tickFormatter={horizontal ? tick : undefined}
      axisLine={false}
      tickLine={false}
      tickMargin={9}
      tickSize={horizontal ? 0 : undefined}
      tick={horizontal ? <CategoryAxisTick field={x} horizontal width={categoryWidth} /> : { fontSize: 12 }}
      interval={horizontal ? 0 : undefined}
      width={horizontal ? categoryWidth : "auto"}
      label={
        horizontal || !yTitle || !showYAxisTitle
          ? undefined
          : {
              value: yTitle,
              angle: -90,
              position: "insideLeft",
              offset: 0,
              style: valueAxisLabelStyle,
            }
      }
    />
  );
  const SecondaryAxis = horizontal ? NumericXAxis : NumericYAxis;
  const secondaryAxis = separateAxes && <SecondaryAxis
    type="number"
    {...(horizontal ? { xAxisId: "secondary", orientation: "top", height: 44 }
      : { yAxisId: "secondary", orientation: "right", width: "auto" })}
    percent={axisRatio(rightFields)}
    domain={secondaryBenchmarkDomain ?? (spec.startAtZero === false ? ["dataMin", "auto"] : undefined)}
    tickCount={secondaryBenchmarkDomain ? 6 : undefined}
    axisLine={false} tickLine={false} tickMargin={9}
    tick={{ fontSize: 12, fill: rightFields.length === 1 ? fieldColor(rightFields[0]) : "var(--secondary)" }}
    label={!(horizontal ? showXAxisTitle : showYAxisTitle) || !rightTitle ? undefined
      : { value: rightTitle, angle: horizontal ? 0 : 90, position: horizontal ? "insideTop" : "insideRight",
        style: valueAxisLabelStyle }}
  />;
  const everyCategory =
    !horizontal && ["bar", "stackedBar", "stackedBar100"].includes(type) && !rows.some((row) => dateValue(row[x]));
  const temporalValues =
    !horizontal && data.length > 0 && data.every((row) => dateValue(row[x]))
      ? [...new Set(data.map((row) => row[x]))]
      : [];
  const categoryValues = everyCategory ? [...new Set(data.map((row) => row[x]))] : [];
  const categoryCount = categoryValues.length;
  const angledCategories = everyCategory && categoryAxisLayout(categoryValues.map((value) => categoryLabel(x, value)),
    { preference: spec.xTickLabelLayout }) === "angled";
  const quantitativeXAxis = !horizontal && numericTrend && new Set(data.map((row) => row[x])).size > 1;
  const numericXValues = quantitativeXAxis ? data.map((row) => row[x]) : [];
  const numericXDomain = quantitativeXAxis ? [Math.min(...numericXValues), Math.max(...numericXValues)] : null;
  const integerXAxis = numericXValues.every(Number.isInteger);
  const numericXTicks = quantitativeXAxis
    ? [...new Set([numericXDomain[0], ...getNiceTickValues(numericXDomain, 5, !integerXAxis)
      .filter((value) => value > numericXDomain[0] && value < numericXDomain[1]), numericXDomain[1]])]
    : [];
  const ValueXAxis = horizontal || quantitativeXAxis ? NumericXAxis : temporalValues.length ? TemporalXAxis : XAxis;
  const xAxis = (
    <ValueXAxis
      type={horizontal || quantitativeXAxis ? "number" : "category"}
      dataKey={horizontal ? undefined : x}
      {...(temporalValues.length ? { values: temporalValues } : {})}
      {...(horizontal ? { percent: proportional || (separateAxes ? axisRatio(leftFields) : ratio) } : quantitativeXAxis ? { allowDecimals: !integerXAxis } : {})}
      domain={quantitativeXAxis ? numericXDomain : horizontal ? benchmarkDomain ?? (spec.startAtZero === false ? ["dataMin", "auto"] : undefined) : undefined}
      tickCount={horizontal && benchmarkDomain ? 6 : undefined}
      axisLine={false}
      tickLine={false}
      tickMargin={8}
      minTickGap={18}
      ticks={quantitativeXAxis ? numericXTicks : undefined}
      interval={everyCategory ? 0 : quantitativeXAxis ? "preserveStartEnd" : undefined}
      height={angledCategories ? 76 : everyCategory ? 44 : 30}
      tick={
        everyCategory ? (
          <CategoryAxisTick field={x} count={categoryCount} angled={angledCategories} />
        ) : (
          { fontSize: 12 }
        )
      }
      tickFormatter={horizontal || quantitativeXAxis ? undefined : tick}
    />
  );
  const categoryTooltipColors =
    !stacked &&
    ["bar", "horizontalBar", "leaderboard", "pie", "funnel"].includes(type) &&
    visibleFields.length === 1 &&
    !visibleFields.some((field) => spec.colors?.[field]) &&
    (semanticCategoryDimension(x) || rows.some((row) => typeof spec.colors?.[row[x]] === "string"));
  const tooltip = (
    <Tooltip
      content={
        <ChartTooltip
          stacked={stacked}
          vertical={["line", "area", "sparkline"].includes(type)}
          mode={["heatmap", "scatter", "boxPlot"].includes(type) ? type : "default"}
          xField={x}
          yField={y}
          groupField={heatmapGroup}
          xLabel={xTitle}
          yLabel={yTitle}
          formatValue={formatTooltipValue}
          resolveColor={
            categoryTooltipColors
              ? (item) =>
                  type === "pie"
                    ? pieColor(item.payload)
                    : colorFor({
                        field: y,
                        dimension: x,
                        value: item.payload?.[x],
                        index: rows.findIndex((row) => row[x] === item.payload?.[x]),
                        explicitColor: spec.colors?.[item.payload?.[x]],
                      })
              : undefined
          }
        />
      }
      itemSorter={() => 0}
      labelFormatter={(value) => categoryLabel(x, tooltipLabel(value))}
      isAnimationActive={false}
      cursor={["heatmap", "scatter"].includes(type) ? false : { fill: "var(--text)", fillOpacity: 0.06 }}
    />
  );
  const legend =
    type === "pie"
      ? pieRows.map((row) => ({
          label: categoryLabel(x, row[x]),
          value: String(row[x]),
          color: pieColor(row),
          visible: isVisible(row[x]),
        }))
      : type !== "heatmap" && fields.length > 1
        ? fields.map((field, index) => ({
            label: series ? categoryLabel(series, field) : label(String(field)),
            value: String(field),
            color:
              ["line", "sparkline"].includes(type) && referenceField(field) && !spec.colors?.[field]
                ? "var(--secondary)"
                : fieldColor(field),
            opacity:
              ["line", "sparkline"].includes(type) && referenceField(field) && !spec.colors?.[field] ? 0.7 : undefined,
            visible: isVisible(field),
            type:
              ["line", "sparkline"].includes(type) && !barFields.includes(field)
                ? referenceField(field)
                  ? "line line-dashed"
                  : "line"
                : "square",
          }))
        : [];
  const sourceCategoryIdentities = (values) => [...new Set([...values].flatMap((value) =>
    String(value) === "Other" && groupedCategories.length ? groupedCategories : [String(value)]))];
  const persistVisibleSeries = (all, next) => {
    const visibleCategories = sourceCategoryIdentities(next);
    if (onVisibleSeriesChange) onVisibleSeriesChange(visibleCategories);
    else {
      const selected = new Set(visibleCategories);
      setHiddenSeries(new Set(sourceCategoryIdentities(all).filter((category) => !selected.has(category))));
    }
  };
  const toggleSeries = (value) => {
    const all = type === "pie" ? pieRows.map((row) => String(row[x])) : fields.map(String);
    const next = new Set(all.filter(isVisible));
    if (next.has(String(value))) next.delete(String(value));
    else next.add(String(value));
    if (!next.size) return;
    persistVisibleSeries(all, next);
  };
  const isolateSeries = (value) => {
    const all = type === "pie" ? pieRows.map((row) => String(row[x])) : fields.map(String);
    const active = all.filter(isVisible);
    const next = active.length === 1 && active[0] === String(value) ? new Set(all) : new Set([String(value)]);
    persistVisibleSeries(all, next);
  };
  const setZoom = (next) => {
    if (onZoomChange) onZoomChange(next);
    else setLocalZoom(next);
  };
  const interactionProps = zoomable
    ? {
        onMouseDown: (state) => {
          suppressSelectionAfterZoom.current = false;
          if (state?.activeLabel != null)
            setSelection({ start: String(state.activeLabel), end: String(state.activeLabel) });
        },
        onMouseMove: (state) => {
          if (selection && state?.activeLabel != null) {
            setSelection((current) => (current ? { ...current, end: String(state.activeLabel) } : null));
          }
        },
        onMouseUp: (state) => {
          if (!selection) return;
          const end = String(state?.activeLabel ?? selection.end);
          const range = normalizeZoomRange(rows, x, selection.start, end);
          if (range) {
            suppressSelectionAfterZoom.current = true;
            setZoom(range);
          }
          setSelection(null);
        },
        onMouseLeave: () => setSelection(null),
      }
    : {};
  const selectedArea =
    selection && selection.start !== selection.end ? (
      <ReferenceArea
        x1={selection.start}
        x2={selection.end}
        fill="var(--accent)"
        fillOpacity={0.12}
        stroke="var(--accent)"
        strokeOpacity={0.35}
      />
    ) : null;

  if (type === "rankedList") {
    const ordered = spec.sortOrder ? sorted(data) : [...data].sort((left, right) => Number(right[y]) - Number(left[y]));
    const maximum = Math.max(0, ...ordered.map((row) => Number(row[y]) || 0));
    const initialVisibleCount = Math.min(ordered.length, Math.max(minimumRankingRows, fittedRankingRows));
    const visibleRankings = rankingExpanded ? ordered : ordered.slice(0, initialVisibleCount);
    return (
      <>
        <div
          ref={rankingListRef}
          className="chart-ranked-list"
          role="list"
          aria-label={`${label(y)} by ${label(x)}`}
          data-chart-id={chartId}
          data-ranked-list-variant={spec.variant ?? "inset"}
        >
          {visibleRankings.map((row, index) => {
            const value = Number(row[y]);
            const reviewedLabel = categoryLabel(x, row[x]);
            const width =
              Number.isFinite(value) && maximum > 0 ? `${Math.max(0, Math.min(100, (value / maximum) * 100))}%` : "0%";
            const categoryColor = spec.colors?.[row[x]];
            const rankedFill = categoryColor ? {
              width,
              "--ranked-list-fill": `color-mix(in srgb, ${categoryColor} 18%, var(--surface))`,
              "--ranked-list-fill-hover": `color-mix(in srgb, ${categoryColor} 26%, var(--surface))`,
            } : { width };
            return (
              <button
                key={`${String(row[x])}-${index}`}
                type="button"
                role="listitem"
                className="chart-ranked-list-row"
                aria-label={`${reviewedLabel}: ${formatMarkValue(value, y)}`}
                onClick={(event) =>
                  selectChartMark(
                    { kind: "chart", chartType: type, label: reviewedLabel, series: label(y), value, row },
                    event.nativeEvent,
                  )
                }
              >
                <span className="chart-ranked-list-fill" style={rankedFill} aria-hidden="true" />
                <span className="chart-ranked-list-label">{reviewedLabel}</span>
                <span className="chart-ranked-list-value">{formatMarkValue(value, y)}</span>
              </button>
            );
          })}
        </div>
        {ordered.length > initialVisibleCount && (
          <button
            type="button"
            className="chart-ranked-list-toggle"
            aria-expanded={rankingExpanded}
            onClick={() => {
              if (rankingExpanded) setFittedRankingRows(minimumRankingRows);
              setRankingExpanded((expanded) => !expanded);
            }}
          >
            {rankingExpanded ? "Show fewer" : `Show ${ordered.length - initialVisibleCount} more`}
          </button>
        )}
      </>
    );
  }

  if (type === "funnel") return <div role="group" aria-label={accessibleLabel || `${label(y)} by ${label(x)}`}>
    <FunnelRenderer rows={rows} x={x} y={y} height={height} chartId={chartId}
    formatValue={formatMarkValue} colorFor={(stage) => spec.colors?.[stage.__funnelStage]
      ?? colorFor({ field: y, index: 0, explicitColor: spec.colors?.[y] })}
    formatExactValue={valueIsRatio(y) ? (value) => new Intl.NumberFormat(undefined,
      { style: "percent", maximumSignificantDigits: 15 }).format(value) : undefined}
    formatDropoff={valueIsRatio(y) ? (value) => `${new Intl.NumberFormat(undefined, { maximumSignificantDigits: 15 }).format(value * 100)} pp` : undefined}
    onChartClick={(event) => selectChartSection({ kind: "chart", chartType: type, label: "Chart" }, event.nativeEvent)}
    onSelect={selectionEnabled ? (row, index, event) => selectMark(y, rows)({ payload: row }, index, event) : undefined} />
  </div>;

  let chart;
  let scaleLegend;

  if (type === "pie") {
    const slices = sorted(visibleRows);
    const total = slices.reduce((sum, row) => sum + Math.max(0, Number(row[y]) || 0), 0);
    const positiveSlices = slices.filter((row) => row[y] > 0);
    const minimumSliceAngle = total ? Math.min(...positiveSlices.map((row) => row[y] / total * 360)) : 0;
    const sliceGap = positiveSlices.length > 1 ? Math.min(1.5, minimumSliceAngle / 4) : 0;
    const centerValue =
      spec.centerValue ??
      (spec.centerLabel && slices[0] && total ? percentage(Number(slices[0][y]) / total).replace("+", "") : undefined);
    chart = (
      <PieChart accessibilityLayer>
        {tooltip}
        <Pie
          data={slices}
          dataKey={y}
          nameKey={x}
          cx="50%"
          cy="50%"
          innerRadius="54%"
          outerRadius="82%"
          paddingAngle={sliceGap}
          stroke="none"
          isAnimationActive={false}
          onClick={selectMark(y, visibleRows)}
        >
          {slices.map((row) => (
            <Cell
              key={String(row[x])}
              fill={pieColor(row)}
            />
          ))}
          {spec.showValues && (
            <LabelList dataKey={y} position="outside" formatter={compact} fill="var(--secondary)" fontSize={12} />
          )}
          {centerValue != null && (
            <Label
              position="center"
              content={({ viewBox }) => {
                const centerX = Number(viewBox?.cx ?? Number(viewBox?.x) + Number(viewBox?.width) / 2);
                const centerY = Number(viewBox?.cy ?? Number(viewBox?.y) + Number(viewBox?.height) / 2);
                if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return null;
                return (
                  <g className="chart-donut-center">
                    <text
                      x={centerX}
                      y={centerY - (spec.centerLabel ? 5 : 0)}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="var(--text)"
                      fontSize={26}
                      fontWeight={500}
                    >
                      {centerValue}
                    </text>
                    {spec.centerLabel && (
                      <text
                        x={centerX}
                        y={centerY + 19}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill="var(--secondary)"
                        fontSize={12}
                      >
                        {spec.centerLabel}
                      </text>
                    )}
                  </g>
                );
              }}
            />
          )}
        </Pie>
      </PieChart>
    );
  } else if (type === "sankey") {
    const graph = sankeyGraph(rows, sankeyStages, y);
    chart =
      graph.nodes.length && graph.links.length ? (
        <Sankey
          data={graph}
          node={<SankeyNode colorFor={colorFor} />}
          nodeWidth={12}
          nodePadding={16}
          iterations={48}
          link={{ stroke: "var(--chart-1)", strokeOpacity: 0.22 }}
          margin={{ top: 12, right: 110, bottom: 12, left: 20 }}
        />
      ) : (
        <svg role="img" aria-label="No complete reviewed flow paths" />
      );
  } else if (type === "histogram") {
    const buckets = histogram(rows, y).map((bucket) => ({
      ...bucket,
      range: `${compact(bucket.start)}–${compact(bucket.end)}`,
    }));
    chart = (
      <BarChart data={buckets} margin={chartMargin} barCategoryGap="2%" accessibilityLayer>
        {grid}
        <XAxis dataKey="range" axisLine={false} tickLine={false} tickMargin={10} interval={0} height={30}
          tick={<CategoryAxisTick field="range" count={buckets.length} />} />
        <NumericYAxis allowDecimals={false} axisLine={false} tickLine={false} tickMargin={9} width="auto" />
        {tooltip}
        <Bar
          dataKey="count"
          fill={fieldColor(y)}
          radius={[Math.min(markRadius, 3), Math.min(markRadius, 3), 0, 0]}
          isAnimationActive={false}
          onClick={selectMark("count", buckets)}
        >
          {spec.showValues && (
            <LabelList dataKey="count" position="top" formatter={compact} fill="var(--secondary)" fontSize={12} />
          )}
        </Bar>
      </BarChart>
    );
  } else if (type === "heatmap") {
    const heat = heatmap(rows, x, heatmapGroup, y, {
      domain: spec.colorDomain,
      startAtZero: spec.colorScaleStartAtZero,
    });
    scaleLegend = {
      label: yTitle,
      color: spec.baseColor,
      minimum: formatMarkValue(heat.minimum),
      maximum: formatMarkValue(heat.maximum),
    };
    const angledHeatmap = categoryAxisLayout(heat.xValues.map((value) => categoryLabel(x, value)),
      { preference: spec.xTickLabelLayout }) === "angled";
    chart = (
      <ScatterChart margin={{ ...chartMargin, bottom: showXAxisTitle ? 4 : 0 }} accessibilityLayer>
        <XAxis
          type="number"
          dataKey="xIndex"
          domain={[-0.5, Math.max(0.5, heat.xValues.length - 0.5)]}
          ticks={heat.xValues.map((_, index) => index)}
          interval={0}
          tick={<HeatmapTick values={heat.xValues} angled={angledHeatmap} />}
          axisLine={false}
          tickLine={false}
          height={angledHeatmap ? 76 : 30}
        />
        <YAxis
          type="number"
          dataKey="yIndex"
          domain={[-0.5, Math.max(0.5, heat.yValues.length - 0.5)]}
          ticks={heat.yValues.map((_, index) => index)}
          tickFormatter={(index) => heat.yValues[index]}
          axisLine={false}
          tickLine={false}
          tickMargin={5}
          tick={{ fontSize: 12 }}
          width={heatmapCategoryWidth}
          label={
            showYAxisTitle && heatmapYAxisTitle
              ? {
                  value: heatmapYAxisTitle,
                  angle: -90,
                  position: "insideLeft",
                  offset: 0,
                  style: { fill: "var(--secondary)", fontSize: 12, textAnchor: "middle" },
                }
              : undefined
          }
        />
        <ZAxis dataKey={y} />
        {tooltip}
        <Scatter
          data={heat.rows}
          shape={
            <HeatCell
              columnCount={heat.xValues.length}
              rowCount={heat.yValues.length}
              radius={markRadius}
              baseColor={spec.baseColor}
            />
          }
          isAnimationActive={false}
          onClick={selectMark(y, heat.rows)}
        />
      </ScatterChart>
    );
  } else if (type === "scatter") {
    const ScatterXAxis = numeric.includes(x) ? NumericXAxis : XAxis;
    chart = (
      <ScatterChart margin={chartMargin} accessibilityLayer>
        <CartesianGrid stroke="var(--border)" strokeWidth={0.5} />
        <ScatterXAxis
          type={numeric.includes(x) ? "number" : "category"}
          dataKey={x}
          {...(numeric.includes(x)
            ? {
                percent: ratioMetric(
                  x,
                  rows.map((row) => row[x]),
                ),
              }
            : { tickFormatter: tick })}
          axisLine={false}
          tickLine={false}
          tickMargin={10}
          height={30}
        />
        <NumericYAxis
          type="number"
          dataKey={y}
          domain={spec.startAtZero === false ? ["dataMin", "auto"] : undefined}
          percent={ratio}
          width="auto"
          axisLine={false}
          tickLine={false}
          tickMargin={9}
          label={
            !showYAxisTitle || !yTitle
              ? undefined
              : {
                  value: yTitle,
                  angle: -90,
                  position: "insideLeft",
                  offset: 0,
                  style: { fill: "var(--secondary)", fontSize: 12, textAnchor: "middle" },
                }
          }
        />
        {tooltip}
        {seriesValues.length ? (
          seriesValues.filter(isVisible).map((value) => (
            <Scatter
              key={value}
              name={String(value)}
              data={rows.filter((row) => row[series] === value)}
              fill={fieldColor(value)}
              isAnimationActive={false}
              onClick={selectMark(
                y,
                rows.filter((row) => row[series] === value),
              )}
            >
              {spec.showValues && (
                <LabelList dataKey={y} position="top" formatter={compact} fill="var(--secondary)" fontSize={12} />
              )}
            </Scatter>
          ))
        ) : (
          <Scatter data={rows} fill={fieldColor(y)} isAnimationActive={false} onClick={selectMark(y, rows)}>
            {spec.showValues && (
              <LabelList dataKey={y} position="top" formatter={compact} fill="var(--secondary)" fontSize={12} />
            )}
          </Scatter>
        )}
      </ScatterChart>
    );
  } else if (type === "waterfall") {
    const bridge = waterfall(rows, y, {
      categoryField: x,
      beginning: Number.isFinite(spec.beginning) ? spec.beginning : undefined,
      ending: Number.isFinite(spec.ending) ? spec.ending : undefined,
      includeEnding: true,
    }).map((row) => ({
      ...row,
      __waterfallLabel: row.isTotal
        ? compact(row.balance)
        : row.change > 0
          ? `+${compact(row.change)}`
          : compact(row.change).replace(/^-/, "−"),
    }));
    const focusedDomain = spec.startAtZero === false ? waterfallValueDomain(bridge) : undefined;
    const waterfallAxis = focusedDomain
      ? React.cloneElement(yAxis, { domain: focusedDomain, allowDataOverflow: true })
      : yAxis;
    chart = (
      <BarChart data={bridge} margin={{ ...chartMargin, top: 26 }} accessibilityLayer>
        {grid}
        {xAxis}
        {waterfallAxis}
        {tooltip}
        <Bar
          dataKey="range"
          radius={[markRadius, markRadius, markRadius, markRadius]}
          shape={<WaterfallShape />}
          isAnimationActive={false}
          onClick={selectMark(y, bridge)}
        >
          {bridge.map((row, index) => (
            <Cell
              key={index}
              fill={
                row.isTotal
                  ? "var(--chart-neutral-fill, color-mix(in srgb, var(--text) 3%, var(--surface)))"
                  : row.change < 0
                    ? "var(--negative)"
                    : "var(--positive)"
              }
            />
          ))}
          <LabelList dataKey="__waterfallLabel" position="top" fill="var(--secondary)" fontSize={12} offset={5} />
        </Bar>
      </BarChart>
    );
  } else if (type === "boxPlot") {
    const boxes = boxPlots(rows, x, y);
    const whiskerMaximum = Math.max(1, ...boxes.map((box) => Number(box.maximum) || 0));
    chart = (
      <ComposedChart data={boxes} margin={{ ...chartMargin, top: 20 }} barCategoryGap="38%" accessibilityLayer>
        {grid}
        <XAxis
          dataKey={x}
          axisLine={false}
          tickLine={false}
          tickMargin={10}
          interval={0}
          height={44}
          tick={<CategoryAxisTick field={x} count={boxes.length} />}
        />
        <NumericYAxis
          domain={[0, whiskerMaximum * 1.12]}
          width="auto"
          tickCount={5}
          axisLine={false}
          tickLine={false}
          tickMargin={9}
          label={
            !showYAxisTitle || !yTitle
              ? undefined
              : {
                  value: yTitle,
                  angle: -90,
                  position: "insideLeft",
                  offset: 0,
                  style: { fill: "var(--secondary)", fontSize: 12, textAnchor: "middle" },
                }
          }
        />
        {tooltip}
        <Bar dataKey="lowerQuartile" stackId="box" maxBarSize={66} fill="transparent" isAnimationActive={false} />
        <Bar
          dataKey="spread"
          stackId="box"
          maxBarSize={66}
          shape={<BoxShape />}
          isAnimationActive={false}
          onClick={selectMark("median", boxes)}
        >
          {boxes.map((box, index) => (
            <Cell
              key={String(box[x])}
              fill={colorFor({ field: y, dimension: x, value: box[x], index, explicitColor: spec.colors?.[box[x]] })}
            />
          ))}
        </Bar>
      </ComposedChart>
    );
  } else if (["line", "sparkline"].includes(type)) {
    const TrendChart = barFields.length && type === "line" ? ComposedChart : LineChart;
    const lineFields = visibleFields.filter((field) => !barFields.includes(field));
    const isolatedPoints = new Map(
      type === "line" ? lineFields.map((field) => [field, new Set(isolatedPointIndexes(data, field))]) : [],
    );
    chart = (
      <TrendChart
        data={data}
        margin={type === "sparkline" ? { top: 8, right: 8, bottom: 8, left: 8 } : chartMargin}
        {...(type === "sparkline" ? {} : interactionProps)}
        accessibilityLayer
        onClick={selectLinePoint}
      >
        {type !== "sparkline" && grid}
        {type !== "sparkline" && xAxis}
        {type !== "sparkline" && yAxis}
        {secondaryAxis}
        {barFields.length > 0 && (
          <NumericYAxis
            yAxisId="weekly-change"
            orientation="right"
            width="auto"
            axisLine={false}
            tickLine={false}
            tickMargin={7}
            tick={{ fontSize: 12 }}
          />
        )}
        {selectedArea}
        {annotationRanges}
        {tooltip}
        {barFields.filter(isVisible).map((field) => (
          <Bar
            key={`bar-${field}`}
            yAxisId="weekly-change"
            dataKey={field}
            stackId="weekly-growth"
            fill={fieldColor(field)}
            fillOpacity={0.68}
            maxBarSize={28}
            isAnimationActive={false}
          />
        ))}
        {lineFields.map((field) => (
          <Line
            key={field}
            yAxisId={fieldAxis(field)}
            type="monotone"
            dataKey={field}
            stroke={referenceField(field) && !spec.colors?.[field] ? "var(--secondary)" : fieldColor(field)}
            strokeWidth={referenceField(field) && !spec.colors?.[field] ? 1.5 : 2.25}
            strokeOpacity={referenceField(field) && !spec.colors?.[field] ? 0.7 : undefined}
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeDasharray={referenceField(field) ? "5 4" : undefined}
            connectNulls={false}
            dot={isolatedPoints.get(field)?.size ? <IsolatedLineDot indexes={isolatedPoints.get(field)} /> : false}
            activeDot={{ r: 5, strokeWidth: 3, stroke: "var(--surface)" }}
            isAnimationActive={false}
          >
            {spec.showValues && field === primaryValueField && (
              <LabelList dataKey={field} content={<SparseValueLabel count={data.length} />} />
            )}
          </Line>
        ))}
        {annotationMarks}
        {annotationLabels}
      </TrendChart>
    );
  } else if (["area", "stackedArea"].includes(type)) {
    chart = (
      <AreaChart data={data} margin={chartMargin} {...interactionProps} accessibilityLayer>
        {grid}
        {xAxis}
        {yAxis}
        {secondaryAxis}
        {selectedArea}
        {annotationRanges}
        {tooltip}
        {visibleFields.map((field) => (
          <Area
            key={field}
            yAxisId={fieldAxis(field)}
            dataKey={field}
            type="monotone"
            stackId={stacked ? "stack" : undefined}
            stroke={fieldColor(field)}
            fill={fieldColor(field)}
            fillOpacity={stacked ? 0.5 : 0.18}
            strokeWidth={2}
            strokeLinecap="round"
            strokeLinejoin="round"
            isAnimationActive={false}
            onClick={selectMark(field, data)}
          >
            {spec.showValues && field === primaryValueField && (
              <LabelList dataKey={field} content={<SparseValueLabel count={data.length} />} />
            )}
          </Area>
        ))}
        {annotationMarks}
        {annotationLabels}
      </AreaChart>
    );
  } else {
    const ordered = spec.sortOrder
      ? sorted(data)
      : type === "leaderboard"
        ? [...data].sort((left, right) => right[y] - left[y])
        : data;
    const categoryColors =
      !stacked &&
      visibleFields.length === 1 &&
      !visibleFields.some((field) => spec.colors?.[field]) &&
      (semanticCategoryDimension(x) || ordered.some((row) => typeof spec.colors?.[row[x]] === "string"));
    const signedComparison =
      !stacked &&
      visibleFields.length === 1 &&
      (spec.colorBySign === true ||
        (ordered.some((row) => Number(row[y]) < 0) && ordered.some((row) => Number(row[y]) > 0)));
    const barChartMargin =
      horizontal && (type === "leaderboard" || spec.showValues) ? { ...chartMargin, right: 58 } : chartMargin;
    chart = (
      <BarChart
        key={stacked ? visibleFields.join("|") : undefined}
        data={ordered}
        margin={barChartMargin}
        layout={horizontal ? "vertical" : "horizontal"}
        accessibilityLayer
        barCategoryGap={orderedDistribution(spec, rows) ? "3%" : "24%"}
        stackOffset={proportional ? "expand" : stacked ? "sign" : "none"}
      >
        <CartesianGrid stroke="var(--border)" strokeWidth={0.5} vertical={horizontal} horizontal={!horizontal} />
        {xAxis}
        {yAxis}
        {secondaryAxis}
        {annotationRanges}
        {tooltip}
        {visibleFields.map((field, index) => (
          <Bar
            key={field}
            {...(horizontal ? { xAxisId: fieldAxis(field) } : { yAxisId: fieldAxis(field) })}
            dataKey={field}
            stackId={stacked ? "stack" : undefined}
            fill={fieldColor(field)}
            radius={markCorners(index)}
            shape={
              stacked ? (
                <StackedMarkShape fields={visibleFields} field={field} horizontal={horizontal} radius={markRadius} />
              ) : undefined
            }
            isAnimationActive={false}
            onClick={selectMark(field, ordered)}
          >
            {(categoryColors || stacked || signedComparison) &&
              ordered.map((row, rowIndex) => (
                <Cell
                  key={`${String(row[x])}-${rowIndex}`}
                  {...(signedComparison
                    ? { fill: Number(row[field]) < 0 ? "var(--negative)" : "var(--positive)" }
                    : categoryColors
                      ? {
                          fill: colorFor({
                            field: y,
                            dimension: x,
                            value: row[x],
                            index: rowIndex,
                            explicitColor: spec.colors?.[row[x]],
                          }),
                        }
                      : {})}
                  {...(stacked ? { radius: markCorners(index, row) } : {})}
                />
              ))}
            {(type === "leaderboard" || spec.showValues) && (
              <LabelList
                dataKey={field}
                position={horizontal ? "right" : "top"}
                formatter={(value) => formatMarkValue(value, field)}
              />
            )}
          </Bar>
        ))}
        {annotationMarks}
        {annotationLabels}
      </BarChart>
    );
  }

  const chartHeight = horizontal
    ? Math.max(height, data.length * (Number(spec.rowHeight) || 42))
    : type === "heatmap"
      ? Math.max(
          height,
          Math.min(
            460,
            new Set(rows.map((row) => row[heatmapGroup])).size * (Number(spec.rowHeight) || 43) +
              (showXAxisTitle ? 60 : 32),
          ),
        )
      : height;
  const plotInset = ["pie", "funnel", "sparkline", "sankey"].includes(type)
    ? { left: 0, right: 0 }
    : {
        left: showYAxisTitle
          ? chartMargin.left +
            (type === "heatmap" ? heatmapCategoryWidth : type === "histogram" ? numericAxisWidth : categoryWidth)
          : 0,
        right: showYAxisTitle ? chartMargin.right : 0,
      };
  return (
    <ChartFrame
      chart={chart}
      accessibleLabel={accessibleLabel
        ?? (x && y ? `${label(y)} by ${label(x)}` : `${label(type)} chart`)}
      height={chartHeight}
      xLabel={
        ["sparkline", "pie", "funnel", "sankey"].includes(type) || !showXAxisTitle
          ? ""
          : type === "histogram"
            ? label(y)
            : xTitle
      }
      plotInset={plotInset}
      legend={spec.showLegend === false ? [] : legend}
      scaleLegend={scaleLegend}
      onChartClick={(event) =>
        selectChartSection(
          { kind: "chart", chartType: type, label: "Chart" },
          event.nativeEvent,
        )
      }
      onLegendToggle={toggleSeries}
      onLegendIsolate={isolateSeries}
      zoomed={zoomable && Boolean(activeZoom)}
      onResetZoom={zoomable ? () => setZoom(null) : undefined}
      legendPosition={spec.legend?.position}
      annotationNotes={<ChartAnnotationNotes annotations={annotations} placedIds={placedAnnotationIds} />}
      data-chart-id={chartId}
    />
  );
}
