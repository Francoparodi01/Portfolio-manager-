import React from "react";

import { categoryLabel, displayValue, label as humanize, tick } from "./chart-theme.js";
import { scatterTooltipIdentityField } from "./chart-data-shape.js";
import { orderTooltipEntries } from "./chart-transforms.js";

export function ChartTooltip({
  active,
  label,
  payload = [],
  stacked = false,
  vertical = false,
  resolveColor,
  mode = "default",
  xField,
  yField,
  groupField,
  xLabel,
  yLabel,
  formatValue,
  details,
  headerValue,
  children,
}) {
  if (active && details) return <div className={`chart-tooltip chart-tooltip--plain${headerValue != null ? " chart-tooltip--details" : ""}`}>
    {headerValue != null ? <div className="chart-tooltip-heading"><strong>{label}</strong><b>{headerValue}</b></div>
      : label != null && <strong>{label}</strong>}
    {details.map(({ label: name, value }) => <span key={name}>{name}<b>{value}</b></span>)}
    {children}
  </div>;
  if (!active || !payload.length) return null;
  const row = payload[0]?.payload;
  if (mode === "heatmap" && row) {
    return (
      <div className="chart-tooltip chart-tooltip--plain">
        <strong>{String(tick(row[xField] ?? ""))}</strong>
        <span>
          {humanize(String(groupField))}
          <b>{String(row[groupField] ?? "—")}</b>
        </span>
        <span>
          {yLabel ?? humanize(String(yField))}
          <b>{formatValue ? formatValue(row[yField], yField) : displayValue(row[yField])}</b>
        </span>
      </div>
    );
  }
  if (mode === "scatter" && row) {
    const identityField = scatterTooltipIdentityField(row);
    const identity = identityField ? row[identityField] : undefined;
    return (
      <div className="chart-tooltip chart-tooltip--plain">
        {identity && <strong>{String(identity)}</strong>}
        <span>
          {xLabel ?? humanize(String(xField))}
          <b>{formatValue ? formatValue(row[xField], xField) : displayValue(row[xField])}</b>
        </span>
        <span>
          {yLabel ?? humanize(String(yField))}
          <b>{formatValue ? formatValue(row[yField], yField) : displayValue(row[yField])}</b>
        </span>
      </div>
    );
  }
  if (mode === "boxPlot" && row) {
    const statistics = [
      ["Maximum", row.maximum],
      ["75th percentile", row.upperQuartile],
      ["Median", row.median],
      ["25th percentile", row.lowerQuartile],
      ["Minimum", row.minimum],
    ];
    return (
      <div className="chart-tooltip chart-tooltip--plain chart-tooltip--distribution">
        <strong>{String(tick(row[xField] ?? label ?? ""))}</strong>
        {statistics.map(([name, value]) => (
          <span key={name} data-box-statistic={name}>
            {name}
            <b>{formatValue ? formatValue(value, yField) : displayValue(value)}</b>
          </span>
        ))}
      </div>
    );
  }
  const seenFields = new Set();
  const items = payload
    .filter((item) => {
      if (item.value == null || item.dataKey === "baseline") return false;
      const field = String(item.dataKey ?? item.name);
      if (seenFields.has(field)) return false;
      seenFields.add(field);
      return true;
    })
    .map((item) => {
      if (item.dataKey !== "magnitude" && item.dataKey !== "range") {
        return resolveColor ? { ...item, color: resolveColor(item) ?? item.color } : item;
      }
      const isTotal = Boolean(item.payload?.isTotal);
      const change = Number(item.payload?.change ?? item.value);
      const color = change < 0 ? "var(--negative)" : "var(--positive)";
      return {
        ...item,
        name: "Net change",
        value: isTotal ? item.payload?.runningTotal ?? item.payload?.balance : change,
        color: isTotal ? "var(--chart-neutral-fill, color-mix(in srgb, var(--text) 3%, var(--surface)))" : color,
        ...(isTotal
          ? {
              name: item.payload.totalType === "beginning" ? "Beginning total" : "Ending total",
            }
          : {}),
      };
    });
  const ordered = orderTooltipEntries(items, { stacked, vertical });
  return (
    <div className="chart-tooltip">
      {label != null && <strong>{categoryLabel(xField, tick(label))}</strong>}
      {ordered.map((item) => (
        <span key={`${item.dataKey}-${item.name}`}>
          <i style={{ background: item.color ?? item.payload?.fill ?? item.fill ?? "var(--chart-1)" }} />
          {humanize(String(item.name ?? item.dataKey))}
          <b>{formatValue ? formatValue(item.value, item.dataKey) : displayValue(item.value)}</b>
        </span>
      ))}
    </div>
  );
}
