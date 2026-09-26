import React, { useId } from "react";

export function MetricSparkline({ values = [], negative = false, className = "" }) {
  const id = useId().replaceAll(":", "");
  const reviewed = values.filter(Number.isFinite).slice(-12);
  if (reviewed.length < 2) return null;
  const minimum = Math.min(...reviewed);
  const range = Math.max(Number.EPSILON, Math.max(...reviewed) - minimum);
  const points = reviewed.map((value, index) =>
    `${2 + index / (reviewed.length - 1) * 54},${21 - (value - minimum) / range * 16}`);
  const gradient = `metric-fill-${id}`;
  const stroke = `metric-stroke-${id}`;
  return <svg className={["metric-mini-trend", "data-metric-sparkline", className].filter(Boolean).join(" ")}
    viewBox="0 0 58 25" aria-hidden="true" data-direction={negative ? "negative" : "positive"}>
    <defs>
      <linearGradient id={gradient} x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor="currentColor" stopOpacity=".32" />
        <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
      </linearGradient>
      <linearGradient id={stroke} x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%" stopColor="currentColor" stopOpacity=".58" />
        <stop offset="100%" stopColor="currentColor" stopOpacity=".94" />
      </linearGradient>
    </defs>
    <polygon points={`2,23 ${points.join(" ")} 56,23`} fill={`url(#${gradient})`} />
    <polyline points={points.join(" ")} fill="none" stroke={`url(#${stroke})`} strokeWidth="1.7"
      strokeLinecap="round" strokeLinejoin="round" />
  </svg>;
}
