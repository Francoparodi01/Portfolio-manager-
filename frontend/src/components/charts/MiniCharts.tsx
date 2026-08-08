import type { RowRecord, Tone } from "../../types/api";
import { getNumber, getString } from "../../utils/data";
import { formatPercent } from "../../utils/format";

function scale(value: number, min: number, max: number, start: number, end: number): number {
  const span = Math.max(0.000001, max - min);
  return start + ((value - min) / span) * (end - start);
}

export function LineChart({
  rows,
  valueKey,
  labelKey,
  title,
  description,
}: {
  rows: RowRecord[];
  valueKey: string;
  labelKey: string;
  title: string;
  description: string;
}) {
  const points = rows
    .map((row, index) => ({ index, label: getString(row, labelKey), value: getNumber(row, valueKey) }))
    .filter((point): point is { index: number; label: string; value: number } => point.value !== null);
  if (points.length < 2) return <div className="chart-empty">Sin historia suficiente para graficar.</div>;

  const width = 720;
  const height = 240;
  const pad = { bottom: 34, left: 46, right: 22, top: 20 };
  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const path = points
    .map((point, index) => {
      const x = scale(index, 0, points.length - 1, pad.left, width - pad.right);
      const y = scale(point.value, min, max, height - pad.bottom, pad.top);
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const titleId = `${title.replace(/\s+/g, "-")}-title`;
  const descId = `${title.replace(/\s+/g, "-")}-desc`;

  return (
    <svg className="chart-surface" viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby={`${titleId} ${descId}`}>
      <title id={titleId}>{title}</title>
      <desc id={descId}>{description}</desc>
      <path className="chart-axis" d={`M${pad.left},${height - pad.bottom} L${width - pad.right},${height - pad.bottom}`} />
      <path className="chart-axis muted" d={`M${pad.left},${pad.top} L${pad.left},${height - pad.bottom}`} />
      <path className="chart-line" d={path} />
      {points.map((point, index) => {
        const x = scale(index, 0, points.length - 1, pad.left, width - pad.right);
        const y = scale(point.value, min, max, height - pad.bottom, pad.top);
        return <circle key={`${point.label}-${index}`} className="chart-dot" cx={x} cy={y} r="4" />;
      })}
      <text x={pad.left} y={height - 10}>{points[0]?.label}</text>
      <text x={width - pad.right - 92} y={height - 10}>{points[points.length - 1]?.label}</text>
    </svg>
  );
}

export function HorizontalBars({
  rows,
  title,
  description,
}: {
  rows: Array<{ label: string; value: number; display?: string; tone?: Tone }>;
  title: string;
  description: string;
}) {
  const visibleRows = rows.filter((row) => Number.isFinite(row.value));
  if (!visibleRows.length) return <div className="chart-empty">Sin valores comparables.</div>;
  const max = Math.max(1, ...visibleRows.map((row) => Math.abs(row.value)));

  return (
    <div className="bar-chart" role="img" aria-label={`${title}. ${description}`}>
      {visibleRows.map((row) => {
        const width = `${Math.max(4, (Math.abs(row.value) / max) * 100).toFixed(1)}%`;
        return (
          <div className={`bar-chart-row ${row.tone || "neutral"}`} key={row.label}>
            <span>{row.label}</span>
            <i style={{ inlineSize: width }} />
            <b>{row.display || row.value.toLocaleString("es-AR")}</b>
          </div>
        );
      })}
    </div>
  );
}

export function ScatterChart({
  rows,
  xKey,
  yKey,
  labelKey,
  title,
  description,
}: {
  rows: RowRecord[];
  xKey: string;
  yKey: string;
  labelKey: string;
  title: string;
  description: string;
}) {
  const points = rows
    .map((row) => ({ label: getString(row, labelKey), x: getNumber(row, xKey), y: getNumber(row, yKey) }))
    .filter((point): point is { label: string; x: number; y: number } => point.x !== null && point.y !== null);
  if (points.length < 4) return <div className="chart-empty">Muestra escasa: se muestra mejor como tabla.</div>;

  const width = 720;
  const height = 260;
  const pad = 38;
  const minX = Math.min(-0.05, ...points.map((point) => point.x));
  const maxX = Math.max(0.05, ...points.map((point) => point.x));
  const minY = Math.min(-0.05, ...points.map((point) => point.y));
  const maxY = Math.max(0.05, ...points.map((point) => point.y));
  const zeroX = scale(0, minX, maxX, pad, width - pad);
  const zeroY = scale(0, minY, maxY, height - pad, pad);
  const titleId = `${title.replace(/\s+/g, "-")}-title`;
  const descId = `${title.replace(/\s+/g, "-")}-desc`;

  return (
    <svg className="chart-surface" viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby={`${titleId} ${descId}`}>
      <title id={titleId}>{title}</title>
      <desc id={descId}>{description}</desc>
      <path className="chart-axis" d={`M${pad},${zeroY} L${width - pad},${zeroY}`} />
      <path className="chart-axis muted" d={`M${zeroX},${pad} L${zeroX},${height - pad}`} />
      {points.map((point) => {
        const x = scale(point.x, minX, maxX, pad, width - pad);
        const y = scale(point.y, minY, maxY, height - pad, pad);
        return (
          <g key={`${point.label}-${point.x}-${point.y}`}>
            <circle className={point.y >= 0 ? "scatter-dot positive" : "scatter-dot negative"} cx={x} cy={y} r="5" />
            <text x={x + 8} y={y - 8}>{point.label}</text>
          </g>
        );
      })}
      <text x={pad} y={height - 8}>score</text>
      <text x={width - 128} y={height - 8}>{formatPercent(maxY, 1, true)}</text>
    </svg>
  );
}
