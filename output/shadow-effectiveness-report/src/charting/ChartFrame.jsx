import React, { useEffect, useRef, useState } from "react";
import { ResponsiveContainer } from "recharts";

export function ChartLegend({ items = [], onToggle, onIsolate, position = "bottom" }) {
  const legendRef = useRef(null);
  const [overflowing, setOverflowing] = useState(false);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    const legend = legendRef.current;
    if (!legend || position === "right") return undefined;
    const updateOverflow = () => setOverflowing(!expanded && legend.clientHeight < legend.scrollHeight - 1);
    updateOverflow();
    const observer = new ResizeObserver(updateOverflow);
    observer.observe(legend);
    return () => {
      observer.disconnect();
    };
  }, [expanded, items.length, position]);

  if (!items.length) return null;
  return <>
    <ul className={["chart-legend", position === "right" ? "chart-legend--right" : ""].filter(Boolean).join(" ")}
      ref={legendRef} data-expanded={expanded || undefined} data-overflowing={overflowing || undefined}
      aria-label="Chart legend" data-legend-position={position}
      tabIndex={items.length > 5 ? 0 : undefined}>
      {items.map((item, index) => <li key={`${item.label}-${index}`}>
        <button type="button" className="chart-legend-button"
          aria-label={`Toggle ${item.label}`} aria-pressed={item.visible !== false}
          title="Click to toggle. Double-click or press Shift+Enter to isolate."
          onClick={() => onToggle?.(item.value ?? item.label)}
          onDoubleClick={() => onIsolate?.(item.value ?? item.label)}
          onKeyDown={(event) => {
            if (event.shiftKey && ["Enter", " "].includes(event.key)) {
              event.preventDefault();
              onIsolate?.(item.value ?? item.label);
            }
          }} disabled={!onToggle}>
          <span className={`chart-legend-mark ${item.type ?? "square"}`}
            style={{ "--legend-color": item.color, opacity: item.opacity }} aria-hidden="true" />
          <span>{item.label}</span>
        </button>
      </li>)}
    </ul>
    {position !== "right" && (overflowing || expanded) && <button
      type="button"
      className="chart-legend-toggle"
      aria-expanded={expanded}
      onClick={() => setExpanded((current) => !current)}
    >
      {expanded ? "Show fewer categories" : `Show all ${items.length} categories`}
    </button>}
  </>;
}

export function ChartFrame({ chart, height = 240, xLabel, legend = [], onLegendToggle,
  onLegendIsolate, onChartClick, plotInset, zoomed = false, onResetZoom, legendPosition, scaleLegend, accessibleLabel, annotationNotes }) {
  const right = legendPosition === "right";
  const legendContent = <ChartLegend items={legend} onToggle={onLegendToggle}
    onIsolate={onLegendIsolate} position={right ? "right" : "bottom"} />;

  return (
    <div className={["chart-layout", right ? "chart-layout--legend-right" : ""].filter(Boolean).join(" ")}
      style={plotInset ? {
        "--chart-plot-left": `${plotInset.left ?? 0}px`,
        "--chart-plot-right": `${plotInset.right ?? 0}px`,
      } : undefined}>
      {zoomed && onResetZoom && <button type="button" className="chart-reset-zoom"
        onClick={onResetZoom}>Reset zoom</button>}
      <div className="chart-frame" style={{ minHeight: height }} role="group"
        aria-label={accessibleLabel || xLabel || "Reviewed data chart"} onClickCapture={onChartClick}>
        <ResponsiveContainer width="100%" height="100%">{chart}</ResponsiveContainer>
      </div>
      {right && legendContent}
      {Boolean(xLabel || scaleLegend || (!right && legend.length)) && <div className="chart-footer">
        {xLabel && <p className="chart-axis-label">{xLabel}</p>}
        {scaleLegend && <div className="chart-scale-legend" aria-label={`${scaleLegend.label} color scale`}>
          <span>{scaleLegend.minimum}</span>
          <i aria-hidden="true" style={{ "--chart-scale-color": scaleLegend.color }} />
          <span>{scaleLegend.maximum}</span>
        </div>}
        {!right && legendContent}
      </div>}
      {annotationNotes}
    </div>
  );
}
