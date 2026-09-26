import React from "react";

import { MetricSparkline } from "../charting/MetricSparkline.jsx";
import { DataComponent } from "./DataComponent.jsx";

export { MetricSparkline };

export function MetricCard({ value, comparison, negative = false, trendValues = [], className = "", ...component }) {
  const delta = typeof comparison === "string" ? comparison.split(/\s+vs\.?\s+/iu)[0] : comparison;
  return <DataComponent variant="card" padding="spacious" {...component} kind="metric"
    className={["data-metric-card", "metric-item", className].filter(Boolean).join(" ")}>
    <div className="metric-primary data-metric-primary">
      <p className="metric-value data-metric-value">{value}</p>
      {delta && <span className="metric-change data-metric-change">
        <MetricSparkline values={trendValues} negative={negative} />
        <span className={["comparison", "data-metric-delta", negative && "negative"].filter(Boolean).join(" ")}>
          {delta}
        </span>
      </span>}
    </div>
  </DataComponent>;
}
