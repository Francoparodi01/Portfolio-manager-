import type { ReactNode } from "react";
import type { Tone } from "../../types/api";

export function Metric({
  label,
  value,
  detail,
  tone = "neutral",
  badge,
}: {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  tone?: Tone;
  badge?: ReactNode;
}) {
  return (
    <article className={`metric ${tone}`}>
      <div className="metric-topline">
        <span>{label}</span>
        {badge}
      </div>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </article>
  );
}

export function MetricGroup({ children }: { children: ReactNode }) {
  return <div className="metric-group">{children}</div>;
}
