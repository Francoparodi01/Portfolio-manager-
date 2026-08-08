import { ageLabel, formatDateTime } from "../../utils/format";
import { StatusBadge } from "./StatusBadge";

export function DataFreshness({
  label = "Actualización",
  source,
  value,
  staleAfterHours = 24,
}: {
  label?: string;
  source?: string;
  value?: unknown;
  staleAfterHours?: number;
}) {
  const date = value ? new Date(String(value)) : null;
  const stale =
    date && !Number.isNaN(date.getTime())
      ? Date.now() - date.getTime() > staleAfterHours * 3_600_000
      : true;

  return (
    <div className="freshness">
      <span>{label}</span>
      <strong>{value ? ageLabel(value) : "sin datos"}</strong>
      <small>
        {source ? `${source} · ` : ""}
        {formatDateTime(value)}
      </small>
      <StatusBadge tone={stale ? "warning" : "real"}>
        {stale ? "Dato desactualizado" : "Dato vigente"}
      </StatusBadge>
    </div>
  );
}
