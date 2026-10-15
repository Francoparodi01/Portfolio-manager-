import { EmptyState, ErrorState, LoadingState } from "../../components/feedback/States";
import { Panel } from "../../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../../components/ui/ResponsiveTable";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useTopOpportunitiesQuery } from "../../hooks/useMonitorData";
import type { TopOpportunity } from "../../types/api";
import { getNumber, getString } from "../../utils/data";
import { formatDateTime, formatNumber, formatScore } from "../../utils/format";

const levels = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 });

function price(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value);
  return value === null || value === undefined || !Number.isFinite(number) ? "-" : levels.format(number);
}

const columns: TableColumn<TopOpportunity>[] = [
  { id: "rank", header: "#", render: (row) => formatNumber(row.rank_position), sortValue: (row) => getNumber(row, "rank_position") },
  { id: "ticker", header: "Ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong>, sortValue: (row) => getString(row, "ticker") },
  { id: "readiness", header: "Preparación", render: (row) => <StatusBadge tone="info">{getString(row, "readiness_state") || "Sin clasificación"}</StatusBadge> },
  { id: "ranking", header: "Ranking", align: "right", render: (row) => formatScore(getNumber(row, "ranking_score")), sortValue: (row) => getNumber(row, "ranking_score") },
  { id: "trigger", header: "Activación", align: "right", render: (row) => price(row.trigger_price) },
  { id: "invalidation", header: "Invalidación", align: "right", render: (row) => price(row.invalidation_price) },
  { id: "target", header: "Objetivo", align: "right", render: (row) => price(row.target_price) },
  { id: "rr", header: "R/R", align: "right", render: (row) => price(row.setup_risk_reward) },
  { id: "quality", header: "Evidencia", render: (row) => (
    <span title={getString(row, "data_source_mode")}>
      Precio {getString(row, "price_quality_flag", "UNKNOWN")} · Features {getString(row, "feature_quality_flag", "UNKNOWN")}
    </span>
  ) },
  { id: "reason", header: "Contexto / límite", render: (row) => (
    getString(row, "manual_event_risk") ||
    getString(row, "why_not_now") ||
    getString(row, "acceptance_reason") ||
    getString(row, "technical_regime") ||
    "Sin tesis textual verificable"
  ) },
];

export function TopTenOpportunities() {
  const top = useTopOpportunitiesQuery();
  const data = top.data;
  return (
    <Panel kicker="Captura prospectiva · solo lectura" title="Top 10 para investigar">
      {top.isLoading && !data ? (
        <LoadingState label="Consultando ultima captura del Radar Discovery" />
      ) : top.isError ? (
        <ErrorState message="No se pudo cargar /api/opportunities/top10" onRetry={() => top.refetch()} />
      ) : data?.status !== "READY" ? (
        <EmptyState
          label="No hay ranking prospectivo disponible"
          detail={
            data?.status === "SCHEMA_MISSING"
              ? "Las tablas de Radar Discovery no estan instaladas. No se usan decisiones historicas como sustituto."
              : data?.status === "OWNER_NOT_CONFIGURED"
                ? "El monitor no tiene un owner configurado."
                : "No existe una captura prospectiva completa para esta cuenta. Revisar RADAR_DISCOVERY_LEDGER_ENABLED y el job --capture-discovery."
          }
        />
      ) : (
        <>
          <div className="audit-note">
            <strong>{data.stale ? "Captura vencida — no es un ranking vigente" : "Ultima captura congelada"}</strong>
            <span>
              Corte: {formatDateTime(data.captured_at)} · {formatNumber(data.count)} candidatos fuera de cartera ·
              Universo: {formatNumber(data.universe_count)} · Elegibles: {formatNumber(data.eligible_count)}.
            </span>
          </div>
          {data.items.length ? (
            <ResponsiveTable
              columns={columns}
              rows={data.items}
              rowKey={(row) => getString(row, "ticker")}
            />
          ) : (
            <EmptyState label="La ultima captura no tiene candidatos elegibles fuera de cartera" />
          )}
          <p className="table-note">
            Orden original congelado del Radar, no probabilidad de suba ni señal de compra.
            Activación, objetivo e invalidación son niveles teóricos en la unidad de la fuente, no precios ejecutables verificados.
            Sin análisis fundamental certificado en esta vista. No se usan outcomes futuros para ordenar.
            {data.stale ? " No operar sobre esta captura vencida." : ""}
          </p>
        </>
      )}
    </Panel>
  );
}
