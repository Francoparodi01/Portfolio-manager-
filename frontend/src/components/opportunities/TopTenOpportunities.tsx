import { EmptyState, ErrorState, LoadingState } from "../../components/feedback/States";
import { Panel } from "../../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../../components/ui/ResponsiveTable";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useTopOpportunitiesQuery } from "../../hooks/useMonitorData";
import type { TopOpportunity } from "../../types/api";
import { getString } from "../../utils/data";
import { formatDateTime, formatNumber } from "../../utils/format";

const levels = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 });

function price(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value);
  return value === null || value === undefined || value === "" || !Number.isFinite(number) ? "No informado" : levels.format(number);
}

const columns: TableColumn<TopOpportunity>[] = [
  { id: "ticker", header: "Ranking / ticker", render: (row) => <span>#{formatNumber(row.rank_position)} <strong className="ticker">{getString(row, "ticker")}</strong></span> },
  { id: "state", header: "Estado", render: (row) => <span style={{ whiteSpace: "nowrap" }}><StatusBadge tone={row.display_state === "COMPRAR" ? "positive" : "warning"}>{row.display_state || "ESPERAR"}</StatusBadge></span> },
  { id: "price", header: "Último precio registrado", render: (row) => <span>{price(row.reference_price)} {row.currency || "Moneda no verificada"}<br />{formatDateTime(row.reference_ts)}</span> },
  { id: "technical", header: "Técnico / tendencia / volumen", render: (row) => <span>Score: {price(row.technical_score)}<br />{getString(row, "technical_regime", "No informada")}<br /><span title={row.volume_definition || "Sin definición"}>Volumen relativo: {price(row.volume_ratio)}</span></span> },
  { id: "ranking", header: "Score ranking / niveles", render: (row) => <span>{price(row.ranking_score)}<details><summary>Niveles teóricos</summary>Activación: {price(row.trigger_price)}<br />Invalidación: {price(row.invalidation_price)}<br />Objetivo: {price(row.target_price)}<br />R/R: {price(row.setup_risk_reward)}</details></span> },
  { id: "quality", header: "Evidencia", render: (row) => (
    <span title={(row.missing_data || []).join("; ")}>
      {row.quality || "PARTIAL"} · {getString(row, "price_quality_flag", "UNKNOWN")}<br />
      Fuente: {Object.keys(row.data_source_counts || {}).join(", ") || getString(row, "data_source_mode", "No informada")}<br />
      Antigüedad: {row.price_age_seconds == null || row.price_age_seconds < 0 ? "No verificable" : `${(row.price_age_seconds / 3600).toFixed(1)} h`}
    </span>
  ) },
  { id: "reason", header: "Motivo / riesgo principal", render: (row) => <span>{row.selection_reason}<br />Riesgo: {row.principal_risk}<br />{(row.missing_data || []).join("; ")}</span> },
];

export function TopTenOpportunities() {
  const top = useTopOpportunitiesQuery();
  const data = top.data;
  return (
    <Panel kicker="Mismo ranking que /top10 · solo lectura" title="Top 10 de oportunidades">
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
                : data?.status === "INVALID_CAPTURE"
                  ? "La captura tiene datos inconsistentes. No se generan recomendaciones."
                  : "No existe una captura completa para esta cuenta."
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
            Orden original del Radar. El estado refleja sus reglas en la fecha de captura; ESPERAR indica también evidencia insuficiente.
            Se muestra el último precio registrado, no una cotización en vivo. El score no es una probabilidad de suba.
            Activación, objetivo e invalidación son niveles teóricos en la unidad de la fuente, no precios ejecutables verificados.
            Sin análisis fundamental certificado en esta vista. No se usan outcomes futuros para ordenar.
            {data.stale ? " No operar sobre esta captura vencida." : ""}
          </p>
        </>
      )}
    </Panel>
  );
}
