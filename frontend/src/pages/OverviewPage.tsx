import { AlertTriangle, CheckCircle2, Clock3, ShieldAlert } from "lucide-react";
import { LineChart } from "../components/charts/MiniCharts";
import { ErrorState, LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { DataFreshness } from "../components/ui/DataFreshness";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import {
  useDecisionsQuery,
  useFillsQuery,
  useHealthQuery,
  useIngestionQuery,
  useOverrideQuery,
  usePerformanceQuery,
  usePortfolioQuery,
  useRadarQuery,
} from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getBoolean, getNumber, getRecord, getString, maxNumber } from "../utils/data";
import { formatDateTime, formatMoney, formatNumber, formatPercent, formatScore, sampleLabel, toneForNumber } from "../utils/format";
import { decisionLabel, scopeLabel, sourceLabel, statusLabel, toneForScope, toneForStatus } from "../utils/labels";

function queryError(...errors: unknown[]): string | null {
  const found = errors.find(Boolean);
  return found instanceof Error ? found.message : found ? "No se pudo cargar un endpoint" : null;
}

function portfolioVariation(history: RowRecord[]): number | null {
  const values = [...history]
    .sort((a, b) => String(getString(a, "scraped_at") || getString(a, "day")).localeCompare(String(getString(b, "scraped_at") || getString(b, "day"))))
    .map((row) => getNumber(row, "total_value_ars"))
    .filter((value): value is number => value !== null && value > 0);
  if (values.length < 2) return null;
  const previous = values[values.length - 2];
  const current = values[values.length - 1];
  return previous ? current / previous - 1 : null;
}

function riskCopy(topWeight: number | null, dataStale: boolean): { label: string; detail: string; tone: "warning" | "negative" | "neutral" } {
  if (dataStale) {
    return {
      detail: "Actualizar datos antes de usar esta lectura.",
      label: "Datos desactualizados",
      tone: "warning",
    };
  }
  if (topWeight !== null && topWeight >= 0.35) {
    return {
      detail: "La posición principal concentra más de un tercio del valor.",
      label: "Concentración alta",
      tone: "negative",
    };
  }
  if (topWeight !== null && topWeight >= 0.25) {
    return {
      detail: "La cartera requiere seguimiento por peso relativo del principal activo.",
      label: "Concentración moderada",
      tone: "warning",
    };
  }
  return {
    detail: "No aparece una concentración dominante en el snapshot actual.",
    label: "Riesgo controlado",
    tone: "neutral",
  };
}

function hasNonZeroScore(row: RowRecord): boolean {
  const score = getNumber(row, "final_score");
  return score !== null && Math.abs(score) > 0.000001;
}

export default function OverviewPage() {
  const health = useHealthQuery();
  const ingestion = useIngestionQuery();
  const portfolio = usePortfolioQuery(90);
  const decisions = useDecisionsQuery(90);
  const performance = usePerformanceQuery(180);
  const override = useOverrideQuery(90);
  const radar = useRadarQuery(90);
  const fills = useFillsQuery(90);

  const errorMessage = queryError(
    health.error,
    ingestion.error,
    portfolio.error,
    decisions.error,
    performance.error,
  );
  const snapshot = getRecord(portfolio.data, "snapshot");
  const positions = asRows(portfolio.data?.positions);
  const history = asRows(portfolio.data?.history);
  const latest = getRecord(ingestion.data?.portfolio, "latest");
  const totalValue = getNumber(snapshot, "total_value_ars") ?? getNumber(latest, "total_value_ars");
  const cash = getNumber(snapshot, "cash_ars") ?? getNumber(latest, "cash_ars");
  const variation = portfolioVariation(history);
  const topWeight = maxNumber(positions, "weight_in_portfolio");
  const risk = riskCopy(topWeight, !getBoolean(health.data, "ok") && Boolean(health.data));
  const summary = getRecord(performance.data, "summary");
  const decisionRows = asRows(decisions.data?.recent);
  const overrideRows = asRows(override.data?.recent);
  const radarRows = asRows(radar.data?.recent).filter(hasNonZeroScore);
  const priority = overrideRows[0] || decisionRows[0] || radarRows[0] || null;
  const performanceClosed = getNumber(summary, "closed_5d") ?? 0;
  const sourceDate = getString(snapshot, "scraped_at") || getString(latest, "scraped_at");

  if (portfolio.isLoading && !portfolio.data) {
    return <LoadingState label="Cargando resumen de cartera" />;
  }

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Resumen"
        title="Estado de cartera y atención prioritaria"
        description="Lectura operativa basada en portfolio, decisiones, auditoría y performance expuestos por la API monitor."
      />

      {errorMessage ? <ErrorState message={errorMessage} onRetry={() => void portfolio.refetch()} /> : null}

      <MetricGroup>
        <Metric
          detail={variation === null ? "Sin punto anterior comparable" : "Cambio contra último snapshot disponible"}
          label="Valor total"
          tone="real"
          value={formatMoney(totalValue)}
        />
        <Metric
          detail="Variación relevante"
          label="Cambio"
          tone={toneForNumber(variation)}
          value={formatPercent(variation, 1, true)}
        />
        <Metric detail="Disponible en el snapshot" label="Efectivo" tone="neutral" value={formatMoney(cash)} />
        <Metric detail={`${formatNumber(positions.length)} posiciones`} label="Concentración" tone={risk.tone} value={formatPercent(topWeight)} />
      </MetricGroup>

      <div className="overview-grid">
        <Panel className="primary-reading" kicker="Lectura de Quantia" title="Qué cambió">
          <div className="reading-block">
            <p>
              {risk.label}. {risk.detail} La performance operativa se interpreta con{" "}
              {sampleLabel(performanceClosed).toLowerCase()}.
            </p>
            <ul className="evidence-list">
              <li>
                <CheckCircle2 size={16} aria-hidden="true" />
                <span>Principal oportunidad: {radarRows[0] ? `${getString(radarRows[0], "ticker")} en estado ${statusLabel(getString(radarRows[0], "candidate_status") || getString(radarRows[0], "status"))}` : "sin oportunidad priorizada"}</span>
              </li>
              <li>
                <ShieldAlert size={16} aria-hidden="true" />
                <span>Riesgo principal: {risk.label}</span>
              </li>
              <li>
                <Clock3 size={16} aria-hidden="true" />
                <span>Último dato de cartera: {formatDateTime(sourceDate)}</span>
              </li>
            </ul>
          </div>
        </Panel>

        <Panel kicker="Acción prioritaria" title={priority ? `${decisionLabel(getString(priority, "decision"))} ${getString(priority, "ticker")}` : "Sin acción prioritaria"}>
          {priority ? (
            <article className="decision-focus">
              <div>
                <StatusBadge tone={toneForStatus(getString(priority, "override_status") || getString(priority, "status"))}>
                  {statusLabel(getString(priority, "override_status") || getString(priority, "status"))}
                </StatusBadge>
                <StatusBadge tone={toneForScope(getString(priority, "metric_scope"))}>
                  {scopeLabel(getString(priority, "metric_scope"))}
                </StatusBadge>
              </div>
              <p>{getString(priority, "reason") || "La API no expone una explicación textual para esta fila."}</p>
              <dl className="compact-dl">
                <div>
                  <dt>Monto o referencia</dt>
                  <dd>{formatMoney(getNumber(priority, "target_amount_ars") ?? getNumber(priority, "same_amount_ars") ?? getNumber(priority, "price_at_decision"))}</dd>
                </div>
                <div>
                  <dt>Confianza</dt>
                  <dd>{formatPercent(getNumber(priority, "confidence"))}</dd>
                </div>
                <div>
                  <dt>Score cuantitativo</dt>
                  <dd>{formatScore(getNumber(priority, "final_score"))}</dd>
                </div>
                <div>
                  <dt>Momento sugerido</dt>
                  <dd>{formatDateTime(getString(priority, "next_executable_at") || getString(priority, "decided_at"))}</dd>
                </div>
              </dl>
            </article>
          ) : (
            <p className="muted-copy">No hay una recomendación operable expuesta por la API actual.</p>
          )}
        </Panel>
      </div>

      <div className="panel-grid two">
        <Panel kicker="Cartera" title="Evolución de valor">
          <LineChart
            description="Serie de valor total de cartera por snapshot disponible."
            labelKey="scraped_at"
            rows={history}
            title="Valor de cartera"
            valueKey="total_value_ars"
          />
        </Panel>
        <Panel kicker="Riesgos accionables" title="Qué revisar">
          <ul className="risk-list">
            <li>
              <AlertTriangle size={16} aria-hidden="true" />
              <span>{risk.label}: {risk.detail}</span>
            </li>
            <li>
              <AlertTriangle size={16} aria-hidden="true" />
              <span>{sampleLabel(performanceClosed)}. Evitar conclusiones fuertes si la muestra es baja.</span>
            </li>
            <li>
              <AlertTriangle size={16} aria-hidden="true" />
              <span>Fills sin reconciliar: {formatNumber(getNumber(getRecord(fills.data, "summary"), "unreconciled"))}</span>
            </li>
          </ul>
          <DataFreshness source="portfolio_snapshots" value={sourceDate} />
        </Panel>
      </div>

      <div className="panel-grid two">
        <Panel kicker="Posiciones principales" title="Mayor peso en cartera">
          <ResponsiveTable
            columns={positionColumns}
            emptyLabel="Sin posiciones en el snapshot"
            rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
            rows={positions.slice(0, 5)}
          />
        </Panel>
        <Panel kicker="Oportunidades" title="Radar priorizado">
          <ResponsiveTable
            columns={radarColumns}
            emptyLabel="Sin oportunidades recientes"
            rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
            rows={radarRows.slice(0, 4)}
          />
        </Panel>
      </div>

      <Panel kicker="Actividad reciente" title="Decisiones, ejecuciones y movimientos">
        <ResponsiveTable
          columns={activityColumns}
          emptyLabel="Sin actividad reciente"
          rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
          rows={[...decisionRows.slice(0, 5), ...asRows(fills.data?.recent).slice(0, 5)].slice(0, 8)}
        />
      </Panel>
    </div>
  );
}

const positionColumns: TableColumn<RowRecord>[] = [
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { align: "right", header: "Valor", id: "value", render: (row) => formatMoney(getNumber(row, "market_value")) },
  { align: "right", header: "Peso", id: "weight", render: (row) => formatPercent(getNumber(row, "weight_in_portfolio")) },
  { align: "right", header: "P/L", id: "pnl", render: (row) => <span className={toneForNumber(getNumber(row, "unrealized_pnl_pct"))}>{formatPercent(getNumber(row, "unrealized_pnl_pct"), 1, true)}</span> },
];

const radarColumns: TableColumn<RowRecord>[] = [
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "candidate_status") || getString(row, "status"))}>{statusLabel(getString(row, "candidate_status") || getString(row, "status"))}</StatusBadge> },
  { align: "right", header: "Score", id: "score", render: (row) => formatScore(getNumber(row, "final_score")) },
  { header: "Fuente", id: "source", render: (row) => getString(row, "technical_source") || "decision_log" },
];

const activityColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at") || getString(row, "executed_at")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Evento", id: "event", render: (row) => decisionLabel(getString(row, "decision") || getString(row, "side")) },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source") || getString(row, "metric_scope")) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge> },
];
