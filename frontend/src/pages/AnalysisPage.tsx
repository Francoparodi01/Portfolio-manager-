import { CheckCircle2, Info, ShieldAlert } from "lucide-react";
import { PageHeader } from "../components/layout/PageHeader";
import { LoadingState } from "../components/feedback/States";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { StatusBadge } from "../components/ui/StatusBadge";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { useCandlesQuery, useDecisionsQuery, usePerformanceQuery, useShadowQuery } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getNumber, getRecord, getString } from "../utils/data";
import { formatNumber, formatPercent, formatScore, sampleLabel, toneForNumber } from "../utils/format";
import { decisionLabel, scopeLabel, statusLabel, toneForScope, toneForStatus } from "../utils/labels";

function signalNarrative(row: RowRecord): string {
  const score = getNumber(row, "final_score");
  const ticker = getString(row, "ticker", "El activo");
  if (score === null) return `${ticker} no tiene score cuantitativo expuesto en esta fila.`;
  if (score <= -0.08) {
    return `${ticker} muestra una señal moderadamente negativa: perdió fuerza relativa o elevó el riesgo reciente.`;
  }
  if (score >= 0.08) {
    return `${ticker} muestra una señal positiva moderada, pero debe leerse junto con confianza, muestra y estado de ejecución.`;
  }
  return `${ticker} está en zona neutral; el score no alcanza por sí solo para explicar una acción.`;
}

export default function AnalysisPage() {
  const performance = usePerformanceQuery(180);
  const decisions = useDecisionsQuery(90);
  const shadow = useShadowQuery();
  const candles = useCandlesQuery();
  const recentSignals = asRows(performance.data?.bot_prediction_recent);
  const decisionSummary = getRecord(decisions.data, "summary");
  const perfSummary = getRecord(performance.data, "summary");
  const shadowMetrics = asRows(shadow.data?.metrics);
  const closed = getNumber(perfSummary, "closed_5d") ?? 0;

  if (performance.isLoading && !performance.data) return <LoadingState label="Cargando análisis" />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Análisis"
        title="Lectura explicada de Quantia"
        description="Los scores se muestran como evidencia secundaria; la explicación principal separa señal, riesgo, contexto y alcance."
      />

      <MetricGroup>
        <Metric detail={sampleLabel(closed)} label="EV 5 ruedas" tone={toneForNumber(getNumber(perfSummary, "ev_5d"))} value={formatPercent(getNumber(perfSummary, "ev_5d"), 1, true)} />
        <Metric label="Cerradas 5D" value={formatNumber(closed)} />
        <Metric label="Pendientes" tone="pending" value={formatNumber(getNumber(getRecord(performance.data, "window_counts"), "pending_primary_5d"))} />
        <Metric label="Debug/radar" tone="theoretical" value={formatNumber((getNumber(decisionSummary, "radar_audit") ?? 0) + (getNumber(decisionSummary, "debug_events") ?? 0))} />
      </MetricGroup>

      <div className="analysis-layout">
        <Panel kicker="Lectura general" title="Conclusión operativa">
          <div className="reading-block">
            <p>
              La lectura actual es preliminar cuando la muestra no supera el umbral estadístico. La métrica principal queda limitada a ejecuciones reales validadas; radares, bloqueos y debug permanecen como evidencia auditada.
            </p>
            <ul className="evidence-list">
              <li>
                <CheckCircle2 size={16} aria-hidden="true" />
                <span>Señales positivas: {formatNumber(recentSignals.filter((row) => (getNumber(row, "final_score") ?? 0) > 0.08).length)}</span>
              </li>
              <li>
                <ShieldAlert size={16} aria-hidden="true" />
                <span>Riesgos: {formatNumber(recentSignals.filter((row) => (getNumber(row, "final_score") ?? 0) < -0.08).length)} señales negativas recientes.</span>
              </li>
              <li>
                <Info size={16} aria-hidden="true" />
                <span>Contexto macro: no expuesto por la API monitor actual.</span>
              </li>
            </ul>
          </div>
        </Panel>

        <Panel kicker="Contexto técnico" title="Shadow y datos de mercado">
          <div className="context-stack">
            <StatusBadge tone={shadow.data?.available ? "info" : "pending"}>
              {shadow.data?.available ? "Shadow disponible" : "Shadow pendiente"}
            </StatusBadge>
            <p>{shadow.data?.note || "Shadow se trata como capa experimental separada de la decisión ejecutable."}</p>
            <dl className="compact-dl">
              <div>
                <dt>Muestras 5 ruedas</dt>
                <dd>{formatNumber(getNumber(shadowMetrics.find((row) => getNumber(row, "horizon_sessions") === 5), "samples"))}</dd>
              </div>
              <div>
                <dt>Velas faltantes</dt>
                <dd>{formatNumber(getNumber(candles.data?.coverage, "missing_internal"))}</dd>
              </div>
            </dl>
          </div>
        </Panel>
      </div>

      <Panel kicker="Señales recientes" title="Explicación por activo">
        <div className="signal-list">
          {recentSignals.length ? recentSignals.map((row, index) => (
            <article className="signal-item" key={`${getString(row, "ticker")}-${index}`}>
              <div className="signal-title">
                <strong>{getString(row, "ticker")}</strong>
                <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge>
                <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge>
              </div>
              <p>{signalNarrative(row)}</p>
              <dl className="compact-dl">
                <div>
                  <dt>Acción</dt>
                  <dd>{decisionLabel(getString(row, "decision"))}</dd>
                </div>
                <div>
                  <dt>Score cuantitativo</dt>
                  <dd>{formatScore(getNumber(row, "final_score"))}</dd>
                </div>
                <div>
                  <dt>Resultado 5D</dt>
                  <dd className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</dd>
                </div>
                <div>
                  <dt>Evidencia</dt>
                  <dd>{getString(row, "signal_family") || getString(row, "buy_confirmation") || "sin detalle"}</dd>
                </div>
              </dl>
            </article>
          )) : (
            <p className="muted-copy">No hay señales recientes en `/api/performance`.</p>
          )}
        </div>
      </Panel>

      <Panel kicker="Evidencia utilizada" title="Filas de decisión recientes">
        <ResponsiveTable
          columns={decisionColumns}
          emptyLabel="Sin filas recientes"
          rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
          rows={asRows(decisions.data?.recent)}
        />
      </Panel>
    </div>
  );
}

const decisionColumns: TableColumn<RowRecord>[] = [
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Acción", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge> },
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge> },
  { header: "Score", id: "score", render: (row) => formatScore(getNumber(row, "final_score")) },
];
