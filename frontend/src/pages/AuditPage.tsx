import { useSearchParams } from "react-router-dom";
import { LoadingState } from "../components/feedback/States";
import { LineChart } from "../components/charts/MiniCharts";
import { PageHeader } from "../components/layout/PageHeader";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { useDecisionsQuery, useLearningShadowQuery, useLogsQuery, usePerformanceQuery } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatNumber, formatPercent, toneForNumber } from "../utils/format";
import { decisionLabel, scopeLabel, sourceLabel, statusLabel, toneForScope, toneForStatus } from "../utils/labels";

const scopes = ["todos", "primary", "planner_audit", "radar_audit", "blocked_audit", "debug"] as const;

export default function AuditPage() {
  const [params, setParams] = useSearchParams();
  const scope = (params.get("scope") || "todos") as (typeof scopes)[number];
  const decisions = useDecisionsQuery(90);
  const performance = usePerformanceQuery(180);
  const learning = useLearningShadowQuery(365);
  const logs = useLogsQuery();
  const summary = getRecord(decisions.data, "summary");
  const groups = asRows(decisions.data?.groups).filter((row) => scope === "todos" || getString(row, "metric_scope") === scope);
  const statusCounts = asRows(performance.data?.status_counts).filter((row) => scope === "todos" || getString(row, "metric_scope") === scope);
  const learningMetrics = asRows(learning.data?.metrics);
  const learning5d = learningMetrics.find((row) => getNumber(row, "horizon_days") === 5);
  const learningCohorts5d = asRows(learning.data?.cohorts).filter((row) => getNumber(row, "horizon_days") === 5);
  const learningRun = getRecord(learning.data, "run");
  const learningQuality = getRecord(learning.data, "data_quality");
  const learningReviews = asRows(learning.data?.review_summary);
  const learningCandidates = asRows(learning.data?.rule_candidates);

  const setScope = (nextScope: string) => {
    const next = new URLSearchParams(params);
    next.set("scope", nextScope);
    setParams(next, { replace: true });
  };

  if (decisions.isLoading && !decisions.data) return <LoadingState label="Cargando auditoría" />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Auditoría"
        title="Trazabilidad de eventos y métricas"
        description="La métrica principal queda separada de planner audit, radar audit, bloqueos y debug."
      />

      <MetricGroup>
        <Metric label="Primary" tone="real" value={formatNumber(getNumber(summary, "primary_metric"))} />
        <Metric label="Planner audit" tone="info" value={formatNumber(getNumber(summary, "execution_plan"))} />
        <Metric label="Radar audit" tone="theoretical" value={formatNumber(getNumber(summary, "radar_audit"))} />
        <Metric label="Debug" tone="pending" value={formatNumber(getNumber(summary, "debug_events"))} />
      </MetricGroup>

      <Panel kicker="Learning shadow v2" title="Qué pasó con los bloqueos del planner">
        <div className="analysis-layout">
          <LineChart
            description="Proporción semanal que superó el umbral, tuvo recorrido controlado y alpha positivo contra SPY."
            labelKey="cohort_date"
            rows={learningCohorts5d}
            title="Oportunidades limpias por cohorte"
            valueKey="clean_miss_rate"
          />
          <div className="context-stack">
            <StatusBadge tone={learning.data?.available ? "theoretical" : "pending"}>
              {learning.data?.available ? "Experimental" : "Sin corrida"}
            </StatusBadge>
            <dl className="compact-dl">
              <div>
                <dt>Bloqueos maduros 5D</dt>
                <dd>{formatNumber(getNumber(learning5d, "matured_cases"))}</dd>
              </div>
              <div>
                <dt>Falsos negativos potenciales</dt>
                <dd>{formatNumber(getNumber(learning5d, "potential_false_negatives"))}</dd>
              </div>
              <div>
                <dt>Tasa potencial</dt>
                <dd>{formatPercent(getNumber(learning5d, "potential_false_negative_rate"), 1)}</dd>
              </div>
              <div>
                <dt>Oportunidades limpias</dt>
                <dd>{formatNumber(getNumber(learning5d, "clean_missed_opportunities"))}</dd>
              </div>
              <div>
                <dt>Tasa limpia</dt>
                <dd>{formatPercent(getNumber(learning5d, "clean_miss_rate"), 1)}</dd>
              </div>
              <div>
                <dt>Recorrido riesgoso</dt>
                <dd>{formatNumber(getNumber(learning5d, "risky_counterfactual_wins"))}</dd>
              </div>
              <div>
                <dt>Cobertura benchmark</dt>
                <dd>{formatPercent(getNumber(learning5d, "benchmark_coverage_rate"), 1)}</dd>
              </div>
              <div>
                <dt>Controles únicos</dt>
                <dd>{formatNumber(getNumber(learningQuality, "unique_control_cases"))}</dd>
              </div>
              <div>
                <dt>Cobertura shadow previa</dt>
                <dd>{formatPercent(getNumber(learning5d, "shadow_coverage_rate"), 1)}</dd>
              </div>
              <div>
                <dt>Umbral material</dt>
                <dd>{formatNumber(getNumber(learningRun, "material_return_bps"))} bps</dd>
              </div>
              <div>
                <dt>Última evaluación</dt>
                <dd>{formatDateTime(learningRun.captured_at)}</dd>
              </div>
            </dl>
            <p>{learning.data?.note || "La capa todavía no tiene una corrida persistida."}</p>
          </div>
        </div>
        <ResponsiveTable
          columns={learningCaseColumns}
          emptyLabel="Sin casos materiales maduros"
          rowKey={(row, index) => `${getString(row, "ticker")}-${getNumber(row, "horizon_days")}-${index}`}
          rows={asRows(learning.data?.recent_cases)}
        />
      </Panel>

      <Panel kicker="Revisión v2" title="Cómo se separa la evidencia">
        <ResponsiveTable
          columns={learningReviewColumns}
          emptyLabel="Sin casos clasificados"
          rowKey={(row, index) => `${getString(row, "review_label")}-${index}`}
          rows={learningReviews}
        />
      </Panel>

      <Panel kicker="Shadow only" title="Propuestas pendientes de revisión humana">
        <ResponsiveTable
          columns={learningCandidateColumns}
          emptyLabel="Sin propuestas con muestra suficiente"
          rowKey={(row, index) => `${getNumber(row, "id")}-${index}`}
          rows={learningCandidates}
        />
      </Panel>

      <Panel
        action={
          <div className="segmented-control wide" aria-label="Filtro de alcance">
            {scopes.map((item) => (
              <button className={scope === item ? "active" : ""} key={item} onClick={() => setScope(item)} type="button">
                {item}
              </button>
            ))}
          </div>
        }
        kicker="decision_log"
        title="Clasificación de eventos"
      >
        <ResponsiveTable
          columns={groupColumns}
          emptyLabel="Sin eventos para el alcance seleccionado"
          rowKey={(row, index) => `${getString(row, "metric_scope")}-${getString(row, "source")}-${index}`}
          rows={groups}
        />
      </Panel>

      <Panel kicker="Muestra por estado" title="Qué cuenta y qué no cuenta">
        <ResponsiveTable
          columns={statusColumns}
          emptyLabel="Sin conteos de estado"
          rowKey={(row, index) => `${getString(row, "metric_scope")}-${getString(row, "status")}-${index}`}
          rows={statusCounts}
        />
      </Panel>

      <Panel kicker="Logs recientes" title="Eventos técnicos">
        <ResponsiveTable
          columns={logColumns}
          emptyLabel={logs.data?.note || "Sin logs recientes"}
          rowKey={(row, index) => `${getString(row, "file")}-${index}`}
          rows={asRows(logs.data?.items)}
        />
      </Panel>
    </div>
  );
}

const groupColumns: TableColumn<RowRecord>[] = [
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge> },
  { header: "Intención", id: "intent", render: (row) => getString(row, "run_intent", "unknown") },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge> },
  { header: "Acción", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { align: "right", header: "N", id: "n", render: (row) => formatNumber(getNumber(row, "n")) },
  { align: "right", header: "5D", id: "con_5d", render: (row) => formatNumber(getNumber(row, "con_5d")) },
];

const statusColumns: TableColumn<RowRecord>[] = [
  { header: "Alcance", id: "scope", render: (row) => scopeLabel(getString(row, "metric_scope")) },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")) },
  { header: "Estado", id: "status", render: (row) => statusLabel(getString(row, "status")) },
  { align: "right", header: "Total", id: "n", render: (row) => formatNumber(getNumber(row, "n")) },
  { align: "right", header: "Cerradas 5D", id: "closed", render: (row) => formatNumber(getNumber(row, "closed_5d")) },
  { align: "right", header: "Avg 5D", id: "avg", render: (row) => <span className={toneForNumber(getNumber(row, "avg_5d"))}>{formatPercent(getNumber(row, "avg_5d"), 1, true)}</span> },
];

const logColumns: TableColumn<RowRecord>[] = [
  { header: "Archivo", id: "file", render: (row) => getString(row, "file", "log") },
  { header: "Línea", id: "line", render: (row) => <code>{getString(row, "line")}</code> },
];

const learningCaseColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(row.decided_at) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Acción", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { align: "right", header: "Horizonte", id: "horizon", render: (row) => `${formatNumber(getNumber(row, "horizon_days"))}D` },
  { align: "right", header: "Resultado", id: "outcome", render: (row) => <span className={toneForNumber(getNumber(row, "directional_outcome"))}>{formatPercent(getNumber(row, "directional_outcome"), 1, true)}</span> },
  { align: "right", header: "Alpha SPY", id: "alpha", render: (row) => <span className={toneForNumber(getNumber(row, "alpha_vs_benchmark"))}>{formatPercent(getNumber(row, "alpha_vs_benchmark"), 1, true)}</span> },
  { align: "right", header: "MAE", id: "mae", render: (row) => formatPercent(getNumber(row, "mae"), 1, true) },
  { header: "Revisión", id: "review", render: (row) => learningReviewLabel(getString(row, "review_label")) },
  { header: "Categoría", id: "category", render: (row) => getString(row, "block_category", "OTHER") },
];

const learningReviewColumns: TableColumn<RowRecord>[] = [
  { header: "Clasificación", id: "review", render: (row) => learningReviewLabel(getString(row, "review_label")) },
  { align: "right", header: "Casos", id: "cases", render: (row) => formatNumber(getNumber(row, "cases")) },
  { align: "right", header: "Resultado medio", id: "outcome", render: (row) => <span className={toneForNumber(getNumber(row, "mean_directional_outcome"))}>{formatPercent(getNumber(row, "mean_directional_outcome"), 1, true)}</span> },
  { align: "right", header: "MAE medio", id: "mae", render: (row) => formatPercent(getNumber(row, "mean_mae"), 1, true) },
  { align: "right", header: "Alpha medio", id: "alpha", render: (row) => <span className={toneForNumber(getNumber(row, "mean_alpha_vs_benchmark"))}>{formatPercent(getNumber(row, "mean_alpha_vs_benchmark"), 1, true)}</span> },
];

const learningCandidateColumns: TableColumn<RowRecord>[] = [
  { header: "Categoría", id: "category", render: (row) => getString(row, "block_category") },
  { header: "Experimento", id: "type", render: (row) => getString(row, "candidate_type") },
  { align: "right", header: "Muestra", id: "sample", render: (row) => formatNumber(getNumber(row, "sample_size")) },
  { align: "right", header: "Limpios", id: "clean", render: (row) => formatNumber(getNumber(row, "clean_miss_count")) },
  { align: "right", header: "Tasa limpia", id: "rate", render: (row) => formatPercent(getNumber(row, "clean_miss_rate"), 1) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone="pending">{getString(row, "status", "PROPOSED")}</StatusBadge> },
];

function learningReviewLabel(value: string): string {
  return {
    CLEAN_MISSED_OPPORTUNITY: "Oportunidad limpia",
    RISKY_COUNTERFACTUAL_WIN: "Ganador con recorrido riesgoso",
    MARKET_DRIVEN_WIN: "Explicado por el mercado",
    UNCONTROLLED_COUNTERFACTUAL_WIN: "Sin benchmark válido",
    NO_MATERIAL_UPSIDE: "Sin suba material",
    INSUFFICIENT_EVIDENCE: "Evidencia insuficiente",
  }[value] || value;
}
