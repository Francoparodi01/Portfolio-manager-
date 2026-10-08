import { useState } from "react";
import { HorizontalBars, LineChart } from "../components/charts/MiniCharts";
import { EmptyState, ErrorState, LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { useHistoricalReplayQuery } from "../hooks/useMonitorData";
import type { RowRecord, Tone } from "../types/api";
import { asRecord, asRows, getBoolean, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatNumber, formatPercent, formatScore, toneForNumber } from "../utils/format";

const SIGNALS = ["TODAS", "BUY", "SELL", "HOLD"] as const;
const SEVERITIES = ["TODAS", "PASS", "WARN", "FAIL", "UNKNOWN"] as const;

function severityTone(value: string): Tone {
  if (value === "PASS") return "positive";
  if (value === "WARN" || value === "UNKNOWN") return "warning";
  if (value === "FAIL") return "negative";
  return "neutral";
}

function signalTone(value: string): Tone {
  if (value === "BUY") return "positive";
  if (value === "SELL") return "negative";
  return "neutral";
}

const columns: TableColumn<RowRecord>[] = [
  { id: "date", header: "Fecha", render: (row) => getString(row, "observed_date", "-") },
  { id: "ticker", header: "Ticker", render: (row) => <strong>{getString(row, "ticker", "-")}</strong> },
  {
    id: "signal",
    header: "Base / nuevo",
    render: (row) => {
      const oldSignal = getString(row, "old_signal", "-");
      const newSignal = getString(row, "new_signal", "-");
      return <StatusBadge tone={signalTone(oldSignal)}>{oldSignal} / {newSignal}</StatusBadge>;
    },
  },
  { id: "score", header: "Score", align: "right", render: (row) => formatScore(getNumber(row, "old_score_raw")) },
  {
    id: "context",
    header: "Contexto",
    render: (row) => {
      const severity = getString(row, "context_severity", "UNKNOWN");
      return <StatusBadge tone={severityTone(severity)}>{severity}</StatusBadge>;
    },
  },
  { id: "confidence", header: "Conf.", render: (row) => getString(row, "context_confidence", "-") },
  {
    id: "return",
    header: "Outcome 5D",
    align: "right",
    render: (row) => formatPercent(getNumber(row, "directional_or_hold_return_5d"), 2, true),
  },
  { id: "status", header: "Estado", render: (row) => getString(row, "outcome_5d_status", "-") },
];

export default function HistoricalReplayPage() {
  const query = useHistoricalReplayQuery();
  const [signal, setSignal] = useState<(typeof SIGNALS)[number]>("TODAS");
  const [severity, setSeverity] = useState<(typeof SEVERITIES)[number]>("TODAS");

  if (query.isLoading && !query.data) return <LoadingState label="Cargando replay histórico" />;
  if (query.isError) return <ErrorState message="No se pudo leer el replay histórico." onRetry={() => query.refetch()} />;
  if (!query.data?.available) {
    return <EmptyState label="Replay histórico todavía no disponible" detail={query.data?.note} />;
  }

  const summary = asRecord(query.data.summary);
  const population = getRecord(summary, "population");
  const oldAnalysis = getRecord(summary, "old_analysis");
  const contextAnalysis = getRecord(summary, "contextual_analysis");
  const nonRegression = getRecord(summary, "non_regression");
  const run = asRecord(query.data.run);
  const reconstruction = asRecord(query.data.reconstruction);
  const allRows = query.data.rows || [];
  const dailyRows = asRows(summary.daily_static_hold_5d);

  const filteredRows = allRows.filter((row) => (
    (signal === "TODAS" || getString(row, "old_signal") === signal)
    && (severity === "TODAS" || getString(row, "context_severity") === severity)
    && getBoolean(row, "metric_eligible") === true
  ));

  const signalBars = ["BUY", "SELL", "HOLD"].map((item) => {
    const metric = getRecord(getRecord(getRecord(oldAnalysis, "by_signal"), item), "directional_or_hold");
    const value = getNumber(metric, "mean") ?? 0;
    return {
      display: `${formatPercent(value, 2, true)} · n=${formatNumber(getNumber(metric, "n"))}`,
      label: item,
      tone: toneForNumber(value),
      value,
    };
  });
  const contextBars = ["PASS", "WARN", "FAIL", "UNKNOWN"].map((item) => {
    const metric = getRecord(getRecord(getRecord(contextAnalysis, "by_severity"), item), "asset_return");
    const value = getNumber(metric, "mean") ?? 0;
    return {
      display: `${formatPercent(value, 2, true)} · n=${formatNumber(getNumber(metric, "n"))}`,
      label: item,
      tone: severityTone(item),
      value,
    };
  });

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Replay histórico"
        title="Análisis base + contexto E2"
        description="El portfolio observado se conserva y el contexto se adjunta en shadow. Esta vista es retrospectiva y no representa una corrida PIT histórica."
        action={<StatusBadge tone="theoretical">SHADOW ONLY</StatusBadge>}
      />

      <div className="inline-alert">
        <StatusBadge tone="warning">No PIT</StatusBadge>
        <span>Las velas históricas fueron ingeridas después de parte de los cortes. Los resultados describen el código actual sobre precios pasados; no son PnL realizado ni alteran decisiones.</span>
      </div>

      <MetricGroup>
        <Metric label="Filas preservadas" value={formatNumber(getNumber(population, "canonical_rows"))} detail={`${formatNumber(getNumber(reconstruction, "observed_dates"))} fechas observadas`} />
        <Metric label="Elegibles" value={formatNumber(getNumber(population, "metric_eligible_rows"))} detail="ticker + rueda únicos" />
        <Metric label="Outcomes 5D" value={formatNumber(getNumber(population, "evaluated_5d_rows"))} detail="ventanas maduras" />
        <Metric
          label="BUY/SELL positivos"
          value={formatPercent(getNumber(getRecord(oldAnalysis, "action_only_directional_5d"), "positive_rate"))}
          detail={`n=${formatNumber(getNumber(getRecord(oldAnalysis, "action_only_directional_5d"), "n"))}`}
        />
      </MetricGroup>

      <div className="panel-grid two">
        <Panel kicker="Análisis base" title="Outcome direccional por señal">
          <HorizontalBars rows={signalBars} title="Outcome por señal" description="Media a cinco ruedas del análisis técnico actual." />
          <p className="table-note">SELL se expresa como retorno evitado; HOLD mide la exposición mantenida.</p>
        </Panel>
        <Panel kicker="Contexto E2" title="Retorno del activo por severidad">
          <HorizontalBars rows={contextBars} title="Retorno por severidad" description="Cohortes descriptivas del contexto shadow." />
          <p className="table-note">La severidad no bloqueó ni cambió ninguna señal.</p>
        </Panel>
      </div>

      <Panel kicker="Portfolio observado" title="Exposición estática posterior a cinco ruedas">
        <LineChart rows={dailyRows} valueKey="static_hold_return_5d" labelKey="market_session" title="Retorno estático 5D" description="Retorno ponderado de las posiciones observadas con cobertura suficiente." />
      </Panel>

      <Panel
        kicker="Explorador"
        title="Observaciones históricas"
        action={<span className="table-note">{formatNumber(filteredRows.length)} filas</span>}
      >
        <div className="intel-control-grid">
          <div>
            <span className="control-label">Señal</span>
            <div className="segmented-control" aria-label="Filtrar señal">
              {SIGNALS.map((item) => <button className={signal === item ? "active" : ""} key={item} onClick={() => setSignal(item)} type="button">{item}</button>)}
            </div>
          </div>
          <div>
            <span className="control-label">Contexto</span>
            <div className="segmented-control" aria-label="Filtrar contexto">
              {SEVERITIES.map((item) => <button className={severity === item ? "active" : ""} key={item} onClick={() => setSeverity(item)} type="button">{item}</button>)}
            </div>
          </div>
        </div>
        <ResponsiveTable columns={columns} emptyLabel="Sin filas para estos filtros" rowKey={(row, index) => `${getString(row, "observed_date")}-${getString(row, "ticker")}-${index}`} rows={filteredRows.slice(0, 120)} />
        <p className="table-note">Se muestran hasta 120 filas. La API conserva el universo completo del run.</p>
      </Panel>

      <div className="panel-grid two">
        <Panel kicker="No regresión" title="Autoridad productiva intacta">
          <div className="context-stack">
            <p><strong>Scores:</strong> {getBoolean(nonRegression, "scores_equal") ? "iguales" : "revisar"}</p>
            <p><strong>Señales:</strong> {getBoolean(nonRegression, "signals_equal") ? "iguales" : "revisar"}</p>
            <p><strong>Decisiones cambiadas:</strong> {formatNumber(getNumber(nonRegression, "decisions_changed"))}</p>
            <p><strong>Órdenes creadas:</strong> {formatNumber(getNumber(nonRegression, "orders_created"))}</p>
          </div>
        </Panel>
        <Panel kicker="Linaje" title="Corrida persistida">
          <div className="context-stack">
            <p><strong>Run:</strong> {getString(run, "run_id", "-")}</p>
            <p><strong>Estado:</strong> {getString(run, "status", "-")}</p>
            <p><strong>Período:</strong> {getString(run, "window_start", "-")} → {getString(run, "window_end", "-")}</p>
            <p><strong>Completado:</strong> {formatDateTime(getString(run, "completed_at"))}</p>
          </div>
        </Panel>
      </div>
    </div>
  );
}
