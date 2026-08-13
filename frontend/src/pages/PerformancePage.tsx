import { useMemo, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { ScatterChart, HorizontalBars } from "../components/charts/MiniCharts";
import { LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { PERIODS } from "../services/monitorApi";
import { usePeriodParam, usePerformanceQuery } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatNumber, formatPercent, formatScore, sampleLabel, toneForNumber } from "../utils/format";
import { decisionLabel, scopeLabel, sourceLabel, statusLabel, toneForScope, toneForStatus } from "../utils/labels";

const viewOptions = ["score", "fuentes", "senales", "tickers", "recientes"] as const;
const scopeOptions = ["todos", "primary", "followed_plan", "planner_audit", "radar_audit", "blocked_audit"] as const;
const actionOptions = ["todas", "BUY", "SELL", "HOLD"] as const;
const scoreOptions = ["con_score", "score_0", "todos"] as const;

type IntelView = (typeof viewOptions)[number];
type ScopeFilter = (typeof scopeOptions)[number];
type ActionFilter = (typeof actionOptions)[number];
type ScoreFilter = (typeof scoreOptions)[number];

export default function PerformancePage() {
  const [period, setPeriod] = usePeriodParam(180);
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedView = parseOption(searchParams.get("vista"), viewOptions, "score");
  const scopeFilter = parseOption(searchParams.get("alcance"), scopeOptions, "todos");
  const actionFilter = parseOption(searchParams.get("accion"), actionOptions, "todas");
  const scoreFilter = parseOption(searchParams.get("score"), scoreOptions, "con_score");
  const performance = usePerformanceQuery(period);
  const summary = getRecord(performance.data, "summary");
  const closed = getNumber(summary, "closed_5d") ?? 0;
  const directionRows = asRows(performance.data?.bot_direction_breakdown);
  const signalRows = asRows(performance.data?.bot_signal_breakdown);
  const sourceRows = asRows(performance.data?.source_breakdown);
  const scorePoints = asRows(performance.data?.score_points);
  const tickerRows = asRows(performance.data?.by_ticker);
  const recentRows = asRows(performance.data?.bot_prediction_recent);
  const baseScorePoints = useMemo(
    () => scorePoints.filter((row) => matchesFilters(row, scopeFilter, actionFilter)),
    [actionFilter, scopeFilter, scorePoints],
  );
  const scoreZeroCount = useMemo(() => baseScorePoints.filter(hasZeroScore).length, [baseScorePoints]);
  const filteredScorePoints = useMemo(
    () => baseScorePoints.filter((row) => matchesScoreFilter(row, scoreFilter)),
    [baseScorePoints, scoreFilter],
  );
  const filteredRecentRows = useMemo(
    () => recentRows.filter((row) => matchesFilters(row, scopeFilter, actionFilter) && matchesScoreFilter(row, scoreFilter)),
    [actionFilter, scopeFilter, recentRows, scoreFilter],
  );
  const filteredClosed = useMemo(
    () => filteredScorePoints.filter((row) => getNumber(row, "outcome_5d") !== null),
    [filteredScorePoints],
  );
  const filteredWinRate = winRate(filteredClosed, "outcome_5d");
  const filteredAverage = average(filteredClosed, "outcome_5d");

  const setFilter = (key: "vista" | "alcance" | "accion" | "score", value: string, defaultValue: string) => {
    const next = new URLSearchParams(searchParams);
    if (value === defaultValue) next.delete(key);
    else next.set(key, value);
    setSearchParams(next, { replace: true });
  };

  if (performance.isLoading && !performance.data) return <LoadingState label="Cargando performance" />;

  return (
    <div className="page-stack">
      <PageHeader
        action={
          <div className="segmented-control" aria-label="Periodo">
            {PERIODS.map((item) => (
              <button className={period === item ? "active" : ""} key={item} onClick={() => setPeriod(item)} type="button">
                {item === 365 ? "365d" : `${item}d`}
              </button>
            ))}
          </div>
        }
        eyebrow="Performance"
        title="Intel operativo con evidencia separada"
        description="Los resultados ejecutados, auditorias del bot y radar se miran por separado para no confundir ejecucion real con señales teoricas."
      />

      <MetricGroup>
        <Metric detail={sampleLabel(closed)} label="EV 5D" tone={toneForNumber(getNumber(summary, "ev_5d"))} value={formatPercent(getNumber(summary, "ev_5d"), 1, true)} />
        <Metric label="Acierto 5D" tone={toneForNumber((getNumber(summary, "win_rate_5d") ?? 0) - 0.5)} value={formatPercent(getNumber(summary, "win_rate_5d"))} />
        <Metric label="Retorno medio" tone={toneForNumber(getNumber(summary, "avg_5d"))} value={formatPercent(getNumber(summary, "avg_5d"), 1, true)} />
        <Metric label="Pendiente primaria" tone="pending" value={formatNumber(getNumber(getRecord(performance.data, "window_counts"), "pending_primary_5d"))} />
      </MetricGroup>

      {closed < 30 ? (
        <div className="inline-alert">
          <StatusBadge tone="warning">Evidencia preliminar</StatusBadge>
          <span>La muestra cerrada 5D es menor a 30; no conviene tratar la metrica como concluyente.</span>
        </div>
      ) : null}

      <Panel kicker="Explorador" title="Intel operativo">
        <div className="intel-control-grid">
          <ControlBlock label="Vista">
            <div className="segmented-control wide" aria-label="Vista Intel">
              {viewOptions.map((item) => (
                <button className={selectedView === item ? "active" : ""} key={item} onClick={() => setFilter("vista", item, "score")} type="button">
                  {viewLabel(item)}
                </button>
              ))}
            </div>
          </ControlBlock>
          <ControlBlock label="Alcance">
            <div className="segmented-control wide" aria-label="Filtro de alcance">
              {scopeOptions.map((item) => (
                <button className={scopeFilter === item ? "active" : ""} key={item} onClick={() => setFilter("alcance", item, "todos")} type="button">
                  {filterScopeLabel(item)}
                </button>
              ))}
            </div>
          </ControlBlock>
          <ControlBlock label="Accion">
            <div className="segmented-control" aria-label="Filtro de accion">
              {actionOptions.map((item) => (
                <button className={actionFilter === item ? "active" : ""} key={item} onClick={() => setFilter("accion", item, "todas")} type="button">
                  {filterActionLabel(item)}
                </button>
              ))}
            </div>
          </ControlBlock>
          <ControlBlock label="Score">
            <div className="segmented-control" aria-label="Filtro de score">
              {scoreOptions.map((item) => (
                <button className={scoreFilter === item ? "active" : ""} key={item} onClick={() => setFilter("score", item, "con_score")} type="button">
                  {filterScoreLabel(item)}
                </button>
              ))}
            </div>
          </ControlBlock>
          <div className="intel-stat-strip" aria-label="Resumen de filtros">
            <IntelStat label="Muestra filtrada" value={`${formatNumber(filteredClosed.length)} cerradas`} />
            <IntelStat label="Win filtrado" value={formatPercent(filteredWinRate)} />
            <IntelStat tone={toneForNumber(filteredAverage)} label="Avg 5D filtrado" value={formatPercent(filteredAverage, 1, true)} />
            <IntelStat tone="warning" label="Score 0 fuera" value={scoreFilter === "con_score" ? formatNumber(scoreZeroCount) : "-"} />
          </div>
        </div>
      </Panel>

      <Panel kicker={viewKicker(selectedView)} title={viewTitle(selectedView)}>
        {selectedView === "score" ? (
          <ScatterChart
            description="Relacion entre score cuantitativo y resultado posterior a cinco ruedas."
            labelKey="ticker"
            rows={filteredScorePoints}
            title="Score y outcome 5D"
            xKey="final_score"
            yKey="outcome_5d"
          />
        ) : null}
        {selectedView === "fuentes" ? (
          <HorizontalBars
            description="Retorno promedio a cinco ruedas por fuente y alcance."
            rows={sourceRows.map((row) => ({
              display: `${formatPercent(getNumber(row, "avg_5d"), 1, true)} / ${formatNumber(getNumber(row, "closed_5d"))}`,
              label: `${sourceLabel(getString(row, "source"))} - ${scopeLabel(getString(row, "metric_scope"))}`,
              tone: toneForNumber(getNumber(row, "avg_5d")),
              value: getNumber(row, "avg_5d") ?? 0,
            }))}
            title="Performance por fuente"
          />
        ) : null}
        {selectedView === "senales" ? (
          <HorizontalBars
            description="Retorno promedio a cinco ruedas por familia de señal."
            rows={signalRows.map((row) => ({
              display: `${formatPercent(getNumber(row, "avg_5d"), 1, true)} / ${formatNumber(getNumber(row, "closed_5d"))}`,
              label: signalFamilyLabel(getString(row, "signal_family", "Sin familia")),
              tone: toneForNumber(getNumber(row, "avg_5d")),
              value: getNumber(row, "avg_5d") ?? 0,
            }))}
            title="Performance por señal"
          />
        ) : null}
        {selectedView === "tickers" ? (
          <HorizontalBars
            description="Retorno promedio a cinco ruedas por ticker, ordenado por muestra."
            rows={[...tickerRows]
              .sort((a, b) => (getNumber(b, "n") ?? 0) - (getNumber(a, "n") ?? 0))
              .slice(0, 12)
              .map((row) => ({
                display: `${formatPercent(getNumber(row, "avg_5d"), 1, true)} / n=${formatNumber(getNumber(row, "n"))}`,
                label: getString(row, "ticker", "SIN"),
                tone: toneForNumber(getNumber(row, "avg_5d")),
                value: getNumber(row, "avg_5d") ?? 0,
              }))}
            title="Performance por ticker"
          />
        ) : null}
        {selectedView === "recientes" ? (
          <>
            <ResponsiveTable
              columns={recentPlanColumns}
              emptyLabel="Sin planes recientes para el filtro"
              rowKey={(row, index) => `${getString(row, "decided_at")}-${getString(row, "ticker")}-${index}`}
              rows={filteredRecentRows.slice(0, 16)}
            />
            <p className="table-note">Se muestran los ultimos 16 registros que cumplen los filtros activos.</p>
          </>
        ) : null}
      </Panel>

      <div className="panel-grid two">
        <Panel kicker="Score points" title="Muestra filtrada">
          <ResponsiveTable
            columns={scoreColumns}
            emptyLabel="Sin decisiones para los filtros elegidos"
            rowKey={(row, index) => `${getString(row, "decided_at")}-${getString(row, "ticker")}-${index}`}
            rows={filteredScorePoints.slice(0, 18)}
          />
          <p className="table-note">Vista acotada a 18 filas para lectura rapida; los filtros conservan el universo consultado.</p>
        </Panel>
        <Panel kicker="Planes bot" title="Ultimas decisiones registradas">
          <ResponsiveTable
            columns={recentPlanColumns}
            emptyLabel="Sin planes recientes del bot"
            rowKey={(row, index) => `${getString(row, "decided_at")}-${getString(row, "ticker")}-${index}`}
            rows={filteredRecentRows.slice(0, 8)}
          />
        </Panel>
      </div>

      <Panel kicker="Desglose" title="Performance por tipo de accion">
        <ResponsiveTable
          columns={directionColumns}
          emptyLabel="Sin desglose por accion"
          rowKey={(row, index) => `${getString(row, "decision")}-${index}`}
          rows={directionRows}
        />
      </Panel>

      <Panel kicker="Fuentes y alcances" title="No mezclar evidencia">
        <ResponsiveTable
          columns={sourceColumns}
          emptyLabel="Sin desglose de fuentes"
          rowKey={(row, index) => `${getString(row, "source")}-${getString(row, "metric_scope")}-${index}`}
          rows={sourceRows}
        />
      </Panel>
    </div>
  );
}

function ControlBlock({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="intel-control-block">
      <span>{label}</span>
      {children}
    </label>
  );
}

function IntelStat({ label, value, tone = "neutral" }: { label: string; value: string; tone?: string }) {
  return (
    <div className={`intel-stat ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function parseOption<T extends string>(value: string | null, options: readonly T[], fallback: T): T {
  return value && (options as readonly string[]).includes(value) ? (value as T) : fallback;
}

function matchesFilters(row: RowRecord, scopeFilter: ScopeFilter, actionFilter: ActionFilter): boolean {
  const rowScope = getString(row, "metric_scope").toLowerCase();
  const rowAction = getString(row, "decision").toUpperCase();
  if (scopeFilter !== "todos" && rowScope !== scopeFilter) return false;
  if (actionFilter !== "todas" && rowAction !== actionFilter) return false;
  return true;
}

function hasNonZeroScore(row: RowRecord): boolean {
  const score = getNumber(row, "final_score");
  return score !== null && Math.abs(score) > 0.000001;
}

function hasZeroScore(row: RowRecord): boolean {
  const score = getNumber(row, "final_score");
  return score !== null && Math.abs(score) <= 0.000001;
}

function matchesScoreFilter(row: RowRecord, scoreFilter: ScoreFilter): boolean {
  if (scoreFilter === "todos") return true;
  if (scoreFilter === "score_0") return hasZeroScore(row);
  return hasNonZeroScore(row);
}

function average(rows: RowRecord[], key: string): number | null {
  const values = rows.map((row) => getNumber(row, key)).filter((value): value is number => value !== null);
  if (!values.length) return null;
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function winRate(rows: RowRecord[], key: string): number | null {
  const values = rows.map((row) => getNumber(row, key)).filter((value): value is number => value !== null);
  if (!values.length) return null;
  return values.filter((value) => value > 0).length / values.length;
}

function viewLabel(view: IntelView): string {
  if (view === "score") return "Score";
  if (view === "fuentes") return "Fuentes";
  if (view === "senales") return "Señales";
  if (view === "tickers") return "Tickers";
  return "Recientes";
}

function viewTitle(view: IntelView): string {
  if (view === "score") return "Dispersion score vs outcome";
  if (view === "fuentes") return "Resultado por fuente";
  if (view === "senales") return "Resultado por familia de señal";
  if (view === "tickers") return "Resultado por ticker";
  return "Planes recientes del bot";
}

function viewKicker(view: IntelView): string {
  if (view === "score") return "Score points";
  if (view === "fuentes") return "Fuente / alcance";
  if (view === "senales") return "Familia de señal";
  if (view === "tickers") return "Top por muestra";
  return "Ultimos registros";
}

function filterScopeLabel(scope: ScopeFilter): string {
  if (scope === "todos") return "Todos";
  return scopeLabel(scope);
}

function filterActionLabel(action: ActionFilter): string {
  if (action === "todas") return "Todas";
  return decisionLabel(action);
}

function filterScoreLabel(scoreFilter: ScoreFilter): string {
  if (scoreFilter === "con_score") return "Con score";
  if (scoreFilter === "score_0") return "Score 0";
  return "Todos";
}

function signalFamilyLabel(value: string): string {
  return value.replace(/_/g, " ").toLowerCase();
}

const directionColumns: TableColumn<RowRecord>[] = [
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { align: "right", header: "Total", id: "total", render: (row) => formatNumber(getNumber(row, "total")) },
  { align: "right", header: "Cerradas", id: "closed", render: (row) => formatNumber(getNumber(row, "closed_5d")) },
  { align: "right", header: "Pendientes", id: "pending", render: (row) => formatNumber(getNumber(row, "pending_5d")) },
  { align: "right", header: "Acierto", id: "win", render: (row) => formatPercent(getNumber(row, "win_rate_5d")) },
  { align: "right", header: "Avg 5D", id: "avg", render: (row) => <span className={toneForNumber(getNumber(row, "avg_5d"))}>{formatPercent(getNumber(row, "avg_5d"), 1, true)}</span> },
  { align: "right", header: "Payoff", id: "payoff", render: (row) => formatNumber(getNumber(row, "payoff_ratio")) },
];

const sourceColumns: TableColumn<RowRecord>[] = [
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")) },
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge> },
  { align: "right", header: "Total", id: "total", render: (row) => formatNumber(getNumber(row, "total")) },
  { align: "right", header: "Cerradas", id: "closed", render: (row) => formatNumber(getNumber(row, "closed_5d")) },
  { align: "right", header: "Acierto", id: "win", render: (row) => formatPercent(getNumber(row, "win_rate_5d")) },
  { align: "right", header: "Avg 5D", id: "avg", render: (row) => <span className={toneForNumber(getNumber(row, "avg_5d"))}>{formatPercent(getNumber(row, "avg_5d"), 1, true)}</span> },
  { align: "right", header: "Peor", id: "worst", render: (row) => <span className={toneForNumber(getNumber(row, "worst_5d"))}>{formatPercent(getNumber(row, "worst_5d"), 1, true)}</span> },
  { align: "right", header: "Mejor", id: "best", render: (row) => <span className={toneForNumber(getNumber(row, "best_5d"))}>{formatPercent(getNumber(row, "best_5d"), 1, true)}</span> },
];

const scoreColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")), sortValue: (row) => getString(row, "decided_at") },
  { header: "Ticker", id: "ticker", render: (row) => <span className="ticker">{getString(row, "ticker", "-")}</span>, sortValue: (row) => getString(row, "ticker") },
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")), sortValue: (row) => getString(row, "decision") },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge>, sortValue: (row) => getString(row, "status") },
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge>, sortValue: (row) => getString(row, "metric_scope") },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")), sortValue: (row) => getString(row, "source") },
  { align: "right", header: "Score", id: "score", render: (row) => formatScore(getNumber(row, "final_score")), sortValue: (row) => getNumber(row, "final_score") },
  { align: "right", header: "Conf.", id: "confidence", render: (row) => formatPercent(getNumber(row, "confidence")), sortValue: (row) => getNumber(row, "confidence") },
  { align: "right", header: "5D", id: "outcome5", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_5d") },
  { align: "right", header: "10D", id: "outcome10", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_10d"))}>{formatPercent(getNumber(row, "outcome_10d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_10d") },
  { align: "right", header: "20D", id: "outcome20", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_20d"))}>{formatPercent(getNumber(row, "outcome_20d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_20d") },
];

const recentPlanColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")), sortValue: (row) => getString(row, "decided_at") },
  { header: "Ticker", id: "ticker", render: (row) => <span className="ticker">{getString(row, "ticker", "-")}</span>, sortValue: (row) => getString(row, "ticker") },
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")), sortValue: (row) => getString(row, "decision") },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge>, sortValue: (row) => getString(row, "status") },
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge>, sortValue: (row) => getString(row, "metric_scope") },
  { header: "Señal", id: "signal", render: (row) => signalFamilyLabel(getString(row, "signal_family", "-")), sortValue: (row) => getString(row, "signal_family") },
  { header: "Confirm.", id: "buyConfirmation", render: (row) => getString(row, "buy_confirmation", "-"), sortValue: (row) => getString(row, "buy_confirmation") },
  { header: "Regimen", id: "regime", render: (row) => getString(row, "trend_shadow_regime", "-"), sortValue: (row) => getString(row, "trend_shadow_regime") },
  { align: "right", header: "Score", id: "score", render: (row) => formatScore(getNumber(row, "final_score")), sortValue: (row) => getNumber(row, "final_score") },
  { align: "right", header: "Dir. 5D", id: "directional", render: (row) => <span className={toneForNumber(getNumber(row, "directional_5d"))}>{formatPercent(getNumber(row, "directional_5d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "directional_5d") },
  { align: "right", header: "Outcome", id: "outcome", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_5d") },
];
