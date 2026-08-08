import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { PageHeader } from "../components/layout/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/feedback/States";
import { HorizontalBars } from "../components/charts/MiniCharts";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { StatusBadge } from "../components/ui/StatusBadge";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { useRadarQuery } from "../hooks/useMonitorData";
import type { RowRecord, Tone } from "../types/api";
import { asRows, getNumber, getString } from "../utils/data";
import { formatDateTime, formatMoney, formatNumber, formatPercent, formatScore, sampleLabel, toneForNumber } from "../utils/format";
import { decisionLabel, statusLabel, toneForStatus } from "../utils/labels";

const filters = ["todas", "operables", "vigilar", "teoricas", "bloqueadas"] as const;
const scoreFilters = ["con_score", "score_0", "todos"] as const;
const periods = [30, 90, 180, 365] as const;
const sampleThreshold = 30;

type OpportunityFilter = (typeof filters)[number];
type ScoreFilter = (typeof scoreFilters)[number];

function parsePeriod(value: string | null): (typeof periods)[number] {
  const parsed = Number(value);
  return periods.includes(parsed as (typeof periods)[number]) ? (parsed as (typeof periods)[number]) : 90;
}

function radarStatus(row: RowRecord): string {
  return (getString(row, "candidate_status") || getString(row, "status")).toUpperCase();
}

function opportunityBucket(row: RowRecord): OpportunityFilter {
  const status = radarStatus(row);
  const risk = getString(row, "path_risk").toUpperCase();
  if (status === "BLOCKED" || risk === "HIGH") return "bloqueadas";
  if (status === "THEORETICAL") return "teoricas";
  if (status === "COMPRABLE_AHORA" || status === "COMPRA_HABILITADA" || status === "EXECUTABLE" || status === "APPROVED") return "operables";
  if (status === "SWAP_CANDIDATO" || status.startsWith("VIGILANCIA")) return "vigilar";
  return "vigilar";
}

function radarStatusLabel(value: unknown): string {
  const key = String(value || "").toUpperCase();
  if (key === "COMPRABLE_AHORA") return "Comprable ahora";
  if (key === "COMPRA_HABILITADA") return "Compra habilitada";
  if (key === "SWAP_CANDIDATO") return "Swap candidato";
  if (key === "VIGILANCIA_A") return "Vigilancia A";
  if (key === "VIGILANCIA_B") return "Vigilancia B";
  if (key === "VIGILANCIA_C") return "Vigilancia C";
  return statusLabel(value);
}

function toneForRadarStatus(value: unknown): Tone {
  const key = String(value || "").toUpperCase();
  if (key === "COMPRABLE_AHORA" || key === "COMPRA_HABILITADA") return "real";
  if (key === "SWAP_CANDIDATO" || key.startsWith("VIGILANCIA")) return "info";
  return toneForStatus(value);
}

function closedRows(rows: RowRecord[], key: string): RowRecord[] {
  return rows.filter((row) => getNumber(row, key) !== null);
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

function mean(values: Array<number | null>): number | null {
  const finite = values.filter((value): value is number => value !== null && Number.isFinite(value));
  if (!finite.length) return null;
  return finite.reduce((acc, value) => acc + value, 0) / finite.length;
}

function formatRatio(value: unknown): string {
  const numeric = typeof value === "number" && Number.isFinite(value) ? value : Number(value);
  return Number.isFinite(numeric) ? numeric.toFixed(1) : "-";
}

function riskLabel(value: string): string {
  if (value === "HIGH") return "Riesgo alto";
  if (value === "MEDIUM") return "Riesgo medio";
  if (value === "LOW") return "Riesgo bajo";
  return "Sin path";
}

function riskTone(value: string): Tone {
  if (value === "HIGH") return "warning";
  if (value === "MEDIUM") return "info";
  if (value === "LOW") return "positive";
  return "pending";
}

function riskBuckets(rows: RowRecord[]) {
  return ["LOW", "MEDIUM", "HIGH", "PENDING"].map((risk) => {
    const subset = rows.filter((row) => (getString(row, "path_risk", "PENDING").toUpperCase() || "PENDING") === risk);
    const closed10 = closedRows(subset, "outcome_10d");
    return {
      avg10: mean(closed10.map((row) => getNumber(row, "outcome_10d"))),
      avgMae: mean(subset.map((row) => getNumber(row, "mae_10d"))),
      closed10: closed10.length,
      label: riskLabel(risk),
      risk,
      total: subset.length,
    };
  }).filter((bucket) => bucket.total > 0);
}

function winRate(rows: RowRecord[], key: string): number | null {
  const values = rows.map((row) => getNumber(row, key)).filter((value): value is number => value !== null);
  if (!values.length) return null;
  return values.filter((value) => value > 0).length / values.length;
}

function scoreDomain(values: number[], fallback: number): [number, number] {
  if (!values.length) return [-fallback, fallback];
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const pad = Math.max((max - min) * 0.18, fallback);
  return [min - pad, max + pad];
}

function scale(value: number, min: number, max: number, start: number, end: number): number {
  const span = Math.max(0.000001, max - min);
  return start + ((value - min) / span) * (end - start);
}

function RadarOutcomeScatter({ rows }: { rows: RowRecord[] }) {
  const points = rows
    .map((row) => ({
      label: getString(row, "ticker", "-"),
      risk: getString(row, "path_risk", "PENDING").toLowerCase(),
      status: radarStatusLabel(radarStatus(row)),
      x: getNumber(row, "outcome_2d"),
      y: getNumber(row, "outcome_10d"),
    }))
    .filter((point): point is { label: string; risk: string; status: string; x: number; y: number } => point.x !== null && point.y !== null);

  if (points.length < 3) return <div className="chart-empty">Muestra limitada: faltan outcomes 2D/10D para graficar.</div>;

  const width = 760;
  const height = 300;
  const pad = { bottom: 38, left: 48, right: 28, top: 24 };
  const [minX, maxX] = scoreDomain(points.map((point) => point.x), 0.015);
  const [minY, maxY] = scoreDomain(points.map((point) => point.y), 0.02);
  const zeroX = scale(0, minX, maxX, pad.left, width - pad.right);
  const zeroY = scale(0, minY, maxY, height - pad.bottom, pad.top);
  const titleId = "radar-outcome-scatter-title";
  const descId = "radar-outcome-scatter-desc";

  return (
    <svg className="chart-surface radar-outcome-scatter" viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby={`${titleId} ${descId}`}>
      <title id={titleId}>Radar audit: outcome 2D contra 10D</title>
      <desc id={descId}>Cada punto es una idea radar con outcome 2D y 10D cerrado. No entra al EV operativo.</desc>
      <path className="chart-axis" d={`M${pad.left},${zeroY} L${width - pad.right},${zeroY}`} />
      <path className="chart-axis muted" d={`M${zeroX},${pad.top} L${zeroX},${height - pad.bottom}`} />
      <text x={pad.left} y={height - 10}>2D {formatPercent(minX, 1, true)}</text>
      <text x={width - 126} y={height - 10}>2D {formatPercent(maxX, 1, true)}</text>
      <text x={8} y={pad.top + 4}>10D {formatPercent(maxY, 1, true)}</text>
      <text x={8} y={height - pad.bottom}>10D {formatPercent(minY, 1, true)}</text>
      {points.map((point, index) => {
        const x = scale(point.x, minX, maxX, pad.left, width - pad.right);
        const y = scale(point.y, minY, maxY, height - pad.bottom, pad.top);
        return (
          <circle
            className={`radar-dot risk-${point.risk} ${point.y >= 0 ? "positive" : "negative"}`}
            cx={x}
            cy={y}
            key={`${point.label}-${index}-${point.x}-${point.y}`}
            r="5.5"
          >
            <title>{`${point.label} | ${point.status} | 2D ${formatPercent(point.x, 1, true)} | 10D ${formatPercent(point.y, 1, true)}`}</title>
          </circle>
        );
      })}
    </svg>
  );
}

function RadarRanking({ rows }: { rows: RowRecord[] }) {
  const ranked = closedRows(rows, "outcome_10d")
    .slice()
    .sort((a, b) => (getNumber(b, "outcome_10d") ?? 0) - (getNumber(a, "outcome_10d") ?? 0));
  if (!ranked.length) return <EmptyState label="Sin ranking 10D cerrado" detail="El radar necesita velas posteriores para evaluar el camino." />;

  const best = ranked.slice(0, 4);
  const worst = ranked.slice(-4).reverse();

  return (
    <div className="radar-ranking-grid">
      <div>
        <p className="source-note">Mejores 10D</p>
        <div className="radar-ranking-list">
          {best.map((row, index) => <RankingItem key={`best-${getString(row, "ticker")}-${index}`} row={row} />)}
        </div>
      </div>
      <div>
        <p className="source-note">Peores 10D</p>
        <div className="radar-ranking-list">
          {worst.map((row, index) => <RankingItem key={`worst-${getString(row, "ticker")}-${index}`} row={row} />)}
        </div>
      </div>
    </div>
  );
}

function RankingItem({ row }: { row: RowRecord }) {
  const value = getNumber(row, "outcome_10d");
  return (
    <article className="radar-ranking-item">
      <div>
        <strong className="ticker">{getString(row, "ticker")}</strong>
        <span>{radarStatusLabel(radarStatus(row))}</span>
      </div>
      <b className={toneForNumber(value)}>{formatPercent(value, 1, true)}</b>
    </article>
  );
}

export default function OpportunitiesPage() {
  const [params, setParams] = useSearchParams();
  const selected = filters.includes((params.get("estado") || "todas") as OpportunityFilter)
    ? ((params.get("estado") || "todas") as OpportunityFilter)
    : "todas";
  const scoreFilter = scoreFilters.includes((params.get("score") || "con_score") as ScoreFilter)
    ? ((params.get("score") || "con_score") as ScoreFilter)
    : "con_score";
  const period = parsePeriod(params.get("period"));
  const radar = useRadarQuery(period);
  const rows = asRows(radar.data?.chart_items);
  const recentRows = asRows(radar.data?.recent);
  const displayRows = rows.length ? rows : recentRows;
  const scoreFilteredRows = useMemo(
    () => displayRows.filter((row) => matchesScoreFilter(row, scoreFilter)),
    [displayRows, scoreFilter],
  );
  const filtered = useMemo(
    () => (selected === "todas" ? scoreFilteredRows : scoreFilteredRows.filter((row) => opportunityBucket(row) === selected)),
    [scoreFilteredRows, selected],
  );
  const scoreZeroCount = useMemo(() => displayRows.filter(hasZeroScore).length, [displayRows]);
  const closed5Rows = useMemo(() => closedRows(scoreFilteredRows, "outcome_5d"), [scoreFilteredRows]);
  const closed10Rows = useMemo(() => closedRows(scoreFilteredRows, "outcome_10d"), [scoreFilteredRows]);
  const closed5 = closed5Rows.length;
  const closed10 = closed10Rows.length;
  const avg5 = mean(closed5Rows.map((row) => getNumber(row, "outcome_5d")));
  const win5 = winRate(closed5Rows, "outcome_5d");
  const highPathRisk = scoreFilteredRows.filter((row) => getString(row, "path_risk", "PENDING").toUpperCase() === "HIGH").length;
  const avgMae = mean(scoreFilteredRows.map((row) => getNumber(row, "mae_10d")));
  const avgMfe = mean(scoreFilteredRows.map((row) => getNumber(row, "mfe_10d")));
  const buckets = riskBuckets(scoreFilteredRows);

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    next.set(key, value);
    setParams(next, { replace: true });
  };

  if (radar.isLoading && !radar.data) return <LoadingState label="Cargando radar audit" />;
  if (radar.isError) return <ErrorState message="No se pudo cargar /api/radar-audit" onRetry={() => radar.refetch()} />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Radar audit"
        title="Ideas teoricas, outcomes y riesgo de camino"
        description="El radar descubre oportunidades. Esta pantalla audita si esas ideas tuvieron resultado posterior, sin mezclarlas con fills reales ni con el EV operativo del planner."
      />

      <MetricGroup>
        <Metric
          label="Ideas radar"
          value={formatNumber(scoreFilteredRows.length)}
          detail={scoreFilter === "con_score" ? `${period} dias; ${formatNumber(scoreZeroCount)} score 0 fuera` : `${period} dias auditados`}
        />
        <Metric
          label="Outcomes 5D"
          value={formatNumber(closed5)}
          detail={sampleLabel(closed5, sampleThreshold)}
          tone={closed5 >= sampleThreshold ? "real" : "warning"}
        />
        <Metric label="Avg 5D" value={formatPercent(avg5, 1, true)} detail={`Win ${formatPercent(win5)}`} tone={toneForNumber(avg5)} />
        <Metric label="Path alto" value={formatNumber(highPathRisk)} detail={`10D cerrados: ${formatNumber(closed10)}`} tone="warning" />
      </MetricGroup>

      <Panel
        action={
          <div className="radar-toolbar">
            <div className="segmented-control" aria-label="Periodo radar">
              {periods.map((item) => (
                <button className={period === item ? "active" : ""} key={item} onClick={() => setParam("period", String(item))} type="button">
                  {item}d
                </button>
              ))}
            </div>
            <div className="segmented-control" aria-label="Filtro de score radar">
              {scoreFilters.map((filter) => (
                <button className={scoreFilter === filter ? "active" : ""} key={filter} onClick={() => setParam("score", filter)} type="button">
                  {scoreFilterLabel(filter)}
                </button>
              ))}
            </div>
            <div className="segmented-control" aria-label="Filtro de oportunidades">
              {filters.map((filter) => (
                <button className={selected === filter ? "active" : ""} key={filter} onClick={() => setParam("estado", filter)} type="button">
                  {filter}
                </button>
              ))}
            </div>
          </div>
        }
        kicker="poblacion radar_audit"
        title="Muestra y controles"
      >
        <div className="audit-note">
          <strong>Separacion obligatoria</strong>
          <span>El default no cuenta `score=0` porque mezcla filas auditables sin score real. Usalo con el filtro "Score 0" solo para investigar de donde vienen, no para calibrar el radar.</span>
        </div>
      </Panel>

      <div className="panel-grid two radar-audit-layout">
        <Panel kicker="outcome path" title="Outcome 2D contra 10D">
          <RadarOutcomeScatter rows={scoreFilteredRows} />
          <p className="table-note">Un punto arriba de cero en 10D favorecio la direccion propuesta por radar. El color del borde marca riesgo de camino cuando esta disponible.</p>
        </Panel>

        <Panel kicker="riesgo por path" title="Buckets de MAE/MFE">
          <HorizontalBars
            description="Cantidad de ideas radar por bucket de path risk, con promedio 10D cuando hay muestra."
            rows={buckets.map((bucket) => ({
              display: `${formatNumber(bucket.total)} | 10D ${formatPercent(bucket.avg10, 1, true)} | n ${formatNumber(bucket.closed10)}`,
              label: bucket.label,
              tone: riskTone(bucket.risk),
              value: bucket.total,
            }))}
            title="Radar por riesgo de camino"
          />
          <p className="table-note">MAE promedio: {formatPercent(avgMae, 1, true)}. MFE promedio: {formatPercent(avgMfe, 1, true)}.</p>
        </Panel>
      </div>

      <Panel kicker="ranking" title="Mejores y peores ideas cerradas">
        <RadarRanking rows={scoreFilteredRows} />
      </Panel>

      <Panel kicker="ideas" title="Oportunidades auditables">
        {filtered.length ? (
          <div className="opportunity-grid">
            {filtered.slice(0, 24).map((row, index) => (
              <article className="opportunity-item" key={`${getString(row, "ticker")}-${getString(row, "decided_at")}-${index}`}>
                <div className="item-heading">
                  <strong className="ticker">{getString(row, "ticker")}</strong>
                  <StatusBadge tone={toneForRadarStatus(radarStatus(row))}>{radarStatusLabel(radarStatus(row))}</StatusBadge>
                  <StatusBadge tone={riskTone(getString(row, "path_risk", "PENDING").toUpperCase())}>{riskLabel(getString(row, "path_risk", "PENDING").toUpperCase())}</StatusBadge>
                </div>
                <p>{getString(row, "edge_label") || getString(row, "block_reason") || "Idea sin razon textual expuesta por API."}</p>
                <dl className="compact-dl">
                  <div>
                    <dt>Accion</dt>
                    <dd>{decisionLabel(getString(row, "decision"))}</dd>
                  </div>
                  <div>
                    <dt>Score</dt>
                    <dd>{formatScore(getNumber(row, "final_score"))}</dd>
                  </div>
                  <div>
                    <dt>R/R</dt>
                    <dd>{formatRatio(getNumber(row, "rr_ratio"))}</dd>
                  </div>
                  <div>
                    <dt>Entrada audit</dt>
                    <dd>{formatMoney(getNumber(row, "audit_entry_price") ?? getNumber(row, "price_at_decision"))}</dd>
                  </div>
                  <div>
                    <dt>2D / 10D</dt>
                    <dd>
                      <span className={toneForNumber(getNumber(row, "outcome_2d"))}>{formatPercent(getNumber(row, "outcome_2d"), 1, true)}</span>
                      {" / "}
                      <span className={toneForNumber(getNumber(row, "outcome_10d"))}>{formatPercent(getNumber(row, "outcome_10d"), 1, true)}</span>
                    </dd>
                  </div>
                  <div>
                    <dt>Fecha</dt>
                    <dd>{formatDateTime(getString(row, "decided_at"))}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        ) : (
          <EmptyState label="No hay oportunidades para este filtro" />
        )}
      </Panel>

      <Panel kicker="tabla audit" title="Outcomes del radar">
        <ResponsiveTable
          columns={columns}
          emptyLabel="Sin outcomes cerrados"
          rowKey={(row, index) => `${getString(row, "ticker")}-${getString(row, "decided_at")}-${index}`}
          rows={scoreFilteredRows}
        />
        <p className="table-note">Radar Audit es teorico. Si una idea termina en compra/venta real, debe auditarse tambien por fills/movements y no solo por esta tabla.</p>
      </Panel>
    </div>
  );
}

const columns: TableColumn<RowRecord>[] = [
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong>, sortValue: (row) => getString(row, "ticker") },
  { header: "Estado", id: "status", render: (row) => radarStatusLabel(radarStatus(row)), sortValue: (row) => radarStatus(row) },
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")), sortValue: (row) => getString(row, "decision") },
  { header: "Score", id: "score", align: "right", render: (row) => formatScore(getNumber(row, "final_score")), sortValue: (row) => getNumber(row, "final_score") },
  { header: "2D", id: "outcome_2d", align: "right", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_2d"))}>{formatPercent(getNumber(row, "outcome_2d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_2d") },
  { header: "5D", id: "outcome_5d", align: "right", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_5d") },
  { header: "10D", id: "outcome_10d", align: "right", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_10d"))}>{formatPercent(getNumber(row, "outcome_10d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_10d") },
  { header: "20D", id: "outcome_20d", align: "right", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_20d"))}>{formatPercent(getNumber(row, "outcome_20d"), 1, true)}</span>, sortValue: (row) => getNumber(row, "outcome_20d") },
  { header: "MAE/MFE", id: "path", render: (row) => `${formatPercent(getNumber(row, "mae_10d"), 1, true)} / ${formatPercent(getNumber(row, "mfe_10d"), 1, true)}` },
  { header: "Riesgo", id: "risk", render: (row) => riskLabel(getString(row, "path_risk", "PENDING").toUpperCase()), sortValue: (row) => getString(row, "path_risk") },
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")), sortValue: (row) => getString(row, "decided_at") },
];

function scoreFilterLabel(filter: ScoreFilter): string {
  if (filter === "con_score") return "Con score";
  if (filter === "score_0") return "Score 0";
  return "Todos";
}
