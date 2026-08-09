import { useSearchParams } from "react-router-dom";
import { AuditTimeline } from "../components/audit/AuditTimeline";
import { PageHeader } from "../components/layout/PageHeader";
import { LoadingState } from "../components/feedback/States";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { PERIODS } from "../services/monitorApi";
import { useAuditTimelineQuery, useDecisionsQuery, useFillsQuery, useOverrideQuery, usePeriodParam } from "../hooks/useMonitorData";
import type { RowRecord, Tone } from "../types/api";
import { asRows, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatMoney, formatNumber, formatPercent, formatScore, toneForNumber } from "../utils/format";
import { decisionLabel, scopeLabel, sourceLabel, statusLabel, toneForScope, toneForStatus } from "../utils/labels";

const movementLimitOptions = [40, 80, 160, 300] as const;
type MovementLimit = (typeof movementLimitOptions)[number];

type DecisionBucket = {
  key: string;
  label: string;
  includes: string;
  n: number;
  closed5d: number;
  tone: Tone;
};

const bucketDefinitions: Array<Omit<DecisionBucket, "n" | "closed5d">> = [
  {
    includes: "Filas manuales materializadas en decision_log; el historial broker completo esta abajo.",
    key: "manual",
    label: "Ejecutado por el usuario",
    tone: "real",
  },
  {
    includes: "Planes de Quantia que terminaron con fill real.",
    key: "botExecuted",
    label: "Plan de Quantia ejecutado",
    tone: "real",
  },
  {
    includes: "Planes aprobados sin fill real primario; pueden tener outcome audit-only.",
    key: "botPending",
    label: "Plan aprobado auditado",
    tone: "info",
  },
  {
    includes: "Ordenes rechazadas por reglas; pueden tener outcome audit-only.",
    key: "blocked",
    label: "Bloqueado por reglas",
    tone: "blocked",
  },
  {
    includes: "Radar, optimizer o debug: ideas para auditar, no ejecuciones.",
    key: "radar",
    label: "Teorico / radar",
    tone: "theoretical",
  },
  {
    includes: "Filas historicas o estados no clasificados todavia.",
    key: "other",
    label: "Otros registros",
    tone: "pending",
  },
];

export default function DecisionsPage() {
  const [period, setPeriod] = usePeriodParam(180);
  const [searchParams, setSearchParams] = useSearchParams();
  const movementLimit = parseMovementLimit(searchParams.get("limite"));
  const movementSearch = searchParams.get("movimiento") || "";
  const decisions = useDecisionsQuery(period);
  const override = useOverrideQuery(period);
  const fills = useFillsQuery(period, movementLimit);
  const timeline = useAuditTimelineQuery(period, 120);
  const summary = getRecord(decisions.data, "summary");
  const fillSummary = getRecord(fills.data, "summary");
  const movementPayload = getRecord(fills.data, "movements");
  const movementSummary = getRecord(movementPayload, "summary");
  const movementRows = asRows(movementPayload.recent);
  const recent = asRows(decisions.data?.recent);
  const overrideRows = asRows(override.data?.recent);
  const readableGroups = summarizeDecisionGroups(asRows(decisions.data?.groups));
  const filteredMovements = filterMovementRows(movementRows, movementSearch);

  const setUrlParam = (key: string, value: string, fallback = "") => {
    const next = new URLSearchParams(searchParams);
    if (!value || value === fallback) next.delete(key);
    else next.set(key, value);
    setSearchParams(next, { replace: true });
  };

  if (decisions.isLoading && !decisions.data) return <LoadingState label="Cargando decisiones" />;

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
        eyebrow="Decisiones"
        title="Decisiones y ejecuciones reales"
        description="Separa planes de Quantia, bloqueos, radar y movimientos ejecutados por el usuario."
      />

      <MetricGroup>
        <Metric detail="decision_log" label="Registros" value={formatNumber(getNumber(summary, "total"))} />
        <Metric detail="execution_plan" label="Aprobadas bot" tone="info" value={formatNumber(getNumber(summary, "approved"))} />
        <Metric detail="broker_movements" label="Trades usuario" tone="real" value={formatNumber(getNumber(movementSummary, "trades"))} />
        <Metric detail="broker_fills" label="Fills reconciliados" tone="real" value={formatNumber(getNumber(fillSummary, "reconciled"))} />
      </MetricGroup>

      <section className="ledger-rail" aria-label="Etapas de decision">
        <div>
          <span>01</span>
          <strong>Decision</strong>
          <small>Senal registrada</small>
        </div>
        <div>
          <span>02</span>
          <strong>Plan</strong>
          <small>Aprobado o bloqueado</small>
        </div>
        <div>
          <span>03</span>
          <strong>Movimiento</strong>
          <small>Movimiento real Cocos</small>
        </div>
        <div>
          <span>04</span>
          <strong>Fill</strong>
          <small>Ejecucion reconciliada</small>
        </div>
        <div>
          <span>05</span>
          <strong>Outcome</strong>
          <small>5/10/20/40 ruedas</small>
        </div>
      </section>

      <AuditTimeline
        data={timeline.data}
        error={timeline.error instanceof Error ? timeline.error.message : ""}
        isLoading={timeline.isLoading}
        onRetry={() => void timeline.refetch()}
      />

      <Panel kicker="Lectura rapida" title="Que significa cada grupo">
        <ResponsiveTable
          columns={bucketColumns}
          emptyLabel="Sin grupos para clasificar"
          rowKey={(row) => row.key}
          rows={readableGroups}
        />
        <details className="technical-details">
          <summary>Ver agrupacion tecnica completa</summary>
          <ResponsiveTable
            columns={groupColumns}
            emptyLabel="Sin grupos de decision"
            rowKey={(row, index) => `${getString(row, "metric_scope")}-${getString(row, "source")}-${index}`}
            rows={asRows(decisions.data?.groups)}
          />
        </details>
      </Panel>

      <Panel className="movement-history-panel" kicker="broker_movements" title="Movimientos ejecutados por el usuario">
        <div className="table-toolbar">
          <label className="table-search">
            <span>Filtrar ticker</span>
            <input
              value={movementSearch}
              onChange={(event) => setUrlParam("movimiento", event.currentTarget.value.trim().toUpperCase())}
              placeholder="Ej: YPF, VST, SPY"
            />
          </label>
          <div className="table-control">
            <span>Filas</span>
            <div className="segmented-control" aria-label="Cantidad de movimientos">
              {movementLimitOptions.map((item) => (
                <button className={movementLimit === item ? "active" : ""} key={item} onClick={() => setUrlParam("limite", String(item), "160")} type="button">
                  {item}
                </button>
              ))}
            </div>
          </div>
          <dl className="movement-facts" aria-label="Resumen de movimientos reales">
            <div>
              <dt>Total</dt>
              <dd>{formatNumber(getNumber(movementSummary, "total"))}</dd>
            </div>
            <div>
              <dt>Trades</dt>
              <dd>{formatNumber(getNumber(movementSummary, "trades"))}</dd>
            </div>
            <div>
              <dt>Ultimo</dt>
              <dd>{formatDateTime(getString(movementSummary, "latest_executed_at"))}</dd>
            </div>
          </dl>
        </div>
        <ResponsiveTable
          columns={movementColumns}
          emptyLabel="Sin movimientos reales para el filtro"
          rowKey={(row, index) => `${getString(row, "executed_at")}-${getString(row, "ticker")}-${getString(row, "movement_type")}-${index}`}
          rows={filteredMovements}
        />
        <p className="table-note">Fuente: tabla broker_movements. Estos son movimientos reales detectados en Cocos, no recomendaciones del bot.</p>
      </Panel>

      <div className="panel-grid two">
        <Panel kicker="Bot vs usuario" title="Planes comparados contra movimientos">
          <ResponsiveTable
            columns={overrideColumns}
            emptyLabel="Sin comparaciones recientes"
            rowKey={(row, index) => `${getString(row, "ticker")}-${getString(row, "decided_at")}-${index}`}
            rows={overrideRows.slice(0, 12)}
          />
        </Panel>

        <Panel kicker="decision_log" title="Historial de decisiones">
          <ResponsiveTable
            columns={recentColumns}
            emptyLabel="Sin decisiones recientes"
            rowKey={(row, index) => `${getString(row, "ticker")}-${getString(row, "decided_at")}-${index}`}
            rows={recent}
          />
        </Panel>
      </div>
    </div>
  );
}

function parseMovementLimit(value: string | null): MovementLimit {
  const parsed = Number(value);
  return movementLimitOptions.includes(parsed as MovementLimit) ? (parsed as MovementLimit) : 160;
}

function filterMovementRows(rows: RowRecord[], search: string): RowRecord[] {
  const normalized = search.trim().toUpperCase();
  if (!normalized) return rows;
  return rows.filter((row) => getString(row, "ticker").toUpperCase().includes(normalized));
}

function summarizeDecisionGroups(rows: RowRecord[]): DecisionBucket[] {
  const buckets = new Map<string, DecisionBucket>();
  for (const definition of bucketDefinitions) {
    buckets.set(definition.key, { ...definition, closed5d: 0, n: 0 });
  }

  for (const row of rows) {
    const bucketKey = bucketForGroup(row);
    const bucket = buckets.get(bucketKey) ?? buckets.get("other");
    if (!bucket) continue;
    bucket.n += getNumber(row, "n") ?? 0;
    bucket.closed5d += getNumber(row, "con_5d") ?? 0;
  }

  return Array.from(buckets.values()).filter((bucket) => bucket.n > 0);
}

function bucketForGroup(row: RowRecord): string {
  const source = getString(row, "source").toLowerCase();
  const status = getString(row, "status").toUpperCase();
  const scope = getString(row, "metric_scope").toLowerCase();
  if (source === "broker_movement" || source === "broker_fill" || status === "EXECUTED_MANUAL") return "manual";
  if (source === "execution_plan" && status === "EXECUTED") return "botExecuted";
  if (source === "execution_plan" && status === "APPROVED") return "botPending";
  if (status === "BLOCKED" || scope === "blocked_audit") return "blocked";
  if (source === "radar" || source === "optimizer" || scope === "radar_audit" || scope === "debug" || status === "THEORETICAL") return "radar";
  return "other";
}

function formatQuantity(value: unknown): string {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return new Intl.NumberFormat("es-AR", { maximumFractionDigits: 4 }).format(numeric);
}

function formatDateOnly(value: unknown): string {
  if (!value) return "-";
  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleDateString("es-AR", { day: "2-digit", month: "2-digit", year: "2-digit" });
}

const bucketColumns: TableColumn<DecisionBucket>[] = [
  { header: "Grupo", id: "label", render: (row) => <StatusBadge tone={row.tone}>{row.label}</StatusBadge> },
  { header: "Incluye", id: "includes", render: (row) => row.includes },
  { align: "right", header: "N", id: "n", render: (row) => formatNumber(row.n) },
  { align: "right", header: "Con 5D", id: "closed5d", render: (row) => formatNumber(row.closed5d) },
];

const movementColumns: TableColumn<RowRecord>[] = [
  { header: "Ejecucion", id: "executed", render: (row) => formatDateTime(getString(row, "executed_at")) },
  { header: "Liq.", id: "settlement", render: (row) => formatDateOnly(getString(row, "settlement_date")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Lado", id: "side", render: (row) => <StatusBadge tone={getString(row, "movement_type") === "BUY" ? "real" : "info"}>{decisionLabel(getString(row, "movement_type"))}</StatusBadge> },
  { align: "right", header: "Cantidad", id: "quantity", render: (row) => formatQuantity(getNumber(row, "quantity")) },
  { align: "right", header: "Precio", id: "price", render: (row) => formatMoney(getNumber(row, "price")) },
  { align: "right", header: "Monto", id: "amount", render: (row) => formatMoney(getNumber(row, "amount")) },
  { header: "Moneda", id: "currency", render: (row) => getString(row, "currency", "-") },
  { header: "Instrumento", id: "instrument", render: (row) => getString(row, "instrument_type", "-") },
];

const groupColumns: TableColumn<RowRecord>[] = [
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge> },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge> },
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { align: "right", header: "N", id: "n", render: (row) => formatNumber(getNumber(row, "n")) },
  { align: "right", header: "5D", id: "con_5d", render: (row) => formatNumber(getNumber(row, "con_5d")) },
];

const overrideColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Quantia", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { header: "Usuario", id: "override", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "override_status"))}>{statusLabel(getString(row, "override_status"))}</StatusBadge> },
  { align: "right", header: "Seguido", id: "same", render: (row) => formatPercent(getNumber(row, "same_ratio")) },
  { align: "right", header: "5D bot", id: "outcome", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span> },
];

const recentColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Accion", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { header: "Estado", id: "status", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "status"))}>{statusLabel(getString(row, "status"))}</StatusBadge> },
  { header: "Alcance", id: "scope", render: (row) => <StatusBadge tone={toneForScope(getString(row, "metric_scope"))}>{scopeLabel(getString(row, "metric_scope"))}</StatusBadge> },
  { header: "Fuente", id: "source", render: (row) => sourceLabel(getString(row, "source")) },
  { align: "right", header: "Score", id: "score", render: (row) => formatScore(getNumber(row, "final_score")) },
  { align: "right", header: "5D", id: "outcome_5d", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span> },
];
