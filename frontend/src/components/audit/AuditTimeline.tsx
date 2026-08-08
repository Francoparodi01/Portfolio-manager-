import {
  AlertTriangle,
  ArrowLeftRight,
  ChartNoAxesCombined,
  ClipboardCheck,
  ListChecks,
  ReceiptText,
  Search,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { useMemo, useState } from "react";
import type { AuditTimelineEvent, AuditTimelinePayload, RowRecord, Tone } from "../../types/api";
import { getBoolean, getNumber, getString } from "../../utils/data";
import { formatDateTime, formatMoney, formatPercent, formatScore, toneForNumber } from "../../utils/format";
import { decisionLabel, statusLabel, toneForStatus } from "../../utils/labels";
import { EmptyState, ErrorState, LoadingState } from "../feedback/States";
import { Panel } from "../ui/Panel";
import { StatusBadge } from "../ui/StatusBadge";

type TimelineStage = "all" | "plan" | "movement" | "fill" | "outcome";

type EventPresentation = {
  Icon: LucideIcon;
  label: string;
  stage: Exclude<TimelineStage, "all"> | "decision";
};

type TimelineFact = {
  label: string;
  value: string;
};

const stageOptions: Array<{ key: TimelineStage; label: string }> = [
  { key: "all", label: "Todo" },
  { key: "plan", label: "Planes" },
  { key: "movement", label: "Movimientos" },
  { key: "fill", label: "Fills" },
  { key: "outcome", label: "Outcomes" },
];

const presentations: Record<string, EventPresentation> = {
  decision_logged: { Icon: ClipboardCheck, label: "Decision registrada", stage: "decision" },
  plan_created: { Icon: ListChecks, label: "Plan generado", stage: "plan" },
  movement_detected: { Icon: ArrowLeftRight, label: "Movimiento real detectado", stage: "movement" },
  fill_detected: { Icon: ReceiptText, label: "Fill reconciliado", stage: "fill" },
  outcome_updated: { Icon: ChartNoAxesCombined, label: "Outcome actualizado", stage: "outcome" },
};

const gapLabels: Record<string, string> = {
  missing_decision_link: "Sin vinculo directo a decision_log",
  missing_feature_snapshot_id: "Sin feature snapshot",
  missing_order_id: "Sin order ID persistido",
  missing_portfolio_snapshot_id: "Sin portfolio snapshot",
  missing_run_context: "Sin contexto de corrida",
  missing_run_id: "Sin run ID",
};

export function AuditTimeline({
  data,
  error,
  isLoading,
  onRetry,
}: {
  data?: AuditTimelinePayload;
  error?: string;
  isLoading: boolean;
  onRetry: () => void;
}) {
  const [ticker, setTicker] = useState("");
  const [stage, setStage] = useState<TimelineStage>("all");
  const events = useMemo(() => {
    const normalizedTicker = ticker.trim().toUpperCase();
    return [...(data?.events ?? [])]
      .sort((left, right) => Date.parse(right.ts) - Date.parse(left.ts))
      .filter((event) => !normalizedTicker || String(event.ticker || "").toUpperCase().includes(normalizedTicker))
      .filter((event) => stage === "all" || eventPresentation(event).stage === stage)
      .slice(0, 120);
  }, [data?.events, stage, ticker]);
  const counts = countEvents(data?.events ?? []);

  return (
    <Panel
      action={<StatusBadge tone="real">{events.length} visibles</StatusBadge>}
      className="audit-timeline-panel"
      kicker="Plan -> movimiento -> fill -> outcome"
      title="Timeline auditable"
    >
      <div className="timeline-toolbar">
        <label className="timeline-search">
          <span>Filtrar ticker</span>
          <div>
            <Search size={15} aria-hidden="true" />
            <input
              onChange={(event) => setTicker(event.currentTarget.value.toUpperCase())}
              placeholder="Ej: YPFD, V, SPY"
              value={ticker}
            />
          </div>
        </label>
        <div className="table-control">
          <span>Etapa</span>
          <div className="segmented-control timeline-stage-control" aria-label="Filtrar etapa del timeline">
            {stageOptions.map((option) => (
              <button
                className={stage === option.key ? "active" : ""}
                key={option.key}
                onClick={() => setStage(option.key)}
                type="button"
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <dl className="timeline-counts" aria-label="Eventos por etapa">
        <TimelineCount label="Planes" value={counts.plan} />
        <TimelineCount label="Movimientos" value={counts.movement} />
        <TimelineCount label="Fills" value={counts.fill} />
        <TimelineCount label="Outcomes" value={counts.outcome} />
      </dl>

      {isLoading && !data ? <LoadingState label="Cargando timeline" /> : null}
      {error && !data ? <ErrorState message={error} onRetry={onRetry} /> : null}
      {!isLoading && !error && !events.length ? (
        <EmptyState label="Sin eventos para estos filtros" detail="El timeline solo muestra evidencia persistida en las fuentes auditables." />
      ) : null}

      {events.length ? (
        <ol className="audit-timeline-list" aria-live="polite">
          {events.map((event) => <TimelineEvent event={event} key={event.event_id} />)}
        </ol>
      ) : null}
      {(data?.events?.length ?? 0) > 120 ? (
        <p className="table-note">La vista muestra hasta 120 eventos filtrados; el endpoint conserva hasta {data?.limit ?? 400}.</p>
      ) : null}
    </Panel>
  );
}

function TimelineCount({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function TimelineEvent({ event }: { event: AuditTimelineEvent }) {
  const presentation = eventPresentation(event);
  const tone = eventTone(event);
  const facts = eventFacts(event);
  const gaps = event.gaps ?? [];
  const Icon = presentation.Icon;

  return (
    <li className="audit-timeline-event" data-tone={tone}>
      <div className="timeline-marker" aria-hidden="true"><Icon size={17} /></div>
      <article className="timeline-event-body">
        <header className="timeline-event-header">
          <div>
            <div className="timeline-event-meta">
              <time dateTime={event.ts}>{formatDateTime(event.ts)}</time>
              {event.ticker ? <strong>{event.ticker}</strong> : null}
              {event.decision_log_id ? <span>decision #{event.decision_log_id}</span> : null}
            </div>
            <h3>{presentation.label}</h3>
            <p>{timelineSourceLabel(event.source)}</p>
          </div>
          <StatusBadge tone={tone}>{stageLabel(presentation.stage)}</StatusBadge>
        </header>

        {facts.length ? (
          <dl className="timeline-facts">
            {facts.map((fact) => (
              <div key={`${event.event_id}-${fact.label}`}>
                <dt>{fact.label}</dt>
                <dd>{fact.value}</dd>
              </div>
            ))}
          </dl>
        ) : null}

        {gaps.length ? (
          <details className="timeline-gaps">
            <summary><AlertTriangle size={14} aria-hidden="true" /> {gaps.length} hueco{gaps.length === 1 ? "" : "s"} de trazabilidad</summary>
            <ul>{gaps.map((gap) => <li key={gap}>{gapLabels[gap] || gap}</li>)}</ul>
          </details>
        ) : (
          <p className="timeline-linked">Vinculo auditable disponible</p>
        )}
      </article>
    </li>
  );
}

function eventPresentation(event: AuditTimelineEvent): EventPresentation {
  return presentations[event.event_type] ?? { Icon: ClipboardCheck, label: event.event_type, stage: "decision" };
}

function eventTone(event: AuditTimelineEvent): Tone {
  const stage = eventPresentation(event).stage;
  const payload = event.payload ?? {};
  if (stage === "movement" || stage === "fill") return "real";
  if (stage === "outcome") return toneForNumber(firstOutcome(payload));
  if (stage === "plan" || stage === "decision") return toneForStatus(getString(payload, "status"));
  return "neutral";
}

function firstOutcome(payload: RowRecord): number | null {
  for (const key of ["outcome_5d", "outcome_10d", "outcome_20d", "outcome_40d"]) {
    const value = getNumber(payload, key);
    if (value !== null) return value;
  }
  return null;
}

function eventFacts(event: AuditTimelineEvent): TimelineFact[] {
  const payload = event.payload ?? {};
  const facts: TimelineFact[] = [];
  const action = getString(payload, "decision") || getString(payload, "side");
  if (action) facts.push({ label: "Accion", value: decisionLabel(action) });

  if (event.event_type === "decision_logged") {
    pushFact(facts, "Estado", statusLabel(getString(payload, "status")));
    pushFact(facts, "Score", formatScore(getNumber(payload, "final_score")));
    pushFact(facts, "Confianza", formatPercent(getNumber(payload, "confidence")));
    pushFact(facts, "Precio base", formatMoney(getNumber(payload, "price_at_decision")));
  }
  if (event.event_type === "plan_created") {
    pushFact(facts, "Estado", statusLabel(getString(payload, "status")));
    pushFact(facts, "Monto plan", formatMoney(getNumber(payload, "theoretical_amount_ars")));
    pushFact(facts, "Monto ejecutado", formatMoney(getNumber(payload, "executed_amount_ars")));
  }
  if (event.event_type === "movement_detected") {
    pushFact(facts, "Cantidad", formatQuantity(getNumber(payload, "quantity")));
    pushFact(facts, "Precio", formatMoney(getNumber(payload, "price")));
    pushFact(facts, "Monto", formatMoney(getNumber(payload, "amount_ars")));
  }
  if (event.event_type === "fill_detected") {
    pushFact(facts, "Cantidad", formatQuantity(getNumber(payload, "quantity")));
    pushFact(facts, "Precio promedio", formatMoney(getNumber(payload, "avg_fill_price")));
    pushFact(facts, "Monto bruto", formatMoney(getNumber(payload, "gross_amount_ars")));
    pushFact(facts, "Comisiones", formatMoney(getNumber(payload, "fees_ars")));
  }
  if (event.event_type === "outcome_updated") {
    for (const [key, label] of [["outcome_5d", "5D"], ["outcome_10d", "10D"], ["outcome_20d", "20D"], ["outcome_40d", "40D"]] as const) {
      const value = getNumber(payload, key);
      if (value !== null) facts.push({ label, value: formatPercent(value, 1, true) });
    }
    pushFact(facts, "Base", getString(payload, "outcome_basis"));
    const primary = getBoolean(payload, "is_primary_metric");
    if (primary !== null) facts.push({ label: "Metrica", value: primary ? "Principal" : "Auditoria" });
  }
  return facts.filter((fact) => fact.value && fact.value !== "-");
}

function pushFact(facts: TimelineFact[], label: string, value: string) {
  if (value && value !== "-") facts.push({ label, value });
}

function countEvents(events: AuditTimelineEvent[]) {
  return events.reduce((counts, event) => {
    const stage = eventPresentation(event).stage;
    if (stage === "plan" || stage === "movement" || stage === "fill" || stage === "outcome") counts[stage] += 1;
    return counts;
  }, { fill: 0, movement: 0, outcome: 0, plan: 0 });
}

function formatQuantity(value: number | null): string {
  if (value === null) return "-";
  return new Intl.NumberFormat("es-AR", { maximumFractionDigits: 4 }).format(value);
}

function stageLabel(stage: EventPresentation["stage"]): string {
  if (stage === "decision") return "Decision";
  if (stage === "movement") return "Movimiento";
  if (stage === "fill") return "Fill";
  if (stage === "outcome") return "Outcome";
  return "Plan";
}

function timelineSourceLabel(source: string): string {
  const key = String(source || "").toLowerCase();
  if (key === "execution_plan") return "Fuente: plan operativo de Quantia";
  if (key === "broker_fill" || key === "cocos") return "Fuente: fill reconciliado de Cocos";
  if (key === "cocos_movements" || key === "broker_movement") return "Fuente: movimiento real observado en Cocos";
  if (key === "decision_log") return "Fuente: resultado persistido en decision_log";
  return `Fuente: ${source || "sin identificar"}`;
}
