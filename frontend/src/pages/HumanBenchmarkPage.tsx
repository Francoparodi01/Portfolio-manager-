import { HorizontalBars } from "../components/charts/MiniCharts";
import { LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { useHumanQuery, useOverrideQuery } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRecord, asRows, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatMoney, formatNumber, formatPercent, toneForNumber } from "../utils/format";
import { decisionLabel, statusLabel, toneForStatus } from "../utils/labels";

function statusRows(summary: RowRecord): Array<{ label: string; value: number; display: string }> {
  const byStatus = asRecord(summary.by_status);
  return Object.entries(byStatus).map(([label, value]) => ({
    display: formatNumber(value),
    label: statusLabel(label),
    value: typeof value === "number" ? value : Number(value) || 0,
  }));
}

export default function HumanBenchmarkPage() {
  const override = useOverrideQuery(90);
  const human = useHumanQuery(7);
  const summary = getRecord(override.data, "summary");
  const humanSummary = getRecord(human.data, "summary");

  if (override.isLoading && !override.data) return <LoadingState label="Cargando benchmark" />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Bot vs humano"
        title="Benchmark de decisiones reales"
        description="Comparación observada entre lo recomendado por Quantia y lo ejecutado por el usuario, sin convertirlo en competencia."
      />

      <MetricGroup>
        <Metric label="Planes" value={formatNumber(getNumber(summary, "plans"))} />
        <Metric label="Cerradas 5D" value={formatNumber(getNumber(summary, "closed_5d"))} />
        <Metric label="Prom. Quantia" tone={toneForNumber(getNumber(summary, "avg_bot_5d"))} value={formatPercent(getNumber(summary, "avg_bot_5d"), 1, true)} />
        <Metric label="Delta observado" tone={toneForNumber(getNumber(summary, "avg_override_delta_5d"))} value={formatPercent(getNumber(summary, "avg_override_delta_5d"), 1, true)} />
      </MetricGroup>

      <div className="panel-grid two">
        <Panel kicker="Clasificación" title="Seguida, ignorada, parcial o contraria">
          <HorizontalBars
            description="Cantidad de planes por estado de override."
            rows={statusRows(summary)}
            title="Estados de comparación"
          />
        </Panel>
        <Panel kicker="Actividad humana" title="Movimientos inferidos">
          <dl className="compact-dl">
            <div>
              <dt>Total</dt>
              <dd>{formatNumber(getNumber(humanSummary, "total"))}</dd>
            </div>
            <div>
              <dt>Confirmados</dt>
              <dd>{formatNumber(getNumber(humanSummary, "confirmed"))}</dd>
            </div>
            <div>
              <dt>Pendientes</dt>
              <dd>{formatNumber(getNumber(humanSummary, "pending"))}</dd>
            </div>
          </dl>
          <p className="muted-copy">{getString(humanSummary, "note")}</p>
        </Panel>
      </div>

      <Panel kicker="Comparación" title="Resultado posterior">
        <ResponsiveTable
          columns={overrideColumns}
          emptyLabel="Sin comparaciones recientes"
          rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
          rows={asRows(override.data?.recent)}
        />
      </Panel>

      <Panel kicker="Movimientos reales" title="Actividad detectada en cartera">
        <ResponsiveTable
          columns={humanColumns}
          emptyLabel="Sin actividad humana reciente"
          rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
          rows={asRows(human.data?.recent)}
        />
      </Panel>
    </div>
  );
}

const overrideColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "decided_at")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Recomendó Quantia", id: "decision", render: (row) => decisionLabel(getString(row, "decision")) },
  { header: "Acción real", id: "override", render: (row) => <StatusBadge tone={toneForStatus(getString(row, "override_status"))}>{statusLabel(getString(row, "override_status"))}</StatusBadge> },
  { align: "right", header: "Ratio seguido", id: "same", render: (row) => formatPercent(getNumber(row, "same_ratio")) },
  { align: "right", header: "Resultado 5D", id: "outcome", render: (row) => <span className={toneForNumber(getNumber(row, "outcome_5d"))}>{formatPercent(getNumber(row, "outcome_5d"), 1, true)}</span> },
  {
    header: "Lectura",
    id: "read",
    render: (row) => {
      const outcome = getNumber(row, "outcome_5d");
      if (outcome === null) return "No existe evidencia suficiente para concluir";
      if (["FOLLOWED", "OVERFOLLOWED"].includes(getString(row, "override_status"))) {
        return "La decisión real siguió la recomendación";
      }
      if (["FOLLOWED_PROVISIONAL", "OVERFOLLOWED_PROVISIONAL"].includes(getString(row, "override_status"))) {
        return "El snapshot indica que seguiste la recomendación; falta confirmación de Cocos";
      }
      return outcome > 0 ? "La recomendación de Quantia tuvo mejor resultado observado" : "La decisión real obtuvo mejor resultado observado";
    },
  },
];

const humanColumns: TableColumn<RowRecord>[] = [
  { header: "Fecha", id: "date", render: (row) => formatDateTime(getString(row, "scraped_at") || getString(row, "confirmed_at")) },
  { header: "Ticker", id: "ticker", render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong> },
  { header: "Lado", id: "side", render: (row) => decisionLabel(getString(row, "side")) },
  { align: "right", header: "Monto inferido", id: "amount", render: (row) => formatMoney(getNumber(row, "inferred_amount_ars")) },
  { header: "Estado", id: "status", render: (row) => (getString(row, "confirmed_at") ? "Confirmado" : "Pendiente") },
];
