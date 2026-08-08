import { CheckCircle2, CircleAlert } from "lucide-react";
import { LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { DataFreshness } from "../components/ui/DataFreshness";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { endpointDefinitions } from "../services/monitorApi";
import { useCandlesQuery, useHealthQuery, useIngestionQuery, useLogsQuery, usePeriodParam } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getBoolean, getNumber, getRecord, getString } from "../utils/data";
import { formatDateTime, formatNumber } from "../utils/format";

export default function DataPage() {
  const [period] = usePeriodParam(90);
  const health = useHealthQuery();
  const ingestion = useIngestionQuery();
  const candles = useCandlesQuery();
  const logs = useLogsQuery();
  const portfolioLatest = getRecord(ingestion.data?.portfolio, "latest");
  const marketLatest = getRecord(getRecord(ingestion.data, "market_prices"), "latest");
  const coverage = getRecord(candles.data, "coverage");

  if (ingestion.isLoading && !ingestion.data) return <LoadingState label="Cargando estado de datos" />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Datos"
        title="Frescura, cobertura y endpoints"
        description="Estado operacional de fuentes reales, precios, velas internas y logs expuestos por el monitor."
      />

      <MetricGroup>
        <Metric label="DB" tone={getBoolean(health.data?.database, "ok") ? "real" : "warning"} value={getBoolean(health.data?.database, "ok") ? "OK" : "Check"} />
        <Metric label="Redis" tone={getBoolean(health.data?.redis, "ok") ? "real" : "warning"} value={getBoolean(health.data?.redis, "ok") ? "OK" : "Check"} />
        <Metric label="Tickers 24h" value={formatNumber(getNumber(marketLatest, "tickers_24h"))} />
        <Metric label="Velas faltantes" tone={getNumber(coverage, "missing_internal") ? "warning" : "real"} value={formatNumber(getNumber(coverage, "missing_internal"))} />
      </MetricGroup>

      <div className="panel-grid two">
        <Panel kicker="Portfolio" title="Último snapshot">
          <DataFreshness source="portfolio_snapshots" value={getString(portfolioLatest, "scraped_at")} />
        </Panel>
        <Panel kicker="Market data" title="Últimos precios">
          <DataFreshness source="market_prices" value={getString(marketLatest, "latest_ts")} />
        </Panel>
      </div>

      <Panel kicker="Cobertura" title="Velas internas recientes">
        <ResponsiveTable
          columns={candleColumns}
          emptyLabel="Sin cobertura reciente"
          rowKey={(row, index) => `${getString(row, "business_day")}-${index}`}
          rows={asRows(candles.data?.recent)}
        />
      </Panel>

      <Panel kicker="Endpoints" title="Contratos usados por el frontend">
        <div className="endpoint-grid">
          {endpointDefinitions(period).map((endpoint) => (
            <article key={endpoint.key}>
              {endpoint.public ? <CheckCircle2 size={16} aria-hidden="true" /> : <CircleAlert size={16} aria-hidden="true" />}
              <strong>{endpoint.label}</strong>
              <code>{endpoint.path}</code>
              <StatusBadge tone={endpoint.public ? "info" : "real"}>{endpoint.public ? "público" : "read-only auth"}</StatusBadge>
            </article>
          ))}
        </div>
      </Panel>

      <Panel kicker="Logs" title="Alertas técnicas recientes">
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

const candleColumns: TableColumn<RowRecord>[] = [
  { header: "Día", id: "day", render: (row) => formatDateTime(getString(row, "business_day")) },
  { align: "right", header: "Filas", id: "rows", render: (row) => formatNumber(getNumber(row, "rows")) },
  { align: "right", header: "Tickers", id: "tickers", render: (row) => formatNumber(getNumber(row, "tickers")) },
  { header: "Mín.", id: "min", render: (row) => formatDateTime(getString(row, "min_ts")) },
  { header: "Máx.", id: "max", render: (row) => formatDateTime(getString(row, "max_ts")) },
];

const logColumns: TableColumn<RowRecord>[] = [
  { header: "Archivo", id: "file", render: (row) => getString(row, "file", "log") },
  { header: "Línea", id: "line", render: (row) => <code>{getString(row, "line")}</code> },
];
