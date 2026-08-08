import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { PageHeader } from "../components/layout/PageHeader";
import { DataFreshness } from "../components/ui/DataFreshness";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type SortDirection, type TableColumn } from "../components/ui/ResponsiveTable";
import { StatusBadge } from "../components/ui/StatusBadge";
import { LoadingState } from "../components/feedback/States";
import { HorizontalBars } from "../components/charts/MiniCharts";
import { usePortfolioQuery } from "../hooks/useMonitorData";
import type { RowRecord } from "../types/api";
import { asRows, getNumber, getRecord, getString, maxNumber } from "../utils/data";
import { formatMoney, formatNumber, formatPercent, toneForNumber } from "../utils/format";

export default function PortfolioPage() {
  const [params, setParams] = useSearchParams();
  const query = params.get("q") || "";
  const sortColumn = params.get("sort") || "market_value";
  const sortDirection = (params.get("dir") === "asc" ? "asc" : "desc") as SortDirection;
  const portfolio = usePortfolioQuery(90);
  const snapshot = getRecord(portfolio.data, "snapshot");
  const positions = asRows(portfolio.data?.positions);
  const allocation = asRows(portfolio.data?.allocation);
  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return positions;
    return positions.filter((row) =>
      [getString(row, "ticker"), getString(row, "asset_type")].some((value) => value.toLowerCase().includes(normalized)),
    );
  }, [positions, query]);

  const setQuery = (value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set("q", value);
    else next.delete("q");
    setParams(next, { replace: true });
  };

  const setSort = (columnId: string) => {
    const next = new URLSearchParams(params);
    const nextDirection = sortColumn === columnId && sortDirection === "desc" ? "asc" : "desc";
    next.set("sort", columnId);
    next.set("dir", nextDirection);
    setParams(next, { replace: true });
  };

  if (portfolio.isLoading && !portfolio.data) return <LoadingState label="Cargando cartera" />;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Cartera"
        title="Posiciones y exposición"
        description="Snapshot real de portfolio, con pesos, P/L y fecha de captura cuando el backend la informa."
      />

      <MetricGroup>
        <Metric label="Valor total" tone="real" value={formatMoney(getNumber(snapshot, "total_value_ars"))} />
        <Metric label="Efectivo" value={formatMoney(getNumber(snapshot, "cash_ars"))} />
        <Metric label="Posiciones" value={formatNumber(positions.length)} />
        <Metric label="Mayor peso" tone="warning" value={formatPercent(maxNumber(positions, "weight_in_portfolio"))} />
      </MetricGroup>

      <div className="panel-grid two">
        <Panel kicker="Distribución" title="Exposición por tipo de activo">
          <HorizontalBars
            description="Valor de mercado agregado por asset_type."
            rows={allocation.map((row) => ({
              display: formatMoney(getNumber(row, "market_value")),
              label: getString(row, "asset_type", "UNKNOWN"),
              value: getNumber(row, "market_value") ?? 0,
            }))}
            title="Exposición por tipo de activo"
          />
        </Panel>
        <Panel kicker="Frescura" title="Fuente del snapshot">
          <DataFreshness source="portfolio_snapshots" value={getString(snapshot, "scraped_at")} />
          <div className="source-note">
            <StatusBadge tone="real">Dato operativo real</StatusBadge>
            <span>Los precios por posición provienen del último snapshot expuesto por `/api/portfolio`.</span>
          </div>
        </Panel>
      </div>

      <Panel
        action={
          <label className="table-search">
            <span>Buscar</span>
            <input onChange={(event) => setQuery(event.target.value)} placeholder="Ticker o tipo" value={query} />
          </label>
        }
        kicker={`${formatNumber(filtered.length)} filas`}
        title="Detalle de posiciones"
      >
        <ResponsiveTable
          columns={columns}
          emptyLabel="Sin posiciones para el filtro actual"
          onSort={setSort}
          rowKey={(row, index) => `${getString(row, "ticker")}-${index}`}
          rows={filtered}
          sort={{ columnId: sortColumn, direction: sortDirection }}
        />
      </Panel>
    </div>
  );
}

const columns: TableColumn<RowRecord>[] = [
  {
    header: "Ticker",
    id: "ticker",
    render: (row) => <strong className="ticker">{getString(row, "ticker")}</strong>,
    sortValue: (row) => getString(row, "ticker"),
  },
  {
    header: "Tipo",
    id: "asset_type",
    render: (row) => getString(row, "asset_type", "UNKNOWN"),
    sortValue: (row) => getString(row, "asset_type"),
  },
  {
    align: "right",
    header: "Cantidad",
    id: "quantity",
    render: (row) => formatNumber(getNumber(row, "quantity")),
    sortValue: (row) => getNumber(row, "quantity") ?? 0,
  },
  {
    align: "right",
    header: "Precio",
    id: "current_price",
    render: (row) => formatMoney(getNumber(row, "current_price")),
    sortValue: (row) => getNumber(row, "current_price") ?? 0,
  },
  {
    align: "right",
    header: "Costo prom.",
    id: "avg_cost",
    render: (row) => formatMoney(getNumber(row, "avg_cost")),
    sortValue: (row) => getNumber(row, "avg_cost") ?? 0,
  },
  {
    align: "right",
    header: "Valor",
    id: "market_value",
    render: (row) => formatMoney(getNumber(row, "market_value")),
    sortValue: (row) => getNumber(row, "market_value") ?? 0,
  },
  {
    align: "right",
    header: "Peso",
    id: "weight_in_portfolio",
    render: (row) => formatPercent(getNumber(row, "weight_in_portfolio")),
    sortValue: (row) => getNumber(row, "weight_in_portfolio") ?? 0,
  },
  {
    align: "right",
    header: "P/L",
    id: "unrealized_pnl_pct",
    render: (row) => (
      <span className={toneForNumber(getNumber(row, "unrealized_pnl_pct"))}>
        {formatPercent(getNumber(row, "unrealized_pnl_pct"), 1, true)}
      </span>
    ),
    sortValue: (row) => getNumber(row, "unrealized_pnl_pct") ?? 0,
  },
];
