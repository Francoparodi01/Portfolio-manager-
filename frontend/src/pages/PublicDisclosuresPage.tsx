import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSession } from "../app/session";
import { ErrorState, LoadingState } from "../components/feedback/States";
import { PageHeader } from "../components/layout/PageHeader";
import { Metric, MetricGroup } from "../components/ui/Metric";
import { Panel } from "../components/ui/Panel";
import { ResponsiveTable, type TableColumn } from "../components/ui/ResponsiveTable";
import { fetchJson } from "../services/apiClient";
import { formatDateTime } from "../utils/format";

type Disclosure = {
  event_key: string;
  ticker: string;
  person_name: string;
  person_role: string;
  direction: "BUY" | "SELL";
  occurred_on: string;
  filed_on: string;
  first_seen_at: string;
  shares: number | null;
  unit_price_usd: number | null;
  value_usd: number | null;
  ownership_type: string | null;
  source_url: string;
};

type DisclosureResponse = {
  ok: boolean;
  state: "NOT_INITIALIZED" | "HAS_DATA" | "NO_DATA";
  count_returned: number;
  last_capture: string | null;
  items: Disclosure[];
};

const demo: DisclosureResponse = {
  ok: true, state: "NOT_INITIALIZED", count_returned: 0, last_capture: null, items: [],
};

function amount(value: number | null, decimals = 0): string {
  return value === null || !Number.isFinite(value)
    ? "No informado"
    : new Intl.NumberFormat("en-US", {
        style: "currency", currency: "USD", maximumFractionDigits: decimals,
      }).format(value);
}

const columns: TableColumn<Disclosure>[] = [
  { id: "ticker", header: "Ticker", render: (row) => <strong>{row.ticker}</strong> },
  { id: "person", header: "Persona / cargo", render: (row) => <span>{row.person_name}<small style={{ display: "block" }}>{row.person_role}</small></span> },
  { id: "direction", header: "Operación", render: (row) =>
      <strong>{row.direction === "BUY" ? "Compra" : "Venta"}</strong> },
  { id: "date", header: "Operó", render: (row) => row.occurred_on },
  { id: "filed", header: "Declaró", render: (row) => row.filed_on },
  { id: "value", header: "Valor estimado", align: "right", render: (row) =>
      amount(row.value_usd, 2) },
  { id: "source", header: "Evidencia", render: (row) =>
      <a href={row.source_url} rel="noopener noreferrer" target="_blank">Ver formulario SEC</a> },
];

export default function PublicDisclosuresPage() {
  const { session } = useSession();
  const [draftTicker, setDraftTicker] = useState("");
  const [ticker, setTicker] = useState("");
  const [direction, setDirection] = useState<"ALL" | "BUY" | "SELL">("ALL");
  const query = useQuery({
    queryKey: ["monitor", "public-disclosures", session?.mode, session?.apiBase, ticker],
    queryFn: async () => {
      if (!session) throw new Error("Sesión no disponible");
      if (session.mode === "demo") return demo;
      return fetchJson<DisclosureResponse>(
        session,
        "/api/public-disclosures?limit=120" + (ticker ? "&ticker=" + encodeURIComponent(ticker) : ""),
      );
    },
    enabled: Boolean(session),
    refetchOnWindowFocus: false,
  });

  const filtered = useMemo(() => (query.data?.items || []).filter(
    (row) => direction === "ALL" || row.direction === direction,
  ), [query.data, direction]);
  const buys = filtered.filter((row) => row.direction === "BUY").length;
  const sells = filtered.filter((row) => row.direction === "SELL").length;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="Research / evidencia pública"
        title="Operaciones de insiders"
        description="Compras y ventas declaradas en formularios SEC Form 4. Solo transacciones P/S no derivadas (de mercado o privadas); no son señales de compra."
      />
      <MetricGroup>
        <Metric label="Operaciones visibles" value={filtered.length} detail="Hasta 120 registros del alcance consultado" />
        <Metric label="Compras" value={buys} tone="real" />
        <Metric label="Ventas" value={sells} tone="warning" />
        <Metric label="Última captura" value={query.data?.last_capture ? formatDateTime(query.data.last_capture) : "Sin captura"} />
      </MetricGroup>

      <Panel kicker="SEC EDGAR" title="Movimientos publicados">
        <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
          <label htmlFor="insider-ticker">Ticker</label>
          <input
            id="insider-ticker"
            placeholder="Ej.: AMD"
            value={draftTicker}
            maxLength={12}
            onChange={(event) => setDraftTicker(event.target.value.toUpperCase())}
            onKeyDown={(event) => {
              if (event.key === "Enter") setTicker(/^[A-Z0-9.]{0,12}$/.test(draftTicker.trim()) ? draftTicker.trim() : "");
            }}
          />
          <button type="button" onClick={() => setTicker(/^[A-Z0-9.]{0,12}$/.test(draftTicker.trim()) ? draftTicker.trim() : "")}>Aplicar</button>
          <label htmlFor="insider-direction">Tipo</label>
          <select id="insider-direction" value={direction} onChange={(event) =>
            setDirection(event.target.value as "ALL" | "BUY" | "SELL")}>
            <option value="ALL">Todas</option>
            <option value="BUY">Compras</option>
            <option value="SELL">Ventas</option>
          </select>
        </div>
        {query.isLoading && !query.data ? <LoadingState label="Consultando operaciones" /> : null}
        {query.isError ? <ErrorState message={query.error instanceof Error ? query.error.message : "No se pudo consultar"} /> : null}
        {!query.isError && !query.isLoading ? (
          <ResponsiveTable
            columns={columns}
            rows={filtered}
            rowKey={(row) => row.event_key}
            emptyLabel={query.data?.state === "NOT_INITIALIZED"
              ? "Sin ingesta SEC persistida. Ejecutá el colector para poblar el panel."
              : "Sin operaciones P/S para los filtros seleccionados."}
          />
        ) : null}
      </Panel>

      <Panel kicker="Límites" title="Cómo interpretar esta información">
        <p>“Operó” es la fecha de transacción; “Declaró” es la fecha presentada ante la SEC; la captura de Quantia puede ser posterior. Un formulario publicado hoy puede describir una transacción anterior.</p>
        <p>Se excluyen ejercicios de opciones, adjudicaciones y otros códigos diferentes de P/S. El código P/S no diferencia mercado abierto de transacciones privadas. Los movimientos pueden surgir de planes 10b5-1 y no prueban convicción discrecional. Los valores son aproximados cuando existen precio y cantidad.</p>
        <p>Congreso de EE. UU.: <strong>fuentes House/Senate pendientes de integración</strong>. No se incluyen operaciones de congresistas en los totales actuales.</p>
      </Panel>
    </div>
  );
}
