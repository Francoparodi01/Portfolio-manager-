import React from "react";
import { createRoot } from "react-dom/client";

import { DataAppShell } from "../../src/DataAppShell.jsx";
import { Chart, DataComponent, Section, SectionHeader, useDataApp } from "../../src/data-app-public.jsx";
import "../../src/styles.css";
import "../../src/print.css";
import "../../src/theme.css";
import "./example.css";

export const examples = [
  { id: "sales", title: "Sales funnel", colors: { count: "var(--chart-1)" }, rows: [
    ["Emails", 17000], ["Visits", 13000], ["Logins", 5900], ["Purchases", 4000], ["Payments", 2300],
  ] },
  { id: "market", title: "Market to sales", colors: { count: "var(--chart-2)" }, rows: [
    ["Total market", 142901], ["Prospects", 101020], ["Leads", 60314], ["Sales", 54280],
  ] },
  { id: "activation", title: "Product activation", rows: [
    ["Signed up", 12000], ["Created workspace", 8400], ["Invited team", 5160], ["Activated", 3000],
  ], colors: { count: "var(--chart-3)" } },
];

const snapshot = {
  id: "funnel-component-examples", title: "Funnel component examples", status: "fixture", surface: "dashboard", filters: [],
  queries: Object.fromEntries(examples.map(({ id, rows }) => [id, {
    rows: rows.map(([stage, count]) => ({ stage, count })),
    source: { label: "Synthetic component example", description: "Illustrative values for visual testing, not business data.",
      caveats: ["Synthetic example data. No external query was executed."] },
  }])),
};

function Example({ example }) {
  const { reviewedRows, chartProps, chartOverrides, visible } = useDataApp();
  if (!visible(example.id)) return null;
  const rows = reviewedRows(example.id);
  const chart = chartOverrides[example.id] ?? { type: "funnel", x: "stage", y: "count", colors: example.colors };
  return <DataComponent id={example.id} title={example.title} queryId={example.id} kind="chart"
    chart={chart} sourceRows={rows} displayRows={rows} variant="card" padding="spacious">
    <Chart spec={chart} rows={rows} height={280} {...chartProps(example.id)} />
  </DataComponent>;
}

function FunnelExamples() {
  return <article className="page funnel-examples">
    <header className="funnel-examples-intro"><SectionHeader id="funnel-examples-title" as="h1" title="Follow the conversion" />
      <p>Conversion funnels · synthetic example data</p></header>
    <Section spacing="none"><Example example={examples[0]} /></Section>
    <Section spacing="content" columns={2}>{examples.slice(1).map((example) => <Example key={example.id} example={example} />)}</Section>
  </article>;
}

createRoot(document.getElementById("root")).render(
  <DataAppShell snapshot={snapshot} hosted={false}><FunnelExamples /></DataAppShell>,
);
