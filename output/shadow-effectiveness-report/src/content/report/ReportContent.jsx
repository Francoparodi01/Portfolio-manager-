import React from "react";

import {
  ChartRenderer, DataComponent, DataTable, MetricCard, ReportSection,
  RichNarrative, SortableItem, SortableRegion, useDataApp,
} from "../../data-app-public.jsx";

const thesisChart = {
  type: "bar", x: "horizon", y: "accuracy", fields: ["accuracy", "neutral"],
  yLabel: "Acierto direccional", showXAxisLabel: false,
  colors: { accuracy: "var(--chart-1)", neutral: "var(--chart-8)" },
};
const calibrationChart = {
  type: "bar", x: "metric", y: "value", series: "series", yLabel: "Error (menor es mejor)",
  showXAxisLabel: false, colors: { "V2 crudo": "var(--chart-8)", "V3 calibrado": "var(--chart-4)" },
};
const technicalChart = {
  type: "bar", x: "horizon", y: "meanReturn", series: "method", yLabel: "Retorno direccional medio",
  showXAxisLabel: false, colors: { "Baseline actual": "var(--chart-8)", "Technical V2": "var(--chart-4)" },
};
const learningChart = {
  type: "bar", x: "horizon", y: "alphaVsSpy", yLabel: "Alpha medio vs SPY", showXAxisLabel: false,
  colors: { alphaVsSpy: "var(--chart-4)" },
};
const radarChart = {
  type: "bar", x: "horizon", y: "matured", yLabel: "Outcomes maduros", showXAxisLabel: false,
  colors: { matured: "var(--chart-2)" },
};
const followChart = {
  type: "bar", x: "stage", y: "count", yLabel: "Cantidad", showXAxisLabel: false,
  colors: { count: "var(--chart-2)" },
};

const inventoryColumns = [
  { field: "module", label: "Módulo", presentation: "identity" },
  { field: "status", label: "Estado" },
  { field: "evidence", label: "Evidencia disponible" },
  { field: "conclusion", label: "Lectura" },
];
const sectionOrder = ["thesis-section", "calibration-section", "technical-section", "learning-section", "radar-section"];

export function ReportContent() {
  const { appTitle, canEdit, chartOverrides, chartProps, mode, reviewedRows, setAppTitle, visible } = useDataApp();
  const inventory = reviewedRows("status_inventory");
  const thesis = reviewedRows("thesis_quality");
  const calibration = reviewedRows("calibration_walk_forward");
  const technical = reviewedRows("technical_comparison");
  const learning = reviewedRows("learning_quality");
  const radar = reviewedRows("radar_maturity");
  const follow = reviewedRows("follow_trace");
  const calibrationSpec = chartOverrides["calibration-walk-forward"] ?? calibrationChart;
  const technicalSpec = chartOverrides["technical-return"] ?? technicalChart;

  return <article className="report-content" aria-label="Informe de efectividad Shadow">
    <header className="report-hero">
      <h1 data-data-app-title contentEditable={canEdit && mode === "edit"} suppressContentEditableWarning
        aria-label={canEdit && mode === "edit" ? "Editar título" : undefined}
        onBlur={canEdit && mode === "edit" ? (event) => setAppTitle(event.currentTarget.textContent.trim() || appTitle) : undefined}
        onKeyDown={canEdit && mode === "edit" ? (event) => { if (event.key === "Enter") { event.preventDefault(); event.currentTarget.blur(); } } : undefined}>{appTitle}</h1>
      <RichNarrative id="report:deck" className="report-deck" label="Editar introducción"
        value="Corte vivo al 31 de agosto de 2026. El panel separa desempeño medible, falta de madurez y evidencia que no supera al baseline. Ninguna cifra es PnL, señal operativa ni evidencia de edge bot-only." />
    </header>

    <ReportSection id="inventory" title="Mapa de evidencia" queryId="status_inventory" sourceRows={inventory} showHeading={false}>
      <RichNarrative id="inventory:summary" className="report-analysis" label="Editar resumen de evidencia"
        value="## Veredicto general\n\nLa medición existe, pero **no hay una mejora validada para promover**. La tesis de precio tiene volumen suficiente y no supera claramente el azar en dirección; la calibración V3 falla a 5 ruedas; el técnico V2 queda debajo de su baseline; Radar/V3 de compra todavía no alcanzaron masa crítica; y Learning Shadow detecta oportunidades contrafactuales, pero con alpha negativo y controles demasiado reutilizados." />
      <DataComponent id="inventory" title="Estado de cada Shadow" queryId="status_inventory" kind="table" variant="card" sourceRows={inventory}>
        <DataTable rows={inventory} columns={inventoryColumns} />
      </DataComponent>
    </ReportSection>

    <div className="report-facts" aria-label="Indicadores clave">
      <MetricCard id="price-v2-sample" title="Tesis de precio V2" queryId="thesis_quality" sourceRows={thesis}
        value="6.113" comparison="outcomes maduros a 5 ruedas" description="La muestra más amplia entre los Shadows de precio." trendValues={thesis.map((row) => row.samples)} />
      <MetricCard id="v3-gate" title="Calibración V3 · 5r" queryId="calibration_walk_forward" sourceRows={calibration}
        value="Rechazada" comparison="MAE y Brier peores fuera de muestra" negative description="El gate conserva la calibración fuera de decisiones." />
      <MetricCard id="technical-v2-result" title="Technical V2 · 5d" queryId="technical_comparison" sourceRows={technical}
        value="+0,5%" comparison="baseline +1,7% · n=41" negative description="Retorno direccional medio, no retorno de una operación." />
    </div>

    <SortableRegion id="report:sections" label="Secciones del informe" variant="stack" authoredOrder={sectionOrder} className="report-sortable-sections">
      {visible("thesis-section") && <SortableItem id="thesis-section" label="Tesis de precio" kind="chart">
        <section className="report-section">
          <ReportSection id="thesis-reading" title="Tesis de precio V2" queryId="thesis_quality" sourceRows={thesis} showHeading={false}>
            <RichNarrative id="thesis-reading:body" className="report-analysis" label="Editar lectura de tesis"
              value="## Tesis de precio V2: volumen alto, eficacia direccional débil\n\nA 5 y 20 ruedas el acierto es 50,7% y 50,9%, apenas por encima de la referencia neutral de 50%. A 40 ruedas cae a 46,6%. El MAE crece de 5,0% a 17,9% al extender el horizonte. Esto da una base útil para auditar sesgos, pero no una mejora demostrada que deba entrar en decisiones." />
          </ReportSection>
          <DataComponent id="thesis-accuracy" title="Acierto direccional por horizonte" queryId="thesis_quality" kind="chart" chart={thesisChart} sourceRows={thesis}>
            <ChartRenderer spec={chartOverrides["thesis-accuracy"] ?? thesisChart} rows={thesis} height={320} {...chartProps("thesis-accuracy")} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("calibration-section") && <SortableItem id="calibration-section" label="Calibración V3" kind="chart">
        <section className="report-section">
          <ReportSection id="calibration-reading" title="Calibración V3" queryId="calibration_walk_forward" sourceRows={calibration} showHeading={false}>
            <RichNarrative id="calibration-reading:body" className="report-analysis" label="Editar lectura de calibración"
              value="## Calibración V3: la mejora in-sample no sobrevive la prueba\n\nEn el ajuste interno V3 reduce el error, pero la comparación sin leakage de 5 ruedas empeora tanto el MAE (4,61% → 4,85%) como Brier (0,259 → 0,281). Por eso el gate es `FAILED_WALK_FORWARD`. A 20 ruedas todavía no hay cohortes válidas fuera de muestra. Además, el recalibrado quedó limitado a 500 outcomes nuevos, para no volver a ajustar sobre casi la misma evidencia." />
          </ReportSection>
          <DataComponent id="calibration-walk-forward" title="Error fuera de muestra: V2 crudo vs V3 calibrado" queryId="calibration_walk_forward" kind="chart" chart={calibrationSpec} sourceRows={calibration}>
            <ChartRenderer spec={calibrationSpec} rows={calibration} height={320} {...chartProps("calibration-walk-forward")} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("technical-section") && <SortableItem id="technical-section" label="Técnico V2" kind="chart">
        <section className="report-section">
          <ReportSection id="technical-reading" title="Technical Shadow V2" queryId="technical_comparison" sourceRows={technical} showHeading={false}>
            <RichNarrative id="technical-reading:body" className="report-analysis" label="Editar lectura técnica"
              value="## Technical Shadow V2: pérdida frente al baseline observado\n\nEn 41 episodios maduros a 5 días, Technical V2 da +0,5% medio y 58,5% de acierto contra +1,7% y 65,9% del baseline. A 10 días conserva el acierto, pero el retorno medio también queda por debajo (+1,8% vs +4,1%) y la muestra baja a 13. No se justifica promoción ni sustitución del proceso actual." />
          </ReportSection>
          <DataComponent id="technical-return" title="Retorno direccional medio: baseline vs Technical V2" queryId="technical_comparison" kind="chart" chart={technicalSpec} sourceRows={technical}>
            <ChartRenderer spec={technicalSpec} rows={technical} height={320} {...chartProps("technical-return")} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("learning-section") && <SortableItem id="learning-section" label="Learning Shadow" kind="chart">
        <section className="report-section">
          <ReportSection id="learning-reading" title="Learning Shadow" queryId="learning_quality" sourceRows={learning} showHeading={false}>
            <RichNarrative id="learning-reading:body" className="report-analysis" label="Editar lectura de aprendizaje"
              value="## Learning Shadow: oportunidades potenciales, pero evidencia no promovible\n\nLos bloqueos del planner muestran misses contrafactuales, pero el alpha medio contra SPY es negativo en los cuatro horizontes. Los controles se reutilizan entre 6,3× y 7,1×, muy por encima del máximo de 2× fijado para revisar una regla. Por eso FUNDING, nominal mínimo, peso mínimo y SCORE_GUARD quedan como `EVIDENCE_REVIEW`, no propuestas de umbral." />
          </ReportSection>
          <DataComponent id="learning-alpha" title="Alpha medio vs SPY en decisiones bloqueadas" queryId="learning_quality" kind="chart" chart={learningChart} sourceRows={learning}>
            <ChartRenderer spec={chartOverrides["learning-alpha"] ?? learningChart} rows={learning} height={320} {...chartProps("learning-alpha")} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("radar-section") && <SortableItem id="radar-section" label="Radar y seguimiento" kind="chart">
        <section className="report-section">
          <ReportSection id="radar-reading" title="Technical Buy V3, Radar y seguimiento" queryId="radar_maturity" queryIds={["radar_maturity", "follow_trace"]} sourceRowsByQuery={{ radar_maturity: radar, follow_trace: follow }} showHeading={false}>
            <RichNarrative id="radar-reading:body" className="report-analysis" label="Editar lectura de Radar"
              value="## Radar / Technical Buy V3: instrumentación lista, efectividad pendiente\n\nEl Ledger prospectivo acumula seis cohortes de la versión actual, pero solo una observación llegó a 5 ruedas; el retorno neto teórico es +0,24% y el exceso frente al universo es 0,0%. No sirve para comparar niveles V3 ni para inferir edge. En seguimiento hay siete ideas y cero compras reales vinculadas: es trazabilidad descriptiva, no retorno realizado." />
          </ReportSection>
          <div className="paired-charts">
            <DataComponent id="radar-maturity" title="Madurez de outcomes del Ledger Radar" queryId="radar_maturity" kind="chart" chart={radarChart} sourceRows={radar}>
              <ChartRenderer spec={chartOverrides["radar-maturity"] ?? radarChart} rows={radar} height={300} {...chartProps("radar-maturity")} />
            </DataComponent>
            <DataComponent id="follow-trace" title="Seguimiento vs compras reales vinculadas" queryId="follow_trace" kind="chart" chart={followChart} sourceRows={follow}>
              <ChartRenderer spec={chartOverrides["follow-trace"] ?? followChart} rows={follow} height={300} {...chartProps("follow-trace")} />
            </DataComponent>
          </div>
        </section>
      </SortableItem>}
    </SortableRegion>

    <RichNarrative id="report:boundary" className="report-disclosure" label="Editar límite metodológico"
      value="**Límite de interpretación.** Los módulos Shadow persisten hipótesis, comparaciones o trazas. `ExecutionPlan` y los fills reales siguen siendo la verdad operativa. Este informe no cambia score, ranking, optimizer, planner, órdenes ni thresholds." />
  </article>;
}
