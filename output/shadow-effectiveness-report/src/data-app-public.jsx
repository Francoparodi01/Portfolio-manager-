export { ChartRenderer, ChartRenderer as Chart } from "./charting/ChartRenderer.jsx";
export { chartDataShape, chartSpecKeys, groupAdditiveCategories, projectChartSpec } from "./charting/chart-data-shape.js";
export { normalizeChartAnnotations, resolveChartAnnotations } from "./charting/chart-annotations.js";
export {
  compact,
  displayValue,
  label,
  percentage,
  periodComparison,
  semanticColorResolver,
  shortDate,
} from "./charting/chart-theme.js";
export { DataTable, DataTable as Table, Dropdown, Filters, InlineFilters } from "./components/Controls.jsx";
export { DataComponent, ReportSection } from "./components/DataComponent.jsx";
export { MetricCard, MetricSparkline } from "./components/MetricCard.jsx";
export { EditableText } from "./components/EditableText.jsx";
export { Section, SectionHeader } from "./components/Section.jsx";
export { useSectionFilters } from "./use-section-filters.js";
export { RichNarrative } from "./components/RichMarkdown.jsx";
export { Icon } from "./components/Icon.jsx";
export { SourceInspector, SourceSidebar } from "./components/SourceInspector.jsx";
export { useDataAppShell as useDataApp, useDashboardTabs } from "./DataAppContext.jsx";
export { previousPeriodRows } from "./use-data-app.js";

export { SortableItem, SortableRegion } from "./components/SortableRegion.jsx";
