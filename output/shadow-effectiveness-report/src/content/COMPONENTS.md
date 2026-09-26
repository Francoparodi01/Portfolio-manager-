# Data app public component API

Import product-owned behavior from the protected public module:

```jsx
import {
  Chart,
  DataComponent,
  EditableText,
  Filters,
  InlineFilters,
  MetricCard,
  Section,
  SectionHeader,
  SortableItem,
  SortableRegion,
  Table,
  compact,
  percentage,
  useDataApp,
  useSectionFilters,
} from "../../data-app-public.jsx";
```

`useDataApp()` exposes reviewed queries, reviewed rows, active filters, chart state,
presentation state, stable app title, and surface capabilities from the protected shell.

```jsx
function AdoptionTrend() {
  const { reviewedRows, chartProps, chartOverrides } = useDataApp();
  const chart = chartOverrides["adoption-trend"] ?? { type: "line", x: "week", y: "activeUsers" };
  const rows = reviewedRows("usage_summary", [chart.x, chart.series].filter(Boolean));

  return (
    <DataComponent id="adoption-trend" queryId="usage_summary"
      title="Weekly adoption" kind="chart" chart={chart} displayRows={rows} sourceRows={rows}>
      <EditableText>Adoption continues to change across the reviewed period.</EditableText>
      <Chart spec={chart} rows={rows}
        height={320} {...chartProps("adoption-trend")} />
    </DataComponent>
  );
}
```

`DataComponent` automatically inherits source inspection, chart exploration, copy/hide
actions, title editing, and persisted presentation. Never implement those behaviors in
authored content or derive component identity from titles, positions, or query indices.

## Shared cards and sections

Page-wide `<Filters sticky ... />` stays below the protected top bar while scrolling.
Place it directly in the full-height page content, not a short filter-only wrapper;
CSS sticky positioning ends at its containing block. Omit `sticky` for section-local
controls in `SectionHeader`. Keep page controls outside sortable content.

Use `<DataComponent variant="card">` for an ordinary chart, table, or custom card
on either surface. It owns padding, border, background, shadow, theme radius, and
60% Figma-style corner smoothing. Standard padding is **16px vertically / 20px
horizontally**; `padding="spacious"` uses **20px on every side**. `MetricCard` uses
`variant="card" padding="spacious"` by default, with a 112px minimum height.
Use `variant="plain"` for container-free content; it is the `DataComponent` default
and the preferred report presentation. Reports may explicitly opt into cards without
CSS exceptions or a separate report-card component.

Classic cards have a 20px smoothed radius. Other themes retain their own radius;
square cards remain square. Theme tokens `--data-card-radius`,
`--data-card-background`, `--card-shadow`, and `--border` own their appearance.
A deliberately custom plain wrapper may still opt into `smoothCorners` at its own
uniform CSS radius. Smoothing never clips menus, focus rings, or chart interactions.

Use `Section` for a fixed group and `SectionHeader` in a canvas row's `header` slot.
Both are surface-neutral; neither invents a query or source-backed component identity.
Pass a stable globally unique header `id`; it is also the persisted text-edit key.
Headers use 20px / 28px, weight 600, and -0.5px tracking. They accept optional
`filters={<Filters {...scope.filterProps} />}` on the right, wrapping below the title
when needed. A filtered `Section` requires an authored title to name its scope.
In Edit mode, titles can be edited inline and their menu offers **Hide heading**.
Hiding affects only the heading, never its charts or filters. The stable header ID
also persists its hidden state; undo/redo and **Restore hidden** work on headings
and cards alike. Headings are not draggable.

`Section` defaults to one column and `spacing="section"`; `columns={2}` stretches
adjacent cards to equal height and collapses to one column on mobile. Standard gaps:

- Cards: **20px**; KPI cards (`kind="metrics"`): **12px** in both directions.
- Header to content: **20px**.
- New section (`spacing="section"`): **40px**; continuation row
  (`spacing="content"` or `spacing="continuation"`): **20px**.
- After KPIs: **32px total** with `spacing="after-metrics"`.
- `spacing="none"` adds no leading space; `spacing="metrics"` adds 16px.

Do not reproduce these rules in `dashboard.css` or `report.css`. Those stylesheets
own only the particular composition, custom visual internals, and analytical heights.
Do not add outer gaps on top of shared section spacing.

## Funnels

For ordered conversion stages, `Chart` with `type: "funnel"` adapts to its own
container width and stage count: at least 144px per stage uses the horizontal
ribbon, 120–143px uses compact horizontal typography, and less uses vertical
stage rows with proportional bars. All stages remain visible without horizontal
scrolling. Preserve input order; do not sort stages by value. Ribbon thickness
and bar length are proportional to the largest stage; the ribbon stays flat
after the last stage. Missing counts leave gaps and zero values stay zero.

Labels and percentages show share of the first stage. Hover/focus adds only the
exact value, previous → current conversion, and signed absolute drop-off (or
increase). Percentage measures retain exact percent and percentage-point units.
Tap pins the details, with Done/tap-away dismissal and an explicit Ask action
only where chart selection is permitted. Mouse/keyboard selection is unchanged.

Set `colors[y]` for one shared hue or `colors[stageName]` for stage hues. Tints start
at the unmodified core hue and match editor swatches; vertical corners respect
theme mark geometry. Image export includes all stages in the current layout.
See `/examples/funnel/` for synthetic examples using the shared component.

## Optional section filters

`useSectionFilters(definitions, initialValues?)` keeps local selections independent
of page filters and other sections, even when IDs match. Definitions use the same
`id`, `label`, `field`, `defaultValue`, `mode`, and optional `queryIds` contract as
page filters. It intersects page and section scope against the original reviewed
rows before aggregate-row selection. An incompatible page/local selection returns
no rows; local "All" never broadens page scope. Controls do not rerun source queries.

```jsx
function RegionalSection() {
  const { chartProps, chartOverrides } = useDataApp();
  const scope = useSectionFilters([
    { id: "region", label: "Region", field: "region", defaultValue: "all",
      queryIds: ["usage_summary"] },
  ]);
  const chart = chartOverrides["regional-adoption"] ?? { type: "line", x: "week", y: "activeUsers" };
  const scoped = scope.componentProps("usage_summary", [chart.x, chart.series].filter(Boolean));
  return (
    <Section id="regional-section-title" title="Regional adoption"
      filters={<Filters {...scope.filterProps} />}>
      <DataComponent id="regional-adoption" queryId="usage_summary"
        title="Active users over time" kind="chart" chart={chart}
        variant="card" {...scoped}>
        <Chart spec={chart} rows={scoped.displayRows}
          {...chartProps("regional-adoption")} />
      </DataComponent>
    </Section>
  );
}
```

`componentProps(queryId, breakdown?)` supplies `displayRows`, `sourceRows`, and
`scopeFilters`: use it on **every affected source-backed block** so charts, copied
data, source inspection, and the chart editor agree. Derive captions, totals, and
local comparisons from those same rows; do not use unscoped page rows or global
prior-period helpers for a locally scoped comparison. Include temporal/dimension
fields in `breakdown` when retaining their full history/categories. Ranged dates
otherwise select the latest available scoped endpoint. Section **All dates** selects
all dates within page scope. Reset a section selection through its dropdown's
**All** / **All dates** option; section filters omit a redundant reset button.

For movable canvas consumers, provide placement metadata as the hook's third argument:

```jsx
const scope = useSectionFilters(definitions, {}, {
  rowId: "dashboard:engagement",
  componentIds: ["engagement-heatmap", "engagement-scatter"],
  label: "Engagement filters",
});
```

Use `scope.filterProps` in that row's header and `scope.componentProps` on every
listed consumer. If a block moves out, its filters appear locally on the card. If
an unrelated block joins the filtered row, controls move onto the affected cards
instead of implying the newcomer is filtered. Returning to the original group
restores the header controls without duplicates. The selection remains shared by
that semantic group; dragging never silently changes its data scope. Fixed sections
do not need this placement metadata.

Selections are local view state, not persisted presentation, URL parameters, or
shared-link state; they reset when the section unmounts/reloads. Print retains visible
section filter values. Page-wide filters remain separately controlled by `useDataApp`.
For a new report without visible page-wide controls, author `snapshot.filters: []`;
do not copy hidden dashboard filter definitions that silently constrain report data.

Wrap intentionally movable authored content with the protected sortable primitives:

```jsx
<SortableRegion id="dashboard:overview:canvas" variant="canvas" spacing="standard" columns={12}
  rows={[{ id: "overview:metrics", kind: "metrics", items: ["active-users"] }]}
  label="Dashboard blocks">
  <SortableItem id="active-users" label="Weekly active users"
    kind="metric" span={3} minSpan={2}>
    <MetricCard id="active-users" queryId="usage_summary"
      title="Weekly active users" value={compact(reviewedRows("usage_summary").at(-1)?.activeUsers)} />
  </SortableItem>
</SortableRegion>
```

The canvas is optional: use one stable surface/page-scoped dashboard canvas only when
independently movable blocks should cross semantic rows and support shared-divider
resizing. Reuse the existing authored component ID for every movable block. Its `rows` describe the initial semantic
composition. Use `spacing="standard"` to inherit shared section spacing for every row;
row metadata can declare `kind="metrics"` and override `spacing` explicitly. Explicit
spacing wins, including after metrics. Legacy/custom canvases default to
`spacing="authored"` and retain authored gaps; a row with explicit `kind` or `spacing`
opts just that row into shared presentation. A row may provide a `header` React node for an authored section heading;
it spans the row, stays outside block dragging/resizing, and hides when the row is empty.
Drop beside a block to join its row, or briefly pause in the visible,
sticky insertion zone above, between, or below rows to create a row. Existing rows
remain the preferred destination, including at block edges; sibling movement and the
opened row provide placement feedback without a blue insertion rule. The protected
runtime rebalances unequal widths when row membership changes within a 12-column desktop / 6-column tablet /
single-column mobile layout without shrinking blocks below their semantic `minSpan` or
measured readable width, moving locked blocks, or accepting overcrowded rows. Wide,
dense tables remain alone when their reviewed columns require the full row. Authored
`span` and explicit owner-selected widths remain separate preferred sizes; temporary
row rebalancing must not overwrite them. Reordering blocks within the same row preserves
every existing width. Authorized owners can drag a compact grip centered in the measured
gutter between neighboring dashboard blocks or adjust it with the arrow keys. The grip
borrows width from the nearest block with available space when adjacent blocks are already
at their minimum; component action menus do not expose redundant width presets.
Do not add separate row drag affordances: rows are derived from block placement. Start
pointer drags from existing component headers or metric surfaces; their grab cursor is
the only pointer-drag cue, and editable titles retain the shared inline-text hover and
focus treatment. Blocks displace neighbors when their moving edge crosses the
neighbor's midpoint, independent of where the pointer grabbed the block. Keep
component titles and menus aligned, and do not add movement icons or
header-wide hover panels. Menus, source links, editable titles, chart gestures, and
scenario controls remain interactive. A visually hidden Move control appears above the
header only for keyboard focus and preserves full keyboard access without permanent
handles.

For creative editable layouts, use `variant="freeform"` and ordinary authored CSS:

```jsx
<SortableRegion id="dashboard:custom:visuals" variant="freeform"
  authoredRevision={2} className="custom-bento">
  <SortableItem id="hero-chart" label="Regional performance"
    kind="chart" style={{ gridRow: "span 2" }}>
    <DataComponent id="hero-chart" queryId="usage_summary"
      title="Regional performance" kind="chart">
      <CustomRegionalVisual />
    </DataComponent>
  </SortableItem>
  <SortableItem id="compact-trend" label="Growth trend" kind="chart">
    <DataComponent id="compact-trend" queryId="usage_summary"
      title="Growth trend" kind="chart"><CompactTrend /></DataComponent>
  </SortableItem>
</SortableRegion>
```

```css
.custom-bento {
  display: grid;
  grid-template-columns: 2fr repeat(5, minmax(0, 1fr));
  grid-auto-rows: minmax(120px, auto);
  gap: 18px;
}
```

Omit the `columns` prop in freeform mode and declare any desired track count and
placement in authored CSS. Freeform regions retain protected dragging, keyboard access, source inspection,
owner-only editing, hidden-item reconciliation, and saved order without imposing a
12-column grid, canvas rows, minimum spans, automatic resizing, or divider handles.
The agent may use CSS Grid areas, equal fifths, any track count, flexbox, vertical
spans, dense small multiples, custom visuals, and arbitrary internal React markup.
Keep a tightly coupled visualization together as one draggable composite, or author
an ordinary fixed `DataComponent` section outside the sortable region when preserving
a user-requested layout is more important than independently moving every piece.

Separate freeform regions may opt in to exchanging compatible blocks by sharing a
safe `transferGroup`, for example `transferGroup="dashboard:related-charts"`.
Only blocks of the same kind and comparable dimensions transfer; their stable
component identities, reviewed sources, protected actions, and region layouts persist.
Leave unrelated freeform, canvas, report, and fixed bespoke sections ungrouped.

For an existing custom dashboard, wrap its composed content in
`src/content/shared/SortableDashboardLayout.jsx`. Compatible ordinary authored grids are
measured and promoted into full protected canvas rows, preserving their original
relative widths while restoring smooth sibling motion, adjacent-card resize dividers,
and persisted ordering and widths. Explicit grid areas, vertical row spans, and other
incompatible creative layouts retain freeform movement. In owner Edit mode, verify a
real pointer drag and adjacent-card resize; both changes must survive reload.

Increase `authoredRevision` only when the user explicitly requests replacing an
existing layout. The new authored order/placement then overrides that region's stale
saved layout without resetting other regions, filters, titles, reviewed values,
permissions, or source metadata. Routine edits must leave the revision unchanged.

`variant="stack"` preserves report editorial flow; keep each report section's narrative
and supporting evidence together instead of converting the document to a dashboard
grid; row dividers and dashboard row insertion do not apply to reports. Pass its complete
stable `authoredOrder` when sections can be hidden so restoring a section preserves its
original slot even before the first saved reorder.
`variant="grid"` remains available for an intentionally isolated fixed-column group;
prefer `variant="freeform"` when the model should own its complete spatial layout.
Choose semantic row membership, block kinds, spans, and readable minimums from actual
reviewed content rather than forcing a universal component count or composition. Keep
shared page controls pinned; report summaries and introductions may be ordered when useful. The
protected runtime owns pointer/keyboard interaction, permissions, motion, hide/restore,
constrained width changes, and presentation-only persistence.

Use `Table` for reviewed tabular data. Its `columns` definitions support shared
`identity` (with `secondaryField`), `sparkline`, `bar`, `status`, and `percent` cell
presentations, including theme-aware visuals and accessible tooltips. They require no
example-specific CSS. Keep unusual row density and chosen columns in authored content.
 Put `Filters` beside the page title only when
relevant page-wide KPIs and charts respond; render section-exclusive definitions beside
their first affected heading with `SectionHeader` and `useSectionFilters` instead. For one consumer of a shared query, place
`InlineFilters` beside its component, keep selection in local state, and pass the same
filtered reviewed rows as `displayRows` and `sourceRows` to `DataComponent`.
Mark authored copy with `EditableText` or `data-editable-narrative`, assign
stable `data-editable-id` values, and mark custom source collections with `data-reviewed-rows`.
Keep reviewed values, rows, controls, statuses, chart axes, and computed outputs read-only;
visible captions are editable, while tooltip/source metadata is not. Put user-owned logos
and custom visual assets in `src/content/assets/`.
