# Data App Authoring Guide

Read this file before planning or editing this report or dashboard. Its ownership and
interaction boundaries continue to apply when a later task reopens the generated app.

## Editing boundary

- Build app-specific UI, sections, custom visualizations, reusable helpers, and approved
  brand assets anywhere beneath `src/content/`.
- Reports start in `src/content/report/ReportContent.jsx` and use
  `src/content/report/report.css`. Dashboards start in
  `src/content/dashboard/DashboardContent.jsx` and use
  `src/content/dashboard/dashboard.css`.
- `src/content/shared/` is available for shared authored components;
  `src/content/assets/` is available for user-approved logos, images, and custom icons.
- Update reviewed rows and exact source provenance in `src/data.json`.
- Customize approved theme tokens in `src/theme.css`. Use
  `--data-app-chrome-background` and `--data-app-chrome-text` to style protected top-bar
  branding without changing its layout, visibility, or functionality. The protected shell
  preserves readable label, menu, Publish, and icon contrast; product typography; and the
  View/Edit toggle's shape and geometry regardless of authored theme tokens.
- The protected top bar and its dashboard tabs span the full viewport with responsive
  surface gutters, independent of narrow report columns or dashboard content max widths.
  Adjust authored content width, sections, and padding normally; never restyle, replace,
  or manually reposition protected chrome.
- Dashboard content defaults to an actual 1440px usable column with 32px desktop and
  16px mobile gutters. A genuinely dense dashboard may deliberately use the 1600px
  preset by setting `--data-app-layout-intent: wide` and
  `--data-app-content-width: var(--data-app-dashboard-wide-content-width)` on `.page`.
- The sample dashboard is an example, not a required template. Choose chart containers,
  borders, period controls, date pickers, tabs, and sections only when the reviewed data
  and requested dashboard actually benefit from them. Do not add decorative right-aligned
  labels, duplicated reporting dates, filler descriptions, or unnecessary dividers.
- Preserve responsive gutters inside narrow embedded previews. Metric values, sparklines,
  and deltas must fit their cards; filters should wrap only when their row is genuinely
  full; and wide evidence tables must scroll inside their own reviewed-data container.
- Right-align quantitative table values, including formatted percentages and currency;
  keep text and miniature visualization columns left-aligned. Omit redundant chart-axis
  titles when the chart title and tick labels already explain that dimension.
- A geographic map must use real projected geography and correctly positioned reviewed
  coordinates. Reuse `src/content/dashboard/regional-world-map.js` when suitable; do not
  describe an arbitrary schematic or regional bubble grouping as a map.
- Reports default to one actual 748px editorial content column for narrative and visual
  evidence, with 36px desktop and 20px mobile gutters. Widths exclude gutters: the
  default report frame is 820px on desktop. Keep headings, narrative, and evidence aligned.
  When the evidence benefits from a different report width, set
  `--data-app-layout-intent: authored-report` and `--data-app-content-width` on
  `.report-page` in report content CSS. Choose readable prose and responsive evidence
  layouts; this content-only choice does not need another user request.
- Inherit the report's font family, readable mid-tone body text, and spacing. The default
  title is 48px (36px on mobile), body text is 16px with a 1.6 line height, and section
  headings are 20px. Keep prose unboxed. Chart cards use 24px padding and corners, a
  faint border and subtle shadow; narrow screens reduce padding to 16px vertically and
  12px horizontally. Start ordinary charts at 300px of chart content, not total card
  height. Allow more height for categories, labels, legends, or annotations when needed.
  Use a muted comparison color for contextual series when it clarifies the story; choose
  distinct category colors only when they help identify those categories consistently.
- Full-bleed dashboard section backgrounds may span the viewport when `.page` declares
  `--data-app-layout-intent: full-bleed`; their meaningful inner content must still use
  the selected dashboard content column and standard responsive gutters.
- If the user explicitly requests a different width or gutter, set
  `--data-app-layout-intent: user-requested` and `--data-app-content-width: <requested>`
  on the authored `.page` or `.report-page`. This approved content-only change requires
  no confirmation and must not modify protected infrastructure. Dashboard generation
  continues to use its approved width presets; reports may use the authored-report option above.
- For an app-wide dark, branded, or restyled experience, update the existing `:root`
  palette in `src/theme.css`: `--background`, `--surface`, `--surface-raised`,
  `--control`, `--control-hover`, `--text`, `--secondary`, `--border`, chart colors, and
  related tokens. Set `--data-app-chrome-background: var(--background)` and
  `--data-app-chrome-text: var(--text)` when chrome should follow that palette. These
  theme-only edits are explicitly approved, require no confirmation, and preserve the
  existing theme-switching infrastructure. Do not move a whole-app palette into `.page`,
  `.report-page`, or `[data-data-app-content]`; that strands white chrome, page gutters,
  or filter controls around otherwise dark content.
- Import protected charting, reviewed-data access, tables, filters, formatting, component
  menus, and editable narrative only through `src/data-app-public.jsx`.
- Reuse `DataComponent variant="card"` for standard chart/table/custom cards on either
  surface and `Section` / `SectionHeader` for shared headings, spacing, and optional
  filters. Cards own their padding, theme radius, 60% corner smoothing, border, and
  shadow; do not recreate those rules in authored CSS. `MetricCard` is a card by
  default; `DataComponent` defaults to `variant="plain"` for report/editorial content.
  See `src/content/COMPONENTS.md` for the shared API.
- Use `useSectionFilters` with `Filters` in a section header for section-local scope.
  Apply its `componentProps` to every affected source-backed block and render its
  filtered reviewed rows so display, copy, sources, and editing remain consistent.
  Never filter already-aggregated page rows. New reports without visible page-wide
  controls must use `snapshot.filters: []`, not hidden dashboard filters.
- Use the public `MetricCard` for ordinary source-backed dashboard/report KPIs and pass
  `trendValues` from reviewed historical rows when available. Its compact layout, inline
  comparison, sparkline, theme geometry, component menu, and source inspection are shared;
  do not recreate older stacked cards or append repetitive comparison-date labels.
- Reuse public `ChartRenderer` defaults for theme-aware adaptive heatmaps, centered
  donuts, localized country labels, and reviewed Sankey flows. Preserve standard responsive
  page gutters inside narrow embedded previews as well as full-width browser windows.
- The protected shell owns the application's only `main` landmark; authored content
  must use `article`, `section`, or `div` instead of rendering a second nested `main`.

`AGENTS.md`, `src/App.jsx`, `src/DataAppRuntime.jsx`, `src/DataAppShell.jsx`, `src/DataAppContext.jsx`,
`src/data-app-public.jsx`, shared components, charting, hooks, action handlers,
presentation, publication, source inspection, official chrome icons, `src/data-app-worker.js`, the Worker, build
wiring, `protected-runtime.json`, and protected scripts are product-owned infrastructure.
Do not edit, replace, delete, shadow, hide, disable, or bypass them during ordinary app
creation, styling, or revision.

## Protected behavior

Preserve exactly one real application top bar, theme switching, dashboard refresh,
component menus, source inspection, chart editing, inline title/narrative editing,
autosave, stable component/query identities, reviewed provenance, direct browser PDF
printing, and non-PDF export/publishing handoffs. Preserve the existing project, artifact
identity, published destination, and sharing/access boundaries.
Reports retain their snapshot date as plain metadata, without a data-only refresh
control. Set `report.asOf` to an explicit `YYYY-MM-DD` evidence cutoff when known;
the header then says “As of.” Otherwise it says “Prepared” using `generatedAt`.
Never infer a shared cutoff from unrelated source dates or change it after a cosmetic
edit. Keep differing source coverage beside the relevant evidence. A requested report
revision must reconcile evidence and narrative together.

Give each new app a fresh, stable top-level `id` in `src/data.json` (for example,
a UUID). Keep it unchanged when the title, evidence, or presentation changes.
To rename an older app that has no ID, first assign a new safe ID and record its
exact old snapshot title as `legacyPresentationTitle`. Keep both values in any
data reproducer. The runtime checks only that explicit old title at the same
app path, preserves an existing stable record, and never deletes legacy edits.
Do not scan browser storage, copy another app's ID, or implement an authored
storage migration. The ordinary title editor remains the simplest way to rename
the current presentation.

Choose the least restrictive layout that supports the requested dashboard:

- Use one product-owned `SortableRegion variant="canvas"` when independently movable
  blocks should move between semantic rows and support shared-divider resizing. Author
  the rows, component IDs, responsive spans, and readable minimums. Twelve desktop
  columns and metric/chart/table minimums apply only to this optional canvas mode;
  they are not a required page format or a restriction on authored React/CSS.
- Use `SortableRegion variant="freeform"` for an intentionally unusual editable
  composition. Author its actual layout with scoped CSS Grid, named areas, flexbox,
  masonry-like placement, vertical row spans, arbitrary column counts, equal fifths,
  compact small multiples, or custom `SortableItem` styles. This mode preserves
  protected pointer/keyboard reordering, stable component identity, permissions, and
  presentation persistence without imposing canvas rows, 12 columns, semantic width
  minimums, automatic rebalancing, or row resize dividers. Omit the `columns` prop in
  freeform mode and declare any desired track count and geometry in authored CSS.
- Compose an internally complex visualization or grouped bento section as one
  draggable `SortableItem` when its internal elements belong together. Use an ordinary
  authored fixed section without a sortable region when a user-requested composition
  must remain fixed; retain `DataComponent` wrappers, reviewed source access, inline
  editing, existing actions, and all other protected behavior. A fixed or custom
  section may coexist beside an editable canvas or freeform region.

The agent chooses which mode, section structure, component count, chart types, nesting,
heights, proportions, and custom visuals best answer the request; never reshape a
creative design into the starter dashboard merely to enable universal dragging. For an
explicitly requested redesign of an existing region, increment that region's positive
`authoredRevision` prop. Its new authored arrangement then replaces only that region's
stale saved layout while preserving other regions, user titles, filters, reviewed data,
sharing, and source provenance. Do not increment it for routine data, copy, or styling
changes: existing user rearrangements must continue to win.

Canvas movement favors existing rows and rebalances only when row membership changes;
new rows require an intentional pause. Dense tables reject incompatible neighbors.
Locked blocks and hidden generated rows retain their positions. A compact shared
divider safely resizes horizontal neighbors; a lone canvas block has an edge handle
that preserves its chosen width without requiring a neighbor;
component menus never expose redundant Width presets. Start direct dragging only from
aligned component headers, metric surfaces, or explicit `data-block-drag-surface`
elements; preserve reviewed-text selection, chart gestures, original inline-text hover,
and focus-visible keyboard movement without permanent handles or movement icons.
Reports may use `variant="stack"` for independently reorderable editorial sections.
Keep shared page controls outside the sortable region. The
protected runtime owns interactions, owner-only Edit mode, responsive behavior, and
presentation-only layout persistence; reviewed rows and source provenance stay separate.

Dashboards retain their authored filters, tabs, scenarios, scheduled refresh, and
duplication. Reports default to an answer-first editorial flow, readable prose, and
restrained containers. No summary, section title, audience category, chart count, or
business schema is required. Use wider evidence, metrics, methods, or separate reader
views only when the task benefits. Dashboard-only scheduling and duplication stay in
the dashboard shell.

### Report composition map

Import these existing primitives from `src/data-app-public.jsx`:

- `RichNarrative`: editable Markdown with a stable ID; authored text owns its headings.
  For selected source links, optionally pass `sourcePreviews`, keyed by exact HTTPS
  URL. Each entry has `title`, `summary`, optional `source` and `date`, and
  `approvedForReport: true`. Write the summary from a source actually read, explaining
  what it establishes rather than repeating the title. Approve only text suitable for
  every recipient of the report: previews are embedded in the shared artifact, not an
  access-controlled fetch. Omit restricted excerpts and never put material qualifications
  only in a preview. Unlisted links remain ordinary links. The existing source inspector
  still owns data, SQL, and methodology. Preview tooltips center above the hovered
  link line, shift within the viewport, and fall below when needed. They work on
  deliberate hover (250 ms) or immediate keyboard focus; Escape closes them. On touch, tap once for the
  preview and again to open the original link. Tooltips have no separate action.
  The shared tooltip identifies known source providers from the validated destination
  and displays a bundled provider icon. Unknown destinations use a neutral source icon;
  do not add remote favicon requests or treat a provider icon as a trust badge.
- `ReportSection`: optional source-backed narrative wrapper with component actions;
  use `showHeading={false}` when its Markdown already contains the heading.
- `DataComponent`, `ChartRenderer`, `DataTable`, and `MetricCard`: evidence and source
  inspection. Charts and tables use one reviewed query each.
  `DataTable` columns with `presentation: "percent"` round to whole percentages.
  When finer rate differences matter, provide formatted display values while keeping
  raw numeric rows in `sourceRows`; do not round nonzero rates or meaningful changes away.
  `DataTable` columns can set `deltaTone` to `"positive"`, `"negative"`, `"neutral"`,
  or a pure, synchronous `(value, row) => tone`. Use reviewed evidence and the metric's desired direction
  to choose a tone; keep inconclusive or ambiguous results neutral. This changes
  color only, preserving signs, values, sorting, and existing table interactions.
  Keep interpretation and material uncertainty readable without color. With no
  `deltaTone`, existing `signedDeltas` sign-based coloring is unchanged; an invalid
  explicit tone or a failing callback resolves to neutral rather than inferring benefit from the sign.
  Line, area, bar, and horizontal-bar specs may include up to eight authored
  `annotations`. Each has a stable `id`, `kind`, and short factual `label`.
  Supply the label as one nonempty plain-text string of at most 160 characters,
  without line breaks, tabs, or other control characters; the renderer wraps it.
  A `benchmark` names its plotted `measure` and a numeric reviewed `field` that is constant across plotted
  rows, or supplies `at` to select one unambiguous x value. An `event` names an
  exact ISO-date `at` and a reviewed text/boolean evidence `field`. A `range` names
  exact ISO-date `at` and `end` values. A `point` names an exact `at` and a visible
  plotted `field`. Do not supply arbitrary y values, rows, SQL, or another query.
  Include event/target definitions and lineage in the same reviewed query's source.
  Missing, conflicting, filtered-out, or unsupported anchors are omitted. Annotations
  are plain, noninteractive text. Fit them inside the actual plot first, without
  covering evidence or adding a permanent outer label band. Prefer compact,
  left-aligned wording that the renderer can wrap to two lines beside a point, date, or period. Chart-wide
  horizontal benchmark notes use a plot edge with matching text justification,
  preferring the right edge when a safe nearby fit exists. The text and its marker
  or arrow share one readable theme color. Preserve whole words and the full label
  when a different wrap is needed. Nearby text needs no
  connector; displaced labels may use an arrow pointing toward their anchor. When a full
  label cannot fit inside the plot, the figure shows it as a plain note
  below the plot. Source inspection, print, and copied images retain its evidence.
  Use annotations only for additional sourced context absent from the chart, such
  as a documented operational change, one-time adjustment, or measurement change.
  Do not restate visible trends or comparisons. Zero annotations is a valid choice.
  An event establishes timing, not causation; do not invent context or confidence
  intervals. The chart editor's Show annotations switch hides or restores all authored
  annotations for that chart, including print and copied images, without deleting them.
  Annotations default to visible (`showAnnotations !== false`). There is no annotation
  text editor or focus UI.
- `useDataApp`: current reviewed queries/rows, period comparisons, visibility, chart
  presentation, and shell actions. Use `reviewedPeriodRows(queryId, { period })` for
  `history`, `latest`, or `previous`, and `reviewedAggregatePeriodRows` for comparisons
  without breakdown rows. Set a query's `reportingField` when its time grain is ambiguous.
  A report ignores dashboard filters unless they are declared visible for the report
  (for example in `snapshot.report.visibleFilterIds`) and controls are visibly authored.
- `SortableRegion` / `SortableItem`: optional persisted section ordering.

For a small task link beside an editable recommendation, use the authored helper
`src/content/shared/ReportTaskLink.jsx` with `id`, `narrativeId`, `text`, `queryId`,
optional `queryIds` and `period`, and the visible label as children. Use the real
component and narrative IDs so the task retains the current saved recommendation.
The helper uses the shared `reportFollowUpHref` handoff, renders only when launch is
available in View mode, and supplies the accent link, arrow, and spacing. Keep the
recommendation's `RichNarrative` immediately before its link in one `ReportSection`
or `div`; use `.report-recommendations` around separate recommendation groups. The
helper aligns with a preceding single Markdown bullet automatically.
Do not duplicate task URL or permission logic. The default is a chat-only investigation.
For a specific reviewable draft, add `intent="prepare"` and a short plain-text
`deliverable` (at most 160 characters), such as `"recovery flow"`. The output label
stays separate from saved narrative edits. Opening an unsent task does not mean it has
started and does not authorize report edits, implementation, messages, or external writes.
Prepare requests cannot use the direct-submit path.

For optional calculation or method detail, the editable example helper
`src/content/shared/ReportDisclosure.jsx` preserves its open/closed state in Edit mode
and includes its content in print. Place it inside a visible source-aware component, not around the component
whose permalink and source menu must remain reachable. Keep the main evidence and
material limitations visible; avoid placing a responsive chart in a closed container.

Compose ordinary React and scoped CSS in `src/content/report/`. For a narrative supported
by several queries, give `ReportSection` a primary `queryId`, ordered `queryIds`, and,
when needed, `sourceRowsByQuery` containing exact rows from each current artifact query.
The source drawer and Copy data action retain each source independently. The runnable report is a complete short example, not a placeholder outline. Establish the user's question and analysis before choosing its composition; replace the example's topic and evidence rather than filling its existing sections. Use `examples/README.md` to find a relevant finished report and understand why its author chose that form. The `build-report` skill owns the analytical and editorial quality criteria; examples demonstrate those judgments, not required outlines.

Every source-backed `DataComponent` needs a stable `id` and existing reviewed `queryId`.
Use `EditableText` or `data-editable-narrative` for authored copy and stable
`data-editable-id` values for independently editable blocks. Keep reviewed values, tables,
custom rows, controls, statuses, chart axes, and computed outputs read-only; identify custom
source collections with `data-reviewed-rows`. Visible captions are editable; tooltip/source
metadata is not. These are inline-editor boundaries, not a ban on user-requested data
corrections. Revise artifact-local data through `src/data.json` or the existing authorized
hosted update path, and update source notes/caveats so corrected values are not falsely
presented as an unchanged external query result. Never silently write back to external
source systems or alter protected chrome and controls.

For every editable report narrative, import `RichNarrative` from `src/data-app-public.jsx`
and give each executive or technical summary, introduction, finding, interpretation,
recommendation, caveat, and other prose block its own stable `id` and Markdown `value`.
Do not author report prose as plain `data-editable-narrative` paragraphs, lists, or
headings: those legacy fields do not mount the shared formatting toolbar. Owners can
select authored text in Edit mode to format links, medium-weight emphasis, italics,
headings, paragraphs, lists, and checklists with the existing shared Lexical toolbar.
Keep metric labels, chart/table titles, reviewed values, and source data on their existing
protected editing paths; never attach this editor to reviewed values or expose it to viewers.

Content CSS must not target protected shell selectors, global document roots, or
visibility/interaction of product chrome. Content JavaScript must not directly remove,
disable, replace, or globally overlay protected product DOM.

## Explicit protected changes

An explicit user request is authorization to change the protected behavior or
infrastructure needed to fulfill that request. Identify the affected components, keep
the change scoped, and proceed without asking for redundant confirmation.
Only ask for permission when the agent proposes a protected change the user did not
request. Vague requests such as "clean this up," "make it custom," "remove clutter," or
"start over" do not authorize unrelated shell changes.

For a user-requested or separately approved change, edit only the authorized paths. Use the absolute Codex Node executable returned by `load_workspace_dependencies` as `<codex-node>` and run from this app's directory:

```sh
DATA_APP_USER_CONFIRMED=1 "<codex-node>" scripts/authorize-protected-change.mjs \
  --confirmed --scope src/components/DataAppChrome.jsx \
  --reason "User requested moving refresh into the overflow menu"
```

List each explicitly authorized path with its own `--scope`. This authorization is scoped
to the confirmed change and must never persist as a general unlock. Never run
`integrity:authorize` without an explicit user request or approval, weaken runtime checks, or manually
rewrite the integrity manifest.

`DATA_APP_MAINTAINER=1 "<codex-node>" scripts/verify-protected-runtime.mjs --update --maintainer` is reserved for maintainers changing the canonical starter itself; it is not a normal artifact-authoring or user-confirmation workflow.

## Build and preview

Call `load_workspace_dependencies` for Codex's absolute Node executable, resolve the installed Data plugin root, and run `"<codex-node>" "<data-plugin-root>/scripts/data-app.mjs" build --project-dir "<app-project>"`. This single command verifies the installed prebuilt runtime and authored boundary, compiles editable local source and assets, and embeds reviewed data in self-contained HTML. Ordinary content edits need no separate `prepare`, copied verifier, full test suite, npm, network access, or project `node_modules`. Leave copied protected source intact; it need not match the current starter or require upgrade confirmation or an old plugin cache. Report unavailable imports or runtime assets instead of fetching CDN libraries or reconstructing the app.

Follow the installed Data plugin's shared first-preview policy: build and open the first useful source-backed view early, then continue the full requested scope in the same app. Do not deliver a placeholder or remove existing sections to shorten the work. Rebuild after content or data changes and refresh the same preview tab. Publication remains separate; it rebuilds missing or outdated-runtime HTML and otherwise preserves the reviewed same-runtime client.

Do not run browser screenshots, DOM inspection, clicking, resizing, or visual QA unless the user explicitly requests browser testing or a specific reported failure needs diagnosis. Keep testing scoped to that request or failure; do not automatically test every shared feature or claim checks that were not performed.

Static imports, re-exports, and module cycles are supported. The default build rejects authored dynamic `import()` calls, including literal paths, top-level `await`, and unsupported `import.meta` expressions. Use the explicit `--source` path when those features are required; do not change lazy-import side effects merely to bypass the error.

After an explicitly confirmed protected-source change, or for an explicitly selected build using an unbundled npm package, add `--source` to the same build command. This path compiles copied runtime source and runs `"<codex-node>" scripts/verify-protected-runtime.mjs`; never silently replace requested custom behavior with the prebuilt runtime. It requires compatible Vite dependencies already installed locally and never installs packages or runs as an automatic fallback. Report missing dependencies; ordinary content edits or publication must not trigger an npm install.

The copied `npm test` suite is for maintainers of the canonical starter. Some tests intentionally assert its bundled example data, content IDs, or plugin-relative files; they are not requirements for a newly authored report or dashboard. Do not weaken protected tests to make a custom layout resemble the starter.

If source-build verification fails, restore copied protected infrastructure or complete the explicit,
confirmed, narrowly scoped authorization flow. Do not create a second renderer, bypass
the verifier, replace the app with static HTML, or change publication/access settings
unless the user explicitly requests or approves the applicable protected change.
