# Responsive funnel examples

Run `npm run dev` from the template root and open `/examples/funnel/`.
All rows are synthetic, explicitly labeled examples. Cards, chart editing, themes,
source inspection, image/data copying, and the funnel itself use shared components.

The shared chart uses its container width divided by stage count: 144px or more
is horizontal, 120–143px is compact horizontal, and below 120px is vertical rows
with proportional bars. The example stacks cards earlier so medium screens can
still compare complete horizontal funnels. No stage is hidden behind scrolling.

The first stage uses the full core theme color; later stages use lighter tints.
Stage labels and percentages show share of the first stage. Hover/focus reveals
exact values, previous → current conversion, and signed drop-off or increase.
Touch pins details for inspection; Done/tap-away dismisses them. Ask remains an
explicit action where permitted. Missing values leave gaps and zeros stay zero.
Exports preserve every stage in the current responsive layout.
