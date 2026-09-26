import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  reviewedComponentClipboard,
  reviewedNarrativeQueries,
  reviewedQueryLabel,
  reviewedRowsAsTsv,
  reviewedSource,
  safeSourceHref,
  scopedMetricDefinitions,
} from "../src/source-provenance.js";

const queries = {
  current: {
    label: "Current results",
    rows: [{ period: "2026-07", value: 12, detail: { count: 3, label: "A" } }],
    source: {
      sql: "SELECT value FROM current_results",
      tables: ["current_results"],
      metricDefinitions: [{ label: "Value", definition: "Current count", componentIds: ["finding"] }],
      evidenceFlow: [{ title: "Current evidence", detail: "Current query" }],
    },
  },
  comparison: {
    rows: [{ period: "2026-06", value: 10 }],
    source: {
      label: "Comparison results",
      sql: "SELECT value FROM comparison_results",
      tables: ["comparison_results"],
      metricDefinitions: [{ label: "Value", definition: "Comparison count", componentIds: ["finding"] }],
      evidenceFlow: [{ title: "Comparison evidence", detail: "Comparison query" }],
    },
  },
};
const component = { id: "finding", kind: "narrative", queryId: "current", queryIds: ["comparison"] };

test("narrative evidence declares ordered, unique, existing query identities", () => {
  assert.deepEqual(reviewedNarrativeQueries({ ...component, queryIds: ["comparison", "current"] }, queries),
    ["current", "comparison"]);
  assert.deepEqual(reviewedNarrativeQueries({ ...component, kind: "custom" }, queries), ["current", "comparison"]);
  assert.deepEqual(reviewedNarrativeQueries({ id: "chart", kind: "chart", queryId: "current" }, queries), ["current"]);
  assert.throws(() => reviewedNarrativeQueries({ ...component, queryIds: "comparison" }, queries), /to be an array/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component, queryIds: [""] }, queries), /non-empty/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component, queryIds: ["missing"] }, queries), /missing reviewed query/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component, queryIds: ["constructor"] }, queries), /missing reviewed query/u);
  for (const kind of ["chart", "table", "metric"]) {
    assert.throws(() => reviewedNarrativeQueries({ ...component, kind }, queries), /only for narrative/u);
  }
  assert.throws(() => reviewedNarrativeQueries({ ...component, chart: { x: "period" } }, queries), /only for narrative/u);
});

test("scoped evidence accepts actual snapshot rows independent of object key order", () => {
  const scoped = { current: [{ detail: { label: "A", count: 3 }, value: 12, period: "2026-07" }], comparison: [] };
  assert.deepEqual(reviewedNarrativeQueries({ ...component, sourceRowsByQuery: scoped }, queries),
    ["current", "comparison"]);
  assert.throws(() => reviewedNarrativeQueries({ ...component, sourceRowsByQuery: [] }, queries), /map reviewed query/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component, sourceRowsByQuery: { extra: [] } }, queries), /undeclared/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component, sourceRowsByQuery: { current: {} } }, queries), /array of scoped rows/u);
  assert.throws(() => reviewedNarrativeQueries({ ...component,
    sourceRowsByQuery: { current: [{ ...queries.current.rows[0], value: 99 }] } }, queries), /non-reviewed rows/u);

  const revisedRow = { ...queries.current.rows[0], value: 99 };
  const revisedSnapshot = { ...queries, current: { ...queries.current, rows: [revisedRow] } };
  assert.deepEqual(reviewedNarrativeQueries({ ...component,
    sourceRowsByQuery: { current: [revisedRow] } }, revisedSnapshot), ["current", "comparison"],
  "A requested artifact-data revision becomes the current reviewed snapshot; this is not a permanent value lock");
});

test("copy preserves single-source TSV and includes each selected narrative source", () => {
  const scope = { current: [], comparison: queries.comparison.rows };
  const registered = { ...component, queryIds: reviewedNarrativeQueries(component, queries) };
  const copied = reviewedComponentClipboard(registered, queries, (queryId) => scope[queryId]);
  assert.equal(copied, "Current results\n\n\nComparison results\nperiod\tvalue\n2026-06\t10");
  assert.equal(reviewedComponentClipboard({ queryId: "comparison" }, queries, (id) => scope[id]),
    reviewedRowsAsTsv(queries.comparison.rows));
  assert.equal(reviewedRowsAsTsv([{ label: "one\ttwo\nthree", value: 0 }, { extra: "yes" }]),
    "label\tvalue\textra\none two three\t0\t\n\t\tyes");
  assert.equal(reviewedQueryLabel({ source: { query: { description: "Reviewed query" } } }, "id"), "Reviewed query");
  assert.equal(reviewedQueryLabel(undefined, "id"), "id");
});

test("each narrative source retains its own SQL, evidence and metric definitions", () => {
  for (const queryId of reviewedNarrativeQueries(component, queries)) {
    const source = reviewedSource(queries[queryId].source);
    assert.equal(source.sql, queries[queryId].source.sql);
    assert.deepEqual(source.evidenceFlow, queries[queryId].source.evidenceFlow);
    assert.deepEqual(scopedMetricDefinitions(source.definitions, component.id), queries[queryId].source.metricDefinitions);
  }
  assert.equal(safeSourceHref("https://example.test/query?token=private"), null);
  assert.equal(safeSourceHref("https://example.test/tokens/private"), null);
  assert.equal(safeSourceHref("https://example.test/query/123"), "https://example.test/query/123");
});

test("source selection preserves lazy loading, shared drawer behavior and copy permissions", async () => {
  const inspector = await readFile(new URL("../src/components/SourceInspector.jsx", import.meta.url), "utf8");
  assert.match(inspector, /ready \? getSource\(selectedQueryId\) : null/u);
  assert.match(inspector, /label="Choose reviewed data source"/u);
  assert.match(inspector, /queryIds\.length > 1/u);
  assert.match(inspector, /queryId: selectedQueryId, sourceRows: source\?\.rows/u);
  assert.match(inspector, /<SourceInspector component=\{selectedComponent\} \{\.\.\.source\} allowCopy=\{allowCopy\}/u);
  assert.match(inspector, /<DataTable key=\{component\.queryId\}/u);
  assert.match(inspector, /setCopied\(false\), \[component\.queryId\]/u);
});
