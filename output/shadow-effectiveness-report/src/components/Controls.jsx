import * as DropdownPrimitive from "@radix-ui/react-dropdown-menu";
import React, { useEffect, useId, useMemo, useRef, useState } from "react";

import { deltaDirection, displayValue, label as chartLabel } from "../charting/chart-theme.js";
import { compareTableValues, distributionBuckets, distributionPercentiles, numericValue, resolveDeltaTone, statusTone } from "../charting/table-data.js";
import { TableSparkline } from "../charting/TableSparkline.jsx";
import { isTemporalField } from "../source-provenance.js";
import { Icon } from "./Icon.jsx";
import { useSectionFilterPlacement } from "./Section.jsx";
import { Select, Tooltip } from "./ui.jsx";

export const Dropdown = Select;

function columnLabel(field) {
  return chartLabel(field).split(" ").map((word, index) =>
    index && !/^[A-Z\d]+$/u.test(word) ? word.toLowerCase() : word).join(" ");
}

function numericColumn(rows, column) {
  return rows.some((row) => typeof row[column] === "number"
    || /^[+-]?(?:[$€£])?\d[\d,.]*(?:\s?[KMBT])?%?$/iu.test(String(row[column] ?? "").trim()));
}

function yearColumn(column) {
  const field = String(column).replace(/([a-z\d])([A-Z])/gu, "$1_$2");
  return /^(?:(?:calendar|fiscal|reporting|academic)[ _-])?year$/iu.test(field);
}

function numericPresentation(rows, column, definition) {
  return numericColumn(rows, column)
    && !yearColumn(column)
    && !["sparkline", "bar", "status"].includes(definition?.presentation);
}

function DateRangePicker({ label, value, choices, formatChoice, onChange, allValue }) {
  const [open, setOpen] = useState(false);
  const earliest = choices.at(-1);
  const latest = choices[0];
  const allDates = allValue ?? latest;
  const [selectedStart, selectedEnd] = typeof value === "string" && value.includes("..")
    ? value.split("..") : [earliest, choices.includes(value) ? value : latest];
  const [pendingStart, setPendingStart] = useState(null);
  const [displayMonth, setDisplayMonth] = useState(() => selectedEnd?.slice(0, 7));
  const month = new Date(`${displayMonth ?? selectedEnd?.slice(0, 7)}-01T12:00:00Z`);
  const earliestMonth = earliest?.slice(0, 7);
  const latestMonth = latest?.slice(0, 7);
  const fullDate = (date) => new Intl.DateTimeFormat(undefined,
    { weekday: "long", month: "long", day: "numeric", year: "numeric", timeZone: "UTC" })
    .format(new Date(`${date}T12:00:00Z`));
  function changeMonth(offset) {
    const next = new Date(month);
    next.setUTCMonth(next.getUTCMonth() + offset);
    setDisplayMonth(next.toISOString().slice(0, 7));
  }
  function applyRange(start, end, close = false) {
    if (!start || !end) return;
    const rangeStart = start > end ? end : start;
    const rangeEnd = start > end ? start : end;
    onChange(rangeStart === earliest && rangeEnd === latest ? allDates : `${rangeStart}..${rangeEnd}`);
    setPendingStart(null);
    if (close) setOpen(false);
  }
  function renderMonth(offset) {
    const calendarMonth = new Date(month);
    calendarMonth.setUTCMonth(calendarMonth.getUTCMonth() + offset);
    const monthValue = calendarMonth.toISOString().slice(0, 7);
    const heading = new Intl.DateTimeFormat(undefined,
      { month: "long", year: "numeric", timeZone: "UTC" }).format(calendarMonth);
    const firstWeekday = calendarMonth.getUTCDay();
    const daysInMonth = new Date(Date.UTC(calendarMonth.getUTCFullYear(), calendarMonth.getUTCMonth() + 1, 0)).getUTCDate();

    return <section key={monthValue} className="date-calendar-month" data-current-month={offset === 0}>
      <header className="date-calendar-header">
        {offset === -1 ? <button type="button" className="date-calendar-nav" aria-label="Previous month"
          disabled={monthValue <= earliestMonth} onClick={() => changeMonth(-1)}>
          <Icon name="chevronLeft" size={16} />
        </button> : <button type="button" className="date-calendar-nav date-calendar-mobile-nav"
          aria-label="Previous month" disabled={displayMonth <= earliestMonth} onClick={() => changeMonth(-1)}>
          <Icon name="chevronLeft" size={16} />
        </button>}
        <strong>{heading}</strong>
        {offset === 0 ? <button type="button" className="date-calendar-nav" aria-label="Next month"
          disabled={displayMonth >= latestMonth} onClick={() => changeMonth(1)}>
          <Icon name="chevronRight" size={16} />
        </button> : <span className="date-calendar-nav-placeholder" aria-hidden="true" />}
      </header>
      <div className="date-calendar-grid" role="grid" aria-label={heading}>
        {["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"].map((day) =>
          <span key={day} className="date-calendar-weekday" aria-hidden="true">{day}</span>)}
        {Array.from({ length: firstWeekday }, (_, index) =>
          <span key={`empty-${index}`} className="date-calendar-empty" aria-hidden="true" />)}
        {Array.from({ length: daysInMonth }, (_, index) => {
          const date = `${monthValue}-${String(index + 1).padStart(2, "0")}`;
          const enabled = date >= earliest && date <= latest;
          const activeStart = pendingStart ?? selectedStart;
          const activeEnd = pendingStart ?? selectedEnd;
          return <button type="button" key={date} className="date-calendar-day"
            aria-label={fullDate(date)} aria-pressed={date === activeStart || date === activeEnd} disabled={!enabled}
            data-in-range={date >= activeStart && date <= activeEnd}
            data-range-start={date === activeStart} data-range-end={date === activeEnd}
            onClick={() => {
              if (!pendingStart) { setPendingStart(date); return; }
              applyRange(pendingStart, date, true);
            }}>{index + 1}</button>;
        })}
      </div>
    </section>;
  }

  return <DropdownPrimitive.Root open={open} onOpenChange={(next) => {
    if (next) { setDisplayMonth(selectedEnd?.slice(0, 7)); setPendingStart(null); }
    setOpen(next);
  }} modal={false}>
    <DropdownPrimitive.Trigger className="filter-trigger date-range-trigger" aria-label={label}>
      <span className="filter-label">{label}</span>
      <span>{formatChoice(value)}</span><Icon name="chevronDown" className="chevron" />
    </DropdownPrimitive.Trigger>
    <DropdownPrimitive.Portal>
      <DropdownPrimitive.Content className="popover date-range-calendar" align="start"
        sideOffset={7} collisionPadding={12} aria-label="Choose reporting date range">
        <div className="date-calendar-months">{renderMonth(-1)}{renderMonth(0)}</div>
        <div className="date-calendar-footer">
          <span className="date-calendar-caption">{pendingStart ? "Select an end date" : "Select a start date"}</span>
          <button type="button" className="date-calendar-reset"
            onClick={() => { onChange(allDates); setPendingStart(null); setOpen(false); }}>All dates</button>
        </div>
      </DropdownPrimitive.Content>
    </DropdownPrimitive.Portal>
  </DropdownPrimitive.Root>;
}

function TableVisualTooltip({ label, value, values, formatValue = displayValue, children }) {
  const id = useId();
  const [hover, setHover] = useState(null);
  const ratioHistory = Array.isArray(values) && values.length > 0
    && values.every((point) => Number.isFinite(point) && point >= 0 && point <= 1)
    && values.some((point) => !Number.isInteger(point));
  const reviewedLabel = ratioHistory ? label.replace(/\s+trend$/iu, " rate") : label;
  const reviewedValue = (point) => ratioHistory
    ? new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 1 }).format(point)
    : formatValue(point);
  function show(event) {
    const bounds = event.currentTarget.getBoundingClientRect();
    const pointerX = typeof event.clientX === "number" && event.clientX > 0
      ? event.clientX : bounds.right;
    const fraction = Math.max(0, Math.min(1, (pointerX - bounds.left) / Math.max(1, bounds.width)));
    const point = Array.isArray(values) && values.length
      ? Math.min(values.length - 1, Math.floor(fraction * values.length)) : null;
    setHover({
      left: Math.max(112, Math.min(globalThis.innerWidth - 112, pointerX)),
      top: Math.max(48, bounds.top - 8),
      point,
    });
  }
  return <span className="table-visual-trigger" tabIndex={0} aria-describedby={hover ? id : undefined}
    onPointerEnter={show} onPointerMove={show} onPointerLeave={() => setHover(null)}
    onFocus={show} onBlur={() => setHover(null)}>
    {children}
    {hover && <Tooltip portal visible id={id} className="table-visual-tooltip"
      style={{ left: hover.left, top: hover.top }}>
      <span className="table-visual-tooltip-label">{reviewedLabel}</span>
      <strong>{hover.point === null ? value : reviewedValue(values[hover.point])}</strong>
    </Tooltip>}
  </span>;
}

export function Filters({ filters = [], queries, values, onChange, sticky = false,
  ariaLabel = "Data app filters", clearLabel = "Clear all", showClear = true, dateAllValue, sectionScope }) {
  const filterBarRef = useRef(null);
  const placement = useSectionFilterPlacement(sectionScope);

  useEffect(() => {
    if (!sticky) return undefined;
    const filterBar = filterBarRef.current;
    const topbar = filterBar?.closest(".dashboard-root")?.querySelector(".dashboard-topbar");
    if (!filterBar || !topbar) return undefined;
    const updateStickyState = () => {
      const topbarBounds = topbar.getBoundingClientRect();
      const filterBounds = filterBar.getBoundingClientRect();
      filterBar.toggleAttribute(
        "data-stuck",
        filterBounds.top <= topbarBounds.bottom + 1 && filterBounds.bottom > topbarBounds.bottom + 1,
      );
    };
    updateStickyState();
    window.addEventListener("scroll", updateStickyState, { passive: true });
    window.addEventListener("resize", updateStickyState);
    return () => {
      window.removeEventListener("scroll", updateStickyState);
      window.removeEventListener("resize", updateStickyState);
      filterBar.removeAttribute("data-stuck");
    };
  }, [filters.length, sticky]);

  if (!filters.length || !placement.header) return null;
  const hasActiveFilters = filters.some(({ id, defaultValue }) =>
    (values[id] ?? defaultValue ?? "all") !== (defaultValue ?? "all"));

  return (
    <section ref={filterBarRef} className={sticky ? "filters filter-bar" : "filters"} aria-label={ariaLabel}>
      {filters.map((filter) => {
        const scopedQueries = Array.isArray(filter.queryIds)
          ? filter.queryIds.map((queryId) => queries[queryId]).filter(Boolean)
          : Object.values(queries);
        const choices = [...new Set(scopedQueries.flatMap(({ rows }) => rows
          .map((row) => String(row[filter.field] ?? ""))
          .filter((value) => value && value.toLowerCase() !== "all")))];
        const temporal = isTemporalField(filter.field, filter.type ?? filter.valueType);
        if (temporal) {
          choices.sort((left, right) => {
            const leftDate = Date.parse(left);
            const rightDate = Date.parse(right);
            return Number.isFinite(leftDate) && Number.isFinite(rightDate)
              ? rightDate - leftDate : right.localeCompare(left, undefined, { numeric: true });
          });
        }

        const label = temporal && filter.mode === "through" ? "Date range"
          : temporal && /^as of$/i.test(filter.label.trim()) ? "Reporting date" : filter.label;
        const formatChoice = temporal ? (choice) => {
          if (choice === "all") return "All dates";
          if (typeof choice !== "string") return choice;
          const format = (value, year = false) => new Intl.DateTimeFormat(undefined,
            { month: "short", day: "numeric", ...(year ? { year: "numeric" } : {}), timeZone: "UTC" })
            .format(new Date(`${value}T00:00:00Z`));
          if (choice.includes("..")) {
            const [start, end] = choice.split("..");
            return `${format(start)} – ${format(end, true)}`;
          }
          if (!/^\d{4}-\d{2}-\d{2}$/.test(choice)) return choice;
          if (filter.mode !== "through") return format(choice, true);
          const earliest = choices.at(-1);
          return earliest && earliest !== choice ? `${format(earliest)} – ${format(choice, true)}` : format(choice, true);
        } : undefined;
        if (temporal && filter.mode === "through" && choices.length) {
          return <DateRangePicker key={filter.id} label={label} value={values[filter.id]}
            choices={choices} formatChoice={formatChoice} allValue={dateAllValue}
            onChange={(choice) => onChange(filter.id, choice)} />;
        }
        return <Dropdown key={filter.id} label={label} value={values[filter.id]}
          choices={temporal && filter.defaultValue && filter.defaultValue !== "all" ? choices : ["all", ...choices]}
          onChange={(choice) => onChange(filter.id, choice)} showLabel
          allLabel={temporal ? "All dates" : "All"} formatChoice={formatChoice} />;
      })}
      {showClear && hasActiveFilters && <button type="button" className="clear-filters"
        onClick={() => filters.forEach(({ id, defaultValue }) =>
          onChange(id, defaultValue ?? "all"))}>{clearLabel}</button>}
    </section>
  );
}

export function InlineFilters({ label, field, rows = [], value = "all", onChange }) {
  const choices = [...new Set(rows.map((row) => row?.[field])
    .filter((entry) => typeof entry === "string" && entry && entry.toLowerCase() !== "all"))];
  if (choices.length < 2) return null;
  return <div className="inline-filters" role="group" aria-label={`${label} chart filter`}>
    <Dropdown label={label} value={value} choices={["all", ...choices]}
      onChange={(choice) => onChange?.(choice)} showLabel />
  </div>;
}

export function DataTable({ rows, searchable = true, compactColumns = [], compactNumbers = true,
  signedDeltas = false, columns: columnDefinitions = [], caption, label, pageSize = 8, paginationStyle = "default" }) {
  const [search, setSearch] = useState("");
  const [order, setOrder] = useState({ field: "", descending: false });
  const [page, setPage] = useState(0);
  const [overflow, setOverflow] = useState({ start: false, end: false });
  const tableRef = useRef(null);
  const accessibleName = caption ?? label ?? "Reviewed data";
  const columns = columnDefinitions.length
    ? columnDefinitions.map(({ field, key }) => field ?? key)
    : [...new Set(rows.flatMap(Object.keys))];
  const definitions = new Map(columnDefinitions.map((column) => [column.field ?? column.key, column]));
  const distributions = useMemo(() => new Map(columnDefinitions
    .filter(({ presentation }) => presentation === "bar")
    .map(({ field, key, max }) => [field ?? key, {
      buckets: distributionBuckets(rows, field ?? key, max),
      percentiles: distributionPercentiles(rows, field ?? key),
    }])),
  [rows, columnDefinitions]);
  const compactFields = new Set(compactColumns);
  const formatCell = (value, column) => {
    if (yearColumn(column) && value !== null && value !== undefined) return String(value);
    const formatted = typeof value === "number" && Number.isFinite(value)
      && !compactNumbers && !compactFields.has(column)
      ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)
      : displayValue(value);
    return signedDeltas && deltaDirection(column, value) === "positive" && !String(formatted).startsWith("+")
      ? `+${formatted}` : formatted;
  };
  const visible = rows.filter((row) => Object.values(row).some((value) =>
    String(value).toLowerCase().includes(search.toLowerCase()))).sort((left, right) => {
    if (!order.field) return 0;
    return compareTableValues(left[order.field], right[order.field], order.descending);
  });
  const pages = Math.max(1, Math.ceil(visible.length / pageSize));
  const currentPage = Math.min(page, pages - 1);

  useEffect(() => {
    const table = tableRef.current;
    if (!table) return undefined;
    const update = () => {
      const next = {
        start: table.scrollLeft > 1,
        end: table.scrollLeft + table.clientWidth < table.scrollWidth - 1,
      };
      setOverflow((current) => current.start === next.start && current.end === next.end ? current : next);
    };
    update();
    table.addEventListener("scroll", update, { passive: true });
    const observer = typeof ResizeObserver === "function" ? new ResizeObserver(update) : null;
    observer?.observe(table);
    return () => { table.removeEventListener("scroll", update); observer?.disconnect(); };
  }, [rows.length, columns.length, search, order.field, order.descending, currentPage]);

  return (
    <>
      {searchable && (
        <div className="toolbar">
          <label className="search-field"><Icon name="search" />
            <input className="search" aria-label="Search data" placeholder="Search data" value={search}
              onChange={(event) => { setSearch(event.target.value); setPage(0); }} />
          </label>
          <span className="source-value">{visible.length} results</span>
        </div>
      )}
      <div className="table-wrap" ref={tableRef} role="region" aria-label={`${accessibleName} table`}
        tabIndex={overflow.start || overflow.end ? 0 : undefined}
        data-overflow-start={overflow.start} data-overflow-end={overflow.end}>
        <table className="table">
          <caption className="visually-hidden">{accessibleName}</caption>
          <thead><tr>{columns.map((column) => (
            <th key={column} scope="col"
              aria-sort={order.field === column ? order.descending ? "descending" : "ascending" : "none"}
              className={numericPresentation(rows, column, definitions.get(column))
              ? "numeric" : undefined}><button type="button" onClick={() => setOrder({
              field: column, descending: order.field === column && !order.descending,
            })}>{definitions.get(column)?.label ?? columnLabel(column)}{order.field === column ? (order.descending ? " ↓" : " ↑") : ""}</button></th>
          ))}</tr></thead>
          <tbody>{visible.slice(currentPage * pageSize, currentPage * pageSize + pageSize).map((row, index) => (
            <tr key={index}>{columns.map((column) => {
              const definition = definitions.get(column);
              const value = row[column];
              const presentation = definition?.presentation;
              const distribution = distributions.get(column);
              const percentile = distribution?.percentiles.get(value);
              return <td key={column}
                className={[numericPresentation(rows, column, definition) ? "numeric" : "",
                  presentation ? `table-cell-${presentation}` : ""]
                  .filter(Boolean).join(" ") || undefined}
                data-delta={resolveDeltaTone(definition?.deltaTone, value, row,
                  signedDeltas ? deltaDirection(column, value) || undefined : undefined)}>
                {presentation === "sparkline" ? <TableVisualTooltip
                  label={definition.label ?? columnLabel(column)} values={value}
                  value="No reviewed history" formatValue={(point) => formatCell(point, column)}>
                  <TableSparkline values={value} label={definition.label ?? columnLabel(column)} />
                </TableVisualTooltip>
                  : presentation === "bar" ? <TableVisualTooltip
                    label={definition.label ?? columnLabel(column)}
                    value={percentile === undefined ? "No reviewed value"
                      : `${formatCell(value, column)} · ${percentile}th percentile`}>
                    <span className="table-data-bar">
                    <span className="table-data-bar-track" aria-hidden="true">
                      {(distribution?.buckets ?? []).map((height, index) =>
                        <span key={index} className="table-data-bar-segment"
                          style={{ "--distribution-height": `${height}%` }} />)}
                      {percentile !== undefined && <span className="table-distribution-marker"
                        style={{ left: `${Math.max(0, Math.min(100, numericValue(value)
                          / (definition.max ?? 100) * 100))}%` }} />}
                    </span>
                  </span></TableVisualTooltip> : presentation === "status" ? <span className="table-status"
                    data-status={statusTone(value)}>
                    {String(value ?? "—")}</span>
                  : presentation === "identity" ? <span className="table-identity"><strong>{String(value ?? "—")}</strong>
                    {definition.secondaryField && <span>{String(row[definition.secondaryField] ?? "")}</span>}</span>
                  : presentation === "percent" && Number.isFinite(value)
                    ? new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 0 }).format(value)
                    : formatCell(row[column], column)}</td>;
            })}</tr>
          ))}</tbody>
        </table>
      </div>
      {pages > 1 && paginationStyle === "receipt" ? <div className="receipt-pagination">
        <button type="button" aria-label="Previous page" disabled={!currentPage}
          onClick={() => setPage(currentPage - 1)}>Previous</button>
        <span role="status" aria-live="polite">Rows {currentPage * pageSize + 1}–{Math.min((currentPage + 1) * pageSize, visible.length)} of {visible.length.toLocaleString()}</span>
        <button type="button" aria-label="Next page" disabled={currentPage + 1 >= pages}
          onClick={() => setPage(currentPage + 1)}>Next</button>
      </div> : pages > 1 && (
        <div className="toolbar table-pagination">
          <span className="source-value">Page {currentPage + 1} of {pages}</span>
          <div className="actions">
            <button type="button" className="table-page-button" aria-label="Previous page"
              disabled={!currentPage} onClick={() => setPage(currentPage - 1)}><Icon name="chevronLeft" /></button>
            <button type="button" className="table-page-button" aria-label="Next page"
              disabled={currentPage + 1 >= pages} onClick={() => setPage(currentPage + 1)}>
              <Icon name="chevronRight" />
            </button>
          </div>
        </div>
      )}
    </>
  );
}
