import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import type { ReactNode } from "react";
import { useMemo } from "react";
import { comparePrimitive } from "../../utils/data";
import { EmptyState } from "../feedback/States";

export type SortDirection = "asc" | "desc";

export type TableColumn<T> = {
  id: string;
  header: string;
  render: (row: T) => ReactNode;
  sortValue?: (row: T) => unknown;
  align?: "left" | "right";
};

export function ResponsiveTable<T>({
  columns,
  rows,
  rowKey,
  sort,
  onSort,
  emptyLabel,
}: {
  columns: TableColumn<T>[];
  rows: T[];
  rowKey: (row: T, index: number) => string;
  sort?: { columnId: string; direction: SortDirection };
  onSort?: (columnId: string) => void;
  emptyLabel?: string;
}) {
  const sortedRows = useMemo(() => {
    if (!sort) return rows;
    const column = columns.find((item) => item.id === sort.columnId);
    if (!column?.sortValue) return rows;
    const multiplier = sort.direction === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => multiplier * comparePrimitive(column.sortValue?.(a), column.sortValue?.(b)));
  }, [columns, rows, sort]);

  if (!sortedRows.length) return <EmptyState label={emptyLabel || "Sin filas"} />;

  return (
    <div className="responsive-table" role="region" aria-label="Tabla de datos">
      <table>
        <thead>
          <tr>
            {columns.map((column) => {
              const active = sort?.columnId === column.id;
              return (
                <th key={column.id} className={column.align === "right" ? "right" : undefined}>
                  {column.sortValue && onSort ? (
                    <button type="button" onClick={() => onSort(column.id)}>
                      <span>{column.header}</span>
                      {active && sort?.direction === "asc" ? <ArrowUp size={13} /> : null}
                      {active && sort?.direction === "desc" ? <ArrowDown size={13} /> : null}
                      {!active ? <ChevronsUpDown size={13} /> : null}
                    </button>
                  ) : (
                    column.header
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, rowIndex) => (
            <tr key={rowKey(row, rowIndex)}>
              {columns.map((column) => (
                <td
                  key={column.id}
                  className={column.align === "right" ? "right" : undefined}
                  data-label={column.header}
                >
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
