import type { RowRecord } from "../types/api";

export function isRecord(value: unknown): value is RowRecord {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function asRecord(value: unknown): RowRecord {
  return isRecord(value) ? value : {};
}

export function asRows(value: unknown): RowRecord[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

export function getRecord(row: unknown, key: string): RowRecord {
  return asRecord(asRecord(row)[key]);
}

export function getString(row: unknown, key: string, fallback = ""): string {
  const value = asRecord(row)[key];
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return fallback;
}

export function getNumber(row: unknown, key: string): number | null {
  const value = asRecord(row)[key];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function getBoolean(row: unknown, key: string): boolean | null {
  const value = asRecord(row)[key];
  return typeof value === "boolean" ? value : null;
}

export function nestedNumber(row: unknown, path: string[]): number | null {
  let cursor: unknown = row;
  for (const part of path) cursor = asRecord(cursor)[part];
  if (typeof cursor === "number" && Number.isFinite(cursor)) return cursor;
  if (typeof cursor === "string") {
    const parsed = Number(cursor);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function maxNumber(rows: RowRecord[], key: string): number | null {
  const values = rows
    .map((row) => getNumber(row, key))
    .filter((value): value is number => value !== null);
  return values.length ? Math.max(...values) : null;
}

export function comparePrimitive(a: unknown, b: unknown): number {
  const numA = typeof a === "number" ? a : Number(a);
  const numB = typeof b === "number" ? b : Number(b);
  if (Number.isFinite(numA) && Number.isFinite(numB)) return numA - numB;
  return String(a ?? "").localeCompare(String(b ?? ""), "es");
}
