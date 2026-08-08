import type { Tone } from "../types/api";

const moneyFormatter = new Intl.NumberFormat("es-AR", {
  currency: "ARS",
  maximumFractionDigits: 0,
  style: "currency",
});
const numberFormatter = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 0 });
const compactFormatter = new Intl.NumberFormat("es-AR", {
  maximumFractionDigits: 1,
  notation: "compact",
});

function finiteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function formatNumber(value: unknown): string {
  const numeric = finiteNumber(value);
  return numeric === null ? "-" : numberFormatter.format(numeric);
}

export function formatCompact(value: unknown): string {
  const numeric = finiteNumber(value);
  return numeric === null ? "-" : compactFormatter.format(numeric);
}

export function formatMoney(value: unknown): string {
  const numeric = finiteNumber(value);
  return numeric === null ? "-" : moneyFormatter.format(numeric);
}

export function formatPercent(value: unknown, digits = 1, signed = false): string {
  const numeric = finiteNumber(value);
  if (numeric === null) return "-";
  const sign = signed && numeric > 0 ? "+" : "";
  return `${sign}${(numeric * 100).toFixed(digits)}%`;
}

export function formatScore(value: unknown): string {
  const numeric = finiteNumber(value);
  return numeric === null ? "-" : numeric.toFixed(3);
}

export function formatDateTime(value: unknown): string {
  if (!value) return "-";
  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("es-AR", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "2-digit",
  });
}

export function ageLabel(value: unknown): string {
  if (!value) return "sin fecha";
  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) return "sin fecha";
  const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
  if (seconds < 60) return `hace ${seconds}s`;
  if (seconds < 3600) return `hace ${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `hace ${Math.round(seconds / 3600)}h`;
  return `hace ${Math.round(seconds / 86400)}d`;
}

export function toneForNumber(value: unknown): Tone {
  const numeric = finiteNumber(value);
  if (numeric === null) return "neutral";
  if (numeric > 0) return "positive";
  if (numeric < 0) return "negative";
  return "neutral";
}

export function sampleLabel(closed: unknown, threshold = 30): string {
  const numeric = finiteNumber(closed) ?? 0;
  if (numeric <= 0) return "Sin muestra cerrada";
  if (numeric < threshold) return `Muestra limitada: ${numberFormatter.format(numeric)}/${threshold}`;
  return `Muestra operativa: ${numberFormatter.format(numeric)} casos`;
}
