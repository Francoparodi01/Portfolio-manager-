import type { Tone } from "../types/api";

export function decisionLabel(value: unknown): string {
  const key = String(value || "").toUpperCase();
  if (key === "BUY") return "Comprar";
  if (key === "SELL") return "Vender";
  if (key === "REDUCE") return "Reducir";
  if (key === "HOLD") return "Mantener";
  if (key === "REBALANCE") return "Rebalancear";
  return key || "Sin decisión";
}

export function sourceLabel(value: unknown): string {
  const key = String(value || "").toLowerCase();
  if (key === "broker_movement") return "Movimiento manual";
  if (key === "broker_fill") return "Fill broker";
  if (key === "execution_plan") return "Plan de Quantia";
  if (key === "radar") return "Radar teórico";
  if (key === "optimizer") return "Optimizador";
  return String(value || "Sin fuente");
}

export function scopeLabel(value: unknown): string {
  const key = String(value || "").toLowerCase();
  if (key === "primary") return "Métrica principal";
  if (key === "planner_audit") return "Auditoría planner";
  if (key === "radar_audit") return "Radar auditado";
  if (key === "blocked_audit") return "Bloqueado auditado";
  if (key === "debug") return "Debug";
  return String(value || "Sin alcance");
}

export function statusLabel(value: unknown): string {
  const key = String(value || "").toUpperCase();
  if (key === "APPROVED") return "Aprobada";
  if (key === "EXECUTED") return "Ejecutada";
  if (key === "EXECUTED_MANUAL") return "Ejecución manual";
  if (key === "BLOCKED") return "Bloqueada";
  if (key === "THEORETICAL") return "Teórica";
  if (key === "FOLLOWED") return "Seguida";
  if (key === "IGNORED") return "Ignorada";
  if (key === "PARTIAL") return "Parcial";
  if (key === "OPPOSITE") return "Contraria";
  return key || "Pendiente";
}

export function toneForScope(value: unknown): Tone {
  const key = String(value || "").toLowerCase();
  if (key === "primary") return "real";
  if (key === "planner_audit") return "info";
  if (key === "radar_audit") return "theoretical";
  if (key === "blocked_audit") return "blocked";
  if (key === "debug") return "pending";
  return "neutral";
}

export function toneForStatus(value: unknown): Tone {
  const key = String(value || "").toUpperCase();
  if (key === "EXECUTED" || key === "FOLLOWED") return "real";
  if (key === "APPROVED" || key === "PARTIAL") return "info";
  if (key === "BLOCKED" || key === "OPPOSITE") return "blocked";
  if (key === "THEORETICAL") return "theoretical";
  if (key === "IGNORED") return "warning";
  return "neutral";
}
