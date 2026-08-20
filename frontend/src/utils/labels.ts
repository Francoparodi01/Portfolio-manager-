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
  if (key === "plan_execution_attribution") return "Plan seguido";
  if (key === "position_analysis") return "Analisis de posicion";
  if (key === "radar") return "Radar teórico";
  if (key === "optimizer") return "Optimizador";
  return String(value || "Sin fuente");
}

export function scopeLabel(value: unknown): string {
  const key = String(value || "").toLowerCase();
  if (key === "primary") return "Métrica principal";
  if (key === "planner_audit") return "Auditoría planner";
  if (key === "followed_plan") return "Ejecución seguida";
  if (key === "hold_audit") return "Mantener auditado";
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
  if (key === "OBSERVED") return "Observada";
  if (key === "FOLLOWED") return "Seguida";
  if (key === "OVERFOLLOWED") return "Seguida por encima";
  if (key === "FOLLOWED_PROVISIONAL") return "Seguida (snapshot)";
  if (key === "OVERFOLLOWED_PROVISIONAL") return "Seguida por encima (snapshot)";
  if (key === "PARTIAL_PROVISIONAL") return "Parcial (snapshot)";
  if (key === "OPPOSITE_PROVISIONAL") return "Contraria (snapshot)";
  if (key === "IGNORED") return "Ignorada";
  if (key === "PARTIAL") return "Parcial";
  if (key === "OPPOSITE") return "Contraria";
  return key || "Pendiente";
}

export function toneForScope(value: unknown): Tone {
  const key = String(value || "").toLowerCase();
  if (key === "primary") return "real";
  if (key === "followed_plan") return "real";
  if (key === "planner_audit") return "info";
  if (key === "hold_audit") return "info";
  if (key === "radar_audit") return "theoretical";
  if (key === "blocked_audit") return "blocked";
  if (key === "debug") return "pending";
  return "neutral";
}

export function toneForStatus(value: unknown): Tone {
  const key = String(value || "").toUpperCase();
  if (key === "EXECUTED" || key === "FOLLOWED" || key === "OVERFOLLOWED") return "real";
  if (key === "FOLLOWED_PROVISIONAL" || key === "OVERFOLLOWED_PROVISIONAL") return "info";
  if (key === "APPROVED" || key === "PARTIAL" || key === "PARTIAL_PROVISIONAL") return "info";
  if (key === "BLOCKED" || key === "OPPOSITE" || key === "OPPOSITE_PROVISIONAL") return "blocked";
  if (key === "THEORETICAL") return "theoretical";
  if (key === "OBSERVED") return "info";
  if (key === "IGNORED") return "warning";
  return "neutral";
}
