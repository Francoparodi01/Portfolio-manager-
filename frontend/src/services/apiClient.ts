import type { ApiSession, RowRecord } from "../types/api";
import { asRecord } from "../utils/data";

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1"]);

function cleanApiBase(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

export function defaultApiBase(): string {
  const configured = cleanApiBase(import.meta.env.VITE_MONITOR_API_BASE_URL || "");
  if (configured && configured !== "auto") return configured;
  const host = window.location.hostname || "localhost";
  const protocol = window.location.protocol === "https:" ? "https" : "http";
  if (!LOCAL_HOSTS.has(host)) return cleanApiBase(window.location.origin);
  return `${protocol}://${host}:8010`;
}

export function normalizeApiBase(value: string): string {
  const normalized = cleanApiBase(value);
  if (!normalized) return defaultApiBase();
  const host = window.location.hostname || "localhost";
  if (LOCAL_HOSTS.has(host)) return normalized;
  try {
    const url = new URL(normalized);
    if (url.hostname === host && url.port === "8010") return cleanApiBase(window.location.origin);
  } catch {
    return normalized;
  }
  return normalized;
}

export async function fetchJson<T>(session: ApiSession, path: string, timeoutMs = 12_000): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  const headers: Record<string, string> = { Accept: "application/json" };
  if (session.token) headers.Authorization = `Bearer ${session.token}`;
  if (session.totp) headers["X-TOTP-Code"] = session.totp;

  try {
    const response = await fetch(`${normalizeApiBase(session.apiBase)}${path}`, {
      headers,
      signal: controller.signal,
    });
    const payload = (await response.json().catch(() => ({}))) as RowRecord;
    if (!response.ok) {
      const message = asRecord(payload).error;
      throw new Error(typeof message === "string" ? message : `HTTP ${response.status}`);
    }
    return payload as T;
  } finally {
    window.clearTimeout(timeout);
  }
}

export async function fetchPublicJson<T>(apiBase: string, path: string, timeoutMs = 8_000): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${normalizeApiBase(apiBase)}${path}`, {
      signal: controller.signal,
    });
    const payload = (await response.json().catch(() => ({}))) as RowRecord;
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return payload as T;
  } finally {
    window.clearTimeout(timeout);
  }
}
