import type {
  ApiSession,
  AuditTimelinePayload,
  AuthStatusPayload,
  CandlesPayload,
  DecisionsPayload,
  EndpointDefinition,
  FillsPayload,
  HealthPayload,
  HumanActivityPayload,
  IngestionPayload,
  LogsPayload,
  LearningShadowPayload,
  OverrideAuditPayload,
  PerformancePayload,
  PortfolioPayload,
  RadarPayload,
  RowRecord,
  ShadowPayload,
} from "../types/api";
import { fetchJson, fetchPublicJson } from "./apiClient";

export const PERIODS = [7, 30, 90, 180, 365] as const;

export function endpointDefinitions(period: number): EndpointDefinition[] {
  return [
    { key: "auth", label: "Auth", path: "/api/auth/status", public: true },
    { key: "health", label: "Health", path: "/api/health" },
    { key: "ingestion", label: "Ingesta", path: "/api/ingestion", timeoutMs: 25_000 },
    { key: "candles", label: "Velas", path: "/api/candles", timeoutMs: 25_000 },
    { key: "decisions", label: "Decisiones", path: "/api/decisions?days=90" },
    { key: "portfolio", label: "Cartera", path: "/api/portfolio?days=90" },
    { key: "performance", label: "Performance", path: `/api/performance?days=${period}`, timeoutMs: 25_000 },
    { key: "override", label: "Bot vs humano", path: `/api/override-audit?days=${period}` },
    { key: "ledger", label: "Ledger", path: "/api/decision-ledger?days=90", timeoutMs: 35_000 },
    { key: "timeline", label: "Timeline", path: `/api/audit-timeline?days=${period}&limit=120`, timeoutMs: 35_000 },
    { key: "radar", label: "Radar", path: "/api/radar-audit?days=90", timeoutMs: 25_000 },
    { key: "shadow", label: "Shadow", path: "/api/shadow", timeoutMs: 22_000 },
    { key: "learning", label: "Learning shadow", path: `/api/learning-shadow?days=${period}`, timeoutMs: 22_000 },
    { key: "human", label: "Humano", path: "/api/human-activity?days=7" },
    { key: "fills", label: "Fills", path: "/api/fills?days=90&limit=80" },
    { key: "logs", label: "Logs", path: "/api/logs/recent?limit=80" },
  ];
}

export const monitorApi = {
  authStatus: (apiBase: string) => fetchPublicJson<AuthStatusPayload>(apiBase, "/api/auth/status"),
  health: (session: ApiSession) => fetchJson<HealthPayload>(session, "/api/health"),
  ingestion: (session: ApiSession) => fetchJson<IngestionPayload>(session, "/api/ingestion", 25_000),
  candles: (session: ApiSession) => fetchJson<CandlesPayload>(session, "/api/candles", 25_000),
  decisions: (session: ApiSession, days = 90) => fetchJson<DecisionsPayload>(session, `/api/decisions?days=${days}`),
  portfolio: (session: ApiSession, days = 90) => fetchJson<PortfolioPayload>(session, `/api/portfolio?days=${days}`),
  performance: (session: ApiSession, days = 180) => fetchJson<PerformancePayload>(session, `/api/performance?days=${days}`, 25_000),
  override: (session: ApiSession, days = 90) => fetchJson<OverrideAuditPayload>(session, `/api/override-audit?days=${days}`),
  ledger: (session: ApiSession, days = 90) => fetchJson<RowRecord>(session, `/api/decision-ledger?days=${days}`, 35_000),
  timeline: (session: ApiSession, days = 90, limit = 120) => fetchJson<AuditTimelinePayload>(session, `/api/audit-timeline?days=${days}&limit=${limit}`, 35_000),
  radar: (session: ApiSession, days = 90) => fetchJson<RadarPayload>(session, `/api/radar-audit?days=${days}`, 25_000),
  shadow: (session: ApiSession) => fetchJson<ShadowPayload>(session, "/api/shadow", 22_000),
  learning: (session: ApiSession, days = 365) => fetchJson<LearningShadowPayload>(session, `/api/learning-shadow?days=${days}`, 22_000),
  human: (session: ApiSession, days = 7) => fetchJson<HumanActivityPayload>(session, `/api/human-activity?days=${days}`),
  fills: (session: ApiSession, days = 90, limit = 80) => fetchJson<FillsPayload>(session, `/api/fills?days=${days}&limit=${limit}`),
  logs: (session: ApiSession) => fetchJson<LogsPayload>(session, "/api/logs/recent?limit=80"),
};
