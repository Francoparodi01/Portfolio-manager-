import { useQuery, useQueryClient, type UseQueryResult } from "@tanstack/react-query";
import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useSession } from "../app/session";
import { demoPayloadFor } from "../services/mockData";
import { monitorApi } from "../services/monitorApi";
import type {
  ApiSession,
  AuditTimelinePayload,
  CandlesPayload,
  DecisionsPayload,
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

function cloneDemo<T>(value: unknown): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function useMonitorQuery<T>(
  key: string,
  fetcher: (session: ApiSession) => Promise<T>,
  params: readonly unknown[] = [],
): UseQueryResult<T> {
  const { session } = useSession();
  return useQuery({
    enabled: Boolean(session),
    queryFn: () => {
      if (!session) throw new Error("Sesión no disponible");
      if (session.mode === "demo") return Promise.resolve(cloneDemo<T>(demoPayloadFor(key)));
      return fetcher(session);
    },
    queryKey: ["monitor", key, session?.mode, session?.apiBase, ...params],
  });
}

export const useHealthQuery = () => useMonitorQuery<HealthPayload>("health", monitorApi.health);
export const useIngestionQuery = () => useMonitorQuery<IngestionPayload>("ingestion", monitorApi.ingestion);
export const useCandlesQuery = () => useMonitorQuery<CandlesPayload>("candles", monitorApi.candles);
export const useShadowQuery = () => useMonitorQuery<ShadowPayload>("shadow", monitorApi.shadow);
export const useLearningShadowQuery = (days = 365) => useMonitorQuery<LearningShadowPayload>("learning", (session) => monitorApi.learning(session, days), [days]);
export const useLogsQuery = () => useMonitorQuery<LogsPayload>("logs", monitorApi.logs);

export function usePortfolioQuery(days = 90) {
  return useMonitorQuery<PortfolioPayload>("portfolio", (session) => monitorApi.portfolio(session, days), [days]);
}

export function useDecisionsQuery(days = 90) {
  return useMonitorQuery<DecisionsPayload>("decisions", (session) => monitorApi.decisions(session, days), [days]);
}

export function usePerformanceQuery(days = 180) {
  return useMonitorQuery<PerformancePayload>("performance", (session) => monitorApi.performance(session, days), [days]);
}

export function useOverrideQuery(days = 90) {
  return useMonitorQuery<OverrideAuditPayload>("override", (session) => monitorApi.override(session, days), [days]);
}

export function useLedgerQuery(days = 90) {
  return useMonitorQuery<RowRecord>("ledger", (session) => monitorApi.ledger(session, days), [days]);
}

export function useAuditTimelineQuery(days = 90, limit = 400) {
  return useMonitorQuery<AuditTimelinePayload>("timeline", (session) => monitorApi.timeline(session, days, limit), [days, limit]);
}

export function useRadarQuery(days = 90) {
  return useMonitorQuery<RadarPayload>("radar", (session) => monitorApi.radar(session, days), [days]);
}

export function useHumanQuery(days = 7) {
  return useMonitorQuery<HumanActivityPayload>("human", (session) => monitorApi.human(session, days), [days]);
}

export function useFillsQuery(days = 90, limit = 80) {
  return useMonitorQuery<FillsPayload>("fills", (session) => monitorApi.fills(session, days, limit), [days, limit]);
}

export function useRefreshAll() {
  const queryClient = useQueryClient();
  return () => queryClient.invalidateQueries({ queryKey: ["monitor"] });
}

export function usePeriodParam(defaultValue = 90) {
  const [searchParams, setSearchParams] = useSearchParams();
  const period = useMemo(() => {
    const parsed = Number(searchParams.get("period"));
    return [7, 30, 90, 180, 365].includes(parsed) ? parsed : defaultValue;
  }, [defaultValue, searchParams]);

  const setPeriod = (nextPeriod: number) => {
    const next = new URLSearchParams(searchParams);
    next.set("period", String(nextPeriod));
    setSearchParams(next, { replace: true });
  };

  return [period, setPeriod] as const;
}
