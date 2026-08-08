export type SessionMode = "api" | "demo";

export type ApiSession = {
  mode: SessionMode;
  token: string;
  totp: string;
  apiBase: string;
};

export type RowRecord = Record<string, unknown>;

export type EndpointKey =
  | "auth"
  | "health"
  | "ingestion"
  | "candles"
  | "decisions"
  | "portfolio"
  | "performance"
  | "override"
  | "ledger"
  | "radar"
  | "shadow"
  | "learning"
  | "human"
  | "fills"
  | "logs";

export type EndpointDefinition = {
  key: EndpointKey;
  label: string;
  path: string;
  timeoutMs?: number;
  public?: boolean;
};

export type HealthPayload = {
  ok: boolean;
  database?: RowRecord;
  redis?: RowRecord;
  market?: RowRecord;
  services?: RowRecord;
};

export type AuthStatusPayload = {
  ok: boolean;
  auth?: RowRecord;
};

export type IngestionPayload = {
  ok: boolean;
  portfolio?: RowRecord;
  market_prices?: RowRecord;
};

export type CandlesPayload = {
  ok: boolean;
  market?: RowRecord;
  coverage?: RowRecord | null;
  recent?: RowRecord[];
};

export type PortfolioPayload = {
  ok: boolean;
  days: number;
  snapshot?: RowRecord | null;
  positions?: RowRecord[];
  allocation?: RowRecord[];
  history?: RowRecord[];
};

export type DecisionsPayload = {
  ok: boolean;
  days: number;
  summary?: RowRecord;
  groups?: RowRecord[];
  recent?: RowRecord[];
};

export type PerformancePayload = {
  ok: boolean;
  days: number;
  summary?: RowRecord;
  by_ticker?: RowRecord[];
  score_points?: RowRecord[];
  status_counts?: RowRecord[];
  window_counts?: RowRecord;
  bot_predictions?: RowRecord;
  bot_direction_breakdown?: RowRecord[];
  bot_signal_breakdown?: RowRecord[];
  source_breakdown?: RowRecord[];
  buy_confirmation_breakdown?: RowRecord[];
  evitable_loss?: RowRecord;
  bot_prediction_recent?: RowRecord[];
};

export type OverrideAuditPayload = {
  ok: boolean;
  days: number;
  match_window_days?: number;
  summary?: RowRecord;
  matches?: RowRecord[];
  recent?: RowRecord[];
};

export type RadarPayload = {
  ok: boolean;
  days: number;
  summary?: RowRecord;
  chart_items?: RowRecord[];
  recent?: RowRecord[];
};

export type ShadowPayload = {
  ok: boolean;
  available: boolean;
  owner_chat_id?: number;
  note?: string;
  run?: RowRecord | null;
  forecasts?: RowRecord[];
  metrics?: RowRecord[];
  axis?: RowRecord;
};

export type LearningShadowPayload = {
  ok: boolean;
  available: boolean;
  owner_chat_id?: number;
  days?: number;
  note?: string;
  run?: RowRecord | null;
  metrics?: RowRecord[];
  population_summary?: RowRecord[];
  trend?: RowRecord[];
  cohorts?: RowRecord[];
  by_block_reason?: RowRecord[];
  by_block_category?: RowRecord[];
  review_summary?: RowRecord[];
  rule_candidates?: RowRecord[];
  data_quality?: RowRecord;
  recent_cases?: RowRecord[];
  boundary?: RowRecord;
};

export type HumanActivityPayload = {
  ok: boolean;
  days: number;
  summary?: RowRecord;
  recent?: RowRecord[];
};

export type FillsPayload = {
  ok: boolean;
  days: number;
  limit?: number;
  summary?: RowRecord;
  by_source?: RowRecord[];
  recent?: RowRecord[];
  movements?: {
    summary?: RowRecord;
    recent?: RowRecord[];
  };
};

export type LogsPayload = {
  ok: boolean;
  log_dir?: string;
  items?: RowRecord[];
  note?: string | null;
};

export type Tone =
  | "positive"
  | "negative"
  | "warning"
  | "info"
  | "neutral"
  | "pending"
  | "blocked"
  | "real"
  | "theoretical";
