export type SourceState = { available: boolean; updatedAt: number | null; stale: boolean; error?: string };
export type PnlTotals = { fills: number; feesCents: number; realizedCents: number; unrealizedCents: number; totalCents: number };
export type BotState = { ticker: string; title: string; botRunning: boolean; watchdogRunning: boolean; watchdogMode: string; watchdogConfidence?: number; watchdogReason?: string; yesBudgetCents: number; noBudgetCents: number };
export type Overview = {
  generatedAt: number;
  fleet: { launcher?: { lifecycle?: string; environment?: string; heartbeatAt?: number; nextRefreshAt?: number; lastError?: string }; counts?: { activeBots?: number; configuredBots?: number; disabledTickers?: number; watchdogModes?: Record<string, number> }; bots?: BotState[] };
  fleetSource: SourceState;
  pnl: PnlTotals;
  positions: Array<{ ticker: string; netPosition: number; totalCents: number }>;
  positionSummary: { markets: number; grossContracts: number; netContracts: number; source: string };
  marketCounts: { screened: number; disabled: number };
  warnings: string[];
};
export type Market = { ticker: string; title: string; rank?: number; expectedEdgeCents?: number; quotedEdgeCents?: number; watchdogMode: string; watchdogConfidence?: number; watchdogReason?: string; botRunning: boolean; disabled: boolean; pnl?: { netPosition: number; totalCents: number }; source?: Record<string, SourceState> };

export type ApiActivity = {
  rest?: { total?: number; successes?: number; errors?: number; requestsLast60s?: number; averageLatencyMs?: number; lastActivityAtMs?: number; byMethod?: Record<string, number>; byOperation?: Record<string, number>; byStatus?: Record<string, number> };
  stream?: { connections?: number; reconnects?: number; connectionErrors?: number; streamErrors?: number; adapterErrors?: number; subscriptionsSent?: number; message?: number; event?: number; messagesLast60s?: number; sequenceResets?: number; lastActivityAtMs?: number; byEventType?: Record<string, number> };
};
export type ClientMonitoring = {
  marketId: string; title: string; pid?: number; lifecycle?: string; socketHealthy?: boolean; restartCount?: number;
  runtime?: { startedAtMs?: number; runningForMs?: number };
  market?: { marketId?: string; title?: string; priceUnits?: number | null; priceSource?: string; priceAtMs?: number | null };
  portfolio?: { startingPositionUnits?: number; currentPositionUnits?: number; updatedAtMs?: number };
  pnl?: PnlTotals & { sessionPositionUnits?: number; markPriceUnits?: number | null; markSource?: string; markAtMs?: number | null };
  fills?: { count?: number; quantityUnits?: number; lastFillAtMs?: number | null; recent?: Array<Record<string, unknown>> };
  orderActivity?: { byAction?: Record<string, { attempts?: number; successes?: number; errors?: number }>; lastActivityAtMs?: number | null; active?: Record<string, Record<string, unknown>>; recent?: Array<Record<string, unknown>> };
  apiActivity?: ApiActivity;
  watchdog?: { running?: boolean; mode?: string; confidence?: number; reason?: string; updatedAtMs?: number };
};
export type Monitoring = {
  generatedAt: number; schemaVersion?: number; source: SourceState; warnings: string[];
  manager: { running?: boolean; lifecycle?: string; startedAtMs?: number; runningForMs?: number; botsRunning?: number; portfolio?: { items?: Array<{ marketId: string; title?: string; positionUnits?: number | null; updatedAtMs?: number; stale?: boolean; available?: boolean }>; grossPositionUnits?: number; netPositionUnits?: number; unknownMarkets?: number; staleMarkets?: number }; pnl?: PnlTotals; apiActivity?: ApiActivity };
  clients: ClientMonitoring[];
  screener: { running?: boolean; currentReason?: string; currentStartedAtMs?: number; currentDurationMs?: number; lastStartedAtMs?: number; lastCompletedAtMs?: number; lastDurationMs?: number; lastSuccessAtMs?: number; lastError?: string; generationId?: number; generatedAtMs?: number; reason?: string; picks?: Array<{ marketId: string; title: string; yesBudgetCents: number; noBudgetCents: number; selectionReason: string; rank?: number }>; changes?: Record<string, string[]>; apiActivity?: ApiActivity };
};
