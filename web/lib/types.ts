export type SourceState = { available: boolean; updatedAt: number | null; stale: boolean; error?: string };
export type SessionConfiguration = {
  schemaVersion: number;
  execution: Record<string, boolean | number | string>;
  launcher: Record<string, boolean | number | string>;
  watchdog: Record<string, boolean | number | string>;
  bot: Record<string, boolean | number | string | Array<number | string>>;
};
export type SavedSession = {
  id: string; name: string; description: string; configuration: SessionConfiguration; version: number;
  createdAt: number; updatedAt: number; archivedAt?: number | null; selected: boolean; runCount: number;
};
export type HistoricalRun = {
  id: string; sessionId: string; sessionName: string; configurationVersion: number; configuration: SessionConfiguration;
  status: string; createdAt: number; startedAt?: number | null; endedAt?: number | null; heartbeatAt?: number | null;
  artifactPath: string; artifactBytes?: number; error?: string | null; metrics: Record<string, unknown>;
};
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

export type PortfolioPosition = {
  marketId: string; ticker: string; title: string; marketUrl?: string | null; side: "yes" | "no";
  contractsUnits: number; lastPriceUnits?: number | null; bidPriceUnits?: number | null; askPriceUnits?: number | null; midPriceUnits?: number | null;
  costBasisUnits?: number | null; averageCostPriceUnits?: number | null; realizedPnlUnits?: number | null; feesUnits?: number | null;
  unrealizedPnlUnits?: number | null; unrealizedReturnBps?: number | null; totalPnlUnits?: number | null; totalReturnBps?: number | null;
  marketUnrealizedPnlUnits?: number | null; marketUnrealizedReturnBps?: number | null; marketTotalPnlUnits?: number | null; marketTotalReturnBps?: number | null;
  totalTradedUnits?: number | null; openOrderCount: number; updatedAtMs?: number | null;
};
export type PortfolioOrder = {
  orderId: string; marketId: string; ticker: string; title: string; marketUrl?: string | null; side?: "yes" | "no" | null;
  remainingContractsUnits: number; initialContractsUnits: number; filledContractsUnits: number;
  averageFillPriceUnits?: number | null; orderPriceUnits?: number | null; bidPriceUnits?: number | null; askPriceUnits?: number | null;
  midPriceUnits?: number | null; lastPriceUnits?: number | null; openMarketValueUnits?: number | null;
  createdAtMs?: number | null; lastFillAtMs?: number | null; firstFillTimeMs?: number | null; status: string;
};
export type AccountPortfolio = {
  generatedAt: number; schemaVersion?: number; available?: boolean; stale?: boolean; generatedAtMs?: number | null; lastSuccessAtMs?: number | null;
  running?: boolean; currentStartedAtMs?: number | null; currentDurationMs?: number | null; lastCompletedAtMs?: number | null; lastError?: string | null;
  subaccountNumber?: number; historyWindowMs?: number; source: SourceState; warnings: string[]; apiActivity?: ApiActivity;
  summary: { availableCashUnits?: number | null; portfolioValueUnits?: number | null; balanceUpdatedAtMs?: number | null; unrealizedPnlUnits?: number | null; unrealizedReturnBps?: number | null; positionCount?: number; apiTier?: string | null; readRateLimit?: { refillRate: number; bucketCapacity: number } | null; writeRateLimit?: { refillRate: number; bucketCapacity: number } | null };
  positions: PortfolioPosition[];
  orders: { summary: { openOrderCount?: number; lastOrderAtMs?: number | null; lastFillAtMs?: number | null; openMarketValueUnits?: number | null; averageFirstFillTimeMs?: number | null; filledOrderSampleSize?: number }; items: PortfolioOrder[] };
};
