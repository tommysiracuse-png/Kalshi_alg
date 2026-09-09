export type SourceState = { available: boolean; updatedAt: number | null; stale: boolean; error?: string };
export type BotSettingValue = boolean | number | string | Array<number | string>;
// Schema v3 per-market-class overrides: each entry names a session-exposed
// BotSettings field and the value that class uses instead of `bot[field]`.
export type BotClassOverride = { field: string; value: BotSettingValue };
export type BotClassName = "thickCalm" | "thinWide" | "toxic" | "default";
export type BotClassOverrides = { overrides: BotClassOverride[] };
export type BotClassesConfiguration = {
  enabled: boolean;
  classifier: Record<string, number>;
} & Record<BotClassName, BotClassOverrides>;
// Market-screener filters (defaults mirror kalshi_screener_config.py). Schema
// v5 stores the shared defaults under `general` and optional per-venue
// overrides under `venues`; the flat fields remain for older sessions.
// The fleet reads them at Start; a running fleet keeps the snapshot it launched with.
export type ScreenerStatus = "open" | "unopened" | "paused" | "closed" | "settled";
export type ScreenerMveFilter = "exclude" | "only" | "all" | "";
// Markout horizons the fill telemetry records; markoutFilterHorizonSeconds must be one of them.
export const SCREENER_MARKOUT_HORIZONS = [1, 5, 30, 120] as const;
export type ScreenerFilters = {
  status: ScreenerStatus;
  mveFilter: ScreenerMveFilter;
  maxMarketsToScan: number;
  topN: number;
  minSpreadCents: number;
  maxSpreadCents: number;
  minYesBidCents: number;
  minNoBidCents: number;
  minVol24h: number;
  minOpenInterest: number;
  minTimeToCloseHours: number;
  maxTimeToCloseHours: number;
  excludedTickerKeywords: string[];
  targetEdgeCents: number;
  quoteSize: number;
  markoutFilterEnabled: boolean;
  markoutFilterNetThresholdCents: number;
  markoutFilterTickerMinFills: number;
  markoutFilterSeriesMinFills: number;
  markoutFilterHorizonSeconds: number;
  markoutFilterLookbackDays: number;
  markoutFilterTotalNetThresholdCents: number;
};
export type VenueName = "kalshi" | "polymarket";
export type VenueConfiguration = { enabled: boolean; priority: number; maxBots: number; client: Record<string, unknown> };
export type ScreenerConfiguration = ScreenerFilters & {
  general?: ScreenerFilters;
  venues?: Partial<Record<VenueName, Partial<ScreenerFilters>>>;
};
export type SessionConfiguration = {
  schemaVersion: number;
  execution: Record<string, boolean | number | string>;
  launcher: Record<string, boolean | number | string>;
  watchdog: Record<string, boolean | number | string>;
  fleetRuntime?: Record<string, boolean | number | string>;
  bot: Record<string, BotSettingValue>;
  botClasses?: BotClassesConfiguration;
  screener?: ScreenerConfiguration;
  venues?: Record<VenueName, VenueConfiguration>;
  venue?: VenueName;
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
export type MarkoutAggregate = {
  horizonMs: number; grossMarkoutUnits: number; feeUnits: number; netMarkoutUnits: number;
  averageNetMarkoutPriceUnits?: number | null; coveredFillCount: number; coveredContractsUnits: number;
  totalFillCount: number; pendingFillCount: number; unavailableFillCount: number; complete: boolean;
};
export type FillMarkout = {
  horizonMs: number; capturedAtMs: number; futureMidYesUnits: number; signedMarkoutPriceUnits: number;
  grossMarkoutUnits: number; feeUnits: number; netMarkoutUnits: number;
};
export type MetricsSummary = {
  timesRun: number; runtimeMs: number; orders: number; ordersPerMinute: number; fills: number; fillsPerMinute: number;
  apiCalls: number; apiErrors: number; realizedCents: number; unrealizedCents: number; totalCents: number;
  pnlComplete: boolean; outcomes: Record<string, number>; apiByComponent: Record<string, number>;
  markoutsByHorizon?: Record<string, MarkoutAggregate>;
};
export type MetricsResponse = { generatedAt: number; summary: MetricsSummary; runs: HistoricalRun[] };
export type MetricsHeartbeat = {
  generatedAt: number; source: SourceState;
  activeRun?: {
    id: string; sessionId: string; status: string; heartbeatAt?: number | null;
    activityRevision: string; summary: Partial<Pick<MetricsSummary, "runtimeMs" | "orders" | "fills" | "apiCalls" | "apiErrors" | "realizedCents" | "unrealizedCents" | "totalCents" | "pnlComplete" | "markoutsByHorizon">>;
    source: SourceState;
  } | null;
};
export type RunMarketMetrics = {
  ticker: string; description: string; marketUrl?: string | null; venue?: string | null; side: "YES" | "NO" | "BOTH" | "—";
  yesContractsUnits: number; noContractsUnits: number; yesAverageCostPriceUnits?: number | null; noAverageCostPriceUnits?: number | null;
  totalCostUnits?: number | null; realizedPnlUnits?: number | null; realizedReturnBps?: number | null;
  markoutsByHorizon?: Record<string, MarkoutAggregate>;
  fillCount: number; orderCount: number; firstFillAtMs?: number | null; lastFillAtMs?: number | null;
  coverage: { fillsComplete: boolean; ordersComplete: boolean; fillPricesComplete?: boolean; fillFeesComplete?: boolean; descriptionComplete?: boolean; marketLinkAvailable?: boolean }; warnings: string[];
};
export type RunMarketsResponse = {
  generatedAt: number; runId: string; activityRevision?: string; source: SourceState; items: RunMarketMetrics[]; warnings: string[];
};
export type RunFillActivity = {
  fillId: string; orderId?: string | null; filledAtMs: number; side: "yes" | "no";
  contractsUnits: number; matchedContractsUnits: number; openContractsUnits: number;
  timeToFillMs?: number | null; totalPaidUnits?: number | null;
  liquidationValueUnits?: number | null; unrealizedValueUnits?: number | null; fillPnlUnits?: number | null;
  realizedPnlUnits?: number | null; unrealizedPnlUnits?: number | null;
  markoutsByHorizon?: Record<string, FillMarkout>;
};
export type RunOrderRevision = {
  revisionKey: string; orderId?: string | null; placedAtMs: number; side: "yes" | "no"; contractsUnits: number;
  timeOnBookMs: number; bookBidPriceUnits?: number | null; bookAskPriceUnits?: number | null;
  bookMidPriceUnits?: number | null; orderPriceUnits?: number | null; endedState: string;
};
export type RunMarketActivityResponse = {
  generatedAt: number; runId: string; market: RunMarketMetrics; source: SourceState;
  fills: { items: RunFillActivity[]; totalCount: number; truncated: boolean };
  orders: { items: RunOrderRevision[]; totalCount: number; truncated: boolean }; warnings: string[];
};
export type PnlTotals = { fills: number; feesCents: number; realizedCents: number; unrealizedCents: number; totalCents: number; complete?: boolean };
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
  openInterest?: { batches?: number; marketsRequested?: number; marketsResolved?: number; marketsMissing?: number; apiErrors?: number; forbiddenResponses?: number; cloudflare403?: number; retries?: number; retryExhausted?: number; suppressedRequests?: number };
  openInterestDiagnostics?: { lastCloudflare?: { atMs?: number; statusCode?: number; batch?: number; attempt?: number; cfRay?: string; cfCacheStatus?: string; server?: string; retryAfter?: string } | null };
};
export type ApiErrorRecord = {
  operation?: string; statusCode?: number | null; errorClass?: string; timestampMs?: number; message?: string;
};
export type ApiErrors = {
  total?: number; byOperation?: Record<string, { count?: number; last?: ApiErrorRecord | null }>; last?: ApiErrorRecord | null;
};
export type ClientMonitoring = {
  venue?: string; workerId?: string; marketId: string; title: string; pid?: number; lifecycle?: string; socketHealthy?: boolean; restartCount?: number;
  runtime?: { startedAtMs?: number; runningForMs?: number };
  market?: { marketId?: string; title?: string; seriesTicker?: string; eventTicker?: string; seriesTitle?: string; marketUrl?: string | null; priceUnits?: number | null; priceSource?: string; priceAtMs?: number | null; lastQuoteAtMs?: number | null };
  portfolio?: { startingPositionUnits?: number; currentPositionUnits?: number; updatedAtMs?: number };
  pnl?: PnlTotals & { sessionPositionUnits?: number; markPriceUnits?: number | null; markSource?: string; markAtMs?: number | null };
  fills?: { count?: number; quantityUnits?: number; lastFillAtMs?: number | null; recent?: Array<Record<string, unknown>> };
  orderActivity?: { byAction?: Record<string, { attempts?: number; successes?: number; errors?: number }>; lastActivityAtMs?: number | null; lastCreateAtMs?: number | null; active?: Record<string, Record<string, unknown>>; recent?: Array<Record<string, unknown>> };
  apiActivity?: ApiActivity; apiErrors?: ApiErrors;
  watchdog?: { running?: boolean; mode?: string; confidence?: number; reason?: string; updatedAtMs?: number };
};
export type Monitoring = {
  generatedAt: number; schemaVersion?: number; source: SourceState; warnings: string[];
  manager: { running?: boolean; lifecycle?: string; startedAtMs?: number; runningForMs?: number; activeVenues?: string[]; configuredBots?: number; botsRunning?: number; portfolio?: { items?: Array<{ marketId: string; title?: string; positionUnits?: number | null; updatedAtMs?: number; stale?: boolean; available?: boolean }>; grossPositionUnits?: number; netPositionUnits?: number; unknownMarkets?: number; staleMarkets?: number }; pnl?: PnlTotals; apiActivity?: ApiActivity; threadBudget?: Record<string, number | null> };
  venues?: Array<{ venue: string; active: boolean; botsRunning: number; configuredBots: number; apiActivity?: ApiActivity }>;
  clients: ClientMonitoring[];
  workers?: Array<{ venue?: string; workerId: string; pid?: number; running: boolean; phase?: string; lastRecoveryError?: string | null; recoveryReason?: string | null; startedAtMs?: number | null; runningForMs?: number | null; assignedMarkets: number; marketIds?: string[]; botsRunning?: number; startupPendingMarkets?: string[]; startupAttempts?: Record<string, number>; startupProgressAtMs?: number | null; startupElapsedMs?: number; lastStartupError?: string | null; workerError?: string | null; watchdog?: { mode?: string; counts?: Record<string, number> }; heartbeatAtMs?: number | null; stale: boolean; memoryRssBytes?: number | null; queueDepth?: number | null; eventLagMs?: number | null; apiActivity?: ApiActivity; apiErrors?: ApiErrors }>;
  threadBudget?: Record<string, number | null>;
  broker?: { pid?: number | null; running?: boolean; queue?: { queueWaitMs?: Record<string, unknown> }; apiActivity?: ApiActivity; apiErrors?: ApiErrors };
  capacity?: Record<string, unknown> | null;
  allocation?: Record<string, unknown> | null;
  screener: {
    running?: boolean; currentReason?: string; currentStartedAtMs?: number; currentDurationMs?: number;
    lastStartedAtMs?: number; lastCompletedAtMs?: number; lastDurationMs?: number; lastSuccessAtMs?: number;
    lastError?: string; generationId?: number; generatedAtMs?: number; reason?: string;
    picks?: Array<{ marketId: string; title: string; yesBudgetCents: number; noBudgetCents: number; selectionReason: string; marketClass?: string; rank?: number }>;
    changes?: Record<string, string[]>; apiActivity?: ApiActivity;
    lastRun?: ScreenerRunMetrics; scanMetadata?: Record<string, unknown>;
    history?: ScreenerRun[]; historySummary?: ScreenerRunSummary; historyNextCursor?: string | null; historyWarnings?: string[];
  };
};

export type ScreenerRunMetrics = {
  status?: "running" | "succeeded" | "failed" | "interrupted" | string;
  reason?: string; startedAt?: number | null; endedAt?: number | null;
  startedAtMs?: number | null; endedAtMs?: number | null; durationMs?: number | null;
  generationId?: number | null; configuredLimit?: number | null; effectiveLimit?: number | null;
  scannedMarkets?: number | null; apiRequests?: number | null; apiErrors?: number | null;
  added?: number; changed?: number; removed?: number; inventoryCarried?: number; inventoryUnknown?: number;
  warnings?: string[]; error?: string | null;
};

export type ScreenerRun = ScreenerRunMetrics & {
  id: string; fleetRunId: string; sessionId: string; sessionName: string; venue?: VenueName | "unknown";
};

export type ScreenerRunSummary = {
  totalRuns: number; succeeded: number; failed: number; interrupted: number; running: number;
  scannedMarkets: number; apiRequests: number; averageDurationMs?: number | null;
  added: number; changed: number; removed: number;
};

export type PortfolioPosition = {
  marketId: string; ticker: string; title: string; seriesTicker?: string; eventTicker?: string; seriesTitle?: string; marketUrl?: string | null; side: "yes" | "no";
  contractsUnits: number; lastPriceUnits?: number | null; bidPriceUnits?: number | null; askPriceUnits?: number | null; midPriceUnits?: number | null;
  costBasisUnits?: number | null; averageCostPriceUnits?: number | null; realizedPnlUnits?: number | null; feesUnits?: number | null;
  netRealizedPnlUnits?: number | null;
  unrealizedPnlUnits?: number | null; unrealizedReturnBps?: number | null; totalPnlUnits?: number | null; totalReturnBps?: number | null;
  marketUnrealizedPnlUnits?: number | null; marketUnrealizedReturnBps?: number | null; marketTotalPnlUnits?: number | null; marketTotalReturnBps?: number | null;
  totalTradedUnits?: number | null; openOrderCount: number; updatedAtMs?: number | null;
  liquidationValueUnits?: number | null; unrealizedValueUnits?: number | null; currentMarketValueUnits?: number | null;
  totalFillCount?: number; totalOrderCount?: number; lastTradeAtMs?: number | null; runningInCurrentSession?: boolean;
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

export type AnalyticsSource = { available: boolean; updatedAt: number | null; stale: boolean };
export type AnalyticsCoverage = { startedAtMs?: number | null; requestedWindowMs?: number; actualWindowMs?: number; partial?: boolean };
export type PortfolioHistoryMetric = {
  currentUnits?: number | null; baselineUnits?: number | null; changeUnits?: number | null; changeBps?: number | null;
  points: Array<{ timestampMs: number; valueUnits: number }>; partial: boolean; actualWindowMs: number;
};
export type PortfolioSummaryAnalytics = {
  generatedAt: number; snapshotAtMs?: number | null; source: AnalyticsSource; coverage: AnalyticsCoverage; warnings: string[];
  summary: {
    availableCashUnits?: number | null; midpointPositionValueUnits?: number | null; totalPortfolioValueUnits?: number | null;
    balanceComplete?: boolean; portfolioValueComplete?: boolean; liquidationValueComplete?: boolean;
    positionsLiquidationValueUnits?: number | null; apiTier?: string | null; positionCount?: number;
    readRateLimit?: { refillRate: number; bucketCapacity: number } | null;
    writeRateLimit?: { refillRate: number; bucketCapacity: number } | null;
  };
  perVenue?: Record<string, AccountPortfolio["summary"]>; includedVenues?: string[]; missingVenues?: string[]; displayCurrency?: string;
  history: { availableCash?: PortfolioHistoryMetric; totalPortfolioValue?: PortfolioHistoryMetric; positionsLiquidationValue?: PortfolioHistoryMetric };
};
export type PortfolioPositionsAnalytics = {
  generatedAt: number; snapshotAtMs?: number | null; source: AnalyticsSource; coverage: AnalyticsCoverage; warnings: string[]; items: PortfolioPosition[];
};
export type PortfolioFill = {
  fillId: string; tradeId: string; orderId: string; ticker: string; side?: "yes" | "no" | null;
  filledAtMs?: number | null; timeToFillMs?: number | null; contractsUnits: number;
  costOfContractsUnits?: number | null; notionalCostUnits?: number | null; feeUnits: number; costInPositionUnits?: number | null;
  liquidationValueUnits?: number | null; unrealizedValueUnits?: number | null; liquidationPnlUnits?: number | null; marketPnlUnits?: number | null;
  isTaker: boolean;
};
export type PortfolioFillsAnalytics = {
  generatedAt: number; snapshotAtMs?: number | null; source: AnalyticsSource; coverage: AnalyticsCoverage; warnings: string[];
  ticker: string; items: PortfolioFill[]; nextCursor?: string | null;
};
export type PortfolioOrderLine = {
  ticker: string; marketId: string; title: string; seriesTicker?: string; eventTicker?: string; seriesTitle?: string; marketUrl?: string | null; openOrderCount: number; ordersAttempted: number;
  remainingContractsUnits: number; initialContractsUnits: number; filledContractsUnits: number; totalFillCount: number;
  firstCreatedAtMs?: number | null; lastUpdatedAtMs?: number | null; totalTimeOnBookMs?: number | null;
  midPriceUnits?: number | null; totalMarketValueUnits?: number | null; runningInCurrentSession: boolean;
  sideBreakdown: Array<{ side: string; openOrderCount: number; remainingContractsUnits: number; midPriceUnits?: number | null; marketValueUnits?: number | null }>;
};
export type OptimizerRunArgs = {
  candidates?: number | null; workers?: number | null; markets?: number | null;
  budgetMinutes?: number | null; writeBack?: boolean; seed?: number | null; baseParams?: boolean;
  baseParamsJson?: string | null; tier?: number; recordRoot?: string | null; marketClass?: string | null;
  onlyParams?: string[]; topParams?: number | null; splits?: number | null; fillShare?: number | null;
  baseSession?: string | null; screenerSession?: string | null; screenerFilter?: boolean | null;
  screenerSource?: string | null; screenerEvalHours?: number | null; lastDays?: number | null;
  fromDate?: string | null; toDate?: string | null;
};
export type OptimizerRun = {
  id: string; status: string; startedMs?: number | null; finishedMs?: number | null;
  dataFromMs?: number | null; dataToMs?: number | null; marketCount: number; args: OptimizerRunArgs;
};
export type OptimizerQueueItem = {
  id: string; position?: number | null; status: string; requestId?: string | null; operator?: string | null;
  queuedAtMs?: number | null; launchedMs?: number | null; finishedMs?: number | null; error?: string | null;
  options: Record<string, unknown>; commandLine?: string | null; pid?: number | null;
};
export type OptimizerRunsResponse = {
  generatedAt: number; running: boolean; items: OptimizerRun[]; queue?: OptimizerQueueItem[]; lastLogLines: string[];
};
export type OptimizerParamDim = {
  name: string; kind: "bool" | "int" | "float"; low: number; high: number; default: unknown; group: string;
};
export type OptimizerParams = {
  generatedAt?: number; tier: number; searchable: OptimizerParamDim[]; pinned: Array<{ name: string; reason: string }>;
  presets: Record<string, string[]>; groups: string[];
};
export type OptimizerSessionOption = {
  id: string; name: string; hasScreener: boolean; selected: boolean; updatedAt?: number | null; description?: string | null;
};
export type OptimizerRecorderCoverage = {
  available: boolean; recordRoot?: string; hours: number; days: number; markets: number; bytes?: number;
  fromMs?: number | null; toMs?: number | null;
};
export type OptimizerOptions = {
  generatedAt?: number; classes: string[]; sessions: OptimizerSessionOption[];
  tiers: Array<{ tier: number; label: string; available: boolean }>; recorder: OptimizerRecorderCoverage;
  workers: { default: number; max: number };
  defaults?: { topParams?: number; splits?: number; fillShare?: number; screenerEvalHours?: number | null };
  presets?: Record<string, string[]>; previousSessionToken: string; baseParamsWinnerAvailable: boolean;
  commandPrefix: string[]; outputDir: string;
};
export type OptimizerStartPayload = {
  tier?: number; recordRoot?: string; marketClass?: string; onlyParams?: string[];
  topParams?: number; workers?: number; splits?: number; fillShare?: number; seed?: number;
  baseSession?: string; screenerSession?: string; screenerFilter?: boolean; screenerEvalHours?: number;
  writeBack?: boolean; useBaseParams?: boolean; candidates?: number; markets?: number; budgetMinutes?: number;
  lastDays?: number; fromDate?: string; toDate?: string; queueAfterCurrent?: boolean;
};
export type OptimizerDataAvailability = {
  available: boolean;
  marketCount?: number; marketsWithTrades?: number; settledCount?: number;
  tradeCount?: number; candleCount?: number;
  fromMs?: number | null; toMs?: number | null; spanDays?: number | null;
};
export type OptimizerCandidate = {
  candidateId: string; scoreUnits?: number | null; trainScoreUnits?: number | null;
  fills: number; errorCount: number; params: Record<string, unknown>;
};
export type OptimizerSensitivity = { field: string; deltaUnits: number; rank: number };
export type OptimizerRunDetail = {
  generatedAt: number; run: OptimizerRun; leaderboard: OptimizerCandidate[];
  sensitivity: OptimizerSensitivity[]; report?: string | null;
  argsFull?: Record<string, unknown>; commandLine?: string | null;
  launch?: { launchId?: string | null; requestId?: string | null; operator?: string | null; launchedMs?: number | null; pid?: number | null; queueId?: string | null } | null;
};

export type PortfolioOrdersAnalytics = {
  generatedAt: number; snapshotAtMs?: number | null; source: AnalyticsSource; coverage: AnalyticsCoverage; warnings: string[];
  summary: {
    totalOpenOrders: number; ordersAttempted: number; lastOrderAtMs?: number | null; averageTimeBetweenOrdersMs?: number | null;
    lastFillAtMs?: number | null; averageFillTimeMs?: number | null; fillSampleSize: number; totalMarketValueUnits?: number | null;
  };
  items: PortfolioOrderLine[];
};
