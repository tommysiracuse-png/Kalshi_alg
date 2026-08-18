"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  HistoricalRun,
  MetricsHeartbeat,
  MetricsResponse,
  RunMarketActivityResponse,
  RunMarketsResponse,
  SavedSession,
} from "@/lib/types";
import { duration, moneyUnits, percentBps, priceUnits, contractUnits } from "./live-portfolio";
import { Money, StatusBadge, Time } from "./status";

const LIMITS = [25, 50, 100, 250, 500] as const;
const ACTIVE_STATES = new Set(["pending", "starting", "running"]);

type LoadState<T> = { data?: T; loading: boolean; error?: string };
function bytes(value?: number) {
  if (value == null) return "—";
  const units = ["B", "KB", "MB", "GB"];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) { size /= 1024; index += 1; }
  return `${size.toFixed(index ? 1 : 0)} ${units[index]}`;
}

function metric(run: HistoricalRun, key: string) {
  const value = run.metrics[key];
  return typeof value === "number" ? value : 0;
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api/backend${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return response.json() as Promise<T>;
}

function toggleSet(setter: React.Dispatch<React.SetStateAction<Set<string>>>, key: string) {
  setter(previous => {
    const next = new Set(previous);
    if (next.has(key)) next.delete(key); else next.add(key);
    return next;
  });
}

function Side({ value }: { value?: string | null }) {
  const normalized = value?.toLowerCase();
  return <span className={`side ${normalized === "yes" ? "side-yes" : normalized === "no" ? "side-no" : "side-unknown"}`}>{value?.toUpperCase() ?? "—"}</span>;
}

function Limits({ fillLimit, orderLimit, onChange }: { fillLimit: number; orderLimit: number; onChange: (kind: "fill" | "order", value: number) => void }) {
  return <div className="metrics-limits" aria-label="Activity display caps">
    <label>Fills<select value={fillLimit} onChange={event => onChange("fill", Number(event.target.value))}>{LIMITS.map(value => <option key={value}>{value}</option>)}</select></label>
    <label>Orders<select value={orderLimit} onChange={event => onChange("order", Number(event.target.value))}>{LIMITS.map(value => <option key={value}>{value}</option>)}</select></label>
    <small>Browser-local newest-row caps</small>
  </div>;
}

export function LiveMetrics({ initial, sessions, query, filters = { sessionId: "", status: "", from: "", to: "" } }: { initial: MetricsResponse; sessions: SavedSession[]; query: string; filters?: { sessionId: string; status: string; from: string; to: string } }) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const [expandedSessions, setExpandedSessions] = useState<Set<string>>(new Set());
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());
  const [expandedMarkets, setExpandedMarkets] = useState<Set<string>>(new Set());
  const [runMarkets, setRunMarkets] = useState<Record<string, LoadState<RunMarketsResponse>>>({});
  const [marketActivity, setMarketActivity] = useState<Record<string, LoadState<RunMarketActivityResponse>>>({});
  const [fillLimit, setFillLimit] = useState(100);
  const [orderLimit, setOrderLimit] = useState(100);
  const expandedRunsRef = useRef(expandedRuns);
  const expandedMarketsRef = useRef(expandedMarkets);
  const dataRef = useRef(data);
  const initialActive = initial.runs.find(run => ACTIVE_STATES.has(run.status));
  const activeIdentityRef = useRef(initialActive?.id ?? "none");
  const activityRevisionRef = useRef("");

  useEffect(() => { expandedRunsRef.current = expandedRuns; }, [expandedRuns]);
  useEffect(() => { expandedMarketsRef.current = expandedMarkets; }, [expandedMarkets]);
  useEffect(() => { dataRef.current = data; }, [data]);
  useEffect(() => {
    const storedFill = Number(window.localStorage.getItem("kalshi.metrics.fillLimit"));
    const storedOrder = Number(window.localStorage.getItem("kalshi.metrics.orderLimit"));
    if (LIMITS.includes(storedFill as typeof LIMITS[number])) setFillLimit(storedFill);
    if (LIMITS.includes(storedOrder as typeof LIMITS[number])) setOrderLimit(storedOrder);
  }, []);

  const loadRunMarkets = useCallback(async (runId: string) => {
    setRunMarkets(previous => ({ ...previous, [runId]: { ...previous[runId], loading: true, error: undefined } }));
    try {
      const response = await fetchJson<RunMarketsResponse>(`/api/v1/runs/${encodeURIComponent(runId)}/markets`);
      setRunMarkets(previous => ({ ...previous, [runId]: { data: response, loading: false } }));
    } catch (error) {
      setRunMarkets(previous => ({ ...previous, [runId]: { ...previous[runId], loading: false, error: error instanceof Error ? error.message : "Could not load markets" } }));
    }
  }, []);

  const loadActivity = useCallback(async (runId: string, ticker: string, fills = fillLimit, orders = orderLimit) => {
    const key = `${runId}:${ticker}`;
    setMarketActivity(previous => ({ ...previous, [key]: { ...previous[key], loading: true, error: undefined } }));
    try {
      const response = await fetchJson<RunMarketActivityResponse>(`/api/v1/runs/${encodeURIComponent(runId)}/markets/${encodeURIComponent(ticker)}/activity?fill_limit=${fills}&order_limit=${orders}`);
      setMarketActivity(previous => ({ ...previous, [key]: { data: response, loading: false } }));
    } catch (error) {
      setMarketActivity(previous => ({ ...previous, [key]: { ...previous[key], loading: false, error: error instanceof Error ? error.message : "Could not load activity" } }));
    }
  }, [fillLimit, orderLimit]);

  const refreshMetrics = useCallback(async () => {
    try {
      const params = new URLSearchParams(query);
      params.set("include_artifact_bytes", "false");
      const response = await fetchJson<MetricsResponse>(`/api/v1/metrics?${params}`);
      setData(previous => {
        const prior = new Map(previous.runs.map(run => [run.id, run]));
        return { ...response, runs: response.runs.map(run => ({ ...prior.get(run.id), ...run, artifactBytes: prior.get(run.id)?.artifactBytes })) };
      });
    } catch {
      setConnected(false);
    }
  }, [query]);

  const applyHeartbeat = useCallback((heartbeat: MetricsHeartbeat) => {
    const active = heartbeat.activeRun;
    setConnected(Boolean(heartbeat.source?.available && !heartbeat.source?.stale));
    if (active) {
      setData(previous => {
        const index = previous.runs.findIndex(run => run.id === active.id);
        if (index < 0) return previous;
        const priorRun = previous.runs[index];
        const metrics = { ...priorRun.metrics, ...active.summary };
        const runs = [...previous.runs];
        runs[index] = { ...priorRun, status: active.status, heartbeatAt: active.heartbeatAt, metrics };
        const numeric = ["runtimeMs", "orders", "fills", "apiCalls", "apiErrors", "realizedCents", "unrealizedCents", "totalCents"] as const;
        const summary = { ...previous.summary };
        for (const key of numeric) {
          const before = typeof priorRun.metrics[key] === "number" ? priorRun.metrics[key] as number : 0;
          const after = typeof metrics[key] === "number" ? metrics[key] as number : before;
          summary[key] += after - before;
        }
        const minutes = summary.runtimeMs / 60_000;
        summary.ordersPerMinute = minutes ? summary.orders / minutes : 0;
        summary.fillsPerMinute = minutes ? summary.fills / minutes : 0;
        summary.pnlComplete = runs.every(run => run.metrics.pnlComplete !== false);
        if (priorRun.status !== active.status) {
          summary.outcomes = { ...summary.outcomes };
          summary.outcomes[priorRun.status] = Math.max(0, (summary.outcomes[priorRun.status] ?? 0) - 1);
          summary.outcomes[active.status] = (summary.outcomes[active.status] ?? 0) + 1;
        }
        return { generatedAt: heartbeat.generatedAt, runs, summary };
      });
    }

    const nextIdentity = active?.id ?? "none";
    if (nextIdentity !== activeIdentityRef.current) {
      const previousIdentity = activeIdentityRef.current;
      activeIdentityRef.current = nextIdentity;
      activityRevisionRef.current = active?.activityRevision ?? "";
      void refreshMetrics();
      if (previousIdentity !== "none" && expandedRunsRef.current.has(previousIdentity)) {
        void loadRunMarkets(previousIdentity);
        for (const key of expandedMarketsRef.current) {
          if (key.startsWith(`${previousIdentity}:`)) void loadActivity(previousIdentity, key.slice(previousIdentity.length + 1));
        }
      }
      return;
    }
    if (!active || active.activityRevision === activityRevisionRef.current) return;
    activityRevisionRef.current = active.activityRevision;
    if (!expandedRunsRef.current.has(active.id)) return;
    void loadRunMarkets(active.id);
    for (const key of expandedMarketsRef.current) {
      if (key.startsWith(`${active.id}:`)) void loadActivity(active.id, key.slice(active.id.length + 1));
    }
  }, [loadActivity, loadRunMarkets, refreshMetrics]);

  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events");
    events.addEventListener("metrics_heartbeat", event => {
      applyHeartbeat(JSON.parse((event as MessageEvent).data) as MetricsHeartbeat);
    });
    events.onopen = () => setConnected(true);
    events.onerror = () => setConnected(false);
    const fallback = window.setInterval(() => {
      if (events.readyState !== EventSource.OPEN) {
        void fetchJson<MetricsHeartbeat>("/api/v1/metrics/heartbeat")
          .then(applyHeartbeat)
          .catch(() => setConnected(false));
      }
    }, 5000);
    return () => { events.close(); window.clearInterval(fallback); };
  }, [applyHeartbeat]);

  const grouped = useMemo(() => {
    const byId = new Map(sessions.map(session => [session.id, { session, runs: [] as HistoricalRun[] }]));
    for (const run of data.runs) {
      const group = byId.get(run.sessionId) ?? { session: { id: run.sessionId, name: run.sessionName } as SavedSession, runs: [] };
      group.runs.push(run);
      byId.set(run.sessionId, group);
    }
    return [...byId.values()].filter(group => group.runs.length).sort((a, b) => Math.max(...b.runs.map(run => run.createdAt)) - Math.max(...a.runs.map(run => run.createdAt)));
  }, [data.runs, sessions]);

  const changeLimit = (kind: "fill" | "order", value: number) => {
    const nextFill = kind === "fill" ? value : fillLimit;
    const nextOrder = kind === "order" ? value : orderLimit;
    if (kind === "fill") { setFillLimit(value); window.localStorage.setItem("kalshi.metrics.fillLimit", String(value)); }
    else { setOrderLimit(value); window.localStorage.setItem("kalshi.metrics.orderLimit", String(value)); }
    void Promise.all([...expandedMarketsRef.current].map(key => {
      const separator = key.indexOf(":");
      return loadActivity(key.slice(0, separator), key.slice(separator + 1), nextFill, nextOrder);
    }));
  };

  const summary = data.summary;
  return <>
    <header className="page-header"><div><span className="eyebrow">SESSION PERFORMANCE</span><h1>Metrics</h1><p>Live and completed run telemetry from local storage. This view makes no exchange requests.</p></div><div className="header-status"><strong>{summary.timesRun} runs</strong><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    <form className="filters"><label>Session<select name="session_id" defaultValue={filters.sessionId}><option value="">All sessions</option>{sessions.map(item => <option key={item.id} value={item.id}>{item.name}{item.archivedAt ? " (archived)" : ""}</option>)}</select></label><label>Status<select name="status" defaultValue={filters.status}><option value="">All outcomes</option>{["pending", "starting", "running", "stopped", "failed", "shutdown_failed", "interrupted"].map(item => <option key={item}>{item}</option>)}</select></label><label>From<input type="date" name="from" defaultValue={filters.from} /></label><label>To<input type="date" name="to" defaultValue={filters.to} /></label><button className="button">Apply</button></form>
    {!summary.pnlComplete && <section className="warning-panel"><strong>P&amp;L is incomplete</strong><p>At least one open position had no durable end-of-run mark; unknown value remains explicitly unavailable.</p></section>}
    <section className="metrics order-metrics"><article><span>Strategy P&amp;L</span><strong><Money cents={summary.totalCents} signed /></strong><p>{summary.pnlComplete ? "Complete marks" : "Incomplete marks"}</p></article><article><span>Orders / min</span><strong>{summary.ordersPerMinute.toFixed(2)}</strong><p>{summary.orders} attempts</p></article><article><span>Fills / min</span><strong>{summary.fillsPerMinute.toFixed(2)}</strong><p>{summary.fills} fills</p></article><article><span>Runtime</span><strong className="small-value">{duration(summary.runtimeMs)}</strong><p>{summary.timesRun} launches</p></article><article><span>API calls</span><strong>{summary.apiCalls}</strong><p>{summary.apiErrors} errors</p></article></section>
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">EXCHANGE REST</span><h2>API calls by component</h2></div></div><div className="source-list">{Object.entries(summary.apiByComponent).map(([name, count]) => <div key={name}><strong>{name}</strong><span>{count}</span></div>)}</div></section>
    <div className="metrics-table-toolbar"><div><span className="eyebrow">SESSIONS → RUNS → MARKETS</span><h2>Trading activity</h2></div><Limits fillLimit={fillLimit} orderLimit={orderLimit} onChange={changeLimit} /></div>
    <div className="metrics-tree">
      {grouped.map(({ session, runs }) => {
        const open = expandedSessions.has(session.id);
        const runtime = runs.reduce((total, run) => total + metric(run, "runtimeMs"), 0);
        const orders = runs.reduce((total, run) => total + metric(run, "orders"), 0);
        const fills = runs.reduce((total, run) => total + metric(run, "fills"), 0);
        const pnl = runs.reduce((total, run) => total + metric(run, "totalCents"), 0);
        return <section className="metrics-session" key={session.id}>
          <button className="metrics-session-row" type="button" aria-expanded={open} onClick={() => toggleSet(setExpandedSessions, session.id)}><span className="tree-chevron">{open ? "−" : "+"}</span><span><strong>{session.name}</strong><small>{runs.length} run{runs.length === 1 ? "" : "s"}</small></span><span><small>Runtime</small>{duration(runtime)}</span><span><small>Orders / fills</small>{orders} / {fills}</span><span><small>P&amp;L</small><Money cents={pnl} signed /></span></button>
          {open && <div className="metrics-runs table-wrap"><table><thead><tr><th>Run</th><th>Status</th><th>Started / runtime</th><th>Orders / fills</th><th>P&amp;L</th><th>API</th><th>Artifacts</th></tr></thead><tbody>
            {runs.map(run => {
              const runOpen = expandedRuns.has(run.id);
              const state = runMarkets[run.id];
              return <Fragment key={run.id}><tr className="expandable-position" onClick={() => { toggleSet(setExpandedRuns, run.id); if (!runOpen && !state?.data) void loadRunMarkets(run.id); }}><td><div className="market-cell"><button type="button" className="row-toggle" aria-label={`${runOpen ? "Collapse" : "Expand"} run ${run.id}`} aria-expanded={runOpen}>{runOpen ? "−" : "+"}</button><span className="mono">{run.id.slice(0, 8)}{ACTIVE_STATES.has(run.status) && <small className="session-tag">Live run</small>}</span></div></td><td><StatusBadge value={run.status} /></td><td><Time value={run.startedAt ?? run.createdAt} /><small>{duration(metric(run, "runtimeMs"))}</small></td><td>{metric(run, "orders")} / {metric(run, "fills")}</td><td><Money cents={metric(run, "totalCents")} signed /></td><td>{metric(run, "apiCalls")}<small>{metric(run, "apiErrors")} errors</small></td><td>{bytes(run.artifactBytes)}</td></tr>
                {runOpen && <tr className="metrics-nested-row"><td colSpan={7}><RunMarkets run={run} state={state} expandedMarkets={expandedMarkets} activity={marketActivity} onToggle={(ticker, isOpen) => { const key = `${run.id}:${ticker}`; toggleSet(setExpandedMarkets, key); if (!isOpen && !marketActivity[key]?.data) void loadActivity(run.id, ticker); }} /></td></tr>}
              </Fragment>;
            })}
          </tbody></table></div>}
        </section>;
      })}
      {!grouped.length && <p className="empty">No session-aware runs match these filters. Legacy artifacts were intentionally not guessed into sessions.</p>}
    </div>
  </>;
}

function RunMarkets({ run, state, expandedMarkets, activity, onToggle }: { run: HistoricalRun; state?: LoadState<RunMarketsResponse>; expandedMarkets: Set<string>; activity: Record<string, LoadState<RunMarketActivityResponse>>; onToggle: (ticker: string, isOpen: boolean) => void }) {
  if (state?.loading && !state.data) return <p className="empty">Loading markets…</p>;
  if (state?.error && !state.data) return <p className="error">{state.error}</p>;
  if (!state?.data?.items.length) return <p className="empty">No markets with recorded order or fill activity.</p>;
  return <div className="run-markets"><div className="nested-heading"><strong>Markets</strong><span><Time value={state.data.source.updatedAt} />{state.data.source.stale ? " · stale" : ""}</span></div>{state.error && <div className="coverage-warning">Refresh failed; showing last-known markets. {state.error}</div>}{state.data.warnings.length > 0 && <div className="coverage-warning">{state.data.warnings.join(" ")}</div>}<div className="table-wrap market-metrics-table"><table><thead><tr><th>Market</th><th>Side</th><th>Contracts</th><th>Avg cost</th><th>Total cost</th><th>Realized</th><th>Fills / orders</th><th>First fill</th><th>Last fill</th></tr></thead><tbody>
    {state.data.items.map(market => {
      const key = `${run.id}:${market.ticker}`;
      const open = expandedMarkets.has(key);
      return <Fragment key={market.ticker}><tr className="expandable-position" onClick={() => onToggle(market.ticker, open)}><td><div className="market-cell"><button type="button" className="row-toggle" aria-expanded={open} aria-label={`${open ? "Collapse" : "Expand"} ${market.ticker}`}>{open ? "−" : "+"}</button><span><strong>{market.marketUrl ? <a className="external-link" href={market.marketUrl} target="_blank" rel="noreferrer" onClick={event => event.stopPropagation()}>{market.description} ↗</a> : market.description}</strong><small className="mono">{market.ticker}</small></span></div></td><td><Side value={market.side} /></td><td><span>YES {contractUnits(market.yesContractsUnits)}</span><small>NO {contractUnits(market.noContractsUnits)}</small></td><td><span>YES {priceUnits(market.yesAverageCostPriceUnits)}</span><small>NO {priceUnits(market.noAverageCostPriceUnits)}</small></td><td>{moneyUnits(market.totalCostUnits)}</td><td className={market.realizedPnlUnits == null ? "muted" : market.realizedPnlUnits < 0 ? "negative" : market.realizedPnlUnits > 0 ? "positive" : ""}>{moneyUnits(market.realizedPnlUnits, true)}<small>{percentBps(market.realizedReturnBps)}</small></td><td>{market.fillCount} / {market.orderCount}</td><td><Time value={market.firstFillAtMs} /></td><td><Time value={market.lastFillAtMs} /></td></tr>
        {open && <tr className="metrics-nested-row"><td colSpan={9}><MarketActivity state={activity[key]} /></td></tr>}
      </Fragment>;
    })}
  </tbody></table></div></div>;
}

function MarketActivity({ state }: { state?: LoadState<RunMarketActivityResponse> }) {
  if (state?.loading && !state.data) return <p className="empty">Loading fills and orders…</p>;
  if (state?.error && !state.data) return <p className="error">{state.error}</p>;
  if (!state?.data) return null;
  const { fills, orders, warnings } = state.data;
  return <div className="market-activity">{state.error && <div className="coverage-warning">Refresh failed; showing last-known fills and orders. {state.error}</div>}{warnings.length > 0 && <div className="coverage-warning">{warnings.join(" ")}</div>}
    <div className="nested-heading"><strong>Fills</strong><span>{fills.truncated ? `Newest ${fills.items.length} of ${fills.totalCount}` : `${fills.totalCount} total`}</span></div>
    <div className="table-wrap activity-table"><table><thead><tr><th>Fill time</th><th>Fill ID</th><th>Side</th><th>Contracts</th><th>Time to fill</th><th>Total paid</th><th>Realized P&amp;L</th><th>Unrealized P&amp;L</th><th>Fill P&amp;L</th></tr></thead><tbody>{fills.items.map(fill => <tr key={`${fill.fillId}:${fill.filledAtMs}`}><td><Time value={fill.filledAtMs} /></td><td className="mono">{fill.fillId}</td><td><Side value={fill.side} /></td><td>{contractUnits(fill.contractsUnits)}<small>{contractUnits(fill.matchedContractsUnits)} matched · {contractUnits(fill.openContractsUnits)} open</small></td><td>{duration(fill.timeToFillMs)}</td><td>{moneyUnits(fill.totalPaidUnits)}</td><td className={fill.realizedPnlUnits == null ? "muted" : fill.realizedPnlUnits < 0 ? "negative" : fill.realizedPnlUnits > 0 ? "positive" : ""}>{moneyUnits(fill.realizedPnlUnits, true)}</td><td className={fill.unrealizedPnlUnits == null ? "muted" : fill.unrealizedPnlUnits < 0 ? "negative" : fill.unrealizedPnlUnits > 0 ? "positive" : ""}>{moneyUnits(fill.unrealizedPnlUnits, true)}</td><td className={fill.fillPnlUnits == null ? "muted" : fill.fillPnlUnits < 0 ? "negative" : fill.fillPnlUnits > 0 ? "positive" : ""}>{moneyUnits(fill.fillPnlUnits, true)}</td></tr>)}</tbody></table>{!fills.items.length && <p className="empty">No fills recorded.</p>}</div>
    <div className="nested-heading orders-heading"><strong>Orders</strong><span>{orders.truncated ? `Newest ${orders.items.length} of ${orders.totalCount}` : `${orders.totalCount} total`}</span></div>
    <div className="table-wrap activity-table"><table><thead><tr><th>Order time</th><th>Order ID</th><th>Side</th><th>Contracts</th><th>Time on book</th><th>Book bid / ask / mid</th><th>Order price</th><th>Ended state</th></tr></thead><tbody>{orders.items.map(order => <tr key={order.revisionKey}><td><Time value={order.placedAtMs} /></td><td className="mono">{order.orderId ?? "Unavailable"}</td><td><Side value={order.side} /></td><td>{contractUnits(order.contractsUnits)}</td><td>{duration(order.timeOnBookMs)}</td><td>{priceUnits(order.bookBidPriceUnits)} / {priceUnits(order.bookAskPriceUnits)} / {priceUnits(order.bookMidPriceUnits)}</td><td>{priceUnits(order.orderPriceUnits)}</td><td><StatusBadge value={order.endedState} /></td></tr>)}</tbody></table>{!orders.items.length && <p className="empty">{warnings.length ? "Detailed orders unavailable for this run." : "No order attempts recorded."}</p>}</div>
  </div>;
}
