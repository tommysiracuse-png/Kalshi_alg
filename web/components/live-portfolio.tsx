"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import type {
  AccountPortfolio,
  PortfolioFill,
  PortfolioFillsAnalytics,
  PortfolioOrdersAnalytics,
  PortfolioPositionsAnalytics,
  PortfolioSummaryAnalytics,
} from "@/lib/types";
import { PortfolioHistoryChart, type PortfolioWindow } from "./portfolio-history-chart";
import { PortfolioMarketsTable } from "./portfolio-markets-table";
import { StatusBadge, Time } from "./status";

export function moneyUnits(value?: number | null, signed = false) {
  if (value == null) return "Unavailable";
  const prefix = signed && value > 0 ? "+" : "";
  return `${prefix}$${(value / 10_000).toFixed(2)}`;
}

export function priceUnits(value?: number | null) {
  return value == null ? "—" : `${(value / 100).toFixed(2)}¢`;
}

export function contractUnits(value?: number | null) {
  return value == null ? "—" : (value / 100).toFixed(2);
}

export function percentBps(value?: number | null) {
  if (value == null) return "—";
  return `${value > 0 ? "+" : ""}${(value / 100).toFixed(2)}%`;
}

export function duration(milliseconds?: number | null) {
  if (milliseconds == null) return "—";
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days) return `${days}d ${hours}h`;
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

function age(timestamp: number | null | undefined, now: number) {
  return timestamp ? `${duration(now - timestamp)} ago` : "Unavailable";
}

function Value({ value, signed = false }: { value?: number | null; signed?: boolean }) {
  return <span className={value == null ? "muted" : value < 0 ? "negative" : value > 0 ? "positive" : ""}>{moneyUnits(value, signed)}</span>;
}

function numericRange(value: number | null | undefined, minimum: string, maximum: string, scale = 1) {
  if (value == null) return minimum === "" && maximum === "";
  const normalized = value / scale;
  return (minimum === "" || normalized >= Number(minimum)) && (maximum === "" || normalized <= Number(maximum));
}

function dateRange(value: number | null | undefined, from: string, to: string) {
  if (value == null) return from === "" && to === "";
  const lower = from ? new Date(from).getTime() : null;
  const upper = to ? new Date(to).getTime() : null;
  return (lower == null || value >= lower) && (upper == null || value <= upper);
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api/backend${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return response.json() as Promise<T>;
}

type FillPageState = { items: PortfolioFill[]; nextCursor?: string | null; loading: boolean; error?: string };

const emptyPositionFilters = { ticker: "", contractsMin: "", contractsMax: "", valueMin: "", valueMax: "", ordersMin: "", ordersMax: "", currentSession: false };
const emptyOrderFilters = { ticker: "", createdFrom: "", createdTo: "", updatedFrom: "", updatedTo: "", contractsMin: "", contractsMax: "", bookMin: "", bookMax: "", fillsMin: "", fillsMax: "", currentSession: false };

export function LivePortfolio({
  initialSummary,
  initialPositions,
  initialOrders,
  view = "summary",
}: {
  initialSummary: PortfolioSummaryAnalytics;
  initialPositions: PortfolioPositionsAnalytics;
  initialOrders: PortfolioOrdersAnalytics;
  view?: "summary" | "markets";
}) {
  const [summaryData, setSummaryData] = useState(initialSummary);
  const [positionsData, setPositionsData] = useState(initialPositions);
  const [ordersData, setOrdersData] = useState(initialOrders);
  const [connected, setConnected] = useState(false);
  const [clockMs, setClockMs] = useState(initialSummary.generatedAt);
  const [historyWindow, setHistoryWindow] = useState<PortfolioWindow>("24h");
  const [historyLoading, setHistoryLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState<string | null>(null);
  const [positionFilters, setPositionFilters] = useState(emptyPositionFilters);
  const [orderFilters, setOrderFilters] = useState(emptyOrderFilters);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [fillPages, setFillPages] = useState<Record<string, FillPageState>>({});
  const expandedRef = useRef(expanded);
  const snapshotRef = useRef(initialSummary.snapshotAtMs ?? 0);

  useEffect(() => { expandedRef.current = expanded; }, [expanded]);

  const loadFills = useCallback(async (ticker: string, cursor = "", replace = false) => {
    setFillPages(previous => ({
      ...previous,
      [ticker]: { items: replace ? [] : previous[ticker]?.items ?? [], nextCursor: previous[ticker]?.nextCursor, loading: true },
    }));
    try {
      const query = cursor ? `?limit=100&cursor=${encodeURIComponent(cursor)}` : "?limit=100";
      const response = await fetchJson<PortfolioFillsAnalytics>(`/api/v1/portfolio/positions/${encodeURIComponent(ticker)}/fills${query}`);
      setFillPages(previous => ({
        ...previous,
        [ticker]: {
          items: replace ? response.items : [...(previous[ticker]?.items ?? []), ...response.items],
          nextCursor: response.nextCursor,
          loading: false,
        },
      }));
    } catch (error) {
      setFillPages(previous => ({
        ...previous,
        [ticker]: { ...(previous[ticker] ?? { items: [] }), loading: false, error: error instanceof Error ? error.message : "Could not load fills" },
      }));
    }
  }, []);

  const refreshAnalytics = useCallback(async () => {
    try {
      const [summary, positions, orders] = await Promise.all([
        fetchJson<PortfolioSummaryAnalytics>(`/api/v1/portfolio/summary?window=${historyWindow}`),
        fetchJson<PortfolioPositionsAnalytics>("/api/v1/portfolio/positions"),
        fetchJson<PortfolioOrdersAnalytics>("/api/v1/portfolio/orders"),
      ]);
      snapshotRef.current = summary.snapshotAtMs ?? snapshotRef.current;
      setSummaryData(summary);
      setPositionsData(positions);
      setOrdersData(orders);
      setAnalyticsError(null);
      await Promise.all([...expandedRef.current].map(ticker => loadFills(ticker, "", true)));
    } catch (error) {
      setAnalyticsError(error instanceof Error ? error.message : "Could not refresh portfolio analytics");
    }
  }, [historyWindow, loadFills]);

  const changeHistoryWindow = useCallback(async (next: PortfolioWindow) => {
    if (next === historyWindow) return;
    setHistoryWindow(next);
    setHistoryLoading(true);
    try {
      const summary = await fetchJson<PortfolioSummaryAnalytics>(`/api/v1/portfolio/summary?window=${next}`);
      snapshotRef.current = summary.snapshotAtMs ?? snapshotRef.current;
      setSummaryData(summary);
      setAnalyticsError(null);
    } catch (error) {
      setHistoryWindow(historyWindow);
      setAnalyticsError(error instanceof Error ? error.message : "Could not load portfolio history");
    } finally {
      setHistoryLoading(false);
    }
  }, [historyWindow]);

  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events?topics=portfolio");
    events.addEventListener("portfolio", event => {
      const payload = JSON.parse((event as MessageEvent).data) as AccountPortfolio;
      setConnected(true);
      const nextSnapshot = payload.generatedAtMs ?? payload.lastSuccessAtMs ?? 0;
      if (nextSnapshot && nextSnapshot !== snapshotRef.current) void refreshAnalytics();
    });
    events.onerror = () => setConnected(false);
    const fallback = window.setInterval(() => {
      if (events.readyState !== EventSource.OPEN) void refreshAnalytics();
    }, 5000);
    const clock = window.setInterval(() => setClockMs(Date.now()), 1000);
    return () => { events.close(); window.clearInterval(fallback); window.clearInterval(clock); };
  }, [refreshAnalytics]);

  const visiblePositions = useMemo(() => positionsData.items.filter(position => {
    const ticker = `${position.ticker} ${position.title}`.toLowerCase();
    return ticker.includes(positionFilters.ticker.trim().toLowerCase())
      && numericRange(position.contractsUnits, positionFilters.contractsMin, positionFilters.contractsMax, 100)
      && numericRange(position.unrealizedValueUnits, positionFilters.valueMin, positionFilters.valueMax, 10_000)
      && numericRange(position.openOrderCount, positionFilters.ordersMin, positionFilters.ordersMax)
      && (!positionFilters.currentSession || Boolean(position.runningInCurrentSession));
  }), [positionFilters, positionsData.items]);

  const visibleOrders = useMemo(() => ordersData.items.filter(order => {
    const ticker = `${order.ticker} ${order.title}`.toLowerCase();
    return ticker.includes(orderFilters.ticker.trim().toLowerCase())
      && dateRange(order.firstCreatedAtMs, orderFilters.createdFrom, orderFilters.createdTo)
      && dateRange(order.lastUpdatedAtMs, orderFilters.updatedFrom, orderFilters.updatedTo)
      && numericRange(order.remainingContractsUnits, orderFilters.contractsMin, orderFilters.contractsMax, 100)
      && numericRange(order.totalTimeOnBookMs, orderFilters.bookMin, orderFilters.bookMax, 3_600_000)
      && numericRange(order.totalFillCount, orderFilters.fillsMin, orderFilters.fillsMax)
      && (!orderFilters.currentSession || order.runningInCurrentSession);
  }), [orderFilters, ordersData.items]);

  function togglePosition(ticker: string) {
    const opening = !expanded.has(ticker);
    const next = new Set(expanded);
    if (opening) next.add(ticker); else next.delete(ticker);
    setExpanded(next);
    if (opening && !fillPages[ticker]) void loadFills(ticker);
  }

  const warnings = [...new Set([...summaryData.warnings, ...positionsData.warnings, ...ordersData.warnings, ...(analyticsError ? [analyticsError] : [])])];
  const stale = summaryData.source.stale || positionsData.source.stale || ordersData.source.stale;
  const orderSummary = ordersData.summary;

  return <>
    <header className="page-header"><div><span className="eyebrow">ACCOUNT ANALYTICS</span><h1>Portfolio</h1><p>Account-wide inventory, fill history, and open-order analytics from locally persisted snapshots.</p></div><div className="header-status"><StatusBadge value={stale ? "stale" : "current"} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    <nav className="portfolio-subnav" aria-label="Portfolio views"><Link href="/portfolio" aria-current={view === "summary" ? "page" : undefined}>Summary</Link><Link href="/portfolio?view=markets" aria-current={view === "markets" ? "page" : undefined}>Markets</Link></nav>
    {(stale || warnings.length > 0) && <section className="warning-panel"><strong>{stale ? "Portfolio analytics may be stale" : "Portfolio analytics notice"}</strong><ul>{warnings.map(item => <li key={item}>{item}</li>)}</ul></section>}

    {view === "markets" ? <PortfolioMarketsTable data={positionsData} /> : <>
    <section className="metrics portfolio-summary-metrics">
      <article><span>Available Cash</span><strong>{moneyUnits(summaryData.summary.availableCashUnits)}</strong><p>Cash available to trade</p></article>
      <article><span>Total Portfolio Value</span><strong>{moneyUnits(summaryData.summary.totalPortfolioValueUnits)}</strong><p>Cash plus midpoint position value</p></article>
      <article><span>Liquidation Value</span><strong>{moneyUnits(summaryData.summary.positionsLiquidationValueUnits)}</strong><p>Positions marked at same-side bid</p></article>
      <article><span>API Tier</span><strong>{summaryData.summary.apiTier ?? "Unavailable"}</strong><p>{summaryData.summary.readRateLimit?.refillRate ?? "—"} reads/s · {summaryData.summary.writeRateLimit?.refillRate ?? "—"} writes/s</p></article>
      <article><span>Last Snapshot</span><strong className="small-value">{age(summaryData.snapshotAtMs, clockMs)}</strong><p><Time value={summaryData.snapshotAtMs} /></p></article>
    </section>

    <PortfolioHistoryChart data={summaryData} window={historyWindow} loading={historyLoading} onWindowChange={next => void changeHistoryWindow(next)} />

    <section className="panel portfolio-section">
      <div className="panel-heading"><div><span className="eyebrow">POSITIONS</span><h2>Account Inventory</h2></div><strong>{visiblePositions.length} of {positionsData.items.length} markets</strong></div>
      <div className="portfolio-filter-panel">
        <label className="wide-filter"><span>Ticker</span><input value={positionFilters.ticker} onChange={event => setPositionFilters({ ...positionFilters, ticker: event.target.value })} placeholder="Search ticker or description" /></label>
        <label><span>Contracts min</span><input type="number" value={positionFilters.contractsMin} onChange={event => setPositionFilters({ ...positionFilters, contractsMin: event.target.value })} /></label>
        <label><span>Contracts max</span><input type="number" value={positionFilters.contractsMax} onChange={event => setPositionFilters({ ...positionFilters, contractsMax: event.target.value })} /></label>
        <label><span>Unrealized $ min</span><input type="number" value={positionFilters.valueMin} onChange={event => setPositionFilters({ ...positionFilters, valueMin: event.target.value })} /></label>
        <label><span>Unrealized $ max</span><input type="number" value={positionFilters.valueMax} onChange={event => setPositionFilters({ ...positionFilters, valueMax: event.target.value })} /></label>
        <label><span>Open orders min</span><input type="number" value={positionFilters.ordersMin} onChange={event => setPositionFilters({ ...positionFilters, ordersMin: event.target.value })} /></label>
        <label><span>Open orders max</span><input type="number" value={positionFilters.ordersMax} onChange={event => setPositionFilters({ ...positionFilters, ordersMax: event.target.value })} /></label>
        <label className="checkbox-filter"><input type="checkbox" checked={positionFilters.currentSession} onChange={event => setPositionFilters({ ...positionFilters, currentSession: event.target.checked })} /><span>Running in current session</span></label>
        <button className="button" type="button" onClick={() => setPositionFilters(emptyPositionFilters)}>Clear filters</button>
      </div>
      <div className="table-wrap portfolio-table"><table><thead><tr><th>Market</th><th>Venue</th><th>Side / contracts</th><th>Bid / ask / mid</th><th>Cost / average</th><th>Liquidation value</th><th>Unrealized value</th><th>Fills / orders</th><th>Open orders</th></tr></thead><tbody>
        {visiblePositions.map(position => {
          const isExpanded = expanded.has(position.ticker);
          const fills = fillPages[position.ticker];
          return <Fragment key={position.marketId}>
            <tr className="expandable-position" tabIndex={0} aria-expanded={isExpanded} onClick={() => togglePosition(position.ticker)} onKeyDown={event => { if (event.target === event.currentTarget && (event.key === "Enter" || event.key === " ")) { event.preventDefault(); togglePosition(position.ticker); } }}>
              <td><div className="market-cell"><button type="button" className="row-toggle" aria-label={`${isExpanded ? "Collapse" : "Expand"} fills for ${position.ticker}`} aria-expanded={isExpanded} onClick={event => { event.stopPropagation(); togglePosition(position.ticker); }}>{isExpanded ? "⌄" : "›"}</button><div>{position.marketUrl ? <a className="external-link" href={position.marketUrl} target="_blank" rel="noreferrer" onClick={event => event.stopPropagation()}><strong>{position.title || position.ticker}</strong> ↗</a> : <strong>{position.title || position.ticker}</strong>}<small className="mono">{position.ticker}</small>{position.runningInCurrentSession && <small className="session-tag">Current session</small>}</div></div></td>
              <td>{position.marketUrl ? <a className="external-link venue-market-link" href={position.marketUrl} target="_blank" rel="noreferrer" onClick={event => event.stopPropagation()}>Open market ↗</a> : <span className="muted">Unavailable</span>}</td>
              <td><span className={`side side-${position.side}`}>{position.side.toUpperCase()}</span><small>{contractUnits(position.contractsUnits)} contracts</small></td>
              <td>{priceUnits(position.bidPriceUnits)} / {priceUnits(position.askPriceUnits)}<small>{priceUnits(position.midPriceUnits)} midpoint</small></td>
              <td>{moneyUnits(position.costBasisUnits)}<small>{priceUnits(position.averageCostPriceUnits)} average</small></td>
              <td>{moneyUnits(position.liquidationValueUnits)}</td>
              <td>{moneyUnits(position.unrealizedValueUnits)}</td>
              <td>{position.totalFillCount ?? 0} / {position.totalOrderCount ?? 0}<small>Observed since <Time value={positionsData.coverage.startedAtMs} /></small></td>
              <td>{position.openOrderCount}</td>
            </tr>
            <tr className={`position-detail-row ${isExpanded ? "open" : ""}`}><td colSpan={9}><div className="position-expansion" aria-hidden={!isExpanded}><div>
              <div className="fill-heading"><strong>Fill history</strong><span>{fills?.items.length ?? 0} loaded</span></div>
              {fills?.error && <p className="error">{fills.error} <button type="button" className="link-inline" onClick={() => void loadFills(position.ticker, "", true)}>Retry</button></p>}
              {!fills && <p className="empty">Open the position to load fills.</p>}
              {fills && !fills.loading && !fills.error && fills.items.length === 0 && <p className="empty">No stored fills are available for this market.</p>}
              {fills && fills.items.length > 0 && <div className="table-wrap fill-table"><table><thead><tr><th>Filled</th><th>Side / contracts</th><th>Time to fill</th><th>Cost / total paid</th><th>Liquidation value</th><th>Unrealized value</th><th>Fill P&amp;L</th></tr></thead><tbody>{fills.items.map(fill => <tr key={fill.fillId}><td><Time value={fill.filledAtMs} /><small className="mono">{fill.orderId}</small></td><td><span className={`side side-${fill.side ?? "unknown"}`}>{fill.side?.toUpperCase() ?? "UNKNOWN"}</span><small>{contractUnits(fill.contractsUnits)} contracts</small></td><td>{duration(fill.timeToFillMs)}</td><td>{priceUnits(fill.costOfContractsUnits)}<small>{moneyUnits(fill.costInPositionUnits)} incl. {moneyUnits(fill.feeUnits)} fees</small></td><td>{moneyUnits(fill.liquidationValueUnits)}</td><td>{moneyUnits(fill.unrealizedValueUnits)}</td><td><Value value={fill.liquidationPnlUnits} signed /><small>Liquidation · <Value value={fill.marketPnlUnits} signed /> midpoint</small></td></tr>)}</tbody></table></div>}
              {fills?.loading && <p className="empty">Loading fills…</p>}
              {fills?.nextCursor && !fills.loading && <button className="button load-more" type="button" onClick={() => void loadFills(position.ticker, fills.nextCursor ?? "")}>Load more</button>}
            </div></div></td></tr>
          </Fragment>;
        })}
      </tbody></table>{visiblePositions.length === 0 && <p className="empty">No positions match the current filters.</p>}</div>
    </section>

    <section className="metrics compact order-summary-metrics">
      <article><span>Total Open Orders</span><strong>{orderSummary.totalOpenOrders ?? 0}</strong><p>Resting now</p></article>
      <article><span>Orders Attempted</span><strong>{orderSummary.ordersAttempted ?? 0}</strong><p>Placement attempts this session</p></article>
      <article><span>Last Order</span><strong className="small-value">{age(orderSummary.lastOrderAtMs, clockMs)}</strong><p><Time value={orderSummary.lastOrderAtMs} /></p></article>
      <article><span>Avg Placement Interval</span><strong className="small-value">{duration(orderSummary.averageTimeBetweenOrdersMs)}</strong><p>Across open orders</p></article>
      <article><span>Last Fill</span><strong className="small-value">{age(orderSummary.lastFillAtMs, clockMs)}</strong><p><Time value={orderSummary.lastFillAtMs} /></p></article>
      <article><span>Avg Fill Time</span><strong className="small-value">{duration(orderSummary.averageFillTimeMs)}</strong><p>{orderSummary.fillSampleSize ?? 0} fill samples</p></article>
      <article><span>Total Market Value</span><strong>{moneyUnits(orderSummary.totalMarketValueUnits)}</strong><p>Remaining contracts at midpoint</p></article>
    </section>

    <section className="panel portfolio-section">
      <div className="panel-heading"><div><span className="eyebrow">OPEN ORDERS</span><h2>Orders by Market Line</h2></div><strong>{visibleOrders.length} of {ordersData.items.length} lines</strong></div>
      <div className="portfolio-filter-panel order-filter-panel">
        <label className="wide-filter"><span>Ticker</span><input value={orderFilters.ticker} onChange={event => setOrderFilters({ ...orderFilters, ticker: event.target.value })} placeholder="Search ticker or description" /></label>
        <label><span>First creation from</span><input type="datetime-local" value={orderFilters.createdFrom} onChange={event => setOrderFilters({ ...orderFilters, createdFrom: event.target.value })} /></label>
        <label><span>First creation to</span><input type="datetime-local" value={orderFilters.createdTo} onChange={event => setOrderFilters({ ...orderFilters, createdTo: event.target.value })} /></label>
        <label><span>Last update from</span><input type="datetime-local" value={orderFilters.updatedFrom} onChange={event => setOrderFilters({ ...orderFilters, updatedFrom: event.target.value })} /></label>
        <label><span>Last update to</span><input type="datetime-local" value={orderFilters.updatedTo} onChange={event => setOrderFilters({ ...orderFilters, updatedTo: event.target.value })} /></label>
        <label><span>Contracts min</span><input type="number" value={orderFilters.contractsMin} onChange={event => setOrderFilters({ ...orderFilters, contractsMin: event.target.value })} /></label>
        <label><span>Contracts max</span><input type="number" value={orderFilters.contractsMax} onChange={event => setOrderFilters({ ...orderFilters, contractsMax: event.target.value })} /></label>
        <label><span>Book hours min</span><input type="number" value={orderFilters.bookMin} onChange={event => setOrderFilters({ ...orderFilters, bookMin: event.target.value })} /></label>
        <label><span>Book hours max</span><input type="number" value={orderFilters.bookMax} onChange={event => setOrderFilters({ ...orderFilters, bookMax: event.target.value })} /></label>
        <label><span>Total fills min</span><input type="number" value={orderFilters.fillsMin} onChange={event => setOrderFilters({ ...orderFilters, fillsMin: event.target.value })} /></label>
        <label><span>Total fills max</span><input type="number" value={orderFilters.fillsMax} onChange={event => setOrderFilters({ ...orderFilters, fillsMax: event.target.value })} /></label>
        <label className="checkbox-filter"><input type="checkbox" checked={orderFilters.currentSession} onChange={event => setOrderFilters({ ...orderFilters, currentSession: event.target.checked })} /><span>Running in current session</span></label>
        <button className="button" type="button" onClick={() => setOrderFilters(emptyOrderFilters)}>Clear filters</button>
      </div>
      <div className="table-wrap portfolio-table order-lines-table"><table><thead><tr><th>Market</th><th>Sides / open</th><th>Contracts</th><th>First created / last update</th><th>Time on book</th><th>Orders attempted</th><th>Total fills</th><th>Market value</th></tr></thead><tbody>{visibleOrders.map(order => <tr key={order.ticker}><td>{order.marketUrl ? <a className="external-link" href={order.marketUrl} target="_blank" rel="noreferrer"><strong>{order.title || order.ticker}</strong> ↗</a> : <strong>{order.title || order.ticker}</strong>}<small className="mono">{order.ticker}</small>{order.runningInCurrentSession && <small className="session-tag">Current session</small>}</td><td><div className="side-breakdown">{order.sideBreakdown.map(side => <span key={side.side} className={`side side-${side.side}`}>{side.side.toUpperCase()} {side.openOrderCount} · {priceUnits(side.midPriceUnits)}</span>)}</div><small>{order.openOrderCount} open orders</small></td><td>{contractUnits(order.remainingContractsUnits)} remaining<small>{contractUnits(order.initialContractsUnits)} initial · {contractUnits(order.filledContractsUnits)} filled</small></td><td><Time value={order.firstCreatedAtMs} /><small>Updated <Time value={order.lastUpdatedAtMs} /></small></td><td>{duration(order.totalTimeOnBookMs)}</td><td>{order.ordersAttempted}</td><td>{order.totalFillCount}</td><td>{moneyUnits(order.totalMarketValueUnits)}<small>{order.midPriceUnits == null && order.sideBreakdown.length > 1 ? "Side-specific midpoints" : `${priceUnits(order.midPriceUnits)} midpoint`}</small></td></tr>)}</tbody></table>{visibleOrders.length === 0 && <p className="empty">No open-order lines match the current filters.</p>}</div>
    </section>
    </>}
  </>;
}
