"use client";

import { useEffect, useState } from "react";
import type { AccountPortfolio } from "@/lib/types";
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
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
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

export function LivePortfolio({ initial }: { initial: AccountPortfolio }) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const [clockMs, setClockMs] = useState(initial.generatedAt);
  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events");
    events.addEventListener("portfolio", event => {
      setData(JSON.parse((event as MessageEvent).data));
      setConnected(true);
    });
    events.onerror = () => setConnected(false);
    const fallback = window.setInterval(async () => {
      if (events.readyState === EventSource.OPEN) return;
      const response = await fetch("/api/backend/api/v1/portfolio", { cache: "no-store" });
      if (response.ok) setData(await response.json());
    }, 5000);
    const clock = window.setInterval(() => setClockMs(Date.now()), 1000);
    return () => { events.close(); window.clearInterval(fallback); window.clearInterval(clock); };
  }, []);

  const summary = data.summary ?? {};
  const orderSummary = data.orders?.summary ?? {};
  const positions = data.positions ?? [];
  const orders = data.orders?.items ?? [];
  const stale = Boolean(data.stale || data.source?.stale || !data.available);
  return <>
    <header className="page-header"><div><span className="eyebrow">ACCOUNT MONITORING</span><h1>Portfolio</h1><p>Authoritative positions and resting orders for Kalshi subaccount {data.subaccountNumber ?? 0}.</p></div><div className="header-status"><StatusBadge value={data.running ? "refreshing" : stale ? "stale" : "current"} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    {(stale || data.warnings?.length > 0) && <section className="warning-panel"><strong>{data.available ? "Portfolio data may be incomplete or stale" : "Portfolio data is unavailable"}</strong>{data.lastError && <p>{data.lastError}</p>}<ul>{(data.warnings ?? []).map(item => <li key={item}>{item}</li>)}</ul></section>}

    <section className="metrics"><article><span>Available cash</span><strong>{moneyUnits(summary.availableCashUnits)}</strong><p>Balance updated {age(summary.balanceUpdatedAtMs, clockMs)}</p></article><article><span>Liquidation unrealized</span><strong className={(summary.unrealizedPnlUnits ?? 0) < 0 ? "negative" : "positive"}>{moneyUnits(summary.unrealizedPnlUnits, true)}</strong><p>Bid-marked · {percentBps(summary.unrealizedReturnBps)} across {summary.positionCount ?? 0} positions</p></article><article><span>API tier</span><strong>{summary.apiTier ?? "Unavailable"}</strong><p>{summary.readRateLimit?.refillRate ?? "—"} reads/s · {summary.writeRateLimit?.refillRate ?? "—"} writes/s</p></article><article><span>Snapshot</span><strong>{age(data.lastSuccessAtMs, clockMs)}</strong><p>{data.running ? `Refreshing for ${duration(data.currentStartedAtMs ? clockMs - data.currentStartedAtMs : data.currentDurationMs)}` : `Every 15s · ${data.apiActivity?.rest?.total ?? 0} API requests`}</p></article></section>

    <section className="panel portfolio-section"><div className="panel-heading"><div><span className="eyebrow">POSITIONS</span><h2>Account inventory</h2></div><strong>{positions.length} markets</strong></div><div className="table-wrap portfolio-table"><table><thead><tr><th>Market</th><th>Side / contracts</th><th>Last / liquidation</th><th>Cost / average</th><th>Market Price P&amp;L</th><th>Liquidation P&amp;L</th><th>Open orders</th></tr></thead><tbody>{positions.map(position => <tr key={position.marketId}><td>{position.marketUrl ? <a className="external-link" href={position.marketUrl} target="_blank" rel="noreferrer"><strong>{position.title || position.ticker}</strong> ↗</a> : <strong>{position.title || position.ticker}</strong>}<small className="mono">{position.ticker}</small><small>Updated {age(position.updatedAtMs, clockMs)}</small></td><td><span className={`side side-${position.side}`}>{position.side.toUpperCase()}</span><small>{contractUnits(position.contractsUnits)} contracts</small></td><td>{priceUnits(position.lastPriceUnits)} last<small>{priceUnits(position.bidPriceUnits)} bid · {priceUnits(position.askPriceUnits)} ask</small></td><td>{moneyUnits(position.costBasisUnits)}<small>{priceUnits(position.averageCostPriceUnits)} average</small></td><td><Value value={position.marketTotalPnlUnits} signed /><small>{percentBps(position.marketTotalReturnBps)} total · <Value value={position.marketUnrealizedPnlUnits} signed /> unrealized</small></td><td><Value value={position.totalPnlUnits} signed /><small>{percentBps(position.totalReturnBps)} total · <Value value={position.unrealizedPnlUnits} signed /> unrealized · {moneyUnits(position.feesUnits)} fees</small></td><td>{position.openOrderCount}</td></tr>)}</tbody></table>{positions.length === 0 && <p className="empty">{data.available ? "No open positions in this subaccount." : "No portfolio snapshot is available."}</p>}</div></section>

    <section className="metrics compact order-metrics"><article><span>Open orders</span><strong>{orderSummary.openOrderCount ?? 0}</strong><p>Resting now</p></article><article><span>Last order</span><strong className="small-value">{age(orderSummary.lastOrderAtMs, clockMs)}</strong><p>Rolling 24-hour history</p></article><article><span>Last fill</span><strong className="small-value">{age(orderSummary.lastFillAtMs, clockMs)}</strong><p>Rolling 24-hour history</p></article><article><span>Open market value</span><strong>{moneyUnits(orderSummary.openMarketValueUnits)}</strong><p>At resting limit prices</p></article><article><span>Average fill time</span><strong className="small-value">{duration(orderSummary.averageFirstFillTimeMs)}</strong><p>Creation to first fill · {orderSummary.filledOrderSampleSize ?? 0} orders</p></article></section>

    <section className="panel portfolio-section"><div className="panel-heading"><div><span className="eyebrow">ORDERS</span><h2>Resting orders</h2></div><strong>{orders.length} open</strong></div><div className="table-wrap portfolio-table"><table><thead><tr><th>Market / order</th><th>Side / remaining</th><th>Average fill</th><th>Order price</th><th>Current market</th><th>Last fill</th><th>First-fill time</th></tr></thead><tbody>{orders.map(order => <tr key={order.orderId}><td>{order.marketUrl ? <a className="external-link" href={order.marketUrl} target="_blank" rel="noreferrer"><strong>{order.title || order.ticker}</strong> ↗</a> : <strong>{order.title || order.ticker}</strong>}<small className="mono">{order.ticker} · {order.orderId}</small><small>Created <Time value={order.createdAtMs} /></small></td><td><span className={`side side-${order.side ?? "unknown"}`}>{order.side?.toUpperCase() ?? "UNKNOWN"}</span><small>{contractUnits(order.remainingContractsUnits)} remaining · {contractUnits(order.filledContractsUnits)} filled</small></td><td>{priceUnits(order.averageFillPriceUnits)}</td><td>{priceUnits(order.orderPriceUnits)}<small>{moneyUnits(order.openMarketValueUnits)} open value</small></td><td>{priceUnits(order.bidPriceUnits)} / {priceUnits(order.askPriceUnits)}<small>{priceUnits(order.midPriceUnits)} midpoint</small></td><td>{order.lastFillAtMs ? <><Time value={order.lastFillAtMs} /><small>{age(order.lastFillAtMs, clockMs)}</small></> : <span className="muted">No fill</span>}</td><td>{duration(order.firstFillTimeMs)}</td></tr>)}</tbody></table>{orders.length === 0 && <p className="empty">{data.available ? "No resting orders in this subaccount." : "No order snapshot is available."}</p>}</div></section>
  </>;
}
