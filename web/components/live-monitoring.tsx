"use client";

import { useEffect, useState } from "react";
import type { ApiActivity, ClientMonitoring, Monitoring } from "@/lib/types";
import { Money, StatusBadge, Time } from "./status";

export function formatDuration(milliseconds?: number | null) {
  if (milliseconds == null) return "Unavailable";
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return hours ? `${hours}h ${minutes}m` : minutes ? `${minutes}m ${seconds % 60}s` : `${seconds}s`;
}

function apiTotal(activity?: ApiActivity) { return activity?.rest?.total ?? 0; }
function orderAttempts(client: ClientMonitoring) {
  return Object.values(client.orderActivity?.byAction ?? {}).reduce((sum, item) => sum + (item.attempts ?? 0), 0);
}
function units(value?: number | null) { return value == null ? "Unavailable" : (value / 100).toFixed(2); }

export function LiveMonitoring({ initial }: { initial: Monitoring }) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const [clockMs, setClockMs] = useState(initial.generatedAt);
  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events");
    events.addEventListener("monitoring", event => { setData(JSON.parse((event as MessageEvent).data)); setConnected(true); });
    events.onerror = () => setConnected(false);
    const fallback = window.setInterval(async () => {
      if (events.readyState === EventSource.OPEN) return;
      const response = await fetch("/api/backend/api/v1/monitoring", { cache: "no-store" });
      if (response.ok) setData(await response.json());
    }, 5000);
    const clock = window.setInterval(() => setClockMs(Date.now()), 1000);
    return () => { events.close(); window.clearInterval(fallback); window.clearInterval(clock); };
  }, []);

  const manager = data.manager ?? {};
  const pnl = manager.pnl ?? { fills: 0, feesCents: 0, realizedCents: 0, unrealizedCents: 0, totalCents: 0 };
  const portfolio = manager.portfolio ?? {};
  const screener = data.screener ?? {};
  const currentScreenerDuration = screener.running && screener.currentStartedAtMs ? clockMs - screener.currentStartedAtMs : screener.currentDurationMs;
  return <>
    <header className="page-header"><div><span className="eyebrow">LIVE TELEMETRY</span><h1>Monitoring</h1><p>Manager, client, venue transport, portfolio, and screener activity.</p></div><div className="header-status"><StatusBadge value={manager.lifecycle} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    {(data.source.stale || data.warnings.length > 0) && <section className="warning-panel"><strong>Monitoring data may be stale</strong><ul>{data.warnings.map(item => <li key={item}>{item}</li>)}</ul></section>}
    <section className="metrics"><article><span>Bots running</span><strong>{manager.botsRunning ?? 0}<small> / {data.clients.length}</small></strong><p>Manager uptime {formatDuration(manager.startedAtMs ? clockMs - manager.startedAtMs : manager.runningForMs)}</p></article><article><span>Session P&amp;L</span><strong><Money cents={pnl.totalCents} signed /></strong><p><Money cents={pnl.realizedCents} signed /> realized · {pnl.fills ?? 0} fills</p></article><article><span>Portfolio</span><strong>{units(portfolio.grossPositionUnits)}</strong><p>{units(portfolio.netPositionUnits)} net · {portfolio.staleMarkets ?? 0} stale</p></article><article><span>Venue requests</span><strong>{apiTotal(manager.apiActivity)}</strong><p>{manager.apiActivity?.rest?.requestsLast60s ?? 0} REST / min · {manager.apiActivity?.rest?.errors ?? 0} errors</p></article></section>

    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">CLIENTS</span><h2>Bot activity</h2></div><strong>{data.clients.length} processes</strong></div><div className="table-wrap monitoring-table"><table><thead><tr><th>Market</th><th>Runtime / watchdog</th><th>Price</th><th>Position / P&amp;L</th><th>Fills / orders</th><th>API activity</th></tr></thead><tbody>{data.clients.map(client => <tr key={client.marketId}><td><strong>{client.title || client.market?.title || client.marketId}</strong><small className="mono">{client.marketId} · PID {client.pid ?? "—"}</small><details><summary>Activity details</summary><div className="monitor-detail"><span>Active orders: {Object.values(client.orderActivity?.active ?? {}).filter(item => item.orderId).length}</span><span>Last fill: <Time value={client.fills?.lastFillAtMs} /></span><span>Last order: <Time value={client.orderActivity?.lastActivityAtMs} /></span><span>Socket: {client.socketHealthy ? "healthy" : "unavailable"}</span>{client.orderActivity?.recent?.slice(-3).reverse().map((item, index) => <span key={`order-${index}`}>Order: {String(item.action ?? "unknown")} · {String(item.outcome ?? "unknown")}</span>)}{client.fills?.recent?.slice(-3).reverse().map((item, index) => <span key={`fill-${index}`}>Fill: {String(item.side ?? "unknown")} · {units(Number(item.quantityUnits ?? 0))} contracts</span>)}</div></details></td><td><StatusBadge value={client.lifecycle} /><small>{formatDuration(client.runtime?.startedAtMs ? clockMs - client.runtime.startedAtMs : client.runtime?.runningForMs)}</small><StatusBadge value={client.watchdog?.running ? client.watchdog.mode : "stopped"} /></td><td>{client.market?.priceUnits == null ? "—" : `${(client.market.priceUnits / 100).toFixed(2)}¢`}<small>{client.market?.priceSource ?? "unavailable"}</small></td><td>{units(client.portfolio?.currentPositionUnits)}<small><Money cents={client.pnl?.totalCents} signed /></small></td><td>{client.fills?.count ?? 0} fills<small>{orderAttempts(client)} order attempts</small></td><td>{apiTotal(client.apiActivity)} REST<small>{client.apiActivity?.rest?.errors ?? 0} errors · {client.apiActivity?.stream?.message ?? 0} messages</small></td></tr>)}</tbody></table>{data.clients.length === 0 && <p className="empty">No bot clients are currently reporting.</p>}</div></section>

    <section className="grid-two"><article className="panel"><div className="panel-heading"><div><span className="eyebrow">SCREENER</span><h2>Refresh activity</h2></div><StatusBadge value={screener.running ? "running" : screener.lastError ? "failed" : "idle"} /></div><dl className="details"><div><dt>Current duration</dt><dd>{screener.running ? formatDuration(currentScreenerDuration) : "Not running"}</dd></div><div><dt>Last completed</dt><dd><Time value={screener.lastCompletedAtMs} /></dd></div><div><dt>Last duration</dt><dd>{formatDuration(screener.lastDurationMs)}</dd></div><div><dt>Generation</dt><dd>{screener.generationId ?? "—"}</dd></div><div><dt>Latest changes</dt><dd>{screener.changes?.added?.length ?? 0} added · {screener.changes?.changed?.length ?? 0} changed · {screener.changes?.removed?.length ?? 0} removed</dd></div><div><dt>Venue requests</dt><dd>{apiTotal(screener.apiActivity)} total</dd></div>{screener.lastError && <div><dt>Last error</dt><dd className="negative">{screener.lastError}</dd></div>}</dl></article><article className="panel"><div className="panel-heading"><div><span className="eyebrow">CURRENT PICKS</span><h2>{screener.picks?.length ?? 0} selected markets</h2></div></div><div className="pick-list">{screener.picks?.map(pick => <div key={pick.marketId}><span><strong>{pick.title || pick.marketId}</strong><small className="mono">{pick.marketId} · {pick.selectionReason}</small></span><span>#{pick.rank ?? "—"}<small><Money cents={pick.yesBudgetCents} /> / <Money cents={pick.noBudgetCents} /></small></span></div>)}{!screener.picks?.length && <p className="empty">No successful screener generation is available.</p>}</div></article></section>
  </>;
}
