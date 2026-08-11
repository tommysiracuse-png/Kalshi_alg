"use client";

import { useEffect, useRef, useState } from "react";
import type { Overview, SavedSession } from "@/lib/types";
import { Money, StatusBadge, Time } from "./status";
import { ControlPanel } from "./control-panel";
import { OverviewSessionSelector } from "./overview-session-selector";

export function LiveOverview({ initial, sessions, activeRun }: { initial: Overview; sessions: SavedSession[]; activeRun?: { id: string; sessionId: string; sessionName: string; status: string } | null }) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const failures = useRef(0);
  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events");
    events.addEventListener("overview", event => { setData(JSON.parse((event as MessageEvent).data)); setConnected(true); failures.current = 0; });
    events.onerror = () => { setConnected(false); failures.current += 1; };
    const fallback = window.setInterval(async () => {
      if (connected || failures.current < 2) return;
      const response = await fetch("/api/backend/api/v1/overview", { cache: "no-store" });
      if (response.ok) setData(await response.json());
    }, 5000);
    return () => { events.close(); window.clearInterval(fallback); };
  }, [connected]);
  const launcher = data.fleet.launcher ?? {};
  const counts = data.fleet.counts ?? {};
  return <>
    <header className="page-header"><div><span className="eyebrow">OPERATIONS & RISK</span><h1>Fleet overview</h1><p>Live health, inventory, and performance across the market-maker fleet.</p></div><div className="header-status"><StatusBadge value={launcher.lifecycle} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    {launcher.environment === "production" && <div className="prod-banner" role="status">Production environment · controls affect live quoting</div>}
    {data.warnings.length > 0 && <section className="warning-panel"><strong>Attention required</strong><ul>{data.warnings.slice(0, 5).map(item => <li key={item}>{item}</li>)}</ul></section>}
    <section className="metrics" aria-label="Key metrics">
      <article><span>Active bots</span><strong>{counts.activeBots ?? 0}<small> / {counts.configuredBots ?? 0}</small></strong><p>{data.marketCounts.screened} screened markets</p></article>
      <article><span>Total P&amp;L</span><strong><Money cents={data.pnl.totalCents} signed /></strong><p><Money cents={data.pnl.realizedCents} signed /> realized</p></article>
      <article><span>Open positions</span><strong>{data.positionSummary.markets}</strong><p>{data.positionSummary.grossContracts} gross · {data.positionSummary.netContracts > 0 ? "+" : ""}{data.positionSummary.netContracts} net contracts</p></article>
      <article><span>Disabled</span><strong>{data.marketCounts.disabled}</strong><p>Heartbeat <Time value={launcher.heartbeatAt} /></p></article>
    </section>
    <OverviewSessionSelector initial={sessions} activeRun={activeRun} />
    <ControlPanel />
    <section className="grid-two">
      <article className="panel"><div className="panel-heading"><div><span className="eyebrow">RISK MODES</span><h2>Watchdogs</h2></div></div><div className="mode-list">{Object.entries(counts.watchdogModes ?? {}).map(([mode, count]) => <div key={mode}><StatusBadge value={mode} /><strong>{count}</strong></div>)}{Object.keys(counts.watchdogModes ?? {}).length === 0 && <p className="empty">No watchdog state available.</p>}</div></article>
      <article className="panel"><div className="panel-heading"><div><span className="eyebrow">INVENTORY</span><h2>Open positions</h2></div></div>{data.positions.length ? <div className="position-list">{data.positions.slice(0, 8).map(item => <div key={item.ticker}><span>{item.ticker}<small>{item.netPosition > 0 ? "YES" : "NO"} {Math.abs(item.netPosition)}</small></span><Money cents={item.totalCents} signed /></div>)}</div> : <p className="empty">No positions reported by fill telemetry.</p>}</article>
    </section>
  </>;
}
