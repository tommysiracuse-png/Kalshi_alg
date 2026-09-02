import Link from "next/link";
import { apiGet } from "@/lib/api";
import type { Market } from "@/lib/types";
import { Money, StatusBadge } from "@/components/status";

export default async function MarketsPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const params = await searchParams;
  const search = typeof params.search === "string" ? params.search : "";
  const mode = typeof params.mode === "string" ? params.mode : "";
  const sort = typeof params.sort === "string" ? params.sort : "rank";
  const query = new URLSearchParams({ search, watchdog_mode: mode, sort });
  const data = await apiGet<{ items: Market[] }>(`/api/v1/markets?${query}`);
  return <>
    <header className="page-header"><div><span className="eyebrow">MARKET FLEET</span><h1>Markets</h1><p>Opportunity, bot state, watchdog risk, and performance in one view.</p></div><strong>{data.items.length} results</strong></header>
    <form className="filters"><label>Search<input name="search" defaultValue={search} placeholder="Ticker or title" /></label><label>Watchdog<select name="mode" defaultValue={mode}><option value="">All modes</option><option>normal</option><option>reduction_only</option><option>flatten_only</option><option>unknown</option></select></label><label>Sort<select name="sort" defaultValue={sort}><option value="rank">Screener rank</option><option value="edge">Expected edge</option><option value="ticker">Ticker</option></select></label><button className="button">Apply</button></form>
    <div className="table-wrap"><table><thead><tr><th>Market</th><th>Rank</th><th>Expected edge</th><th>Position / P&amp;L</th><th>Watchdog</th><th>Bot</th></tr></thead><tbody>{data.items.map(item => <tr key={item.ticker}><td><Link href={`/markets/${encodeURIComponent(item.ticker)}`}><strong>{item.title}</strong><small>{item.ticker}</small></Link>{item.disabled && <span className="tag">Disabled</span>}</td><td>{item.rank ?? "—"}</td><td>{item.expectedEdgeCents?.toFixed(2) ?? "—"}¢</td><td>{item.pnl ? <><span>{item.pnl.netPosition > 0 ? "+" : ""}{item.pnl.netPosition}</span><small><Money cents={item.pnl.totalCents} signed /></small></> : "—"}</td><td><StatusBadge value={item.watchdogMode} /><small>{item.watchdogConfidence == null ? (item.watchdogReason ?? "") : `${Math.round(item.watchdogConfidence * 100)}% confidence`}</small></td><td><StatusBadge value={item.botRunning ? "running" : "stopped"} /></td></tr>)}</tbody></table>{data.items.length === 0 && <p className="empty">No markets match these filters.</p>}</div>
  </>;
}
