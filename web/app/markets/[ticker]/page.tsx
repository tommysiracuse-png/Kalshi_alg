import { ControlPanel } from "@/components/control-panel";
import { LogViewer } from "@/components/log-viewer";
import { MiniChart } from "@/components/mini-chart";
import { Money, StatusBadge } from "@/components/status";
import { apiGet } from "@/lib/api";
import type { Market } from "@/lib/types";

type Detail = { generatedAt: number; market: Market; position: { available: boolean; stale: boolean; positionUnits?: number; fetchedAt?: number; error?: string }; watchdog: Record<string, unknown>; telemetry: { fills: Array<Record<string, unknown>>; quotes: Array<Record<string, unknown>>; markouts: Array<Record<string, unknown>>; market_state: Array<Record<string, unknown>> }; source: Record<string, unknown> };

export default async function MarketPage({ params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;
  const data = await apiGet<Detail>(`/api/v1/markets/${encodeURIComponent(ticker)}`);
  const market = data.market;
  const quoteValues = data.telemetry.quotes.slice().reverse().map(row => Number(row.quote_units ?? 0) / 100).filter(Number.isFinite);
  return <>
    <header className="page-header"><div><span className="eyebrow">MARKET DETAIL</span><h1>{market.title}</h1><p className="mono">{market.ticker}</p></div><div className="header-status"><StatusBadge value={market.botRunning ? "running" : "stopped"} /><StatusBadge value={market.watchdogMode} /></div></header>
    <section className="metrics compact"><article><span>Screener rank</span><strong>#{market.rank ?? "—"}</strong><p>{market.expectedEdgeCents?.toFixed(2) ?? "—"}¢ expected edge</p></article><article><span>Exchange position</span><strong>{data.position.available && data.position.positionUnits != null ? (data.position.positionUnits / 100).toFixed(2) : "Unavailable"}</strong><p>{data.position.stale ? "Stale or unavailable" : "Canonical exchange inventory"}</p></article><article><span>Watchdog confidence</span><strong>{market.watchdogConfidence == null ? "—" : `${Math.round(market.watchdogConfidence * 100)}%`}</strong><p>{market.watchdogReason ?? "No reason reported"}</p></article><article><span>Total P&amp;L</span><strong><Money cents={market.pnl?.totalCents} signed /></strong><p>{data.telemetry.fills.length} recent fills</p></article></section>
    <ControlPanel ticker={market.ticker} disabled={market.disabled} />
    <section className="grid-two"><article className="panel"><div className="panel-heading"><div><span className="eyebrow">QUOTE TELEMETRY</span><h2>Recent quote levels</h2></div></div><MiniChart values={quoteValues} /></article><article className="panel"><div className="panel-heading"><div><span className="eyebrow">WATCHDOG</span><h2>Risk profile</h2></div></div><dl className="details">{["mode", "confidence", "reason", "observed_volatility_bp_60s_equiv", "bid_flip_rate_per_minute", "effective_close_time_ms"].map(key => <div key={key}><dt>{key.replaceAll("_", " ")}</dt><dd>{String(data.watchdog[key] ?? "Unavailable")}</dd></div>)}</dl></article></section>
    <LogViewer ticker={market.ticker} />
  </>;
}
