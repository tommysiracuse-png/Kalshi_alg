import { apiGet } from "@/lib/api";
import type { HistoricalRun, SavedSession } from "@/lib/types";
import { Money, StatusBadge, Time } from "@/components/status";

type Summary = { timesRun: number; runtimeMs: number; orders: number; ordersPerMinute: number; fills: number; fillsPerMinute: number; apiCalls: number; apiErrors: number; totalCents: number; pnlComplete: boolean; outcomes: Record<string, number>; apiByComponent: Record<string, number> };
type RunMetric = { runtimeMs?: number; orders?: number; fills?: number; totalCents?: number; apiCalls?: number; apiErrors?: number; markets?: Record<string, unknown> };
function duration(ms: number) { const seconds = Math.floor(ms / 1000); const hours = Math.floor(seconds / 3600); const minutes = Math.floor((seconds % 3600) / 60); return `${hours}h ${minutes}m`; }
function bytes(value?: number) { if (value == null) return "—"; const units = ["B", "KB", "MB", "GB"]; let size = value, index = 0; while (size >= 1024 && index < units.length - 1) { size /= 1024; index++; } return `${size.toFixed(index ? 1 : 0)} ${units[index]}`; }

export default async function MetricsPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const params = await searchParams;
  const sessionId = typeof params.session_id === "string" ? params.session_id : "";
  const status = typeof params.status === "string" ? params.status : "";
  const from = typeof params.from === "string" ? params.from : "";
  const to = typeof params.to === "string" ? params.to : "";
  const query = new URLSearchParams(); if (sessionId) query.set("session_id", sessionId); if (status) query.set("status", status);
  if (from && Number.isFinite(Date.parse(`${from}T00:00:00`))) query.set("from_ms", String(Date.parse(`${from}T00:00:00`)));
  if (to && Number.isFinite(Date.parse(`${to}T23:59:59.999`))) query.set("to_ms", String(Date.parse(`${to}T23:59:59.999`)));
  const [data, sessionData] = await Promise.all([apiGet<{ summary: Summary; runs: HistoricalRun[] }>(`/api/v1/metrics?${query}`), apiGet<{ items: SavedSession[] }>("/api/v1/sessions?include_archived=true")]);
  const summary = data.summary;
  return <>
    <header className="page-header"><div><span className="eyebrow">HISTORICAL PERFORMANCE</span><h1>Metrics</h1><p>Local, read-only run history. No exchange requests are made by this view.</p></div><strong>{summary.timesRun} runs</strong></header>
    <form className="filters"><label>Session<select name="session_id" defaultValue={sessionId}><option value="">All sessions</option>{sessionData.items.map(item => <option key={item.id} value={item.id}>{item.name}{item.archivedAt ? " (archived)" : ""}</option>)}</select></label><label>Status<select name="status" defaultValue={status}><option value="">All outcomes</option>{["pending","starting","running","stopped","failed","shutdown_failed","interrupted"].map(item => <option key={item}>{item}</option>)}</select></label><label>From<input type="date" name="from" defaultValue={from} /></label><label>To<input type="date" name="to" defaultValue={to} /></label><button className="button">Apply</button></form>
    {!summary.pnlComplete && <section className="warning-panel"><strong>P&amp;L is incomplete</strong><p>At least one open position had no durable end-of-run mark; unknown unrealized value was not silently treated as profit.</p></section>}
    <section className="metrics order-metrics"><article><span>Strategy P&amp;L</span><strong><Money cents={summary.totalCents} signed /></strong><p>{summary.pnlComplete ? "Complete marks" : "Incomplete marks"}</p></article><article><span>Orders / min</span><strong>{summary.ordersPerMinute.toFixed(2)}</strong><p>{summary.orders} attempts</p></article><article><span>Fills / min</span><strong>{summary.fillsPerMinute.toFixed(2)}</strong><p>{summary.fills} fills</p></article><article><span>Runtime</span><strong className="small-value">{duration(summary.runtimeMs)}</strong><p>{summary.timesRun} launches</p></article><article><span>API calls</span><strong>{summary.apiCalls}</strong><p>{summary.apiErrors} errors</p></article></section>
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">EXCHANGE REST</span><h2>API calls by component</h2></div></div><div className="source-list">{Object.entries(summary.apiByComponent).map(([name, count]) => <div key={name}><strong>{name}</strong><span>{count}</span></div>)}</div></section>
    <div className="table-wrap"><table><thead><tr><th>Run</th><th>Session</th><th>Status</th><th>Started / runtime</th><th>Orders / fills</th><th>P&amp;L</th><th>API</th><th>Artifacts</th></tr></thead><tbody>{data.runs.map(run => { const metric = run.metrics as RunMetric; return <tr key={run.id}><td><details><summary className="mono">{run.id.slice(0, 8)}</summary><div className="run-detail"><strong>Configuration v{run.configurationVersion}</strong><pre>{JSON.stringify(run.configuration, null, 2)}</pre>{metric.markets && <pre>{JSON.stringify(metric.markets, null, 2)}</pre>}<small className="mono">{run.artifactPath}</small>{run.error && <p className="negative">{run.error}</p>}</div></details></td><td>{run.sessionName}</td><td><StatusBadge value={run.status} /></td><td><Time value={run.startedAt ?? run.createdAt} /><small>{duration(Number(metric.runtimeMs ?? 0))}</small></td><td>{Number(metric.orders ?? 0)} / {Number(metric.fills ?? 0)}</td><td><Money cents={Number(metric.totalCents ?? 0)} signed /></td><td>{Number(metric.apiCalls ?? 0)}<small>{Number(metric.apiErrors ?? 0)} errors</small></td><td>{bytes(run.artifactBytes)}</td></tr>; })}</tbody></table>{!data.runs.length && <p className="empty">No session-aware runs match these filters. Legacy artifacts were intentionally not guessed into sessions.</p>}</div>
  </>;
}
