"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ApiActivity, ClientMonitoring, Monitoring, ScreenerRun, ScreenerRunSummary } from "@/lib/types";
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

type ScreenerColumnId = "session" | "status" | "started" | "ended" | "markets" | "api" | "duration" | "changes";
type ScreenerSortDirection = "asc" | "desc";
type ScreenerSort = { id: ScreenerColumnId; direction: ScreenerSortDirection };
type ScreenerColumnPreference = { id: ScreenerColumnId; enabled: boolean };
type ScreenerDropPosition = "before" | "after";
type ScreenerColumnWidths = Record<ScreenerColumnId, number>;
type ScreenerColumnDragRef = { current: { id: ScreenerColumnId } | null };

const SCREENER_COLUMN_KEY = "kalshi.screener.columns.v1";
const SCREENER_WIDTH_KEY = "kalshi.screener.widths.v1";
const SCREENER_COLUMN_VERSION = 1;
const SCREENER_MIN_WIDTH = 90;
const SCREENER_MAX_WIDTH = 720;
const SCREENER_COLUMNS: Array<{ id: ScreenerColumnId; label: string }> = [
  { id: "session", label: "Session run in" }, { id: "status", label: "Status of screener run" },
  { id: "started", label: "Time Started" }, { id: "ended", label: "Time Ended" },
  { id: "markets", label: "Markets to Screen" }, { id: "api", label: "API Requests to Venue" },
  { id: "duration", label: "Duration" }, { id: "changes", label: "Latest Changes" },
];
const DEFAULT_SCREENER_WIDTHS: ScreenerColumnWidths = {
  session: 240, status: 180, started: 170, ended: 170,
  markets: 180, api: 180, duration: 130, changes: 220,
};

function defaultScreenerPreferences(): ScreenerColumnPreference[] {
  return SCREENER_COLUMNS.map(column => ({ id: column.id, enabled: true }));
}

function sanitizeScreenerPreferences(value: unknown): ScreenerColumnPreference[] {
  const defaults = defaultScreenerPreferences();
  if (!value || typeof value !== "object") return defaults;
  const candidate = value as { version?: unknown; columns?: unknown };
  if (candidate.version !== SCREENER_COLUMN_VERSION || !Array.isArray(candidate.columns)) return defaults;
  const known = new Set(SCREENER_COLUMNS.map(column => column.id));
  const seen = new Set<ScreenerColumnId>();
  const result: ScreenerColumnPreference[] = [];
  for (const entry of candidate.columns) {
    if (!entry || typeof entry !== "object") continue;
    const item = entry as { id?: unknown; enabled?: unknown };
    if (typeof item.id !== "string" || !known.has(item.id as ScreenerColumnId) || seen.has(item.id as ScreenerColumnId)) continue;
    result.push({ id: item.id as ScreenerColumnId, enabled: typeof item.enabled === "boolean" ? item.enabled : true });
    seen.add(item.id as ScreenerColumnId);
  }
  for (const column of SCREENER_COLUMNS) if (!seen.has(column.id)) result.push({ id: column.id, enabled: true });
  return result;
}

function sanitizeScreenerWidths(value: unknown): ScreenerColumnWidths {
  const result = { ...DEFAULT_SCREENER_WIDTHS };
  if (!value || typeof value !== "object") return result;
  const candidate = value as { version?: unknown; widths?: unknown };
  if (candidate.version !== SCREENER_COLUMN_VERSION || !candidate.widths || typeof candidate.widths !== "object") return result;
  for (const [id, width] of Object.entries(candidate.widths)) {
    if (id in result && typeof width === "number" && Number.isFinite(width)) result[id as ScreenerColumnId] = Math.max(SCREENER_MIN_WIDTH, Math.min(SCREENER_MAX_WIDTH, Math.round(width)));
  }
  return result;
}

function formatCount(value?: number | null) { return value == null ? "Unavailable" : value.toLocaleString(); }

function mergeScreenerRuns(previous: ScreenerRun[], incoming: ScreenerRun[]) {
  const merged = new Map(previous.map(item => [item.id, item]));
  for (const item of incoming) merged.set(item.id, { ...merged.get(item.id), ...item });
  return [...merged.values()].sort((a, b) => (Number(b.startedAt ?? 0) - Number(a.startedAt ?? 0)) || b.id.localeCompare(a.id));
}

function screenerSummaryFallback(): ScreenerRunSummary {
  return { totalRuns: 0, succeeded: 0, failed: 0, interrupted: 0, running: 0, scannedMarkets: 0, apiRequests: 0, averageDurationMs: null, added: 0, changed: 0, removed: 0 };
}

function normalizeScreenerSummary(value?: ScreenerRunSummary): ScreenerRunSummary {
  return { ...screenerSummaryFallback(), ...(value ?? {}) };
}

function ScreenerColumnChooser({ preferences, onToggle, onMove }: {
  preferences: ScreenerColumnPreference[];
  onToggle: (id: ScreenerColumnId) => void;
  onMove: (id: ScreenerColumnId, direction: -1 | 1) => void;
}) {
  const labels = new Map(SCREENER_COLUMNS.map(column => [column.id, column.label]));
  return <details className="metrics-columns screener-columns"><summary className="button">Columns</summary><div className="metrics-columns-menu"><p className="metrics-columns-hint">Drag a table header left or right to reorder it. Arrow buttons provide the same control here.</p><section><strong>Screener runs</strong><div className="metrics-column-list">{preferences.map((item, index) => <div className="metrics-column-option" key={item.id}><label><input type="checkbox" aria-label={`Screener runs: ${labels.get(item.id)}`} checked={item.enabled} onChange={() => onToggle(item.id)} /><span>{labels.get(item.id)}</span></label><span className="metrics-column-movers"><button className="column-move" type="button" aria-label={`Move ${labels.get(item.id)} left in Screener runs`} disabled={index === 0} onClick={() => onMove(item.id, -1)}>←</button><button className="column-move" type="button" aria-label={`Move ${labels.get(item.id)} right in Screener runs`} disabled={index === preferences.length - 1} onClick={() => onMove(item.id, 1)}>→</button></span></div>)}</div></section></div></details>;
}

function ScreenerSortableHeader({ column, width, sort, dragRef, onSort, onReorder, onResize }: {
  column: { id: ScreenerColumnId; label: string }; width: number; sort: ScreenerSort; dragRef: ScreenerColumnDragRef;
  onSort: (id: ScreenerColumnId) => void; onReorder: (source: ScreenerColumnId, target: ScreenerColumnId, position: ScreenerDropPosition) => void; onResize: (width: number) => void;
}) {
  const active = sort.id === column.id;
  const [dropPosition, setDropPosition] = useState<ScreenerDropPosition | null>(null);
  return <th style={{ width, minWidth: width }} aria-sort={active ? (sort.direction === "asc" ? "ascending" : "descending") : undefined} className={`metrics-column-draggable${dropPosition ? ` column-drop-${dropPosition}` : ""}`} draggable title="Drag left or right to reorder this column" onDragStart={event => { dragRef.current = { id: column.id }; event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", column.id); }} onDragOver={event => { event.preventDefault(); const bounds = event.currentTarget.getBoundingClientRect(); setDropPosition(event.clientX <= bounds.left + bounds.width / 2 ? "before" : "after"); }} onDragLeave={() => setDropPosition(null)} onDrop={event => { event.preventDefault(); const source = dragRef.current?.id ?? event.dataTransfer.getData("text/plain") as ScreenerColumnId; const position = dropPosition ?? "before"; setDropPosition(null); dragRef.current = null; if (source && source !== column.id) onReorder(source, column.id, position); }} onDragEnd={() => { dragRef.current = null; setDropPosition(null); }}><button className="metrics-sort-button" type="button" onClick={() => onSort(column.id)}><span className="metrics-sort-label"><span className="column-drag-grip" aria-hidden="true">⋮⋮</span>{column.label}</span><span className="sort-indicator" aria-hidden="true">{active ? sort.direction === "asc" ? "↑" : "↓" : "↕"}</span></button><span className="metrics-column-resize-handle" aria-hidden="true" title={`Resize ${column.label} column`} onPointerDown={event => { event.preventDefault(); event.stopPropagation(); const startX = event.clientX; const startWidth = width; const move = (moveEvent: PointerEvent) => onResize(Math.max(SCREENER_MIN_WIDTH, Math.min(SCREENER_MAX_WIDTH, Math.round(startWidth + moveEvent.clientX - startX)))); const finish = () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", finish); document.body.classList.remove("metrics-column-resizing"); }; document.body.classList.add("metrics-column-resizing"); window.addEventListener("pointermove", move); window.addEventListener("pointerup", finish, { once: true }); }} /></th>;
}

function ScreenerRunCell({ run, column, durationMs }: { run: ScreenerRun; column: ScreenerColumnId; durationMs?: number | null }) {
  switch (column) {
    case "session":
      return <td><strong>{run.sessionName}</strong><small className="mono">run {run.fleetRunId.slice(0, 8)}</small></td>;
    case "status":
      return <td><StatusBadge value={run.status} />{run.error && <small className="negative" title={run.error}>{run.error}</small>}</td>;
    case "started":
      return <td><Time value={run.startedAt} /></td>;
    case "ended":
      return <td><Time value={run.endedAt} /></td>;
    case "markets":
      return <td>{formatCount(run.scannedMarkets)}{run.configuredLimit != null && <small>cap {formatCount(run.configuredLimit)}</small>}</td>;
    case "api":
      return <td>{formatCount(run.apiRequests)}{run.apiErrors != null && <small>{run.apiErrors} errors</small>}</td>;
    case "duration":
      return <td>{formatDuration(durationMs)}</td>;
    case "changes":
      return <td>{run.added ?? 0} added · {run.changed ?? 0} changed · {run.removed ?? 0} removed</td>;
  }
}

export function ScreenerHistory({ initial, onLoadMore, clockMs }: { initial: Monitoring; onLoadMore: (cursor: string) => Promise<{ items: ScreenerRun[]; nextCursor?: string | null }>; clockMs: number }) {
  const initialRows = initial.screener.history ?? [];
  const [rows, setRows] = useState<ScreenerRun[]>(initialRows);
  const [summary, setSummary] = useState<ScreenerRunSummary>(() => normalizeScreenerSummary(initial.screener.historySummary));
  const [nextCursor, setNextCursor] = useState<string | null>(initial.screener.historyNextCursor ?? null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [preferences, setPreferences] = useState<ScreenerColumnPreference[]>(() => {
    if (typeof window === "undefined") return defaultScreenerPreferences();
    try { return sanitizeScreenerPreferences(JSON.parse(window.localStorage.getItem(SCREENER_COLUMN_KEY) ?? "null")); } catch { return defaultScreenerPreferences(); }
  });
  const [widths, setWidths] = useState<ScreenerColumnWidths>(() => {
    if (typeof window === "undefined") return { ...DEFAULT_SCREENER_WIDTHS };
    try { return sanitizeScreenerWidths(JSON.parse(window.localStorage.getItem(SCREENER_WIDTH_KEY) ?? "null")); } catch { return { ...DEFAULT_SCREENER_WIDTHS }; }
  });
  const [sort, setSort] = useState<ScreenerSort>({ id: "started", direction: "desc" });
  const dragRef = useRef<{ id: ScreenerColumnId } | null>(null);
  useEffect(() => { window.localStorage.setItem(SCREENER_COLUMN_KEY, JSON.stringify({ version: SCREENER_COLUMN_VERSION, columns: preferences })); }, [preferences]);
  useEffect(() => { window.localStorage.setItem(SCREENER_WIDTH_KEY, JSON.stringify({ version: SCREENER_COLUMN_VERSION, widths })); }, [widths]);
  useEffect(() => {
    const incoming = initial.screener.history ?? [];
    if (incoming.length) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setRows(previous => mergeScreenerRuns(previous, incoming));
    }
    if (initial.screener.historySummary) {
      setSummary(normalizeScreenerSummary(initial.screener.historySummary));
    }
    if (initial.screener.historyNextCursor !== undefined) {
      setNextCursor(initial.screener.historyNextCursor ?? null);
    }
  }, [initial.screener.history, initial.screener.historySummary, initial.screener.historyNextCursor]);
  const toggle = (id: ScreenerColumnId) => setPreferences(previous => previous.map(item => item.id === id ? { ...item, enabled: !item.enabled } : item));
  const move = (id: ScreenerColumnId, direction: -1 | 1) => setPreferences(previous => { const index = previous.findIndex(item => item.id === id); const target = index + direction; if (index < 0 || target < 0 || target >= previous.length) return previous; const next = [...previous]; [next[index], next[target]] = [next[target], next[index]]; return next; });
  const reorder = (source: ScreenerColumnId, target: ScreenerColumnId, position: ScreenerDropPosition) => setPreferences(previous => { const sourceIndex = previous.findIndex(item => item.id === source); const targetIndex = previous.findIndex(item => item.id === target); if (sourceIndex < 0 || targetIndex < 0) return previous; const next = [...previous]; const [item] = next.splice(sourceIndex, 1); let insertAt = next.findIndex(entry => entry.id === target); if (position === "after") insertAt += 1; next.splice(insertAt, 0, item); return next; });
  const selectSort = (id: ScreenerColumnId) => setSort(previous => previous.id === id ? { id, direction: previous.direction === "asc" ? "desc" : "asc" } : { id, direction: id === "started" ? "desc" : "asc" });
  const resize = (id: ScreenerColumnId, width: number) => setWidths(previous => ({ ...previous, [id]: width }));
  const columns = preferences.filter(item => item.enabled).map(item => SCREENER_COLUMNS.find(column => column.id === item.id)).filter((column): column is { id: ScreenerColumnId; label: string } => Boolean(column));
  const sortedRows = useMemo(() => [...rows].sort((a, b) => { const value = (item: ScreenerRun): string | number => { if (sort.id === "session") return `${item.sessionName} ${item.fleetRunId}`; if (sort.id === "status") return item.status ?? ""; if (sort.id === "started") return Number(item.startedAt ?? 0); if (sort.id === "ended") return Number(item.endedAt ?? 0); if (sort.id === "markets") return Number(item.scannedMarkets ?? -1); if (sort.id === "api") return Number(item.apiRequests ?? -1); if (sort.id === "duration") return Number(item.durationMs ?? -1); return Number(item.added ?? 0) + Number(item.changed ?? 0) + Number(item.removed ?? 0); }; const left = value(a); const right = value(b); const comparison = typeof left === "string" ? left.localeCompare(String(right)) : Number(left) - Number(right); return (sort.direction === "asc" ? comparison : -comparison) || b.id.localeCompare(a.id); }), [rows, sort]);
  const loadMore = async () => { if (!nextCursor || loadingMore) return; setLoadingMore(true); setLoadError(null); try { const result = await onLoadMore(nextCursor); setRows(previous => mergeScreenerRuns(previous, result.items)); setNextCursor(result.nextCursor ?? null); } catch (error) { setLoadError(error instanceof Error ? error.message : String(error)); } finally { setLoadingMore(false); } };
  const currentDuration = (run: ScreenerRun) => run.durationMs ?? (run.status === "running" && run.startedAt ? Math.max(0, clockMs - run.startedAt) : null);
  const historyWarnings = initial.screener.historyWarnings ?? [];
  return <section className="panel screener-history-panel"><div className="panel-heading"><div><span className="eyebrow">SCREENER HISTORY</span><h2>Historical runs</h2></div><ScreenerColumnChooser preferences={preferences} onToggle={toggle} onMove={move} /></div>{historyWarnings.length > 0 && <div className="warning-panel"><strong>Screener history may be incomplete</strong><ul>{historyWarnings.map(item => <li key={item}>{item}</li>)}</ul></div>}<section className="metrics screener-history-summary" aria-label="Aggregated screener metrics"><article><span>Runs</span><strong>{summary.totalRuns}</strong><p>{summary.succeeded} succeeded · {summary.failed} failed · {summary.interrupted} interrupted · {summary.running} running</p></article><article><span>Markets scanned</span><strong>{formatCount(summary.scannedMarkets)}</strong><p>Across all recorded runs</p></article><article><span>Venue requests</span><strong>{formatCount(summary.apiRequests)}</strong><p>Screener REST requests</p></article><article><span>Average duration</span><strong>{formatDuration(summary.averageDurationMs)}</strong><p>Completed runs only</p></article><article><span>Latest changes</span><strong>{formatCount(summary.added + summary.changed + summary.removed)}</strong><p>{summary.added} added · {summary.changed} changed · {summary.removed} removed</p></article></section><div className="table-wrap screener-history-table"><table style={{ minWidth: columns.reduce((total, column) => total + widths[column.id], 0) }}><colgroup>{columns.map(column => <col key={column.id} style={{ width: widths[column.id] }} />)}</colgroup><thead><tr>{columns.map(column => <ScreenerSortableHeader key={column.id} column={column} width={widths[column.id]} sort={sort} dragRef={dragRef} onSort={selectSort} onReorder={reorder} onResize={width => resize(column.id, width)} />)}</tr></thead><tbody>{sortedRows.map(run => <tr key={run.id}>{columns.map(column => <ScreenerRunCell key={column.id} run={run} column={column.id} durationMs={currentDuration(run)} />)}</tr>)}</tbody></table>{sortedRows.length === 0 && <p className="empty">No screener runs recorded yet.</p>}</div>{loadError && <p className="negative" role="alert">Could not load older screener runs: {loadError}</p>}{nextCursor && <button className="button screener-history-more" type="button" onClick={() => void loadMore()} disabled={loadingMore}>{loadingMore ? "Loading…" : "Load older runs"}</button>}</section>;
}

function useLiveMonitoring(initial: Monitoring) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const [clockMs, setClockMs] = useState(initial.generatedAt);
  useEffect(() => {
    const events = new EventSource("/api/backend/api/v1/events?topics=monitoring");
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
  return { data, connected, clockMs };
}

export function LiveMonitoring({ initial }: { initial: Monitoring }) {
  const { data, connected, clockMs } = useLiveMonitoring(initial);
  const manager = data.manager ?? {};
  const pnl = manager.pnl ?? { fills: 0, feesCents: 0, realizedCents: 0, unrealizedCents: 0, totalCents: 0 };
  const portfolio = manager.portfolio ?? {};
  return <>
    <header className="page-header"><div><span className="eyebrow">LIVE TELEMETRY</span><h1>Monitoring</h1><p>Manager, client, venue transport, and portfolio activity.</p></div><div className="header-status"><StatusBadge value={manager.lifecycle} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    {(data.source.stale || data.warnings.length > 0) && <section className="warning-panel"><strong>Monitoring data may be stale</strong><ul>{data.warnings.map(item => <li key={item}>{item}</li>)}</ul></section>}
    <section className="metrics"><article><span>Bots running</span><strong>{manager.botsRunning ?? 0}<small> / {data.clients.length}</small></strong><p>Manager uptime {formatDuration(manager.startedAtMs ? clockMs - manager.startedAtMs : manager.runningForMs)}</p></article><article><span>Session P&amp;L</span><strong><Money cents={pnl.totalCents} signed /></strong><p><Money cents={pnl.realizedCents} signed /> realized · {pnl.fills ?? 0} fills</p></article><article><span>Portfolio</span><strong>{units(portfolio.grossPositionUnits)}</strong><p>{units(portfolio.netPositionUnits)} net · {portfolio.staleMarkets ?? 0} stale</p></article><article><span>Venue requests</span><strong>{apiTotal(manager.apiActivity)}</strong><p>{manager.apiActivity?.rest?.requestsLast60s ?? 0} REST / min · {manager.apiActivity?.rest?.errors ?? 0} errors</p></article></section>

    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">CLIENTS</span><h2>Bot activity</h2></div><strong>{data.clients.length} processes</strong></div><div className="table-wrap monitoring-table"><table><thead><tr><th>Market</th><th>Runtime / watchdog</th><th>Price</th><th>Position / P&amp;L</th><th>Fills / orders</th><th>API activity</th></tr></thead><tbody>{data.clients.map(client => <tr key={client.marketId}><td><strong>{client.title || client.market?.title || client.marketId}</strong><small className="mono">{client.marketId} · PID {client.pid ?? "—"}</small><details><summary>Activity details</summary><div className="monitor-detail"><span>Active orders: {Object.values(client.orderActivity?.active ?? {}).filter(item => item.orderId).length}</span><span>Last fill: <Time value={client.fills?.lastFillAtMs} /></span><span>Last order: <Time value={client.orderActivity?.lastActivityAtMs} /></span><span>Socket: {client.socketHealthy ? "healthy" : "unavailable"}</span>{client.orderActivity?.recent?.slice(-3).reverse().map((item, index) => <span key={`order-${index}`}>Order: {String(item.action ?? "unknown")} · {String(item.outcome ?? "unknown")}</span>)}{client.fills?.recent?.slice(-3).reverse().map((item, index) => <span key={`fill-${index}`}>Fill: {String(item.side ?? "unknown")} · {units(Number(item.quantityUnits ?? 0))} contracts</span>)}</div></details></td><td><StatusBadge value={client.lifecycle} /><small>{formatDuration(client.runtime?.startedAtMs ? clockMs - client.runtime.startedAtMs : client.runtime?.runningForMs)}</small><StatusBadge value={client.watchdog?.running ? client.watchdog.mode : "stopped"} /></td><td>{client.market?.priceUnits == null ? "—" : `${(client.market.priceUnits / 100).toFixed(2)}¢`}<small>{client.market?.priceSource ?? "unavailable"}</small></td><td>{units(client.portfolio?.currentPositionUnits)}<small><Money cents={client.pnl?.totalCents} signed /></small></td><td>{client.fills?.count ?? 0} fills<small>{orderAttempts(client)} order attempts</small></td><td>{apiTotal(client.apiActivity)} REST<small>{client.apiActivity?.rest?.errors ?? 0} errors · {client.apiActivity?.stream?.message ?? 0} messages</small></td></tr>)}</tbody></table>{data.clients.length === 0 && <p className="empty">No bot clients are currently reporting.</p>}</div></section>

  </>;
}

export function LiveScreener({ initial }: { initial: Monitoring }) {
  const { data, connected, clockMs } = useLiveMonitoring(initial);
  const screener = data.screener ?? {};
  const currentScreenerDuration = screener.running && screener.currentStartedAtMs ? clockMs - screener.currentStartedAtMs : screener.currentDurationMs;
  const loadMoreScreenerRuns = useCallback(async (cursor: string) => {
    const response = await fetch(`/api/backend/api/v1/screener/runs?limit=100&cursor=${encodeURIComponent(cursor)}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Request failed (${response.status})`);
    return await response.json() as { items: ScreenerRun[]; nextCursor?: string | null };
  }, []);
  return <>
    <header className="page-header"><div><span className="eyebrow">SCREENER ACTIVITY</span><h1>Screener</h1><p>Current refresh activity, selected markets, and historical runs.</p></div><div className="header-status"><StatusBadge value={screener.running ? "running" : screener.lastError ? "failed" : "idle"} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></header>
    {(data.source.stale || data.warnings.length > 0) && <section className="warning-panel"><strong>Monitoring data may be stale</strong><ul>{data.warnings.map(item => <li key={item}>{item}</li>)}</ul></section>}
    <section className="grid-two"><article className="panel"><div className="panel-heading"><div><span className="eyebrow">SCREENER</span><h2>Refresh activity</h2></div><StatusBadge value={screener.running ? "running" : screener.lastError ? "failed" : "idle"} /></div><dl className="details"><div><dt>Current duration</dt><dd>{screener.running ? formatDuration(currentScreenerDuration) : "Not running"}</dd></div><div><dt>Last completed</dt><dd><Time value={screener.lastCompletedAtMs} /></dd></div><div><dt>Last duration</dt><dd>{formatDuration(screener.lastDurationMs)}</dd></div><div><dt>Generation</dt><dd>{screener.generationId ?? "—"}</dd></div><div><dt>Latest changes</dt><dd>{screener.changes?.added?.length ?? 0} added · {screener.changes?.changed?.length ?? 0} changed · {screener.changes?.removed?.length ?? 0} removed</dd></div><div><dt>Venue requests</dt><dd>{apiTotal(screener.apiActivity)} total</dd></div>{screener.lastError && <div><dt>Last error</dt><dd className="negative">{screener.lastError}</dd></div>}</dl></article><article className="panel"><div className="panel-heading"><div><span className="eyebrow">CURRENT PICKS</span><h2>{screener.picks?.length ?? 0} selected markets</h2></div></div><div className="pick-list">{screener.picks?.map(pick => <div key={pick.marketId}><span><strong>{pick.title || pick.marketId}</strong><small className="mono">{pick.marketId} · {pick.selectionReason}</small></span><span>#{pick.rank ?? "—"}<small><Money cents={pick.yesBudgetCents} /> / <Money cents={pick.noBudgetCents} /></small></span></div>)}{!screener.picks?.length && <p className="empty">No successful screener generation is available.</p>}</div></article></section>
    <ScreenerHistory initial={data} clockMs={clockMs} onLoadMore={loadMoreScreenerRuns} />
  </>;
}
