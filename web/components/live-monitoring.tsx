"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type MutableRefObject, type ReactNode } from "react";
import type { ApiActivity, ClientMonitoring, Monitoring, ScreenerRun, ScreenerRunSummary, SystemCapacity, VenueCapacity } from "@/lib/types";
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

type MonitoringTableKind = "api" | "shards" | "markets";
type MonitoringSort = { id: string; direction: "asc" | "desc" };
type MonitoringPreference = { id: string; enabled: boolean };
type MonitoringPreferences = Record<MonitoringTableKind, MonitoringPreference[]>;
type MonitoringWidths = Record<MonitoringTableKind, Record<string, number>>;
type MonitoringColumn = { id: string; label: string; width: number; locked?: boolean };
type MonitoringDropPosition = "before" | "after";

const MONITORING_COLUMNS: Record<MonitoringTableKind, MonitoringColumn[]> = {
  api: [
    { id: "venue", label: "Venue", width: 150, locked: true },
    { id: "restTotal", label: "REST total", width: 120 }, { id: "restRate", label: "REST / min", width: 120 },
    { id: "restSuccesses", label: "REST successes", width: 135 }, { id: "restErrors", label: "REST errors", width: 120 },
    { id: "restDisconnects", label: "REST disconnects", width: 145 },
    { id: "restLatency", label: "Avg latency", width: 125 }, { id: "restLast", label: "Last REST", width: 180 },
    { id: "wsConnections", label: "WS connections", width: 145 }, { id: "wsReconnects", label: "WS reconnects", width: 140 },
    { id: "wsDisconnects", label: "WS disconnects", width: 140 }, { id: "wsErrors", label: "WS errors", width: 115 }, { id: "wsSuccesses", label: "WS successes", width: 130 },
    { id: "wsFailures", label: "WS failures", width: 120 }, { id: "wsLatency", label: "WS avg latency", width: 140 }, { id: "wsMessages", label: "WS messages", width: 130 },
    { id: "wsEvents", label: "WS events", width: 115 }, { id: "wsRate", label: "WS / min", width: 115 },
    { id: "wsLast", label: "Last WS", width: 180 },
  ],
  shards: [
    { id: "identity", label: "Venue / shard", width: 210, locked: true }, { id: "started", label: "Started at", width: 180 },
    { id: "runtime", label: "Running for", width: 125 }, { id: "markets", label: "Assigned markets", width: 230 },
    { id: "state", label: "Running state", width: 190 }, { id: "watchdog", label: "Watchdog", width: 230 },
    { id: "bots", label: "Active actors", width: 125 }, { id: "heartbeat", label: "Last heartbeat", width: 180 },
    { id: "heartbeatAge", label: "Heartbeat age", width: 125 }, { id: "resources", label: "CPU / memory", width: 150 },
    { id: "queue", label: "Queue / wait", width: 145 }, { id: "eventLag", label: "Event-loop lag", width: 135 },
    { id: "transport", label: "REST / WS", width: 190 }, { id: "errors", label: "Errors / warnings", width: 170 },
    { id: "recovery", label: "Recovery", width: 180 },
  ],
  markets: [
    { id: "identity", label: "Market", width: 330, locked: true }, { id: "venueShard", label: "Venue / shard", width: 180 },
    { id: "runtime", label: "Runtime", width: 140 }, { id: "watchdog", label: "Watchdog", width: 190 },
    { id: "price", label: "Price", width: 120 }, { id: "positionPnl", label: "Position / P&L", width: 160 },
    { id: "fillsOrders", label: "Fills / orders", width: 150 }, { id: "lastQuote", label: "Last quote", width: 180 },
    { id: "lastCreate", label: "Last order create", width: 180 }, { id: "lastFill", label: "Last fill", width: 180 },
  ],
};
const MONITORING_COLUMN_KEY = "kalshi.monitoring.columns.v1";
const MONITORING_WIDTH_KEY = "kalshi.monitoring.widths.v1";
const MONITORING_TABLE_TITLES: Record<MonitoringTableKind, string> = { api: "API", shards: "Shards", markets: "Markets" };

function defaultMonitoringPreferences(): MonitoringPreferences {
  return Object.fromEntries((Object.keys(MONITORING_COLUMNS) as MonitoringTableKind[]).map(kind => [kind, MONITORING_COLUMNS[kind].map(column => ({ id: column.id, enabled: true }))])) as MonitoringPreferences;
}
function defaultMonitoringWidths(): MonitoringWidths {
  return Object.fromEntries((Object.keys(MONITORING_COLUMNS) as MonitoringTableKind[]).map(kind => [kind, Object.fromEntries(MONITORING_COLUMNS[kind].map(column => [column.id, column.width]))])) as MonitoringWidths;
}
function sanitizeMonitoringPreferences(value: unknown): MonitoringPreferences {
  const result = defaultMonitoringPreferences();
  if (!value || typeof value !== "object") return result;
  const tables = (value as { version?: unknown; tables?: unknown }).version === 1 ? (value as { tables?: unknown }).tables : null;
  if (!tables || typeof tables !== "object") return result;
  for (const kind of Object.keys(MONITORING_COLUMNS) as MonitoringTableKind[]) {
    const known = new Map(MONITORING_COLUMNS[kind].map(column => [column.id, column]));
    const saved = Array.isArray((tables as Record<string, unknown>)[kind]) ? (tables as Record<string, unknown>)[kind] as unknown[] : [];
    const seen = new Set<string>();
    const preferences: MonitoringPreference[] = [];
    for (const entry of saved) {
      const item = entry as { id?: unknown; enabled?: unknown };
      if (!item || typeof item.id !== "string" || !known.has(item.id) || seen.has(item.id)) continue;
      const column = known.get(item.id)!;
      preferences.push({ id: item.id, enabled: column.locked ? true : item.enabled !== false });
      seen.add(item.id);
    }
    for (const column of MONITORING_COLUMNS[kind]) if (!seen.has(column.id)) preferences.push({ id: column.id, enabled: true });
    result[kind] = preferences;
  }
  return result;
}
function sanitizeMonitoringWidths(value: unknown): MonitoringWidths {
  const result = defaultMonitoringWidths();
  if (!value || typeof value !== "object") return result;
  const tables = (value as { version?: unknown; tables?: unknown }).version === 1 ? (value as { tables?: unknown }).tables : null;
  if (!tables || typeof tables !== "object") return result;
  for (const kind of Object.keys(MONITORING_COLUMNS) as MonitoringTableKind[]) {
    const saved = (tables as Record<string, unknown>)[kind];
    if (!saved || typeof saved !== "object" || Array.isArray(saved)) continue;
    for (const [id, width] of Object.entries(saved)) if (id in result[kind] && typeof width === "number" && Number.isFinite(width)) result[kind][id] = Math.max(72, Math.min(720, Math.round(width)));
  }
  return result;
}
function monitoringCompare(left: unknown, right: unknown, direction: "asc" | "desc") {
  const missingLeft = left == null; const missingRight = right == null;
  if (missingLeft || missingRight) return missingLeft === missingRight ? 0 : missingLeft ? 1 : -1;
  const value = typeof left === "number" && typeof right === "number" ? left - right : String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
  return direction === "asc" ? value : -value;
}
function bytes(value?: number | null) {
  if (value == null) return "Unavailable";
  const labels = ["B", "KB", "MB", "GB"]; let size = value; let index = 0;
  while (size >= 1024 && index < labels.length - 1) { size /= 1024; index += 1; }
  return `${size.toFixed(index ? 1 : 0)} ${labels[index]}`;
}

type CapacityRecord = Record<string, unknown> | null | undefined;
function capacityNumber(value: CapacityRecord, ...keys: string[]) {
  for (const key of keys) {
    const candidate = value?.[key];
    if (typeof candidate === "number" && Number.isFinite(candidate)) return candidate;
  }
  return undefined;
}
function capacityBoolean(value: CapacityRecord, ...keys: string[]) {
  for (const key of keys) {
    const candidate = value?.[key];
    if (typeof candidate === "boolean") return candidate;
  }
  return undefined;
}
function capacityString(value: CapacityRecord, ...keys: string[]) {
  for (const key of keys) {
    const candidate = value?.[key];
    if (typeof candidate === "string" && candidate.trim()) return candidate;
  }
  return undefined;
}
function milliseconds(value?: number | null) {
  if (value == null) return "Unavailable";
  return value < 1000 ? `${Math.round(value)} ms` : `${(value / 1000).toFixed(1)} s`;
}
function percentage(value?: number | null) {
  return value == null ? "Unavailable" : `${value.toFixed(1)}%`;
}
function capacityReasonLabel(reason?: string | null) {
  const normalized = reason?.trim().toLowerCase();
  if (!normalized) return "healthy";
  const labels: Record<string, string> = {
    cpu: "CPU limited",
    memory: "memory limited",
    resource_cpu: "CPU limited",
    resource_memory: "memory limited",
    worker_starvation: "worker-starvation limited",
    event_loop_lag: "event-loop-lag limited",
    queue_wait: "queue-wait limited",
    worker_recovery: "worker-recovery limited",
    global_capacity: "global capacity limited",
    capacity: "Venue/API limited",
  };
  return labels[normalized] ?? normalized.replaceAll("_", " ");
}
function capacityState(capacity: SystemCapacity | null | undefined, clockMs: number) {
  if (!capacity) return "unavailable";
  const reason = capacityReasonLabel(capacityString(capacity, "systemCapacityReason", "reason"));
  if (reason !== "healthy") return reason;
  const effective = capacityNumber(capacity, "effectiveMaxBots");
  const configured = capacityNumber(capacity, "configuredMaxBots");
  const healthySince = capacityNumber(capacity, "healthySinceMs");
  if (effective != null && configured != null && effective < configured && healthySince != null && clockMs - healthySince < 60_000) return "recovery cooldown";
  return "healthy";
}

function MonitoringColumnChooser({ preferences, onToggle, onMove }: { preferences: MonitoringPreferences; onToggle: (kind: MonitoringTableKind, id: string) => void; onMove: (kind: MonitoringTableKind, id: string, direction: -1 | 1) => void }) {
  return <details className="metrics-columns"><summary className="button">Columns</summary><div className="metrics-columns-menu"><p className="metrics-columns-hint">Drag a table header left or right to reorder it. Arrow buttons provide the same control here.</p>{(Object.keys(MONITORING_COLUMNS) as MonitoringTableKind[]).map(kind => <section key={kind}><strong>{MONITORING_TABLE_TITLES[kind]}</strong><div className="metrics-column-list">{preferences[kind].map((item, index) => { const column = MONITORING_COLUMNS[kind].find(value => value.id === item.id)!; return <div className="metrics-column-option" key={item.id}><label><input type="checkbox" aria-label={`${MONITORING_TABLE_TITLES[kind]}: ${column.label}`} checked={item.enabled} disabled={column.locked} onChange={() => onToggle(kind, item.id)} /><span>{column.label}{column.locked ? " (locked)" : ""}</span></label><span className="metrics-column-movers"><button className="column-move" type="button" aria-label={`Move ${column.label} left in ${MONITORING_TABLE_TITLES[kind]}`} disabled={column.locked || index <= 1} onClick={() => onMove(kind, item.id, -1)}>←</button><button className="column-move" type="button" aria-label={`Move ${column.label} right in ${MONITORING_TABLE_TITLES[kind]}`} disabled={column.locked || index === preferences[kind].length - 1} onClick={() => onMove(kind, item.id, 1)}>→</button></span></div>; })}</div></section>)}</div></details>;
}

function MonitoringHeader({ column, width, activeSort, dragRef, onSort, onReorder, onResize }: { column: MonitoringColumn; width: number; activeSort: MonitoringSort; dragRef: MutableRefObject<string | null>; onSort: (id: string) => void; onReorder: (source: string, target: string, position: MonitoringDropPosition) => void; onResize: (width: number) => void }) {
  const [drop, setDrop] = useState<MonitoringDropPosition | null>(null); const active = activeSort.id === column.id;
  return <th style={{ width, minWidth: width }} aria-sort={active ? (activeSort.direction === "asc" ? "ascending" : "descending") : undefined} className={`metrics-column-draggable${drop ? ` column-drop-${drop}` : ""}`} draggable onDragStart={event => { dragRef.current = column.id; event.dataTransfer.setData("text/plain", column.id); }} onDragOver={event => { event.preventDefault(); const bounds = event.currentTarget.getBoundingClientRect(); setDrop(event.clientX <= bounds.left + bounds.width / 2 ? "before" : "after"); }} onDragLeave={() => setDrop(null)} onDrop={event => { event.preventDefault(); const source = dragRef.current ?? event.dataTransfer.getData("text/plain"); if (source && source !== column.id) onReorder(source, column.id, drop ?? "before"); dragRef.current = null; setDrop(null); }} onDragEnd={() => { dragRef.current = null; setDrop(null); }}><button type="button" className="metrics-sort-button" onClick={() => onSort(column.id)}><span className="metrics-sort-label"><span className="column-drag-grip" aria-hidden>⋮⋮</span>{column.label}</span><span className="sort-indicator" aria-hidden>{active ? activeSort.direction === "asc" ? "↑" : "↓" : "↕"}</span></button><span className="metrics-column-resize-handle" aria-hidden title={`Resize ${column.label} column`} onPointerDown={event => { event.preventDefault(); event.stopPropagation(); const startX = event.clientX; const startWidth = width; const move = (next: PointerEvent) => onResize(Math.max(72, Math.min(720, Math.round(startWidth + next.clientX - startX)))); const finish = () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", finish); document.body.classList.remove("metrics-column-resizing"); }; document.body.classList.add("metrics-column-resizing"); window.addEventListener("pointermove", move); window.addEventListener("pointerup", finish, { once: true }); }} /></th>;
}

function MonitoringTable<T>({ kind, rows, preferences, widths, sort, rowKey, sortValue, renderCell, empty, onSort, onReorder, onResize }: { kind: MonitoringTableKind; rows: T[]; preferences: MonitoringPreference[]; widths: Record<string, number>; sort: MonitoringSort; rowKey: (row: T) => string; sortValue: (row: T, id: string) => unknown; renderCell: (row: T, id: string) => ReactNode; empty: string; onSort: (id: string) => void; onReorder: (source: string, target: string, position: MonitoringDropPosition) => void; onResize: (id: string, width: number) => void }) {
  const dragRef = useRef<string | null>(null);
  const columns = preferences.filter(item => item.enabled).map(item => MONITORING_COLUMNS[kind].find(column => column.id === item.id)).filter((column): column is MonitoringColumn => Boolean(column));
  const sorted = useMemo(() => [...rows].sort((a, b) => monitoringCompare(sortValue(a, sort.id), sortValue(b, sort.id), sort.direction) || rowKey(a).localeCompare(rowKey(b))), [rows, sort, sortValue, rowKey]);
  if (!columns.length) return <p className="column-empty">No columns enabled for {MONITORING_TABLE_TITLES[kind]}.</p>;
  return <div className="table-wrap monitoring-table"><table style={{ minWidth: columns.reduce((total, column) => total + widths[column.id], 0) }}><colgroup>{columns.map(column => <col key={column.id} style={{ width: widths[column.id] }} />)}</colgroup><thead><tr>{columns.map(column => <MonitoringHeader key={column.id} column={column} width={widths[column.id]} activeSort={sort} dragRef={dragRef} onSort={onSort} onReorder={onReorder} onResize={width => onResize(column.id, width)} />)}</tr></thead><tbody>{sorted.map(row => <tr key={rowKey(row)}>{columns.map(column => <td key={column.id}>{renderCell(row, column.id)}</td>)}</tr>)}</tbody></table>{rows.length === 0 && <p className="empty">{empty}</p>}</div>;
}

type ScreenerColumnId = "venue" | "session" | "status" | "started" | "ended" | "markets" | "api" | "duration" | "changes";
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
  { id: "venue", label: "Venue" }, { id: "session", label: "Session run in" }, { id: "status", label: "Status of screener run" },
  { id: "started", label: "Time Started" }, { id: "ended", label: "Time Ended" },
  { id: "markets", label: "Markets to Screen" }, { id: "api", label: "API Requests to Venue" },
  { id: "duration", label: "Duration" }, { id: "changes", label: "Latest Changes" },
];
const DEFAULT_SCREENER_WIDTHS: ScreenerColumnWidths = {
  venue: 130, session: 240, status: 180, started: 170, ended: 170,
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
    case "venue":
      return <td><strong>{run.venue ?? "unknown"}</strong></td>;
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
  const sortedRows = useMemo(() => [...rows].sort((a, b) => { const value = (item: ScreenerRun): string | number => { if (sort.id === "venue") return item.venue ?? "unknown"; if (sort.id === "session") return `${item.sessionName} ${item.fleetRunId}`; if (sort.id === "status") return item.status ?? ""; if (sort.id === "started") return Number(item.startedAt ?? 0); if (sort.id === "ended") return Number(item.endedAt ?? 0); if (sort.id === "markets") return Number(item.scannedMarkets ?? -1); if (sort.id === "api") return Number(item.apiRequests ?? -1); if (sort.id === "duration") return Number(item.durationMs ?? -1); return Number(item.added ?? 0) + Number(item.changed ?? 0) + Number(item.removed ?? 0); }; const left = value(a); const right = value(b); const comparison = typeof left === "string" ? left.localeCompare(String(right)) : Number(left) - Number(right); return (sort.direction === "asc" ? comparison : -comparison) || b.id.localeCompare(a.id); }), [rows, sort]);
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
  const [preferences, setPreferences] = useState<MonitoringPreferences>(() => {
    if (typeof window === "undefined") return defaultMonitoringPreferences();
    try { return sanitizeMonitoringPreferences(JSON.parse(window.localStorage.getItem(MONITORING_COLUMN_KEY) ?? "null")); } catch { return defaultMonitoringPreferences(); }
  });
  const [widths, setWidths] = useState<MonitoringWidths>(() => {
    if (typeof window === "undefined") return defaultMonitoringWidths();
    try { return sanitizeMonitoringWidths(JSON.parse(window.localStorage.getItem(MONITORING_WIDTH_KEY) ?? "null")); } catch { return defaultMonitoringWidths(); }
  });
  const [sorts, setSorts] = useState<Record<MonitoringTableKind, MonitoringSort>>({ api: { id: "venue", direction: "asc" }, shards: { id: "identity", direction: "asc" }, markets: { id: "identity", direction: "asc" } });
  useEffect(() => { try { window.localStorage.setItem(MONITORING_COLUMN_KEY, JSON.stringify({ version: 1, tables: preferences })); } catch { /* Preferences remain active for this page view. */ } }, [preferences]);
  useEffect(() => { try { window.localStorage.setItem(MONITORING_WIDTH_KEY, JSON.stringify({ version: 1, tables: widths })); } catch { /* Widths remain active for this page view. */ } }, [widths]);
  const toggle = (kind: MonitoringTableKind, id: string) => setPreferences(previous => ({ ...previous, [kind]: previous[kind].map(item => item.id === id ? { ...item, enabled: !item.enabled } : item) }));
  const move = (kind: MonitoringTableKind, id: string, direction: -1 | 1) => setPreferences(previous => { const list = [...previous[kind]]; const index = list.findIndex(item => item.id === id); const target = index + direction; if (index < 1 || target < 1 || target >= list.length) return previous; [list[index], list[target]] = [list[target], list[index]]; return { ...previous, [kind]: list }; });
  const reorder = (kind: MonitoringTableKind, source: string, target: string, position: MonitoringDropPosition) => setPreferences(previous => { const list = [...previous[kind]]; const sourceIndex = list.findIndex(item => item.id === source); const targetIndex = list.findIndex(item => item.id === target); if (sourceIndex < 1 || targetIndex < 0 || targetIndex === 0) return previous; const [item] = list.splice(sourceIndex, 1); let insertAt = list.findIndex(value => value.id === target); if (position === "after") insertAt += 1; list.splice(Math.max(1, insertAt), 0, item); return { ...previous, [kind]: list }; });
  const resize = (kind: MonitoringTableKind, id: string, width: number) => setWidths(previous => ({ ...previous, [kind]: { ...previous[kind], [id]: width } }));
  const selectSort = (kind: MonitoringTableKind, id: string) => setSorts(previous => ({ ...previous, [kind]: previous[kind].id === id ? { id, direction: previous[kind].direction === "asc" ? "desc" : "asc" } : { id, direction: "asc" } }));
  const activeVenues = manager.activeVenues;
  const pnl = manager.pnl;
  const shardHealth = manager.shardHealth;
  const venues = data.venues ?? [];
  const workers = data.workers ?? [];
  const systemCapacity = data.systemCapacity ?? null;
  const allocation = data.allocation as CapacityRecord;
  const effectiveSystemCapacity = capacityNumber(systemCapacity, "effectiveMaxBots");
  const configuredSystemCapacity = capacityNumber(systemCapacity, "configuredMaxBots");
  const admittedMarkets = capacityNumber(allocation, "admittedMarkets") ?? manager.botsRunning;
  const slotsRemaining = capacityNumber(allocation, "slotsRemaining") ?? (effectiveSystemCapacity != null && admittedMarkets != null ? Math.max(0, effectiveSystemCapacity - admittedMarkets) : undefined);
  const systemCapacityState = capacityState(systemCapacity, clockMs);
  const rawVenueCapacity = data.venueCapacity ?? (data.capacity as Record<string, VenueCapacity> | null | undefined);
  const capacityEntries = Object.entries(rawVenueCapacity ?? {}).filter(([, value]) => value && typeof value === "object") as Array<[string, VenueCapacity]>;
  const venueCapacityRows = capacityEntries.length
    ? capacityEntries
    : venues.filter(venue => venue.capacity).map((venue): [string, VenueCapacity] => [venue.venue, venue.capacity as VenueCapacity]);
  const apiCell = useCallback((row: NonNullable<Monitoring["venues"]>[number], id: string): ReactNode => {
    const rest = row.apiActivity?.rest; const stream = row.apiActivity?.stream;
    switch (id) {
      case "venue": return <><strong>{row.venue}</strong><small><StatusBadge value={row.active ? "active" : "stopped"} /></small></>;
      case "restTotal": return rest?.total ?? "Unavailable"; case "restRate": return rest?.requestsLast60s ?? "Unavailable";
      case "restSuccesses": return rest?.successes ?? "Unavailable"; case "restErrors": return rest?.errors ?? "Unavailable";
      case "restDisconnects": return rest?.disconnects ?? 0;
      case "restLatency": return rest?.averageLatencyMs == null ? "Unavailable" : `${rest.averageLatencyMs.toFixed(1)} ms`;
      case "restLast": return <Time value={rest?.lastActivityAtMs} />; case "wsConnections": return stream?.connections ?? "Unavailable";
      case "wsReconnects": return stream?.reconnects ?? "Unavailable";
      case "wsDisconnects": return stream?.disconnects ?? stream?.closes ?? 0;
      case "wsErrors": return stream ? (stream.connectionErrors ?? 0) + (stream.streamErrors ?? 0) + (stream.adapterErrors ?? 0) : "Unavailable";
      case "wsSuccesses": return stream?.messageSuccesses ?? "Unavailable";
      case "wsFailures": return stream?.messageFailures ?? "Unavailable";
      case "wsLatency": return stream?.averageMessageLatencyMs == null ? "Unavailable" : `${stream.averageMessageLatencyMs.toFixed(1)} ms`;
      case "wsMessages": return stream?.message ?? "Unavailable"; case "wsEvents": return stream?.event ?? "Unavailable";
      case "wsRate": return stream?.messagesLast60s ?? "Unavailable"; case "wsLast": return <Time value={stream?.lastActivityAtMs} />;
      default: return null;
    }
  }, []);
  const apiSort = useCallback((row: NonNullable<Monitoring["venues"]>[number], id: string) => { const rest = row.apiActivity?.rest; const stream = row.apiActivity?.stream; const values: Record<string, unknown> = { venue: row.venue, restTotal: rest?.total, restRate: rest?.requestsLast60s, restSuccesses: rest?.successes, restErrors: rest?.errors, restDisconnects: rest?.disconnects, restLatency: rest?.averageLatencyMs, restLast: rest?.lastActivityAtMs, wsConnections: stream?.connections, wsReconnects: stream?.reconnects, wsDisconnects: stream?.disconnects ?? stream?.closes, wsErrors: stream ? (stream.connectionErrors ?? 0) + (stream.streamErrors ?? 0) + (stream.adapterErrors ?? 0) : null, wsSuccesses: stream?.messageSuccesses, wsFailures: stream?.messageFailures, wsLatency: stream?.averageMessageLatencyMs, wsMessages: stream?.message, wsEvents: stream?.event, wsRate: stream?.messagesLast60s, wsLast: stream?.lastActivityAtMs }; return values[id]; }, []);
  const shardCell = useCallback((row: NonNullable<Monitoring["workers"]>[number], id: string): ReactNode => {
    const rest = row.apiActivity?.rest;
    const stream = row.apiActivity?.stream;
    switch (id) {
      case "identity": return <><strong>{row.venue ?? "unknown"} / {row.workerId}</strong><small className="mono">PID {row.pid ?? "—"}</small></>;
      case "started": return <Time value={row.startedAtMs} />;
      case "runtime": return formatDuration(row.startedAtMs ? clockMs - row.startedAtMs : row.runningForMs);
      case "markets": return <details><summary>{row.assignedMarkets ?? "Unavailable"} assigned</summary><div className="monitor-detail mono">{row.marketIds?.map(market => <span key={market}>{market}</span>)}</div></details>;
      case "state": return <><StatusBadge value={row.running == null ? "Unavailable" : !row.running ? "stopped" : row.stale ? "stale" : row.starved ? "starved" : row.degraded ? "degraded" : "running"} /><small>{row.phase ?? "Unavailable"}{row.heartbeatSource ? ` · ${row.heartbeatSource}` : ""}</small></>;
      case "watchdog": return <><StatusBadge value={row.watchdog?.mode} /><small>{Object.entries(row.watchdog?.counts ?? {}).map(([mode, count]) => `${mode}: ${count}`).join(" · ") || "No market states"}</small></>;
      case "bots": return `${row.botsRunning ?? "Unavailable"} / ${row.assignedMarkets}`; case "heartbeat": return <Time value={row.heartbeatReceivedAtMs ?? row.heartbeatAtMs} />;
      case "heartbeatAge": return formatDuration(row.heartbeatAgeMs);
      case "resources": return <><strong>{row.cpuPercent == null ? "Unavailable" : `${row.cpuPercent.toFixed(1)}%`}</strong><small>{bytes(row.memoryRssBytes)} · {row.threadCount ?? "—"} threads</small></>;
      case "queue": return <><strong>{row.commandQueueDepth ?? row.queueDepth ?? "Unavailable"}</strong><small>{row.commandWaitMs == null ? "—" : `${row.commandWaitMs} ms wait`}</small></>;
      case "eventLag": return <><strong>{formatDuration(row.eventLoopLagMs)}</strong><small>market {formatDuration(row.eventLagMs)}</small></>;
      case "transport": return <details><summary>{rest?.total ?? "—"} REST · {stream?.messages ?? stream?.message ?? "—"} WS</summary><div className="monitor-detail"><span>REST: {rest?.requestsLast60s ?? "—"}/min · {rest?.errors ?? "—"} errors · {rest?.disconnects ?? 0} disconnects · {rest?.averageLatencyMs?.toFixed(1) ?? "—"} ms</span><span>WS: {stream?.messagesLast60s ?? "—"}/min · {stream?.messageFailures ?? 0} failures · {stream?.disconnects ?? stream?.closes ?? 0} disconnects · {stream?.averageMessageLatencyMs?.toFixed(1) ?? "—"} ms</span></div></details>;
      case "errors": return <details><summary>{row.errorCount ?? 0} errors · {row.warningCount ?? 0} warnings</summary><div className="monitor-detail"><span>{row.lastError || row.lastWarning || "No recent worker errors"}</span>{row.apiErrors?.last?.message && <span>API: {row.apiErrors.last.message}</span>}</div></details>;
      case "recovery": return <><strong>{row.recoveryCount ?? 0} recoveries</strong><small>{row.recoveryReason || (row.lastRecoveryAtMs ? <Time value={row.lastRecoveryAtMs} /> : "No recovery recorded")}</small></>;
      default: return null;
    }
  }, [clockMs]);
  const shardSort = useCallback((row: NonNullable<Monitoring["workers"]>[number], id: string) => ({ identity: `${row.venue}:${row.workerId}`, started: row.startedAtMs, runtime: row.startedAtMs, markets: row.assignedMarkets, state: row.running ? row.stale ? 1 : row.starved ? 2 : row.degraded ? 3 : 4 : 0, watchdog: row.watchdog?.mode, bots: row.botsRunning, heartbeat: row.heartbeatReceivedAtMs ?? row.heartbeatAtMs, heartbeatAge: row.heartbeatAgeMs, resources: row.cpuPercent, queue: row.commandWaitMs ?? row.commandQueueDepth ?? row.queueDepth, eventLag: row.eventLoopLagMs, transport: row.apiActivity?.rest?.errors, errors: row.errorCount, recovery: row.recoveryCount } as Record<string, unknown>)[id], []);
  const marketCell = useCallback((client: ClientMonitoring, id: string): ReactNode => {
    switch (id) {
      case "identity": return <><strong>{client.title || client.market?.title || client.marketId}</strong><small className="mono">{client.marketId} · PID {client.pid ?? "—"}</small><details><summary>Activity details</summary><div className="monitor-detail"><span>Active orders: {Object.values(client.orderActivity?.active ?? {}).filter(item => item.orderId).length}</span><span>Last fill: <Time value={client.fills?.lastFillAtMs} /></span><span>Last order: <Time value={client.orderActivity?.lastActivityAtMs} /></span><span>Socket: {client.socketHealthy == null ? "Unavailable" : client.socketHealthy ? "healthy" : "unavailable"}</span>{client.orderActivity?.recent?.slice(-3).reverse().map((item, index) => <span key={`order-${index}`}>Order: {String(item.action ?? "unknown")} · {String(item.outcome ?? "unknown")}</span>)}{client.fills?.recent?.slice(-3).reverse().map((item, index) => <span key={`fill-${index}`}>Fill: {String(item.side ?? "unknown")} · {units(Number(item.quantityUnits ?? 0))} contracts</span>)}</div></details></>;
      case "venueShard": return <><strong>{client.venue ?? "unknown"}</strong><small>{client.workerId ?? "Unavailable"}</small></>;
      case "runtime": return <><StatusBadge value={client.lifecycle} /><small>{formatDuration(client.runtime?.startedAtMs ? clockMs - client.runtime.startedAtMs : client.runtime?.runningForMs)}</small></>;
      case "watchdog": return <><StatusBadge value={!client.watchdog ? "Unavailable" : client.watchdog.running ? client.watchdog.mode : "stopped"} /><small>{client.watchdog?.reason ?? "Unavailable"}</small></>;
      case "price": return <>{client.market?.priceUnits == null ? "Unavailable" : `${(client.market.priceUnits / 100).toFixed(2)}¢`}<small>{client.market?.priceSource ?? "unavailable"}</small></>;
      case "positionPnl": return <>{units(client.portfolio?.currentPositionUnits)}<small><Money cents={client.pnl?.totalCents} signed /></small></>;
      case "fillsOrders": return <>{client.fills?.count ?? "Unavailable"} fills<small>{orderAttempts(client)} order attempts</small></>;
      case "lastQuote": return <Time value={client.market?.lastQuoteAtMs} />; case "lastCreate": return <Time value={client.orderActivity?.lastCreateAtMs} />; case "lastFill": return <Time value={client.fills?.lastFillAtMs} />;
      default: return null;
    }
  }, [clockMs]);
  const marketSort = useCallback((client: ClientMonitoring, id: string) => ({ identity: client.title || client.marketId, venueShard: `${client.venue}:${client.workerId}`, runtime: client.runtime?.startedAtMs, watchdog: client.watchdog?.mode, price: client.market?.priceUnits, positionPnl: client.pnl?.totalCents, fillsOrders: client.fills?.count, lastQuote: client.market?.lastQuoteAtMs, lastCreate: client.orderActivity?.lastCreateAtMs, lastFill: client.fills?.lastFillAtMs } as Record<string, unknown>)[id], []);
  return <>
    <header className="page-header"><div><span className="eyebrow">LIVE TELEMETRY</span><h1>Monitoring</h1><p>Aggregate venue, shard, transport, and market activity.</p></div><div className="metrics-header-actions"><MonitoringColumnChooser preferences={preferences} onToggle={toggle} onMove={move} /><div className="header-status"><StatusBadge value={manager.lifecycle} /><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div></div></header>
    {(data.source.stale || data.warnings.length > 0) && <section className="warning-panel"><strong>Monitoring data may be stale</strong><ul>{data.warnings.map(item => <li key={item}>{item}</li>)}</ul></section>}
    <section className="panel capacity-panel" aria-label="Fleet capacity">
      <div className="panel-heading"><div><span className="eyebrow">CAPACITY</span><h2>Fleet capacity</h2></div><span><StatusBadge value={systemCapacityState === "healthy" ? "ok" : systemCapacityState === "unavailable" ? "unknown" : "limited"} /> <small>{systemCapacityState}</small></span></div>
      <section className="metrics capacity-summary" aria-label="System capacity summary">
        <article><span>Effective bot capacity</span><strong>{formatCount(effectiveSystemCapacity)}<small> / {formatCount(configuredSystemCapacity)}</small></strong><p>Hard cap {formatCount(capacityNumber(systemCapacity, "hardMaxBots"))} · {formatCount(slotsRemaining)} slots remaining</p></article>
        <article><span>Admitted markets</span><strong>{formatCount(admittedMarkets)}</strong><p>Current fleet allocation</p></article>
        <article><span>Resource capacity</span><strong>{formatCount(capacityNumber(systemCapacity, "resourceCapacity"))}</strong><p>Health capacity {formatCount(capacityNumber(systemCapacity, "healthCapacity"))}</p></article>
        <article><span>CPU / memory</span><strong>{percentage(capacityNumber(systemCapacity, "cpuPercent"))}</strong><p>{percentage(capacityNumber(systemCapacity, "memoryPercent"))} host memory · {bytes(capacityNumber(systemCapacity, "workerMemoryRssBytes"))} workers</p></article>
        <article><span>Starved workers</span><strong>{formatCount(capacityNumber(systemCapacity, "starvedWorkers"))}<small> / {formatCount(capacityNumber(systemCapacity, "totalWorkers"))}</small></strong><p>{formatCount(capacityNumber(systemCapacity, "unhealthySamples"))} unhealthy samples</p></article>
        <article><span>Queue / event-loop pressure</span><strong>{milliseconds(capacityNumber(systemCapacity, "maxQueueWaitMs"))}</strong><p>{milliseconds(capacityNumber(systemCapacity, "maxEventLoopLagMs"))} event-loop lag</p></article>
        <article><span>Worker budget</span><strong>{formatCount(capacityNumber(systemCapacity, "activeWorkerBudget"))}</strong><p>{formatCount(capacityNumber(systemCapacity, "workersRetained"))} retained · {formatCount(capacityNumber(systemCapacity, "workersScaledDown"))} scaled down</p></article>
        <article><span>Capacity reason</span><strong className="small-value">{systemCapacityState}</strong><p>Healthy since <Time value={capacityNumber(systemCapacity, "healthySinceMs")} /></p></article>
      </section>
      <div className="grid-two capacity-detail-grid">
        <div><div className="panel-heading"><div><span className="eyebrow">SYSTEM LIMITS</span><h3>Scaling diagnostics</h3></div></div><dl className="details"><div><dt>Last reduced</dt><dd><Time value={capacityNumber(systemCapacity, "lastReducedAtMs")} /></dd></div><div><dt>Last recovered</dt><dd><Time value={capacityNumber(systemCapacity, "lastRecoveredAtMs")} /></dd></div><div><dt>Worker CPU</dt><dd>{percentage(capacityNumber(systemCapacity, "workerCpuPercent"))}</dd></div><div><dt>Queue wait</dt><dd>{milliseconds(capacityNumber(systemCapacity, "maxQueueWaitMs"))}</dd></div></dl></div>
        <div><div className="panel-heading"><div><span className="eyebrow">VENUE LIMITS</span><h3>Venue capacity</h3></div><strong>{venueCapacityRows.length} venues</strong></div><div className="table-wrap capacity-table"><table><thead><tr><th>Venue</th><th>Markets</th><th>Quote sides</th><th>Write refill</th><th>State</th><th>Reason</th></tr></thead><tbody>{venueCapacityRows.map(([venue, capacity]) => { const marketLimit = capacityNumber(capacity, "venueCapacityLimit", "capacityMarketLimit", "venue_capacity_limit", "capacity_market_limit"); const admitted = capacityNumber(capacity, "admittedMarkets", "admitted_markets"); const quoteSides = capacityNumber(capacity, "venueQuoteSideCapacity", "normalQuoteSideCapacity", "venue_quote_side_capacity", "normal_quote_side_capacity"); const admittedQuoteSides = capacityNumber(capacity, "admittedQuoteSides", "admitted_quote_sides"); const refill = capacityNumber(capacity, "writeRefillRate", "write_refill_rate"); const limited = capacityBoolean(capacity, "venueCapacityLimited", "capacityLimited", "venue_capacity_limited", "capacity_limited"); const reason = capacityReasonLabel(capacityString(capacity, "venueCapacityReason", "omittedReason", "venue_capacity_reason", "omitted_reason")); return <tr key={venue}><td><strong>{venue}</strong></td><td>{formatCount(admitted)}<small> / {formatCount(marketLimit)}</small></td><td>{formatCount(admittedQuoteSides)}<small> / {formatCount(quoteSides)}</small></td><td>{formatCount(refill)}</td><td><StatusBadge value={limited ? "limited" : "ok"} /></td><td>{limited ? reason : "healthy"}</td></tr>; })}</tbody></table>{venueCapacityRows.length === 0 && <p className="empty">No venue capacity data is available.</p>}</div></div>
      </div>
    </section>
    <section className="metrics"><article><span>Active venues</span><strong>{activeVenues?.length ?? "Unavailable"}</strong><p>{activeVenues?.join(" · ") || "No venue data available"}</p></article><article><span>Active shards</span><strong>{shardHealth?.activeShards ?? "Unavailable"}<small> / {shardHealth?.totalShards ?? "Unavailable"}</small></strong><p>{shardHealth?.degradedShards ?? "—"} degraded · {shardHealth?.starvedShards ?? "—"} starved</p></article><article><span>Active actors</span><strong>{shardHealth?.activeActors ?? manager.botsRunning ?? "Unavailable"}</strong><p>{shardHealth?.recoveringShards ?? "—"} shards recovering</p></article><article><span>Resource consumption</span><strong>{shardHealth?.totalCpuPercent == null ? "Unavailable" : `${shardHealth.totalCpuPercent.toFixed(1)}%`}</strong><p>{bytes(shardHealth?.totalMemoryRssBytes)} memory</p></article><article><span>Last heartbeat</span><strong>{formatDuration(shardHealth?.oldestHeartbeatAgeMs)}</strong><p>oldest shard heartbeat age</p></article><article><span>Bots running</span><strong>{manager.botsRunning ?? "Unavailable"}<small> / {manager.configuredBots ?? "Unavailable"}</small></strong><p>Manager uptime {formatDuration(manager.startedAtMs ? clockMs - manager.startedAtMs : manager.runningForMs)}</p></article><article><span>Session P&amp;L</span><strong><Money cents={pnl?.totalCents} signed /></strong><p><Money cents={pnl?.realizedCents} signed /> realized · {pnl?.fills ?? "Unavailable"} fills{pnl?.complete === false ? " · partial" : ""}</p></article><article><span>Total API requests</span><strong>{manager.apiActivity?.rest?.total ?? "Unavailable"}</strong><p>{manager.apiActivity?.rest?.requestsLast60s ?? "Unavailable"} REST / min · {manager.apiActivity?.rest?.errors ?? "Unavailable"} errors</p></article></section>
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">API INFORMATION</span><h2>Venue transport activity</h2></div><strong>{venues.length} venues</strong></div><MonitoringTable kind="api" rows={venues} preferences={preferences.api} widths={widths.api} sort={sorts.api} rowKey={row => row.venue} sortValue={apiSort} renderCell={apiCell} empty="No per-venue API activity is available." onSort={id => selectSort("api", id)} onReorder={(source, target, position) => reorder("api", source, target, position)} onResize={(id, width) => resize("api", id, width)} /></section>
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">SHARDS</span><h2>Shard health</h2></div><strong>{workers.length} shards</strong></div><MonitoringTable kind="shards" rows={workers} preferences={preferences.shards} widths={widths.shards} sort={sorts.shards} rowKey={row => `${row.venue}:${row.workerId}`} sortValue={shardSort} renderCell={shardCell} empty="No shards are currently reporting." onSort={id => selectSort("shards", id)} onReorder={(source, target, position) => reorder("shards", source, target, position)} onResize={(id, width) => resize("shards", id, width)} /></section>
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">MARKETS</span><h2>Market activity</h2></div><strong>{data.clients.length} markets</strong></div><MonitoringTable kind="markets" rows={data.clients} preferences={preferences.markets} widths={widths.markets} sort={sorts.markets} rowKey={row => `${row.venue}:${row.workerId}:${row.marketId}`} sortValue={marketSort} renderCell={marketCell} empty="No market clients are currently reporting." onSort={id => selectSort("markets", id)} onReorder={(source, target, position) => reorder("markets", source, target, position)} onResize={(id, width) => resize("markets", id, width)} /></section>
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
