"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  HistoricalRun,
  MarkoutAggregate,
  MetricsHeartbeat,
  MetricsResponse,
  RunFillActivity,
  RunMarketMetrics,
  RunMarketActivityResponse,
  RunMarketsResponse,
  RunOrderRevision,
  SavedSession,
} from "@/lib/types";
import { duration, moneyUnits, priceUnits, contractUnits } from "./live-portfolio";
import { StatusBadge, Time } from "./status";

const LIMITS = [25, 50, 100, 250, 500] as const;
const MARKOUT_HORIZONS = [1_000, 5_000, 30_000, 120_000] as const;
const ACTIVE_STATES = new Set(["pending", "starting", "running"]);
const COLUMN_STORAGE_KEY = "kalshi.metrics.columns.v1";
const COLUMN_STORAGE_VERSION = 2;
const COLUMN_DRAG_TYPE = "application/x-kalshi-metrics-column";

type TableKind = "markets" | "fills" | "orders";
type SortDirection = "asc" | "desc";
type DropPosition = "before" | "after";
type SortState = { id: string; direction: SortDirection };
type ColumnPreference = { id: string; enabled: boolean };
type ColumnPreferences = Record<TableKind, ColumnPreference[]>;
type SortValue = string | number | null | undefined;
type ColumnDefinition<T> = {
  id: string;
  header: string;
  locked?: boolean;
  initialDirection: SortDirection;
  render: (item: T) => React.ReactNode;
  sortValue: (item: T) => SortValue;
};
type ColumnReorder = (kind: TableKind, sourceId: string, targetId: string, position: DropPosition) => void;
type ColumnDragRef = React.MutableRefObject<{ kind: TableKind; id: string } | null>;

type MetricsFilters = {
  activityFrom: string;
  activityTo: string;
  markoutMin: string;
  markoutMax: string;
  costMin: string;
  costMax: string;
  averageMarkoutMin: string;
  averageMarkoutMax: string;
  contractsMin: string;
  contractsMax: string;
};

type ParsedRange = { active: boolean; valid: boolean; minimum?: number; maximum?: number };
type ParsedFilters = {
  activity: ParsedRange;
  markout: ParsedRange;
  cost: ParsedRange;
  averageMarkout: ParsedRange;
  contracts: ParsedRange;
};

const COLUMN_OPTIONS: Record<TableKind, Array<{ id: string; label: string; locked?: boolean }>> = {
  markets: [
    { id: "identity", label: "Name / Description / Link / Ticker", locked: true },
    { id: "side", label: "Side" },
    { id: "contracts", label: "Contracts" },
    { id: "averageCost", label: "Avg Cost" },
    { id: "totalCost", label: "Total Cost" },
    { id: "markout", label: "Net Markout" },
    { id: "activityCount", label: "Amount of Fills / Orders" },
    { id: "firstFill", label: "First Fill Time" },
    { id: "lastFill", label: "Last Fill Time" },
  ],
  fills: [
    { id: "fillTime", label: "Fill Time" },
    { id: "fillId", label: "Fill ID" },
    { id: "side", label: "Side" },
    { id: "contracts", label: "Contracts (Matched and Open)" },
    { id: "timeToFill", label: "Time to Fill" },
    { id: "totalPaid", label: "Total Paid" },
    { id: "signedMarkout", label: "Signed Markout" },
    { id: "futureMidpoint", label: "Future Midpoint" },
    { id: "fee", label: "Fee" },
    { id: "netMarkout", label: "Net Markout" },
  ],
  orders: [
    { id: "orderTime", label: "Order Time" },
    { id: "orderId", label: "Order ID" },
    { id: "side", label: "Side" },
    { id: "contracts", label: "Contracts" },
    { id: "timeOnBook", label: "Time on Book" },
    { id: "book", label: "Bid / Ask / Mid" },
    { id: "orderPrice", label: "Order Price" },
    { id: "latestState", label: "Latest State" },
  ],
};

const EMPTY_METRICS_FILTERS: MetricsFilters = {
  activityFrom: "", activityTo: "",
  markoutMin: "", markoutMax: "",
  costMin: "", costMax: "",
  averageMarkoutMin: "", averageMarkoutMax: "",
  contractsMin: "", contractsMax: "",
};

function defaultColumnPreferences(): ColumnPreferences {
  return {
    markets: COLUMN_OPTIONS.markets.map(column => ({ id: column.id, enabled: true })),
    fills: COLUMN_OPTIONS.fills.map(column => ({ id: column.id, enabled: true })),
    orders: COLUMN_OPTIONS.orders.map(column => ({ id: column.id, enabled: true })),
  };
}

function sanitizeColumnPreferences(value: unknown): ColumnPreferences {
  const defaults = defaultColumnPreferences();
  if (!value || typeof value !== "object") return defaults;
  const candidate = value as { version?: unknown; tables?: unknown };
  if (candidate.version !== COLUMN_STORAGE_VERSION || !candidate.tables || typeof candidate.tables !== "object") return defaults;
  const tables = candidate.tables as Partial<Record<TableKind, unknown>>;
  const result = defaultColumnPreferences();
  for (const kind of Object.keys(COLUMN_OPTIONS) as TableKind[]) {
    const known = new Set(COLUMN_OPTIONS[kind].map(column => column.id));
    const saved = Array.isArray(tables[kind]) ? tables[kind] : [];
    const seen = new Set<string>();
    const preferences: ColumnPreference[] = [];
    for (const entry of saved) {
      if (!entry || typeof entry !== "object") continue;
      const row = entry as { id?: unknown; enabled?: unknown };
      if (typeof row.id !== "string" || !known.has(row.id) || seen.has(row.id)) continue;
      preferences.push({ id: row.id, enabled: typeof row.enabled === "boolean" ? row.enabled : true });
      seen.add(row.id);
    }
    for (const column of COLUMN_OPTIONS[kind]) {
      if (!seen.has(column.id)) preferences.push({ id: column.id, enabled: true });
    }
    if (kind === "markets") {
      const identity = preferences.find(column => column.id === "identity") ?? { id: "identity", enabled: true };
      result[kind] = [{ ...identity, enabled: true }, ...preferences.filter(column => column.id !== "identity")];
    } else {
      result[kind] = preferences;
    }
  }
  return result;
}

function parseNumericRange(minimum: string, maximum: string, scale: number): ParsedRange {
  const active = minimum !== "" || maximum !== "";
  const min = minimum === "" ? undefined : Number(minimum) * scale;
  const max = maximum === "" ? undefined : Number(maximum) * scale;
  const valid = (!active || ((min == null || Number.isFinite(min)) && (max == null || Number.isFinite(max))))
    && (min == null || max == null || min <= max);
  return { active, valid, minimum: min, maximum: max };
}

function parseDateRange(from: string, to: string): ParsedRange {
  const active = from !== "" || to !== "";
  const minimum = from === "" ? undefined : new Date(from).getTime();
  const maximum = to === "" ? undefined : new Date(to).getTime();
  const valid = (!active || ((minimum == null || Number.isFinite(minimum)) && (maximum == null || Number.isFinite(maximum))))
    && (minimum == null || maximum == null || minimum <= maximum);
  return { active, valid, minimum, maximum };
}

function parseFilters(filters: MetricsFilters): ParsedFilters {
  return {
    activity: parseDateRange(filters.activityFrom, filters.activityTo),
    markout: parseNumericRange(filters.markoutMin, filters.markoutMax, 10_000),
    cost: parseNumericRange(filters.costMin, filters.costMax, 10_000),
    averageMarkout: parseNumericRange(filters.averageMarkoutMin, filters.averageMarkoutMax, 100),
    contracts: parseNumericRange(filters.contractsMin, filters.contractsMax, 100),
  };
}

function matchesRange(value: number | null | undefined, range: ParsedRange) {
  if (!range.active || !range.valid) return true;
  if (value == null || !Number.isFinite(value)) return false;
  return (range.minimum == null || value >= range.minimum) && (range.maximum == null || value <= range.maximum);
}

function selectedMarkout(values: Record<string, MarkoutAggregate> | undefined, horizonMs: number) {
  return values?.[String(horizonMs)];
}

function runMarkout(run: HistoricalRun, horizonMs: number) {
  const values = run.metrics.markoutsByHorizon;
  return selectedMarkout(
    values && typeof values === "object" ? values as Record<string, MarkoutAggregate> : undefined,
    horizonMs,
  );
}

function aggregateRunMarkouts(runs: HistoricalRun[]) {
  const output: Record<string, MarkoutAggregate> = {};
  for (const horizonMs of MARKOUT_HORIZONS) {
    const rows = runs.map(run => runMarkout(run, horizonMs)).filter((row): row is MarkoutAggregate => Boolean(row));
    const sum = (field: keyof MarkoutAggregate) => rows.reduce((total, row) => total + Number(row[field] ?? 0), 0);
    const contracts = sum("coveredContractsUnits");
    const net = sum("netMarkoutUnits");
    const pending = sum("pendingFillCount");
    const unavailable = sum("unavailableFillCount");
    output[String(horizonMs)] = {
      horizonMs,
      grossMarkoutUnits: sum("grossMarkoutUnits"), feeUnits: sum("feeUnits"), netMarkoutUnits: net,
      averageNetMarkoutPriceUnits: contracts ? Math.round(net * 100 / contracts) : null,
      coveredFillCount: sum("coveredFillCount"), coveredContractsUnits: contracts,
      totalFillCount: sum("totalFillCount"), pendingFillCount: pending,
      unavailableFillCount: unavailable, complete: pending === 0 && unavailable === 0,
    };
  }
  return output;
}

function marketMatchesFilters(market: RunMarketMetrics, filters: ParsedFilters, horizonMs: number) {
  const markout = selectedMarkout(market.markoutsByHorizon, horizonMs);
  return matchesRange(market.lastFillAtMs, filters.activity)
    && matchesRange(markout?.netMarkoutUnits, filters.markout)
    && matchesRange(market.totalCostUnits, filters.cost)
    && matchesRange(markout?.averageNetMarkoutPriceUnits, filters.averageMarkout)
    && matchesRange(market.yesContractsUnits + market.noContractsUnits, filters.contracts);
}

function fillMatchesFilters(fill: RunFillActivity, filters: ParsedFilters, horizonMs: number) {
  const markout = fill.markoutsByHorizon?.[String(horizonMs)];
  return matchesRange(fill.filledAtMs, filters.activity)
    && matchesRange(markout?.netMarkoutUnits, filters.markout)
    && matchesRange(fill.totalPaidUnits, filters.cost)
    && matchesRange(markout?.signedMarkoutPriceUnits, filters.averageMarkout)
    && matchesRange(fill.contractsUnits, filters.contracts);
}

function orderNotionalUnits(order: RunOrderRevision) {
  return order.orderPriceUnits == null ? null : Math.round(order.contractsUnits * order.orderPriceUnits / 100);
}

function orderMatchesFilters(order: RunOrderRevision, filters: ParsedFilters) {
  return matchesRange(order.placedAtMs, filters.activity)
    && matchesRange(orderNotionalUnits(order), filters.cost)
    && matchesRange(order.contractsUnits, filters.contracts);
}

function compareSortValues(left: SortValue, right: SortValue, direction: SortDirection) {
  const leftMissing = left == null || (typeof left === "number" && !Number.isFinite(left));
  const rightMissing = right == null || (typeof right === "number" && !Number.isFinite(right));
  if (leftMissing || rightMissing) return leftMissing === rightMissing ? 0 : leftMissing ? 1 : -1;
  const comparison = typeof left === "number" && typeof right === "number"
    ? left - right
    : String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
  return direction === "asc" ? comparison : -comparison;
}

function sortRows<T>(items: T[], columns: ColumnDefinition<T>[], sort: SortState, stableKey: (item: T) => string) {
  const column = columns.find(item => item.id === sort.id) ?? columns[0];
  return items.map((item, index) => ({ item, index })).sort((left, right) => {
    const comparison = column ? compareSortValues(column.sortValue(left.item), column.sortValue(right.item), sort.direction) : 0;
    if (comparison) return comparison;
    const keyComparison = stableKey(left.item).localeCompare(stableKey(right.item), undefined, { numeric: true, sensitivity: "base" });
    return keyComparison || left.index - right.index;
  }).map(entry => entry.item);
}

function enabledColumns<T>(definitions: ColumnDefinition<T>[], preferences: ColumnPreference[]) {
  const byId = new Map(definitions.map(column => [column.id, column]));
  return preferences.filter(preference => preference.enabled).map(preference => byId.get(preference.id)).filter((column): column is ColumnDefinition<T> => Boolean(column));
}

function selectSort(setter: React.Dispatch<React.SetStateAction<SortState>>, column: { id: string; initialDirection: SortDirection }) {
  setter(previous => previous.id === column.id
    ? { id: column.id, direction: previous.direction === "asc" ? "desc" : "asc" }
    : { id: column.id, direction: column.initialDirection });
}

function marketAverageCost(market: RunMarketMetrics) {
  const totalContracts = market.yesContractsUnits + market.noContractsUnits;
  if (!totalContracts) return null;
  if ((market.yesContractsUnits && market.yesAverageCostPriceUnits == null) || (market.noContractsUnits && market.noAverageCostPriceUnits == null)) return null;
  return ((market.yesAverageCostPriceUnits ?? 0) * market.yesContractsUnits + (market.noAverageCostPriceUnits ?? 0) * market.noContractsUnits) / totalContracts;
}

function bookSortValue(order: RunOrderRevision) {
  return order.bookMidPriceUnits ?? order.bookBidPriceUnits ?? order.bookAskPriceUnits;
}

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

function ColumnChooser({ preferences, onToggle, onMove }: {
  preferences: ColumnPreferences;
  onToggle: (kind: TableKind, id: string) => void;
  onMove: (kind: TableKind, id: string, direction: -1 | 1) => void;
}) {
  return <details className="metrics-columns">
    <summary className="button">Columns</summary>
    <div className="metrics-columns-menu">
      <p className="metrics-columns-hint">Drag a table header left or right to reorder it everywhere. Arrow buttons provide the same control here.</p>
      {(Object.keys(COLUMN_OPTIONS) as TableKind[]).map(kind => {
        const options = new Map(COLUMN_OPTIONS[kind].map(column => [column.id, column]));
        const title = kind[0].toUpperCase() + kind.slice(1);
        return <section key={kind}>
          <strong>{title}</strong>
          <div className="metrics-column-list">
            {preferences[kind].map((preference, index) => {
              const option = options.get(preference.id);
              if (!option) return null;
              const minimumIndex = kind === "markets" ? 1 : 0;
              return <div className="metrics-column-option" key={preference.id}>
                <label>
                  <input
                    type="checkbox"
                    aria-label={`${title}: ${option.label}`}
                    checked={preference.enabled}
                    disabled={Boolean(option.locked)}
                    onChange={() => onToggle(kind, preference.id)}
                  />
                  <span>{option.label}{option.locked ? " (locked)" : ""}</span>
                </label>
                <span className="metrics-column-movers">
                  <button className="column-move" type="button" aria-label={`Move ${option.label} left in ${title}`} disabled={Boolean(option.locked) || index <= minimumIndex} onClick={() => onMove(kind, preference.id, -1)}>←</button>
                  <button className="column-move" type="button" aria-label={`Move ${option.label} right in ${title}`} disabled={Boolean(option.locked) || index >= preferences[kind].length - 1} onClick={() => onMove(kind, preference.id, 1)}>→</button>
                </span>
              </div>;
            })}
          </div>
        </section>;
      })}
    </div>
  </details>;
}

function MetricsFilterPanel({ value, enabled, onChange, onClear }: {
  value: MetricsFilters;
  enabled: boolean;
  onChange: (value: MetricsFilters) => void;
  onClear: () => void;
}) {
  const parsed = parseFilters(value);
  const invalid = Object.values(parsed).some(range => !range.valid);
  const update = (key: keyof MetricsFilters, next: string) => onChange({ ...value, [key]: next });
  return <section className={`metrics-filter-panel${enabled ? "" : " disabled"}`} aria-labelledby="metrics-detail-filters">
    <div className="metrics-filter-heading">
      <div><span className="eyebrow">SELECTED SESSION</span><h2 id="metrics-detail-filters">Activity filters</h2></div>
      {!enabled && <p>Choose one Session above to filter its loaded Markets, Fills, and Orders.</p>}
    </div>
    <fieldset disabled={!enabled}>
      <div className={`metrics-filter-group${!parsed.activity.valid ? " invalid" : ""}`}>
        <strong>Last activity time</strong>
        <label><span>From</span><input aria-label="Last activity from" type="datetime-local" value={value.activityFrom} onChange={event => update("activityFrom", event.target.value)} /></label>
        <label><span>To</span><input aria-label="Last activity to" type="datetime-local" value={value.activityTo} onChange={event => update("activityTo", event.target.value)} /></label>
      </div>
      <NumericFilterGroup label="Net Markout" minimum={value.markoutMin} maximum={value.markoutMax} valid={parsed.markout.valid} onMinimum={next => update("markoutMin", next)} onMaximum={next => update("markoutMax", next)} />
      <NumericFilterGroup label="Total Cost" minimum={value.costMin} maximum={value.costMax} valid={parsed.cost.valid} onMinimum={next => update("costMin", next)} onMaximum={next => update("costMax", next)} />
      <NumericFilterGroup label="Average Markout" minimum={value.averageMarkoutMin} maximum={value.averageMarkoutMax} valid={parsed.averageMarkout.valid} onMinimum={next => update("averageMarkoutMin", next)} onMaximum={next => update("averageMarkoutMax", next)} units="cents" />
      <NumericFilterGroup label="Total Contracts" minimum={value.contractsMin} maximum={value.contractsMax} valid={parsed.contracts.valid} onMinimum={next => update("contractsMin", next)} onMaximum={next => update("contractsMax", next)} units="contracts" />
      <button className="button metrics-filter-clear" type="button" onClick={onClear}>Clear filters</button>
    </fieldset>
    {enabled && invalid && <p className="metrics-filter-error" role="alert">A From/Min value cannot be greater than its To/Max value. That range is ignored until corrected.</p>}
  </section>;
}

function NumericFilterGroup({ label, minimum, maximum, valid, onMinimum, onMaximum, units = "dollars" }: {
  label: string;
  minimum: string;
  maximum: string;
  valid: boolean;
  onMinimum: (value: string) => void;
  onMaximum: (value: string) => void;
  units?: "dollars" | "contracts" | "cents";
}) {
  const step = units === "dollars" ? "0.01" : "0.01";
  return <div className={`metrics-filter-group${valid ? "" : " invalid"}`}>
    <strong>{label}</strong>
    <label><span>Min {units === "dollars" ? "$" : units === "cents" ? "¢" : ""}</span><input aria-label={`${label} minimum`} type="number" step={step} value={minimum} onChange={event => onMinimum(event.target.value)} /></label>
    <label><span>Max {units === "dollars" ? "$" : units === "cents" ? "¢" : ""}</span><input aria-label={`${label} maximum`} type="number" step={step} value={maximum} onChange={event => onMaximum(event.target.value)} /></label>
  </div>;
}

function SortableHeader<T>({ kind, column, sort, dragRef, onSort, onReorder }: {
  kind: TableKind;
  column: ColumnDefinition<T>;
  sort: SortState;
  dragRef: ColumnDragRef;
  onSort: (column: ColumnDefinition<T>) => void;
  onReorder: ColumnReorder;
}) {
  const active = sort.id === column.id;
  const [dropPosition, setDropPosition] = useState<DropPosition | null>(null);
  const draggable = !column.locked;
  const readDrag = (event: React.DragEvent): { kind: string; id: string } | null => {
    try {
      const raw = event.dataTransfer.getData(COLUMN_DRAG_TYPE) || event.dataTransfer.getData("text/plain");
      const payload = JSON.parse(raw) as { kind?: unknown; id?: unknown };
      return typeof payload.kind === "string" && typeof payload.id === "string" ? { kind: payload.kind, id: payload.id } : null;
    } catch {
      return null;
    }
  };
  return <th
    aria-sort={active ? (sort.direction === "asc" ? "ascending" : "descending") : undefined}
    className={`${draggable ? "metrics-column-draggable" : "metrics-column-locked"}${dropPosition ? ` column-drop-${dropPosition}` : ""}`}
    draggable={draggable}
    title={draggable ? "Drag left or right to reorder this column" : "This column is locked first"}
    onDragStart={event => {
      if (!draggable) { event.preventDefault(); return; }
      const payload = JSON.stringify({ kind, id: column.id });
      dragRef.current = { kind, id: column.id };
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData(COLUMN_DRAG_TYPE, payload);
      event.dataTransfer.setData("text/plain", payload);
    }}
    onDragOver={event => {
      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
      const bounds = event.currentTarget.getBoundingClientRect();
      setDropPosition(event.clientX <= bounds.left + bounds.width / 2 ? "before" : "after");
    }}
    onDragLeave={event => {
      if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setDropPosition(null);
    }}
    onDrop={event => {
      event.preventDefault();
      const payload = dragRef.current ?? readDrag(event);
      const position = dropPosition ?? "before";
      setDropPosition(null);
      dragRef.current = null;
      if (payload?.kind === kind && payload.id !== column.id) onReorder(kind, payload.id, column.id, position);
    }}
    onDragEnd={() => { dragRef.current = null; setDropPosition(null); }}
  >
    <button className="metrics-sort-button" type="button" onClick={() => onSort(column)}>
      <span className="metrics-sort-label">{draggable && <span className="column-drag-grip" aria-hidden="true">⋮⋮</span>}{column.header}</span><span className="sort-indicator" aria-hidden="true">{active ? sort.direction === "asc" ? "↑" : "↓" : "↕"}</span>
    </button>
  </th>;
}

function Limits({ fillLimit, orderLimit, onChange }: { fillLimit: number; orderLimit: number; onChange: (kind: "fill" | "order", value: number) => void }) {
  return <div className="metrics-limits" aria-label="Activity display caps">
    <label>Fills<select value={fillLimit} onChange={event => onChange("fill", Number(event.target.value))}>{LIMITS.map(value => <option key={value}>{value}</option>)}</select></label>
    <label>Orders<select value={orderLimit} onChange={event => onChange("order", Number(event.target.value))}>{LIMITS.map(value => <option key={value}>{value}</option>)}</select></label>
    <small>Browser-local newest-row caps</small>
  </div>;
}

export function LiveMetrics({ initial, sessions, query, markoutHorizonMs = 30_000, filters = { sessionId: "", status: "", from: "", to: "" } }: { initial: MetricsResponse; sessions: SavedSession[]; query: string; markoutHorizonMs?: number; filters?: { sessionId: string; status: string; from: string; to: string } }) {
  const [data, setData] = useState(initial);
  const [connected, setConnected] = useState(false);
  const [expandedSessions, setExpandedSessions] = useState<Set<string>>(new Set());
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());
  const [expandedMarkets, setExpandedMarkets] = useState<Set<string>>(new Set());
  const [runMarkets, setRunMarkets] = useState<Record<string, LoadState<RunMarketsResponse>>>({});
  const [marketActivity, setMarketActivity] = useState<Record<string, LoadState<RunMarketActivityResponse>>>({});
  const [fillLimit, setFillLimit] = useState(100);
  const [orderLimit, setOrderLimit] = useState(100);
  const [columnPreferences, setColumnPreferences] = useState<ColumnPreferences>(defaultColumnPreferences);
  const [columnsHydrated, setColumnsHydrated] = useState(false);
  const [metricFilters, setMetricFilters] = useState<MetricsFilters>(EMPTY_METRICS_FILTERS);
  const [selectedHorizonMs, setSelectedHorizonMs] = useState(markoutHorizonMs);
  const [marketSort, setMarketSort] = useState<SortState>({ id: "lastFill", direction: "desc" });
  const [fillSort, setFillSort] = useState<SortState>({ id: "fillTime", direction: "desc" });
  const [orderSort, setOrderSort] = useState<SortState>({ id: "orderTime", direction: "desc" });
  const expandedRunsRef = useRef(expandedRuns);
  const expandedMarketsRef = useRef(expandedMarkets);
  const columnDragRef = useRef<{ kind: TableKind; id: string } | null>(null);
  const dataRef = useRef(data);
  const initialActive = initial.runs.find(run => ACTIVE_STATES.has(run.status));
  const activeIdentityRef = useRef(initialActive?.id ?? "none");
  const activityRevisionRef = useRef("");

  useEffect(() => { expandedRunsRef.current = expandedRuns; }, [expandedRuns]);
  useEffect(() => { expandedMarketsRef.current = expandedMarkets; }, [expandedMarkets]);
  useEffect(() => { dataRef.current = data; }, [data]);
  const changeHorizon = (value: number) => {
    const horizon = MARKOUT_HORIZONS.includes(value as typeof MARKOUT_HORIZONS[number]) ? value : 30_000;
    setSelectedHorizonMs(horizon);
    const params = new URLSearchParams(window.location.search);
    params.set("markout_horizon_ms", String(horizon));
    window.history.replaceState(null, "", `${window.location.pathname}?${params.toString()}`);
  };
  useEffect(() => {
    const storedFill = Number(window.localStorage.getItem("kalshi.metrics.fillLimit"));
    const storedOrder = Number(window.localStorage.getItem("kalshi.metrics.orderLimit"));
    if (LIMITS.includes(storedFill as typeof LIMITS[number])) setFillLimit(storedFill);
    if (LIMITS.includes(storedOrder as typeof LIMITS[number])) setOrderLimit(storedOrder);
  }, []);
  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(COLUMN_STORAGE_KEY);
      if (stored) setColumnPreferences(sanitizeColumnPreferences(JSON.parse(stored)));
    } catch {
      setColumnPreferences(defaultColumnPreferences());
    } finally {
      setColumnsHydrated(true);
    }
  }, []);
  useEffect(() => {
    if (!columnsHydrated) return;
    try {
      window.localStorage.setItem(COLUMN_STORAGE_KEY, JSON.stringify({ version: COLUMN_STORAGE_VERSION, tables: columnPreferences }));
    } catch {
      // Browser storage can be disabled; table preferences remain usable for this page view.
    }
  }, [columnPreferences, columnsHydrated]);

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
        summary.markoutsByHorizon = aggregateRunMarkouts(runs);
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

  const parsedMetricFilters = useMemo(() => parseFilters(metricFilters), [metricFilters]);

  const toggleColumn = (kind: TableKind, id: string) => {
    if (COLUMN_OPTIONS[kind].find(column => column.id === id)?.locked) return;
    setColumnPreferences(previous => ({
      ...previous,
      [kind]: previous[kind].map(column => column.id === id ? { ...column, enabled: !column.enabled } : column),
    }));
  };

  const moveColumn = (kind: TableKind, id: string, direction: -1 | 1) => {
    if (COLUMN_OPTIONS[kind].find(column => column.id === id)?.locked) return;
    setColumnPreferences(previous => {
      const columns = [...previous[kind]];
      const index = columns.findIndex(column => column.id === id);
      const target = index + direction;
      const minimum = kind === "markets" ? 1 : 0;
      if (index < 0 || target < minimum || target >= columns.length) return previous;
      [columns[index], columns[target]] = [columns[target], columns[index]];
      return { ...previous, [kind]: columns };
    });
  };

  const reorderColumn: ColumnReorder = (kind, sourceId, targetId, position) => {
    if (sourceId === targetId || COLUMN_OPTIONS[kind].find(column => column.id === sourceId)?.locked) return;
    setColumnPreferences(previous => {
      const columns = [...previous[kind]];
      const sourceIndex = columns.findIndex(column => column.id === sourceId);
      if (sourceIndex < 0 || !columns.some(column => column.id === targetId)) return previous;
      const [source] = columns.splice(sourceIndex, 1);
      const targetIndex = columns.findIndex(column => column.id === targetId);
      const minimum = kind === "markets" ? 1 : 0;
      const insertionIndex = Math.max(minimum, targetIndex + (position === "after" ? 1 : 0));
      columns.splice(Math.min(insertionIndex, columns.length), 0, source);
      if (columns.every((column, index) => column.id === previous[kind][index]?.id)) return previous;
      return { ...previous, [kind]: columns };
    });
  };

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
  const summaryMarkout = selectedMarkout(summary.markoutsByHorizon, selectedHorizonMs);
  const horizonLabel = `${selectedHorizonMs / 1000}s`;
  return <>
    <header className="page-header"><div><span className="eyebrow">POST-TRADE EXECUTION QUALITY</span><h1>Metrics</h1><p>Size-weighted venue-midpoint markouts from local telemetry. This view makes no exchange requests.</p></div><div className="metrics-header-actions"><label>Markout horizon<select aria-label="Markout horizon" value={selectedHorizonMs} onChange={event => changeHorizon(Number(event.target.value))}>{MARKOUT_HORIZONS.map(value => <option key={value} value={value}>{value / 1000}s</option>)}</select></label><div className="header-status"><strong>{summary.timesRun} runs</strong><span className={connected ? "positive" : "negative"}>{connected ? "Live" : "Reconnecting"}</span></div><ColumnChooser preferences={columnPreferences} onToggle={toggleColumn} onMove={moveColumn} /></div></header>
    <form className="filters"><input type="hidden" name="markout_horizon_ms" value={selectedHorizonMs} /><label>Session<select name="session_id" defaultValue={filters.sessionId}><option value="">All sessions</option>{sessions.map(item => <option key={item.id} value={item.id}>{item.name}{item.archivedAt ? " (archived)" : ""}</option>)}</select></label><label>Status<select name="status" defaultValue={filters.status}><option value="">All outcomes</option>{["pending", "starting", "running", "stopped", "failed", "shutdown_failed", "interrupted"].map(item => <option key={item}>{item}</option>)}</select></label><label>From<input type="date" name="from" defaultValue={filters.from} /></label><label>To<input type="date" name="to" defaultValue={filters.to} /></label><button className="button">Apply</button></form>
    {summaryMarkout && !summaryMarkout.complete && <section className="warning-panel"><strong>Markout coverage is incomplete</strong><p>{summaryMarkout.unavailableFillCount} unavailable and {summaryMarkout.pendingFillCount} pending fills are excluded from the {horizonLabel} result.</p></section>}
    <section className="metrics order-metrics"><article><span>{horizonLabel} Net Markout</span><strong className={(summaryMarkout?.netMarkoutUnits ?? 0) < 0 ? "negative" : (summaryMarkout?.netMarkoutUnits ?? 0) > 0 ? "positive" : ""}>{moneyUnits(summaryMarkout?.netMarkoutUnits, true)}</strong><p>{priceUnits(summaryMarkout?.averageNetMarkoutPriceUnits)} per covered contract · {summaryMarkout?.coveredFillCount ?? 0}/{summaryMarkout?.totalFillCount ?? 0} fills</p></article><article><span>Orders / min</span><strong>{summary.ordersPerMinute.toFixed(2)}</strong><p>{summary.orders} attempts</p></article><article><span>Fills / min</span><strong>{summary.fillsPerMinute.toFixed(2)}</strong><p>{summary.fills} fills</p></article><article><span>Runtime</span><strong className="small-value">{duration(summary.runtimeMs)}</strong><p>{summary.timesRun} launches</p></article><article><span>API calls</span><strong>{summary.apiCalls}</strong><p>{summary.apiErrors} errors</p></article></section>
    <MetricsFilterPanel value={metricFilters} enabled={Boolean(filters.sessionId)} onChange={setMetricFilters} onClear={() => setMetricFilters(EMPTY_METRICS_FILTERS)} />
    <section className="panel"><div className="panel-heading"><div><span className="eyebrow">EXCHANGE REST</span><h2>API calls by component</h2></div></div><div className="source-list">{Object.entries(summary.apiByComponent).map(([name, count]) => <div key={name}><strong>{name}</strong><span>{count}</span></div>)}</div></section>
    <div className="metrics-table-toolbar"><div><span className="eyebrow">SESSIONS → RUNS → MARKETS</span><h2>Trading activity</h2></div><Limits fillLimit={fillLimit} orderLimit={orderLimit} onChange={changeLimit} /></div>
    <div className="metrics-tree">
      {grouped.map(({ session, runs }) => {
        const open = expandedSessions.has(session.id);
        const runtime = runs.reduce((total, run) => total + metric(run, "runtimeMs"), 0);
        const orders = runs.reduce((total, run) => total + metric(run, "orders"), 0);
        const fills = runs.reduce((total, run) => total + metric(run, "fills"), 0);
        const sessionMarkouts = runs
          .map((run) => runMarkout(run, selectedHorizonMs))
          .filter((aggregate): aggregate is MarkoutAggregate => aggregate !== undefined);
        const sessionMarkout = sessionMarkouts.length
          ? sessionMarkouts.reduce((total, aggregate) => total + aggregate.netMarkoutUnits, 0)
          : undefined;
        return <section className="metrics-session" key={session.id}>
          <button className="metrics-session-row" type="button" aria-expanded={open} onClick={() => toggleSet(setExpandedSessions, session.id)}><span className="tree-chevron">{open ? "−" : "+"}</span><span><strong>{session.name}</strong><small>{runs.length} run{runs.length === 1 ? "" : "s"}</small></span><span><small>Runtime</small>{duration(runtime)}</span><span><small>Orders / fills</small>{orders} / {fills}</span><span><small>{horizonLabel} net markout</small>{moneyUnits(sessionMarkout, true)}</span></button>
          {open && <div className="metrics-runs table-wrap"><table><thead><tr><th>Run</th><th>Status</th><th>Started / runtime</th><th>Orders / fills</th><th>{horizonLabel} net markout</th><th>API</th><th>Artifacts</th></tr></thead><tbody>
            {runs.map(run => {
              const runOpen = expandedRuns.has(run.id);
              const state = runMarkets[run.id];
              return <Fragment key={run.id}><tr className="expandable-position" onClick={() => { toggleSet(setExpandedRuns, run.id); if (!runOpen && !state?.data) void loadRunMarkets(run.id); }}><td><div className="market-cell"><button type="button" className="row-toggle" aria-label={`${runOpen ? "Collapse" : "Expand"} run ${run.id}`} aria-expanded={runOpen}>{runOpen ? "−" : "+"}</button><span className="mono">{run.id.slice(0, 8)}{ACTIVE_STATES.has(run.status) && <small className="session-tag">Live run</small>}</span></div></td><td><StatusBadge value={run.status} /></td><td><Time value={run.startedAt ?? run.createdAt} /><small>{duration(metric(run, "runtimeMs"))}</small></td><td>{metric(run, "orders")} / {metric(run, "fills")}</td><td>{moneyUnits(runMarkout(run, selectedHorizonMs)?.netMarkoutUnits, true)}</td><td>{metric(run, "apiCalls")}<small>{metric(run, "apiErrors")} errors</small></td><td>{bytes(run.artifactBytes)}</td></tr>
                {runOpen && <tr className="metrics-nested-row"><td colSpan={7}><RunMarkets run={run} state={state} horizonMs={selectedHorizonMs} expandedMarkets={expandedMarkets} activity={marketActivity} columnPreferences={columnPreferences} filters={parsedMetricFilters} marketSort={marketSort} fillSort={fillSort} orderSort={orderSort} columnDragRef={columnDragRef} onMarketSort={column => selectSort(setMarketSort, column)} onFillSort={column => selectSort(setFillSort, column)} onOrderSort={column => selectSort(setOrderSort, column)} onColumnReorder={reorderColumn} onToggle={(ticker, isOpen) => { const key = `${run.id}:${ticker}`; toggleSet(setExpandedMarkets, key); if (!isOpen && !marketActivity[key]?.data) void loadActivity(run.id, ticker); }} /></td></tr>}
              </Fragment>;
            })}
          </tbody></table></div>}
        </section>;
      })}
      {!grouped.length && <p className="empty">No session-aware runs match these filters. Legacy artifacts were intentionally not guessed into sessions.</p>}
    </div>
  </>;
}

function RunMarkets({ run, state, horizonMs, expandedMarkets, activity, columnPreferences, filters, marketSort, fillSort, orderSort, columnDragRef, onMarketSort, onFillSort, onOrderSort, onColumnReorder, onToggle }: {
  run: HistoricalRun;
  state?: LoadState<RunMarketsResponse>;
  horizonMs: number;
  expandedMarkets: Set<string>;
  activity: Record<string, LoadState<RunMarketActivityResponse>>;
  columnPreferences: ColumnPreferences;
  filters: ParsedFilters;
  marketSort: SortState;
  fillSort: SortState;
  orderSort: SortState;
  columnDragRef: ColumnDragRef;
  onMarketSort: (column: ColumnDefinition<RunMarketMetrics>) => void;
  onFillSort: (column: ColumnDefinition<RunFillActivity>) => void;
  onOrderSort: (column: ColumnDefinition<RunOrderRevision>) => void;
  onColumnReorder: ColumnReorder;
  onToggle: (ticker: string, isOpen: boolean) => void;
}) {
  const marketColumns: ColumnDefinition<RunMarketMetrics>[] = [
    {
      id: "identity", header: "Market", locked: true, initialDirection: "asc", sortValue: market => `${market.description}\u0000${market.ticker}`,
      render: market => {
        const key = `${run.id}:${market.ticker}`;
        const open = expandedMarkets.has(key);
        return <div className="market-cell"><button type="button" className="row-toggle" aria-expanded={open} aria-label={`${open ? "Collapse" : "Expand"} ${market.ticker}`}>{open ? "−" : "+"}</button><span><strong>{market.marketUrl ? <a className="external-link" href={market.marketUrl} target="_blank" rel="noreferrer" onClick={event => event.stopPropagation()}>{market.description} ↗</a> : market.description}</strong><small className="mono">{market.ticker}</small></span></div>;
      },
    },
    { id: "side", header: "Side", initialDirection: "asc", sortValue: market => market.side, render: market => <Side value={market.side} /> },
    { id: "contracts", header: "Contracts", initialDirection: "desc", sortValue: market => market.yesContractsUnits + market.noContractsUnits, render: market => <><span>YES {contractUnits(market.yesContractsUnits)}</span><small>NO {contractUnits(market.noContractsUnits)}</small></> },
    { id: "averageCost", header: "Avg cost", initialDirection: "desc", sortValue: marketAverageCost, render: market => <><span>YES {priceUnits(market.yesAverageCostPriceUnits)}</span><small>NO {priceUnits(market.noAverageCostPriceUnits)}</small></> },
    { id: "totalCost", header: "Total cost", initialDirection: "desc", sortValue: market => market.totalCostUnits, render: market => moneyUnits(market.totalCostUnits) },
    { id: "markout", header: `${horizonMs / 1000}s net markout`, initialDirection: "desc", sortValue: market => selectedMarkout(market.markoutsByHorizon, horizonMs)?.netMarkoutUnits, render: market => { const markout = selectedMarkout(market.markoutsByHorizon, horizonMs); return <span className={markout == null ? "muted" : markout.netMarkoutUnits < 0 ? "negative" : markout.netMarkoutUnits > 0 ? "positive" : ""}>{moneyUnits(markout?.netMarkoutUnits, true)}<small>{priceUnits(markout?.averageNetMarkoutPriceUnits)} / contract · {markout?.coveredFillCount ?? 0}/{markout?.totalFillCount ?? market.fillCount} fills</small></span>; } },
    { id: "activityCount", header: "Fills / orders", initialDirection: "desc", sortValue: market => market.fillCount + market.orderCount, render: market => `${market.fillCount} / ${market.orderCount}` },
    { id: "firstFill", header: "First fill", initialDirection: "desc", sortValue: market => market.firstFillAtMs, render: market => <Time value={market.firstFillAtMs} /> },
    { id: "lastFill", header: "Last fill", initialDirection: "desc", sortValue: market => market.lastFillAtMs, render: market => <Time value={market.lastFillAtMs} /> },
  ];
  const visibleColumns = enabledColumns(marketColumns, columnPreferences.markets);
  const visibleMarkets = sortRows((state?.data?.items ?? []).filter(market => marketMatchesFilters(market, filters, horizonMs)), marketColumns, marketSort, market => market.ticker);
  if (state?.loading && !state.data) return <p className="empty">Loading markets…</p>;
  if (state?.error && !state.data) return <p className="error">{state.error}</p>;
  if (!state?.data?.items.length) return <p className="empty">No markets with recorded order or fill activity.</p>;
  return <div className="run-markets">
    <div className="nested-heading"><strong>Markets</strong><span>{visibleMarkets.length !== state.data.items.length ? `${visibleMarkets.length} of ${state.data.items.length} match · ` : ""}<Time value={state.data.source.updatedAt} />{state.data.source.stale ? " · stale" : ""}</span></div>
    {state.error && <div className="coverage-warning">Refresh failed; showing last-known markets. {state.error}</div>}
    {state.data.warnings.length > 0 && <div className="coverage-warning">{state.data.warnings.join(" ")}</div>}
    <div className="table-wrap market-metrics-table"><table style={{ minWidth: Math.max(360, visibleColumns.length * 150 + 180) }}><thead><tr>{visibleColumns.map(column => <SortableHeader key={column.id} kind="markets" column={column} sort={marketSort} dragRef={columnDragRef} onSort={onMarketSort} onReorder={onColumnReorder} />)}</tr></thead><tbody>
      {visibleMarkets.map(market => {
        const key = `${run.id}:${market.ticker}`;
        const open = expandedMarkets.has(key);
        return <Fragment key={market.ticker}>
          <tr className="expandable-position" onClick={() => onToggle(market.ticker, open)}>{visibleColumns.map(column => <td key={column.id}>{column.render(market)}</td>)}</tr>
          {open && <tr className="metrics-nested-row"><td colSpan={visibleColumns.length}><MarketActivity state={activity[key]} horizonMs={horizonMs} columnPreferences={columnPreferences} filters={filters} fillSort={fillSort} orderSort={orderSort} columnDragRef={columnDragRef} onFillSort={onFillSort} onOrderSort={onOrderSort} onColumnReorder={onColumnReorder} /></td></tr>}
        </Fragment>;
      })}
    </tbody></table>{visibleMarkets.length === 0 && <p className="empty">No markets match the current filters.</p>}</div>
  </div>;
}

function MarketActivity({ state, horizonMs, columnPreferences, filters, fillSort, orderSort, columnDragRef, onFillSort, onOrderSort, onColumnReorder }: {
  state?: LoadState<RunMarketActivityResponse>;
  horizonMs: number;
  columnPreferences: ColumnPreferences;
  filters: ParsedFilters;
  fillSort: SortState;
  orderSort: SortState;
  columnDragRef: ColumnDragRef;
  onFillSort: (column: ColumnDefinition<RunFillActivity>) => void;
  onOrderSort: (column: ColumnDefinition<RunOrderRevision>) => void;
  onColumnReorder: ColumnReorder;
}) {
  if (state?.loading && !state.data) return <p className="empty">Loading fills and orders…</p>;
  if (state?.error && !state.data) return <p className="error">{state.error}</p>;
  if (!state?.data) return null;
  const { fills, orders, warnings } = state.data;
  const fillColumns: ColumnDefinition<RunFillActivity>[] = [
    { id: "fillTime", header: "Fill time", initialDirection: "desc", sortValue: fill => fill.filledAtMs, render: fill => <Time value={fill.filledAtMs} /> },
    { id: "fillId", header: "Fill ID", initialDirection: "asc", sortValue: fill => fill.fillId, render: fill => <span className="mono">{fill.fillId}</span> },
    { id: "side", header: "Side", initialDirection: "asc", sortValue: fill => fill.side, render: fill => <Side value={fill.side} /> },
    { id: "contracts", header: "Contracts", initialDirection: "desc", sortValue: fill => fill.contractsUnits, render: fill => <>{contractUnits(fill.contractsUnits)}<small>{contractUnits(fill.matchedContractsUnits)} matched · {contractUnits(fill.openContractsUnits)} open</small></> },
    { id: "timeToFill", header: "Time to fill", initialDirection: "desc", sortValue: fill => fill.timeToFillMs, render: fill => duration(fill.timeToFillMs) },
    { id: "totalPaid", header: "Total paid", initialDirection: "desc", sortValue: fill => fill.totalPaidUnits, render: fill => moneyUnits(fill.totalPaidUnits) },
    { id: "signedMarkout", header: "Signed markout", initialDirection: "desc", sortValue: fill => fill.markoutsByHorizon?.[String(horizonMs)]?.signedMarkoutPriceUnits, render: fill => priceUnits(fill.markoutsByHorizon?.[String(horizonMs)]?.signedMarkoutPriceUnits) },
    { id: "futureMidpoint", header: "Future midpoint", initialDirection: "desc", sortValue: fill => fill.markoutsByHorizon?.[String(horizonMs)]?.futureMidYesUnits, render: fill => priceUnits(fill.markoutsByHorizon?.[String(horizonMs)]?.futureMidYesUnits) },
    { id: "fee", header: "Fee", initialDirection: "desc", sortValue: fill => fill.markoutsByHorizon?.[String(horizonMs)]?.feeUnits, render: fill => moneyUnits(fill.markoutsByHorizon?.[String(horizonMs)]?.feeUnits) },
    { id: "netMarkout", header: `${horizonMs / 1000}s net markout`, initialDirection: "desc", sortValue: fill => fill.markoutsByHorizon?.[String(horizonMs)]?.netMarkoutUnits, render: fill => { const value = fill.markoutsByHorizon?.[String(horizonMs)]?.netMarkoutUnits; return <span className={value == null ? "muted" : value < 0 ? "negative" : value > 0 ? "positive" : ""}>{moneyUnits(value, true)}</span>; } },
  ];
  const orderColumns: ColumnDefinition<RunOrderRevision>[] = [
    { id: "orderTime", header: "Order time", initialDirection: "desc", sortValue: order => order.placedAtMs, render: order => <Time value={order.placedAtMs} /> },
    { id: "orderId", header: "Order ID", initialDirection: "asc", sortValue: order => order.orderId, render: order => <span className="mono">{order.orderId ?? "Unavailable"}</span> },
    { id: "side", header: "Side", initialDirection: "asc", sortValue: order => order.side, render: order => <Side value={order.side} /> },
    { id: "contracts", header: "Contracts", initialDirection: "desc", sortValue: order => order.contractsUnits, render: order => contractUnits(order.contractsUnits) },
    { id: "timeOnBook", header: "Time on book", initialDirection: "desc", sortValue: order => order.timeOnBookMs, render: order => duration(order.timeOnBookMs) },
    { id: "book", header: "Bid / ask / mid", initialDirection: "desc", sortValue: bookSortValue, render: order => `${priceUnits(order.bookBidPriceUnits)} / ${priceUnits(order.bookAskPriceUnits)} / ${priceUnits(order.bookMidPriceUnits)}` },
    { id: "orderPrice", header: "Order price", initialDirection: "desc", sortValue: order => order.orderPriceUnits, render: order => priceUnits(order.orderPriceUnits) },
    { id: "latestState", header: "Latest state", initialDirection: "asc", sortValue: order => order.endedState, render: order => <StatusBadge value={order.endedState} /> },
  ];
  const visibleFillColumns = enabledColumns(fillColumns, columnPreferences.fills);
  const visibleOrderColumns = enabledColumns(orderColumns, columnPreferences.orders);
  const visibleFills = sortRows(fills.items.filter(fill => fillMatchesFilters(fill, filters, horizonMs)), fillColumns, fillSort, fill => `${fill.filledAtMs}:${fill.fillId}`);
  const visibleOrders = sortRows(orders.items.filter(order => orderMatchesFilters(order, filters)), orderColumns, orderSort, order => `${order.placedAtMs}:${order.revisionKey}`);
  const fillCount = visibleFills.length !== fills.items.length
    ? `Showing ${visibleFills.length} of ${fills.items.length} loaded${fills.truncated ? ` · ${fills.totalCount} total` : ""}`
    : fills.truncated ? `Newest ${fills.items.length} of ${fills.totalCount}` : `${fills.totalCount} total`;
  const orderCount = visibleOrders.length !== orders.items.length
    ? `Showing ${visibleOrders.length} of ${orders.items.length} loaded${orders.truncated ? ` · ${orders.totalCount} total` : ""}`
    : orders.truncated ? `Newest ${orders.items.length} of ${orders.totalCount}` : `${orders.totalCount} total`;
  return <div className="market-activity">
    {state.error && <div className="coverage-warning">Refresh failed; showing last-known fills and orders. {state.error}</div>}
    {warnings.length > 0 && <div className="coverage-warning">{warnings.join(" ")}</div>}
    <div className="nested-heading"><strong>Fills</strong><span>{fillCount}</span></div>
    {visibleFillColumns.length === 0
      ? <p className="empty column-empty">No columns enabled for Fills. Use Columns at the top of the page to enable one.</p>
      : <div className="table-wrap activity-table"><table style={{ minWidth: Math.max(420, visibleFillColumns.length * 155) }}><thead><tr>{visibleFillColumns.map(column => <SortableHeader key={column.id} kind="fills" column={column} sort={fillSort} dragRef={columnDragRef} onSort={onFillSort} onReorder={onColumnReorder} />)}</tr></thead><tbody>{visibleFills.map(fill => <tr key={`${fill.fillId}:${fill.filledAtMs}`}>{visibleFillColumns.map(column => <td key={column.id}>{column.render(fill)}</td>)}</tr>)}</tbody></table>{visibleFills.length === 0 && <p className="empty">{fills.items.length ? "No fills match the current filters." : "No fills recorded."}</p>}</div>}
    <div className="nested-heading orders-heading"><strong>Orders</strong><span>{orderCount}</span></div>
    {visibleOrderColumns.length === 0
      ? <p className="empty column-empty">No columns enabled for Orders. Use Columns at the top of the page to enable one.</p>
      : <div className="table-wrap activity-table"><table style={{ minWidth: Math.max(420, visibleOrderColumns.length * 155) }}><thead><tr>{visibleOrderColumns.map(column => <SortableHeader key={column.id} kind="orders" column={column} sort={orderSort} dragRef={columnDragRef} onSort={onOrderSort} onReorder={onColumnReorder} />)}</tr></thead><tbody>{visibleOrders.map(order => <tr key={order.revisionKey}>{visibleOrderColumns.map(column => <td key={column.id}>{column.render(order)}</td>)}</tr>)}</tbody></table>{visibleOrders.length === 0 && <p className="empty">{orders.items.length ? "No orders match the current filters." : warnings.length ? "Detailed orders unavailable for this run." : "No order attempts recorded."}</p>}</div>}
  </div>;
}
