"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { PortfolioPosition, PortfolioPositionsAnalytics } from "@/lib/types";
import { Time } from "./status";

const STORAGE_KEY = "kalshi.portfolio.markets.columns.v1";
const STORAGE_VERSION = 1;

type ColumnId = "identity" | "sideContracts" | "totalCost" | "currentValue" | "liquidationValue" | "pnl" | "openOrders" | "lastTrade" | "activity" | "book";
type Preference = { id: ColumnId; enabled: boolean };
type Column = { id: ColumnId; label: string; render: (position: PortfolioPosition, data: PortfolioPositionsAnalytics) => React.ReactNode };

function money(value?: number | null, signed = false) {
  if (value == null) return "Unavailable";
  return `${signed && value > 0 ? "+" : ""}$${(value / 10_000).toFixed(2)}`;
}

function price(value?: number | null) {
  return value == null ? "Unavailable" : `${(value / 100).toFixed(2)}¢`;
}

function contracts(value?: number | null) {
  return value == null ? "Unavailable" : (value / 100).toFixed(2);
}

function PnlValue({ value }: { value?: number | null }) {
  return <span className={value == null ? "muted" : value < 0 ? "negative" : value > 0 ? "positive" : ""}>{money(value, true)}</span>;
}

const COLUMNS: Column[] = [
  {
    id: "identity", label: "Market",
    render: position => <div className="portfolio-market-identity">
      {position.marketUrl ? <a className="external-link" href={position.marketUrl} target="_blank" rel="noreferrer"><strong>{position.title || position.ticker}</strong> ↗</a> : <strong>{position.title || position.ticker}</strong>}
      <small className="mono">{position.ticker}</small>
      <small>{position.seriesTitle || "Description unavailable"}</small>
    </div>,
  },
  { id: "sideContracts", label: "Side / contracts", render: position => <><span className={`side side-${position.side}`}>{position.side.toUpperCase()}</span><small>{contracts(position.contractsUnits)} contracts</small></> },
  { id: "totalCost", label: "Total cost", render: position => money(position.costBasisUnits) },
  { id: "currentValue", label: "Current market value", render: position => <>{money(position.currentMarketValueUnits)}<small>Midpoint marked</small></> },
  { id: "liquidationValue", label: "Liquidation value", render: position => <>{money(position.liquidationValueUnits)}<small>Same-side bid</small></> },
  {
    id: "pnl", label: "Total P&L",
    render: position => <div className="portfolio-pnl-breakdown"><strong><PnlValue value={position.totalPnlUnits} /></strong><small>Realized <PnlValue value={position.netRealizedPnlUnits} /></small><small>Unrealized <PnlValue value={position.unrealizedPnlUnits} /></small></div>,
  },
  { id: "openOrders", label: "Open orders", render: position => position.openOrderCount },
  { id: "lastTrade", label: "Last trade", render: position => position.lastTradeAtMs == null ? <span className="muted">Unavailable</span> : <Time value={position.lastTradeAtMs} /> },
  { id: "activity", label: "Fills / orders", render: (position, data) => <>{position.totalFillCount ?? "Unavailable"} / {position.totalOrderCount ?? "Unavailable"}{data.coverage.startedAtMs != null && <small>Since <Time value={data.coverage.startedAtMs} /></small>}</> },
  { id: "book", label: "Bid / ask / mid", render: position => <>{price(position.bidPriceUnits)} / {price(position.askPriceUnits)}<small>{price(position.midPriceUnits)} midpoint</small></> },
];

function defaults(): Preference[] {
  return COLUMNS.map(column => ({ id: column.id, enabled: true }));
}

function sanitize(value: unknown): Preference[] {
  if (!value || typeof value !== "object") return defaults();
  const candidate = value as { version?: unknown; columns?: unknown };
  if (candidate.version !== STORAGE_VERSION || !Array.isArray(candidate.columns)) return defaults();
  const known = new Set(COLUMNS.map(column => column.id));
  const result: Preference[] = [];
  const seen = new Set<string>();
  for (const item of candidate.columns) {
    if (!item || typeof item !== "object") continue;
    const row = item as { id?: unknown; enabled?: unknown };
    if (typeof row.id !== "string" || !known.has(row.id as ColumnId) || seen.has(row.id)) continue;
    result.push({ id: row.id as ColumnId, enabled: typeof row.enabled === "boolean" ? row.enabled : true });
    seen.add(row.id);
  }
  for (const column of COLUMNS) if (!seen.has(column.id)) result.push({ id: column.id, enabled: true });
  return result;
}

export function PortfolioMarketsTable({ data }: { data: PortfolioPositionsAnalytics }) {
  const [preferences, setPreferences] = useState<Preference[]>(defaults);
  const [hydrated, setHydrated] = useState(false);
  const dragRef = useRef<ColumnId | null>(null);

  useEffect(() => {
    let next = defaults();
    try { next = sanitize(JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "null")); }
    catch { /* Browser storage may be unavailable. */ }
    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setPreferences(next);
      setHydrated(true);
    });
    return () => { cancelled = true; };
  }, []);
  useEffect(() => {
    if (!hydrated) return;
    try { window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ version: STORAGE_VERSION, columns: preferences })); }
    catch { /* Browser storage may be unavailable. */ }
  }, [hydrated, preferences]);

  const visible = useMemo(() => preferences
    .filter(preference => preference.enabled)
    .map(preference => COLUMNS.find(column => column.id === preference.id))
    .filter((column): column is Column => Boolean(column)), [preferences]);
  const positions = useMemo(() => data.items.filter(position => position.contractsUnits !== 0), [data.items]);

  const toggle = (id: ColumnId) => setPreferences(previous => previous.map(item => item.id === id ? { ...item, enabled: !item.enabled } : item));
  const move = (id: ColumnId, direction: -1 | 1) => setPreferences(previous => {
    const next = [...previous];
    const index = next.findIndex(item => item.id === id);
    const target = index + direction;
    if (index < 0 || target < 0 || target >= next.length) return previous;
    [next[index], next[target]] = [next[target], next[index]];
    return next;
  });
  const drop = (target: ColumnId) => {
    const source = dragRef.current;
    dragRef.current = null;
    if (!source || source === target) return;
    setPreferences(previous => {
      const next = [...previous];
      const sourceIndex = next.findIndex(item => item.id === source);
      const targetIndex = next.findIndex(item => item.id === target);
      if (sourceIndex < 0 || targetIndex < 0) return previous;
      const [item] = next.splice(sourceIndex, 1);
      next.splice(next.findIndex(column => column.id === target), 0, item);
      return next;
    });
  };

  return <section className="panel portfolio-markets-panel">
    <div className="panel-heading portfolio-markets-heading"><div><span className="eyebrow">CURRENT POSITIONS</span><h2>Markets</h2></div><div><strong>{positions.length} open markets</strong><details className="portfolio-market-columns"><summary className="button">Columns</summary><div>
      <p>Enable columns or move them left and right. Headers can also be dragged.</p>
      {preferences.map((preference, index) => {
        const column = COLUMNS.find(item => item.id === preference.id)!;
        return <div className="portfolio-market-column-option" key={preference.id}><label><input type="checkbox" checked={preference.enabled} disabled={!hydrated} onChange={() => toggle(preference.id)} aria-label={`Markets: ${column.label}`} />{column.label}</label><span><button type="button" aria-label={`Move ${column.label} left`} disabled={!hydrated || index === 0} onClick={() => move(preference.id, -1)}>←</button><button type="button" aria-label={`Move ${column.label} right`} disabled={!hydrated || index === preferences.length - 1} onClick={() => move(preference.id, 1)}>→</button></span></div>;
      })}
    </div></details></div></div>
    {!visible.length ? <p className="empty">No market columns are enabled. Use Columns to enable one.</p> : <div className="table-wrap portfolio-markets-table"><table style={{ minWidth: Math.max(720, visible.length * 165) }}><thead><tr>{visible.map(column => <th
      key={column.id}
      draggable={hydrated}
      className="portfolio-market-draggable"
      onDragStart={() => { dragRef.current = column.id; }}
      onDragOver={event => event.preventDefault()}
      onDrop={() => drop(column.id)}
      onDragEnd={() => { dragRef.current = null; }}
    ><span className="column-drag-grip" aria-hidden="true">⋮⋮</span>{column.label}</th>)}</tr></thead><tbody>{positions.map(position => <tr key={position.marketId}>{visible.map(column => <td key={column.id}>{column.render(position, data)}</td>)}</tr>)}</tbody></table>{!positions.length && <p className="empty">No current open positions.</p>}</div>}
  </section>;
}
