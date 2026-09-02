"use client";

import { useEffect, useMemo, useState } from "react";
import { PREVIOUS_SESSION, buildCommandPreview, type OptimizerFormState } from "../lib/optimizer-command";
import type { OptimizerOptions, OptimizerParamDim, OptimizerParams } from "../lib/types";

type Props = {
  value: OptimizerFormState;
  onChange: (next: OptimizerFormState) => void;
  disabled: boolean;
};

const PRESET_LABELS: Array<{ key: string; label: string }> = [
  { key: "toxicity", label: "Toxicity group" },
  { key: "priceFloors", label: "Price floors" },
];

function formatCoverage(options?: OptimizerOptions): string {
  const recorder = options?.recorder;
  if (!recorder?.available) return "No recorded order-book coverage yet - start the recorder first.";
  const from = recorder.fromMs != null ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" }).format(recorder.fromMs) : "?";
  const to = recorder.toMs != null ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" }).format(recorder.toMs) : "?";
  return `${recorder.markets.toLocaleString("en-US")} markets · ${recorder.hours} recorded hour${recorder.hours === 1 ? "" : "s"} over ${recorder.days} day${recorder.days === 1 ? "" : "s"} (${from} – ${to} UTC)`;
}

function formatBound(dim: OptimizerParamDim): string {
  if (dim.kind === "bool") return "bool";
  const format = (value: number) => Number.isInteger(value) ? String(value) : value.toPrecision(3);
  return `${format(dim.low)} … ${format(dim.high)} (default ${String(dim.default)})`;
}

export function OptimizationStartForm({ value, onChange, disabled }: Props) {
  const [options, setOptions] = useState<OptimizerOptions>();
  const [params, setParams] = useState<OptimizerParams>();
  const [loadError, setLoadError] = useState<string>();
  const [paramSearch, setParamSearch] = useState("");

  const set = (patch: Partial<OptimizerFormState>) => onChange({ ...value, ...patch });

  // Options (classes, sessions, tiers, recorder coverage) once on mount.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch("/api/backend/api/v1/optimizer/options", { cache: "no-store" });
        if (!response.ok) throw new Error(`options failed (${response.status})`);
        const body: OptimizerOptions = await response.json();
        if (!cancelled) setOptions(body);
      } catch (error) {
        if (!cancelled) setLoadError(error instanceof Error ? error.message : "options failed");
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Searchable fields depend on the tier (Tier 2 unpins the depth/queue group).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch(`/api/backend/api/v1/optimizer/params?tier=${value.tier}`, { cache: "no-store" });
        if (!response.ok) throw new Error(`params failed (${response.status})`);
        const body: OptimizerParams = await response.json();
        if (!cancelled) setParams(body);
      } catch (error) {
        if (!cancelled) setLoadError(error instanceof Error ? error.message : "params failed");
      }
    })();
    return () => { cancelled = true; };
  }, [value.tier]);

  const tier2 = options?.tiers.find(tier => tier.tier === 2);
  const tier2Available = Boolean(tier2?.available);

  // Tier 2 without a recording cannot run: fall back to Tier 1 once we know.
  useEffect(() => {
    if (options && value.tier === 2 && !tier2Available) set({ tier: 1 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [options, tier2Available]);

  // Fields pinned at the newly selected tier drop out of the selection.
  useEffect(() => {
    if (!params || params.tier !== value.tier) return;
    const searchable = new Set(params.searchable.map(dim => dim.name));
    const kept = value.onlyParams.filter(name => searchable.has(name));
    if (kept.length !== value.onlyParams.length) set({ onlyParams: kept });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params]);

  const grouped = useMemo(() => {
    const needle = paramSearch.trim().toLowerCase();
    const groups = new Map<string, OptimizerParamDim[]>();
    for (const dim of params?.searchable ?? []) {
      if (needle && !dim.name.toLowerCase().includes(needle)) continue;
      const list = groups.get(dim.group) ?? [];
      list.push(dim);
      groups.set(dim.group, list);
    }
    const order = params?.groups ?? [];
    return [...groups.entries()].sort((a, b) => {
      const ia = order.indexOf(a[0]); const ib = order.indexOf(b[0]);
      return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib) || a[0].localeCompare(b[0]);
    });
  }, [params, paramSearch]);

  const preview = useMemo(() => buildCommandPreview(value, options), [value, options]);

  function toggleParam(name: string, checked: boolean) {
    const next = checked ? [...value.onlyParams.filter(item => item !== name), name] : value.onlyParams.filter(item => item !== name);
    set({ onlyParams: next });
  }

  function applyPreset(key: string) {
    const names = params?.presets?.[key] ?? options?.presets?.[key] ?? [];
    set({ onlyParams: [...names] });
  }

  const sessions = options?.sessions ?? [];
  const maxWorkers = options?.workers.max ?? 12;

  return <div className="optimizer-form">
    {loadError && <p className="error" role="alert">Form data unavailable: {loadError}. You can still start a run with the defaults.</p>}

    <div className="filters" role="radiogroup" aria-label="Data tier">
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <input type="radio" name="optimizer-tier" checked={value.tier === 1} onChange={() => set({ tier: 1 })} disabled={disabled} />
        Tier 1 — candles + trades from the history db
      </label>
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <input type="radio" name="optimizer-tier" checked={value.tier === 2} onChange={() => set({ tier: 2 })} disabled={disabled || !tier2Available} />
        Tier 2 — recorded order books (full depth, queue model)
      </label>
    </div>
    <p className="muted" style={{ marginTop: -8 }}>{options ? formatCoverage(options) : "Loading recorder coverage…"}</p>
    {value.tier === 2 && <div className="filters">
      <label>Record root<input value={value.recordRoot} onChange={event => set({ recordRoot: event.target.value })} disabled={disabled} /></label>
    </div>}

    <div className="filters">
      <label>Market class
        <select value={value.marketClass} onChange={event => set({ marketClass: event.target.value })} disabled={disabled}>
          <option value="">All classes (base bot section)</option>
          {(options?.classes ?? []).map(name => <option key={name} value={name}>{name}</option>)}
        </select>
      </label>
      <label>Base session
        <select value={value.baseSession} onChange={event => set({ baseSession: event.target.value })} disabled={disabled}>
          <option value="">None (defaults / last winner)</option>
          {sessions.map(session => <option key={session.id} value={session.name}>{session.name}{session.selected ? " (selected)" : ""}</option>)}
          <option value={PREVIOUS_SESSION}>{PREVIOUS_SESSION} — session written back by the run before this one</option>
        </select>
      </label>
      <label>Screener session
        <select value={value.screenerSession} onChange={event => set({ screenerSession: event.target.value })} disabled={disabled || !value.screenerFilter}>
          <option value="">Same as base session{value.baseSession ? "" : " (config defaults)"}</option>
          {sessions.map(session => <option key={session.id} value={session.name}>{session.name}{session.hasScreener ? "" : " (no screener section)"}</option>)}
          <option value={PREVIOUS_SESSION}>{PREVIOUS_SESSION}</option>
        </select>
      </label>
    </div>
    <div className="filters">
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <input type="checkbox" checked={value.screenerFilter} onChange={event => set({ screenerFilter: event.target.checked })} disabled={disabled} />
        Screener filter (only markets the live screener would have passed)
      </label>
      <label>Screener re-evaluation (hours)
        <input type="number" min={0} max={168} step={0.5} placeholder="default" value={value.screenerEvalHours} onChange={event => set({ screenerEvalHours: event.target.value })} disabled={disabled || !value.screenerFilter} />
      </label>
    </div>

    <div className="chip-editor" role="group" aria-label="Only these parameters">
      <span className="muted">Only these parameters (targeted run; empty = full search, top-params applies)</span>
      <div className="button-row" style={{ margin: "6px 0" }}>
        {PRESET_LABELS.map(preset => <button key={preset.key} type="button" className="button" onClick={() => applyPreset(preset.key)} disabled={disabled || !params}>{preset.label}</button>)}
        <button type="button" className="button" onClick={() => set({ onlyParams: [] })} disabled={disabled || value.onlyParams.length === 0}>Clear</button>
      </div>
      <div className="chip-list" aria-label="Selected parameters">
        {value.onlyParams.length
          ? value.onlyParams.map(name => <span key={name} className="chip">{name}<button type="button" aria-label={`Remove ${name}`} onClick={() => toggleParam(name, false)} disabled={disabled}>×</button></span>)
          : <span className="empty">No restriction: every searchable field is screened and the top {value.topParams || 25} are searched.</span>}
      </div>
      <div className="chip-input">
        <input aria-label="Search parameters" placeholder="filter searchable fields…" value={paramSearch} onChange={event => setParamSearch(event.target.value)} style={{ textTransform: "none" }} />
      </div>
      <div className="metrics-column-list" style={{ maxHeight: 260, overflow: "auto", marginTop: 8, padding: 4 }}>
        {!params && <span className="muted">Loading searchable fields…</span>}
        {params && grouped.length === 0 && <span className="muted">No searchable field matches.</span>}
        {grouped.map(([group, dims]) => <section key={group}>
          <strong style={{ display: "block", margin: "8px 0 4px", color: "var(--green)", fontSize: 11, letterSpacing: ".12em" }}>{group.toUpperCase()}</strong>
          {dims.map(dim => <div key={dim.name} className="metrics-column-option">
            <label>
              <input type="checkbox" checked={value.onlyParams.includes(dim.name)} onChange={event => toggleParam(dim.name, event.target.checked)} disabled={disabled} />
              <span className="mono">{dim.name}</span>
            </label>
            <span className="muted" style={{ fontSize: 11 }}>{formatBound(dim)}</span>
          </div>)}
        </section>)}
        {params && params.pinned.length > 0 && <details style={{ marginTop: 8 }}>
          <summary className="muted" style={{ cursor: "pointer" }}>{params.pinned.length} pinned field{params.pinned.length === 1 ? "" : "s"} at tier {params.tier}</summary>
          {params.pinned.map(item => <div key={item.name} className="metrics-column-option"><span className="mono">{item.name}</span><span className="muted" style={{ fontSize: 11 }}>{item.reason}</span></div>)}
        </details>}
      </div>
    </div>

    <div className="filters">
      <label>Candidates<input type="number" min={1} max={500} value={value.candidates} onChange={event => set({ candidates: event.target.value })} disabled={disabled} /></label>
      <label>Markets<input type="number" min={1} max={200} value={value.markets} onChange={event => set({ markets: event.target.value })} disabled={disabled} /></label>
      <label>Top params<input type="number" min={1} max={100} value={value.topParams} onChange={event => set({ topParams: event.target.value })} disabled={disabled || value.onlyParams.length > 0} /></label>
      <label>Workers (max {maxWorkers})<input type="number" min={1} max={maxWorkers} value={value.workers} onChange={event => set({ workers: event.target.value })} disabled={disabled} /></label>
    </div>
    <div className="filters">
      <label>Splits<input type="number" min={1} max={10} value={value.splits} onChange={event => set({ splits: event.target.value })} disabled={disabled} /></label>
      <label>Fill share<input type="number" min={0.05} max={1} step={0.05} value={value.fillShare} onChange={event => set({ fillShare: event.target.value })} disabled={disabled} /></label>
      <label>Seed<input type="number" min={0} placeholder="launch time" value={value.seed} onChange={event => set({ seed: event.target.value })} disabled={disabled} /></label>
      <label>Budget (minutes)<input type="number" min={1} max={600} value={value.budgetMinutes} onChange={event => set({ budgetMinutes: event.target.value })} disabled={disabled} /></label>
    </div>
    <div className="filters">
      <label>Data period
        <select value={value.dataPeriod} onChange={event => set({ dataPeriod: event.target.value })} disabled={disabled}>
          <option value="all">All</option>
          <option value="7">Last 7 days</option>
          <option value="14">Last 14 days</option>
          <option value="30">Last 30 days</option>
          <option value="custom">Custom</option>
        </select>
      </label>
      {value.dataPeriod === "custom" && <>
        <label>From<input type="date" value={value.fromDate} onChange={event => set({ fromDate: event.target.value })} disabled={disabled} /></label>
        <label>To<input type="date" value={value.toDate} onChange={event => set({ toDate: event.target.value })} disabled={disabled} /></label>
      </>}
    </div>
    <div className="filters">
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}><input type="checkbox" checked={value.writeBack} onChange={event => set({ writeBack: event.target.checked })} disabled={disabled} />Write winner back as a session</label>
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}><input type="checkbox" checked={value.useBaseParams} onChange={event => set({ useBaseParams: event.target.checked })} disabled={disabled || Boolean(value.baseSession)} />Seed with last winner (when no base session)</label>
      <label style={{ display: "flex", alignItems: "center", gap: 8 }}><input type="checkbox" checked={value.queueAfterCurrent} onChange={event => set({ queueAfterCurrent: event.target.checked })} />Queue after current run</label>
    </div>
    <p className="muted">Workers default to {options?.workers.default ?? 8} and are capped at {maxWorkers} — heavier parallel load destabilizes this machine. A base session seeds the search from that session&apos;s bot fields and supplies the unsearched fields at write-back; {PREVIOUS_SESSION} chains onto the session the previous run wrote back.</p>
    <pre className="mono" aria-label="Command line preview" style={{ margin: "0 0 12px", padding: 10, background: "#050c0b", border: "1px solid #1b302b", borderRadius: 8, whiteSpace: "pre-wrap", wordBreak: "break-all", color: "#b8d3cb" }}>{preview}</pre>
  </div>;
}
