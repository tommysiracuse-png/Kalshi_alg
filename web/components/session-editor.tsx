"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import type { BotClassName, BotClassOverride, BotClassesConfiguration, BotSettingValue, SavedSession, ScreenerConfiguration, ScreenerFilters, ScreenerMveFilter, ScreenerStatus, SessionConfiguration, VenueName } from "@/lib/types";
// Value imports stay relative: vitest resolves no "@/" alias (type imports are erased).
import { SCREENER_MARKOUT_HORIZONS } from "../lib/types";
import { createRequestId } from "../lib/request-id";

type Props = { initial: SavedSession[]; activeRun?: { id: string; sessionId: string; sessionName: string; status: string } | null };
// Inline replacements for window.confirm/window.prompt, which embedded
// browsers block: discard-unsaved, create-name, and archive confirmations.
type Dialog = { kind: "discard"; id: string } | { kind: "create"; name: string } | { kind: "archive" };
// Reports whether the numeric field at `path` currently holds a number.
type ValidityHandler = (path: string, valid: boolean) => void;

const BOT_CLASS_NAMES: BotClassName[] = ["thickCalm", "thinWide", "toxic", "default"];
const BOT_CLASS_HINTS: Record<BotClassName, string> = {
  thickCalm: "deep top level and tight spread",
  thinWide: "thin displayed depth or wide spread",
  toxic: "series net markout at or below the toxic threshold",
  default: "no classification evidence",
};
const NUMBER_REQUIRED = "Enter a number. This field is not saved while it is empty.";

function clone<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function humanize(value: string) { return value.replace(/([A-Z])/g, " $1").replace(/_/g, " ").replace(/^./, c => c.toUpperCase()); }

type NumberFieldProps = {
  path: string; label: string; value: number; step?: number | "any"; min?: number;
  onCommit: (next: number) => void; onValidity: ValidityHandler;
};

// Numeric input that never posts 0 for an emptied box: while the text does
// not parse to a finite number the draft keeps its previous value, the field
// is marked invalid with an inline message and the editor refuses to save.
// The label text is bound through aria-labelledby so the message can sit
// inside the same grid cell without changing the field's accessible name.
function NumberField({ path, label, value, step, min, onCommit, onValidity }: NumberFieldProps) {
  const labelId = useId();
  const [text, setText] = useState<string | null>(null);
  const invalid = text !== null;
  const validity = useRef(onValidity);
  useEffect(() => { validity.current = onValidity; });
  // A field removed while invalid (e.g. a deleted override row) must not
  // keep the save button locked.
  useEffect(() => () => validity.current(path, true), [path]);
  return <label>
    <span id={labelId}>{label}</span>
    <input type="number" aria-labelledby={labelId} aria-invalid={invalid || undefined} step={step ?? "any"} min={min} value={invalid ? text : value} onChange={event => {
      const raw = event.target.value;
      const next = Number(raw);
      if (raw.trim() === "" || !Number.isFinite(next)) { setText(raw); onValidity(path, false); return; }
      setText(null); onValidity(path, true); onCommit(next);
    }} />
    {invalid && <em className="field-error" role="alert">{NUMBER_REQUIRED}</em>}
  </label>;
}

type ClassesProps = { classes: BotClassesConfiguration; bot: SessionConfiguration["bot"]; onChange: (next: BotClassesConfiguration) => void; onValidity: ValidityHandler };

// Third editor level: per class an add/remove list of {field, value}
// overrides (field chosen from the session's bot settings, value input typed
// from that field's base value) plus the classifier thresholds.
function BotClassesEditor({ classes, bot, onChange, onValidity }: ClassesProps) {
  const [pendingField, setPendingField] = useState<Partial<Record<BotClassName, string>>>({});
  const fields = useMemo(() => Object.keys(bot).sort(), [bot]);
  function setOverrides(name: BotClassName, overrides: BotClassOverride[]) {
    const next = { ...classes }; next[name] = { overrides }; onChange(next);
  }
  function valueInput(name: BotClassName, field: string, value: BotSettingValue, set: (next: BotSettingValue) => void) {
    const base = bot[field];
    if (typeof base === "boolean") return <label key={field}><span>{humanize(field)}</span><input type="checkbox" checked={Boolean(value)} onChange={event => set(event.target.checked)} /></label>;
    if (Array.isArray(base)) return <label key={field}><span>{humanize(field)}</span><input value={Array.isArray(value) ? value.join(", ") : String(value)} onChange={event => { const parts = event.target.value.split(",").map(item => item.trim()).filter(Boolean); set(base.length && typeof base[0] === "number" ? parts.map(Number) : parts); }} /></label>;
    if (typeof base === "number") return <NumberField key={field} path={`botClasses.${name}.${field}`} label={humanize(field)} value={typeof value === "number" ? value : Number(value)} onCommit={set} onValidity={onValidity} />;
    return <label key={field}><span>{humanize(field)}</span><input value={String(value)} onChange={event => set(event.target.value)} /></label>;
  }
  return <section className="panel">
    <div className="panel-heading"><div><span className="eyebrow">CONFIGURATION</span><h2>Bot Classes</h2></div><label className="status"><input type="checkbox" checked={classes.enabled} onChange={event => onChange({ ...classes, enabled: event.target.checked })} />Classification enabled</label></div>
    <div className="settings-grid">{Object.entries(classes.classifier).map(([key, value]) => <NumberField key={key} path={`botClasses.classifier.${key}`} label={humanize(key)} value={value} onCommit={next => onChange({ ...classes, classifier: { ...classes.classifier, [key]: next } })} onValidity={onValidity} />)}</div>
    {BOT_CLASS_NAMES.map(name => {
      const overrides = classes[name]?.overrides ?? [];
      const used = new Set(overrides.map(item => item.field));
      const available = fields.filter(field => !used.has(field));
      const candidate = pendingField[name] || available[0] || "";
      return <div key={name} className="settings-grid" style={{ marginTop: 18 }}>
        <label style={{ gridColumn: "1 / -1", borderBottom: 0 }}><span>{humanize(name)} · {BOT_CLASS_HINTS[name]} · {overrides.length} override{overrides.length === 1 ? "" : "s"}</span></label>
        {overrides.map((item, index) => <div key={item.field} className="button-row" style={{ alignItems: "end" }}>{valueInput(name, item.field, item.value, next => setOverrides(name, overrides.map((row, position) => position === index ? { ...row, value: next } : row)))}<button type="button" className="button" onClick={() => setOverrides(name, overrides.filter((_, position) => position !== index))}>Remove</button></div>)}
        <label><span>Add override</span><div className="button-row"><select value={candidate} onChange={event => setPendingField({ ...pendingField, [name]: event.target.value })}>{available.map(field => <option key={field} value={field}>{humanize(field)}</option>)}</select><button type="button" className="button" disabled={!candidate} onClick={() => { setOverrides(name, [...overrides, { field: candidate, value: bot[candidate] }]); setPendingField({ ...pendingField, [name]: "" }); }}>Add</button></div></label>
      </div>;
    })}
  </section>;
}

// Schema v4 screener section. Mirrors session_config._validate_screener: the
// allowed status/MVE values and the recorded markout horizons are selects,
// numbers are numeric inputs grouped the way kalshi_screener_config.py groups
// its constants, booleans are toggles and the excluded ticker keywords are an
// editable chip list.
const SCREENER_STATUS_OPTIONS: ScreenerStatus[] = ["open", "unopened", "paused", "closed", "settled"];
const SCREENER_MVE_OPTIONS: Array<{ value: ScreenerMveFilter; label: string }> = [
  { value: "exclude", label: "Exclude multivariate (MVE) markets" },
  { value: "only", label: "Only multivariate (MVE) markets" },
  { value: "all", label: "All markets" },
  { value: "", label: "No MVE filter sent" },
];
const SCREENER_HORIZON_LABEL = "Markout horizon (seconds)";
type ScreenerNumberField = Exclude<keyof ScreenerFilters, "status" | "mveFilter" | "excludedTickerKeywords" | "markoutFilterEnabled" | "markoutFilterHorizonSeconds">;
const SCREENER_GROUPS: Array<{ title: string; fields: Array<{ key: ScreenerNumberField; label: string; step?: number; min?: number }> }> = [
  { title: "Market scan", fields: [
    { key: "maxMarketsToScan", label: "Default max markets to scan", step: 1, min: 1 },
    { key: "topN", label: "Top N exported (floored at Max Bots)", step: 1, min: 1 },
  ] },
  { title: "Liquidity and time filters", fields: [
    { key: "minSpreadCents", label: "Min spread (cents)", step: 1, min: 0 },
    { key: "maxSpreadCents", label: "Max spread (cents)", step: 1, min: 0 },
    { key: "minYesBidCents", label: "Min YES bid (cents)", step: 1, min: 0 },
    { key: "minNoBidCents", label: "Min NO bid (cents)", step: 1, min: 0 },
    { key: "minVol24h", label: "Min 24h volume (contracts)", min: 0 },
    { key: "minOpenInterest", label: "Min open interest (contracts)", min: 0 },
    { key: "minTimeToCloseHours", label: "Min time to close (hours)", min: 0 },
    { key: "maxTimeToCloseHours", label: "Max time to close (hours)", min: 0 },
  ] },
  { title: "Quote planning", fields: [
    { key: "targetEdgeCents", label: "Target edge (cents)", step: 1, min: 0 },
    { key: "quoteSize", label: "Quote size (contracts)", step: 1, min: 1 },
  ] },
  { title: "Markout toxic-series filter", fields: [
    { key: "markoutFilterNetThresholdCents", label: "Net threshold (cents per contract)" },
    { key: "markoutFilterTotalNetThresholdCents", label: "Total net threshold (cents)" },
    { key: "markoutFilterTickerMinFills", label: "Ticker min fills", step: 1, min: 0 },
    { key: "markoutFilterSeriesMinFills", label: "Series min fills", step: 1, min: 0 },
    { key: "markoutFilterLookbackDays", label: "Lookback (days)", min: 0 },
  ] },
];
const SCREENER_VENUES: VenueName[] = ["kalshi", "polymarket"];
const SCREENER_SEARCH_TERMS = ["screener", "status", "mve filter", "excluded ticker keywords", "markout filter enabled", "maximum markets per venue", "kalshi max markets to scan", "polymarket max markets to scan", SCREENER_HORIZON_LABEL,
  ...SCREENER_GROUPS.flatMap(group => [group.title, ...group.fields.map(field => field.label)])].map(term => term.toLowerCase());

type ScreenerProps = { screener: ScreenerConfiguration; onChange: (next: ScreenerConfiguration) => void; onValidity: ValidityHandler };

function ScreenerEditor({ screener, onChange, onValidity }: ScreenerProps) {
  const [keyword, setKeyword] = useState("");
  const horizonLabelId = useId();
  const values = (screener.general ?? screener) as ScreenerFilters;
  const venueLimit = (venue: VenueName) => {
    const override = screener.venues?.[venue]?.maxMarketsToScan;
    return typeof override === "number" && Number.isFinite(override) ? override : values.maxMarketsToScan;
  };
  const keywords = values.excludedTickerKeywords ?? [];
  const horizon = values.markoutFilterHorizonSeconds;
  const horizonRecorded = (SCREENER_MARKOUT_HORIZONS as readonly number[]).includes(horizon);
  const validity = useRef(onValidity);
  useEffect(() => { validity.current = onValidity; });
  // A stored horizon outside the recorded set (saved before the restriction)
  // is shown as-is but blocks saving until a recorded horizon is chosen.
  useEffect(() => { validity.current("screener.markoutFilterHorizonSeconds", horizonRecorded); }, [horizonRecorded]);
  useEffect(() => () => validity.current("screener.markoutFilterHorizonSeconds", true), []);
  function set<K extends keyof ScreenerConfiguration>(key: K, value: ScreenerConfiguration[K]) {
    if (screener.general) onChange({ ...screener, general: { ...(screener.general as ScreenerFilters), [key]: value } });
    else onChange({ ...screener, [key]: value });
  }
  function setVenueLimit(venue: VenueName, value: number) {
    // A legacy flat screener can still reach the editor before the API has
    // normalized it. Promote its fields into `general` before adding an
    // override, otherwise migration would mistake the new `venues` key for a
    // complete schema-v5 section and discard the flat values.
    const base = screener.general ? screener : { general: screener as ScreenerFilters };
    onChange({
      ...base,
      venues: {
        ...(screener.venues ?? {}),
        [venue]: { ...(screener.venues?.[venue] ?? {}), maxMarketsToScan: value },
      },
    } as ScreenerConfiguration);
  }
  function addKeyword() {
    const next = keyword.trim().toUpperCase();
    setKeyword("");
    if (!next || keywords.some(item => item.toUpperCase() === next)) return;
    set("excludedTickerKeywords", [...keywords, next]);
  }
  return <section className="panel" aria-labelledby="screener-heading">
    <div className="panel-heading"><div><span className="eyebrow">CONFIGURATION</span><h2 id="screener-heading">Screener</h2></div><label className="status"><input type="checkbox" checked={Boolean(values.markoutFilterEnabled)} onChange={event => set("markoutFilterEnabled", event.target.checked)} />Markout filter enabled</label></div>
    <p className="screener-note"><strong>Applies at the next fleet Start.</strong> A running fleet keeps the screener settings it launched with; defaults match kalshi_screener_config.py.</p>
    <div className="settings-grid">
      <label><span>Status</span><select value={values.status} onChange={event => set("status", event.target.value as ScreenerStatus)}>{SCREENER_STATUS_OPTIONS.map(value => <option key={value} value={value}>{value}</option>)}</select></label>
      <label><span>MVE filter</span><select value={values.mveFilter ?? ""} onChange={event => set("mveFilter", event.target.value as ScreenerMveFilter)}>{SCREENER_MVE_OPTIONS.map(option => <option key={option.value || "none"} value={option.value}>{option.label}</option>)}</select></label>
      <div className="settings-grid screener-group-wrap" style={{ gridColumn: "1 / -1" }}>
        <label className="screener-group"><span>Maximum markets per venue</span></label>
        <p className="screener-note" style={{ gridColumn: "1 / -1", margin: "-2px 0 4px" }}>Each venue limit overrides the default above. Leave a venue at the default to use the shared value.</p>
        {SCREENER_VENUES.map(venue => <NumberField key={venue} path={`screener.venues.${venue}.maxMarketsToScan`} label={`${humanize(venue)} max markets to scan`} value={venueLimit(venue)} step={1} min={1} onCommit={next => setVenueLimit(venue, next)} onValidity={onValidity} />)}
      </div>
      {SCREENER_GROUPS.map(group => <div key={group.title} className="settings-grid screener-group-wrap" style={{ gridColumn: "1 / -1" }}>
        <label className="screener-group"><span>{group.title}</span></label>
        {group.fields.map(field => <NumberField key={field.key} path={`screener.${field.key}`} label={field.label} step={field.step} min={field.min} value={values[field.key] as number} onCommit={next => set(field.key, next)} onValidity={onValidity} />)}
        {group.title === "Markout toxic-series filter" && <label>
          <span id={horizonLabelId}>{SCREENER_HORIZON_LABEL}</span>
          <select aria-labelledby={horizonLabelId} aria-invalid={!horizonRecorded || undefined} value={String(horizon)} onChange={event => set("markoutFilterHorizonSeconds", Number(event.target.value))}>
            {!horizonRecorded && <option value={String(horizon)} disabled>{horizon} (not a recorded horizon)</option>}
            {SCREENER_MARKOUT_HORIZONS.map(value => <option key={value} value={String(value)}>{value}</option>)}
          </select>
          {!horizonRecorded && <em className="field-error" role="alert">Choose one of the recorded horizons ({SCREENER_MARKOUT_HORIZONS.join(", ")} seconds).</em>}
        </label>}
      </div>)}
      <div className="chip-editor" role="group" aria-label="Excluded ticker keywords">
        <label className="screener-group"><span>Excluded ticker keywords</span></label>
        <div className="chip-list">{keywords.length ? keywords.map(item => <span key={item} className="chip">{item}<button type="button" aria-label={`Remove ${item}`} onClick={() => set("excludedTickerKeywords", keywords.filter(existing => existing !== item))}>×</button></span>) : <span className="empty">No keywords: every ticker is eligible.</span>}</div>
        <div className="chip-input"><input aria-label="Add excluded ticker keyword" placeholder="e.g. LOWT (case-insensitive substring of the ticker)" value={keyword} maxLength={40} onChange={event => setKeyword(event.target.value)} onKeyDown={event => { if (event.key === "Enter") { event.preventDefault(); addKeyword(); } }} /><button type="button" className="button" onClick={addKeyword} disabled={!keyword.trim()}>Add keyword</button></div>
      </div>
    </div>
  </section>;
}

export function SessionEditor({ initial, activeRun }: Props) {
  const [sessions, setSessions] = useState(initial);
  const [currentId, setCurrentId] = useState(initial.find(item => item.selected)?.id ?? initial[0]?.id);
  const current = sessions.find(item => item.id === currentId) ?? sessions[0];
  const [draft, setDraft] = useState<SavedSession | undefined>(current ? clone(current) : undefined);
  const [search, setSearch] = useState("");
  const [message, setMessage] = useState<string>();
  const [pending, setPending] = useState(false);
  const [dialog, setDialog] = useState<Dialog>();
  // Paths of numeric fields whose box is currently empty / not a number.
  const [invalid, setInvalid] = useState<Record<string, true>>({});
  const dirty = Boolean(current && draft && JSON.stringify(current) !== JSON.stringify(draft));
  const invalidCount = Object.keys(invalid).length;
  const venueValues = draft?.configuration.venues ?? (draft ? { kalshi: { enabled: true, priority: 100, maxBots: Number(draft.configuration.launcher.maxBots ?? 40), client: {} } } : undefined);
  const venueValid = Boolean(venueValues && Object.values(venueValues).some(item => item.enabled) && Object.values(venueValues).every(item => item.maxBots > 0 && item.maxBots <= Number(draft?.configuration.launcher.maxBots ?? 0) && item.priority >= 0));
  const onValidity = useCallback<ValidityHandler>((path, valid) => {
    setInvalid(previous => {
      if (valid) {
        if (!(path in previous)) return previous;
        const next = { ...previous }; delete next[path]; return next;
      }
      return previous[path] ? previous : { ...previous, [path]: true };
    });
  }, []);

  function choose(id: string) {
    const item = sessions.find(session => session.id === id);
    if (!item || item.id === currentId) return;
    if (dirty || invalidCount) { setDialog({ kind: "discard", id }); return; }
    setCurrentId(id); setDraft(clone(item)); setMessage(undefined); setInvalid({});
  }
  function discardAndChoose(id: string) {
    const item = sessions.find(session => session.id === id);
    setDialog(undefined);
    if (!item) return;
    setCurrentId(id); setDraft(clone(item)); setMessage(undefined); setInvalid({});
  }
  function replace(item: SavedSession) {
    setSessions(items => items.map(existing => existing.id === item.id ? item : existing));
    setDraft(clone(item)); setCurrentId(item.id); setInvalid({});
  }
  async function mutate(path: string, method: string, body?: object) {
    setPending(true); setMessage(undefined);
    try {
      const response = await fetch(`/api/backend/api/v1/${path}`, {
        method, headers: { "content-type": "application/json", "x-request-id": createRequestId() },
        body: body === undefined ? undefined : JSON.stringify(body)
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message ?? `Request failed (${response.status})`);
      return result.item as SavedSession;
    } catch (error) { setMessage(error instanceof Error ? error.message : "Request failed"); }
    finally { setPending(false); }
  }
  async function save() {
    if (!draft || invalidCount || !venueValid) return;
    const item = await mutate(`sessions/${draft.id}`, "PUT", { name: draft.name, description: draft.description, configuration: draft.configuration, version: draft.version });
    if (item) { replace(item); setMessage("Session saved. Changes apply to the next run."); }
  }
  async function create(rawName: string) {
    const name = rawName.trim();
    if (!name || !draft) return;
    setDialog(undefined);
    const item = await mutate("sessions", "POST", { name, description: "", configuration: draft.configuration });
    if (item) { setSessions(items => [item, ...items]); setCurrentId(item.id); setDraft(clone(item)); setInvalid({}); setMessage("Session created."); }
  }
  async function select() {
    if (!draft) return;
    const item = await mutate(`sessions/${draft.id}/select`, "POST", {});
    if (item) {
      setSessions(items => items.map(existing => ({ ...existing, selected: existing.id === item.id })));
      setDraft(clone(item)); setInvalid({}); setMessage("Session selected for the next launcher start.");
    }
  }
  async function archive() {
    if (!draft) return;
    setDialog(undefined);
    const item = await mutate(`sessions/${draft.id}`, "DELETE");
    if (item) { replace(item); setMessage("Session archived; historical data was retained."); }
  }
  async function restore() {
    if (!draft) return;
    const item = await mutate(`sessions/${draft.id}/restore`, "POST", {});
    if (item) { replace(item); setMessage("Session restored."); }
  }
  function updateIdentity(key: "name" | "description", value: string) {
    setDraft(item => item ? { ...item, [key]: value } : item);
  }
  function updateField(section: keyof SessionConfiguration, key: string, value: unknown) {
    setDraft(item => item ? { ...item, configuration: { ...item.configuration, [section]: { ...(item.configuration[section] as object), [key]: value } } } : item);
  }
  function updateBotClasses(next: BotClassesConfiguration) {
    setDraft(item => item ? { ...item, configuration: { ...item.configuration, botClasses: next } } : item);
  }
  function updateScreener(next: ScreenerConfiguration) {
    setDraft(item => item ? { ...item, configuration: { ...item.configuration, screener: next } } : item);
  }
  function updateVenue(name: "kalshi" | "polymarket", patch: Partial<import("@/lib/types").VenueConfiguration>) {
    setDraft(item => item ? {
      ...item,
      configuration: {
        ...item.configuration,
        venues: {
          ...(item.configuration.venues ?? {
            kalshi: { enabled: true, priority: 100, maxBots: Number(item.configuration.launcher.maxBots ?? 40), client: {} },
            polymarket: { enabled: false, priority: 100, maxBots: Number(item.configuration.launcher.maxBots ?? 40), client: {} },
          }),
          [name]: { ...(item.configuration.venues?.[name] ?? { enabled: false, priority: 100, maxBots: Number(item.configuration.launcher.maxBots ?? 40), client: {} }), ...patch },
        },
      },
    } : item);
  }
  const visibleSections = useMemo(() => {
    if (!draft) return [];
    const needle = search.trim().toLowerCase();
    return (["execution", "launcher", "fleetRuntime", "watchdog", "bot"] as const).filter(
      section => draft.configuration[section] !== undefined
    ).map(section => ({
      section,
      entries: Object.entries(draft.configuration[section] ?? {}).filter(([key]) => !needle || humanize(key).toLowerCase().includes(needle))
    })).filter(group => group.entries.length);
  }, [draft, search]);
  const botClasses = draft?.configuration.botClasses;
  const classesVisible = useMemo(() => {
    if (!botClasses) return false;
    const needle = search.trim().toLowerCase();
    return !needle || "bot classes".includes(needle)
      || BOT_CLASS_NAMES.some(name => humanize(name).toLowerCase().includes(needle))
      || Object.keys(botClasses.classifier ?? {}).some(key => humanize(key).toLowerCase().includes(needle));
  }, [botClasses, search]);
  const screener = draft?.configuration.screener;
  const screenerVisible = useMemo(() => {
    if (!screener) return false;
    const needle = search.trim().toLowerCase();
    return !needle || SCREENER_SEARCH_TERMS.some(term => term.includes(needle))
      || Object.keys(screener).some(key => humanize(key).toLowerCase().includes(needle));
  }, [screener, search]);

  if (!draft) return <p className="empty">No sessions are available.</p>;
  return <>
    {activeRun && <section className="warning-panel"><strong>Run active: {activeRun.sessionName}</strong><p>Run {activeRun.id} keeps its immutable snapshot. Edits below apply only to the next start, and selection is locked.</p></section>}
    <section className="panel session-toolbar">
      <label>Saved session<select value={draft.id} disabled={Boolean(dialog)} onChange={event => choose(event.target.value)}>{sessions.map(item => <option key={item.id} value={item.id}>{item.name}{item.archivedAt ? " (archived)" : item.selected ? " (selected)" : ""}</option>)}</select></label>
      {dialog?.kind === "create" ? (
        <div className="button-row">
          <label>New session name<input value={dialog.name} maxLength={80} autoFocus onChange={event => setDialog({ kind: "create", name: event.target.value })} onKeyDown={event => { if (event.key === "Enter") void create(dialog.name); }} /></label>
          <button className="button primary" onClick={() => create(dialog.name)} disabled={pending || !dialog.name.trim()}>Confirm</button>
          <button className="button" onClick={() => setDialog(undefined)} disabled={pending}>Cancel</button>
        </div>
      ) : dialog?.kind === "discard" ? (
        <div className="button-row">
          <span role="alert">Discard unsaved session changes?</span>
          <button className="button danger" onClick={() => discardAndChoose(dialog.id)} disabled={pending}>Discard</button>
          <button className="button" onClick={() => setDialog(undefined)} disabled={pending}>Cancel</button>
        </div>
      ) : dialog?.kind === "archive" ? (
        <div className="button-row">
          <span role="alert">Archive {draft.name}? Historical runs will be retained.</span>
          <button className="button danger" onClick={archive} disabled={pending}>Confirm archive</button>
          <button className="button" onClick={() => setDialog(undefined)} disabled={pending}>Cancel</button>
        </div>
      ) : (
        <div className="button-row"><button className="button" onClick={() => setDialog({ kind: "create", name: "New Session" })} disabled={pending}>Create copy</button><button className="button primary" onClick={select} disabled={pending || draft.selected || Boolean(draft.archivedAt) || Boolean(activeRun)}>Select</button>{draft.archivedAt ? <button className="button" onClick={restore} disabled={pending}>Restore</button> : <button className="button danger" onClick={() => setDialog({ kind: "archive" })} disabled={pending || activeRun?.sessionId === draft.id}>Archive</button>}</div>
      )}
    </section>
    <section className="panel session-identity"><label>Name<input value={draft.name} onChange={event => updateIdentity("name", event.target.value)} maxLength={80} /></label><label>Description<textarea value={draft.description} onChange={event => updateIdentity("description", event.target.value)} maxLength={500} /></label><small>Configuration version {draft.version} · {draft.runCount} historical runs · {draft.selected ? "selected for next start" : "not selected"}</small></section>
    <section className="panel" aria-labelledby="venues-heading"><div className="panel-heading"><div><span className="eyebrow">RUNTIME</span><h2 id="venues-heading">Venues</h2></div><strong>Select at least one</strong></div><div className="settings-grid">{(["kalshi", "polymarket"] as const).map(name => { const venue = draft.configuration.venues?.[name] ?? { enabled: name === "kalshi", priority: 100, maxBots: Number(draft.configuration.launcher.maxBots ?? 40), client: {} }; const client = venue.client ?? {}; const setClient = (patch: Record<string, unknown>) => updateVenue(name, { client: { ...client, ...patch } }); return <div key={name} className="panel"><label className="status"><input type="checkbox" checked={venue.enabled} onChange={event => updateVenue(name, { enabled: event.target.checked })} />Enable {name}</label><NumberField path={`venues.${name}.priority`} label="Priority" value={venue.priority} min={0} step={1} onCommit={next => updateVenue(name, { priority: next })} onValidity={onValidity} /><NumberField path={`venues.${name}.maxBots`} label="Max bots" value={venue.maxBots} min={1} step={1} onCommit={next => updateVenue(name, { maxBots: next })} onValidity={onValidity} />{name === "polymarket" && <><label><span>HTTP proxy URL</span><input value={String(client.proxy_url ?? "")} placeholder="http://127.0.0.1:18080" onChange={event => setClient({ proxy_url: event.target.value })} /></label><label><span>Scan mode</span><select value={String(client.scan_mode ?? "catalog_plus_cached_books")} onChange={event => setClient({ scan_mode: event.target.value })}><option value="catalog_only">Catalog only</option><option value="catalog_plus_cached_books">Catalog + cached books</option><option value="bootstrap_missing_books">Bootstrap missing books</option></select></label><label className="status"><input type="checkbox" checked={Boolean(client.mirror_enabled)} onChange={event => setClient({ mirror_enabled: event.target.checked })} />Use local market mirror</label><label className="status"><input type="checkbox" checked={Boolean(client.mirror_required_complete_snapshot)} onChange={event => setClient({ mirror_required_complete_snapshot: event.target.checked })} />Require complete fresh snapshot</label><NumberField path="venues.polymarket.client.mirror_snapshot_max_age_seconds" label="Mirror max age (seconds)" value={Number(client.mirror_snapshot_max_age_seconds ?? 60)} min={1} step={1} onCommit={next => setClient({ mirror_snapshot_max_age_seconds: next })} onValidity={onValidity} /><NumberField path="venues.polymarket.client.mirror_sync_interval_seconds" label="Mirror sync interval (seconds)" value={Number(client.mirror_sync_interval_seconds ?? 30)} min={1} step={1} onCommit={next => setClient({ mirror_sync_interval_seconds: next })} onValidity={onValidity} /><NumberField path="venues.polymarket.client.mirror_max_markets" label="Mirror market limit (0 = all)" value={Number(client.mirror_max_markets ?? 0)} min={0} step={1000} onCommit={next => setClient({ mirror_max_markets: next })} onValidity={onValidity} /><small>Credentials are read from environment/service configuration and are never stored here.</small></>}</div>; })}</div></section>
    <section className="filters"><label>Find a setting<input value={search} onChange={event => setSearch(event.target.value)} placeholder="risk, watchdog, budget…" /></label></section>
    <div className="settings-groups" key={draft.id}>{visibleSections.map(({ section, entries }) => <section className="panel" key={section}><div className="panel-heading"><div><span className="eyebrow">CONFIGURATION</span><h2>{humanize(section)}</h2></div><strong>{entries.length} settings</strong></div><div className="settings-grid">{entries.map(([key, value]) => typeof value === "number"
      ? <NumberField key={key} path={`${section}.${key}`} label={humanize(key)} value={value} onCommit={next => updateField(section, key, next)} onValidity={onValidity} />
      : <label key={key}><span>{humanize(key)}</span>{typeof value === "boolean" ? <input type="checkbox" checked={value} onChange={event => updateField(section, key, event.target.checked)} /> : Array.isArray(value) ? <input value={value.join(", ")} onChange={event => { const parts = event.target.value.split(",").map(item => item.trim()).filter(Boolean); updateField(section, key, value.length && typeof value[0] === "number" ? parts.map(Number) : parts); }} /> : <input value={String(value)} onChange={event => updateField(section, key, event.target.value)} />}</label>)}</div></section>)}{screener && screenerVisible && <ScreenerEditor screener={screener} onChange={updateScreener} onValidity={onValidity} />}{botClasses && classesVisible && <BotClassesEditor classes={botClasses} bot={draft.configuration.bot} onChange={updateBotClasses} onValidity={onValidity} />}</div>
    <section className="sticky-save"><span>{invalidCount ? `${invalidCount} invalid field${invalidCount === 1 ? "" : "s"}` : !venueValid ? "Enable at least one valid venue" : dirty ? "Unsaved changes" : "Saved"}</span><button className="button primary" onClick={save} disabled={pending || !dirty || invalidCount > 0 || !venueValid}>{pending ? "Working…" : "Save session"}</button>{invalidCount > 0 && <span role="status">Enter a number in every highlighted field before saving.</span>}{message && <span role="status">{message}</span>}</section>
  </>;
}
