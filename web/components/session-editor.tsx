"use client";

import { useMemo, useState } from "react";
import type { SavedSession, SessionConfiguration } from "@/lib/types";

type Props = { initial: SavedSession[]; activeRun?: { id: string; sessionId: string; sessionName: string; status: string } | null };

function clone<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function humanize(value: string) { return value.replace(/([A-Z])/g, " $1").replace(/_/g, " ").replace(/^./, c => c.toUpperCase()); }

export function SessionEditor({ initial, activeRun }: Props) {
  const [sessions, setSessions] = useState(initial);
  const [currentId, setCurrentId] = useState(initial.find(item => item.selected)?.id ?? initial[0]?.id);
  const current = sessions.find(item => item.id === currentId) ?? sessions[0];
  const [draft, setDraft] = useState<SavedSession | undefined>(current ? clone(current) : undefined);
  const [search, setSearch] = useState("");
  const [message, setMessage] = useState<string>();
  const [pending, setPending] = useState(false);
  const dirty = Boolean(current && draft && JSON.stringify(current) !== JSON.stringify(draft));

  function choose(id: string) {
    const item = sessions.find(session => session.id === id);
    if (!item || (dirty && !window.confirm("Discard unsaved session changes?"))) return;
    setCurrentId(id); setDraft(clone(item)); setMessage(undefined);
  }
  function replace(item: SavedSession) {
    setSessions(items => items.map(existing => existing.id === item.id ? item : existing));
    setDraft(clone(item)); setCurrentId(item.id);
  }
  async function mutate(path: string, method: string, body?: object) {
    setPending(true); setMessage(undefined);
    try {
      const response = await fetch(`/api/backend/api/v1/${path}`, {
        method, headers: { "content-type": "application/json", "x-request-id": crypto.randomUUID() },
        body: body === undefined ? undefined : JSON.stringify(body)
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message ?? `Request failed (${response.status})`);
      return result.item as SavedSession;
    } catch (error) { setMessage(error instanceof Error ? error.message : "Request failed"); }
    finally { setPending(false); }
  }
  async function save() {
    if (!draft) return;
    const item = await mutate(`sessions/${draft.id}`, "PUT", { name: draft.name, description: draft.description, configuration: draft.configuration, version: draft.version });
    if (item) { replace(item); setMessage("Session saved. Changes apply to the next run."); }
  }
  async function create() {
    const name = window.prompt("Name for the new session", "New Session")?.trim();
    if (!name || !draft) return;
    const item = await mutate("sessions", "POST", { name, description: "", configuration: draft.configuration });
    if (item) { setSessions(items => [item, ...items]); setCurrentId(item.id); setDraft(clone(item)); setMessage("Session created."); }
  }
  async function select() {
    if (!draft) return;
    const item = await mutate(`sessions/${draft.id}/select`, "POST", {});
    if (item) {
      setSessions(items => items.map(existing => ({ ...existing, selected: existing.id === item.id })));
      setDraft(clone(item)); setMessage("Session selected for the next launcher start.");
    }
  }
  async function archive() {
    if (!draft || !window.confirm(`Archive ${draft.name}? Historical runs will be retained.`)) return;
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
  const visibleSections = useMemo(() => {
    if (!draft) return [];
    const needle = search.trim().toLowerCase();
    return (["execution", "launcher", "watchdog", "bot"] as const).map(section => ({
      section,
      entries: Object.entries(draft.configuration[section]).filter(([key]) => !needle || humanize(key).toLowerCase().includes(needle))
    })).filter(group => group.entries.length);
  }, [draft, search]);

  if (!draft) return <p className="empty">No sessions are available.</p>;
  return <>
    {activeRun && <section className="warning-panel"><strong>Run active: {activeRun.sessionName}</strong><p>Run {activeRun.id} keeps its immutable snapshot. Edits below apply only to the next start, and selection is locked.</p></section>}
    <section className="panel session-toolbar">
      <label>Saved session<select value={draft.id} onChange={event => choose(event.target.value)}>{sessions.map(item => <option key={item.id} value={item.id}>{item.name}{item.archivedAt ? " (archived)" : item.selected ? " (selected)" : ""}</option>)}</select></label>
      <div className="button-row"><button className="button" onClick={create} disabled={pending}>Create copy</button><button className="button primary" onClick={select} disabled={pending || draft.selected || Boolean(draft.archivedAt) || Boolean(activeRun)}>Select</button>{draft.archivedAt ? <button className="button" onClick={restore} disabled={pending}>Restore</button> : <button className="button danger" onClick={archive} disabled={pending || activeRun?.sessionId === draft.id}>Archive</button>}</div>
    </section>
    <section className="panel session-identity"><label>Name<input value={draft.name} onChange={event => updateIdentity("name", event.target.value)} maxLength={80} /></label><label>Description<textarea value={draft.description} onChange={event => updateIdentity("description", event.target.value)} maxLength={500} /></label><small>Configuration version {draft.version} · {draft.runCount} historical runs · {draft.selected ? "selected for next start" : "not selected"}</small></section>
    <section className="filters"><label>Find a setting<input value={search} onChange={event => setSearch(event.target.value)} placeholder="risk, watchdog, budget…" /></label></section>
    <div className="settings-groups">{visibleSections.map(({ section, entries }) => <section className="panel" key={section}><div className="panel-heading"><div><span className="eyebrow">CONFIGURATION</span><h2>{humanize(section)}</h2></div><strong>{entries.length} settings</strong></div><div className="settings-grid">{entries.map(([key, value]) => <label key={key}><span>{humanize(key)}</span>{typeof value === "boolean" ? <input type="checkbox" checked={value} onChange={event => updateField(section, key, event.target.checked)} /> : Array.isArray(value) ? <input value={value.join(", ")} onChange={event => { const parts = event.target.value.split(",").map(item => item.trim()).filter(Boolean); updateField(section, key, value.length && typeof value[0] === "number" ? parts.map(Number) : parts); }} /> : typeof value === "number" ? <input type="number" step="any" value={value} onChange={event => updateField(section, key, Number(event.target.value))} /> : <input value={String(value)} onChange={event => updateField(section, key, event.target.value)} />}</label>)}</div></section>)}</div>
    <section className="sticky-save"><span>{dirty ? "Unsaved changes" : "Saved"}</span><button className="button primary" onClick={save} disabled={pending || !dirty}>{pending ? "Working…" : "Save session"}</button>{message && <span role="status">{message}</span>}</section>
  </>;
}
