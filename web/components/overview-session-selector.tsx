"use client";

import { useState } from "react";
import type { SavedSession } from "@/lib/types";
import { createRequestId } from "../lib/request-id";

type ActiveRun = { id: string; sessionId: string; sessionName: string; status: string };

export function OverviewSessionSelector({
  initial,
  activeRun,
}: {
  initial: SavedSession[];
  activeRun?: ActiveRun | null;
}) {
  const [sessions, setSessions] = useState(initial);
  const selected = sessions.find(session => session.selected) ?? sessions[0];
  const [selectedId, setSelectedId] = useState(selected?.id ?? "");
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useState<string>();

  async function select(sessionId: string) {
    const previousId = selectedId;
    setSelectedId(sessionId);
    setPending(true);
    setMessage(undefined);
    try {
      const response = await fetch(`/api/backend/api/v1/sessions/${sessionId}/select`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-request-id": createRequestId() },
        body: "{}",
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message ?? result.detail ?? `Request failed (${response.status})`);
      const item = result.item as SavedSession;
      setSessions(items => items.map(session => ({ ...session, selected: session.id === item.id })));
      setSelectedId(item.id);
      setMessage(`${item.name} will be used for the next launcher start.`);
    } catch (error) {
      setSelectedId(previousId);
      setMessage(error instanceof Error ? error.message : "Session selection failed");
    } finally {
      setPending(false);
    }
  }

  return <section className="panel overview-session" aria-label="Launcher session">
    <div>
      <span className="eyebrow">NEXT LAUNCH</span>
      <h2>Session</h2>
      <p>{activeRun ? `Run ${activeRun.id.slice(0, 8)} is ${activeRun.status} with ${activeRun.sessionName}; selection is locked.` : "Choose the saved configuration for the next fleet start."}</p>
    </div>
    <label>
      <span>Session for next launcher start</span>
      <select value={selectedId} onChange={event => select(event.target.value)} disabled={pending || Boolean(activeRun)}>
        {sessions.map(session => <option key={session.id} value={session.id}>{session.name}{session.selected ? " (selected)" : ""}</option>)}
      </select>
    </label>
    {message && <p className="control-result" role="status">{message}</p>}
  </section>;
}
