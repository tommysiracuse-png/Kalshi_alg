"use client";

import { useState } from "react";

export function ControlPanel({ ticker, disabled = false }: { ticker?: string; disabled?: boolean }) {
  const [pending, setPending] = useState<string>();
  const [message, setMessage] = useState<string>();
  const actions = ticker ? [disabled ? "enable" : "disable"] : ["start", "stop", "refresh"];
  async function run(action: string) {
    const warning = ticker ? `${action} ${ticker}? Open inventory will be retained.` : `${action} the trading fleet? Open inventory will be retained.`;
    if (!window.confirm(warning)) return;
    setPending(action); setMessage(undefined);
    const path = ticker ? `api/v1/controls/markets/${encodeURIComponent(ticker)}/${action}` : `api/v1/controls/fleet/${action}`;
    const requestId = crypto.randomUUID();
    try {
      const response = await fetch(`/api/backend/${path}`, { method: "POST", headers: { "x-request-id": requestId, "content-type": "application/json" }, body: "{}" });
      const body = await response.json();
      if (!response.ok) throw new Error(body.message ?? `Control failed (${response.status})`);
      setMessage(`${action} completed · ${body.requestId ?? requestId}`);
      window.setTimeout(() => window.location.reload(), 700);
    } catch (error) { setMessage(error instanceof Error ? error.message : "Control failed"); }
    finally { setPending(undefined); }
  }
  return <section className="control-panel" aria-label="Trading controls">
    <div><span className="eyebrow">GUARDED CONTROLS</span><p>Stops cancel bot-owned quotes and retain inventory.</p></div>
    <div className="button-row">{actions.map(action => <button key={action} className={`button ${action === "start" || action === "enable" ? "primary" : action === "stop" || action === "disable" ? "danger" : ""}`} disabled={Boolean(pending)} onClick={() => run(action)}>{pending === action ? "Working…" : action[0].toUpperCase() + action.slice(1)}</button>)}</div>
    {message && <p className="control-result" role="status">{message}</p>}
  </section>;
}
