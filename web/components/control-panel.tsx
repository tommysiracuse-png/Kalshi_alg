"use client";

import { useEffect, useState } from "react";
import { createRequestId } from "../lib/request-id";

function label(action: string) { return action[0].toUpperCase() + action.slice(1); }

export function ControlPanel({ ticker, disabled = false }: { ticker?: string; disabled?: boolean }) {
  const [pending, setPending] = useState<string>();
  const [message, setMessage] = useState<string>();
  // Two-click confirmation: the first click arms the action, the second
  // executes it. window.confirm is unavailable in embedded browsers.
  const [armed, setArmed] = useState<string>();
  // An armed action disarms itself after 5 s; re-arming or disarming clears
  // the pending timer through the effect cleanup.
  useEffect(() => {
    if (!armed) return;
    const timer = window.setTimeout(() => setArmed(undefined), 5000);
    return () => window.clearTimeout(timer);
  }, [armed]);
  const actions = ticker ? [disabled ? "enable" : "disable"] : ["start", "stop", "refresh"];
  function disarm() { setArmed(undefined); }
  function arm(action: string) { setArmed(action); setMessage(undefined); }
  async function run(action: string) {
    disarm();
    setPending(action); setMessage(undefined);
    try {
      const path = ticker ? `api/v1/controls/markets/${encodeURIComponent(ticker)}/${action}` : `api/v1/controls/fleet/${action}`;
      const requestId = createRequestId();
      const response = await fetch(`/api/backend/${path}`, { method: "POST", headers: { "x-request-id": requestId, "content-type": "application/json" }, body: "{}" });
      const body = await response.json();
      if (!response.ok) throw new Error(body.message ?? `Control failed (${response.status})`);
      setMessage(`${action} completed · ${body.requestId ?? requestId}`);
      window.setTimeout(() => window.location.reload(), 700);
    } catch (error) { setMessage(error instanceof Error ? error.message : "Control failed"); }
    finally { setPending(undefined); }
  }
  const buttonClass = (action: string) => `button ${action === "start" || action === "enable" ? "primary" : action === "stop" || action === "disable" ? "danger" : ""}`;
  return <section className="control-panel" aria-label="Trading controls">
    <div><span className="eyebrow">GUARDED CONTROLS</span><p>{armed
      ? (ticker ? `${label(armed)} ${ticker}? Open inventory will be retained.` : `${label(armed)} the trading fleet? Open inventory will be retained.`)
      : "Stops cancel bot-owned quotes and retain inventory."}</p></div>
    <div className="button-row">{armed
      ? <>
        <button className={buttonClass(armed)} disabled={Boolean(pending)} onClick={() => run(armed)}>{`Confirm ${armed}?`}</button>
        <button className="button" disabled={Boolean(pending)} onClick={disarm}>Cancel</button>
      </>
      : actions.map(action => <button key={action} className={buttonClass(action)} disabled={Boolean(pending)} onClick={() => arm(action)}>{pending === action ? "Working…" : label(action)}</button>)}</div>
    {message && <p className="control-result" role="status">{message}</p>}
  </section>;
}
