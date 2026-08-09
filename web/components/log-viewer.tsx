"use client";

import { useEffect, useState } from "react";

export function LogViewer({ ticker }: { ticker: string }) {
  const [source, setSource] = useState("bot");
  const [lines, setLines] = useState<string[]>([]);
  useEffect(() => {
    const events = new EventSource(`/api/backend/api/v1/logs/${encodeURIComponent(ticker)}/stream?source=${source}`);
    events.addEventListener("log", event => setLines(current => [...current.slice(-299), JSON.parse((event as MessageEvent).data)]));
    return () => events.close();
  }, [ticker, source]);
  return <section className="panel log-panel"><div className="panel-heading"><div><span className="eyebrow">LIVE OUTPUT</span><h2>Logs</h2></div><select aria-label="Log source" value={source} onChange={event => { setLines([]); setSource(event.target.value); }}><option value="bot">Bot</option><option value="watchdog">Watchdog</option></select></div><pre aria-live="polite">{lines.length ? lines.join("\n") : "Waiting for log output…"}</pre></section>;
}
