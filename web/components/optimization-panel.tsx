"use client";

import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { createRequestId } from "../lib/request-id";
import { DEFAULT_OPTIMIZER_FORM, buildStartPayload, type OptimizerFormState } from "../lib/optimizer-command";
import { StatusBadge, Time } from "./status";
import { OptimizationRunDetail } from "./optimization-run-detail";
import { OptimizationStartForm } from "./optimization-start-form";
import type { OptimizerDataAvailability, OptimizerQueueItem, OptimizerRun, OptimizerRunsResponse } from "../lib/types";

function argsSummary(run: OptimizerRun): string {
  const args = run.args ?? {};
  const parts = [
    `${args.candidates ?? "?"} candidates`,
    `${args.markets ?? run.marketCount} markets`,
    `${args.budgetMinutes ?? "?"} min budget`,
    `${args.workers ?? "?"} workers`,
  ];
  if (args.lastDays) parts.push(`last ${args.lastDays}d`);
  else if (args.fromDate || args.toDate) parts.push(`${args.fromDate ?? "…"} → ${args.toDate ?? "…"}`);
  if (args.writeBack) parts.push("write-back");
  if (args.baseParams) parts.push("seeded");
  return parts.join(" · ");
}

function onlyParamsSummary(run: OptimizerRun): { label: string; title: string } {
  const names = run.args?.onlyParams ?? [];
  if (!names.length) return { label: `all (top ${run.args?.topParams ?? "?"})`, title: "Full search: every searchable field screened" };
  return { label: `${names.length} field${names.length === 1 ? "" : "s"}`, title: names.join(", ") };
}

function screenerSummary(run: OptimizerRun): string {
  const args = run.args ?? {};
  if (args.screenerFilter === false) return "off";
  if (args.screenerSource) return args.screenerSource.replace(/^session /, "").replace(/ \(--(base|screener)-session\)$/, " ($1)");
  if (args.screenerSession) return args.screenerSession;
  if (args.screenerFilter === true) return "on";
  return "—";
}

function formatCount(value?: number | null): string {
  return typeof value === "number" ? value.toLocaleString("en-US") : "—";
}

// Recorded timestamps are UTC epoch milliseconds; render in UTC so the range
// matches the data window regardless of the operator's local timezone.
function formatDataRange(from?: number | null, to?: number | null, spanDays?: number | null): string {
  if (from == null || to == null) return "—";
  const day: Intl.DateTimeFormatOptions = { month: "short", day: "numeric", timeZone: "UTC" };
  const dayYear: Intl.DateTimeFormatOptions = { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" };
  const fromStr = new Intl.DateTimeFormat("en-US", day).format(from);
  const toStr = new Intl.DateTimeFormat("en-US", dayYear).format(to);
  const span = typeof spanDays === "number" ? ` (${spanDays.toFixed(1)} days)` : "";
  return `${fromStr} – ${toStr}${span}`;
}

function queueOptionsSummary(item: OptimizerQueueItem): string {
  const options = item.options ?? {};
  const parts: string[] = [`tier ${String(options.tier ?? 1)}`];
  if (options.marketClass) parts.push(`class ${String(options.marketClass)}`);
  const only = Array.isArray(options.onlyParams) ? options.onlyParams : [];
  if (only.length) parts.push(`${only.length} field${only.length === 1 ? "" : "s"}`);
  if (options.baseSession) parts.push(`base ${String(options.baseSession)}`);
  if (options.screenerSession) parts.push(`screener ${String(options.screenerSession)}`);
  if (options.candidates != null) parts.push(`${String(options.candidates)} candidates`);
  if (options.budgetMinutes != null) parts.push(`${String(options.budgetMinutes)} min`);
  return parts.join(" · ");
}

export function OptimizationPanel({ initial }: { initial: OptimizerRunsResponse }) {
  const [data, setData] = useState(initial);
  const [availability, setAvailability] = useState<OptimizerDataAvailability>();
  const [form, setForm] = useState<OptimizerFormState>(DEFAULT_OPTIMIZER_FORM);
  const [pending, setPending] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [expanded, setExpanded] = useState<string>();
  // Two-click confirmation: the first click arms the action, the second
  // executes it. window.confirm is unavailable in embedded browsers.
  const [armed, setArmed] = useState<"start" | "stop">();
  const disarmTimer = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(disarmTimer.current), []);

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/api/backend/api/v1/optimizer/runs", { cache: "no-store" });
      if (response.ok) setData(await response.json());
    } catch { /* keep the last snapshot; the next poll retries */ }
  }, []);

  // Recorded-data summary is static enough to fetch once on mount; a failure
  // just hides the card and never blocks launching a run.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch("/api/backend/api/v1/optimizer/data-availability", { cache: "no-store" });
        if (response.ok && !cancelled) setAvailability(await response.json());
      } catch { /* card stays hidden */ }
    })();
    return () => { cancelled = true; };
  }, []);

  const queue = data.queue ?? [];
  const queuedCount = queue.filter(item => item.status === "queued").length;

  // Poll the runs endpoint every 5s while an optimizer process is alive or a
  // queued launch is waiting, so the log tail, queue and run list stay fresh.
  useEffect(() => {
    if (!data.running && queuedCount === 0) return;
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, [data.running, queuedCount, refresh]);

  function disarm() {
    window.clearTimeout(disarmTimer.current);
    setArmed(undefined);
  }
  function arm(action: "start" | "stop") {
    window.clearTimeout(disarmTimer.current);
    setArmed(action); setMessage(undefined);
    disarmTimer.current = window.setTimeout(() => setArmed(undefined), 5000);
  }

  async function run(action: "start" | "stop") {
    disarm();
    setPending(action); setMessage(undefined);
    try {
      const requestId = createRequestId();
      const payload = action === "start" ? buildStartPayload(form) : {};
      const response = await fetch(`/api/backend/api/v1/controls/optimizer/${action}`, {
        method: "POST",
        headers: { "x-request-id": requestId, "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.message ?? `Control failed (${response.status})`);
      const result = body.result ?? {};
      if (action === "start" && result.queued) setMessage(`queued as ${result.queueId} (position ${result.position}) · ${body.requestId ?? requestId}`);
      else if (action === "start" && result.commandLine) setMessage(`start completed (pid ${result.pid}) · ${body.requestId ?? requestId}\n${result.commandLine}`);
      else setMessage(`${action} completed${result.queueCleared ? ` · ${result.queueCleared} queued item(s) cancelled` : ""} · ${body.requestId ?? requestId}`);
      await refresh();
    } catch (error) { setMessage(error instanceof Error ? error.message : "Control failed"); }
    finally { setPending(undefined); }
  }

  async function cancelQueued(queueId: string) {
    setPending(`cancel:${queueId}`); setMessage(undefined);
    try {
      const requestId = createRequestId();
      const response = await fetch(`/api/backend/api/v1/controls/optimizer/queue/${encodeURIComponent(queueId)}/cancel`, {
        method: "POST", headers: { "x-request-id": requestId, "content-type": "application/json" }, body: "{}",
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.message ?? `Cancel failed (${response.status})`);
      setMessage(`cancelled ${queueId} · ${body.requestId ?? requestId}`);
      await refresh();
    } catch (error) { setMessage(error instanceof Error ? error.message : "Cancel failed"); }
    finally { setPending(undefined); }
  }

  const startDisabled = data.running && !form.queueAfterCurrent;

  return <>
    <section className="panel log-panel" aria-label="Optimizer status">
      <div className="panel-heading">
        <div><span className="eyebrow">OPTIMIZER STATUS</span><h2>{data.running ? "Run in progress" : "Idle"}</h2></div>
        <StatusBadge value={data.running ? "running" : "stopped"} />
      </div>
      {data.running
        ? <pre aria-live="polite">{data.lastLogLines.length ? data.lastLogLines.join("\n") : "Waiting for optimizer output…"}</pre>
        : <p className="muted">No optimizer process is running. Start a run below; results append to the table when finished.</p>}
    </section>

    {availability?.available && <section className="panel" aria-label="Recorded data availability">
      <div className="panel-heading">
        <div><span className="eyebrow">RECORDED DATA</span><h2>Recorded data</h2></div>
        <span className="muted">Feasible bounds for markets and time range</span>
      </div>
      <div className="metrics compact" style={{ margin: 0 }}>
        <article>
          <span>Markets</span>
          <strong>{formatCount(availability.marketCount)}</strong>
          <p>{formatCount(availability.marketsWithTrades)} with trades · {formatCount(availability.settledCount)} settled</p>
        </article>
        <article>
          <span>Trades</span>
          <strong>{formatCount(availability.tradeCount)}</strong>
          <p>public prints recorded</p>
        </article>
        <article>
          <span>Candles</span>
          <strong>{formatCount(availability.candleCount)}</strong>
          <p>1-minute bars</p>
        </article>
        <article>
          <span>Date range</span>
          <strong className="small-value">{formatDataRange(availability.fromMs, availability.toMs, availability.spanDays)}</strong>
          <p>recorded history window</p>
        </article>
      </div>
    </section>}

    <section className="panel" aria-label="Start optimization run">
      <div className="panel-heading">
        <div><span className="eyebrow">GUARDED CONTROLS</span><h2>New optimization run</h2></div>
        {data.running && <span className="muted">A run is in progress — tick “Queue after current run” to chain another.</span>}
      </div>
      <OptimizationStartForm value={form} onChange={setForm} disabled={Boolean(pending) || startDisabled} />
      <div className="button-row">
        {armed
          ? <>
            <button className={`button ${armed === "start" ? "primary" : "danger"}`} disabled={Boolean(pending)} onClick={() => run(armed)}>{`Confirm ${armed}?`}</button>
            <button className="button" disabled={Boolean(pending)} onClick={disarm}>Cancel</button>
          </>
          : <>
            <button className="button primary" disabled={Boolean(pending) || startDisabled} onClick={() => arm("start")}>{pending === "start" ? "Working…" : form.queueAfterCurrent && data.running ? "Queue optimizer run" : "Start optimizer"}</button>
            {data.running && <button className="button danger" disabled={Boolean(pending)} onClick={() => arm("stop")}>{pending === "stop" ? "Working…" : "Stop optimizer"}</button>}
          </>}
      </div>
      {message && <p className="control-result" role="status" style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{message}</p>}
    </section>

    {queue.length > 0 && <section className="panel" aria-label="Optimizer queue">
      <div className="panel-heading">
        <div><span className="eyebrow">QUEUE</span><h2>Queued runs</h2></div>
        <span className="muted">{queuedCount} waiting · launched when the running optimizer finishes ($previous = its written-back session)</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead><tr><th>#</th><th>Queue id</th><th>Status</th><th>Queued</th><th>Request</th><th>Command</th><th /></tr></thead>
          <tbody>
            {queue.map(item => <tr key={item.id}>
              <td>{item.position ?? "—"}</td>
              <td className="mono">{item.id}</td>
              <td><StatusBadge value={item.status} />{item.error && <div className="error" style={{ fontSize: 11 }}>{item.error}</div>}</td>
              <td><Time value={item.queuedAtMs} /></td>
              <td className="muted">{queueOptionsSummary(item)}</td>
              <td><details><summary className="link-inline">command</summary><pre className="mono" style={{ maxWidth: 560, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{item.commandLine}</pre></details></td>
              <td>{item.status === "queued" && <button type="button" className="button" disabled={Boolean(pending)} onClick={() => cancelQueued(item.id)}>{pending === `cancel:${item.id}` ? "Working…" : "Cancel"}</button>}</td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </section>}

    <section className="portfolio-section" aria-label="Optimizer runs">
      <div className="panel-heading">
        <div><span className="eyebrow">HISTORY</span><h2>Runs</h2></div>
        <span className="muted">{data.items.length} recorded</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead><tr><th /><th>Run</th><th>Status</th><th>Started</th><th>Finished</th><th>Markets</th><th>Tier</th><th>Class</th><th>Only params</th><th>Base session</th><th>Screener</th><th>Arguments</th></tr></thead>
          <tbody>
            {data.items.length === 0 && <tr><td colSpan={12} className="empty">No optimizer runs recorded yet.</td></tr>}
            {data.items.map(item => {
              const only = onlyParamsSummary(item);
              return <Fragment key={item.id}>
                <tr className="expandable-position" onClick={() => setExpanded(current => current === item.id ? undefined : item.id)}>
                  <td><button type="button" className="row-toggle" aria-expanded={expanded === item.id} aria-label={`Toggle details for run ${item.id}`}>{expanded === item.id ? "−" : "+"}</button></td>
                  <td className="mono">{item.id}</td>
                  <td><StatusBadge value={item.status} /></td>
                  <td><Time value={item.startedMs} /></td>
                  <td><Time value={item.finishedMs} /></td>
                  <td>{item.marketCount}</td>
                  <td>{item.args?.tier ?? 1}</td>
                  <td>{item.args?.marketClass ?? <span className="muted">all</span>}</td>
                  <td title={only.title}>{only.label}</td>
                  <td className="mono">{item.args?.baseSession ?? <span className="muted">—</span>}</td>
                  <td className="muted">{screenerSummary(item)}</td>
                  <td className="muted">{argsSummary(item)}</td>
                </tr>
                {expanded === item.id && <tr className="position-detail-row open"><td colSpan={12}><div className="position-expansion"><div><OptimizationRunDetail runId={item.id} /></div></div></td></tr>}
              </Fragment>;
            })}
          </tbody>
        </table>
      </div>
    </section>
  </>;
}
