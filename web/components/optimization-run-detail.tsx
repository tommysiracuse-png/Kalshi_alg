"use client";

import { useEffect, useState } from "react";
import type { OptimizerRunDetail as RunDetail } from "../lib/types";

function Units({ value }: { value?: number | null }) {
  if (value === null || value === undefined) return <span className="muted">—</span>;
  return <span className={value < 0 ? "negative" : value > 0 ? "positive" : ""}>{value.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>;
}

export function OptimizationRunDetail({ runId }: { runId: string }) {
  const [detail, setDetail] = useState<RunDetail>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch(`/api/backend/api/v1/optimizer/runs/${encodeURIComponent(runId)}`, { cache: "no-store" });
        const body = await response.json();
        if (!response.ok) throw new Error(body.message ?? `Run detail failed (${response.status})`);
        if (!cancelled) setDetail(body);
      } catch (caught) {
        if (!cancelled) setError(caught instanceof Error ? caught.message : "Run detail failed");
      }
    })();
    return () => { cancelled = true; };
  }, [runId]);

  if (error) return <p className="error">{error}</p>;
  if (!detail) return <p className="muted">Loading run detail…</p>;

  const args = detail.run.args ?? {};
  const keyArgs: Array<[string, string]> = [
    ["Tier", String(args.tier ?? 1) + (args.recordRoot ? ` (${args.recordRoot})` : "")],
    ["Class", args.marketClass ?? "all"],
    ["Only params", args.onlyParams?.length ? args.onlyParams.join(", ") : `full search (top ${args.topParams ?? "?"})`],
    ["Base session", args.baseSession ?? "—"],
    ["Screener session", args.screenerSession ?? (args.baseSession ? `${args.baseSession} (base)` : "—")],
    ["Screener filter", args.screenerFilter === false ? "off" : args.screenerSource ?? (args.screenerFilter ? "on" : "—")],
    ["Seed source", args.baseParamsJson ?? "defaults"],
    ["Splits / fill share", `${args.splits ?? "?"} / ${args.fillShare ?? "?"}`],
    ["Workers / seed", `${args.workers ?? "?"} / ${args.seed ?? "?"}`],
    ["Period", args.lastDays ? `last ${args.lastDays} days` : (args.fromDate || args.toDate) ? `${args.fromDate ?? "…"} → ${args.toDate ?? "…"}` : "all data"],
    ["Write-back", args.writeBack ? "yes" : "no"],
  ];

  return <div className="run-markets">
    <div className="nested-heading"><h3>Arguments</h3><span>{detail.launch?.launchId ? `dashboard launch ${detail.launch.launchId}` : "as stored by optimizer.main"}</span></div>
    <div className="settings-grid" style={{ marginBottom: 12 }}>
      {keyArgs.map(([label, value]) => <label key={label} style={{ display: "grid", gap: 4 }}><span>{label}</span><span className="mono" style={{ wordBreak: "break-all" }}>{value}</span></label>)}
    </div>
    {detail.commandLine
      ? <pre className="mono" aria-label="Launch command line" style={{ margin: "0 0 10px", padding: 10, background: "#050c0b", border: "1px solid #1b302b", borderRadius: 8, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{detail.commandLine}</pre>
      : <p className="muted" style={{ margin: "0 0 10px", fontSize: 12 }}>No dashboard launch record for this run (started from the console or before launch journaling).</p>}
    <details style={{ marginBottom: 16 }}><summary className="link-inline">full args JSON</summary><pre className="mono" style={{ maxHeight: 320, overflow: "auto", whiteSpace: "pre-wrap" }}>{JSON.stringify(detail.argsFull ?? {}, null, 1)}</pre></details>

    <div className="nested-heading"><h3>Leaderboard (final stage, out-of-sample)</h3><span>Top {detail.leaderboard.length} candidates · score = mean over test splits</span></div>
    {detail.leaderboard.length === 0
      ? <p className="empty">No final-stage candidates recorded for this run.</p>
      : <div className="table-wrap"><table className="market-metrics-table">
        <thead><tr><th>#</th><th>Candidate</th><th>OOS score</th><th>Train score</th><th>Fills</th><th>Errors</th><th>Params</th></tr></thead>
        <tbody>{detail.leaderboard.map((candidate, index) => <tr key={candidate.candidateId}>
          <td>{index + 1}</td>
          <td className="mono">{candidate.candidateId}</td>
          <td><Units value={candidate.scoreUnits} /></td>
          <td><Units value={candidate.trainScoreUnits} /></td>
          <td>{candidate.fills}</td>
          <td>{candidate.errorCount}</td>
          <td><details><summary className="link-inline">params JSON</summary><pre className="mono" style={{ maxWidth: 560, maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap" }}>{JSON.stringify(candidate.params, null, 1)}</pre></details></td>
        </tr>)}</tbody>
      </table></div>}

    <div className="nested-heading orders-heading"><h3>Sensitivity ranking</h3><span>Top {Math.min(15, detail.sensitivity.length)} of {detail.sensitivity.length} fields (|delta J|)</span></div>
    {detail.sensitivity.length === 0
      ? <p className="empty">No sensitivity rows recorded for this run.</p>
      : <div className="table-wrap"><table>
        <thead><tr><th>Rank</th><th>Field</th><th>Delta (units)</th></tr></thead>
        <tbody>{detail.sensitivity.slice(0, 15).map(item => <tr key={item.field}>
          <td>{item.rank}</td>
          <td className="mono">{item.field}</td>
          <td><Units value={item.deltaUnits} /></td>
        </tr>)}</tbody>
      </table></div>}

    <div className="nested-heading orders-heading"><h3>Report</h3><span className="mono">report_{detail.run.id}.md</span></div>
    {detail.report
      ? <section className="log-panel"><pre>{detail.report}</pre></section>
      : <p className="empty">No report file found for this run.</p>}
  </div>;
}
