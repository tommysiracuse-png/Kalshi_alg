import { apiGet } from "@/lib/api";
import { StatusBadge, Time } from "@/components/status";

type SystemData = { generatedAt: number; workspace: string; service: string; sources: Record<string, { available: boolean; stale: boolean; updatedAt?: number; error?: string }> };
export default async function SystemPage() {
  const data = await apiGet<SystemData>("/api/v1/system");
  return <><header className="page-header"><div><span className="eyebrow">DIAGNOSTICS</span><h1>System</h1><p>Sanitized service configuration and data-source freshness.</p></div></header><section className="panel"><dl className="details"><div><dt>Workspace</dt><dd className="mono">{data.workspace}</dd></div><div><dt>Bot service</dt><dd className="mono">{data.service}</dd></div><div><dt>API generated</dt><dd><Time value={data.generatedAt} /></dd></div></dl></section><section className="panel"><div className="panel-heading"><div><span className="eyebrow">SOURCE HEALTH</span><h2>Runtime inputs</h2></div></div><div className="source-list">{Object.entries(data.sources).map(([name, source]) => <div key={name}><span><strong>{name}</strong><small>{source.error ?? <Time value={source.updatedAt} />}</small></span><StatusBadge value={!source.available ? "unavailable" : source.stale ? "stale" : "ok"} /></div>)}</div></section></>;
}
