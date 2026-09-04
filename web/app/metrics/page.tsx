import { LiveMetrics } from "@/components/live-metrics";
import { apiGet } from "@/lib/api";
import type { MetricsResponse, SavedSession } from "@/lib/types";

export default async function MetricsPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const params = await searchParams;
  const sessionId = typeof params.session_id === "string" ? params.session_id : "";
  const status = typeof params.status === "string" ? params.status : "";
  const from = typeof params.from === "string" ? params.from : "";
  const to = typeof params.to === "string" ? params.to : "";
  const requestedHorizon = typeof params.markout_horizon_ms === "string" ? Number(params.markout_horizon_ms) : 30_000;
  const markoutHorizonMs = [1_000, 5_000, 30_000, 120_000].includes(requestedHorizon) ? requestedHorizon : 30_000;
  const query = new URLSearchParams();
  if (sessionId) query.set("session_id", sessionId);
  if (status) query.set("status", status);
  if (from && Number.isFinite(Date.parse(`${from}T00:00:00`))) query.set("from_ms", String(Date.parse(`${from}T00:00:00`)));
  if (to && Number.isFinite(Date.parse(`${to}T23:59:59.999`))) query.set("to_ms", String(Date.parse(`${to}T23:59:59.999`)));
  query.set("include_artifact_bytes", "false");
  query.set("include_market_metrics", "false");
  const [data, sessionData] = await Promise.all([
    apiGet<MetricsResponse>(`/api/v1/metrics?${query}`),
    apiGet<{ items: SavedSession[] }>("/api/v1/sessions?include_archived=true"),
  ]);
  return <LiveMetrics initial={data} sessions={sessionData.items} query={query.toString()} markoutHorizonMs={markoutHorizonMs} filters={{ sessionId, status, from, to }} />;
}
