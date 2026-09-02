import { OptimizationPanel } from "@/components/optimization-panel";
import { apiGet } from "@/lib/api";
import type { OptimizerRunsResponse } from "@/lib/types";

export default async function OptimizationPage() {
  const initial = await apiGet<OptimizerRunsResponse>("/api/v1/optimizer/runs").catch(() => null);
  if (!initial) return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Operations API is offline</h1><p>Start kalshi-ui-api.service, then reload this page. Trading processes are not changed.</p></section>;
  return <>
    <header className="page-header"><div><span className="eyebrow">PARAMETER SEARCH</span><h1>Optimization</h1><p>Launch replay-driven parameter sweeps against recorded history, follow live progress, and inspect run leaderboards, sensitivity rankings, and reports.</p></div></header>
    <OptimizationPanel initial={initial} />
  </>;
}
