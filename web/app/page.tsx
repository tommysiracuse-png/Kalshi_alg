import { LiveOverview } from "@/components/live-overview";
import { apiGet } from "@/lib/api";
import type { Overview } from "@/lib/types";

export default async function OverviewPage() {
  const initial = await apiGet<Overview>("/api/v1/overview").catch(() => null);
  if (initial) return <LiveOverview initial={initial} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Operations API is offline</h1><p>Start kalshi-ui-api.service, then reload this page. Trading processes are not changed.</p></section>;
}
