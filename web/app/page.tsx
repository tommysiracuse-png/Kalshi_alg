import { LiveOverview } from "@/components/live-overview";
import { apiGet } from "@/lib/api";
import type { Overview, SavedSession } from "@/lib/types";

type SessionData = {
  items: SavedSession[];
  activeRun?: { id: string; sessionId: string; sessionName: string; status: string } | null;
};

export default async function OverviewPage() {
  const [initial, sessionData] = await Promise.all([
    apiGet<Overview>("/api/v1/overview").catch(() => null),
    apiGet<SessionData>("/api/v1/sessions").catch((): SessionData => ({ items: [], activeRun: null })),
  ]);
  if (initial) return <LiveOverview initial={initial} sessions={sessionData.items} activeRun={sessionData.activeRun} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Operations API is offline</h1><p>Start kalshi-ui-api.service, then reload this page. Trading processes are not changed.</p></section>;
}
