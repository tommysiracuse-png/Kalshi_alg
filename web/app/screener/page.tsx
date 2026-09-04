import { LiveScreener } from "@/components/live-monitoring";
import { apiGet } from "@/lib/api";
import type { Monitoring } from "@/lib/types";

export default async function ScreenerPage() {
  const initial = await apiGet<Monitoring>("/api/v1/monitoring").catch(() => null);
  if (initial) return <LiveScreener initial={initial} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Screener is offline</h1><p>The operations API has not published a monitoring snapshot yet.</p></section>;
}
