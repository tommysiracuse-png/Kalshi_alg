import { LivePortfolio } from "@/components/live-portfolio";
import { apiGet } from "@/lib/api";
import type { AccountPortfolio } from "@/lib/types";

export default async function PortfolioPage() {
  const initial = await apiGet<AccountPortfolio>("/api/v1/portfolio").catch(() => null);
  if (initial) return <LivePortfolio initial={initial} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Portfolio is offline</h1><p>The operations API is not available. Trading processes are not changed.</p></section>;
}
