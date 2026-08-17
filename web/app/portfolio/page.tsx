import { LivePortfolio } from "@/components/live-portfolio";
import { apiGet } from "@/lib/api";
import type { PortfolioOrdersAnalytics, PortfolioPositionsAnalytics, PortfolioSummaryAnalytics } from "@/lib/types";

export default async function PortfolioPage() {
  const initial = await Promise.all([
    apiGet<PortfolioSummaryAnalytics>("/api/v1/portfolio/summary?window=24h"),
    apiGet<PortfolioPositionsAnalytics>("/api/v1/portfolio/positions"),
    apiGet<PortfolioOrdersAnalytics>("/api/v1/portfolio/orders"),
  ]).catch(() => null);
  if (initial) return <LivePortfolio initialSummary={initial[0]} initialPositions={initial[1]} initialOrders={initial[2]} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Portfolio is offline</h1><p>The operations API is not available. Trading processes are not changed.</p></section>;
}
