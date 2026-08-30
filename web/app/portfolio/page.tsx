import { LivePortfolio } from "@/components/live-portfolio";
import { apiGet } from "@/lib/api";
import type { PortfolioOrdersAnalytics, PortfolioPositionsAnalytics, PortfolioSummaryAnalytics } from "@/lib/types";

export default async function PortfolioPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const params = await searchParams;
  const view = params.view === "markets" ? "markets" : "summary";
  const initial = await Promise.all([
    apiGet<PortfolioSummaryAnalytics>("/api/v1/portfolio/summary?window=24h"),
    apiGet<PortfolioPositionsAnalytics>("/api/v1/portfolio/positions"),
    apiGet<PortfolioOrdersAnalytics>("/api/v1/portfolio/orders"),
  ]).catch(() => null);
  if (initial) return <LivePortfolio initialSummary={initial[0]} initialPositions={initial[1]} initialOrders={initial[2]} view={view} />;
  return <section className="offline"><span className="eyebrow">SERVICE UNAVAILABLE</span><h1>Portfolio is offline</h1><p>The operations API is not available. Trading processes are not changed.</p></section>;
}
