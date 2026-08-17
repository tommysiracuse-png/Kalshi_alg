import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PortfolioOrdersAnalytics, PortfolioPositionsAnalytics, PortfolioSummaryAnalytics } from "@/lib/types";
import { contractUnits, duration, LivePortfolio, moneyUnits, percentBps, priceUnits } from "./live-portfolio";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("portfolio fixed-point formatting", () => {
  it("formats money, prices, quantities, and percentages", () => {
    expect(moneyUnits(123456)).toBe("$12.35");
    expect(moneyUnits(-10000, true)).toBe("$-1.00");
    expect(priceUnits(4321)).toBe("43.21¢");
    expect(contractUnits(250)).toBe("2.50");
    expect(percentBps(125)).toBe("+1.25%");
  });

  it("formats monitoring durations", () => {
    expect(duration(2500)).toBe("2s");
    expect(duration(65_000)).toBe("1m 5s");
  });

  it("renders analytics, filters positions, and lazily expands fills", async () => {
    class MockEventSource {
      static OPEN = 1;
      readyState = 1;
      onerror: (() => void) | null = null;
      addEventListener() {}
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const request = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source: { available: true, updatedAt: 2_000_000, stale: false },
      coverage: { startedAtMs: 1_000_000 }, ticker: "TEST-1", nextCursor: null, warnings: [],
      items: [{
        fillId: "fill-1", tradeId: "trade-1", orderId: "order-1", ticker: "TEST-1", side: "yes",
        filledAtMs: 1_999_000, timeToFillMs: 1000, contractsUnits: 100, costOfContractsUnits: 4000,
        feeUnits: 100, costInPositionUnits: 4100, liquidationValueUnits: 5000,
        unrealizedValueUnits: 5100, liquidationPnlUnits: 900, marketPnlUnits: 1000, isTaker: false,
      }],
    }), { status: 200, headers: { "content-type": "application/json" } }));
    const source = { available: true, updatedAt: 2_000_000, stale: false };
    const summary: PortfolioSummaryAnalytics = {
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [],
      coverage: { startedAtMs: 1_000_000, requestedWindowMs: 86_400_000, actualWindowMs: 1_000_000, partial: true },
      summary: { availableCashUnits: 100_000, totalPortfolioValueUnits: 110_000, positionsLiquidationValueUnits: 9_000, apiTier: "advanced" },
      history: {
        availableCash: { currentUnits: 100_000, baselineUnits: 90_000, changeUnits: 10_000, changeBps: 1111, partial: true, actualWindowMs: 1_000_000, points: [{ timestampMs: 1_000_000, valueUnits: 90_000 }, { timestampMs: 2_000_000, valueUnits: 100_000 }] },
        totalPortfolioValue: { currentUnits: 110_000, baselineUnits: 100_000, changeUnits: 10_000, changeBps: 1000, partial: true, actualWindowMs: 1_000_000, points: [{ timestampMs: 1_000_000, valueUnits: 100_000 }, { timestampMs: 2_000_000, valueUnits: 110_000 }] },
        positionsLiquidationValue: { currentUnits: 9_000, baselineUnits: 8_000, changeUnits: 1_000, changeBps: 1250, partial: true, actualWindowMs: 1_000_000, points: [{ timestampMs: 1_000_000, valueUnits: 8_000 }, { timestampMs: 2_000_000, valueUnits: 9_000 }] },
      },
    };
    const positions: PortfolioPositionsAnalytics = {
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 },
      items: [
        { marketId: "TEST-1", ticker: "TEST-1", title: "Test market", side: "yes", contractsUnits: 100, bidPriceUnits: 5000, askPriceUnits: 5200, midPriceUnits: 5100, costBasisUnits: 4000, averageCostPriceUnits: 4000, liquidationValueUnits: 5000, unrealizedValueUnits: 5100, totalFillCount: 1, totalOrderCount: 1, openOrderCount: 1, runningInCurrentSession: true },
        { marketId: "OTHER-1", ticker: "OTHER-1", title: "Other market", side: "no", contractsUnits: 200, bidPriceUnits: 3000, askPriceUnits: 3200, midPriceUnits: 3100, liquidationValueUnits: 6000, unrealizedValueUnits: 6200, totalFillCount: 0, totalOrderCount: 0, openOrderCount: 0, runningInCurrentSession: false },
      ],
    };
    const orders: PortfolioOrdersAnalytics = {
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 },
      summary: { totalOpenOrders: 1, ordersAttempted: 12, fillSampleSize: 1, totalMarketValueUnits: 5100 },
      items: [{ ticker: "TEST-1", marketId: "TEST-1", title: "Test market", openOrderCount: 1, ordersAttempted: 12, remainingContractsUnits: 100, initialContractsUnits: 200, filledContractsUnits: 100, totalFillCount: 1, firstCreatedAtMs: 1_900_000, lastUpdatedAtMs: 1_950_000, totalTimeOnBookMs: 100_000, midPriceUnits: 5100, totalMarketValueUnits: 5100, runningInCurrentSession: true, sideBreakdown: [{ side: "yes", openOrderCount: 1, remainingContractsUnits: 100 }] }],
    };

    render(<LivePortfolio initialSummary={summary} initialPositions={positions} initialOrders={orders} />);
    expect(screen.getByText("Available Cash")).toBeInTheDocument();
    expect(screen.getByText("Total Portfolio Value")).toBeInTheDocument();
    expect(screen.getByText("Positions Liquidation Value")).toBeInTheDocument();
    expect(screen.getByText("Orders Attempted")).toBeInTheDocument();
    expect(screen.getByText("2 of 2 markets")).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Ticker")[0], { target: { value: "OTHER" } });
    expect(screen.getByText("1 of 2 markets")).toBeInTheDocument();
    fireEvent.click(screen.getAllByRole("button", { name: "Clear filters" })[0]);
    fireEvent.click(screen.getByRole("button", { name: "Expand fills for TEST-1" }));
    await waitFor(() => expect(request).toHaveBeenCalledWith(expect.stringContaining("/TEST-1/fills"), { cache: "no-store" }));
    expect(await screen.findByText("order-1")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collapse fills for TEST-1" })).toHaveAttribute("aria-expanded", "true");
  });
});
