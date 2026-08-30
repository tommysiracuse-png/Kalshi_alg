import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PortfolioOrdersAnalytics, PortfolioPositionsAnalytics, PortfolioSummaryAnalytics } from "@/lib/types";
import { contractUnits, duration, LivePortfolio, moneyUnits, percentBps, priceUnits } from "./live-portfolio";

afterEach(() => {
  cleanup();
  window.localStorage.clear();
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
        { marketId: "TEST-1", ticker: "TEST-1", title: "Test market", marketUrl: "https://kalshi.com/markets/test/test-market/test-event", side: "yes", contractsUnits: 100, bidPriceUnits: 5000, askPriceUnits: 5200, midPriceUnits: 5100, costBasisUnits: 4000, averageCostPriceUnits: 4000, liquidationValueUnits: 5000, unrealizedValueUnits: 5100, totalFillCount: 1, totalOrderCount: 1, openOrderCount: 1, runningInCurrentSession: true },
        { marketId: "OTHER-1", ticker: "OTHER-1", title: "Other market", side: "no", contractsUnits: 200, bidPriceUnits: 3000, askPriceUnits: 3200, midPriceUnits: 3100, liquidationValueUnits: 6000, unrealizedValueUnits: 6200, totalFillCount: 0, totalOrderCount: 0, openOrderCount: 0, runningInCurrentSession: false },
      ],
    };
    const orders: PortfolioOrdersAnalytics = {
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 },
      summary: { totalOpenOrders: 1, ordersAttempted: 12, fillSampleSize: 1, totalMarketValueUnits: 5100 },
      items: [{ ticker: "TEST-1", marketId: "TEST-1", title: "Test market", openOrderCount: 1, ordersAttempted: 12, remainingContractsUnits: 100, initialContractsUnits: 200, filledContractsUnits: 100, totalFillCount: 1, firstCreatedAtMs: 1_900_000, lastUpdatedAtMs: 1_950_000, totalTimeOnBookMs: 100_000, midPriceUnits: 5100, totalMarketValueUnits: 5100, runningInCurrentSession: true, sideBreakdown: [{ side: "yes", openOrderCount: 1, remainingContractsUnits: 100 }] }],
    };

    render(<LivePortfolio initialSummary={summary} initialPositions={positions} initialOrders={orders} />);
    expect(screen.getAllByText("Available Cash").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Total Portfolio Value").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Liquidation Value").length).toBeGreaterThan(0);
    expect(screen.getByRole("navigation", { name: "Portfolio views" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Summary" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByText("Orders Attempted")).toBeInTheDocument();
    expect(screen.getByText("2 of 2 markets")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Test market ↗" })).toHaveAttribute("href", "https://kalshi.com/markets/test/test-market/test-event");
    expect(screen.getByRole("link", { name: "Open market ↗" })).toHaveAttribute("href", "https://kalshi.com/markets/test/test-market/test-event");
    expect(screen.getByRole("link", { name: "Open market ↗" })).toHaveAttribute("target", "_blank");

    fireEvent.change(screen.getAllByLabelText("Ticker")[0], { target: { value: "OTHER" } });
    expect(screen.getByText("1 of 2 markets")).toBeInTheDocument();
    fireEvent.click(screen.getAllByRole("button", { name: "Clear filters" })[0]);
    fireEvent.click(screen.getByRole("button", { name: "Expand fills for TEST-1" }));
    await waitFor(() => expect(request).toHaveBeenCalledWith(expect.stringContaining("/TEST-1/fills"), { cache: "no-store" }));
    expect(await screen.findByText("order-1")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collapse fills for TEST-1" })).toHaveAttribute("aria-expanded", "true");
  });

  it("changes history ranges, toggles series, and supports keyboard inspection through live refreshes", async () => {
    class MockEventSource {
      static OPEN = 1;
      static listeners = new Map<string, (event: MessageEvent) => void>();
      readyState = 1;
      onerror: (() => void) | null = null;
      addEventListener(name: string, listener: EventListener) { MockEventSource.listeners.set(name, listener as (event: MessageEvent) => void); }
      static emit(name: string, payload: unknown) { MockEventSource.listeners.get(name)?.({ data: JSON.stringify(payload) } as MessageEvent); }
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
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
    const positions: PortfolioPositionsAnalytics = { generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 }, items: [] };
    const orders: PortfolioOrdersAnalytics = { generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 }, summary: { totalOpenOrders: 0, ordersAttempted: 0, fillSampleSize: 0 }, items: [] };
    const request = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
      const url = String(input);
      const body = url.includes("/summary") ? { ...summary, snapshotAtMs: 3_000_000 } : url.includes("/positions") ? positions : orders;
      return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
    });

    render(<LivePortfolio initialSummary={summary} initialPositions={positions} initialOrders={orders} />);
    const cash = screen.getByLabelText("Show Available Cash");
    const total = screen.getByLabelText("Show Total Portfolio Value");
    expect(cash).not.toBeChecked();
    expect(total).toBeChecked();
    fireEvent.click(cash);
    fireEvent.click(total);
    expect(cash).toBeChecked();
    expect(total).not.toBeChecked();
    fireEvent.click(cash);
    expect(cash).toBeChecked();

    const chart = screen.getByRole("img", { name: /Portfolio value history chart/i });
    fireEvent.focus(chart);
    expect(screen.getByRole("status")).toHaveTextContent("Available Cash");
    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    expect(screen.getByRole("status")).toHaveTextContent("$9.00");

    fireEvent.click(screen.getByRole("button", { name: "1W" }));
    await waitFor(() => expect(request).toHaveBeenCalledWith(expect.stringContaining("window=7d"), { cache: "no-store" }));
    expect(cash).toBeChecked();
    MockEventSource.emit("portfolio", { generatedAtMs: 4_000_000 });
    await waitFor(() => expect(request.mock.calls.filter(call => String(call[0]).includes("window=7d")).length).toBeGreaterThanOrEqual(2));
  });

  it("renders account-wide market analytics and persists configurable column order", async () => {
    class MockEventSource {
      static OPEN = 1;
      readyState = 1;
      onerror: (() => void) | null = null;
      addEventListener() {}
      close() {}
    }
    vi.stubGlobal("EventSource", MockEventSource);
    const source = { available: true, updatedAt: 2_000_000, stale: false };
    const summary: PortfolioSummaryAnalytics = { generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { partial: true }, summary: {}, history: {} };
    const positions: PortfolioPositionsAnalytics = {
      generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: { startedAtMs: 1_000_000 },
      items: [{
        marketId: "TEST-1", ticker: "TEST-1", title: "Test market", seriesTitle: "Test description", marketUrl: "https://kalshi.com/markets/test/test-market/test-event", side: "yes",
        contractsUnits: 100, bidPriceUnits: 5_000, askPriceUnits: 5_200, midPriceUnits: 5_100,
        costBasisUnits: 4_000, currentMarketValueUnits: 5_100, liquidationValueUnits: 5_000,
        realizedPnlUnits: 2_000, feesUnits: 100, netRealizedPnlUnits: 1_900, unrealizedPnlUnits: 1_600, totalPnlUnits: 3_500,
        openOrderCount: 2, lastTradeAtMs: 1_999_000, totalFillCount: 3, totalOrderCount: 4,
      }],
    };
    const orders: PortfolioOrdersAnalytics = { generatedAt: 2_000_000, snapshotAtMs: 2_000_000, source, warnings: [], coverage: {}, summary: { totalOpenOrders: 0, ordersAttempted: 0, fillSampleSize: 0 }, items: [] };

    render(<LivePortfolio initialSummary={summary} initialPositions={positions} initialOrders={orders} view="markets" />);
    expect(screen.getByRole("link", { name: "Markets" })).toHaveAttribute("aria-current", "page");
    expect(screen.queryByText("Account Inventory")).not.toBeInTheDocument();
    for (const header of ["Market", "Side / contracts", "Total cost", "Current market value", "Liquidation value", "Total P&L", "Open orders", "Last trade", "Fills / orders", "Bid / ask / mid"]) {
      expect(screen.getByRole("columnheader", { name: new RegExp(`^${header.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") })).toBeInTheDocument();
    }
    expect(screen.getByRole("link", { name: "Test market ↗" })).toHaveAttribute("href", "https://kalshi.com/markets/test/test-market/test-event");
    expect(screen.getByText("Test description")).toBeInTheDocument();
    expect(screen.getByText("+$0.35")).toBeInTheDocument();
    expect(screen.getByText("+$0.19")).toBeInTheDocument();
    expect(screen.getByText("+$0.16")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByLabelText("Markets: Total cost")).toBeEnabled());

    const book = screen.getByRole("columnheader", { name: /Bid.*ask.*mid/i });
    const market = screen.getByRole("columnheader", { name: "Market" });
    fireEvent.dragStart(book);
    fireEvent.dragOver(market);
    fireEvent.drop(market);
    expect(screen.getAllByRole("columnheader")[0]).toHaveTextContent("Bid / ask / mid");

    fireEvent.click(screen.getByLabelText("Markets: Total cost"));
    expect(screen.queryByRole("columnheader", { name: "Total cost" })).not.toBeInTheDocument();
    await waitFor(() => expect(window.localStorage.getItem("kalshi.portfolio.markets.columns.v1")).toContain('"id":"totalCost","enabled":false'));
  });
});
