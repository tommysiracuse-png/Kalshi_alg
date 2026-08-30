import { cleanup, fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PortfolioSummaryAnalytics } from "@/lib/types";
import { PortfolioHistoryChart } from "./portfolio-history-chart";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const data: PortfolioSummaryAnalytics = {
  generatedAt: 5_000_000,
  snapshotAtMs: 5_000_000,
  source: { available: true, updatedAt: 5_000_000, stale: false },
  coverage: { requestedWindowMs: 4_000_000, actualWindowMs: 4_000_000, partial: false },
  warnings: [],
  summary: { totalPortfolioValueUnits: 50_000 },
  history: {
    totalPortfolioValue: {
      currentUnits: 50_000,
      baselineUnits: 10_000,
      changeUnits: 40_000,
      changeBps: 40_000,
      partial: false,
      actualWindowMs: 4_000_000,
      points: [
        { timestampMs: 1_000_000, valueUnits: 10_000 },
        { timestampMs: 2_000_000, valueUnits: 20_000 },
        { timestampMs: 3_000_000, valueUnits: 30_000 },
        { timestampMs: 4_000_000, valueUnits: 40_000 },
        { timestampMs: 5_000_000, valueUnits: 50_000 },
      ],
    },
  },
};

describe("PortfolioHistoryChart pointer tracking", () => {
  it("uses the rendered plot bounds when the SVG is letterboxed in a wider tab", () => {
    const { container } = render(
      <PortfolioHistoryChart data={data} window="24h" loading={false} onWindowChange={() => undefined} />,
    );
    const hitArea = container.querySelector<SVGRectElement>(".portfolio-chart-hit-area");
    expect(hitArea).not.toBeNull();
    vi.spyOn(hitArea!, "getBoundingClientRect").mockReturnValue({
      left: 300,
      width: 600,
      right: 900,
      top: 100,
      height: 200,
      bottom: 300,
      x: 300,
      y: 100,
      toJSON: () => ({}),
    });

    fireEvent.pointerMove(hitArea!, { clientX: 720 });

    const cursor = container.querySelector<SVGLineElement>(".portfolio-chart-cursor");
    expect(cursor).not.toBeNull();
    expect(cursor).toHaveAttribute("x1", "678");
    expect(cursor).toHaveAttribute("x2", "678");
  });
});
