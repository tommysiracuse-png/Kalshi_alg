import { describe, expect, it } from "vitest";
import { contractUnits, duration, moneyUnits, percentBps, priceUnits } from "./live-portfolio";

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
});
