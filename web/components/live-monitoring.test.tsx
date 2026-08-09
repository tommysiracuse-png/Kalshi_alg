import { describe, expect, it } from "vitest";
import { formatDuration } from "./live-monitoring";

describe("monitoring formatting", () => {
  it("formats running durations", () => {
    expect(formatDuration(9_000)).toBe("9s");
    expect(formatDuration(125_000)).toBe("2m 5s");
    expect(formatDuration(3_720_000)).toBe("1h 2m");
  });

  it("handles missing durations", () => {
    expect(formatDuration(undefined)).toBe("Unavailable");
  });
});
