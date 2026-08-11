import { describe, expect, it } from "vitest";
import { hasSameOrigin } from "./request-security";

describe("hasSameOrigin", () => {
  it("uses the public Host header when Next reconstructed an internal URL", () => {
    expect(hasSameOrigin("http://192.168.48.1:3000", "192.168.48.1:3000", "http:")).toBe(true);
  });

  it("normalizes default ports", () => {
    expect(hasSameOrigin("http://kalshi.local", "kalshi.local:80", "http:")).toBe(true);
  });

  it.each([
    ["http://attacker.example", "192.168.48.1:3000", "http:"],
    ["http://192.168.48.1:4000", "192.168.48.1:3000", "http:"],
    ["https://192.168.48.1:3000", "192.168.48.1:3000", "http:"],
    [null, "192.168.48.1:3000", "http:"],
    ["not a URL", "192.168.48.1:3000", "http:"],
  ])("rejects a request that is not same-origin", (origin, host, protocol) => {
    expect(hasSameOrigin(origin, host, protocol)).toBe(false);
  });
});
