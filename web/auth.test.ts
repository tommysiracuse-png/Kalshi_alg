// @vitest-environment node

import { Auth } from "@auth/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("argon2", () => ({
  default: { verify: vi.fn(async () => true) }
}));

import { authConfig } from "./auth.config";

const originalEnv = { ...process.env };

function cookiePair(setCookie: string, name: string): string {
  const cookie = setCookie
    .split(/,(?=\s*[^;,]+=)/)
    .find((candidate) => candidate.trimStart().startsWith(`${name}=`));
  if (!cookie) throw new Error(`Missing ${name} cookie`);
  return cookie.trim().split(";", 1)[0];
}

async function signIn(protocol: "http" | "https"): Promise<string> {
  const origin = `${protocol}://127.0.0.1:3000`;
  const config = { ...authConfig, basePath: "/api/auth", secret: process.env.AUTH_SECRET };
  const csrfResponse = await Auth(new Request(`${origin}/api/auth/csrf`), config);
  const { csrfToken } = await csrfResponse.json() as { csrfToken: string };
  const csrfCookieName = protocol === "https" ? "__Host-authjs.csrf-token" : "authjs.csrf-token";
  const csrfCookie = cookiePair(csrfResponse.headers.get("set-cookie") ?? "", csrfCookieName);

  const response = await Auth(new Request(`${origin}/api/auth/callback/credentials`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      cookie: csrfCookie
    },
    body: new URLSearchParams({
      csrfToken,
      username: "operator",
      password: "correct horse battery staple",
      callbackUrl: origin
    })
  }), config);

  expect(response.status).toBe(302);
  return response.headers.get("set-cookie") ?? "";
}

describe("session cookie security", () => {
  beforeEach(() => {
    process.env.AUTH_SECRET = "test-secret-that-is-long-enough-for-auth-js";
    process.env.KALSHI_UI_USERNAME = "operator";
    process.env.KALSHI_UI_PASSWORD_HASH = "$argon2id$test";
  });

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  it("persists the session when production is served over the HTTP SSH tunnel", async () => {
    const setCookie = await signIn("http");
    const sessionCookie = setCookie
      .split(/,(?=\s*[^;,]+=)/)
      .find((candidate) => candidate.trimStart().startsWith("kalshi.session="));

    expect(sessionCookie).toBeDefined();
    expect(sessionCookie).not.toMatch(/;\s*Secure(?:;|$)/i);
  });

  it("keeps the session cookie secure for HTTPS requests", async () => {
    const setCookie = await signIn("https");
    const sessionCookie = setCookie
      .split(/,(?=\s*[^;,]+=)/)
      .find((candidate) => candidate.trimStart().startsWith("kalshi.session="));

    expect(sessionCookie).toMatch(/;\s*Secure(?:;|$)/i);
  });
});
