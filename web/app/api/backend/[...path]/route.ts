import { auth } from "@/auth";
import { apiBase } from "@/lib/api";
import { NextRequest, NextResponse } from "next/server";

async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const session = await auth();
  if (!session?.user) return NextResponse.json({ code: "unauthorized", message: "Sign in required" }, { status: 401 });
  if (request.method !== "GET") {
    const origin = request.headers.get("origin");
    if (!origin || origin !== request.nextUrl.origin) return NextResponse.json({ code: "invalid_origin", message: "Same-origin request required" }, { status: 403 });
    const authenticatedAt = Number((session as typeof session & { authenticatedAt?: number }).authenticatedAt ?? 0);
    if (Date.now() - authenticatedAt > 15 * 60 * 1000) return NextResponse.json({ code: "reauth_required", message: "Sign in again before using controls" }, { status: 403 });
  }
  const { path } = await context.params;
  const target = new URL(`/${path.join("/")}`, apiBase());
  target.search = request.nextUrl.search;
  const response = await fetch(target, {
    method: request.method,
    headers: {
      "x-internal-token": process.env.KALSHI_UI_INTERNAL_TOKEN ?? "",
      "x-request-id": request.headers.get("x-request-id") ?? crypto.randomUUID(),
      "content-type": request.headers.get("content-type") ?? "application/json"
    },
    body: request.method === "GET" ? undefined : await request.text(),
    cache: "no-store",
    signal: request.signal
  });
  const headers = new Headers();
  headers.set("content-type", response.headers.get("content-type") ?? "application/json");
  headers.set("cache-control", "no-store");
  if (headers.get("content-type")?.startsWith("text/event-stream")) headers.set("x-accel-buffering", "no");
  return new NextResponse(response.body, { status: response.status, headers });
}

export const GET = proxy;
export const POST = proxy;
