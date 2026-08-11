import type { NextAuthConfig } from "next-auth";
import Credentials from "next-auth/providers/credentials";
import argon2 from "argon2";
import { z } from "zod";

const loginAttempts = new Map<string, { count: number; resetAt: number }>();

export const authConfig = {
  trustHost: true,
  pages: { signIn: "/login" },
  session: { strategy: "jwt", maxAge: 12 * 60 * 60 },
  cookies: {
    // Leave `secure` unset so Auth.js derives it from the request protocol.
    // Production is commonly reached through the documented HTTP SSH tunnel;
    // tying this flag to NODE_ENV makes browsers discard the session cookie.
    sessionToken: { name: "kalshi.session", options: { httpOnly: true, sameSite: "strict", path: "/" } }
  },
  providers: [Credentials({
    credentials: { username: {}, password: {} },
    async authorize(raw) {
      const parsed = z.object({ username: z.string().min(1).max(128), password: z.string().min(1).max(1024) }).safeParse(raw);
      if (!parsed.success) return null;
      const throttleKey = parsed.data.username.toLowerCase();
      const now = Date.now();
      const attempt = loginAttempts.get(throttleKey);
      if (attempt && attempt.resetAt > now && attempt.count >= 5) return null;
      const username = process.env.KALSHI_UI_USERNAME;
      const passwordHash = process.env.KALSHI_UI_PASSWORD_HASH;
      if (!username || !passwordHash || parsed.data.username !== username || !(await argon2.verify(passwordHash, parsed.data.password))) {
        loginAttempts.set(throttleKey, { count: attempt && attempt.resetAt > now ? attempt.count + 1 : 1, resetAt: now + 10 * 60 * 1000 });
        return null;
      }
      loginAttempts.delete(throttleKey);
      return { id: "operator", name: username, authenticatedAt: Date.now() };
    }
  })],
  callbacks: {
    jwt({ token, user }) {
      if (user) token.authenticatedAt = (user as { authenticatedAt?: number }).authenticatedAt ?? Date.now();
      return token;
    },
    session({ session, token }) {
      (session as typeof session & { authenticatedAt?: number }).authenticatedAt = Number(token.authenticatedAt ?? 0);
      return session;
    },
    authorized({ auth: session, request }) {
      if (request.nextUrl.pathname === "/login") return true;
      return Boolean(session?.user);
    }
  }
} satisfies NextAuthConfig;
