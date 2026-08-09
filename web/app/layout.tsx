import type { Metadata } from "next";
import { auth } from "@/auth";
import { Nav } from "@/components/nav";
import "./globals.css";

export const metadata: Metadata = { title: "Kalshi Operations", description: "Operations and risk console for Kalshi_alg" };

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const session = await auth();
  return <html lang="en"><body>{session?.user ? <div className="app-shell"><Nav /><main className="content">{children}</main></div> : children}</body></html>;
}
