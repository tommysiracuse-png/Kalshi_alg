import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { SessionEditor } from "./session-editor";
import type { SavedSession } from "@/lib/types";

const session: SavedSession = {
  id: "session-1", name: "Default", description: "Test profile", version: 1,
  createdAt: 1, updatedAt: 1, archivedAt: null, selected: true, runCount: 2,
  configuration: {
    schemaVersion: 1,
    execution: { useDemo: false, dryRun: false, subaccount: 0 },
    launcher: { fixedTicker: "", maxBots: 40, yesBudgetCents: 100 },
    watchdog: { intervalSeconds: 60, flattenRetries: 2 },
    bot: { post_only_quotes: true, markout_horizons_seconds: [1, 5, 30] },
  },
};

describe("SessionEditor", () => {
  it("renders typed configuration sections and active-run guidance", () => {
    render(<SessionEditor initial={[session]} activeRun={{ id: "run-1", sessionId: session.id, sessionName: session.name, status: "running" }} />);
    expect(screen.getByRole("heading", { name: "Launcher" })).toBeInTheDocument();
    expect(screen.getByLabelText("Max Bots")).toHaveAttribute("type", "number");
    expect(screen.getByLabelText("Post only quotes")).toHaveAttribute("type", "checkbox");
    expect(screen.getByText(/apply only to the next start/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Select" })).toBeDisabled();
  });
});
