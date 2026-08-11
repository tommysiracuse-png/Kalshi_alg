import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SavedSession } from "@/lib/types";
import { OverviewSessionSelector } from "./overview-session-selector";

const sessions: SavedSession[] = [
  { id: "default", name: "Default", description: "", configuration: {} as SavedSession["configuration"], version: 1, createdAt: 1, updatedAt: 1, selected: true, runCount: 1 },
  { id: "new", name: "New Session", description: "", configuration: {} as SavedSession["configuration"], version: 1, createdAt: 2, updatedAt: 2, selected: false, runCount: 0 },
];

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("OverviewSessionSelector", () => {
  it("selects the configuration used by the next launcher start", async () => {
    const request = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ item: { ...sessions[1], selected: true } }), { status: 200, headers: { "content-type": "application/json" } }));
    render(<OverviewSessionSelector initial={sessions} />);

    fireEvent.change(screen.getByLabelText("Session for next launcher start"), { target: { value: "new" } });

    await waitFor(() => expect(request).toHaveBeenCalledWith("/api/backend/api/v1/sessions/new/select", expect.objectContaining({ method: "POST", body: "{}" })));
    expect(await screen.findByText("New Session will be used for the next launcher start.")).toBeInTheDocument();
    expect(screen.getByLabelText("Session for next launcher start")).toHaveValue("new");
  });

  it("locks selection while a run is active", () => {
    render(<OverviewSessionSelector initial={sessions} activeRun={{ id: "run-12345678", sessionId: "default", sessionName: "Default", status: "running" }} />);
    expect(screen.getByLabelText("Session for next launcher start")).toBeDisabled();
    expect(screen.getByText(/selection is locked/i)).toBeInTheDocument();
  });
});
