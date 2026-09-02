import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ControlPanel } from "./control-panel";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ControlPanel", () => {
  it("sends controls when randomUUID is unavailable on plain HTTP", async () => {
    vi.stubGlobal("crypto", undefined);
    const request = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ requestId: "server-request-id" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }));

    render(<ControlPanel />);
    // Two-click confirmation: the first click arms, the second executes.
    fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    expect(request).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Confirm stop?" }));

    expect(screen.getByRole("button", { name: "Working…" })).toBeDisabled();
    await waitFor(() => expect(request).toHaveBeenCalledWith(
      "/api/backend/api/v1/controls/fleet/stop",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ "x-request-id": expect.stringMatching(/^request-/) }),
      }),
    ));
    expect(await screen.findByText("stop completed · server-request-id")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop" })).toBeEnabled();
  });
});
