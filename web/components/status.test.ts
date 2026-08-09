import { createElement } from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Money, StatusBadge } from "./status";

describe("operations status components", () => {
  it("distinguishes healthy and risk states", () => {
    const { rerender } = render(createElement(StatusBadge, { value: "running" }));
    expect(screen.getByText("running")).toHaveClass("good");
    rerender(createElement(StatusBadge, { value: "flatten_only" }));
    expect(screen.getByText("flatten_only")).toHaveClass("bad");
  });

  it("renders signed P&L and unavailable data", () => {
    const { rerender } = render(createElement(Money, { cents: 125, signed: true }));
    expect(screen.getByText("+$1.25")).toBeInTheDocument();
    rerender(createElement(Money, { cents: null }));
    expect(screen.getByText("Unavailable")).toBeInTheDocument();
  });
});
