// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { useState } from "react";

import { DeckSubpageNav } from "../../components/DeckSubpageNav";

const ITEMS = [
  { key: "overview", code: "01", label: "Overview", sublabel: "市场总量" },
  { key: "origin", code: "02", label: "Origin", sublabel: "车系走势" },
  { key: "segment", code: "03", label: "Segment", sublabel: "级别结构" },
] as const;

function TestHarness({ initialKey = "overview" }: { initialKey?: (typeof ITEMS)[number]["key"] }) {
  const [activeKey, setActiveKey] = useState<(typeof ITEMS)[number]["key"]>(initialKey);

  return (
    <div>
      <DeckSubpageNav
        items={[...ITEMS]}
        activeKey={activeKey}
        onSelect={setActiveKey}
        ariaLabel="Deck pages"
        tabsClassName="market-scan-tab-strip"
      />
      <div>{`Active page: ${activeKey}`}</div>
      <input aria-label="demo input" />
    </div>
  );
}

// scrollIntoView is not implemented in jsdom
beforeAll(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

describe("DeckSubpageNav", () => {
  afterEach(() => {
    cleanup();
  });

  it("renders all tab buttons", () => {
    render(<TestHarness />);
    expect(screen.getByText("Overview")).toBeTruthy();
    expect(screen.getByText("Origin")).toBeTruthy();
    expect(screen.getByText("Segment")).toBeTruthy();
  });

  it("highlights the active tab", () => {
    render(<TestHarness initialKey="origin" />);
    const originTab = screen.getByText("Origin").closest("button");
    expect(originTab?.className).toContain("is-active");
  });

  it("navigates on tab click", () => {
    render(<TestHarness initialKey="overview" />);
    fireEvent.click(screen.getByText("Segment"));
    expect(screen.getByText("Active page: segment")).toBeTruthy();
  });

  it("supports left/right arrow key navigation without wrapping", () => {
    render(<TestHarness initialKey="overview" />);

    fireEvent.keyDown(window, { key: "ArrowLeft" });
    expect(screen.getByText("Active page: overview")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowRight" });
    expect(screen.getByText("Active page: origin")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(screen.getByText("Active page: origin")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowRight" });
    expect(screen.getByText("Active page: segment")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowUp" });
    expect(screen.getByText("Active page: segment")).toBeTruthy();
  });

  it("does not hijack arrow keys while an input is focused", () => {
    render(<TestHarness initialKey="origin" />);

    const input = screen.getByLabelText("demo input");
    input.focus();
    fireEvent.keyDown(input, { key: "ArrowRight" });

    expect(screen.getByText("Active page: origin")).toBeTruthy();
  });

  it("does not wrap navigation at edges", () => {
    render(<TestHarness initialKey="overview" />);
    fireEvent.keyDown(window, { key: "ArrowLeft" });
    expect(screen.getByText("Active page: overview")).toBeTruthy();

    cleanup();
    render(<TestHarness initialKey="segment" />);
    fireEvent.keyDown(window, { key: "ArrowRight" });
    expect(screen.getByText("Active page: segment")).toBeTruthy();
  });
});
