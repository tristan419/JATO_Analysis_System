// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
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

describe("DeckSubpageNav", () => {
  afterEach(() => {
    cleanup();
  });

  it("navigates to adjacent subpages through the step buttons", () => {
    render(<TestHarness initialKey="origin" />);

    fireEvent.click(screen.getByLabelText("下一页：03 Segment"));
    expect(screen.getByText("Active page: segment")).toBeTruthy();

    fireEvent.click(screen.getByLabelText("上一页：02 Origin"));
    expect(screen.getByText("Active page: origin")).toBeTruthy();
  });

  it("supports left/right subpage navigation without wrapping", () => {
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

  it("disables previous and next buttons at the ends", () => {
    render(<TestHarness initialKey="overview" />);
    expect((screen.getByLabelText("已经是第一页") as HTMLButtonElement).disabled).toBe(true);

    cleanup();

    render(<TestHarness initialKey="segment" />);
    expect((screen.getByLabelText("已经是最后一页") as HTMLButtonElement).disabled).toBe(true);
  });
});
