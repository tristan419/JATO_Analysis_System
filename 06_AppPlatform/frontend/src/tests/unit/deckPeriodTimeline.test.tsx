// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";

import { DeckPeriodTimeline } from "../../components/DeckPeriodTimeline";
import type { MarketScanPeriodRange } from "../../types";

const PERIOD_OPTIONS = [
  { value: "2026-02", label: "26.02" },
  { value: "2026-03", label: "26.03" },
  { value: "2026-04", label: "26.04" },
];

function TestHarness({
  initialValue = null,
  commitOnIdle = true,
}: {
  initialValue?: MarketScanPeriodRange | null;
  commitOnIdle?: boolean;
}) {
  const [period, setPeriod] = useState<MarketScanPeriodRange | null>(initialValue);

  return (
    <div>
      <DeckPeriodTimeline
        options={PERIOD_OPTIONS}
        value={period}
        onChange={setPeriod}
        commitOnIdle={commitOnIdle}
      />
      <div>{`Active period: ${period ? `${period.start}~${period.end}` : "latest"}`}</div>
    </div>
  );
}

describe("DeckPeriodTimeline", () => {
  it("uses the country default anchor but preserves explicit selection of the global latest month", () => {
    function CountryHarness() {
      const [period, setPeriod] = useState<MarketScanPeriodRange | null>(null);
      return <><DeckPeriodTimeline options={PERIOD_OPTIONS} defaultPeriod="2026-03" value={period} onChange={setPeriod} commitOnIdle={false} /><span>{period ? period.end : "default"}</span></>;
    }
    render(<CountryHarness />);
    expect(screen.getByText("26.03")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "展开时间轴" }));
    const start = screen.getByRole("slider", { name: "开始月份" });
    const end = screen.getByRole("slider", { name: "结束月份" });
    expect(start.getAttribute("value")).toBe("1");
    fireEvent.change(end, { target: { value: "2" } }); fireEvent.pointerUp(end);
    fireEvent.change(start, { target: { value: "2" } }); fireEvent.pointerUp(start);
    expect(screen.getByText("2026-04")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "回到默认最新月" }));
    expect(screen.getByText("default")).toBeTruthy();
    expect(start.getAttribute("value")).toBe("1");
    expect(end.getAttribute("value")).toBe("1");
  });
  afterEach(() => {
    cleanup();
  });

  it("starts collapsed and defaults to latest month", () => {
    render(<TestHarness />);

    expect(screen.queryByRole("slider")).toBeNull();
    expect(screen.getByText("26.04")).toBeTruthy();
    expect(screen.getByText("当前默认最新月当月")).toBeTruthy();
  });

  it("lets users pick a custom range and collapse back to latest", async () => {
    render(<TestHarness />);

    fireEvent.click(screen.getByRole("button", { name: "展开时间轴" }));
    fireEvent.change(screen.getByRole("slider", { name: "开始月份" }), { target: { value: "0" } });
    expect(screen.getByText("Active period: latest")).toBeTruthy();
    await waitFor(() => {
      expect(screen.getByText("Active period: 2026-02~2026-04")).toBeTruthy();
    }, { timeout: 1500 });

    fireEvent.click(screen.getByRole("button", { name: "回到默认最新月" }));
    expect(screen.getByText("Active period: latest")).toBeTruthy();
  });

  it("can defer drag commits until pointer release", () => {
    render(<TestHarness commitOnIdle={false} />);

    fireEvent.click(screen.getByRole("button", { name: "展开时间轴" }));
    const startSlider = screen.getByRole("slider", { name: "开始月份" });
    fireEvent.change(startSlider, { target: { value: "0" } });
    expect(screen.getByText("Active period: latest")).toBeTruthy();

    fireEvent.pointerUp(startSlider);
    expect(screen.getByText("Active period: 2026-02~2026-04")).toBeTruthy();
  });
});
