// @vitest-environment jsdom

import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { LazyPlotlyChart } from "../../components/LazyPlotlyChart";

vi.mock("../../components/PlotlyChart", () => ({
  PlotlyChart: () => <div data-testid="plotly-chart" />,
}));

let observerCallback: IntersectionObserverCallback | null = null;
let observerOptions: IntersectionObserverInit | undefined;

class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | Document | null = null;
  readonly rootMargin: string;
  readonly thresholds: readonly number[] = [];

  constructor(callback: IntersectionObserverCallback, options?: IntersectionObserverInit) {
    observerCallback = callback;
    observerOptions = options;
    this.rootMargin = options?.rootMargin ?? "0px";
  }

  disconnect = vi.fn();
  observe = vi.fn();
  takeRecords = vi.fn((): IntersectionObserverEntry[] => []);
  unobserve = vi.fn();
}

describe("LazyPlotlyChart", () => {
  beforeEach(() => {
    observerCallback = null;
    observerOptions = undefined;
    vi.stubGlobal("IntersectionObserver", MockIntersectionObserver);
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("waits for viewport proximity before loading Plotly", async () => {
    render(<LazyPlotlyChart data={[]} height={320} />);

    expect(screen.queryByTestId("plotly-chart")).toBeNull();
    expect(screen.getByText("正在加载图表运行时")).toBeTruthy();
    expect(observerOptions?.rootMargin).toBe("160px 0px");

    const target = document.querySelector("[style]") as Element;
    await act(async () => {
      observerCallback?.([
        {
          isIntersecting: true,
          target,
        } as IntersectionObserverEntry,
      ], new MockIntersectionObserver(() => undefined));
    });

    expect(await screen.findByTestId("plotly-chart")).toBeTruthy();
  });

  it("can defer Plotly loading after the chart reaches the viewport", async () => {
    vi.useFakeTimers();
    try {
      render(<LazyPlotlyChart data={[]} height={320} deferMs={3500} />);

      const target = document.querySelector("[style]") as Element;
      await act(async () => {
        observerCallback?.([
          {
            isIntersecting: true,
            target,
          } as IntersectionObserverEntry,
        ], new MockIntersectionObserver(() => undefined));
      });

      expect(screen.queryByTestId("plotly-chart")).toBeNull();

      await act(async () => {
        vi.advanceTimersByTime(3499);
      });
      expect(screen.queryByTestId("plotly-chart")).toBeNull();

      await act(async () => {
        vi.advanceTimersByTime(1);
        await Promise.resolve();
      });

      expect(screen.getByTestId("plotly-chart")).toBeTruthy();
    } finally {
      vi.useRealTimers();
    }
  });
});
