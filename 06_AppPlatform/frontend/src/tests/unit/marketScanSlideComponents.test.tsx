import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { SlideFitSummary } from "../../components/SlideFitSummary";
import { SlideLayoutEditor } from "../../components/SlideLayoutEditor";
import { DEFAULT_SLIDE_LAYOUT } from "../../utils/slideLayout";

describe("SlideLayoutEditor", () => {
  it("renders all slide layout controls with current values", () => {
    const markup = renderToStaticMarkup(
      <SlideLayoutEditor
        value={{
          ...DEFAULT_SLIDE_LAYOUT,
          paddingX: 40,
          bodyGap: 18,
        }}
        onChange={() => undefined}
        onReset={() => undefined}
      />,
    );

    expect(markup).toContain("Layout Edit");
    expect(markup).toContain("Reset layout");
    expect(markup).toContain("左右边距");
    expect(markup).toContain("上下边距");
    expect(markup).toContain("内容块间距");
    expect(markup).toContain("40px");
    expect(markup.match(/type=\"range\"/g)?.length ?? 0).toBe(6);
  });
});

describe("SlideFitSummary", () => {
  it("caps action chips at three while keeping the status styling", () => {
    const markup = renderToStaticMarkup(
      <SlideFitSummary
        assessment={{
          status: "split",
          score: 98,
          summary: "当前页已超出推荐密度，建议至少拆成 2 页。",
          splitSlides: 2,
          issues: [],
          recommendedActions: [
            "动作一",
            "动作二",
            "动作三",
            "动作四",
          ],
        }}
      />,
    );

    expect(markup).toContain("slide-fit-summary--split");
    expect(markup).toContain("Need Split");
    expect(markup).toContain("动作一");
    expect(markup).toContain("动作二");
    expect(markup).toContain("动作三");
    expect(markup).not.toContain("动作四");
  });

  it("omits the action container when there are no recommendations", () => {
    const markup = renderToStaticMarkup(
      <SlideFitSummary
        assessment={{
          status: "safe",
          score: 0,
          summary: "当前页密度适合固定 1920×1080 导出。",
          splitSlides: 1,
          issues: [],
          recommendedActions: [],
        }}
      />,
    );

    expect(markup).toContain("slide-fit-summary--safe");
    expect(markup).not.toContain("slide-fit-actions");
  });
});