import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { CatMascot, resolveCatMascotMode } from "../../components/CatMascot";

describe("resolveCatMascotMode", () => {
  it("uses curious mode when the chat drawer is open", () => {
    expect(resolveCatMascotMode(true)).toBe("curious");
  });

  it("uses idle mode when the chat drawer is closed", () => {
    expect(resolveCatMascotMode(false)).toBe("idle");
  });
});

describe("CatMascot", () => {
  it("renders the local SVG mascot without depending on Spline markup", () => {
    const markup = renderToStaticMarkup(<CatMascot chatOpen size={64} />);

    expect(markup).toContain("cat-mascot--curious");
    expect(markup).toContain("<svg");
    expect(markup).not.toContain("spline");
  });
});