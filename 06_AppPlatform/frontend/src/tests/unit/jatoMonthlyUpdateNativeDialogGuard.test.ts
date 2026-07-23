import { describe, expect, it } from "vitest";

import pageSource from "../../pages/JatoMonthlyUpdatePage.tsx?raw";

describe("JATO monthly update dialog guard", () => {
  it("keeps browser-native confirm and alert calls out of the page", () => {
    expect(pageSource).not.toMatch(/\bwindow\.(?:confirm|alert)\s*\(/);
    expect(pageSource).toContain("<ConfirmDialog");
  });
});
