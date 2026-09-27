// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { BomFobRepriceAuditCard } from "../../components/orderGenius";

const auditPayload = {
  filters: { materialCodes: [], countryCode: null },
  fingerprint: "audit-123",
  summary: {
    rows: 5,
    autoReprice: 2,
    alreadyCorrect: 1,
    missingBase: 0,
    ambiguousBase: 2,
    explicitFinal: 0,
    missingTier: 0,
    missingRule: 0,
    notApplicable: 0,
  },
  items: [
    {
      materialCode: "T1DUAL",
      brand: "JAECOO",
      modelName: "JAECOO8 SHS",
      version: "Luxury-AWD",
      bomTemplate: "T1**",
      colourCode: "ZE",
      colourName: "Black & White",
      colourTier: "dual",
      countryCode: "AT",
      paymentTermCode: "TT",
      currentBaseFobEur: 28000,
      currentColourSurchargeEur: 0,
      currentFinalFobEur: 28000,
      currentSourceMode: "manual_edit",
      currentUploadedFobEur: 28000,
      category: "auto_reprice",
      reason: "derived_price_or_metadata_drift",
      trustedSingleBaseFobEur: 28000,
      surchargeEur: 300,
      expectedFinalFobEur: 28300,
    },
    {
      materialCode: "T1META",
      brand: "JAECOO",
      modelName: "JAECOO8 SHS",
      version: "Luxury-AWD",
      bomTemplate: "T1**",
      colourCode: "ZK",
      colourName: "Black & Gray",
      colourTier: "dual",
      countryCode: "AT",
      paymentTermCode: "TT",
      currentBaseFobEur: null,
      currentColourSurchargeEur: null,
      currentFinalFobEur: 28300,
      currentSourceMode: "manual_edit",
      currentUploadedFobEur: 28300,
      category: "auto_reprice",
      reason: "derived_price_or_metadata_drift",
      trustedSingleBaseFobEur: 28000,
      surchargeEur: 300,
      expectedFinalFobEur: 28300,
    },
    {
      materialCode: "T1AMB",
      brand: "JAECOO",
      modelName: "JAECOO8 SHS",
      version: "Luxury-AWD",
      bomTemplate: "T1**",
      colourCode: "UE",
      colourName: "Matte Gray",
      colourTier: "special",
      countryCode: "AT",
      paymentTermCode: "TT",
      currentBaseFobEur: null,
      currentColourSurchargeEur: null,
      currentFinalFobEur: 28300,
      currentSourceMode: "manual_edit",
      currentUploadedFobEur: 28300,
      category: "ambiguous_base",
      reason: "multiple_single_base_prices",
      singleBaseCandidates: [28000, 28300],
    },
    {
      materialCode: "T1AMB2",
      brand: "JAECOO",
      modelName: "JAECOO8 SHS",
      version: "Luxury-AWD",
      bomTemplate: "T1**",
      colourCode: "ZK",
      colourName: "Black & Gray",
      colourTier: "dual",
      countryCode: "CZ",
      paymentTermCode: "LC",
      currentBaseFobEur: null,
      currentColourSurchargeEur: null,
      currentFinalFobEur: 28150,
      currentSourceMode: "country_copy",
      currentUploadedFobEur: 28150,
      category: "ambiguous_base",
      reason: "multiple_single_base_prices",
      singleBaseCandidates: [27850, 28150],
    },
  ],
};

beforeEach(() => {
  vi.stubGlobal("localStorage", {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
    clear: () => undefined,
  });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("BOM FOB reprice audit controls", () => {
  it("previews tier-derived changes before applying the fingerprinted plan", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "POST") {
        return Response.json({
          previewFingerprint: "audit-123",
          totals: { requested: 2, updated: 1, unchanged: 1, skipped: 0 },
          details: [],
        });
      }
      return Response.json(auditPayload);
    });
    vi.stubGlobal("fetch", fetchMock);

    const onApplied = vi.fn();
    render(<BomFobRepriceAuditCard onApplied={onApplied} />);

    fireEvent.click(screen.getByRole("button", { name: "Refresh FOB Audit" }));
    expect(await screen.findByRole("button", { name: "Review audit" })).toBeTruthy();
    expect(screen.getByText("price changes")).toBeTruthy();
    expect(screen.getByText("metadata only")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Review audit" }));
    const dialog = screen.getByRole("dialog", { name: "FOB colour reprice audit" });
    expect(dialog.closest(".bom-fob-audit-modal-shell")).toBeTruthy();
    expect(screen.getByText(/T1DUAL · AT/)).toBeTruthy();
    expect(screen.getByText("T1**")).toBeTruthy();
    expect(screen.getByText("2 countries: AT, CZ")).toBeTruthy();
    expect(screen.getByText(/2 affected colour SKUs: UE, ZK/)).toBeTruthy();
    expect(screen.getByText(/Single conflicts: AT 28,000 \/ 28,300 · CZ 27,850 \/ 28,150/)).toBeTruthy();
    expect(screen.getByText(/grouped into 1 actionable issue/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Apply 2 safe rows" }));
    await waitFor(() => expect(onApplied).toHaveBeenCalledTimes(1));

    const applyCall = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(String(applyCall?.[0])).toContain("/order-genius/colour-surcharge-reprice/apply");
    expect(JSON.parse(String(applyCall?.[1]?.body))).toEqual({ previewFingerprint: "audit-123" });
  });

  it("opens the affected BOM template without applying or changing its tier", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => Response.json(auditPayload));
    vi.stubGlobal("fetch", fetchMock);
    const onOpenBomTemplate = vi.fn(async () => undefined);

    render(<BomFobRepriceAuditCard onOpenBomTemplate={onOpenBomTemplate} />);

    fireEvent.click(screen.getByRole("button", { name: "Refresh FOB Audit" }));
    fireEvent.click(await screen.findByRole("button", { name: "Review audit" }));
    fireEvent.click(screen.getByRole("button", { name: "Open BOM template" }));

    await waitFor(() => expect(onOpenBomTemplate).toHaveBeenCalledWith({
      bomTemplate: "T1**",
      brand: "JAECOO",
      modelName: "JAECOO8 SHS",
      version: "Luxury-AWD",
    }));
    expect(fetchMock.mock.calls.every(([, init]) => init?.method !== "POST")).toBe(true);
    expect(screen.queryByRole("dialog", { name: "FOB colour reprice audit" })).toBeNull();
  });
});
