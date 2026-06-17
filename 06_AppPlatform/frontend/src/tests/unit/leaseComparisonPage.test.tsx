// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { LeaseComparisonPage } from "../../pages/LeaseComparisonPage";
import type { MsrpFinanceObservation, MsrpFinanceObservationsResponse } from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
    listMsrpFinanceObservations: vi.fn(),
  },
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: () => ({
    user: {
      role: "viewer",
    },
  }),
}));

vi.mock("../../hooks/useResolvedCountry", () => ({
  useResolvedCountry: () => ({
    primaryCountryISO: "SE",
  }),
}));

function makeFinanceObservation(
  overrides: Partial<MsrpFinanceObservation> = {},
): MsrpFinanceObservation {
  return {
    financeObservationId: "finance-1",
    observationId: "obs-1",
    scrapeBatchId: "batch-1",
    country: "Sweden",
    brand: "Volvo",
    jatoModel: "XC60",
    jatoTrim: "Plus",
    jatoPowertrain: "PHEV",
    officialModel: "XC60",
    officialTrim: "Plus Bright",
    officialEdition: null,
    officialPowertrain: "Recharge",
    priceSemantics: "lease_monthly",
    financeType: "private_lease",
    monthlyPayment: 5990,
    monthlyPaymentEur: 520.87,
    downPayment: 10000,
    downPaymentEur: 869.56,
    downPaymentPct: null,
    termMonths: 36,
    apr: 4.2,
    effectiveApr: null,
    balloonPayment: 250000,
    balloonPaymentEur: 21739.13,
    totalCreditCost: null,
    totalCreditCostEur: null,
    totalAmountPayable: null,
    totalAmountPayableEur: null,
    annualMileageLimit: 15000,
    offerValidUntil: "2026-07-15",
    subsidyAmount: 25000,
    subsidyAmountEur: 2173.91,
    netPriceAfterSubsidy: 720000,
    netPriceAfterSubsidyEur: 62608.7,
    currency: "SEK",
    sourceUrl: "https://www.volvocars.com/se/offers/xc60",
    observedAtUtc: "2026-06-11T20:45:00Z",
    financeContext: { price_semantics: "lease_monthly" },
    createdAtUtc: "2026-06-11T20:45:01Z",
    updatedAtUtc: "2026-06-11T20:45:01Z",
    ...overrides,
  };
}

function makeFinanceResponse(
  items: MsrpFinanceObservation[],
): MsrpFinanceObservationsResponse {
  return {
    rows: items.length,
    total: items.length,
    limit: 100,
    offset: 0,
    summary: {
      priceSemanticsCounts: { lease_monthly: items.length },
      financeTypeCounts: { private_lease: items.length },
      monthlyPaymentCount: items.length,
      monthlyPaymentEurMin: 520.87,
      monthlyPaymentEurMax: 520.87,
      netPriceAfterSubsidyCount: items.length,
      netPriceAfterSubsidyEurMin: 62608.7,
      netPriceAfterSubsidyEurMax: 62608.7,
      subsidyObservationCount: items.length,
    },
    items,
  };
}

describe("LeaseComparisonPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockResolvedValue({ offers: [] });
    vi.mocked(api.post).mockResolvedValue({});
    vi.mocked(api.listMsrpFinanceObservations).mockResolvedValue(
      makeFinanceResponse([makeFinanceObservation()]),
    );
  });

  afterEach(() => {
    cleanup();
  });

  it("renders official finance observations from the MSRP pipeline as lease cards", async () => {
    render(<LeaseComparisonPage />);

    await screen.findByText("Official Finance Observations");

    expect(screen.getByText("Volvo XC60 Plus")).toBeTruthy();
    expect(screen.getAllByText("private_lease").length).toBeGreaterThan(0);
    expect(screen.getByText("5,990 SEK")).toBeTruthy();
    expect(screen.getAllByText("521 EUR").length).toBeGreaterThan(0);
    expect(screen.getByText("Net 62,609 EUR")).toBeTruthy();
    const sourceLink = screen.getByRole("link", { name: "Source" });
    expect(sourceLink.getAttribute("href")).toBe("https://www.volvocars.com/se/offers/xc60");
  });

  it("passes FloatingDeck country, brand, model and finance type filters to official finance observations", async () => {
    render(<LeaseComparisonPage />);

    await screen.findByText("Volvo XC60 Plus");

    fireEvent.change(screen.getByLabelText("Brand"), { target: { value: "Volvo" } });
    fireEvent.change(screen.getByLabelText("Model"), { target: { value: "XC60" } });
    fireEvent.change(screen.getByLabelText("Official finance type"), {
      target: { value: "private" },
    });

    await waitFor(() => {
      expect(vi.mocked(api.listMsrpFinanceObservations)).toHaveBeenLastCalledWith({
        country: "SE",
        brand: "Volvo",
        jato_model: "XC60",
        has_monthly_payment: true,
        limit: 100,
      });
    });
  });

  it("loads a scraped finance observation into the solver inputs", async () => {
    render(<LeaseComparisonPage />);

    await screen.findByText("Volvo XC60 Plus");
    fireEvent.click(screen.getByRole("button", { name: "Use in solver" }));

    expect(await screen.findByText("Volvo XC60 Plus loaded into solver")).toBeTruthy();
    expect((screen.getByLabelText("Monthly Payment") as HTMLInputElement).value).toBe("5990");
    expect((screen.getByLabelText("Cap Cost") as HTMLInputElement).value).toBe("720000");
    expect((screen.getByLabelText("RV") as HTMLInputElement).value).toBe("250000");
    expect((screen.getByLabelText("Term (mo)") as HTMLInputElement).value).toBe("36");
  });
});
