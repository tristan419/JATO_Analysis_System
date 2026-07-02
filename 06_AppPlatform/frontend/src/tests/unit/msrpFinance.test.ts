import { describe, expect, it } from "vitest";

import type { CurrentPrice, MsrpFinanceObservation } from "../../types";
import {
  formatFinanceCurrencyRange,
  formatFinanceMonthlyPayment,
  getFinanceObservationsForCurrentPrice,
  getFinanceObservationValidity,
  getFinanceObservationValidityBadgeClass,
  getFinanceObservationValidityLabel,
  matchesFinanceObservationFilters,
} from "../../utils/msrpFinance";

function makeFinanceObservation(overrides: Partial<MsrpFinanceObservation> = {}): MsrpFinanceObservation {
  return {
    financeObservationId: "fo-1",
    observationId: "obs-1",
    scrapeBatchId: "batch-1",
    country: "Sweden",
    brand: "Volvo",
    jatoModel: "XC60",
    jatoTrim: "Ultra",
    jatoPowertrain: "PHEV",
    officialModel: "XC60 Recharge",
    officialTrim: "Ultra",
    officialEdition: null,
    officialPowertrain: "PHEV",
    priceSemantics: "lease_monthly",
    financeType: "private_lease",
    monthlyPayment: 5990,
    monthlyPaymentEur: 520.87,
    downPayment: 40000,
    downPaymentEur: 3478.26,
    downPaymentPct: 5,
    termMonths: 36,
    apr: 3.9,
    effectiveApr: 4.2,
    balloonPayment: 250000,
    balloonPaymentEur: 21739.13,
    totalCreditCost: 45000,
    totalCreditCostEur: 3913.04,
    totalAmountPayable: 860000,
    totalAmountPayableEur: 74782.61,
    annualMileageLimit: 15000,
    offerValidUntil: "2026-06-30",
    subsidyAmount: 25000,
    subsidyAmountEur: 2173.91,
    netPriceAfterSubsidy: 748000,
    netPriceAfterSubsidyEur: 65043.48,
    currency: "SEK",
    sourceUrl: "https://example.test/xc60",
    observedAtUtc: "2026-04-11T09:10:00+00:00",
    financeContext: { price_semantics: "lease_monthly" },
    createdAtUtc: "2026-04-11T09:10:00+00:00",
    updatedAtUtc: "2026-04-11T09:10:00+00:00",
    ...overrides,
  };
}

function makeCurrentPrice(overrides: Partial<CurrentPrice> = {}): CurrentPrice {
  return {
    id: "cp-1",
    country: "SE",
    brand: "Volvo",
    jatoModel: "XC60",
    jatoTrim: "Ultra",
    jatoPowertrain: "PHEV",
    officialModel: "XC60 Recharge",
    officialTrim: "Ultra",
    officialEdition: null,
    officialPowertrain: "PHEV",
    effectiveObservationId: "obs-1",
    currentMsrpValue: 67217.39,
    currency: "EUR",
    taxIncluded: true,
    matchConfidence: 0.91,
    matchStatus: "auto_accepted",
    sourceUrl: "https://example.test/xc60",
    sourceSnapshotPath: null,
    lastPriceChangeAtUtc: "2026-04-11T09:10:00+00:00",
    updatedAtUtc: "2026-04-11T09:10:00+00:00",
    ...overrides,
  };
}

describe("msrp finance helpers", () => {
  it("matches country aliases and text filters", () => {
    expect(matchesFinanceObservationFilters(makeFinanceObservation(), {
      country: "SE",
      brand: "vol",
      model: "recharge",
      financeType: "lease",
    })).toBe(true);
  });

  it("sorts current-price finance matches by monthly EUR", () => {
    const matches = getFinanceObservationsForCurrentPrice([
      makeFinanceObservation({ financeObservationId: "higher", monthlyPaymentEur: 720 }),
      makeFinanceObservation({ financeObservationId: "lower", monthlyPaymentEur: 510 }),
      makeFinanceObservation({ financeObservationId: "other-brand", brand: "BMW", monthlyPaymentEur: 400 }),
    ], makeCurrentPrice());

    expect(matches.map((item) => item.financeObservationId)).toEqual(["lower", "higher"]);
  });

  it("formats monthly payment in EUR first", () => {
    expect(formatFinanceMonthlyPayment(makeFinanceObservation({ monthlyPaymentEur: 520.87 }))).toBe("521 EUR");
    expect(formatFinanceMonthlyPayment(makeFinanceObservation({ monthlyPaymentEur: null }))).toBe("5,990 SEK");
  });

  it("formats monthly payment ranges for finance summaries", () => {
    expect(formatFinanceCurrencyRange(500, 700)).toBe("500 EUR - 700 EUR");
    expect(formatFinanceCurrencyRange(520, 520)).toBe("520 EUR");
    expect(formatFinanceCurrencyRange(null, 700)).toBe("-");
  });

  it("classifies finance observation validity windows", () => {
    const now = new Date("2026-06-17T00:00:00Z");
    expect(getFinanceObservationValidity(makeFinanceObservation({ offerValidUntil: "2026-07-15" }), now)).toBe("active");
    expect(getFinanceObservationValidity(makeFinanceObservation({ offerValidUntil: "2026-06-20" }), now)).toBe("expiresSoon");
    expect(getFinanceObservationValidity(makeFinanceObservation({ offerValidUntil: "2026-06-01" }), now)).toBe("expired");
    expect(getFinanceObservationValidity(makeFinanceObservation({ offerValidUntil: null }), now)).toBe("undated");
  });

  it("maps finance validity to display labels and badge classes", () => {
    const now = new Date("2026-06-17T00:00:00Z");
    const expired = makeFinanceObservation({ offerValidUntil: "2026-06-01" });
    expect(getFinanceObservationValidityLabel(expired, now)).toBe("Expired");
    expect(getFinanceObservationValidityBadgeClass(expired, now)).toBe("badge-danger");
  });
});
