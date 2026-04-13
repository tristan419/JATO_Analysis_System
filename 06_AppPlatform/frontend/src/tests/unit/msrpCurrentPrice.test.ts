import { describe, expect, it } from "vitest";

import type { CurrentPrice } from "../../types";
import {
  averageCurrentMsrpValue,
  buildCurrentPriceGroupKey,
  formatCurrentPriceDate,
  formatCurrentPriceNumber,
  resolveCurrentPriceGroupModel,
  resolveCurrentMsrpValue,
  resolveLastPriceChangeAtUtc,
  resolveUpdatedAtUtc,
} from "../../utils/msrpCurrentPrice";

function makeCurrentPrice(overrides: Partial<CurrentPrice> = {}): CurrentPrice {
  return {
    id: "cp-1",
    country: "Sweden",
    brand: "Volvo",
    jatoModel: "XC60",
    jatoTrim: "Ultra",
    jatoPowertrain: null,
    officialModel: "XC60",
    officialTrim: "Ultra",
    officialEdition: null,
    officialPowertrain: null,
    effectiveObservationId: "obs-1",
    currentMsrpValue: 529900,
    currency: "SEK",
    taxIncluded: true,
    matchConfidence: 0.96,
    matchStatus: "matched",
    sourceUrl: "https://example.test/xc60",
    sourceSnapshotPath: null,
    lastPriceChangeAtUtc: "2026-04-10T08:00:00+00:00",
    updatedAtUtc: "2026-04-10T09:00:00+00:00",
    ...overrides,
  };
}

describe("msrpCurrentPrice contract helpers", () => {
  it("prefers current price fields from the backend serializer", () => {
    const price = makeCurrentPrice({
      currentMsrpValue: 549900,
      msrpValue: 539900,
    });

    expect(resolveCurrentMsrpValue(price)).toBe(549900);
    expect(resolveLastPriceChangeAtUtc(price)).toBe("2026-04-10T08:00:00+00:00");
    expect(resolveUpdatedAtUtc(price)).toBe("2026-04-10T09:00:00+00:00");
  });

  it("falls back to legacy aliases when the current-price fields are missing", () => {
    const price = makeCurrentPrice({
      currentMsrpValue: undefined,
      lastPriceChangeAtUtc: null,
      updatedAtUtc: undefined,
      msrpValue: 519900,
      observedAtUtc: "2026-04-09T08:00:00+00:00",
      materializedAt: "2026-04-09T09:00:00+00:00",
    });

    expect(resolveCurrentMsrpValue(price)).toBe(519900);
    expect(resolveLastPriceChangeAtUtc(price)).toBe("2026-04-09T08:00:00+00:00");
    expect(resolveUpdatedAtUtc(price)).toBe("2026-04-09T09:00:00+00:00");
  });

  it("formats invalid values as placeholders instead of crashing", () => {
    expect(formatCurrentPriceNumber(undefined)).toBe("—");
    expect(formatCurrentPriceNumber(Number.NaN)).toBe("—");
    expect(formatCurrentPriceDate(undefined)).toBe("—");
    expect(formatCurrentPriceDate("not-a-date")).toBe("—");
  });

  it("averages only resolvable msrp values", () => {
    const prices = [
      makeCurrentPrice({ id: "cp-1", currentMsrpValue: 500000 }),
      makeCurrentPrice({ id: "cp-2", currentMsrpValue: undefined, msrpValue: 530000 }),
      makeCurrentPrice({ id: "cp-3", currentMsrpValue: Number.NaN, msrpValue: undefined }),
    ];

    expect(averageCurrentMsrpValue(prices)).toBe(515000);
  });

  it("prefers jato model when building model groups", () => {
    const price = makeCurrentPrice({
      country: "Sweden",
      brand: "Volvo",
      jatoModel: "XC60",
      officialModel: "XC60 Recharge",
    });

    expect(resolveCurrentPriceGroupModel(price)).toBe("XC60");
    expect(buildCurrentPriceGroupKey(price)).toBe("Sweden::Volvo::XC60");
  });
});