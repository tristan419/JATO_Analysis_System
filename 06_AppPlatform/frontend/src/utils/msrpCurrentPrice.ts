export interface CurrentPriceContract {
  country?: string;
  brand?: string;
  jatoModel?: string | null;
  officialModel?: string | null;
  currentMsrpValue?: number | null;
  msrpValue?: number | null;
  lastPriceChangeAtUtc?: string | null;
  observedAtUtc?: string | null;
  updatedAtUtc?: string | null;
  materializedAt?: string | null;
}

export function resolveCurrentPriceGroupModel(price: CurrentPriceContract): string {
  const jatoModel = price.jatoModel?.trim();
  if (jatoModel) {
    return jatoModel;
  }

  const officialModel = price.officialModel?.trim();
  if (officialModel) {
    return officialModel;
  }

  return "待映射";
}

export function buildCurrentPriceGroupKey(price: CurrentPriceContract): string {
  return [
    price.country ?? "",
    price.brand ?? "",
    resolveCurrentPriceGroupModel(price),
  ].join("::");
}

export function resolveCurrentMsrpValue(price: CurrentPriceContract): number | null {
  const value = price.currentMsrpValue ?? price.msrpValue;
  if (value === null || value === undefined || Number.isNaN(value)) {
    return null;
  }
  return value;
}

export function resolveLastPriceChangeAtUtc(price: CurrentPriceContract): string | null {
  return price.lastPriceChangeAtUtc ?? price.observedAtUtc ?? null;
}

export function resolveUpdatedAtUtc(price: CurrentPriceContract): string | null {
  return price.updatedAtUtc ?? price.materializedAt ?? null;
}

export function formatCurrentPriceNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return Math.round(value).toLocaleString();
}

export function formatCurrentPriceDate(value: string | null | undefined): string {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "—";
  }
  return parsed.toLocaleDateString();
}

export function averageCurrentMsrpValue(prices: CurrentPriceContract[]): number {
  const values = prices
    .map(resolveCurrentMsrpValue)
    .filter((value): value is number => value !== null);

  if (values.length === 0) {
    return 0;
  }

  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}