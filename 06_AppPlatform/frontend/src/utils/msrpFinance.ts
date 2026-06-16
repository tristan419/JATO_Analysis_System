import type { CurrentPrice, MsrpFinanceObservation } from "../types";

function normalizeText(value: string | null | undefined): string {
  return (value ?? "").trim().toLowerCase();
}

function normalizeCountry(value: string | null | undefined): string {
  const normalized = normalizeText(value);
  const aliases: Record<string, string> = {
    se: "sweden",
    sverige: "sweden",
    sweden: "sweden",
    no: "norway",
    norge: "norway",
    norway: "norway",
    dk: "denmark",
    danmark: "denmark",
    denmark: "denmark",
    fi: "finland",
    suomi: "finland",
    finland: "finland",
    de: "germany",
    deutschland: "germany",
    germany: "germany",
    nl: "netherlands",
    netherlands: "netherlands",
  };
  return aliases[normalized] ?? normalized;
}

function includesNormalized(value: string | null | undefined, query: string): boolean {
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) return true;
  return normalizeText(value).includes(normalizedQuery);
}

export function formatFinanceNumber(value: number | null | undefined, maximumFractionDigits = 0): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return value.toLocaleString(undefined, { maximumFractionDigits });
}

export function formatFinanceCurrency(
  value: number | null | undefined,
  currency = "EUR",
  maximumFractionDigits = 0,
): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${formatFinanceNumber(value, maximumFractionDigits)} ${currency}`;
}

export function formatFinanceDate(value: string | null | undefined): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

export function getFinanceObservationLabel(item: MsrpFinanceObservation): string {
  return item.financeType || item.priceSemantics || "finance offer";
}

export function getFinanceObservationMonthlyEur(item: MsrpFinanceObservation): number | null {
  return item.monthlyPaymentEur ?? item.monthlyPayment ?? null;
}

export function formatFinanceMonthlyPayment(item: MsrpFinanceObservation): string {
  if (item.monthlyPaymentEur !== null) {
    return formatFinanceCurrency(item.monthlyPaymentEur);
  }
  return formatFinanceCurrency(item.monthlyPayment, item.currency);
}

export function getFinanceObservationModelLabel(item: MsrpFinanceObservation): string {
  return [item.brand, item.jatoModel, item.jatoTrim].filter(Boolean).join(" ");
}

export function matchesFinanceObservationFilters(
  item: MsrpFinanceObservation,
  filters: { country?: string; brand?: string; model?: string; financeType?: string },
): boolean {
  const countryQuery = normalizeCountry(filters.country);
  const itemCountry = normalizeCountry(item.country);
  const countryMatches = !countryQuery || itemCountry.includes(countryQuery) || countryQuery.includes(itemCountry);
  const modelHaystack = [
    item.jatoModel,
    item.jatoTrim,
    item.officialModel,
    item.officialTrim,
    item.officialEdition,
    item.officialPowertrain,
  ].filter(Boolean).join(" ");

  return countryMatches
    && includesNormalized(item.brand, filters.brand ?? "")
    && includesNormalized(modelHaystack, filters.model ?? "")
    && includesNormalized(getFinanceObservationLabel(item), filters.financeType ?? "");
}

export function matchesFinanceObservationCurrentPrice(
  item: MsrpFinanceObservation,
  price: CurrentPrice,
): boolean {
  const sameCountry = normalizeCountry(item.country) === normalizeCountry(price.country);
  const sameBrand = normalizeText(item.brand) === normalizeText(price.brand);
  const itemModel = normalizeText(item.jatoModel || item.officialModel);
  const priceModel = normalizeText(price.jatoModel || price.officialModel);
  const modelMatches = Boolean(itemModel && priceModel && (
    itemModel.includes(priceModel) || priceModel.includes(itemModel)
  ));

  return sameCountry && sameBrand && modelMatches;
}

export function getFinanceObservationsForCurrentPrice(
  observations: MsrpFinanceObservation[],
  price: CurrentPrice,
): MsrpFinanceObservation[] {
  return observations
    .filter((item) => matchesFinanceObservationCurrentPrice(item, price))
    .sort((left, right) => {
      const leftMonthly = getFinanceObservationMonthlyEur(left);
      const rightMonthly = getFinanceObservationMonthlyEur(right);
      if (leftMonthly !== null && rightMonthly !== null && leftMonthly !== rightMonthly) {
        return leftMonthly - rightMonthly;
      }
      return right.observedAtUtc.localeCompare(left.observedAtUtc);
    });
}
