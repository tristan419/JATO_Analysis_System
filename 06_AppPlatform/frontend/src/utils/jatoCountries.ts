export interface JatoCountryOption {
  countryCode: string;
  countryName: string;
  countryNameZh: string;
  marketScanCountry: string;
}

export const JATO_COUNTRIES: JatoCountryOption[] = [
  { countryCode: "DE", countryName: "Germany", countryNameZh: "德国", marketScanCountry: "德国" },
  { countryCode: "FR", countryName: "France", countryNameZh: "法国", marketScanCountry: "法国" },
  { countryCode: "IT", countryName: "Italy", countryNameZh: "意大利", marketScanCountry: "意大利" },
  { countryCode: "ES", countryName: "Spain", countryNameZh: "西班牙", marketScanCountry: "西班牙" },
  { countryCode: "SE", countryName: "Sweden", countryNameZh: "瑞典", marketScanCountry: "瑞典" },
  { countryCode: "NO", countryName: "Norway", countryNameZh: "挪威", marketScanCountry: "挪威" },
  { countryCode: "DK", countryName: "Denmark", countryNameZh: "丹麦", marketScanCountry: "丹麦" },
  { countryCode: "FI", countryName: "Finland", countryNameZh: "芬兰", marketScanCountry: "芬兰" },
  { countryCode: "AT", countryName: "Austria", countryNameZh: "奥地利", marketScanCountry: "奥地利" },
  { countryCode: "CH", countryName: "Switzerland", countryNameZh: "瑞士", marketScanCountry: "瑞士" },
  { countryCode: "NL", countryName: "Netherlands", countryNameZh: "荷兰", marketScanCountry: "荷兰" },
  { countryCode: "BE", countryName: "Belgium", countryNameZh: "比利时", marketScanCountry: "比利时" },
  { countryCode: "PL", countryName: "Poland", countryNameZh: "波兰", marketScanCountry: "波兰" },
  { countryCode: "CZ", countryName: "Czechia", countryNameZh: "捷克", marketScanCountry: "捷克" },
  { countryCode: "HU", countryName: "Hungary", countryNameZh: "匈牙利", marketScanCountry: "匈牙利" },
  { countryCode: "HR", countryName: "Croatia", countryNameZh: "克罗地亚", marketScanCountry: "克罗地亚" },
  { countryCode: "SI", countryName: "Slovenia", countryNameZh: "斯洛文尼亚", marketScanCountry: "斯洛文尼亚" },
  { countryCode: "RO", countryName: "Romania", countryNameZh: "罗马尼亚", marketScanCountry: "罗马尼亚" },
  { countryCode: "SK", countryName: "Slovakia", countryNameZh: "斯洛伐克", marketScanCountry: "斯洛伐克" },
  { countryCode: "GR", countryName: "Greece", countryNameZh: "希腊", marketScanCountry: "希腊" },
  { countryCode: "PT", countryName: "Portugal", countryNameZh: "葡萄牙", marketScanCountry: "葡萄牙" },
];

const COUNTRY_BY_CODE = new Map(
  JATO_COUNTRIES.map((country) => [country.countryCode, country]),
);

export function getJatoCountryByCode(countryCode: string | null | undefined): JatoCountryOption | null {
  const normalized = String(countryCode ?? "").trim().toUpperCase();
  return COUNTRY_BY_CODE.get(normalized) ?? null;
}

export function countryCodeToDatasetCountry(countryCode: string | null | undefined): string | null {
  return getJatoCountryByCode(countryCode)?.marketScanCountry ?? null;
}

export function formatJatoCountryOption(country: JatoCountryOption): string {
  return `${country.countryNameZh} / ${country.countryName} (${country.countryCode})`;
}

/** Fallback country for admin, anonymous, or unset profile. */
export const FALLBACK_COUNTRY_ISO = "SE";
export const FALLBACK_COUNTRY_ZH = "瑞典";

/**
 * Resolve the default country from a user profile ISO code.
 * Falls back to Sweden when the primaryCountry is unset or unrecognized.
 */
export function resolveDefaultCountry(
  primaryCountry: string | null | undefined,
  representation: "zh" | "iso",
): string {
  if (representation === "iso") {
    return getJatoCountryByCode(primaryCountry)?.countryCode ?? FALLBACK_COUNTRY_ISO;
  }
  return countryCodeToDatasetCountry(primaryCountry) ?? FALLBACK_COUNTRY_ZH;
}
