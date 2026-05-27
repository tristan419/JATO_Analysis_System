import { useMemo } from "react";
import { useAuth } from "../contexts/AuthContext";
import {
  resolveDefaultCountry,
  FALLBACK_COUNTRY_ISO,
} from "../utils/jatoCountries";

export interface ResolvedCountry {
  country: string;
  primaryCountryISO: string;
  secondaryCountriesISO: string[];
  allCountriesISO: string[];
  isFromProfile: boolean;
}

type CountryRepresentation = "zh" | "iso";

/**
 * Resolves the default country for the current page from the user's auth profile.
 *
 * URL params take priority over this return value — each page should apply:
 *   `searchParams.get("country") || resolved.country`
 *
 * @param representation — "zh" for Chinese names (deck/dataset pages), "iso" for Order Genius
 */
export function useResolvedCountry(
  representation: CountryRepresentation = "zh",
): ResolvedCountry {
  const { user } = useAuth();

  return useMemo(() => {
    const primaryISO = user?.primaryCountry ?? null;
    const secondaryISO = user?.secondaryCountries ?? [];
    const allISO: string[] = primaryISO
      ? [primaryISO, ...secondaryISO.filter((c) => c !== primaryISO)]
      : [];

    const resolved = resolveDefaultCountry(primaryISO, representation);

    return {
      country: resolved,
      primaryCountryISO: primaryISO ?? FALLBACK_COUNTRY_ISO,
      secondaryCountriesISO: secondaryISO,
      allCountriesISO: allISO.length > 0 ? allISO : [FALLBACK_COUNTRY_ISO],
      isFromProfile: !!primaryISO,
    };
  }, [user?.primaryCountry, user?.secondaryCountries, representation]);
}
