import type { CountryChatMetadataResponse } from "../types";

export function isKnownCountryValue(
  metadata: CountryChatMetadataResponse | null,
  country: string,
): boolean {
  const countries = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];
  if (countries.length === 0 || !country) {
    return false;
  }
  return countries.some((item) => item.value === country);
}

export function resolveCountrySelection({
  metadata,
  preferredCountry,
  selectedCountry,
  userPicked,
}: {
  metadata: CountryChatMetadataResponse | null;
  preferredCountry: string;
  selectedCountry: string;
  userPicked: boolean;
}): string {
  const availableCountries = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];

  if (selectedCountry && isKnownCountryValue(metadata, selectedCountry)) {
    return selectedCountry;
  }
  if (!userPicked && preferredCountry && isKnownCountryValue(metadata, preferredCountry)) {
    return preferredCountry;
  }
  return availableCountries[0]?.value ?? "";
}
