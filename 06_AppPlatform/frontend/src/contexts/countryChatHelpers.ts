import type {
  CountryChatMetadataResponse,
  CountryChatModelOption,
} from "../types";

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

export function availableChatModels(
  metadata: CountryChatMetadataResponse | null,
): CountryChatModelOption[] {
  return Array.isArray(metadata?.availableChatModels)
    ? metadata.availableChatModels.filter((item) => item?.available !== false)
    : [];
}

export function isKnownChatModelValue(
  metadata: CountryChatMetadataResponse | null,
  chatModel: string,
): boolean {
  const models = availableChatModels(metadata);
  if (models.length === 0 || !chatModel) {
    return false;
  }
  return models.some((item) => item.id === chatModel);
}

export function resolveChatModelSelection({
  metadata,
  selectedChatModel,
}: {
  metadata: CountryChatMetadataResponse | null;
  selectedChatModel: string;
}): string {
  if (selectedChatModel && isKnownChatModelValue(metadata, selectedChatModel)) {
    return selectedChatModel;
  }
  const preferred = String(metadata?.defaultChatModel ?? "").trim();
  if (preferred && isKnownChatModelValue(metadata, preferred)) {
    return preferred;
  }
  return availableChatModels(metadata)[0]?.id ?? "";
}

export function buildCountryChatSessionKey(
  country: string,
  chatModel: string,
): string {
  return `${country}::${chatModel || "auto"}`;
}

export function getChatModelLabel(
  metadata: CountryChatMetadataResponse | null,
  chatModel: string,
): string {
  const match = availableChatModels(metadata).find((item) => item.id === chatModel);
  return match?.label ?? chatModel;
}
