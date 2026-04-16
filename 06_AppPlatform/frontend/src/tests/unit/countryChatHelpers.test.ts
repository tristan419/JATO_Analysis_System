import { describe, expect, it } from "vitest";

import { resolveCountrySelection } from "../../contexts/countryChatHelpers";
import type { CountryChatMetadataResponse } from "../../types";

const metadata: CountryChatMetadataResponse = {
  availableCountries: [
    { value: "德国", label: "Germany" },
    { value: "法国", label: "France" },
    { value: "中国", label: "China" },
  ],
  provider: "test",
  providerAvailable: true,
  suggestedPrompts: [],
};

describe("resolveCountrySelection", () => {
  it("keeps the user's manual country choice when it is still valid", () => {
    expect(
      resolveCountrySelection({
        metadata,
        preferredCountry: "德国",
        selectedCountry: "法国",
        userPicked: true,
      }),
    ).toBe("法国");
  });

  it("uses the shared preferred country before the user picks manually", () => {
    expect(
      resolveCountrySelection({
        metadata,
        preferredCountry: "德国",
        selectedCountry: "",
        userPicked: false,
      }),
    ).toBe("德国");
  });

  it("falls back to the first available country when the manual choice is invalid", () => {
    expect(
      resolveCountrySelection({
        metadata,
        preferredCountry: "德国",
        selectedCountry: "西班牙",
        userPicked: true,
      }),
    ).toBe("德国");
  });
});
