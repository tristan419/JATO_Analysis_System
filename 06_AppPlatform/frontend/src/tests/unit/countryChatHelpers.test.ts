import { describe, expect, it } from "vitest";

import {
  buildCountryChatSessionKey,
  resolveChatModelSelection,
  resolveCountrySelection,
} from "../../contexts/countryChatHelpers";
import type { CountryChatMetadataResponse } from "../../types";

const metadata: CountryChatMetadataResponse = {
  availableCountries: [
    { value: "德国", label: "Germany" },
    { value: "法国", label: "France" },
    { value: "中国", label: "China" },
  ],
  provider: "test",
  providerAvailable: true,
  defaultChatModel: "auto",
  availableChatModels: [
    {
      id: "auto",
      provider: "auto",
      label: "Auto (Recommended)",
      available: true,
    },
    {
      id: "nvidia:meta/llama-3.3-70b-instruct",
      provider: "nvidia",
      model: "meta/llama-3.3-70b-instruct",
      label: "NVIDIA · meta/llama-3.3-70b-instruct",
      available: true,
    },
  ],
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

  it("uses the metadata default chat model when the cached one is invalid", () => {
    expect(
      resolveChatModelSelection({
        metadata,
        selectedChatModel: "gemini:gemini-2.5-flash",
      }),
    ).toBe("auto");
  });

  it("keeps a valid selected chat model", () => {
    expect(
      resolveChatModelSelection({
        metadata,
        selectedChatModel: "nvidia:meta/llama-3.3-70b-instruct",
      }),
    ).toBe("nvidia:meta/llama-3.3-70b-instruct");
  });

  it("builds session keys with country and chat model", () => {
    expect(
      buildCountryChatSessionKey("瑞典", "nvidia:meta/llama-3.3-70b-instruct"),
    ).toBe("瑞典::nvidia:meta/llama-3.3-70b-instruct");
  });
});
