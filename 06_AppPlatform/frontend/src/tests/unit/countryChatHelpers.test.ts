import { describe, expect, it } from "vitest";

import {
  buildCountryChatAnswerSections,
  buildCountryChatAnswerPath,
  buildCountryChatHandoffSearch,
  buildCountryChatLoadingPlan,
  buildCountryChatSessionKey,
  getCountryChatHandoffSearch,
  isCountryChatMobileAccess,
  parseCountryChatHandoffSearch,
  resolveCountryChatDeckScope,
  resolveChatModelSelection,
  resolveCountrySelection,
} from "../../contexts/countryChatHelpers";
import type { CountryChatMetadataResponse } from "../../types/countryChat";

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

  it("builds handoff search params with country, model, and question", () => {
    expect(buildCountryChatHandoffSearch({
      country: "瑞典",
      chatModel: "auto",
      question: "RAV4 HEV 现在有哪些 trim？",
    })).toBe(
      "?cc_country=%E7%91%9E%E5%85%B8&cc_model=auto&cc_q=RAV4+HEV+%E7%8E%B0%E5%9C%A8%E6%9C%89%E5%93%AA%E4%BA%9B+trim%EF%BC%9F",
    );
  });

  it("parses handoff search params back into payload fields", () => {
    expect(parseCountryChatHandoffSearch(
      "?cc_country=%E7%91%9E%E5%85%B8&cc_model=auto&cc_q=RAV4+HEV+%E7%8E%B0%E5%9C%A8%E6%9C%89%E5%93%AA%E4%BA%9B+trim%EF%BC%9F",
    )).toEqual({
      country: "瑞典",
      chatModel: "auto",
      question: "RAV4 HEV 现在有哪些 trim？",
    });
  });

  it("only keeps handoff search active on the copilot route", () => {
    expect(getCountryChatHandoffSearch("/copilot", "?cc_country=瑞典")).toBe(
      "?cc_country=瑞典",
    );
    expect(getCountryChatHandoffSearch("/review", "?cc_country=瑞典")).toBe("");
    expect(getCountryChatHandoffSearch("/copilot", "")).toBe("");
  });

  it("builds a visible answer path for precise lookup answers", () => {
    expect(buildCountryChatAnswerPath({
      country: "瑞典",
      intentRoute: "precise-lookup",
      answerMode: "grounded-direct",
      layers: [
        { kind: "snapshot", label: "Snapshot", detail: "Market base map" },
        { kind: "Dynamic", label: "Dynamic", detail: "Current price rows" },
      ],
      extractedParams: {
        model: "RAV4",
        powertrain: "HEV",
      },
    })).toEqual({
      routeLabel: "精准查询",
      outputLabel: "直接组装",
      focusTags: ["瑞典", "RAV4", "HEV"],
      steps: [
        "锁定具体车型 / trim / 价格条件",
        "读取 Snapshot / Dynamic",
        "按直接组装方式生成答案",
      ],
    });
  });

  it("splits answer copy into lead and reasoning notes", () => {
    expect(buildCountryChatAnswerSections({
      content: "RAV4 HEV 目前主要是中高配版本。\n\n价格带主要落在 42-46 万 SEK。",
      summary: "这次回答优先读取当前价格样本与国家快照。",
    })).toEqual({
      lead: "RAV4 HEV 目前主要是中高配版本。",
      detailParagraphs: ["价格带主要落在 42-46 万 SEK。"],
      reasoningNotes: [
        "这次回答优先读取当前价格样本与国家快照。",
        "价格带主要落在 42-46 万 SEK。",
      ],
    });
  });

  it("falls back to sentence splitting for one-line answers", () => {
    expect(buildCountryChatAnswerSections({
      content: "瑞典 EX30 仍然是 BEV 核心车型。价格带也比同级燃油替代更清晰。",
    })).toEqual({
      lead: "瑞典 EX30 仍然是 BEV 核心车型。",
      detailParagraphs: ["价格带也比同级燃油替代更清晰。"],
      reasoningNotes: ["价格带也比同级燃油替代更清晰。"],
    });
  });

  it("falls back to generic evidence wording when no grounding layers exist", () => {
    expect(buildCountryChatAnswerPath({
      intentRoute: "market-context",
      answerMode: "grounded-model",
      extractedParams: {
        segment: "C-SUV",
      },
    }).steps[1]).toBe("读取 国家快照与已命中证据");
  });

  it("treats phone-sized coarse-pointer access as mobile-only mode", () => {
    expect(isCountryChatMobileAccess(390, true)).toBe(true);
  });

  it("keeps desktop narrow windows out of mobile-only mode", () => {
    expect(isCountryChatMobileAccess(640, false)).toBe(false);
  });

  it("builds positioning-oriented loading steps for length questions", () => {
    expect(buildCountryChatLoadingPlan("车长4820的车属于什么segment")).toEqual({
      label: "正在做定位分析",
      steps: [
        "解析车长/价格条件",
        "匹配同尺寸车型与 segment",
        "汇总该 segment 的燃料、渠道和驱动结构",
        "整理价格与竞品结论",
      ],
    });
  });

  it("builds market-context loading steps for policy questions", () => {
    expect(buildCountryChatLoadingPlan("德国最近补贴政策有什么变化").label).toBe(
      "正在做市场情报分析",
    );
  });

  it("builds model-performance loading steps for why-this-model questions", () => {
    expect(buildCountryChatLoadingPlan("EX40为什么卖得好")).toEqual({
      label: "正在做车型胜因分析",
      steps: [
        "锁定车型与细分页 scope",
        "读取 Market Scan 排名与份额",
        "补齐渠道 / 驱动 / 版本结构",
        "关联最新市场信号并生成结论",
      ],
    });
  });

  it("narrows deck scope for market-context answers", () => {
    expect(resolveCountryChatDeckScope("market-context")).toEqual({
      defaultLens: "intelligence",
      visibleLenses: ["intelligence", "trend"],
    });
  });

  it("narrows deck scope for segment fuel answers", () => {
    expect(resolveCountryChatDeckScope("segment-fuel-focus")).toEqual({
      defaultLens: "market",
      visibleLenses: ["market", "intelligence"],
    });
  });

  it("keeps market-scan scoped answers on the market lens first", () => {
    expect(resolveCountryChatDeckScope("market-scan-scope")).toEqual({
      defaultLens: "market",
      visibleLenses: ["market", "workbench", "trend", "intelligence"],
    });
  });

  it("keeps the full deck scope for overview answers", () => {
    expect(resolveCountryChatDeckScope("market-overview").visibleLenses).toEqual([
      "all",
      "workbench",
      "market",
      "intelligence",
      "trend",
    ]);
  });
});
