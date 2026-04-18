import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  startTransition,
  type ReactNode,
} from "react";

import { api } from "../api/client";
import type {
  CountryChatChartLink,
  CountryChatGrounding,
  CountryChatMetadataResponse,
  CountryChatModelOption,
  CountryChatNewsOpsStatus,
  CountryChatRenderHint,
  CountryChatResponse,
  CountryChatSnapshot,
  CountryChatTurn,
} from "../types";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";
import {
  availableChatModels,
  buildCountryChatSessionKey,
  getChatModelLabel,
  isKnownCountryValue,
  resolveChatModelSelection,
  resolveCountrySelection,
} from "./countryChatHelpers";
import { useSharedFilterScope } from "./SharedFilterScopeContext";

const CHAT_SESSIONS_CACHE_KEY = "country-chat-sessions-v2";
const CHAT_UI_CACHE_KEY = "country-chat-ui-v2";
const CHAT_CACHE_TTL_MS = 12 * 60 * 60 * 1000;

export interface CountryChatTranscriptMessage extends CountryChatTurn {
  id: string;
  country?: string;
  question?: string;
  provider?: string;
  model?: string | null;
  providerReason?: string | null;
  answerMode?: string | null;
  grounding?: CountryChatGrounding | null;
  chartLinks?: CountryChatChartLink[];
  contextSnapshot?: CountryChatSnapshot;
  intents?: string[];
  focusedIntents?: string[];
  intentRoute?: string;
  renderHints?: CountryChatRenderHint[];
  extractedParams?: Record<string, unknown> | null;
}

interface CountryChatSession {
  latestResponse: CountryChatResponse | null;
  messages: CountryChatTranscriptMessage[];
}

type CountryChatSessions = Record<string, CountryChatSession>;

interface CountryChatUiCache {
  drafts: Record<string, string>;
  selectedCountry: string;
  selectedChatModel: string;
  widgetExpanded: boolean;
  widgetWidth?: number;
  widgetHeight?: number;
}

interface CountryChatContextValue {
  draft: string;
  error: string;
  latestResponse: CountryChatResponse | null;
  loadingMetadata: boolean;
  loadingNewsStatus: boolean;
  messages: CountryChatTranscriptMessage[];
  metadata: CountryChatMetadataResponse | null;
  newsStatus: CountryChatNewsOpsStatus | null;
  promptSuggestions: string[];
  providerSummary: string;
  refreshingNews: boolean;
  refreshCountryNews: () => Promise<void>;
  retryLatestQuestionWithFreshNews: () => Promise<void>;
  availableChatModels: CountryChatModelOption[];
  selectedCountry: string;
  selectedChatModel: string;
  sending: boolean;
  setDraft: (value: string) => void;
  setSelectedChatModel: (value: string) => void;
  setSelectedCountry: (country: string) => void;
  setWidgetExpanded: (next: boolean | ((current: boolean) => boolean)) => void;
  sendQuestion: (
    questionOverride?: string,
    options?: { refreshNews?: boolean },
  ) => Promise<void>;
  widgetExpanded: boolean;
  widgetWidth: number;
  widgetHeight: number;
  setWidgetSize: (w: number, h: number) => void;
}

const EMPTY_SESSION: CountryChatSession = {
  latestResponse: null,
  messages: [],
};

const CountryChatContext = createContext<CountryChatContextValue | null>(null);

function injectNewsPayloadIntoSnapshot(
  snapshot: CountryChatSnapshot | undefined,
  payload: {
    marketEvents?: CountryChatSnapshot["marketEvents"];
    newsDigest?: CountryChatSnapshot["newsDigest"];
  },
): CountryChatSnapshot | undefined {
  if (!snapshot) {
    return snapshot;
  }
  return {
    ...snapshot,
    marketEvents: payload.marketEvents ?? snapshot.marketEvents,
    newsDigest: payload.newsDigest ?? snapshot.newsDigest,
  };
}

function mergeNewsPayloadIntoSessions(
  sessions: CountryChatSessions,
  country: string,
  payload: {
    marketEvents?: CountryChatSnapshot["marketEvents"];
    newsDigest?: CountryChatSnapshot["newsDigest"];
  },
): CountryChatSessions {
  const nextSessions: CountryChatSessions = { ...sessions };
  const sessionPrefix = `${country}::`;

  for (const [sessionKey, session] of Object.entries(sessions)) {
    if (!sessionKey.startsWith(sessionPrefix)) {
      continue;
    }
    const latestResponse = session.latestResponse
      ? {
          ...session.latestResponse,
          contextSnapshot: injectNewsPayloadIntoSnapshot(
            session.latestResponse.contextSnapshot,
            payload,
          ) as CountryChatSnapshot,
        }
      : null;
    const messages = [...session.messages];
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      if (messages[index]?.role !== "assistant") {
        continue;
      }
      messages[index] = {
        ...messages[index],
        contextSnapshot: injectNewsPayloadIntoSnapshot(
          messages[index].contextSnapshot,
          payload,
        ),
      };
      break;
    }
    nextSessions[sessionKey] = {
      latestResponse,
      messages,
    };
  }

  return nextSessions;
}

export function CountryChatProvider({ children }: { children: ReactNode }) {
  const { selections } = useSharedFilterScope();
  const preferredCountry = Array.isArray(selections.country)
    ? selections.country[0] ?? ""
    : "";
  const cachedSessions = useMemo(
    () => getCachedPageValue<CountryChatSessions>(CHAT_SESSIONS_CACHE_KEY) ?? {},
    [],
  );
  const cachedUi = useMemo(
    () => getCachedPageValue<CountryChatUiCache>(CHAT_UI_CACHE_KEY),
    [],
  );

  const [metadata, setMetadata] =
    useState<CountryChatMetadataResponse | null>(null);
  const [loadingMetadata, setLoadingMetadata] = useState(true);
  const [loadingNewsStatus, setLoadingNewsStatus] = useState(false);
  const [refreshingNews, setRefreshingNews] = useState(false);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState("");
  const [newsStatus, setNewsStatus] =
    useState<CountryChatNewsOpsStatus | null>(null);
  const [selectedCountry, setSelectedCountryState] = useState(
    () => cachedUi?.selectedCountry ?? "",
  );
  const [selectedChatModel, setSelectedChatModelState] = useState(
    () => cachedUi?.selectedChatModel ?? "",
  );
  const [widgetExpanded, setWidgetExpanded] = useState(
    () => cachedUi?.widgetExpanded ?? false,
  );
  const [widgetWidth, setWidgetWidth] = useState(
    () => cachedUi?.widgetWidth ?? 400,
  );
  const [widgetHeight, setWidgetHeight] = useState(
    () => cachedUi?.widgetHeight ?? 540,
  );
  const setWidgetSize = useCallback((w: number, h: number) => {
    setWidgetWidth(w);
    setWidgetHeight(h);
  }, []);
  const userPickedRef = useRef(false);
  const [drafts, setDrafts] = useState<Record<string, string>>(
    () => cachedUi?.drafts ?? {},
  );
  const [sessions, setSessions] = useState<CountryChatSessions>(cachedSessions);

  useEffect(() => {
    let cancelled = false;
    setLoadingMetadata(true);
    api.countryChatMetadata()
      .then((response) => {
        if (cancelled) {
          return;
        }
        setMetadata(response);
      })
      .catch((reason: unknown) => {
        if (cancelled) {
          return;
        }
        setError(
          reason instanceof Error
            ? reason.message
            : String(reason),
        );
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingMetadata(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!metadata) {
      return;
    }
    const fallbackCountry = resolveCountrySelection({
      metadata,
      preferredCountry,
      selectedCountry,
      userPicked: userPickedRef.current,
    });

    if (fallbackCountry && fallbackCountry !== selectedCountry) {
      setSelectedCountryState(fallbackCountry);
    }
  }, [metadata, preferredCountry, selectedCountry]);

  useEffect(() => {
    if (!metadata) {
      return;
    }
    const resolvedChatModel = resolveChatModelSelection({
      metadata,
      selectedChatModel,
    });
    if (resolvedChatModel && resolvedChatModel !== selectedChatModel) {
      setSelectedChatModelState(resolvedChatModel);
    }
  }, [metadata, selectedChatModel]);

  useEffect(() => {
    if (userPickedRef.current) {
      return;
    }
    if (!metadata || !preferredCountry || sending) {
      return;
    }
    if (!isKnownCountryValue(metadata, preferredCountry)) {
      return;
    }
    if (preferredCountry !== selectedCountry) {
      setSelectedCountryState(preferredCountry);
    }
  }, [metadata, preferredCountry, selectedCountry, sending]);

  useEffect(() => {
    setCachedPageValue(CHAT_SESSIONS_CACHE_KEY, sessions, CHAT_CACHE_TTL_MS);
  }, [sessions]);

  useEffect(() => {
    setCachedPageValue(
      CHAT_UI_CACHE_KEY,
        {
          drafts,
          selectedCountry,
          selectedChatModel,
          widgetExpanded,
          widgetWidth,
          widgetHeight,
        },
        CHAT_CACHE_TTL_MS,
      );
  }, [
    drafts,
    selectedCountry,
    selectedChatModel,
    widgetExpanded,
    widgetWidth,
    widgetHeight,
  ]);

  const activeSessionKey = useMemo(
    () => buildCountryChatSessionKey(selectedCountry, selectedChatModel),
    [selectedCountry, selectedChatModel],
  );
  const activeSession = useMemo(
    () => (selectedCountry ? sessions[activeSessionKey] ?? EMPTY_SESSION : EMPTY_SESSION),
    [activeSessionKey, selectedCountry, sessions],
  );
  const draft = selectedCountry ? drafts[activeSessionKey] ?? "" : "";
  const chatModelOptions = useMemo(
    () => availableChatModels(metadata),
    [metadata],
  );
  const promptSuggestions = useMemo(
    () => activeSession.latestResponse?.suggestedPrompts ?? metadata?.suggestedPrompts ?? [],
    [activeSession.latestResponse, metadata],
  );
  const providerSummary = activeSession.latestResponse?.provider
    ? [
        activeSession.latestResponse.provider.toUpperCase(),
        activeSession.latestResponse.model,
      ].filter(Boolean).join(" · ")
    : (metadata?.providerAvailable
      ? getChatModelLabel(metadata, selectedChatModel)
      : (metadata?.providerReason ?? "当前使用本地降级回答"));

  const setSelectedCountry = useCallback((country: string) => {
    userPickedRef.current = true;
    setSelectedCountryState(country);
    setError("");
  }, []);

  const setSelectedChatModel = useCallback((value: string) => {
    setSelectedChatModelState(value);
    setError("");
  }, []);

  const setDraft = useCallback((value: string) => {
    if (!selectedCountry) {
      return;
    }
    setDrafts((current) => ({
      ...current,
      [activeSessionKey]: value,
    }));
  }, [activeSessionKey, selectedCountry]);

  const loadNewsStatus = useCallback(
    async (country: string, mode: "initial" | "silent" = "initial") => {
      if (!country) {
        setNewsStatus(null);
        return;
      }
      if (mode === "initial") {
        setLoadingNewsStatus(true);
      }
      try {
        const response = await api.countryChatNewsStatus(country);
        setNewsStatus(response);
      } catch (reason: unknown) {
        if (mode === "initial") {
          setError(
            reason instanceof Error
              ? reason.message
              : String(reason),
          );
        }
      } finally {
        if (mode === "initial") {
          setLoadingNewsStatus(false);
        }
      }
    },
    [],
  );

  useEffect(() => {
    if (!selectedCountry) {
      setNewsStatus(null);
      return;
    }
    void loadNewsStatus(selectedCountry);
  }, [loadNewsStatus, selectedCountry]);

  const sendQuestion = useCallback(async (
    questionOverride?: string,
    options?: { refreshNews?: boolean },
  ) => {
    const country = selectedCountry;
    const sessionKey = buildCountryChatSessionKey(country, selectedChatModel);
    const question = String(questionOverride ?? drafts[sessionKey] ?? "").trim();
    if (!country || !question || sending) {
      return;
    }

    const currentSession = sessions[sessionKey] ?? EMPTY_SESSION;
    const userMessage: CountryChatTranscriptMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: question,
    };
    const history = currentSession.messages.map<CountryChatTurn>((message) => ({
      role: message.role,
      content: message.content,
      extracted_params: message.extractedParams ?? undefined,
      intent_route: message.intentRoute ?? undefined,
    }));

    setSending(true);
    setError("");
    setDrafts((current) => ({
      ...current,
      [sessionKey]: "",
    }));
    setSessions((current) => {
      const previous = current[sessionKey] ?? EMPTY_SESSION;
      return {
        ...current,
        [sessionKey]: {
          ...previous,
          messages: [...previous.messages, userMessage],
        },
      };
    });

    try {
      const response = await api.countryChat({
        country,
        question,
        history,
        refresh_news: Boolean(options?.refreshNews),
        model: selectedChatModel,
      });
      startTransition(() => {
        setSessions((current) => {
          const previous = current[sessionKey] ?? EMPTY_SESSION;
          return {
            ...current,
            [sessionKey]: {
              latestResponse: response,
              messages: [
                ...previous.messages,
                {
                  id: `assistant-${Date.now()}`,
                  role: "assistant",
                  country: response.country,
                  question: response.question,
                  content: response.answer,
                  provider: response.provider,
                  model: response.model,
                  providerReason: response.providerReason,
                  answerMode: response.answerMode,
                  grounding: response.grounding,
                  chartLinks: response.chartLinks,
                  contextSnapshot: response.contextSnapshot,
                  intents: response.intents,
                  focusedIntents: response.focusedIntents,
                  intentRoute: response.intentRoute,
                  renderHints: response.renderHints,
                  extractedParams: response.extractedParams,
                },
              ],
            },
          };
        });
      });
      void loadNewsStatus(country, "silent");
    } catch (reason: unknown) {
      setError(
        reason instanceof Error
          ? reason.message
          : String(reason),
      );
    } finally {
      setSending(false);
    }
  }, [drafts, loadNewsStatus, selectedChatModel, selectedCountry, sending, sessions]);

  const refreshCountryNews = useCallback(async () => {
    const country = selectedCountry;
    if (!country || refreshingNews) {
      return;
    }
    setRefreshingNews(true);
    setError("");
    try {
      const response = await api.countryChatNewsRefresh({
        country,
        persist: true,
      });
      setNewsStatus(response.status);
      setSessions((current) => mergeNewsPayloadIntoSessions(
        current,
        country,
        response.payload,
      ));
    } catch (reason: unknown) {
      setError(
        reason instanceof Error
          ? reason.message
          : String(reason),
      );
    } finally {
      setRefreshingNews(false);
    }
  }, [refreshingNews, selectedCountry]);

  const retryLatestQuestionWithFreshNews = useCallback(async () => {
    const latestQuestion = String(
      activeSession.latestResponse?.question
      ?? [...activeSession.messages]
        .reverse()
        .find((message) => message.role === "user")
        ?.content
      ?? "",
    ).trim();
    if (!latestQuestion) {
      return;
    }
    await sendQuestion(latestQuestion, { refreshNews: true });
  }, [activeSession.latestResponse, activeSession.messages, sendQuestion]);

  const value = useMemo<CountryChatContextValue>(() => ({
    availableChatModels: chatModelOptions,
    draft,
    error,
    latestResponse: activeSession.latestResponse,
    loadingMetadata,
    loadingNewsStatus,
    messages: activeSession.messages,
    metadata,
    newsStatus,
    promptSuggestions,
    providerSummary,
    refreshingNews,
    refreshCountryNews,
    retryLatestQuestionWithFreshNews,
    selectedCountry,
    selectedChatModel,
    sending,
    setDraft,
    setSelectedChatModel,
    setSelectedCountry,
    setWidgetExpanded,
    sendQuestion,
    widgetExpanded,
    widgetWidth,
    widgetHeight,
    setWidgetSize,
  }), [
    activeSession.latestResponse,
    activeSession.messages,
    chatModelOptions,
    draft,
    error,
    loadingMetadata,
    loadingNewsStatus,
    metadata,
    newsStatus,
    promptSuggestions,
    providerSummary,
    refreshingNews,
    refreshCountryNews,
    retryLatestQuestionWithFreshNews,
    selectedCountry,
    selectedChatModel,
    sending,
    setDraft,
    setSelectedChatModel,
    setSelectedCountry,
    sendQuestion,
    widgetExpanded,
    widgetWidth,
    widgetHeight,
    setWidgetSize,
  ]);

  return (
    <CountryChatContext.Provider value={value}>
      {children}
    </CountryChatContext.Provider>
  );
}

export function useCountryChatOptional() {
  return useContext(CountryChatContext);
}

export function useCountryChat() {
  const context = useCountryChatOptional();
  if (!context) {
    throw new Error("useCountryChat must be used within CountryChatProvider");
  }
  return context;
}
