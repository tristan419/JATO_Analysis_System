import { ChangeEvent, ReactNode, useEffect, useState } from "react";

export interface TextSearchFilterValue {
  country: string;
  brand: string;
  model: string;
}

const EMPTY_TEXT_SEARCH_FILTERS: TextSearchFilterValue = {
  country: "",
  brand: "",
  model: "",
};

function isSameFilterValue(
  left: TextSearchFilterValue,
  right: TextSearchFilterValue,
) {
  return left.country === right.country
    && left.brand === right.brand
    && left.model === right.model;
}

export function useTextSearchFilters(delayMs = 320) {
  const [draft, setDraft] = useState<TextSearchFilterValue>(
    EMPTY_TEXT_SEARCH_FILTERS,
  );
  const [applied, setApplied] = useState<TextSearchFilterValue>(
    EMPTY_TEXT_SEARCH_FILTERS,
  );
  const [isPending, setIsPending] = useState(false);

  useEffect(() => {
    if (isSameFilterValue(draft, applied)) {
      setIsPending(false);
      return;
    }

    setIsPending(true);
    const timer = window.setTimeout(() => {
      setApplied(draft);
      setIsPending(false);
    }, delayMs);

    return () => window.clearTimeout(timer);
  }, [applied, delayMs, draft]);

  function setField(
    key: keyof TextSearchFilterValue,
    value: string,
  ) {
    setDraft((current) => (
      current[key] === value ? current : { ...current, [key]: value }
    ));
  }

  function commitDraft() {
    setApplied(draft);
    setIsPending(false);
    return draft;
  }

  function reset() {
    setDraft(EMPTY_TEXT_SEARCH_FILTERS);
    setApplied(EMPTY_TEXT_SEARCH_FILTERS);
    setIsPending(false);
  }

  return {
    applied,
    commitDraft,
    draft,
    isPending,
    reset,
    setField,
  };
}

export function TextSearchFilters({
  leading,
  modelLabel = "Model",
  modelPlaceholder = "e.g. XC60 / Model 3",
  onChange,
  value,
}: {
  leading?: ReactNode;
  modelLabel?: string;
  modelPlaceholder?: string;
  onChange: (key: keyof TextSearchFilterValue, value: string) => void;
  value: TextSearchFilterValue;
}) {
  return (
    <>
      {leading}
      <div className="filter-group">
        <label>Country</label>
        <input
          type="search"
          value={value.country}
          onChange={(event: ChangeEvent<HTMLInputElement>) => onChange("country", event.target.value)}
          placeholder="e.g. Sweden / se / 瑞典"
        />
      </div>
      <div className="filter-group">
        <label>Brand</label>
        <input
          type="search"
          value={value.brand}
          onChange={(event: ChangeEvent<HTMLInputElement>) => onChange("brand", event.target.value)}
          placeholder="e.g. BMW / volvo"
        />
      </div>
      <div className="filter-group">
        <label>{modelLabel}</label>
        <input
          type="search"
          value={value.model}
          onChange={(event: ChangeEvent<HTMLInputElement>) => onChange("model", event.target.value)}
          placeholder={modelPlaceholder}
        />
      </div>
    </>
  );
}