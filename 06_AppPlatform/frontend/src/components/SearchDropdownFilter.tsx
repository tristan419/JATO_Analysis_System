import { Fragment, useEffect, useId, useState } from "react";

export type SearchDropdownOption = {
  value: string;
  label: string;
  badge?: string;
  badgeTone?: "library" | "source" | "pending" | "local" | "muted";
  disabled?: boolean;
  path?: string;
  meta?: string;
  group?: string;
  groupRank?: number;
  keepOpenOnSelect?: boolean;
  matchRankBoost?: number;
  preserveQueryOnSelect?: boolean;
  searchText?: string;
};

function optionMetaTags(meta: string | undefined): string[] {
  return (meta ?? "").split(/\s+·\s+/).map((part) => part.trim()).filter(Boolean);
}

function normaliseOptionText(value: string | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
}

function optionTextScore(value: string | undefined, query: string, exactScore: number, prefixScore: number, containsScore: number): number {
  const haystack = normaliseOptionText(value);
  if (!haystack) return 0;
  if (haystack === query) return exactScore;
  if (haystack.startsWith(query)) return prefixScore;
  return haystack.includes(query) ? containsScore : 0;
}

function optionMatchScore(option: SearchDropdownOption, query: string): number {
  const textScores = [
    optionTextScore(option.label, query, 1200, 980, 760),
    optionTextScore(option.value, query, 1100, 900, 700),
    optionTextScore(option.badge, query, 780, 640, 460),
    optionTextScore(option.path, query, 780, 640, 460),
    optionTextScore(option.meta, query, 760, 620, 440),
    optionTextScore(option.searchText, query, 700, 560, 400),
    optionTextScore(option.group, query, 320, 260, 180),
  ];
  const textScore = textScores.reduce((total, score) => total + score, 0);
  return textScore > 0 ? textScore + (option.matchRankBoost ?? 0) : 0;
}

export function SearchDropdownFilter({
  allowCustomValue = false,
  closeMenuOnCustomInput = false,
  initialVisibleCount = 80,
  keepOpenOnSelect = false,
  label,
  loading = false,
  selectedValues = [],
  value,
  visibleCountStep = 40,
  options,
  placeholder,
  emptyLabel,
  onChange,
  onQueryClear,
  onQueryChange,
}: {
  allowCustomValue?: boolean;
  closeMenuOnCustomInput?: boolean;
  initialVisibleCount?: number;
  keepOpenOnSelect?: boolean;
  label: string;
  loading?: boolean;
  selectedValues?: string[];
  value: string;
  visibleCountStep?: number;
  options: SearchDropdownOption[];
  placeholder: string;
  emptyLabel: string;
  onChange: (value: string) => void;
  onQueryClear?: () => void;
  onQueryChange?: (query: string) => void;
}) {
  const listboxId = useId();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const optionInitialLimit = Math.max(1, initialVisibleCount);
  const optionLimitStep = Math.max(1, visibleCountStep);
  const [visibleCount, setVisibleCount] = useState(optionInitialLimit);
  const [activeIndex, setActiveIndex] = useState(-1);
  const selectedValueSet = new Set(selectedValues);
  const selected = options.find((option) => option.value === value);
  const displayValue = open ? query : selected?.label ?? value;
  const normalizedQuery = normaliseOptionText(query);
  const matchedOptions = normalizedQuery
    ? options
      .map((option, index) => ({ option, index, score: optionMatchScore(option, normalizedQuery) }))
      .filter((item) => item.score > 0)
      .sort((a, b) => {
        if (a.score !== b.score) return b.score - a.score;
        const groupRankCompare = (a.option.groupRank ?? 1000) - (b.option.groupRank ?? 1000);
        if (groupRankCompare !== 0) return groupRankCompare;
        return a.index - b.index;
      })
      .map((item) => item.option)
    : options;
  const visibleOptions = matchedOptions.slice(0, visibleCount);
  const hiddenOptionCount = Math.max(matchedOptions.length - visibleOptions.length, 0);
  const showOptionGroups = visibleOptions.some((option) => Boolean(option.group?.trim()));
  const activeOption = activeIndex >= 0 ? visibleOptions[activeIndex] : undefined;
  const activeOptionId = activeOption ? `${listboxId}-option-${activeIndex}` : undefined;
  useEffect(() => {
    if (allowCustomValue && value === "") {
      setQuery("");
      setOpen(false);
    }
  }, [allowCustomValue, value]);
  useEffect(() => {
    setVisibleCount(optionInitialLimit);
  }, [normalizedQuery, open, optionInitialLimit, options.length]);
  useEffect(() => {
    if (!open) {
      setActiveIndex(-1);
      return;
    }
    setActiveIndex(visibleOptions.length > 0 ? 0 : -1);
  }, [normalizedQuery, open, options.length]);
  useEffect(() => {
    if (!open || visibleOptions.length === 0) {
      setActiveIndex(-1);
      return;
    }
    setActiveIndex((current) => (
      current >= 0 && current < visibleOptions.length ? current : 0
    ));
  }, [open, visibleOptions.length]);
  const selectOption = (option: SearchDropdownOption): void => {
    if (option.disabled) return;
    const shouldPreserveQuery = option.preserveQueryOnSelect ?? false;
    onChange(option.value);
    if (!shouldPreserveQuery) setQuery("");
    setOpen(option.keepOpenOnSelect ?? keepOpenOnSelect);
  };
  const clearQuery = (): void => {
    setQuery("");
    onQueryChange?.("");
    onQueryClear?.();
  };

  return (
    <div className="market-scan-field version-comparison-model-picker-field comparison-filter-dropdown-field">
      <span>{label}</span>
      <div className="version-comparison-model-picker comparison-filter-dropdown">
        <div className="version-comparison-model-picker-input-row">
          <input
            type="text"
            className="version-comparison-model-search comparison-filter-dropdown-search"
            placeholder={selected?.label ?? placeholder}
            value={displayValue}
            onChange={(event) => {
              const nextValue = event.target.value;
              setQuery(nextValue);
              setOpen(!(allowCustomValue && closeMenuOnCustomInput));
              onQueryChange?.(nextValue);
              if (allowCustomValue) onChange(nextValue);
            }}
            onFocus={() => {
              const nextQuery = allowCustomValue ? selected?.label ?? value : "";
              setQuery(nextQuery);
              onQueryChange?.(nextQuery);
              setOpen(true);
            }}
            onClick={() => {
              if (open) return;
              const nextQuery = allowCustomValue ? selected?.label ?? value : "";
              setQuery(nextQuery);
              onQueryChange?.(nextQuery);
              setOpen(true);
            }}
            onBlur={() => window.setTimeout(() => setOpen(false), 120)}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.preventDefault();
                clearQuery();
                setOpen(false);
                return;
              }
              if (event.key === "ArrowDown") {
                event.preventDefault();
                setOpen(true);
                if (visibleOptions.length > 0) {
                  setActiveIndex((current) => (current + 1 + visibleOptions.length) % visibleOptions.length);
                }
                return;
              }
              if (event.key === "ArrowUp") {
                event.preventDefault();
                setOpen(true);
                if (visibleOptions.length > 0) {
                  setActiveIndex((current) => {
                    const nextIndex = current < 0 ? visibleOptions.length - 1 : current - 1;
                    return (nextIndex + visibleOptions.length) % visibleOptions.length;
                  });
                }
                return;
              }
              if (event.key === "Enter" && open && matchedOptions.length > 0) {
                event.preventDefault();
                selectOption(activeOption ?? matchedOptions[0]);
                return;
              }
              if (event.key === "Enter" && allowCustomValue) {
                event.preventDefault();
                onChange(query);
                setOpen(false);
              }
            }}
            role="combobox"
            aria-expanded={open}
            aria-haspopup="listbox"
            aria-autocomplete="list"
            aria-activedescendant={activeOptionId}
            aria-label={label}
          />
          {value ? (
            <button
              type="button"
              className="comparison-filter-dropdown-clear"
              aria-label={`清空 ${label}`}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => {
                clearQuery();
                setOpen(false);
                onChange("");
              }}
            >
              清空
            </button>
          ) : null}
        </div>
        {open ? (
          <div id={listboxId} className="version-comparison-model-dropdown comparison-filter-dropdown-menu" role="listbox">
            {visibleOptions.map((option, index) => {
              const selectedOption = option.value === value || selectedValueSet.has(option.value);
              const activeOptionSelected = index === activeIndex;
              const optionGroup = option.group?.trim();
              const previousGroup = index > 0 ? visibleOptions[index - 1]?.group?.trim() : null;
              const metaTags = optionMetaTags(option.meta);
              const optionRenderKey = `${option.value}::${index}`;
              return (
                <Fragment key={optionRenderKey}>
                  {showOptionGroups && optionGroup && optionGroup !== previousGroup ? (
                    <div className="comparison-filter-dropdown-group" role="presentation">{optionGroup}</div>
                  ) : null}
                  <button
                    id={`${listboxId}-option-${index}`}
                    type="button"
                    role="option"
                    aria-selected={selectedOption || activeOptionSelected}
                    aria-disabled={option.disabled}
                    className={`version-comparison-model-option${selectedOption ? " is-selected" : ""}${activeOptionSelected ? " is-active" : ""}${option.disabled ? " is-disabled" : ""}`}
                    disabled={option.disabled}
                    onMouseDown={(event) => event.preventDefault()}
                    onMouseEnter={() => setActiveIndex(index)}
                    onClick={() => selectOption(option)}
                  >
                    <span className={`version-comparison-model-checkbox${selectedOption ? " is-checked" : ""}`}>
                      {selectedOption ? "✓" : ""}
                    </span>
                    <span className="version-comparison-model-option-body">
                      {option.path ? (
                        <span className="version-comparison-model-option-path">{option.path}</span>
                      ) : null}
                      <span className="version-comparison-model-option-main">
                        <span className="version-comparison-model-option-name">{option.label}</span>
                        {option.badge ? (
                          <span className={`version-comparison-model-option-badge is-${option.badgeTone ?? "muted"}`}>
                            {option.badge}
                          </span>
                        ) : null}
                      </span>
                      {metaTags.length > 0 ? (
                        <span className="version-comparison-model-option-meta" aria-label={option.meta}>
                          {metaTags.map((metaTag, metaIndex) => (
                            <Fragment key={`${option.value}-meta-${metaIndex}`}>
                              {metaIndex > 0 ? (
                                <span className="version-comparison-model-option-meta-separator" aria-hidden="true">
                                  {" · "}
                                </span>
                              ) : null}
                              <span className="version-comparison-model-option-meta-tag">{metaTag}</span>
                            </Fragment>
                          ))}
                        </span>
                      ) : null}
                    </span>
                  </button>
                </Fragment>
              );
            })}
            {matchedOptions.length === 0 && !loading ? <div className="version-comparison-model-empty">{emptyLabel}</div> : null}
            {loading ? <div className="version-comparison-model-empty">正在搜索...</div> : null}
            {hiddenOptionCount > 0 ? (
              <button
                type="button"
                className="version-comparison-model-more"
                onMouseDown={(event) => event.preventDefault()}
                onClick={() => setVisibleCount((current) => Math.min(current + optionLimitStep, matchedOptions.length))}
              >
                展开更多 {Math.min(optionLimitStep, hiddenOptionCount)} 项 · 还有 {hiddenOptionCount} 项
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
