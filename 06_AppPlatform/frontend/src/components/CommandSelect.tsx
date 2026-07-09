import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
  type RefObject,
} from "react";

import "./CommandSelect.css";

export interface CommandSelectOption<Value extends string = string> {
  value: Value;
  label: string;
  caption?: string;
  keywords?: string[];
  disabled?: boolean;
}

interface CommandSelectProps<Value extends string = string> {
  label?: string;
  name?: string;
  value?: Value | "";
  defaultValue?: Value | "";
  options: Array<CommandSelectOption<Value>>;
  onChange?: (value: Value | "") => void;
  onValueChange?: (value: Value | "") => void;
  placeholder?: string;
  searchPlaceholder?: string;
  emptyLabel?: string;
  allowClear?: boolean;
  disabled?: boolean;
  compact?: boolean;
  className?: string;
}

interface CommandMultiSelectProps<Value extends string = string> {
  label?: string;
  selected: Value[];
  options: Array<CommandSelectOption<Value>>;
  onChange: (value: Value[]) => void;
  placeholder?: string;
  searchPlaceholder?: string;
  emptyLabel?: string;
  maxSelected?: number;
  disabled?: boolean;
  compact?: boolean;
  className?: string;
}

function optionMatches(option: CommandSelectOption, query: string): boolean {
  const normalized = query.trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  return `${option.label} ${option.caption ?? ""} ${option.value} ${(option.keywords ?? []).join(" ")}`
    .toLowerCase()
    .includes(normalized);
}

function useOutsideClose(open: boolean, onClose: () => void) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const handlePointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [onClose, open]);

  return rootRef;
}

function useCommandQuery(open: boolean) {
  const [query, setQuery] = useState("");
  const searchRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!open) {
      setQuery("");
      return;
    }
    window.setTimeout(() => searchRef.current?.focus(), 0);
  }, [open]);

  return { query, setQuery, searchRef };
}

function PopoverShell({
  children,
  meta,
  query,
  searchPlaceholder,
  onQueryChange,
  searchRef,
}: {
  children: ReactNode;
  meta: ReactNode;
  query: string;
  searchPlaceholder: string;
  onQueryChange: (value: string) => void;
  searchRef: RefObject<HTMLInputElement | null>;
}) {
  return (
    <div className="command-select-popover">
      <input
        ref={searchRef}
        className="command-select-search"
        value={query}
        onChange={(event) => onQueryChange(event.target.value)}
        placeholder={searchPlaceholder}
      />
      <div className="command-select-meta">{meta}</div>
      <div className="command-select-list">{children}</div>
    </div>
  );
}

export function CommandSelect<Value extends string = string>({
  label,
  value,
  defaultValue = "",
  name,
  options,
  onChange,
  onValueChange,
  placeholder = "Select...",
  searchPlaceholder = "Search...",
  emptyLabel = "No options",
  allowClear = false,
  disabled = false,
  compact = false,
  className = "",
}: CommandSelectProps<Value>) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const [internalValue, setInternalValue] = useState<Value | "">(defaultValue);
  const rootRef = useOutsideClose(open, () => setOpen(false));
  const { query, setQuery, searchRef } = useCommandQuery(open);
  const selectedValue = value ?? internalValue;
  const matched = useMemo(
    () => options.filter((option) => optionMatches(option, query)),
    [options, query],
  );
  const selectedOption = options.find((option) => option.value === selectedValue);

  useEffect(() => {
    setActiveIndex(0);
  }, [query, open]);

  function choose(nextValue: Value | ""): void {
    if (value === undefined) {
      setInternalValue(nextValue);
    }
    onChange?.(nextValue);
    onValueChange?.(nextValue);
    setOpen(false);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLDivElement>): void {
    if (!open && (event.key === "ArrowDown" || event.key === "Enter" || event.key === " ")) {
      event.preventDefault();
      setOpen(true);
      return;
    }
    if (!open) {
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      setOpen(false);
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => Math.min(current + 1, Math.max(0, matched.length - 1)));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => Math.max(0, current - 1));
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const option = matched[activeIndex];
      if (option && !option.disabled) {
        choose(option.value);
      }
    }
  }

  return (
    <div
      ref={rootRef}
      className={`command-select${compact ? " is-compact" : ""}${className ? ` ${className}` : ""}`}
      onKeyDown={handleKeyDown}
    >
      {name ? <input type="hidden" name={name} value={selectedValue} /> : null}
      <button
        type="button"
        className="command-select-trigger"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        disabled={disabled}
        onClick={() => setOpen((current) => !current)}
      >
        <span
          className={`command-select-trigger-label${selectedOption ? "" : " command-select-trigger-placeholder"}`}
        >
          {selectedOption?.label ?? placeholder}
        </span>
        <span className="command-select-trigger-caret">▾</span>
      </button>
      {open ? (
        <PopoverShell
          query={query}
          searchPlaceholder={searchPlaceholder}
          onQueryChange={setQuery}
          searchRef={searchRef}
          meta={
            <>
              <span>{matched.length} items</span>
              {allowClear ? (
                <button type="button" className="command-select-action" onClick={() => choose("")}>
                  Clear
                </button>
              ) : null}
            </>
          }
        >
          {matched.length === 0 ? (
            <div className="command-select-empty">{emptyLabel}</div>
          ) : matched.map((option, index) => (
            <button
              key={option.value}
              type="button"
              className={[
                "command-select-option",
                option.value === selectedValue ? "is-selected" : "",
                index === activeIndex ? "is-active" : "",
              ].filter(Boolean).join(" ")}
              disabled={option.disabled}
              role="option"
              aria-selected={option.value === selectedValue}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => choose(option.value)}
            >
              <span className="command-select-option-check">{option.value === selectedValue ? "✓" : ""}</span>
              <span className="command-select-option-text">
                <span className="command-select-option-label">{option.label}</span>
                {option.caption ? (
                  <span className="command-select-option-caption">{option.caption}</span>
                ) : null}
              </span>
            </button>
          ))}
        </PopoverShell>
      ) : null}
    </div>
  );
}

export function CommandMultiSelect<Value extends string = string>({
  label,
  selected,
  options,
  onChange,
  placeholder = "Select...",
  searchPlaceholder = "Search...",
  emptyLabel = "No options",
  maxSelected,
  disabled = false,
  compact = false,
  className = "",
}: CommandMultiSelectProps<Value>) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const rootRef = useOutsideClose(open, () => setOpen(false));
  const { query, setQuery, searchRef } = useCommandQuery(open);
  const selectedSet = useMemo(() => new Set(selected), [selected]);
  const matched = useMemo(
    () => options.filter((option) => optionMatches(option, query)),
    [options, query],
  );
  const selectedLabels = options
    .filter((option) => selectedSet.has(option.value))
    .map((option) => option.label);
  const displayText = selectedLabels.length > 0
    ? selectedLabels.slice(0, 3).join(", ") + (selectedLabels.length > 3 ? ` +${selectedLabels.length - 3}` : "")
    : placeholder;

  useEffect(() => {
    setActiveIndex(0);
  }, [query, open]);

  function optionLimited(option: CommandSelectOption<Value>): boolean {
    return maxSelected !== undefined && selected.length >= maxSelected && !selectedSet.has(option.value);
  }

  function toggle(value: Value): void {
    if (selectedSet.has(value)) {
      onChange(selected.filter((item) => item !== value));
      return;
    }
    if (maxSelected !== undefined && selected.length >= maxSelected) {
      return;
    }
    onChange([...selected, value]);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLDivElement>): void {
    if (!open && (event.key === "ArrowDown" || event.key === "Enter" || event.key === " ")) {
      event.preventDefault();
      setOpen(true);
      return;
    }
    if (!open) {
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      setOpen(false);
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => Math.min(current + 1, Math.max(0, matched.length - 1)));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => Math.max(0, current - 1));
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const option = matched[activeIndex];
      if (option && !option.disabled && !optionLimited(option)) {
        toggle(option.value);
      }
    }
  }

  function selectMatched(): void {
    const next = new Set(selected);
    matched.forEach((option) => {
      if (!option.disabled && (maxSelected === undefined || next.size < maxSelected)) {
        next.add(option.value);
      }
    });
    onChange(Array.from(next));
  }

  return (
    <div
      ref={rootRef}
      className={`command-select command-multi-select${compact ? " is-compact" : ""}${className ? ` ${className}` : ""}`}
      onKeyDown={handleKeyDown}
    >
      <button
        type="button"
        className="command-select-trigger"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        disabled={disabled}
        onClick={() => setOpen((current) => !current)}
      >
        <span
          className={`command-select-trigger-label${selectedLabels.length > 0 ? "" : " command-select-trigger-placeholder"}`}
        >
          {displayText}
        </span>
        <span className="command-select-trigger-caret">▾</span>
      </button>
      {open ? (
        <PopoverShell
          query={query}
          searchPlaceholder={searchPlaceholder}
          onQueryChange={setQuery}
          searchRef={searchRef}
          meta={
            <>
              <span>
                {matched.length} items · {selected.length}{maxSelected ? `/${maxSelected}` : ""} selected
              </span>
              <span className="command-select-meta-actions">
                <button type="button" className="command-select-action" onClick={selectMatched}>
                  All
                </button>
                <button type="button" className="command-select-action" onClick={() => onChange([])}>
                  Clear
                </button>
              </span>
            </>
          }
        >
          {matched.length === 0 ? (
            <div className="command-select-empty">{emptyLabel}</div>
          ) : matched.map((option, index) => {
            const checked = selectedSet.has(option.value);
            const limited = optionLimited(option);
            return (
              <button
                key={option.value}
                type="button"
                className={[
                  "command-select-option",
                  checked ? "is-selected" : "",
                  index === activeIndex ? "is-active" : "",
                ].filter(Boolean).join(" ")}
                disabled={option.disabled || limited}
                role="option"
                aria-selected={checked}
                onMouseEnter={() => setActiveIndex(index)}
                onClick={() => toggle(option.value)}
              >
                <span className="command-select-option-check">{checked ? "✓" : ""}</span>
                <span className="command-select-option-text">
                  <span className="command-select-option-label">{option.label}</span>
                  {option.caption ? (
                    <span className="command-select-option-caption">{option.caption}</span>
                  ) : null}
                </span>
              </button>
            );
          })}
        </PopoverShell>
      ) : null}
    </div>
  );
}
