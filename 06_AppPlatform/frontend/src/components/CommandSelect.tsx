import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";

export type CommandSelectOption = {
  value: string;
  label: string;
  keywords?: string[];
};

type CommandSelectProps = {
  name?: string;
  value?: string;
  defaultValue?: string;
  options: CommandSelectOption[];
  placeholder?: string;
  searchPlaceholder?: string;
  className?: string;
  disabled?: boolean;
  onValueChange?: (value: string) => void;
};

export function CommandSelect({
  name,
  value,
  defaultValue = "",
  options,
  placeholder = "Select...",
  searchPlaceholder = "Search...",
  className,
  disabled = false,
  onValueChange,
}: CommandSelectProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const searchRef = useRef<HTMLInputElement | null>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const [internalValue, setInternalValue] = useState(defaultValue);
  const selectedValue = value ?? internalValue;
  const selectedOption = options.find((option) => option.value === selectedValue);

  const filteredOptions = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery) return options;
    return options.filter((option) => {
      const haystack = [option.value, option.label, ...(option.keywords ?? [])]
        .join(" ")
        .toLowerCase();
      return haystack.includes(normalizedQuery);
    });
  }, [options, query]);

  useEffect(() => {
    if (!open) return;
    setActiveIndex(0);
    window.setTimeout(() => searchRef.current?.focus(), 0);
  }, [open]);

  useEffect(() => {
    if (!open) return undefined;
    const handlePointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [open]);

  const commitValue = (nextValue: string) => {
    if (value == null) {
      setInternalValue(nextValue);
    }
    onValueChange?.(nextValue);
    setQuery("");
    setOpen(false);
  };

  const handleSearchKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      setOpen(false);
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => Math.min(current + 1, Math.max(filteredOptions.length - 1, 0)));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => Math.max(current - 1, 0));
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const option = filteredOptions[activeIndex];
      if (option) commitValue(option.value);
    }
  };

  return (
    <div
      ref={rootRef}
      className={`command-select${open ? " is-open" : ""}${disabled ? " is-disabled" : ""}${className ? ` ${className}` : ""}`}
    >
      {name ? <input type="hidden" name={name} value={selectedValue} /> : null}
      <button
        type="button"
        className="command-select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
      >
        <span className={selectedOption ? "command-select-value" : "command-select-placeholder"}>
          {selectedOption?.label ?? placeholder}
        </span>
        <span className="command-select-caret" aria-hidden="true" />
      </button>
      {open ? (
        <div className="command-select-popover" role="listbox">
          <input
            ref={searchRef}
            type="text"
            className="command-select-search"
            value={query}
            placeholder={searchPlaceholder}
            onChange={(event) => {
              setQuery(event.target.value);
              setActiveIndex(0);
            }}
            onKeyDown={handleSearchKeyDown}
          />
          <div className="command-select-meta">
            {filteredOptions.length} item{filteredOptions.length === 1 ? "" : "s"}
          </div>
          <div className="command-select-options">
            {filteredOptions.length > 0 ? (
              filteredOptions.map((option, index) => {
                const selected = option.value === selectedValue;
                const active = index === activeIndex;
                return (
                  <button
                    key={option.value}
                    type="button"
                    className={`command-select-option${selected ? " is-selected" : ""}${active ? " is-active" : ""}`}
                    role="option"
                    aria-selected={selected}
                    onMouseEnter={() => setActiveIndex(index)}
                    onClick={() => commitValue(option.value)}
                  >
                    <span className={`command-select-check${selected ? " is-checked" : ""}`}>
                      {selected ? "✓" : ""}
                    </span>
                    <span className="command-select-option-label">{option.label}</span>
                  </button>
                );
              })
            ) : (
              <div className="command-select-empty">No matches</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
