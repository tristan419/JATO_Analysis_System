import { useEffect, useState, type CSSProperties, type ChangeEvent } from "react";

interface DebouncedNumberInputProps {
  value: number | null;
  onCommit: (value: number | null) => void;
  min: number;
  max: number;
  step?: number;
  delayMs?: number;
  allowEmpty?: boolean;
  placeholder?: string;
  disabled?: boolean;
  className?: string;
  inputMode?: "numeric" | "decimal";
  style?: CSSProperties;
  onDraftChange?: (value: string) => void;
}

function clampEditableNumber(value: number, min: number, max: number): number {
  return Math.trunc(Math.min(max, Math.max(min, value)));
}

function parseEditableNumber(value: string, min: number, max: number): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return clampEditableNumber(parsed, min, max);
}

export function DebouncedNumberInput({
  value,
  onCommit,
  min,
  max,
  step = 1,
  delayMs = 1200,
  allowEmpty = false,
  placeholder,
  disabled = false,
  className,
  inputMode,
  style,
  onDraftChange,
}: DebouncedNumberInputProps) {
  const [draft, setDraft] = useState<string>(() => (value === null ? "" : String(value)));

  useEffect(() => {
    setDraft(value === null ? "" : String(value));
  }, [value]);

  useEffect(() => {
    const nextValue = parseEditableNumber(draft, min, max);
    if (nextValue === null) {
      if (!allowEmpty || value === null || draft.trim()) {
        return undefined;
      }
      const timeoutId = window.setTimeout(() => {
        onCommit(null);
      }, delayMs);
      return () => window.clearTimeout(timeoutId);
    }
    const normalizedDraft = String(nextValue);
    if (nextValue === value && draft === normalizedDraft) {
      return undefined;
    }
    const timeoutId = window.setTimeout(() => {
      onCommit(nextValue);
      setDraft(normalizedDraft);
    }, delayMs);
    return () => window.clearTimeout(timeoutId);
  }, [allowEmpty, delayMs, draft, max, min, onCommit, value]);

  function handleChange(event: ChangeEvent<HTMLInputElement>): void {
    const nextDraft = event.target.value;
    setDraft(nextDraft);
    onDraftChange?.(nextDraft);
  }

  return (
    <input
      type="number"
      min={min}
      max={max}
      step={step}
      value={draft}
      onChange={handleChange}
      placeholder={placeholder}
      disabled={disabled}
      className={className}
      inputMode={inputMode}
      style={style}
    />
  );
}
