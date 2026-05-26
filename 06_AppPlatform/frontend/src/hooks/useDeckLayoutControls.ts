import { useState, type CSSProperties } from "react";

export type DeckLayoutDirection = "row" | "column";

export interface DeckLayoutStorageKeys {
  direction: string;
  splitRatio: string;
  chartHeight: string;
}

export interface DeckLayoutDefaults {
  direction: DeckLayoutDirection;
  splitRatio: number;
  chartHeight: number;
}

export interface DeckLayoutRanges {
  splitRatio: {
    min: number;
    max: number;
  };
  chartHeight: {
    min: number;
    max: number;
  };
}

export interface DeckLayoutCssVariables {
  chartHeight: `--${string}`;
  splitRatio: `--${string}`;
  remainderRatio: `--${string}`;
}

export interface UseDeckLayoutControlsOptions {
  storageKeys: DeckLayoutStorageKeys;
  defaults: DeckLayoutDefaults;
  ranges: DeckLayoutRanges;
  cssVariables?: DeckLayoutCssVariables;
}

export type DeckLayoutGridStyle = CSSProperties & {
  [key: `--${string}`]: string | number;
};

const DEFAULT_CSS_VARIABLES: DeckLayoutCssVariables = {
  chartHeight: "--deck-chart-height",
  splitRatio: "--deck-split-ratio",
  remainderRatio: "--deck-remainder-ratio",
};

function isDeckLayoutDirection(value: unknown): value is DeckLayoutDirection {
  return value === "row" || value === "column";
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function readStoredJson(key: string): unknown {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function readStoredLayoutDirection(key: string, fallback: DeckLayoutDirection): DeckLayoutDirection {
  const stored = readStoredJson(key);
  return isDeckLayoutDirection(stored) ? stored : fallback;
}

function readStoredNumber(key: string, fallback: number, min: number, max: number): number {
  const stored = readStoredJson(key);
  const value = typeof stored === "number" ? stored : Number(stored);
  return Number.isFinite(value) ? clampNumber(value, min, max) : fallback;
}

function writeStoredJson(key: string, value: string | number): void {
  localStorage.setItem(key, JSON.stringify(value));
}

function buildDeckLayoutGridStyle(
  splitRatio: number,
  chartHeight: number,
  cssVariables: DeckLayoutCssVariables,
): DeckLayoutGridStyle {
  const style: DeckLayoutGridStyle = {};
  style[cssVariables.chartHeight] = `${chartHeight}px`;
  style[cssVariables.splitRatio] = `${splitRatio}fr`;
  style[cssVariables.remainderRatio] = `${100 - splitRatio}fr`;
  return style;
}

export function useDeckLayoutControls({
  storageKeys,
  defaults,
  ranges,
  cssVariables = DEFAULT_CSS_VARIABLES,
}: UseDeckLayoutControlsOptions) {
  const [layoutDirection, setLayoutDirectionState] = useState<DeckLayoutDirection>(
    () => readStoredLayoutDirection(storageKeys.direction, defaults.direction),
  );
  const [splitRatio, setSplitRatioState] = useState<number>(() => (
    readStoredNumber(
      storageKeys.splitRatio,
      defaults.splitRatio,
      ranges.splitRatio.min,
      ranges.splitRatio.max,
    )
  ));
  const [chartHeight, setChartHeightState] = useState<number>(() => (
    readStoredNumber(
      storageKeys.chartHeight,
      defaults.chartHeight,
      ranges.chartHeight.min,
      ranges.chartHeight.max,
    )
  ));

  function setLayoutDirection(nextDirection: DeckLayoutDirection): void {
    setLayoutDirectionState(nextDirection);
    writeStoredJson(storageKeys.direction, nextDirection);
  }

  function setSplitRatio(nextRatio: number): void {
    const normalizedRatio = clampNumber(nextRatio, ranges.splitRatio.min, ranges.splitRatio.max);
    setSplitRatioState(normalizedRatio);
    writeStoredJson(storageKeys.splitRatio, normalizedRatio);
  }

  function setChartHeight(nextHeight: number): void {
    const normalizedHeight = clampNumber(nextHeight, ranges.chartHeight.min, ranges.chartHeight.max);
    setChartHeightState(normalizedHeight);
    writeStoredJson(storageKeys.chartHeight, normalizedHeight);
  }

  function resetLayout(): void {
    setLayoutDirection(defaults.direction);
    setSplitRatio(defaults.splitRatio);
    setChartHeight(defaults.chartHeight);
  }

  return {
    layoutDirection,
    splitRatio,
    chartHeight,
    gridStyle: buildDeckLayoutGridStyle(splitRatio, chartHeight, cssVariables),
    setLayoutDirection,
    setSplitRatio,
    setChartHeight,
    resetLayout,
  };
}
