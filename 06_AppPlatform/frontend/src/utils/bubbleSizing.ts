const DEFAULT_MAX_DIAMETER = 48;
const DEFAULT_MIN_DIAMETER = 6;

export interface BubbleSizingOptions {
  maxDiameter?: number;
  minDiameter?: number;
}

export interface BubbleSizingResult {
  values: number[];
  sizeref: number;
  sizemin: number;
  sizemode: "area";
}

export function buildBubbleSizing(
  input: Array<number | null | undefined>,
  options: BubbleSizingOptions = {},
): BubbleSizingResult {
  const values = input.map((value) => {
    const numeric = Number(value ?? 0);
    return Number.isFinite(numeric) ? Math.max(0, numeric) : 0;
  });
  const maxValue = Math.max(1, ...values);
  const maxDiameter = Math.max(1, options.maxDiameter ?? DEFAULT_MAX_DIAMETER);
  const minDiameter = Math.max(1, options.minDiameter ?? DEFAULT_MIN_DIAMETER);

  return {
    values,
    sizeref: (2 * maxValue) / Math.max(1, maxDiameter * maxDiameter),
    sizemin: minDiameter,
    sizemode: "area",
  };
}