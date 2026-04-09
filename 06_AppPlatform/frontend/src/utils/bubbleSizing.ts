export interface BubbleSizingOptions {
  maxDiameter?: number;
  minDiameter?: number;
}

export interface BubbleSizingResult {
  sizes: number[];
  sizeref: number;
  sizemin: number;
  sizemode: "area";
}

/**
 * Build Plotly bubble sizing parameters so all bubble charts share the same sizing logic.
 * Returns normalized sizeref/sizemin/sizemode suitable for Plotly scatter marker props.
 * Default max diameter is 48px, min diameter is 6px.
 */
export function buildBubbleSizing(
  input: number[],
  options: BubbleSizingOptions = {},
): BubbleSizingResult {
  const maxDiameter = options.maxDiameter ?? 48;
  const minDiameter = options.minDiameter ?? 6;
  const sizes = input.map((v) => Math.max(1, v));
  const maxVal = Math.max(1, ...sizes);
  const sizeref = (2 * maxVal) / Math.max(1, maxDiameter * maxDiameter);
  return { sizes, sizeref, sizemin: minDiameter, sizemode: "area" };
}
