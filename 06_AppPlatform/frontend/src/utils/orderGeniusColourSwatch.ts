export const MISSING_COLOUR_SWATCH_HEX = "#94A3B8";

const COLOUR_HEX_PATTERN = /^#[0-9A-F]{6}$/;

export interface OrderGeniusColourSwatch {
  colours: readonly string[];
  background: string;
  isDual: boolean;
  isMissing: boolean;
}

export function normalizeColourHex(value: unknown): string | null {
  const normalized = String(value ?? "").trim().toUpperCase();
  return COLOUR_HEX_PATTERN.test(normalized) ? normalized : null;
}

export function parseOrderGeniusColourSwatch(value: unknown): OrderGeniusColourSwatch {
  const rawParts = String(value ?? "").split("|");
  const colours = rawParts.map(normalizeColourHex);
  if (
    rawParts.length < 1
    || rawParts.length > 2
    || colours.some((colour) => colour === null)
  ) {
    return {
      colours: [MISSING_COLOUR_SWATCH_HEX],
      background: MISSING_COLOUR_SWATCH_HEX,
      isDual: false,
      isMissing: true,
    };
  }
  const validColours = colours as string[];
  const [first, second] = validColours;
  return {
    colours: validColours,
    background: second
      ? `linear-gradient(135deg, ${first} 50%, ${second} 50%)`
      : first,
    isDual: Boolean(second),
    isMissing: false,
  };
}

export function buildOrderGeniusColourSwatch(
  firstValue: unknown,
  secondValue: unknown,
  isDual: boolean,
): string | null {
  const first = normalizeColourHex(firstValue);
  if (!first) return null;
  if (!isDual) return first;
  const second = normalizeColourHex(secondValue) ?? first;
  return `${first}|${second}`;
}
