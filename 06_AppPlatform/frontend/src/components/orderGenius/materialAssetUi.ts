import { useState } from "react";
import type { ConfigCell } from "../../types/materialAssets";
export type AssetLanguage = "en" | "zh";
export type AssetText = (en: string, zh: string) => string;
export function useAssetLanguage(): { language: AssetLanguage; setLanguage: (language: AssetLanguage) => void; t: AssetText } {
  const [language, update] = useState<AssetLanguage>(() => localStorage.getItem("jato:material-language") === "zh" ? "zh" : "en");
  const setLanguage = (value: AssetLanguage): void => { localStorage.setItem("jato:material-language", value); update(value); };
  return { language, setLanguage, t: (en, zh) => language === "zh" ? zh : en };
}
export function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
export function configurationValue(cell: ConfigCell | null, t: AssetText): string {
  if (!cell) return t("Not recorded", "未记录");
  if (!cell.raw_value?.trim()) return t("Not filled", "未填写");
  const equipment = ["STANDARD", "OPTIONAL", "NOT_AVAILABLE", "NOT_APPLICABLE"].includes(cell.availability ?? "");
  const unit = !equipment && cell.unit && !cell.raw_value.toLowerCase().endsWith(cell.unit.toLowerCase()) ? ` ${cell.unit}` : "";
  return cell.raw_value + unit;
}
