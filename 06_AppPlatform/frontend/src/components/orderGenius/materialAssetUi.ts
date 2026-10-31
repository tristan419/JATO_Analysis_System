import { useState } from "react";
export type AssetLanguage = "en" | "zh";
export type AssetText = (en: string, zh: string) => string;
export function useAssetLanguage(): { language: AssetLanguage; setLanguage: (language: AssetLanguage) => void; t: AssetText } {
  const [language, update] = useState<AssetLanguage>(() => localStorage.getItem("jato:material-language") === "zh" ? "zh" : "en");
  const setLanguage = (value: AssetLanguage): void => { localStorage.setItem("jato:material-language", value); update(value); };
  return { language, setLanguage, t: (en, zh) => language === "zh" ? zh : en };
}
export function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
