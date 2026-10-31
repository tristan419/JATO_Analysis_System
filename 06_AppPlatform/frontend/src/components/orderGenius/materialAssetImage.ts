import { useEffect, useState } from "react";
import { materialAssets } from "../../api/materialAssets";
import { errorMessage } from "./materialAssetUi";

export function useAssetImage(fileId: string | undefined, scope: string, enabled = true): { url: string | null; error: string } {
  const key = `${scope}|${fileId}|${enabled}`;
  const [result, setResult] = useState<{ key: string; url: string | null; error: string } | null>(null);
  useEffect(() => {
    if (!enabled || !fileId) return;
    const abort = new AbortController(); let url: string | null = null;
    void materialAssets.photo(fileId, abort.signal).then(blob => {
      if (abort.signal.aborted) return;
      url = URL.createObjectURL(blob); setResult({ key, url, error: "" });
    }).catch(error => { if (!abort.signal.aborted) setResult({ key, url: null, error: errorMessage(error) }); });
    return () => { abort.abort(); if (url) URL.revokeObjectURL(url); };
  }, [fileId, key, enabled]);
  return result?.key === key ? result : { url: null, error: "" };
}
