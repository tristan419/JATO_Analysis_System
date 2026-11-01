import { useEffect, useState } from "react";
import { materialAssets } from "../../api/materialAssets";
import { errorMessage } from "./materialAssetUi";

type Variant = "thumbnail" | "display";
interface CachedImage { refs: number; abort: AbortController; ready: Promise<string>; url: string | null; revoke: (url: string) => void }
const images = new Map<string, CachedImage>();
function releaseImage(key: string, entry: CachedImage): void {
  if (entry.refs || images.get(key) !== entry) return;
  entry.abort.abort(); if (entry.url) entry.revoke(entry.url); images.delete(key);
}
export function useAssetImage(fileId: string | undefined, scope: string, enabled = true, variant: Variant = "thumbnail"): { url: string | null; error: string } {
  const key = `${scope}|${fileId}|${enabled}|${variant}`;
  const [result, setResult] = useState<{ key: string; url: string | null; error: string } | null>(null);
  useEffect(() => {
    if (!enabled || !fileId) return;
    let entry = images.get(key);
    if (!entry) {
      const abort = new AbortController();
      const created: CachedImage = { refs: 0, abort, url: null, revoke: url => URL.revokeObjectURL(url), ready: Promise.resolve("") };
      const revoke = URL.revokeObjectURL;
      created.revoke = url => revoke(url);
      created.ready = materialAssets.photo(fileId, abort.signal, variant).then(blob => {
        if (abort.signal.aborted) throw new DOMException("Aborted", "AbortError");
        created.url = URL.createObjectURL(blob); return created.url;
      });
      entry = created; images.set(key, entry);
    }
    entry.refs++; images.delete(key); images.set(key, entry);
    let active = true;
    void entry.ready.then(url => { if (active) setResult({ key, url, error: "" }); })
      .catch(error => { if (active && !entry.abort.signal.aborted) setResult({ key, url: null, error: errorMessage(error) }); });
    return () => {
      active = false; entry.refs--;
      // Let the adjacent preload hand its URL to the current image before evicting.
      queueMicrotask(() => releaseImage(key, entry));
    };
  }, [fileId, key, enabled, variant]);
  return result?.key === key ? result : { url: null, error: "" };
}
