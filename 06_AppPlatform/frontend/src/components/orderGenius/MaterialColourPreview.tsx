import { useEffect, useRef, useState, type CSSProperties } from "react";
import { createPortal } from "react-dom";
import type { AssetCatalog, AssetGroup } from "../../types/materialAssets";
import { MaterialPhotoViewer } from "./MaterialPhotoViewer";
import { useAssetImage } from "./materialAssetImage";
import { errorMessage, useAssetLanguage } from "./materialAssetUi";
interface Props {
  material: { brand?: string; modelName: string; bomTemplate?: string | null; colourCode?: string | null; interiorColorName?: string | null; interiorColourCode?: string | null };
  kind: "exterior" | "interior"; name: string; background?: string; scope: string;
  loadCatalog?: () => Promise<AssetCatalog>;
}
export function MaterialColourPreview({ material, kind, name, background, scope, loadCatalog }: Props) {
  const { t } = useAssetLanguage();
  const anchor = useRef<HTMLButtonElement>(null), closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [open, setOpen] = useState(false), [gallery, setGallery] = useState<string | null>(null);
  const [position, setPosition] = useState<CSSProperties>({});
  const key = JSON.stringify([scope, material.brand, material.modelName, material.bomTemplate, kind, material.colourCode, material.interiorColorName, material.interiorColourCode]);
  const [result, setResult] = useState<{ key: string; group: AssetGroup | null; error: string } | null>(null);
  const current = result?.key === key ? result : null;
  const image = useAssetImage(current?.group?.files[0]?.id, scope, open);
  const show = (): void => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    if (!open) setResult(null);
    const rect = anchor.current?.getBoundingClientRect();
    if (rect) setPosition({ left: Math.max(8, Math.min(rect.left, window.innerWidth - 256)), top: rect.bottom + 174 > window.innerHeight ? Math.max(8, rect.top - 174) : rect.bottom + 6 });
    setOpen(true);
  };
  const hide = (): void => { closeTimer.current = setTimeout(() => setOpen(false), 100); };
  useEffect(() => () => { if (closeTimer.current) clearTimeout(closeTimer.current); }, []);
  useEffect(() => {
    if ((!open && gallery !== key) || !loadCatalog || current) return;
    let active = true;
    void loadCatalog().then(data => {
      if (!active) return;
      const base = data.materials.find(item => item.brand === material.brand && item.modelName === material.modelName && item.bomTemplate === material.bomTemplate);
      const code = (kind === "exterior" ? material.colourCode : material.interiorColourCode)?.trim().toUpperCase();
      const colourName = material.interiorColorName?.trim().toUpperCase();
      const colours = (kind === "exterior" ? base?.colours : base?.interiors)?.filter(item => code ? item.code?.trim().toUpperCase() === code
        : kind === "interior" && Boolean(colourName) && item.name?.trim().toUpperCase() === colourName);
      const colour = colours?.length === 1 ? colours[0] : undefined;
      const binding = colour && data.bindings.find(item => item.materialKey === base?.key && item.kind === kind && item.colourKey === colour.key);
      setResult({ key, group: data.groups.find(group => group.id === binding?.groupId) ?? null, error: "" });
    }).catch(failure => { if (active) setResult({ key, group: null, error: errorMessage(failure) }); });
    return () => { active = false; };
  }, [open, gallery, key, loadCatalog, current]);
  useEffect(() => { if (gallery === key && current?.group?.files.length) setOpen(false); }, [gallery, key, current]);
  useEffect(() => {
    if (!open) return;
    const close = (): void => setOpen(false);
    window.addEventListener("scroll", close, true);
    const escape = (event: KeyboardEvent): void => { if (event.key === "Escape") { setOpen(false); event.stopPropagation(); } };
    window.addEventListener("keydown", escape, true);
    return () => { window.removeEventListener("scroll", close, true); window.removeEventListener("keydown", escape, true); };
  }, [open]);
  const view = (): void => { show(); setGallery(key); };
  return <>
    <button ref={anchor} type="button" className="og-photo-swatch" disabled={!loadCatalog || !material.brand || !material.bomTemplate}
      aria-label={`${t("Preview", "预览")} ${name} ${t("photos", "图片")}`} onMouseEnter={show} onMouseLeave={hide} onFocus={show} onBlur={hide}
      onClick={event => { event.stopPropagation(); view(); }} style={background ? { background } : undefined}>{kind === "interior" ? "▧" : null}</button>
    {open ? createPortal(<div className="mw-swatch-preview" style={position} onMouseEnter={show} onMouseLeave={hide} onFocus={show} onBlur={hide}>
      {current?.group?.files.length ? <button type="button" aria-label={t("Open photo gallery", "打开图库")} onClick={view}>{image.url ? <img src={image.url} alt={name} /> : <span role={image.error ? "alert" : "status"}>{image.error || t("Loading…", "读取中…")}</span>}</button>
        : <span role={current?.error ? "alert" : "status"}>{current?.error || (current ? t("No photos linked", "暂无关联图片") : t("Loading…", "读取中…"))}</span>}
    </div>, document.body) : null}
    {gallery === key && current?.group ? <MaterialPhotoViewer group={current.group} scope={scope} t={t} onClose={() => setGallery(null)} /> : null}
  </>;
}
