import { useEffect, useState } from "react";
import { materialAssets } from "../../api/materialAssets";
import type { MaterialSkuMatrixRow } from "../../types/orderGenius";
import type { AssetGroup } from "../../types/materialAssets";
import { ConfirmDialog } from "../ConfirmDialog";
import { MaterialPhotoViewer } from "./MaterialPhotoViewer";
import { errorMessage, useAssetLanguage } from "./materialAssetUi";
interface Props { material: MaterialSkuMatrixRow; scope: string; onClose: () => void }
export function MaterialColourPreview({ material, scope, onClose }: Props) {
  const { t } = useAssetLanguage();
  const [result, setResult] = useState<{ group: AssetGroup | null; error: string } | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    void materialAssets.catalog(abort.signal).then(data => {
      if (abort.signal.aborted) return;
      const m = data.materials.find(item => item.brand === material.brand && item.modelName === material.modelName && item.bomTemplate === material.bomTemplate);
      const binding = data.bindings.find(b => b.materialKey === m?.key && b.kind === "exterior" && b.colourKey === `code:${material.colourCode.trim().toUpperCase()}`);
      setResult({ group: data.groups.find(g => g.id === binding?.groupId) ?? null, error: "" });
    }).catch(e => { if (!abort.signal.aborted) setResult({ group: null, error: errorMessage(e) }); });
    return () => abort.abort();
  }, [material]);
  if (result?.group) return <MaterialPhotoViewer group={result.group} scope={scope} t={t} onClose={onClose} />;
  return <ConfirmDialog title={`${material.bomTemplate} · ${material.colourCode}`} description={result ? t("No photos linked", "暂无关联图片") : t("Loading…", "读取中…")} cancelLabel={t("Close", "关闭")} confirmLabel={t("Download", "下载")} loadingLabel="…" submitting={false} confirmDisabled error={result?.error ? { title: t("Read failed", "读取失败"), message: result.error } : null} onCancel={onClose} onConfirm={onClose} />;
}
