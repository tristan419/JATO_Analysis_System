import { useEffect, useState } from "react";
import type { AvailabilityState, CompareCellValue, CompareRow, CompareTrimItem, ComparisonType, ConfigValueState } from "../types/engineeringConfig";

export interface SourceEvidenceSelection {
  row: CompareRow;
  trim: CompareTrimItem;
  cell: CompareCellValue | null;
  selectionReason?: string;
}

interface SourceEvidenceDrawerProps {
  selection: SourceEvidenceSelection | null;
  onClose: () => void;
}

interface EvidenceBadge {
  label: string;
  tone: "info" | "success" | "warning";
}

const AVAILABILITY_LABELS: Record<AvailabilityState, string> = {
  STANDARD: "标配",
  OPTIONAL: "选装",
  NOT_AVAILABLE: "不配备",
  NOT_APPLICABLE: "不适用",
  VALUE: "参数",
  UNKNOWN: "未知",
  CANCELLED_OR_REMOVED: "取消 / 删除",
};

const VALUE_STATE_LABELS: Record<ConfigValueState, string> = {
  marker_value: "配置标记",
  blank: "空白",
  not_applicable: "不适用",
  cancelled_or_removed: "取消 / 删除",
  text_value: "文本值",
  numeric_value: "数值",
};

const COMPARISON_TYPE_LABELS: Record<ComparisonType, string> = {
  COMMON_SAME: "共同配置",
  DIFFERENT_VALUE: "值不同",
  UNIQUE_TO_TRIM: "独有配置",
  PARTIAL_AVAILABLE: "部分具备",
  MISSING_OR_UNKNOWN: "缺失 / 未知",
  MISSING_UNKNOWN: "待确认",
  NOT_APPLICABLE: "不适用",
  CANCELLED_OR_REMOVED: "取消 / 删除",
  AVAILABILITY_DIFFERENT: "可用性差异",
  OPTIONAL_DIFFERENT: "选装差异",
  UNIQUE_OR_PARTIAL: "部分具备",
};

function splitFeatureName(featureName: string): { en: string; zh: string } {
  const parts = featureName.split(" / ");
  if (parts.length < 2) return { en: featureName, zh: "" };
  return { en: parts[0], zh: parts.slice(1).join(" / ") };
}

function valueText(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

function rawValueText(value: string | number | boolean | null | undefined): string {
  if (value === "") return "空白";
  return valueText(value);
}

function labelWithCode(label: string, code: string): string {
  return `${label} (${code})`;
}

function availabilityText(value: AvailabilityState | null | undefined): string {
  if (!value) return "-";
  return labelWithCode(AVAILABILITY_LABELS[value], value);
}

function valueStateText(value: ConfigValueState | null | undefined): string {
  if (!value) return "-";
  return labelWithCode(VALUE_STATE_LABELS[value], value);
}

function comparisonTypeText(value: ComparisonType): string {
  return labelWithCode(COMPARISON_TYPE_LABELS[value], value);
}

function sourceTypeText(value: string | null | undefined): string {
  if (value === "pdf_text") return labelWithCode("文本 PDF", value);
  if (value === "pdf_ocr") return labelWithCode("扫描 PDF OCR", value);
  if (value === "image_ocr") return labelWithCode("图片 OCR", value);
  return value ? value : "工作簿 / 表格";
}

function trimSourceFileText(trim: CompareTrimItem): string {
  return valueText(trim.sourceFileName || trim.sourceFilePath || trim.sourceUploadId);
}

function trimSourceIdText(trim: CompareTrimItem): string {
  return valueText(trim.sourceUploadId || trim.sourceFilePath);
}

function inferredText(cell: CompareCellValue | null): string {
  if (!cell) return "-";
  return cell.inferred ? "是" : "否";
}

function manualOverrideText(cell: CompareCellValue | null): string {
  if (!cell) return "-";
  return cell.manualOverride ? "是" : "否";
}

function trimmedBusinessNote(row: CompareRow): string | null {
  const note = row.businessNote?.replace(/\s+/g, " ").trim();
  return note || null;
}

function businessNoteRequiresReview(note: string | null): boolean {
  if (!note) return false;
  return /需核对|待核对|待确认|缺失|缺少|回看/.test(note);
}

function evidenceBadges(
  cell: CompareCellValue | null,
  hasSource: boolean,
  isMergedExpanded: boolean,
  isOcrSource: boolean,
  hasReviewBusinessNote: boolean,
): EvidenceBadge[] {
  const badges: EvidenceBadge[] = [];
  if (!cell) {
    badges.push({ label: "缺配置值", tone: "warning" });
  } else if (cell.manualOverride) {
    badges.push({ label: "人工覆盖", tone: "warning" });
  } else if (cell.inferred) {
    badges.push({ label: "规则推断", tone: "warning" });
  } else {
    badges.push({ label: "原始记录", tone: "success" });
  }

  if (isMergedExpanded) badges.push({ label: "合并格展开", tone: "info" });
  if (isOcrSource) badges.push({ label: "OCR 识别", tone: "info" });
  if (hasReviewBusinessNote) badges.push({ label: "需核对说明", tone: "warning" });
  badges.push(hasSource ? { label: "有来源坐标", tone: "success" } : { label: "缺来源证据", tone: "warning" });
  return badges;
}

function evidenceSummaryText(cell: CompareCellValue | null, hasSource: boolean, isMergedExpanded: boolean): string {
  if (!cell) return "当前配置列在已发布配置中没有配置值记录，不能直接等同于不配备。";
  if (cell.manualOverride) return "当前显示值由有权限用户人工覆盖，不是原始文件单元格值；需要结合审计记录确认修改原因。";
  if (cell.inferred && isMergedExpanded) return "当前显示值包含规则推断，并且来源值来自横向合并格展开，需要同时核对推断规则和合并范围。";
  if (cell.inferred) return "当前显示值由规则推断生成，不是 Excel 单元格原文，需要回看推断规则和置信度。";
  if (isMergedExpanded) return "当前显示值来自横向合并格展开，原始值应追溯到原始单元格和合并范围。";
  if (!hasSource) return "当前值存在于已发布配置，但暂无来源证据，无法在页面内追溯到原始文件坐标。";
  return "当前值可追溯到来源文件坐标，可用于解释该配置行在当前配置列下的取值。";
}

function evidenceCopyText(selection: SourceEvidenceSelection, summaryText: string): string {
  const { cell, row, selectionReason, trim } = selection;
  const feature = splitFeatureName(row.featureName);
  const source = cell?.source ?? null;
  return [
    "Config source evidence",
    `Feature: ${row.featureName}`,
    `Feature EN: ${feature.en}`,
    `Feature ZH: ${feature.zh || "-"}`,
    `Category: ${row.category}`,
    `Comparison Type: ${comparisonTypeText(row.comparisonType)}`,
    `Business Note: ${valueText(trimmedBusinessNote(row))}`,
    `Config column: ${trim.fullTrimName || trim.trimName || trim.trimId}`,
    `Source File: ${trimSourceFileText(trim)}`,
    `Source ID: ${trimSourceIdText(trim)}`,
    `Display Value: ${valueText(cell?.displayValue ?? cell?.rawValue)}`,
    `Raw Value: ${rawValueText(cell?.rawValue)}`,
    `Value State: ${valueStateText(cell?.valueState)}`,
    `Availability: ${availabilityText(cell?.availability)}`,
    `Manual Override: ${manualOverrideText(cell)}`,
    `Inferred: ${inferredText(cell)}`,
    `Inference Reason: ${valueText(cell?.inferenceReason || source?.inferenceReason)}`,
    `Confidence: ${valueText(cell?.confidence ?? source?.confidence)}`,
    `Sheet: ${valueText(source?.sheetName)}`,
    `Source Type: ${source ? sourceTypeText(source.sourceType) : "-"}`,
    `Page: ${valueText(source?.pageNumber)}`,
    `OCR Engine: ${valueText(source?.ocrEngine)}`,
    `Row: ${valueText(source?.rowNumber)}`,
    `Column: ${valueText(source?.columnLetter)}`,
    `Cell: ${valueText(source?.cell)}`,
    `Source Cell: ${valueText(source?.sourceCell)}`,
    `Merged Range: ${valueText(source?.mergedRange)}`,
    `Source Evidence: ${source ? "available" : "missing"}`,
    `Selection Reason: ${selectionReason || "-"}`,
    `Evidence Summary: ${summaryText}`,
  ].join("\n");
}

export function SourceEvidenceDrawer({ selection, onClose }: SourceEvidenceDrawerProps) {
  const [copyFeedback, setCopyFeedback] = useState<string | null>(null);

  useEffect(() => {
    setCopyFeedback(null);
  }, [selection]);

  useEffect(() => {
    if (!selection) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose, selection]);

  if (!selection) return null;

  const { cell, row, selectionReason, trim } = selection;
  const feature = splitFeatureName(row.featureName);
  const source = cell?.source ?? null;
  const isMergedExpanded = Boolean(source?.mergedRange && source.sourceCell && source.sourceCell !== source.cell);
  const isOcrSource = Boolean(source?.ocrEngine || source?.sourceType === "pdf_ocr" || source?.sourceType === "image_ocr");
  const businessNote = trimmedBusinessNote(row);
  const hasReviewBusinessNote = businessNoteRequiresReview(businessNote);
  const badges = evidenceBadges(cell, Boolean(source), isMergedExpanded, isOcrSource, hasReviewBusinessNote);
  const summaryText = evidenceSummaryText(cell, Boolean(source), isMergedExpanded);

  async function copyEvidence(): Promise<void> {
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback("当前浏览器不支持复制，请手动选中证据内容。");
      return;
    }
    try {
      await navigator.clipboard.writeText(evidenceCopyText({ cell, row, selectionReason, trim }, summaryText));
      setCopyFeedback("证据包已复制。");
    } catch (reason: unknown) {
      setCopyFeedback(reason instanceof Error ? reason.message : "复制失败，请手动选中证据内容。");
    }
  }

  return (
    <div className="source-evidence-layer" role="presentation" onMouseDown={onClose}>
      <aside className="source-evidence-drawer" aria-label="配置来源证据" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <header className="source-evidence-drawer__header">
          <div>
            <span className="market-scan-panel-eyebrow">来源证据</span>
            <h2>配置来源</h2>
          </div>
          <div className="source-evidence-drawer__actions">
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              aria-label="复制当前配置来源证据"
              onClick={() => {
                void copyEvidence();
              }}
            >
              复制证据
            </button>
            <button className="btn btn-sm btn-secondary" type="button" onClick={onClose}>关闭</button>
          </div>
        </header>
        {copyFeedback ? (
          <em className="source-evidence-copy-feedback" role="status">{copyFeedback}</em>
        ) : null}

        <section className="source-evidence-overview" aria-label="证据摘要">
          <div className="source-evidence-badges">
            {badges.map((badge) => (
              <span className={`source-evidence-badge source-evidence-badge--${badge.tone}`} key={badge.label}>
                {badge.label}
              </span>
            ))}
          </div>
          <p>{summaryText}</p>
          {selectionReason ? (
            <small><strong>触发来源</strong>{selectionReason}</small>
          ) : null}
        </section>

        <section className="source-evidence-section">
          <span className="source-evidence-section__title">配置行</span>
          <dl className="source-evidence-grid">
            <div><dt>英文</dt><dd>{feature.en}</dd></div>
            <div><dt>中文</dt><dd>{feature.zh || "-"}</dd></div>
            <div><dt>大类</dt><dd>{row.category}</dd></div>
            <div><dt>差异类型</dt><dd>{comparisonTypeText(row.comparisonType)}</dd></div>
            {businessNote ? <div><dt>业务备注</dt><dd>{businessNote}</dd></div> : null}
          </dl>
        </section>

        <section className="source-evidence-section">
          <span className="source-evidence-section__title">当前配置列取值</span>
          <dl className="source-evidence-grid">
            <div><dt>配置列</dt><dd>{trim.fullTrimName || trim.trimName || trim.trimId}</dd></div>
            <div><dt>来源文件</dt><dd>{trimSourceFileText(trim)}</dd></div>
            <div><dt>来源 ID</dt><dd>{trimSourceIdText(trim)}</dd></div>
            <div><dt>显示值</dt><dd>{valueText(cell?.displayValue ?? cell?.rawValue)}</dd></div>
            <div><dt>原始值</dt><dd>{rawValueText(cell?.rawValue)}</dd></div>
            <div><dt>原始值类型</dt><dd>{valueStateText(cell?.valueState)}</dd></div>
            <div><dt>配置状态</dt><dd>{availabilityText(cell?.availability)}</dd></div>
            <div><dt>是否人工覆盖</dt><dd>{manualOverrideText(cell)}</dd></div>
            <div><dt>是否规则推断</dt><dd>{inferredText(cell)}</dd></div>
            <div><dt>推断规则</dt><dd>{valueText(cell?.inferenceReason || source?.inferenceReason)}</dd></div>
            <div><dt>置信度</dt><dd>{valueText(cell?.confidence ?? source?.confidence)}</dd></div>
          </dl>
          {!cell ? (
            <div className="source-evidence-callout source-evidence-callout--warning">
              <strong>当前配置列没有配置值记录。</strong>
              <span>这表示该字段在当前已发布配置中缺失，需要回看来源或重新消化数据；不能直接等同于不配备。</span>
            </div>
          ) : null}
          {selectionReason ? (
            <div className="source-evidence-callout">
              <strong>证据选择原因</strong>
              <span>{selectionReason}</span>
            </div>
          ) : null}
          {hasReviewBusinessNote && businessNote ? (
            <div className="source-evidence-callout source-evidence-callout--warning">
              <strong>该配置行带需核对说明。</strong>
              <span>{businessNote}</span>
            </div>
          ) : null}
          {cell?.inferred ? (
            <div className="source-evidence-callout source-evidence-callout--warning">
              <strong>该值为规则推断，不是 Excel 原文。</strong>
              <span>推断规则：{valueText(cell.inferenceReason || source?.inferenceReason)}</span>
            </div>
          ) : null}
          {cell?.manualOverride ? (
            <div className="source-evidence-callout source-evidence-callout--warning">
              <strong>该值为人工覆盖，不是原始文件单元格值。</strong>
              <span>原始值与修改人保留在审计记录中；引用到业务结论前请确认本次人工修改的原因。</span>
            </div>
          ) : null}
        </section>

        <section className="source-evidence-section">
          <span className="source-evidence-section__title">来源坐标</span>
          {source ? (
            <>
              <dl className="source-evidence-grid">
                <div><dt>来源文件</dt><dd>{trimSourceFileText(trim)}</dd></div>
                <div><dt>来源 ID</dt><dd>{trimSourceIdText(trim)}</dd></div>
                <div><dt>工作表</dt><dd>{source.sheetName}</dd></div>
                <div><dt>来源类型</dt><dd>{sourceTypeText(source.sourceType)}</dd></div>
                <div><dt>页码</dt><dd>{valueText(source.pageNumber)}</dd></div>
                <div><dt>OCR 引擎</dt><dd>{valueText(source.ocrEngine)}</dd></div>
                <div><dt>行号</dt><dd>{source.rowNumber}</dd></div>
                <div><dt>列</dt><dd>{source.columnLetter}</dd></div>
                <div><dt>当前单元格</dt><dd>{source.cell}</dd></div>
                <div><dt>原始单元格</dt><dd>{valueText(source.sourceCell)}</dd></div>
                <div><dt>合并范围</dt><dd>{valueText(source.mergedRange)}</dd></div>
              </dl>
              {isOcrSource ? (
                <div className="source-evidence-callout">
                  <strong>该值来自 OCR 消化结果。</strong>
                  <span>请结合来源类型、页码和 OCR 引擎回看原始 PDF / 图片，引用到卖点前建议核对原文。</span>
                </div>
              ) : null}
              {isMergedExpanded ? (
                <div className="source-evidence-callout">
                  <strong>该值来自合并单元格展开。</strong>
                  <span>合并范围 {source.mergedRange}，原始值来自 {source.sourceCell}，当前配置列使用 {source.cell}。</span>
                </div>
              ) : null}
            </>
          ) : (
            <div className="source-evidence-empty">
              {cell?.manualOverride ? "当前值为人工覆盖，故不使用原始文件坐标作为证据。" : "当前已发布配置暂无来源证据。"}
            </div>
          )}
        </section>
      </aside>
    </div>
  );
}
