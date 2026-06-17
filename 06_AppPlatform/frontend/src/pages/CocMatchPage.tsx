import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type DragEvent,
} from "react";
import { api } from "../api/client";
import {
  DeckControlTabs,
  DeckFloatingDrawer,
  type DeckControlTabItem,
} from "../components/deckControls";
import type { CocFillDecision, CocFillJob, CocFillPreviewGroup, CocFillRecord, CocMatchJob } from "../types";

type CocWorkspaceMode = "match" | "fill";
type FillPreviewStatusFilter = "all" | "filled" | "not_found" | "ambiguous" | "skipped_existing";
type PendingFillSelection =
  | { kind: "candidate"; decision: CocFillDecision; candidate: CocFillRecord }
  | { kind: "manual"; decision: CocFillDecision; wvtaNo: string; cocNo: string };

type ManualFillDraft = {
  wvtaNo: string;
  cocNo: string;
};

const WORKSPACE_TABS: Array<DeckControlTabItem<CocWorkspaceMode>> = [
  { key: "match", label: "COC 比对", caption: "VIN 与文件包" },
  { key: "fill", label: "COC 填充", caption: "物料号组回填" },
];

function formatTs(ts: string | null | undefined): string {
  if (!ts) return "-";
  const d = new Date(ts);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function statusLabel(status: string): string {
  const labels: Record<string, string> = {
    queued: "排队中",
    running: "运行中",
    success: "已完成",
    failed: "失败",
  };
  return labels[status] ?? status;
}

function statusColor(status: string): string {
  if (status === "success") return "#16a34a";
  if (status === "failed") return "#dc2626";
  if (status === "running") return "#2563eb";
  return "#6b7280";
}

function cocDifferenceLabel(type: string | null | undefined): string {
  const labels: Record<string, string> = {
    matched: "完全一致",
    missing_archive_files: "Excel 有、压缩包缺失",
    archive_only_files: "压缩包有、Excel 缺码",
    bidirectional_mismatch: "双向不一致",
  };
  return type ? (labels[type] ?? type) : "-";
}

function fillStrategyLabel(strategy: string): string {
  if (strategy === "strict") return "严格唯一";
  if (strategy === "date_country") return "日期 / 国家";
  return strategy;
}

function fillDecisionStatusLabel(status: string): string {
  const labels: Record<string, string> = {
    filled: "已填充",
    skipped_existing: "跳过已有",
    not_found: "未命中",
    ambiguous: "冲突",
    invalid_source: "无效来源",
  };
  return labels[status] ?? status;
}

function fillDecisionStatusColor(status: string): string {
  if (status === "filled") return "#16a34a";
  if (status === "not_found" || status === "invalid_source") return "#dc2626";
  if (status === "ambiguous") return "#b45309";
  return "#64748b";
}

function fillPreviewFilterLabel(filter: FillPreviewStatusFilter): string {
  const labels: Record<FillPreviewStatusFilter, string> = {
    all: "识别",
    filled: "填充",
    not_found: "未命中",
    ambiguous: "冲突",
    skipped_existing: "跳过",
  };
  return labels[filter];
}

function fillPreviewFilterButtonLabel(filter: FillPreviewStatusFilter): string {
  return filter === "all" ? "全部" : fillPreviewFilterLabel(filter);
}

function buildFallbackPreviewGroups(job: CocFillJob): CocFillPreviewGroup[] {
  const groups = new Map<string, CocFillPreviewGroup>();
  for (const decision of job.decisions || []) {
    const group = groups.get(decision.sheetName) || {
      sheetName: decision.sheetName,
      totalRows: 0,
      filledCount: 0,
      notFoundCount: 0,
      ambiguousCount: 0,
      skippedExistingCount: 0,
      invalidSourceCount: 0,
      statusCounts: {},
      decisions: [],
      truncated: false,
    };
    group.totalRows += 1;
    group.statusCounts = {
      ...(group.statusCounts || {}),
      [decision.status]: (group.statusCounts?.[decision.status] || 0) + 1,
    };
    if (decision.status === "filled") group.filledCount += 1;
    if (decision.status === "not_found") group.notFoundCount += 1;
    if (decision.status === "ambiguous") group.ambiguousCount += 1;
    if (decision.status === "skipped_existing") group.skippedExistingCount += 1;
    if (decision.status === "invalid_source") group.invalidSourceCount += 1;
    group.decisions.push(decision);
    groups.set(decision.sheetName, group);
  }
  return Array.from(groups.values());
}

function getFillPreviewGroups(job: CocFillJob): CocFillPreviewGroup[] {
  return job.previewGroups?.length ? job.previewGroups : buildFallbackPreviewGroups(job);
}

function fillDecisionKey(jobId: string, decision: CocFillDecision): string {
  return `${jobId}:${decision.sheetName}:${decision.rowNumber}:${decision.materialGroup}`;
}

function fillDecisionKeyFromParts(jobId: string, sheetName: string, rowNumber: number, materialGroup: string): string {
  return `${jobId}:${sheetName}:${rowNumber}:${materialGroup}`;
}

function isManualFillDecision(decision: CocFillDecision): boolean {
  return decision.status === "filled" && (
    decision.reason === "人工选择 PDF 候选。" || decision.reason === "人工填写 WVTA/COC。"
  );
}

function manualFillPasteParts(value: string): ManualFillDraft {
  const wvtaMatch = value.match(/e\d+\*2018\/858\*[^\s,，;；]+/i);
  const cocMatch = value.match(/\d{5}-\d{2}&[^\s,，;；]*C[O0]C[^\s,，;；]*/i);
  if (wvtaMatch || cocMatch) {
    return {
      wvtaNo: wvtaMatch?.[0] ?? "",
      cocNo: cocMatch?.[0] ?? "",
    };
  }
  const [wvtaNo = "", cocNo = ""] = value.split(/[\t\n\r,，;；]+/).map((part) => part.trim()).filter(Boolean);
  return { wvtaNo, cocNo };
}

function pendingFillSelectionValues(selection: PendingFillSelection | undefined): { wvtaNo: string; cocNo: string } | null {
  if (!selection) return null;
  if (selection.kind === "candidate") {
    return { wvtaNo: selection.candidate.wvtaNo, cocNo: selection.candidate.cocNo };
  }
  return { wvtaNo: selection.wvtaNo, cocNo: selection.cocNo };
}

function candidateOptionKey(record: CocFillRecord, index: number): string {
  return `${record.wvtaNo}:${record.cocNo}:${record.pageNumber}:${record.tableRowNumber}:${index}`;
}

function candidateMainText(record: CocFillRecord): string {
  return `${record.wvtaNo || "-"} / ${record.cocNo || "-"}`;
}

function candidateMetaText(record: CocFillRecord): string {
  const parts = [`PDF ${record.pageNumber || "-"} 页`, `行 ${record.tableRowNumber || "-"}`];
  const validRange = [record.validFrom, record.validTo].filter(Boolean).join(" ~ ");
  if (validRange) parts.push(validRange);
  if (record.comments) parts.push(record.comments);
  return parts.join(" · ");
}

function Dropzone({
  accept,
  label,
  hint,
  file,
  onFile,
  onClear,
}: {
  accept: string;
  label: string;
  hint: string;
  file: File | null;
  onFile: (file: File) => void;
  onClear?: () => void;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const handleDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      setDragging(false);
      const nextFile = event.dataTransfer.files[0];
      if (nextFile) onFile(nextFile);
    },
    [onFile],
  );

  return (
    <div
      className={`dropzone ${file ? "has-file" : ""} ${dragging ? "dragover" : ""}`}
      onClick={() => inputRef.current?.click()}
      onDragOver={(event) => {
        event.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      style={{
        position: "relative",
        border: `2px dashed ${file ? "#16a34a" : dragging ? "#2563eb" : "#d1d5db"}`,
        borderRadius: 8,
        padding: file ? "22px 38px 18px 14px" : "18px 14px",
        minHeight: 110,
        display: "grid",
        alignContent: "center",
        gap: 6,
        cursor: "pointer",
        background: file ? "#f0fdf4" : dragging ? "#eff6ff" : "#fafafa",
      }}
    >
      {file && onClear ? (
        <button
          type="button"
          aria-label={`清除${label}`}
          title="清除文件"
          onClick={(event) => {
            event.stopPropagation();
            onClear();
          }}
          style={dropzoneClearButtonStyle}
        >
          ×
        </button>
      ) : null}
      <strong style={{ color: file ? "#15803d" : "#111827", fontSize: 14 }}>{label}</strong>
      <span style={{ color: "#64748b", fontSize: 12 }}>{file ? file.name : hint}</span>
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        hidden
        onChange={(event) => {
          const nextFile = event.target.files?.[0];
          if (nextFile) onFile(nextFile);
          event.target.value = "";
        }}
      />
    </div>
  );
}

function MetricCard({
  label,
  value,
  tone,
  active = false,
  onClick,
}: {
  label: string;
  value: string | number;
  tone?: "success" | "warning" | "danger" | "info";
  active?: boolean;
  onClick?: () => void;
}) {
  const color = tone === "success" ? "#16a34a" : tone === "warning" ? "#b45309" : tone === "danger" ? "#dc2626" : "#111827";
  const content = (
    <>
      <span style={{ color: "#64748b", fontSize: 11, fontWeight: 700 }}>{label}</span>
      <strong style={{ color, fontSize: 22, lineHeight: 1 }}>{value}</strong>
    </>
  );
  if (onClick) {
    return (
      <button
        type="button"
        style={{
          ...metricCardStyle,
          ...(active ? metricCardActiveStyle : null),
          cursor: "pointer",
          textAlign: "left",
          font: "inherit",
        }}
        onClick={onClick}
      >
        {content}
      </button>
    );
  }
  return (
    <div style={metricCardStyle}>
      {content}
    </div>
  );
}

export function CocMatchPage() {
  const [activeMode, setActiveMode] = useState<CocWorkspaceMode>("fill");
  const [controlOpen, setControlOpen] = useState(true);

  const [matchExcelFile, setMatchExcelFile] = useState<File | null>(null);
  const [archiveFile, setArchiveFile] = useState<File | null>(null);
  const [country, setCountry] = useState("");
  const [month, setMonth] = useState(() => {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  });
  const [fileExt, setFileExt] = useState<".pdf" | ".xml">(".pdf");
  const [matchUploading, setMatchUploading] = useState(false);
  const [matchError, setMatchError] = useState<string | null>(null);
  const [matchUploadDetail, setMatchUploadDetail] = useState<string | null>(null);
  const [currentMatchJob, setCurrentMatchJob] = useState<CocMatchJob | null>(null);
  const [matchJobList, setMatchJobList] = useState<CocMatchJob[]>([]);
  const [matchPollId, setMatchPollId] = useState<string | null>(null);
  const [reportAction, setReportAction] = useState<string | null>(null);
  const matchPollingRef = useRef(false);

  const [fillExcelFile, setFillExcelFile] = useState<File | null>(null);
  const [fillPdfFile, setFillPdfFile] = useState<File | null>(null);
  const [overwriteExisting, setOverwriteExisting] = useState(false);
  const [includeResultSheet, setIncludeResultSheet] = useState(false);
  const [conflictStrategy, setConflictStrategy] = useState<"date_country" | "strict">("strict");
  const [fillUploading, setFillUploading] = useState(false);
  const [fillError, setFillError] = useState<string | null>(null);
  const [fillUploadDetail, setFillUploadDetail] = useState<string | null>(null);
  const [currentFillJob, setCurrentFillJob] = useState<CocFillJob | null>(null);
  const [fillJobList, setFillJobList] = useState<CocFillJob[]>([]);
  const [fillPollId, setFillPollId] = useState<string | null>(null);
  const [fillDownloadAction, setFillDownloadAction] = useState<string | null>(null);
  const [confirmedFillJobIds, setConfirmedFillJobIds] = useState<Set<string>>(() => new Set());
  const [expandedFillPreviewSheets, setExpandedFillPreviewSheets] = useState<Set<string>>(() => new Set());
  const [touchedFillPreviewJobIds, setTouchedFillPreviewJobIds] = useState<Set<string>>(() => new Set());
  const [fillPreviewStatusFilter, setFillPreviewStatusFilter] = useState<FillPreviewStatusFilter>("all");
  const [openCandidatePickerKey, setOpenCandidatePickerKey] = useState<string | null>(null);
  const [fillOverrideAction, setFillOverrideAction] = useState<string | null>(null);
  const [pendingFillCandidateSelections, setPendingFillCandidateSelections] = useState<Record<string, PendingFillSelection>>({});
  const [manualFillDrafts, setManualFillDrafts] = useState<Record<string, ManualFillDraft>>({});
  const fillPollingRef = useRef(false);

  const jobsCountryFilter = country.trim().toUpperCase();

  const refreshMatchJobs = useCallback(() => {
    api.cocMatchListJobs(20, jobsCountryFilter || undefined)
      .then((res) => setMatchJobList(res.items))
      .catch(() => {});
  }, [jobsCountryFilter]);

  const refreshFillJobs = useCallback(() => {
    api.cocFillListJobs(50)
      .then((res) => setFillJobList(res.items))
      .catch(() => {});
  }, []);

  useEffect(() => {
    refreshMatchJobs();
  }, [refreshMatchJobs]);

  useEffect(() => {
    refreshFillJobs();
  }, [refreshFillJobs]);

  useEffect(() => {
    if (!matchPollId) return;
    matchPollingRef.current = true;
    let cancelled = false;

    const poll = async () => {
      while (matchPollingRef.current && !cancelled) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
        if (cancelled || !matchPollingRef.current) break;
        try {
          const res = await api.cocMatchGetJob(matchPollId);
          setCurrentMatchJob(res.item);
          if (res.item.status === "success" || res.item.status === "failed") {
            matchPollingRef.current = false;
            setMatchPollId(null);
            refreshMatchJobs();
          }
        } catch {
          matchPollingRef.current = false;
          setMatchPollId(null);
        }
      }
    };
    void poll();

    return () => {
      cancelled = true;
      matchPollingRef.current = false;
    };
  }, [matchPollId, refreshMatchJobs]);

  useEffect(() => {
    if (!fillPollId) return;
    fillPollingRef.current = true;
    let cancelled = false;

    const poll = async () => {
      while (fillPollingRef.current && !cancelled) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
        if (cancelled || !fillPollingRef.current) break;
        try {
          const res = await api.cocFillGetJob(fillPollId);
          setCurrentFillJob(res.item);
          if (res.item.status === "success" || res.item.status === "failed") {
            fillPollingRef.current = false;
            setFillPollId(null);
            refreshFillJobs();
          }
        } catch {
          fillPollingRef.current = false;
          setFillPollId(null);
        }
      }
    };
    void poll();

    return () => {
      cancelled = true;
      fillPollingRef.current = false;
    };
  }, [fillPollId, refreshFillJobs]);

  useEffect(() => {
    setPendingFillCandidateSelections({});
    setManualFillDrafts({});
    setOpenCandidatePickerKey(null);
  }, [currentFillJob?.jobId]);

  const handleMatchUpload = async () => {
    if (!matchExcelFile || !archiveFile) return;
    if (!country.trim()) {
      setMatchError("请输入国家代码");
      return;
    }

    setMatchUploading(true);
    setMatchError(null);
    setCurrentMatchJob(null);
    setMatchUploadDetail(
      matchExcelFile.size >= 50 * 1024 * 1024 || archiveFile.size >= 50 * 1024 * 1024
        ? "文件较大，使用分片上传。"
        : null,
    );

    try {
      const res = await api.cocMatchUploadAndCreateJob(
        matchExcelFile,
        archiveFile,
        country.toUpperCase(),
        fileExt,
        month || undefined,
      );
      setCurrentMatchJob(res.item);
      setMatchPollId(res.item.jobId);
      setMatchUploadDetail(null);
    } catch (err: unknown) {
      setMatchError(err instanceof Error ? err.message : "上传失败");
      setMatchUploadDetail(null);
    } finally {
      setMatchUploading(false);
    }
  };

  const handleFillUpload = async () => {
    if (!fillExcelFile || !fillPdfFile) return;
    setFillUploading(true);
    setFillError(null);
    setCurrentFillJob(null);
    setFillUploadDetail(
      fillExcelFile.size >= 50 * 1024 * 1024 || fillPdfFile.size >= 50 * 1024 * 1024
        ? "文件较大，使用分片上传。"
        : null,
    );

    try {
      const res = await api.cocFillUploadAndCreateJob(fillExcelFile, fillPdfFile, {
        overwriteExisting,
        conflictStrategy,
        includeResultSheet,
      });
      setCurrentFillJob(res.item);
      setFillPollId(res.item.jobId);
      setFillUploadDetail(null);
    } catch (err: unknown) {
      setFillError(err instanceof Error ? err.message : "填充任务创建失败");
      setFillUploadDetail(null);
    } finally {
      setFillUploading(false);
    }
  };

  const handlePreviewFillJob = (job: CocFillJob) => {
    setCurrentFillJob(job);
  };

  const handleConfirmFillPreview = (jobId: string) => {
    setConfirmedFillJobIds((previous) => {
      const next = new Set(previous);
      next.add(jobId);
      return next;
    });
  };

  const handleFillPreviewFilterClick = (filter: FillPreviewStatusFilter) => {
    setFillPreviewStatusFilter((current) => current === filter ? "all" : filter);
  };

  const toggleFillPreviewSheet = (jobId: string, sheetName: string, expanded: boolean) => {
    const key = `${jobId}:${sheetName}`;
    setTouchedFillPreviewJobIds((previous) => {
      const next = new Set(previous);
      next.add(jobId);
      return next;
    });
    setExpandedFillPreviewSheets((previous) => {
      const next = new Set(previous);
      if (expanded) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const pendingCandidateCountForJob = (jobId: string) =>
    Object.keys(pendingFillCandidateSelections).filter((key) => key.startsWith(`${jobId}:`)).length;

  const handleSelectFillCandidate = (
    job: CocFillJob,
    decision: CocFillDecision,
    candidate: CocFillRecord,
  ) => {
    const selectionKey = fillDecisionKey(job.jobId, decision);
    setPendingFillCandidateSelections((previous) => ({
      ...previous,
      [selectionKey]: { kind: "candidate", decision, candidate },
    }));
    setConfirmedFillJobIds((previous) => {
      const next = new Set(previous);
      next.delete(job.jobId);
      return next;
    });
    setOpenCandidatePickerKey(null);
  };

  const handleManualFillDraftChange = (
    job: CocFillJob,
    decision: CocFillDecision,
    field: keyof ManualFillDraft,
    value: string,
  ) => {
    const selectionKey = fillDecisionKey(job.jobId, decision);
    setManualFillDrafts((previous) => ({
      ...previous,
      [selectionKey]: {
        wvtaNo: previous[selectionKey]?.wvtaNo ?? "",
        cocNo: previous[selectionKey]?.cocNo ?? "",
        [field]: value,
      },
    }));
  };

  const handleManualFillPaste = (
    job: CocFillJob,
    decision: CocFillDecision,
    value: string,
  ) => {
    const parsed = manualFillPasteParts(value);
    if (!parsed.wvtaNo && !parsed.cocNo) return;
    const selectionKey = fillDecisionKey(job.jobId, decision);
    setManualFillDrafts((previous) => ({
      ...previous,
      [selectionKey]: {
        wvtaNo: parsed.wvtaNo || previous[selectionKey]?.wvtaNo || "",
        cocNo: parsed.cocNo || previous[selectionKey]?.cocNo || "",
      },
    }));
  };

  const handleStageManualFill = (job: CocFillJob, decision: CocFillDecision) => {
    const selectionKey = fillDecisionKey(job.jobId, decision);
    const draft = manualFillDrafts[selectionKey] || { wvtaNo: "", cocNo: "" };
    const wvtaNo = draft.wvtaNo.trim();
    const cocNo = draft.cocNo.trim();
    if (!wvtaNo || !cocNo) {
      setFillError("手工填写需要同时提供 WVTA 和 COC。");
      return;
    }
    setFillError(null);
    setPendingFillCandidateSelections((previous) => ({
      ...previous,
      [selectionKey]: { kind: "manual", decision, wvtaNo, cocNo },
    }));
    setConfirmedFillJobIds((previous) => {
      const next = new Set(previous);
      next.delete(job.jobId);
      return next;
    });
  };

  const clearPendingFillCandidateSelection = (job: CocFillJob, decision: CocFillDecision) => {
    const selectionKey = fillDecisionKey(job.jobId, decision);
    setPendingFillCandidateSelections((previous) => {
      const next = { ...previous };
      delete next[selectionKey];
      return next;
    });
  };

  const clearPendingFillCandidateSelections = (jobId: string) => {
    setPendingFillCandidateSelections((previous) => {
      const next = { ...previous };
      for (const key of Object.keys(next)) {
        if (key.startsWith(`${jobId}:`)) delete next[key];
      }
      return next;
    });
  };

  const handleConfirmFillCandidates = async (job: CocFillJob) => {
    const selections = Object.entries(pendingFillCandidateSelections)
      .filter(([key]) => key.startsWith(`${job.jobId}:`))
      .map(([, selection]) => selection);
    if (selections.length === 0) return;

    setFillOverrideAction(`${job.jobId}:batch`);
    setFillError(null);
    try {
      const res = await api.cocFillApplyOverrides(
        job.jobId,
        selections.map((selection) => {
          if (selection.kind === "candidate") {
            return {
              sheetName: selection.decision.sheetName,
              rowNumber: selection.decision.rowNumber,
              materialGroup: selection.decision.materialGroup,
              wvtaNo: selection.candidate.wvtaNo,
              cocNo: selection.candidate.cocNo,
              pageNumber: selection.candidate.pageNumber,
              tableRowNumber: selection.candidate.tableRowNumber,
            };
          }
          return {
            sheetName: selection.decision.sheetName,
            rowNumber: selection.decision.rowNumber,
            materialGroup: selection.decision.materialGroup,
            wvtaNo: selection.wvtaNo,
            cocNo: selection.cocNo,
          };
        }),
      );
      setCurrentFillJob(res.item);
      setFillJobList((previous) => {
        const next = previous.map((item) => item.jobId === res.item.jobId ? res.item : item);
        return next.some((item) => item.jobId === res.item.jobId) ? next : [res.item, ...next];
      });
      clearPendingFillCandidateSelections(job.jobId);
      setConfirmedFillJobIds((previous) => {
        const next = new Set(previous);
        next.delete(res.item.jobId);
        return next;
      });
    } catch (err: unknown) {
      setFillError(err instanceof Error ? err.message : "候选确认失败");
    } finally {
      setFillOverrideAction(null);
    }
  };

  const handleRevertFillCandidate = async (
    job: CocFillJob,
    decision: CocFillDecision,
  ) => {
    const actionKey = fillDecisionKey(job.jobId, decision);
    setFillOverrideAction(actionKey);
    setFillError(null);
    try {
      const res = await api.cocFillRevertOverrides(job.jobId, [
        {
          sheetName: decision.sheetName,
          rowNumber: decision.rowNumber,
          materialGroup: decision.materialGroup,
        },
      ]);
      setCurrentFillJob(res.item);
      setFillJobList((previous) => {
        const next = previous.map((item) => item.jobId === res.item.jobId ? res.item : item);
        return next.some((item) => item.jobId === res.item.jobId) ? next : [res.item, ...next];
      });
      setConfirmedFillJobIds((previous) => {
        const next = new Set(previous);
        next.delete(res.item.jobId);
        return next;
      });
      setOpenCandidatePickerKey(null);
    } catch (err: unknown) {
      setFillError(err instanceof Error ? err.message : "撤回人工选择失败");
    } finally {
      setFillOverrideAction(null);
    }
  };

  const handleRetryMatch = async (jobId: string) => {
    try {
      const res = await api.cocMatchRetryJob(jobId);
      setCurrentMatchJob(res.item);
      setMatchPollId(res.item.jobId);
    } catch (err: unknown) {
      setMatchError(err instanceof Error ? err.message : "重试失败");
    }
  };

  const handleOpenReport = async (jobId: string) => {
    const actionId = `${jobId}:view`;
    const reportWindow = window.open("", "_blank");
    if (!reportWindow) {
      setMatchError("浏览器阻止了新窗口，请允许弹窗后重试。");
      return;
    }
    setReportAction(actionId);
    setMatchError(null);
    try {
      const blob = await api.cocMatchGetReport(jobId);
      const url = URL.createObjectURL(blob);
      reportWindow.location.href = url;
      window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (err: unknown) {
      reportWindow.close();
      setMatchError(err instanceof Error ? err.message : "查看报告失败");
    } finally {
      setReportAction(null);
    }
  };

  const handleDownloadReport = async (jobId: string) => {
    const actionId = `${jobId}:download`;
    setReportAction(actionId);
    setMatchError(null);
    try {
      const blob = await api.cocMatchGetReport(jobId, true);
      downloadBlob(blob, `coc_report_${jobId}.html`);
    } catch (err: unknown) {
      setMatchError(err instanceof Error ? err.message : "下载报告失败");
    } finally {
      setReportAction(null);
    }
  };

  const handleDownloadFillWorkbook = async (job: CocFillJob) => {
    setFillDownloadAction(job.jobId);
    setFillError(null);
    try {
      const blob = await api.cocFillGetWorkbook(job.jobId);
      downloadBlob(blob, fillWorkbookDownloadName(job));
    } catch (err: unknown) {
      setFillError(err instanceof Error ? err.message : "下载填充结果失败");
    } finally {
      setFillDownloadAction(null);
    }
  };

  const matchReady = Boolean(matchExcelFile && archiveFile && country.trim());
  const fillReady = Boolean(fillExcelFile && fillPdfFile);
  const displayMatchJob = currentMatchJob ?? matchJobList[0] ?? null;
  const displayFillJob = currentFillJob ?? fillJobList[0] ?? null;

  return (
    <section className="crud-shell" style={{ padding: 24, maxWidth: 1180, margin: "0 auto" }}>
      <DeckFloatingDrawer
        open={controlOpen}
        onOpenChange={setControlOpen}
        triggerPrimary={activeMode === "match" ? "COC 比对" : "COC 填充"}
        triggerSecondaryOpen="收起控制"
        triggerSecondaryClosed="打开控制"
        eyebrow="COC Workbench"
        title="COC 工作台控制"
        ariaLabel="COC 工作台控制"
        footer={(
          <>
            <span className="market-scan-toolbar-chip">{activeMode === "match" ? "比对" : "填充"}</span>
            <span className="market-scan-toolbar-chip">
              {activeMode === "match" ? `${matchJobList.length} 条比对记录` : `${fillJobList.length} 条填充记录`}
            </span>
            {activeMode === "fill" ? (
              <span className="market-scan-toolbar-chip">{fillStrategyLabel(conflictStrategy)}</span>
            ) : null}
            {activeMode === "fill" ? (
              <span className="market-scan-toolbar-chip">自动扫描全部 Sheet</span>
            ) : null}
          </>
        )}
      >
        <DeckControlTabs
          tabs={WORKSPACE_TABS}
          activeKey={activeMode}
          onChange={setActiveMode}
          ariaLabel="COC 工作台功能切换"
        />
        {activeMode === "match" ? renderMatchControls() : renderFillControls()}
      </DeckFloatingDrawer>

      <header style={heroStyle}>
        <div>
          <span className="market-scan-panel-eyebrow">Product Deck</span>
          <h1 style={{ margin: "4px 0", fontSize: 28 }}>COC 工作台</h1>
          <p style={{ margin: 0, color: "#64748b", fontSize: 14 }}>
            管理 COC 文件比对，并从 WVTA 关联表回填发运清单的 WVTA / COC 编号。
          </p>
        </div>
        <button className="btn btn-secondary" type="button" onClick={() => setControlOpen(true)}>
          打开控制
        </button>
      </header>

      {activeMode === "match" && matchError ? (
        <div className="alert alert-error" style={{ marginBottom: 14 }}>{matchError}</div>
      ) : null}
      {activeMode === "fill" && fillError ? (
        <div className="alert alert-error" style={{ marginBottom: 14 }}>{fillError}</div>
      ) : null}

      <section style={dashboardGridStyle}>
        {activeMode === "match" ? (
          <div style={panelStyle}>
            <div style={panelHeaderStyle}>
              <strong>COC 比对状态</strong>
              {displayMatchJob ? <span style={{ color: statusColor(displayMatchJob.status) }}>{statusLabel(displayMatchJob.status)}</span> : null}
            </div>
            {displayMatchJob ? renderMatchSummary(displayMatchJob) : <EmptyState text="暂无比对任务" />}
          </div>
        ) : (
          <div style={panelStyle}>
            <div style={panelHeaderStyle}>
              <strong>COC 填充状态</strong>
              {displayFillJob ? <span style={{ color: statusColor(displayFillJob.status) }}>{statusLabel(displayFillJob.status)}</span> : null}
            </div>
            {displayFillJob ? renderFillSummary(displayFillJob) : <EmptyState text="暂无填充任务" />}
          </div>
        )}
      </section>

      <section style={historyGridStyle}>
        {activeMode === "match" ? renderMatchHistory() : renderFillHistory()}
      </section>
    </section>
  );

  function renderMatchControls() {
    return (
      <div style={controlPanelStyle}>
        <div style={dropGridStyle}>
          <Dropzone
            accept=".xlsx,.xlsm,.xls"
            label="Excel 注册表"
            hint="拖拽 / 点击选择 Excel"
            file={matchExcelFile}
            onFile={setMatchExcelFile}
            onClear={() => setMatchExcelFile(null)}
          />
          <Dropzone
            accept=".zip,.rar"
            label="ZIP/RAR 文件包"
            hint="拖拽 / 点击选择压缩包"
            file={archiveFile}
            onFile={setArchiveFile}
            onClear={() => setArchiveFile(null)}
          />
        </div>
        <label style={fieldStyle}>
          <span>国家代码</span>
          <input
            type="text"
            placeholder="CZ / SK / HU"
            value={country}
            maxLength={10}
            onChange={(event) => setCountry(event.target.value.toUpperCase())}
          />
        </label>
        <label style={fieldStyle}>
          <span>月份</span>
          <input type="month" value={month} onChange={(event) => setMonth(event.target.value)} />
        </label>
        <div style={segmentedStyle} role="group" aria-label="COC 文件类型">
          {([".pdf", ".xml"] as const).map((ext) => (
            <button
              key={ext}
              type="button"
              className={`btn btn-sm ${fileExt === ext ? "btn-primary" : "btn-secondary"}`}
              onClick={() => setFileExt(ext)}
            >
              {ext.toUpperCase().slice(1)}
            </button>
          ))}
        </div>
        <button className="btn btn-primary" type="button" disabled={!matchReady || matchUploading} onClick={() => void handleMatchUpload()}>
          {matchUploading ? "上传中..." : "开始比对"}
        </button>
        {matchUploadDetail ? <small style={hintStyle}>{matchUploadDetail}</small> : null}
      </div>
    );
  }

  function renderFillControls() {
    return (
      <div style={controlPanelStyle}>
        <div style={dropGridStyle}>
          <Dropzone
            accept=".xlsx,.xlsm"
            label="发运清单 Excel"
            hint="拖拽 / 点击选择发运清单"
            file={fillExcelFile}
            onFile={setFillExcelFile}
            onClear={() => setFillExcelFile(null)}
          />
          <Dropzone
            accept=".pdf"
            label="WVTA 关联 PDF"
            hint="拖拽 / 点击选择 PDF"
            file={fillPdfFile}
            onFile={setFillPdfFile}
            onClear={() => setFillPdfFile(null)}
          />
        </div>
        <label style={fieldStyle}>
          <span>冲突策略</span>
          <select value={conflictStrategy} onChange={(event) => setConflictStrategy(event.target.value as "date_country" | "strict")}>
            <option value="strict">严格唯一，否则标记冲突</option>
            <option value="date_country">按生产日期 / 国家收敛</option>
          </select>
        </label>
        <label style={checkboxStyle}>
          <input
            type="checkbox"
            checked={overwriteExisting}
            onChange={(event) => setOverwriteExisting(event.target.checked)}
          />
          <span>允许覆盖已有 WVTA / COC 值</span>
        </label>
        <label style={checkboxStyle}>
          <input
            type="checkbox"
            checked={includeResultSheet}
            onChange={(event) => setIncludeResultSheet(event.target.checked)}
          />
          <span>导出 COC填充结果 sheet</span>
        </label>
        <button className="btn btn-primary" type="button" disabled={!fillReady || fillUploading} onClick={() => void handleFillUpload()}>
          {fillUploading ? "上传中..." : "开始填充"}
        </button>
        {fillUploadDetail ? <small style={hintStyle}>{fillUploadDetail}</small> : null}
      </div>
    );
  }

  function renderMatchSummary(job: CocMatchJob) {
    const running = job.status !== "success" && job.status !== "failed";
    return (
      <div style={{ display: "grid", gap: 12 }}>
        <div style={metricsGridStyle}>
          <MetricCard label="总行数" value={job.totalRows ?? "-"} />
          <MetricCard label="匹配" value={job.matchedCount ?? "-"} tone="success" />
          <MetricCard label="缺失" value={job.missingCount ?? "-"} tone="danger" />
          <MetricCard label="覆盖率" value={job.coverageRate != null ? `${job.coverageRate}%` : "-"} tone="info" />
        </div>
        <div style={{ color: "#64748b", fontSize: 13 }}>
          {running ? "任务正在处理。" : `差异类型：${cocDifferenceLabel(job.differenceType)}`}
        </div>
        {job.status === "success" ? (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button className="btn btn-sm btn-primary" type="button" disabled={reportAction !== null} onClick={() => void handleOpenReport(job.jobId)}>
              {reportAction === `${job.jobId}:view` ? "打开中..." : "查看报告"}
            </button>
            <button className="btn btn-sm btn-secondary" type="button" disabled={reportAction !== null} onClick={() => void handleDownloadReport(job.jobId)}>
              {reportAction === `${job.jobId}:download` ? "下载中..." : "下载报告"}
            </button>
          </div>
        ) : null}
        {job.status === "failed" ? (
          <button className="btn btn-sm btn-danger" type="button" onClick={() => void handleRetryMatch(job.jobId)}>
            重试
          </button>
        ) : null}
      </div>
    );
  }

  function renderFillSummary(job: CocFillJob) {
    const running = job.status !== "success" && job.status !== "failed";
    const previewConfirmed = confirmedFillJobIds.has(job.jobId);
    const pendingCandidateCount = pendingCandidateCountForJob(job.jobId);
    return (
      <div style={{ display: "grid", gap: 12 }}>
        <div style={metricsGridStyle}>
          <MetricCard
            label="识别"
            value={job.totalRows ?? "-"}
            active={fillPreviewStatusFilter === "all"}
            onClick={() => handleFillPreviewFilterClick("all")}
          />
          <MetricCard
            label="填充"
            value={job.filledCount ?? "-"}
            tone="success"
            active={fillPreviewStatusFilter === "filled"}
            onClick={() => handleFillPreviewFilterClick("filled")}
          />
          <MetricCard
            label="未命中"
            value={job.notFoundCount ?? "-"}
            tone="danger"
            active={fillPreviewStatusFilter === "not_found"}
            onClick={() => handleFillPreviewFilterClick("not_found")}
          />
          <MetricCard
            label="冲突"
            value={job.ambiguousCount ?? "-"}
            tone="warning"
            active={fillPreviewStatusFilter === "ambiguous"}
            onClick={() => handleFillPreviewFilterClick("ambiguous")}
          />
          <MetricCard
            label="跳过"
            value={job.skippedExistingCount ?? "-"}
            active={fillPreviewStatusFilter === "skipped_existing"}
            onClick={() => handleFillPreviewFilterClick("skipped_existing")}
          />
        </div>
        <div style={{ color: "#64748b", fontSize: 13 }}>
          {running
            ? `阶段：${job.phase || "pending"}`
            : `唯一物料 ${job.uniqueMaterialCount ?? "-"} · 跳过已有 ${job.skippedExistingCount ?? "-"} · PDF 记录 ${job.pdfRecordCount ?? "-"}`}
        </div>
        {job.status === "success" ? (
          <>
            {renderFillPreview(job)}
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              {pendingCandidateCount > 0 ? (
                <>
                  <button
                    className="btn btn-sm btn-primary"
                    type="button"
                    disabled={fillOverrideAction !== null}
                    onClick={() => void handleConfirmFillCandidates(job)}
                  >
                    {fillOverrideAction === `${job.jobId}:batch` ? "确认中..." : `批量确认选择 (${pendingCandidateCount})`}
                  </button>
                  <button
                    className="btn btn-sm btn-secondary"
                    type="button"
                    disabled={fillOverrideAction !== null}
                    onClick={() => clearPendingFillCandidateSelections(job.jobId)}
                  >
                    清空暂选
                  </button>
                </>
              ) : null}
              {previewConfirmed ? (
                <button
                  className="btn btn-sm btn-primary"
                  type="button"
                  disabled={fillDownloadAction !== null}
                  onClick={() => void handleDownloadFillWorkbook(job)}
                >
                  {fillDownloadAction === job.jobId ? "下载中..." : "下载填充 Excel"}
                </button>
              ) : (
                <button
                  className="btn btn-sm btn-primary"
                  type="button"
                  disabled={pendingCandidateCount > 0}
                  onClick={() => handleConfirmFillPreview(job.jobId)}
                >
                  确认预览
                </button>
              )}
              <span style={hintStyle}>
                {pendingCandidateCount > 0 ? "先批量确认候选，再确认预览" : previewConfirmed ? "预览已确认" : "确认后可下载"}
              </span>
            </div>
          </>
        ) : null}
        {job.status === "failed" && job.error ? <small style={{ color: "#dc2626" }}>{job.error}</small> : null}
      </div>
    );
  }

  function renderFillPreview(job: CocFillJob) {
    const groups = getFillPreviewGroups(job);
    const filteredGroups = groups
      .map((group) => ({
        ...group,
        decisions: fillPreviewStatusFilter === "all"
          ? group.decisions
          : group.decisions.filter((decision) => decision.status === fillPreviewStatusFilter),
      }))
      .filter((group) => group.decisions.length > 0);
    const previewTouched = touchedFillPreviewJobIds.has(job.jobId);
    return (
      <div style={previewPanelStyle}>
        <div style={previewHeaderStyle}>
          <strong>填充预览</strong>
          <div style={previewFilterToolbarStyle}>
            <span>{fillPreviewFilterLabel(fillPreviewStatusFilter)} · {filteredGroups.length} / {groups.length} 个 Sheet</span>
            {(["all", "filled", "not_found", "ambiguous", "skipped_existing"] as FillPreviewStatusFilter[]).map((filter) => (
              <button
                key={filter}
                type="button"
                style={{
                  ...previewFilterButtonStyle,
                  ...(fillPreviewStatusFilter === filter ? previewFilterButtonActiveStyle : null),
                }}
                onClick={() => handleFillPreviewFilterClick(filter)}
              >
                {fillPreviewFilterButtonLabel(filter)}
              </button>
            ))}
          </div>
        </div>
        {filteredGroups.length === 0 ? <EmptyState text="当前筛选没有预览记录" /> : (
          <div style={previewGroupsStyle}>
            {filteredGroups.map((group, groupIndex) => {
              const key = `${job.jobId}:${group.sheetName}`;
              const expanded = expandedFillPreviewSheets.has(key) || (!previewTouched && groupIndex === 0);
              return (
                <div key={group.sheetName} style={previewGroupStyle}>
                  <button
                    type="button"
                    style={previewGroupHeaderButtonStyle}
                    onClick={() => toggleFillPreviewSheet(job.jobId, group.sheetName, expanded)}
                  >
                    <span aria-hidden="true" style={{ ...previewDisclosureStyle, transform: expanded ? "rotate(90deg)" : "rotate(0deg)" }} />
                    <strong>{group.sheetName}</strong>
                    <span style={previewGroupMetricsStyle}>
                      <span style={previewMetricChipStyle}>识别 {group.totalRows}</span>
                      <span style={previewMetricChipStyle}>填充 {group.filledCount}</span>
                      <span style={previewMetricChipStyle}>未命中 {group.notFoundCount}</span>
                      <span style={previewMetricChipStyle}>冲突 {group.ambiguousCount}</span>
                      <span style={previewMetricChipStyle}>跳过 {group.skippedExistingCount}</span>
                    </span>
                  </button>
                  {expanded ? (
                    <div style={previewTableWrapStyle}>
                      <table style={tableStyle}>
                        <thead>
                          <tr>
                            <th style={thStyle}>状态</th>
                            <th style={thStyle}>行</th>
                            <th style={thStyle}>物料号组</th>
                            <th style={thStyle}>WVTA</th>
                            <th style={thStyle}>COC</th>
                            <th style={thStyle}>候选</th>
                            <th style={thStyle}>原因</th>
                          </tr>
                        </thead>
                        <tbody>
                          {group.decisions.map((decision, index) => {
                            const pickerKey = fillDecisionKey(job.jobId, decision);
                            const candidates = decision.candidateRecords || [];
                            const pickerOpen = openCandidatePickerKey === pickerKey;
                            const pickerBusy = fillOverrideAction === pickerKey;
                            const pendingSelection = pendingFillCandidateSelections[pickerKey];
                            const pendingValues = pendingFillSelectionValues(pendingSelection);
                            const displayedWvta = pendingValues?.wvtaNo || decision.writtenWvta || "-";
                            const displayedCoc = pendingValues?.cocNo || decision.writtenCoc || "-";
                            const manualDraft = manualFillDrafts[pickerKey] || { wvtaNo: "", cocNo: "" };
                            return (
                              <tr key={`${group.sheetName}-${decision.rowNumber}-${decision.materialGroup}-${index}`}>
                                <td style={{ ...tdStyle, color: pendingSelection ? "#2563eb" : fillDecisionStatusColor(decision.status), fontWeight: 700 }}>
                                  {pendingSelection ? "待确认" : fillDecisionStatusLabel(decision.status)}
                                </td>
                                <td style={tdStyle}>{decision.rowNumber}</td>
                                <td style={tdStyle}>{decision.materialGroup}</td>
                                <td style={tdStyle}>{displayedWvta}</td>
                                <td style={tdStyle}>{displayedCoc}</td>
                                <td style={{ ...tdStyle, position: "relative" }}>
                                  {decision.status === "ambiguous" && candidates.length > 0 ? (
                                    <div style={candidatePickerWrapStyle}>
                                      <span style={candidateActionRowStyle}>
                                        <button
                                          className="btn btn-sm btn-secondary"
                                          type="button"
                                          disabled={fillOverrideAction !== null}
                                          onClick={() => setOpenCandidatePickerKey(pickerOpen ? null : pickerKey)}
                                        >
                                          {pendingSelection ? "更换候选" : `暂选候选 (${candidates.length})`}
                                        </button>
                                        {pendingSelection ? (
                                          <button
                                            className="btn btn-sm btn-secondary"
                                            type="button"
                                            disabled={fillOverrideAction !== null}
                                            onClick={() => clearPendingFillCandidateSelection(job, decision)}
                                          >
                                            取消暂选
                                          </button>
                                        ) : null}
                                      </span>
                                      {pickerOpen ? (
                                        <div style={candidateMenuStyle}>
                                          {candidates.map((candidate, candidateIndex) => (
                                            <button
                                              key={candidateOptionKey(candidate, candidateIndex)}
                                              type="button"
                                              style={candidateOptionStyle}
                                              disabled={fillOverrideAction !== null}
                                              onClick={() => handleSelectFillCandidate(job, decision, candidate)}
                                            >
                                              <strong style={candidateOptionMainStyle}>{candidateMainText(candidate)}</strong>
                                              <span style={candidateOptionMetaStyle}>{candidateMetaText(candidate)}</span>
                                            </button>
                                          ))}
                                        </div>
                                      ) : null}
                                    </div>
                                  ) : decision.status === "not_found" ? (
                                    <div style={manualFillWrapStyle}>
                                      <input
                                        type="text"
                                        placeholder="粘贴 / 输入 WVTA"
                                        value={manualDraft.wvtaNo}
                                        onChange={(event) => handleManualFillDraftChange(job, decision, "wvtaNo", event.target.value)}
                                        onPaste={(event) => {
                                          const text = event.clipboardData.getData("text");
                                          const parsed = manualFillPasteParts(text);
                                          if (parsed.wvtaNo || parsed.cocNo) {
                                            event.preventDefault();
                                            handleManualFillPaste(job, decision, text);
                                          }
                                        }}
                                        style={manualFillInputStyle}
                                      />
                                      <input
                                        type="text"
                                        placeholder="输入 COC"
                                        value={manualDraft.cocNo}
                                        onChange={(event) => handleManualFillDraftChange(job, decision, "cocNo", event.target.value)}
                                        style={manualFillInputStyle}
                                      />
                                      <span style={candidateActionRowStyle}>
                                        <button
                                          className="btn btn-sm btn-secondary"
                                          type="button"
                                          disabled={fillOverrideAction !== null}
                                          onClick={() => handleStageManualFill(job, decision)}
                                        >
                                          暂存手填
                                        </button>
                                        {pendingSelection ? (
                                          <button
                                            className="btn btn-sm btn-secondary"
                                            type="button"
                                            disabled={fillOverrideAction !== null}
                                            onClick={() => clearPendingFillCandidateSelection(job, decision)}
                                          >
                                            取消暂选
                                          </button>
                                        ) : null}
                                      </span>
                                    </div>
                                  ) : isManualFillDecision(decision) ? (
                                    <button
                                      className="btn btn-sm btn-secondary"
                                      type="button"
                                      disabled={fillOverrideAction !== null}
                                      onClick={() => void handleRevertFillCandidate(job, decision)}
                                    >
                                      {pickerBusy ? "撤回中..." : "撤回人工选择"}
                                    </button>
                                  ) : "-"}
                                </td>
                                <td style={{ ...tdStyle, whiteSpace: "normal", minWidth: 180 }}>
                                  {pendingSelection ? "已暂选，尚未批量确认。" : decision.reason}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                      {group.truncated ? (
                        <div style={previewTruncatedStyle}>
                          {job.includeResultSheet
                            ? `该 Sheet 仅展示前 ${group.previewLimit ?? group.decisions.length} 行预览，完整清单在下载文件的 COC填充结果 sheet 中。`
                            : `该 Sheet 仅展示前 ${group.previewLimit ?? group.decisions.length} 行预览。`}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  }

  function renderMatchHistory() {
    return (
      <div style={panelStyle}>
        <div style={panelHeaderStyle}>
          <strong>COC 比对记录</strong>
          <button className="btn btn-sm btn-secondary" type="button" onClick={refreshMatchJobs}>刷新</button>
        </div>
        {matchJobList.length === 0 ? <EmptyState text="暂无比对记录" /> : (
          <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
              <thead>
                <tr>
                  <th style={thStyle}>国家</th>
                  <th style={thStyle}>月份</th>
                  <th style={thStyle}>状态</th>
                  <th style={thStyle}>匹配</th>
                  <th style={thStyle}>缺失</th>
                  <th style={thStyle}>覆盖率</th>
                  <th style={thStyle}>操作</th>
                </tr>
              </thead>
              <tbody>
                {matchJobList.map((job) => (
                  <tr key={job.jobId}>
                    <td style={tdStyle}>{job.country}</td>
                    <td style={tdStyle}>{job.month}</td>
                    <td style={{ ...tdStyle, color: statusColor(job.status), fontWeight: 700 }}>{statusLabel(job.status)}</td>
                    <td style={tdStyle}>{job.matchedCount ?? "-"}</td>
                    <td style={tdStyle}>{job.missingCount ?? "-"}</td>
                    <td style={tdStyle}>{job.coverageRate != null ? `${job.coverageRate}%` : "-"}</td>
                    <td style={tdStyle}>{renderMatchActions(job)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    );
  }

  function renderFillHistory() {
    return (
      <div style={panelStyle}>
        <div style={panelHeaderStyle}>
          <strong>COC 填充记录</strong>
          <button className="btn btn-sm btn-secondary" type="button" onClick={refreshFillJobs}>刷新</button>
        </div>
        {fillJobList.length === 0 ? <EmptyState text="暂无填充记录" /> : (
          <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
              <thead>
                <tr>
                  <th style={thStyle}>状态</th>
                  <th style={thStyle}>识别行</th>
                  <th style={thStyle}>填充</th>
                  <th style={thStyle}>未命中</th>
                  <th style={thStyle}>冲突</th>
                  <th style={thStyle}>策略</th>
                  <th style={thStyle}>创建时间</th>
                  <th style={thStyle}>操作</th>
                </tr>
              </thead>
              <tbody>
                {fillJobList.map((job) => (
                  <tr key={job.jobId}>
                    <td style={{ ...tdStyle, color: statusColor(job.status), fontWeight: 700 }}>{statusLabel(job.status)}</td>
                    <td style={tdStyle}>{job.totalRows ?? "-"}</td>
                    <td style={tdStyle}>{job.filledCount ?? "-"}</td>
                    <td style={tdStyle}>{job.notFoundCount ?? "-"}</td>
                    <td style={tdStyle}>{job.ambiguousCount ?? "-"}</td>
                    <td style={tdStyle}>{fillStrategyLabel(job.conflictStrategy)}</td>
                    <td style={tdStyle}>{formatTs(job.createdAt)}</td>
                    <td style={tdStyle}>{renderFillActions(job)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    );
  }

  function renderMatchActions(job: CocMatchJob) {
    if (job.status === "success") {
      return (
        <span style={{ display: "inline-flex", gap: 6 }}>
          <button className="btn btn-sm btn-secondary" type="button" disabled={reportAction !== null} onClick={() => void handleOpenReport(job.jobId)}>查看</button>
          <button className="btn btn-sm btn-secondary" type="button" disabled={reportAction !== null} onClick={() => void handleDownloadReport(job.jobId)}>下载</button>
        </span>
      );
    }
    if (job.status === "failed") {
      return <button className="btn btn-sm btn-secondary" type="button" onClick={() => void handleRetryMatch(job.jobId)}>重试</button>;
    }
    return "-";
  }

  function renderFillActions(job: CocFillJob) {
    if (job.status !== "success") return "-";
    if (!confirmedFillJobIds.has(job.jobId)) {
      return (
        <button
          className="btn btn-sm btn-secondary"
          type="button"
          onClick={() => handlePreviewFillJob(job)}
        >
          预览
        </button>
      );
    }
    return (
      <button
        className="btn btn-sm btn-secondary"
        type="button"
        disabled={fillDownloadAction !== null}
        onClick={() => void handleDownloadFillWorkbook(job)}
      >
        下载
      </button>
    );
  }
}

function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function fillWorkbookDownloadName(job: CocFillJob): string {
  const filename = job.excelFilename.trim();
  return filename || `coc_fill_${job.jobId}.xlsx`;
}

function EmptyState({ text }: { text: string }) {
  return <div style={{ padding: 24, textAlign: "center", color: "#94a3b8", fontSize: 13 }}>{text}</div>;
}

const heroStyle: CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "flex-start",
  gap: 16,
  marginBottom: 18,
};

const dashboardGridStyle: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
  gap: 16,
  marginBottom: 16,
};

const historyGridStyle: CSSProperties = {
  display: "grid",
  gap: 16,
};

const panelStyle: CSSProperties = {
  background: "white",
  border: "1px solid #e2e8f0",
  borderRadius: 8,
  overflow: "hidden",
  boxShadow: "0 1px 3px rgba(15, 23, 42, 0.08)",
};

const panelHeaderStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  gap: 10,
  padding: "14px 16px",
  borderBottom: "1px solid #e2e8f0",
};

const metricsGridStyle: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(110px, 1fr))",
  gap: 8,
  padding: 12,
};

const metricCardStyle: CSSProperties = {
  display: "grid",
  gap: 5,
  minHeight: 64,
  padding: 10,
  border: "1px solid #e2e8f0",
  borderRadius: 6,
  background: "#f8fafc",
};

const metricCardActiveStyle: CSSProperties = {
  borderColor: "#2563eb",
  background: "#eff6ff",
  boxShadow: "inset 0 0 0 1px #2563eb",
};

const previewPanelStyle: CSSProperties = {
  border: "1px solid #e2e8f0",
  borderRadius: 8,
  overflow: "hidden",
  background: "#ffffff",
};

const previewHeaderStyle: CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  gap: 10,
  padding: "10px 12px",
  borderBottom: "1px solid #e2e8f0",
  color: "#334155",
  fontSize: 13,
};

const previewFilterToolbarStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "flex-end",
  gap: 6,
  flexWrap: "wrap",
};

const previewFilterButtonStyle: CSSProperties = {
  border: "1px solid #cbd5e1",
  borderRadius: 6,
  background: "#ffffff",
  color: "#475569",
  padding: "4px 8px",
  fontSize: 12,
  fontWeight: 700,
  cursor: "pointer",
};

const previewFilterButtonActiveStyle: CSSProperties = {
  borderColor: "#2563eb",
  background: "#eff6ff",
  color: "#1d4ed8",
};

const previewTableWrapStyle: CSSProperties = {
  maxHeight: 520,
  overflow: "auto",
};

const previewGroupsStyle: CSSProperties = {
  display: "grid",
};

const previewGroupStyle: CSSProperties = {
  borderTop: "1px solid #e2e8f0",
};

const previewGroupHeaderButtonStyle: CSSProperties = {
  width: "100%",
  display: "grid",
  gridTemplateColumns: "18px minmax(120px, 1fr) auto",
  alignItems: "center",
  gap: 10,
  padding: "10px 12px",
  border: 0,
  background: "#f8fafc",
  color: "#334155",
  cursor: "pointer",
  textAlign: "left",
};

const previewDisclosureStyle: CSSProperties = {
  display: "inline-block",
  width: 0,
  height: 0,
  borderTop: "5px solid transparent",
  borderBottom: "5px solid transparent",
  borderLeft: "8px solid #2563eb",
  transition: "transform 120ms ease",
  transformOrigin: "45% 50%",
};

const previewGroupMetricsStyle: CSSProperties = {
  display: "flex",
  gap: 6,
  flexWrap: "wrap",
  justifyContent: "flex-end",
};

const previewMetricChipStyle: CSSProperties = {
  border: "1px solid #e2e8f0",
  borderRadius: 999,
  padding: "2px 8px",
  background: "#ffffff",
  color: "#475569",
  fontSize: 11,
  fontWeight: 700,
  whiteSpace: "nowrap",
};

const previewTruncatedStyle: CSSProperties = {
  padding: "8px 12px",
  color: "#64748b",
  fontSize: 12,
  borderTop: "1px solid #e2e8f0",
};

const candidatePickerWrapStyle: CSSProperties = {
  position: "relative",
  display: "inline-block",
};

const candidateActionRowStyle: CSSProperties = {
  display: "inline-flex",
  gap: 6,
  alignItems: "center",
  flexWrap: "wrap",
};

const candidateMenuStyle: CSSProperties = {
  position: "absolute",
  top: "calc(100% + 6px)",
  left: 0,
  zIndex: 20,
  width: 520,
  maxWidth: "70vw",
  maxHeight: 420,
  overflowY: "auto",
  border: "1px solid #bfdbfe",
  borderRadius: 8,
  background: "#ffffff",
  boxShadow: "0 18px 36px rgba(15, 23, 42, 0.18)",
  padding: 6,
};

const candidateOptionStyle: CSSProperties = {
  width: "100%",
  display: "grid",
  gap: 3,
  padding: "7px 8px",
  border: 0,
  borderRadius: 6,
  background: "#ffffff",
  color: "#111827",
  cursor: "pointer",
  textAlign: "left",
  font: "inherit",
};

const candidateOptionMainStyle: CSSProperties = {
  fontSize: 12,
  lineHeight: 1.25,
  color: "#111827",
  whiteSpace: "normal",
};

const candidateOptionMetaStyle: CSSProperties = {
  color: "#64748b",
  fontSize: 11,
  lineHeight: 1.25,
  whiteSpace: "normal",
};

const manualFillWrapStyle: CSSProperties = {
  display: "grid",
  gap: 6,
  width: 300,
  maxWidth: "38vw",
};

const manualFillInputStyle: CSSProperties = {
  width: "100%",
  minWidth: 0,
  border: "1px solid #cbd5e1",
  borderRadius: 6,
  padding: "5px 7px",
  fontSize: 12,
};

const controlPanelStyle: CSSProperties = {
  display: "grid",
  gap: 12,
};

const dropGridStyle: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
  gap: 10,
};

const dropzoneClearButtonStyle: CSSProperties = {
  position: "absolute",
  top: 8,
  right: 8,
  width: 24,
  height: 24,
  border: "1px solid #86efac",
  borderRadius: 999,
  background: "#ffffff",
  color: "#15803d",
  cursor: "pointer",
  fontSize: 18,
  lineHeight: "20px",
  fontWeight: 700,
};

const fieldStyle: CSSProperties = {
  display: "grid",
  gap: 5,
  color: "#475569",
  fontSize: 12,
  fontWeight: 700,
};

const checkboxStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  color: "#475569",
  fontSize: 12,
  fontWeight: 700,
};

const segmentedStyle: CSSProperties = {
  display: "flex",
  gap: 8,
  flexWrap: "wrap",
};

const hintStyle: CSSProperties = {
  color: "#64748b",
  fontSize: 12,
};

const tableStyle: CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
};

const thStyle: CSSProperties = {
  textAlign: "left",
  padding: "10px 12px",
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  background: "#f8fafc",
  textTransform: "uppercase",
  whiteSpace: "nowrap",
};

const tdStyle: CSSProperties = {
  padding: "10px 12px",
  borderTop: "1px solid #f1f5f9",
  fontSize: 13,
  whiteSpace: "nowrap",
};
