import { useEffect, useLayoutEffect, useState } from "react";
import type { ReactElement } from "react";
import { isCandidatePreviewOrigin } from "../utils/candidateRuntime";

const CANDIDATE_METADATA_URL = "/candidate-preview.json";
const LATEST_MAIN_URL = "https://api.github.com/repos/tristan419/JATO_Analysis_System/commits/main";
const LATEST_MAIN_TIMEOUT_MS = 5_000;
const FULL_SHA_PATTERN = /^[0-9a-f]{40}$/;
const ARCHIVE_SHA256_PATTERN = /^[0-9a-f]{64}$/;
const UTC_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/;
const SANDBOX_DATABASE_PATTERN = /^jato_candidate_[a-z0-9](?:[a-z0-9_]{0,47})$/;

export interface CandidatePreviewMetadata {
  role: "candidate";
  commitSha: string;
  archiveSha256: string;
  databaseName: string;
  databaseSnapshotAt: string;
  candidateSlot: 8001;
  previewPort: number;
}

type CandidateBannerState =
  | { status: "loading" }
  | {
    status: "verified";
    metadata: CandidatePreviewMetadata;
    freshness:
      | { status: "checking" }
      | { status: "current"; mainSha: string }
      | { status: "stale"; mainSha: string }
      | { status: "unknown" };
  }
  | { status: "unverified" }
  | { status: "inactive" };

export function parseCandidatePreviewMetadata(value: unknown): CandidatePreviewMetadata | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  if (
    record.role !== "candidate"
    || typeof record.commitSha !== "string"
    || !FULL_SHA_PATTERN.test(record.commitSha)
    || typeof record.archiveSha256 !== "string"
    || !ARCHIVE_SHA256_PATTERN.test(record.archiveSha256)
    || typeof record.databaseName !== "string"
    || !SANDBOX_DATABASE_PATTERN.test(record.databaseName)
    || typeof record.databaseSnapshotAt !== "string"
    || !UTC_TIMESTAMP_PATTERN.test(record.databaseSnapshotAt)
    || Number.isNaN(Date.parse(record.databaseSnapshotAt))
    || record.candidateSlot !== 8001
    || record.previewPort !== 18002
  ) {
    return null;
  }
  return {
    role: "candidate",
    commitSha: record.commitSha,
    archiveSha256: record.archiveSha256,
    databaseName: record.databaseName,
    databaseSnapshotAt: record.databaseSnapshotAt,
    candidateSlot: record.candidateSlot,
    previewPort: record.previewPort,
  };
}

async function loadCandidatePreviewMetadata(signal: AbortSignal): Promise<CandidatePreviewMetadata | null> {
  try {
    const response = await fetch(CANDIDATE_METADATA_URL, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal,
    });
    if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) {
      return null;
    }
    return parseCandidatePreviewMetadata(await response.json());
  } catch {
    return null;
  }
}

async function loadLatestMainSha(parentSignal: AbortSignal): Promise<string | null> {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (parentSignal.aborted) {
    abort();
  } else {
    parentSignal.addEventListener("abort", abort, { once: true });
  }
  const timeout = window.setTimeout(abort, LATEST_MAIN_TIMEOUT_MS);

  try {
    const response = await fetch(LATEST_MAIN_URL, {
      cache: "no-store",
      credentials: "omit",
      headers: { Accept: "application/vnd.github+json" },
      signal: controller.signal,
    });
    if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) {
      return null;
    }
    const payload = await response.json() as { sha?: unknown };
    return typeof payload.sha === "string" && FULL_SHA_PATTERN.test(payload.sha)
      ? payload.sha
      : null;
  } catch {
    return null;
  } finally {
    window.clearTimeout(timeout);
    parentSignal.removeEventListener("abort", abort);
  }
}

export function CandidateEnvironmentBanner(): ReactElement | null {
  const candidateOrigin = isCandidatePreviewOrigin(globalThis.location);
  const [state, setState] = useState<CandidateBannerState>({ status: "loading" });

  useEffect(() => {
    const controller = new AbortController();
    void loadCandidatePreviewMetadata(controller.signal).then((nextMetadata) => {
      if (controller.signal.aborted) return;
      if (nextMetadata) {
        setState({ status: "verified", metadata: nextMetadata, freshness: { status: "checking" } });
      } else {
        setState({ status: candidateOrigin ? "unverified" : "inactive" });
      }
    });
    return () => controller.abort();
  }, [candidateOrigin]);

  const verifiedMetadata = state.status === "verified" ? state.metadata : null;
  useEffect(() => {
    if (!verifiedMetadata) return undefined;
    let refreshController: AbortController | null = null;

    const refreshLatestMain = async (): Promise<void> => {
      refreshController?.abort();
      const controller = new AbortController();
      refreshController = controller;
      setState({ status: "verified", metadata: verifiedMetadata, freshness: { status: "checking" } });
      const mainSha = await loadLatestMainSha(controller.signal);
      if (controller.signal.aborted) return;
      if (!mainSha) {
        setState({ status: "verified", metadata: verifiedMetadata, freshness: { status: "unknown" } });
      } else if (mainSha === verifiedMetadata.commitSha) {
        setState({ status: "verified", metadata: verifiedMetadata, freshness: { status: "current", mainSha } });
      } else {
        setState({ status: "verified", metadata: verifiedMetadata, freshness: { status: "stale", mainSha } });
      }
    };
    const refreshWhenVisible = (): void => {
      if (document.visibilityState === "visible") void refreshLatestMain();
    };

    void refreshLatestMain();
    window.addEventListener("focus", refreshLatestMain);
    document.addEventListener("visibilitychange", refreshWhenVisible);
    return () => {
      refreshController?.abort();
      window.removeEventListener("focus", refreshLatestMain);
      document.removeEventListener("visibilitychange", refreshWhenVisible);
    };
  }, [verifiedMetadata]);

  const guardCandidateIdentity = candidateOrigin || state.status === "verified";
  useLayoutEffect(() => {
    if (!guardCandidateIdentity) return undefined;
    document.documentElement.dataset.releaseRole = "candidate";
    return () => {
      delete document.documentElement.dataset.releaseRole;
    };
  }, [guardCandidateIdentity]);

  if (candidateOrigin && state.status !== "verified") {
    const isLoading = state.status === "loading";
    return (
      <aside
        className="candidate-environment-banner candidate-environment-banner--unverified"
        role="alert"
        aria-live="assertive"
      >
        <strong>
          {isLoading
            ? "Candidate 身份验证中，禁止据此验收"
            : "Candidate 身份不可验证，禁止据此验收"}
        </strong>
      </aside>
    );
  }

  if (state.status !== "verified") return null;
  const { freshness, metadata } = state;
  const blocksAcceptance = freshness.status !== "current";
  const headline = freshness.status === "current"
    ? "Candidate · 当前 main · 可测试"
    : freshness.status === "stale"
      ? "Candidate 落后 main · 禁止据此验收"
      : freshness.status === "unknown"
        ? "Candidate 身份已验证，但无法确认最新 main"
        : "Candidate 身份已验证 · 正在确认最新 main";

  return (
    <aside
      className={`candidate-environment-banner${blocksAcceptance ? " candidate-environment-banner--unverified" : ""}`}
      role={blocksAcceptance ? "alert" : "status"}
      aria-live={blocksAcceptance ? "assertive" : "polite"}
    >
      <strong>{headline}</strong>
      <span>不是正式 www</span>
      <span>
        Candidate commit <code title={metadata.commitSha}>{metadata.commitSha.slice(0, 12)}</code>
      </span>
      {freshness.status === "stale" && (
        <span>
          main commit <code title={freshness.mainSha}>{freshness.mainSha.slice(0, 12)}</code>
        </span>
      )}
      <span>
        artifact <code title={metadata.archiveSha256}>{metadata.archiveSha256.slice(0, 12)}</code>
      </span>
      <span>Active DB 快照开始于 {new Date(metadata.databaseSnapshotAt).toLocaleString()}</span>
      <span>
        数据库沙箱 <code title={metadata.databaseName}>{metadata.databaseName.slice(-8)}</code>
      </span>
      <span>物理诊断 slot {metadata.candidateSlot}（角色固定，不互换）</span>
    </aside>
  );
}
