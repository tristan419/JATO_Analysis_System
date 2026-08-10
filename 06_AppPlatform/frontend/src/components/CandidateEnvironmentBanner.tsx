import { useEffect, useLayoutEffect, useState } from "react";
import type { ReactElement } from "react";

const CANDIDATE_METADATA_URL = "/candidate-preview.json";
const FULL_SHA_PATTERN = /^[0-9a-f]{40}$/;
const ARCHIVE_SHA256_PATTERN = /^[0-9a-f]{64}$/;

export interface CandidatePreviewMetadata {
  role: "candidate";
  commitSha: string;
  archiveSha256: string;
  candidateSlot: 8001;
  previewPort: number;
}

interface CandidateLocation {
  hostname: string;
  port: string;
}

type CandidateBannerState =
  | { status: "loading" }
  | { status: "verified"; metadata: CandidatePreviewMetadata }
  | { status: "unverified" }
  | { status: "inactive" };

export function isCandidatePreviewOrigin(location: CandidateLocation): boolean {
  const hostname = location.hostname.toLowerCase().replace(/\.$/, "");
  if (hostname === "candidate.ojeur.cloud") return location.port === "";
  return (hostname === "127.0.0.1" || hostname === "localhost") && location.port === "18002";
}

export function parseCandidatePreviewMetadata(value: unknown): CandidatePreviewMetadata | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  if (
    record.role !== "candidate"
    || typeof record.commitSha !== "string"
    || !FULL_SHA_PATTERN.test(record.commitSha)
    || typeof record.archiveSha256 !== "string"
    || !ARCHIVE_SHA256_PATTERN.test(record.archiveSha256)
    || record.candidateSlot !== 8001
    || record.previewPort !== 18002
  ) {
    return null;
  }
  return {
    role: "candidate",
    commitSha: record.commitSha,
    archiveSha256: record.archiveSha256,
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

export function CandidateEnvironmentBanner(): ReactElement | null {
  const candidateOrigin = isCandidatePreviewOrigin(globalThis.location);
  const [state, setState] = useState<CandidateBannerState>({ status: "loading" });

  useEffect(() => {
    const controller = new AbortController();
    void loadCandidatePreviewMetadata(controller.signal).then((nextMetadata) => {
      if (controller.signal.aborted) return;
      if (nextMetadata) {
        setState({ status: "verified", metadata: nextMetadata });
      } else {
        setState({ status: candidateOrigin ? "unverified" : "inactive" });
      }
    });
    return () => controller.abort();
  }, [candidateOrigin]);

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
  const { metadata } = state;

  return (
    <aside className="candidate-environment-banner" role="status" aria-live="polite">
      <strong>Candidate 测试实例 · 待人工验收</strong>
      <span>不是正式 www</span>
      <span>
        commit <code title={metadata.commitSha}>{metadata.commitSha.slice(0, 12)}</code>
      </span>
      <span>
        artifact <code title={metadata.archiveSha256}>{metadata.archiveSha256.slice(0, 12)}</code>
      </span>
      <span>物理诊断 slot {metadata.candidateSlot}（角色固定，不互换）</span>
    </aside>
  );
}
