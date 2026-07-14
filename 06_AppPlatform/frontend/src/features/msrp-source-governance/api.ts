import { apiUrl, request } from "../../api/core";
import type {
  EvidenceAsset,
  HermesDiagnosisRequest,
  MonitoringTarget,
  MonitoringTargetCreate,
  PdfEvidenceUpload,
  RepairCaseDetailResponse,
  RepairProposal,
  TargetDetailResponse,
  TargetFilters,
  TargetListResponse,
  UploadSession,
  UrlEvidenceCreate,
} from "./types";


const PREFIX = "/msrp/source-governance";


function idempotencyKey(action: string): string {
  const identity = typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${action}:${identity}`;
}


function mutationHeaders(action: string): HeadersInit {
  return { "X-Idempotency-Key": idempotencyKey(action) };
}


function queryString(values: Record<string, string>): string {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    const normalized = value.trim();
    if (normalized) params.set(key, normalized);
  });
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}


async function sha256Hex(data: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest))
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}


export const governanceApi = {
  listTargets(filters: TargetFilters, signal?: AbortSignal): Promise<TargetListResponse> {
    return request<TargetListResponse>(
      `${PREFIX}/targets${queryString({
        country: filters.country,
        brand: filters.brand,
        monitoring_status: filters.monitoringStatus,
        roster_type: filters.rosterType,
      })}`,
      { signal },
    );
  },

  getTarget(targetId: string, signal?: AbortSignal): Promise<TargetDetailResponse> {
    return request<TargetDetailResponse>(`${PREFIX}/targets/${targetId}`, { signal });
  },

  async createTarget(payload: MonitoringTargetCreate): Promise<MonitoringTarget> {
    const response = await request<{ item: MonitoringTarget }>(`${PREFIX}/targets`, {
      method: "POST",
      headers: mutationHeaders("target-create"),
      body: JSON.stringify(payload),
    });
    return response.item;
  },

  async addUrlEvidence(targetId: string, payload: UrlEvidenceCreate): Promise<EvidenceAsset> {
    const response = await request<{ item: EvidenceAsset }>(
      `${PREFIX}/targets/${targetId}/url-evidence`,
      {
        method: "POST",
        headers: mutationHeaders("url-evidence"),
        body: JSON.stringify(payload),
      },
    );
    return response.item;
  },

  async uploadPdfEvidence(value: PdfEvidenceUpload): Promise<void> {
    const completeSha256 = await sha256Hex(await value.file.arrayBuffer());
    const initiated = await request<{ item: UploadSession }>(
      `${PREFIX}/evidence-uploads/initiate`,
      {
        method: "POST",
        headers: mutationHeaders("pdf-initiate"),
        body: JSON.stringify({
          targetId: value.targetId,
          sourceId: value.sourceId,
          repairCaseId: value.repairCaseId,
          sourceUrl: value.sourceUrl,
          officialDomain: value.officialDomain,
          originalFilename: value.file.name,
          expectedMimeType: "application/pdf",
          expectedSizeBytes: value.file.size,
          expectedSha256: completeSha256,
          sourceType: value.sourceType,
          semanticLane: value.semanticLane,
        }),
      },
    );
    let upload = initiated.item;
    if (upload.uploadStatus === "completed") return;

    for (let partNumber = 1; partNumber <= upload.totalParts; partNumber += 1) {
      const start = (partNumber - 1) * upload.chunkSizeBytes;
      const part = value.file.slice(start, Math.min(start + upload.chunkSizeBytes, value.file.size));
      const partSha256 = await sha256Hex(await part.arrayBuffer());
      const response = await request<{ item: UploadSession }>(
        `${PREFIX}/evidence-uploads/${upload.uploadSessionId}/parts/${partNumber}`,
        {
          method: "PUT",
          headers: {
            "Content-Type": "application/octet-stream",
            "X-Part-Sha256": partSha256,
          },
          body: part,
        },
      );
      upload = response.item;
      value.onProgress?.(partNumber, upload.totalParts);
    }

    await request<{ item: { upload: UploadSession; evidence: EvidenceAsset } }>(
      `${PREFIX}/evidence-uploads/${upload.uploadSessionId}/complete`,
      {
        method: "POST",
        headers: mutationHeaders("pdf-complete"),
        body: JSON.stringify({ rowVersion: upload.rowVersion }),
      },
    );
  },

  getCase(caseId: string, signal?: AbortSignal): Promise<RepairCaseDetailResponse> {
    return request<RepairCaseDetailResponse>(`${PREFIX}/cases/${caseId}`, { signal });
  },

  async requestHermes(caseId: string, payload: HermesDiagnosisRequest): Promise<void> {
    await request(`${PREFIX}/cases/${caseId}/request-hermes-diagnosis`, {
      method: "POST",
      headers: mutationHeaders("hermes-diagnosis"),
      body: JSON.stringify(payload),
    });
  },

  async submitProposal(proposal: RepairProposal): Promise<RepairProposal> {
    const response = await request<{ item: RepairProposal }>(
      `${PREFIX}/proposals/${proposal.proposalId}/submit`,
      {
        method: "POST",
        headers: mutationHeaders("proposal-submit"),
        body: JSON.stringify({ expectedStatus: proposal.proposalStatus }),
      },
    );
    return response.item;
  },

  async publishVersion(
    sourceVersionId: string,
    targetRowVersion: number,
    decisionReason: string,
  ): Promise<void> {
    await request(`${PREFIX}/source-versions/${sourceVersionId}/publish`, {
      method: "POST",
      headers: mutationHeaders("version-publish"),
      body: JSON.stringify({ targetRowVersion, decisionReason }),
    });
  },

  async rollbackVersion(
    sourceVersionId: string,
    targetRowVersion: number,
    decisionReason: string,
  ): Promise<void> {
    await request(`${PREFIX}/source-versions/${sourceVersionId}/rollback`, {
      method: "POST",
      headers: mutationHeaders("version-rollback"),
      body: JSON.stringify({ targetRowVersion, decisionReason }),
    });
  },

  matchingReviewUrl(target: MonitoringTarget): string {
    const params = new URLSearchParams({
      country: target.country,
      brand: target.brand,
      model: target.model,
    });
    return `/data/matching-review?${params.toString()}`;
  },

  evidenceUrl(evidence: EvidenceAsset): string | null {
    if (!evidence.finalUrl) return null;
    const parsed = new URL(evidence.finalUrl, window.location.origin);
    if (parsed.protocol !== "https:") return null;
    return parsed.toString();
  },

  apiHref(path: string): string {
    return apiUrl(path);
  },
};
