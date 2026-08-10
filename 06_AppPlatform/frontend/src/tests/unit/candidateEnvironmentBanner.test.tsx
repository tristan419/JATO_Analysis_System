// @vitest-environment jsdom

import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CandidateEnvironmentBanner,
  parseCandidatePreviewMetadata,
} from "../../components/CandidateEnvironmentBanner";
import type { CandidatePreviewMetadata } from "../../components/CandidateEnvironmentBanner";
import { isCandidatePreviewOrigin } from "../../utils/candidateRuntime";

const commitSha = "a".repeat(40);
const newerMainSha = "c".repeat(40);
const archiveSha256 = "b".repeat(64);
const databaseName = "jato_candidate_20260809t083000z_0123456789abcdef";
const databaseSnapshotAt = "2026-08-09T08:30:00Z";

function useOrigin(hostname: string, port: string): void {
  vi.stubGlobal("location", { hostname, port });
}

function validMetadata(): CandidatePreviewMetadata {
  return {
    role: "candidate",
    commitSha,
    archiveSha256,
    databaseName,
    databaseSnapshotAt,
    candidateSlot: 8001,
    previewPort: 18002,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  delete document.documentElement.dataset.releaseRole;
});

describe("CandidateEnvironmentBanner", () => {
  it("shows that a release-bound Candidate matches current main", async () => {
    useOrigin("127.0.0.1", "18002");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockResolvedValueOnce(jsonResponse({ sha: commitSha }));
    vi.stubGlobal("fetch", fetchMock);

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate · 当前 main · 可测试")).toBeTruthy();
    expect(screen.getByText("不是正式 www")).toBeTruthy();
    expect(screen.getByText(commitSha.slice(0, 12))).toBeTruthy();
    expect(screen.getByText(archiveSha256.slice(0, 12))).toBeTruthy();
    expect(screen.getByText(/Active DB 快照开始于/)).toBeTruthy();
    expect(screen.getByText(databaseName.slice(-8))).toBeTruthy();
    expect(screen.getByText("物理诊断 slot 8001（角色固定，不互换）")).toBeTruthy();
    expect(document.documentElement.dataset.releaseRole).toBe("candidate");
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "https://api.github.com/repos/tristan419/JATO_Analysis_System/commits/main",
      expect.objectContaining({ cache: "no-store", credentials: "omit" }),
    );
  });

  it("blocks acceptance and shows both SHAs when Candidate is behind main", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockResolvedValueOnce(jsonResponse({ sha: newerMainSha })));

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate 落后 main · 禁止据此验收")).toBeTruthy();
    expect(screen.getByText(commitSha.slice(0, 12))).toBeTruthy();
    expect(screen.getByText(newerMainSha.slice(0, 12))).toBeTruthy();
    expect(screen.getByRole("alert").getAttribute("aria-live")).toBe("assertive");
  });

  it("blocks acceptance while the latest main check is pending", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockReturnValueOnce(new Promise<Response>(() => undefined)));

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate 身份已验证 · 正在确认最新 main")).toBeTruthy();
    expect(screen.getByRole("alert").getAttribute("aria-live")).toBe("assertive");
    expect(screen.getByRole("alert").classList.contains("candidate-environment-banner--unverified")).toBe(true);
    expect(screen.queryByText("Candidate · 当前 main · 可测试")).toBeNull();
  });

  it("rechecks main when a long-open Candidate tab regains focus or becomes visible", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockResolvedValueOnce(jsonResponse({ sha: commitSha }))
      .mockResolvedValueOnce(jsonResponse({ sha: newerMainSha }))
      .mockResolvedValueOnce(jsonResponse({ sha: commitSha }));
    vi.stubGlobal("fetch", fetchMock);

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    await act(async () => Promise.resolve());
    expect(screen.getByText("Candidate · 当前 main · 可测试")).toBeTruthy();

    await act(async () => window.dispatchEvent(new Event("focus")));
    await act(async () => Promise.resolve());
    expect(screen.getByText("Candidate 落后 main · 禁止据此验收")).toBeTruthy();

    vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    await act(async () => document.dispatchEvent(new Event("visibilitychange")));
    await act(async () => Promise.resolve());
    expect(screen.getByText("Candidate · 当前 main · 可测试")).toBeTruthy();
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("keeps verified identity but does not claim freshness when GitHub is unavailable", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockRejectedValueOnce(new Error("GitHub unavailable")));

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate 身份已验证，但无法确认最新 main")).toBeTruthy();
    expect(screen.queryByText("Candidate · 当前 main · 可测试")).toBeNull();
    expect(screen.getByRole("alert").getAttribute("aria-live")).toBe("assertive");
    expect(screen.getByRole("alert").classList.contains("candidate-environment-banner--unverified")).toBe(true);
  });

  it("times out the GitHub freshness request", async () => {
    vi.useFakeTimers();
    useOrigin("candidate.ojeur.cloud", "");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(validMetadata()))
      .mockImplementationOnce(
        (_input: RequestInfo | URL, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(new DOMException("Aborted", "AbortError")),
            { once: true },
          );
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    await act(async () => vi.advanceTimersByTimeAsync(5_000));

    expect(screen.getByText("Candidate 身份已验证，但无法确认最新 main")).toBeTruthy();
    expect(screen.getByRole("alert").getAttribute("aria-live")).toBe("assertive");
    expect(fetchMock.mock.calls[1]?.[1]?.signal.aborted).toBe(true);
  });

  it("stays absent when Active serves the SPA fallback instead of Candidate JSON", async () => {
    useOrigin("www.ojeur.cloud", "");
    const fetchMock = vi.fn().mockResolvedValue(new Response("<!doctype html>", {
      status: 200,
      headers: { "content-type": "text/html" },
    }));
    vi.stubGlobal("fetch", fetchMock);

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());

    expect(screen.queryByText("Candidate · 当前 main · 可测试")).toBeNull();
    expect(screen.queryByText("Candidate 身份不可验证，禁止据此验收")).toBeNull();
    expect(document.documentElement.dataset.releaseRole).toBeUndefined();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith("/candidate-preview.json", expect.any(Object));
  });

  it("blocks Candidate acceptance while identity metadata is loading", () => {
    useOrigin("localhost", "18002");
    vi.stubGlobal("fetch", vi.fn().mockReturnValue(new Promise<Response>(() => undefined)));

    render(<CandidateEnvironmentBanner />);

    expect(screen.getByText("Candidate 身份验证中，禁止据此验收")).toBeTruthy();
    expect(screen.getByRole("alert")).toBeTruthy();
    expect(document.documentElement.dataset.releaseRole).toBe("candidate");
  });

  it("fails closed when Candidate identity metadata is invalid", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      ...validMetadata(),
      archiveSha256: "not-a-sha256",
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })));

    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate 身份不可验证，禁止据此验收")).toBeTruthy();
    expect(screen.getByRole("alert").getAttribute("aria-live")).toBe("assertive");
    expect(document.documentElement.dataset.releaseRole).toBe("candidate");
  });

  it("fails closed when Candidate metadata cannot be loaded or decoded", async () => {
    useOrigin("127.0.0.1", "18002");
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network unavailable")));

    const firstRender = render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());
    expect(screen.getByText("Candidate 身份不可验证，禁止据此验收")).toBeTruthy();

    firstRender.unmount();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("{", {
      status: 200,
      headers: { "content-type": "application/json" },
    })));
    render(<CandidateEnvironmentBanner />);
    await act(async () => Promise.resolve());

    expect(screen.getByText("Candidate 身份不可验证，禁止据此验收")).toBeTruthy();
  });
});

describe("parseCandidatePreviewMetadata", () => {
  it("rejects a wrong port, slot, role, commit SHA, or archive SHA-256", () => {
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      commitSha: "abc123",
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      archiveSha256: "b".repeat(63),
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      role: "active",
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      candidateSlot: 8000,
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      previewPort: 18001,
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      databaseSnapshotAt: "not-a-timestamp",
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      databaseName: "production",
    })).toBeNull();
    expect(parseCandidatePreviewMetadata({
      ...validMetadata(),
      databaseName: undefined,
    })).toBeNull();
  });
});

describe("isCandidatePreviewOrigin", () => {
  it("recognizes only the dedicated loopback and Candidate hosts", () => {
    expect(isCandidatePreviewOrigin({ hostname: "127.0.0.1", port: "18002" })).toBe(true);
    expect(isCandidatePreviewOrigin({ hostname: "localhost", port: "18002" })).toBe(true);
    expect(isCandidatePreviewOrigin({ hostname: "candidate.ojeur.cloud", port: "" })).toBe(true);
    expect(isCandidatePreviewOrigin({ hostname: "www.ojeur.cloud", port: "" })).toBe(false);
    expect(isCandidatePreviewOrigin({ hostname: "localhost", port: "4175" })).toBe(false);
  });
});
