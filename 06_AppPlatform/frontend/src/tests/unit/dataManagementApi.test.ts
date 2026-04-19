import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

function createStorageMock() {
  const store = new Map<string, string>();
  return {
    getItem(key: string) {
      return store.has(key) ? store.get(key) ?? null : null;
    },
    setItem(key: string, value: string) {
      store.set(key, value);
    },
    removeItem(key: string) {
      store.delete(key);
    },
    clear() {
      store.clear();
    },
  };
}

describe("data management api", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", createStorageMock());
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("returns airflow action payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              action: "start",
              detail: "Airflow 本地栈已启动。",
              status: {
                available: true,
                mode: "running",
                detail: "running",
                uiUrl: "http://127.0.0.1:8080",
                running: true,
                runningServices: 3,
                totalServices: 3,
                updatedAt: "2026-04-18T08:00:00+00:00",
                services: [],
                actions: {
                  canStart: false,
                  canStop: true,
                  canOpenUi: true,
                },
              },
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).resolves.toMatchObject({
      action: "start",
      detail: "Airflow 本地栈已启动。",
      status: {
        mode: "running",
      },
    });
  });

  it("returns voc sync payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              root: "04_Processed_data/voc",
              countryCount: 8,
              sourceRunCount: 24,
              documentCount: 46,
              errorCount: 1,
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.syncVocRawToStore()).resolves.toMatchObject({
      countryCount: 8,
      sourceRunCount: 24,
      documentCount: 46,
    });
  });

  it("preserves conflict detail for airflow stop errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "Airflow 当前已经停止。" }),
          { status: 409, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.stopAirflow()).rejects.toThrow(
      "409 Airflow 当前已经停止。",
    );
  });

  it("preserves runtime detail for airflow start errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "postgres failed" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).rejects.toThrow("500 postgres failed");
  });
});
