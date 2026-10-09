// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { request } from "../../api/core";

afterEach(() => { localStorage.clear(); vi.unstubAllGlobals(); });

it("JSON and blob exports retain the current auth token when callers set Content-Type", async () => {
  const fetch = vi.fn(async () => Response.json({ ok: true }));
  vi.stubGlobal("fetch", fetch);
  localStorage.setItem("jato_auth_token", "fresh-session");
  await request("/test-pi-export", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
  await api.exportOrderGeniusPi("SE", 2026, {});
  await api.createVehicleAllocationPi({ countryCode: "SE" });
  localStorage.setItem("jato_auth_token", "renewed-session");
  await api.exportPiInvoice("PI-SE-202609-001", { invoiceDate: "2026-09-01", referenceNo: "PI", portOfShipment: "China", portOfDischarge: "Destination" });
  const headers = fetch.mock.calls.map((call) => new Headers((call as unknown as [string, RequestInit])[1].headers));
  expect(headers.map((value) => value.get("X-Auth-Token"))).toEqual(["fresh-session", "fresh-session", "fresh-session", "renewed-session"]);
  expect(headers.every((value) => value.get("Content-Type") === "application/json")).toBe(true);
});
