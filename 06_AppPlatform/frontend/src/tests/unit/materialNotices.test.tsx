// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { MaterialNotices } from "../../components/orderGenius/MaterialNotices";
import type { MaterialNotice } from "../../types/orderGenius";

const make = (country: string, code: string, values: Partial<MaterialNotice> = {}): MaterialNotice => ({
  key: `${country}-${code}`, countryCode: country, brand: "OMODA", modelName: "OMODA5 HEV", date: "2026-10-30",
  kind: "start", bomTemplate: code, target: null, note: "", read: false, ...values,
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it("aggregates country × base into one unread event and requires an explicit viewing country", async () => {
  const data = [make("CH", "A"), make("CH", "B"), make("SE", "A"), make("SE", "B")];
  vi.spyOn(api, "getMaterialNotices").mockImplementation(async (country) => ({ countryCode: country, notices: data.filter((notice) => notice.countryCode === country).map((notice) => ({ ...notice })) }));
  const mark = vi.spyOn(api, "readMaterialNotice").mockImplementation(async (key, country, read) => {
    const notice = data.find((value) => value.key === key && value.countryCode === country);
    if (notice) notice.read = read;
    return { read };
  });
  const view = vi.fn();
  render(<MaterialNotices countries={["CH", "SE"]} authorizationKey="reader" revision={0} defaultExpanded onViewModel={view} />);
  await screen.findByText(/^My material reminders.*1 unread changes/);
  fireEvent.click(screen.getByText(/^OMODA · OMODA5 HEV · 1 changes/));
  expect(document.querySelectorAll(".og-material-notice")).toHaveLength(1);
  expect(screen.getByText("2 bases / 基准物料 · 2 countries / 国家")).toBeTruthy();
  expect((screen.getByRole("button", { name: /View model/ }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.change(screen.getByRole("combobox"), { target: { value: "SE" } });
  fireEvent.click(screen.getByRole("button", { name: /View model/ }));
  expect(view).toHaveBeenCalledWith("OMODA", "OMODA5 HEV", "SE", "A");
  fireEvent.click(screen.getByRole("button", { name: /Mark read/ }));
  await screen.findByText(/^My material reminders.*0 unread changes/);
  expect(mark).toHaveBeenCalledTimes(4);
  for (const notice of data) expect(mark).toHaveBeenCalledWith(notice.key, notice.countryCode, true);
  expect(document.querySelector(".og-material-notices > details")?.hasAttribute("open")).toBe(true);
  expect((screen.getByRole("combobox") as HTMLSelectElement).value).toBe("SE");
});

it("reports partial group failure and re-reads without pretending the whole event is read", async () => {
  const data = [make("CH", "A"), make("CH", "B")];
  vi.spyOn(api, "getMaterialNotices").mockImplementation(async (country) => ({ countryCode: country, notices: data.map((notice) => ({ ...notice })) }));
  vi.spyOn(api, "readMaterialNotice").mockImplementation(async (key, _country, read) => {
    if (key.endsWith("B")) throw new Error("Scope revoked");
    data[0].read = read; return { read };
  });
  render(<MaterialNotices countries={["CH"]} authorizationKey="reader" revision={0} defaultExpanded onViewModel={vi.fn()} />);
  await screen.findByText(/^OMODA · OMODA5 HEV/);
  fireEvent.click(screen.getByText(/^OMODA · OMODA5 HEV/));
  fireEvent.click(screen.getByRole("button", { name: /Mark read/ }));
  await screen.findByRole("alert");
  expect(screen.getByRole("alert").textContent).toContain("1条记录未能更新");
  await waitFor(() => expect(screen.getByText(/^My material reminders.*1 unread changes/)).toBeTruthy());
  expect(data[0].read).toBe(true); expect(data[1].read).toBe(false);
});

it("keeps distinct dates, kinds and replacement targets as separate changes", async () => {
  const data = [make("CH", "A"), make("CH", "B", { date: "2026-11-01" }), make("CH", "C", { kind: "end" }),
    make("CH", "D", { kind: "relation", target: "E" }), make("CH", "F", { kind: "relation", target: "G" })];
  vi.spyOn(api, "getMaterialNotices").mockResolvedValue({ countryCode: "CH", notices: data });
  render(<MaterialNotices countries={["CH"]} authorizationKey="reader" revision={0} onViewModel={vi.fn()} />);
  await screen.findByText(/^My material reminders.*5 unread changes/);
  expect(document.querySelector(".og-material-notices")?.hasAttribute("open")).toBe(false);
  expect(document.querySelectorAll(".og-material-notice")).toHaveLength(5);
});
