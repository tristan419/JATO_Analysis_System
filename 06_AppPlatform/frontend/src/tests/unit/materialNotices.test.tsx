// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { MaterialNotices } from "../../components/orderGenius/MaterialNotices";
import type { MaterialNotice } from "../../types/orderGenius";
const make = (country: string, base: string, values: Partial<MaterialNotice> = {}): MaterialNotice => ({ key: country + base, countryCode: country, brand: "JAECOO", modelName: "J7", bomTemplate: base, date: "2026-10-15", kind: "start", target: null, note: "", read: false, ...values });
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
it("groups country/base events but only explicit mark updates personal read state", async () => {
  const notices = [make("CH", "A"), make("SE", "B")];
  vi.spyOn(api, "getMaterialNotices").mockImplementation(async country => ({ countryCode: country, notices: notices.filter(n => n.countryCode === country).map(n => ({ ...n })) }));
  const mark = vi.spyOn(api, "readMaterialNotice").mockImplementation(async (key, _country, read) => { const n = notices.find(n => n.key === key); if (n) n.read = read; return { read }; });
  const view = vi.fn(); render(<MaterialNotices countries={["CH", "SE"]} authorizationKey="a" revision={0} defaultExpanded onViewModel={view} />);
  await screen.findByText("2 records"); fireEvent.click(screen.getByRole("button", { name: "J7" })); expect(mark).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Mark read" })); await screen.findByRole("button", { name: "Mark unread" }); expect(mark).toHaveBeenCalledTimes(2);
});
it("reports partial failure, re-reads and never presents the whole group read", async () => {
  const notices = [make("CH", "A"), make("CH", "B")];
  vi.spyOn(api, "getMaterialNotices").mockImplementation(async country => ({ countryCode: country, notices: notices.map(n => ({ ...n })) }));
  vi.spyOn(api, "readMaterialNotice").mockImplementation(async key => { if (key === "CHB") throw new Error("revoked"); notices[0].read = true; return { read: true }; });
  render(<MaterialNotices countries={["CH"]} authorizationKey="a" revision={0} defaultExpanded onViewModel={vi.fn()} />);
  await screen.findByText("2 records"); fireEvent.click(screen.getByRole("button", { name: "Mark read" })); await screen.findByRole("alert");
  await waitFor(() => expect(screen.getByRole("button", { name: "Mark read" })).toBeTruthy());
});
it("keeps different dates, kinds, and relationship targets distinct", async () => {
  vi.spyOn(api, "getMaterialNotices").mockResolvedValue({ countryCode: "CH", notices: [make("CH", "A"), make("CH", "B", { date: "2026-11-01" }), make("CH", "C", { kind: "end" }), make("CH", "D", { kind: "relation", target: "E" }), make("CH", "F", { kind: "relation", target: "G" })] });
  render(<MaterialNotices countries={["CH"]} authorizationKey="a" revision={0} onViewModel={vi.fn()} />);
  await waitFor(() => expect(document.querySelectorAll(".mw-notice")).toHaveLength(5)); expect(document.querySelector("details")?.open).toBe(false);
});
