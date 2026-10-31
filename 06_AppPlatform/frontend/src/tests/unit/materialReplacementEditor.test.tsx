// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { MaterialReplacementEditor } from "../../components/orderGenius/MaterialReplacementEditor";
import { MaterialNotices } from "../../components/orderGenius/MaterialNotices";
import type { MaterialNotice } from "../../types/orderGenius";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const catalog = { nodes: ["OLD", "NEW"].map((bomTemplate) => ({ bomTemplate, brand: "JAECOO", modelName: "JAECOO5 BEV", version: "Select", powertrain: "BEV", materialCodes: [`${bomTemplate}-BW`] })), links: [] };

describe("Explicit material relationship maintenance", () => {
  it("previews and saves the exact accepted link without changing lifecycle or codes", async () => {
    vi.spyOn(api, "getMaterialReplacements").mockResolvedValue(catalog);
    const save = vi.spyOn(api, "saveMaterialReplacement").mockResolvedValue({ changed: true, rowVersion: 1 });
    const lifecycle = vi.spyOn(api, "updateSkuLifecycle");
    const onSaved = vi.fn();
    render(<MaterialReplacementEditor brand="JAECOO" bomTemplate="OLD" onSaved={onSaved} />);
    fireEvent.click(screen.getByRole("button", { name: /Link versions/ }));
    await screen.findByLabelText("Successor / 新base");
    await waitFor(() => expect(screen.getByRole("button", { name: /Preview relation/ }).hasAttribute("disabled")).toBe(false));
    fireEvent.change(screen.getByLabelText("Successor / 新base"), { target: { value: "NEW" } });
    fireEvent.change(screen.getByLabelText("Change note / 更替说明"), { target: { value: "New battery" } });
    fireEvent.click(screen.getByRole("button", { name: /Preview relation/ }));
    expect(save).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog").textContent).toContain("未关联 → NEW");
    fireEvent.click(screen.getByRole("button", { name: /Save relation/ }));
    await screen.findByText(/Saved \/ 已保存/);
    expect(save).toHaveBeenCalledWith({ brand: "JAECOO", from: "OLD", to: "NEW", note: "New battery", rowVersion: 0 });
    expect(lifecycle).not.toHaveBeenCalled();
    fireEvent.click(screen.getAllByRole("button", { name: /Close & reload/ })[0]);
    expect(onSaved).toHaveBeenCalledOnce();
  });

  it("cancel is zero writes and a rejected save retains its draft", async () => {
    vi.spyOn(api, "getMaterialReplacements").mockResolvedValue(catalog);
    const save = vi.spyOn(api, "saveMaterialReplacement").mockRejectedValue(new Error("Cycle rejected"));
    render(<MaterialReplacementEditor brand="JAECOO" bomTemplate="OLD" onSaved={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: /Link versions/ }));
    await waitFor(() => expect(screen.getByRole("button", { name: /Preview relation/ }).hasAttribute("disabled")).toBe(false));
    fireEvent.click(screen.getByRole("button", { name: "Cancel / 取消" }));
    expect(save).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /Link versions/ }));
    await waitFor(() => expect(screen.getByRole("button", { name: /Preview relation/ }).hasAttribute("disabled")).toBe(false));
    fireEvent.change(screen.getByLabelText("Successor / 新base"), { target: { value: "NEW" } });
    fireEvent.click(screen.getByRole("button", { name: /Preview relation/ }));
    fireEvent.click(screen.getByRole("button", { name: /Save relation/ }));
    await screen.findByText("Cycle rejected");
    fireEvent.click(screen.getByRole("button", { name: /Back/ }));
    expect((screen.getByLabelText("Successor / 新base") as HTMLSelectElement).value).toBe("NEW");
  });

  it("marks only the current account notice and re-reads, without hiding the reminder", async () => {
    const notice: MaterialNotice = { key: "key", countryCode: "SE", kind: "start", date: "2026-10-30", bomTemplate: "NEW", brand: "JAECOO", modelName: "JAECOO5 BEV", target: null, note: "", read: false };
    const request = vi.spyOn(api, "getMaterialNotices").mockImplementation(async () => ({ countryCode: "SE", notices: [{ ...notice }] }));
    const mark = vi.spyOn(api, "readMaterialNotice").mockImplementation(async () => { notice.read = true; return { read: true }; });
    render(<MaterialNotices countries={["SE"]} authorizationKey="filler" revision={0} defaultExpanded onViewModel={vi.fn()} />);
    await screen.findByRole("button", { name: "JAECOO5 BEV" });
    fireEvent.click(screen.getByRole("button", { name: /Mark read/ }));
    await screen.findByRole("button", { name: /Mark unread/ });
    expect(mark).toHaveBeenCalledWith("key", "SE", true);
    expect(request).toHaveBeenCalledTimes(2);
    expect(screen.getByTitle("SE · NEW")).toBeTruthy();
  });
});
