// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { BomCountryColumns } from "../../components/orderGenius/BomCountryColumns";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function deleteDialog() {
  fireEvent.change(screen.getByLabelText("Country column to delete / 待删除国家列"), { target: { value: "DM" } });
  fireEvent.click(screen.getByRole("button", { name: "Delete column / 删除国家列" }));
  return within(screen.getByRole("dialog"));
}

describe("BOM country columns", () => {
  it("requires confirmation and deletes the full country, then refreshes and offers restore", async () => {
    const remove = vi.spyOn(api, "deleteCountryFobs").mockResolvedValue({ countryCode: "DM", cleared: 8 });
    vi.spyOn(api, "getCountryFobTrash").mockResolvedValue({ items: [{ countryCode: "DM", rows: 8 }] });
    const changed = vi.fn().mockResolvedValue(true);
    render(<BomCountryColumns countries={["CH", "DM"]} onChanged={changed} />);
    fireEvent.click(deleteDialog().getByRole("button", { name: "Cancel / 取消" }));
    expect(remove).not.toHaveBeenCalled();
    fireEvent.click(deleteDialog().getByRole("button", { name: "Delete / 删除" }));
    await waitFor(() => expect(changed).toHaveBeenCalledOnce());
    expect(remove).toHaveBeenCalledExactlyOnceWith("DM");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByRole("status").textContent).toContain("deleted 8 records");
    expect(screen.getByRole("button", { name: "Restore DM / 恢复", hidden: true })).toBeTruthy();
  });

  it("retains a failed confirmation for retry without claiming success", async () => {
    vi.spyOn(api, "deleteCountryFobs").mockRejectedValue(new Error("403 forbidden"));
    const changed = vi.fn();
    render(<BomCountryColumns countries={["DM"]} onChanged={changed} />);
    fireEvent.click(deleteDialog().getByRole("button", { name: "Delete / 删除" }));
    await screen.findByText("403 forbidden");
    expect(screen.getByRole("dialog")).toBeTruthy();
    expect(changed).not.toHaveBeenCalled();
  });

  it("reports a completed write separately from a failed refresh", async () => {
    vi.spyOn(api, "deleteCountryFobs").mockResolvedValue({ countryCode: "DM", cleared: 8 });
    vi.spyOn(api, "getCountryFobTrash").mockResolvedValue({ items: [] });
    render(<BomCountryColumns countries={["DM"]} onChanged={async () => false} />);
    fireEvent.click(deleteDialog().getByRole("button", { name: "Delete / 删除" }));
    await waitFor(() => expect(screen.getByRole("status").textContent).toContain("Refresh failed"));
    expect(screen.getByRole("status").textContent).toContain("deleted 8");
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("restores explicitly and reports skipped conflicts, without exposing permanent deletion", async () => {
    vi.spyOn(api, "getCountryFobTrash").mockResolvedValue({ items: [{ countryCode: "DM", rows: 8 }] });
    const restore = vi.spyOn(api, "restoreCountryFobs").mockResolvedValue({ countryCode: "DM", rows: 8, restored: 6, skippedActiveConflict: 2 });
    const purge = vi.spyOn(api, "purgeCountryFobTrash");
    render(<BomCountryColumns countries={[]} onChanged={async () => true} />);
    const summary = screen.getByText("Deleted columns / 已删除国家列");
    fireEvent.click(summary);
    fireEvent.click(await screen.findByRole("button", { name: "Restore DM / 恢复" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Restore / 恢复" }));
    await waitFor(() => expect(screen.getByRole("status").textContent).toContain("skipped 2 conflicts"));
    expect(restore).toHaveBeenCalledExactlyOnceWith("DM");
    expect(purge).not.toHaveBeenCalled();
  });
});
