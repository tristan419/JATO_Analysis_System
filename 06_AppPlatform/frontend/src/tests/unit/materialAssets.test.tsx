// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { materialAssets } from "../../api/materialAssets";
import { MaterialAssetsPage } from "../../pages/MaterialAssetsPage";
import { MaterialAssetInspector } from "../../components/orderGenius/MaterialAssetInspector";
import type { AssetCatalog, AssetMaterial } from "../../types/materialAssets";
const auth = vi.hoisted(() => ({ role: "editor", brands: ["JAECOO"] }));
vi.mock("../../contexts/AuthContext", () => ({ useAuth: () => ({ user: { username: "user", role: auth.role, primaryCountry: "CH", secondaryCountries: [], brands: auth.brands }, token: "test", profileLoaded: true }) }));
const t = (en: string): string => en;
const base = (key: string, model = "J7", historical = false): AssetMaterial => ({ key, brand: "JAECOO", modelName: model, bomTemplate: `T7150D**${key}`, version: "Large battery FWD", powertrain: "PHEV", effectiveFrom: "2026-01-01", effectiveTo: null, historical, colours: [{ key: "code:CL", code: "CL", name: "Black", hex: "#000000" }], interiors: [{ key: "name:BLACK", code: null, name: "Black", hex: null }], notes: [], issues: [] });
function catalog(): AssetCatalog {
  return { today: "2026-10-31", materials: [base("A"), base("B"), base("C", "J8"), base("H", "J7", true)], bindings: [{ id: "link", materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", versionId: null, rowVersion: 1 }], groups: [{ id: "album", brand: "JAECOO", modelName: "J7", kind: "exterior", colourKey: "code:CL", name: "Shared black", rowVersion: 1, references: ["A"], files: [{ id: "photo", filename: "black.png", contentType: "image/png", sha256: "hash" }] }], versions: [] };
}
beforeEach(() => { auth.role = "editor"; localStorage.clear(); vi.spyOn(materialAssets, "catalog").mockResolvedValue(catalog()); });
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
describe("Material Assets", () => {
  it("opens the requested material inside the selection page and exposes Close", async () => {
    const close = vi.fn();
    render(<MemoryRouter><MaterialAssetsPage request={{ materials: [{ brand: "JAECOO", modelName: "J8", bomTemplate: "T7150D**C" }], compare: false }} onClose={close} /></MemoryRouter>);
    await screen.findByRole("heading", { name: "T7150D**C" });
    expect(document.querySelector(".mw-workspace.is-embedded")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Close Material Assets" })); expect(close).toHaveBeenCalledOnce();
  });
  it("defaults EN, groups bases by model, keeps historical collapsed and has no BOM edit jump", async () => {
    render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>); await screen.findByRole("heading", { name: "T7150D**A" });
    expect(document.querySelector<HTMLDetailsElement>(".mw-historical")?.open).toBe(false);
    expect(screen.queryByText(/Edit.*BOM/)).toBeNull(); expect(screen.getByRole("button", { name: "Upload" })).toBeTruthy();
  });
  it("Order Filler can compare but never sees mutation controls", async () => {
    auth.role = "order_filler"; render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>); await screen.findByRole("heading", { name: "T7150D**A" });
    expect(screen.queryByRole("button", { name: "Upload" })).toBeNull(); expect(screen.queryByText("Add photos")).toBeNull();
    fireEvent.click(screen.getByLabelText("Compare T7150D**A")); fireEvent.click(screen.getByLabelText("Compare T7150D**C"));
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Compare 2" }).disabled).toBe(false);
  });
  it("reuses same-model colour references in one explicit atomic request", async () => {
    const bind = vi.spyOn(materialAssets, "bind").mockResolvedValue({ saved: true });
    const data = catalog(); render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Reuse…" }));
    const dialog = screen.getByRole("dialog"); expect(dialog.textContent).not.toContain("T7150D**C");
    const labels = [...dialog.querySelectorAll("label")]; const target = labels.find(l => l.textContent?.includes("T7150D**B"))?.querySelector("input"); expect(target).toBeTruthy();
    if (target) fireEvent.click(target); expect(bind).not.toHaveBeenCalled(); fireEvent.click(screen.getByRole("button", { name: "Apply to 1" }));
    await waitFor(() => expect(bind).toHaveBeenCalledWith([{ materialKey: "B", kind: "exterior", colourKey: "code:CL", groupId: "album", versionId: null, rowVersion: 0 }]));
  });
  it("requires explicit config column, sends the original file and selected base version", async () => {
    vi.spyOn(materialAssets, "previewConfig").mockResolvedValue({ columns: [{ column: 26, name: "Large battery" }], warnings: [] });
    const upload = vi.spyOn(materialAssets, "uploadConfig").mockResolvedValue({ versionId: "uuid" }); const data = catalog();
    render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Upload" }));
    const fileInput = screen.getByRole("dialog").querySelector<HTMLInputElement>('input[type="file"]'); const file = new File(["xlsx"], "source.xlsx");
    if (fileInput) fireEvent.change(fileInput, { target: { files: [file] } });
    const select = await screen.findByLabelText("Configuration column"); expect(screen.getByRole<HTMLButtonElement>("button", { name: "Save version" }).disabled).toBe(true);
    fireEvent.change(select, { target: { value: "26" } }); fireEvent.click(screen.getByRole("button", { name: "Save version" }));
    await waitFor(() => expect(upload).toHaveBeenCalledOnce()); const form = upload.mock.calls[0][0]; expect(form.get("file")).toBe(file); expect(form.get("source_column")).toBe("26"); expect(form.get("material_key")).toBe("A");
  });
  it("failed upload retains input for retry and never claims saved", async () => {
    vi.spyOn(materialAssets, "previewConfig").mockRejectedValue(new Error("422 matrix unavailable")); const data = catalog();
    render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Upload" })); const input = screen.getByRole("dialog").querySelector<HTMLInputElement>('input[type="file"]');
    if (input) fireEvent.change(input, { target: { files: [new File(["bad"], "bad.xlsx")] } });
    await screen.findByText("422 matrix unavailable"); expect(screen.getByRole<HTMLButtonElement>("button", { name: "Save version" }).disabled).toBe(true);
  });
});
