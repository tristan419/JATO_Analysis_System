// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { OrderingBrandNotice } from "../../components/RoleUpgradeModal";
import { api } from "../../api/client";
import type { User } from "../../contexts/AuthContext";

function user(role: string, brands: string[]): User {
  return { username: "test", role, brands, secondaryCountries: [], primaryCountry: "CH",
    email: null, oauthProvider: null, avatarUrl: null, displayName: null,
    preferredLandingPage: "/dashboard", profileComplete: true };
}

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("contextual brand requests", () => {
  it.each(["admin", "developer", "viewer"])("does not show a no-brand prompt to %s", (role) => {
    const { container } = render(<OrderingBrandNotice user={user(role, [])} />);
    expect(container.textContent).toBe("");
  });

  it("does not prompt an assigned filler", () => {
    const { container } = render(<OrderingBrandNotice user={user("order_filler", ["OMODA", "JAECOO"])} />);
    expect(container.textContent).toBe("");
  });

  it("requires brands and a reason, then submits through the existing request API", async () => {
    const request = vi.spyOn(api, "requestRoleUpgrade").mockResolvedValue({ status: "pending" });
    render(<OrderingBrandNotice user={user("order_filler", [])} />);
    fireEvent.click(screen.getByRole("button", { name: "Request brand access / 申请品牌" }));
    expect(screen.getByText(/批准仅分配品牌，不提升角色/)).toBeTruthy();
    const submit = screen.getByRole("button", { name: "提交申请" });
    expect(submit.hasAttribute("disabled")).toBe(true);
    fireEvent.click(screen.getByRole("checkbox", { name: "OMODA" }));
    expect(submit.hasAttribute("disabled")).toBe(true);
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Handle vehicle deliveries in CH" } });
    fireEvent.click(submit);
    await waitFor(() => expect(request).toHaveBeenCalledWith({ requested_role: "editor",
      requestedBrands: ["OMODA"], reason: "Handle vehicle deliveries in CH" }));
    expect(await screen.findByText(/当前状态: pending/)).toBeTruthy();
  });
});
