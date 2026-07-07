// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { MegaMenu } from "../../components/MegaMenu";

const logoutMock = vi.fn();

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: () => ({
    logout: logoutMock,
    user: {
      avatarUrl: null,
      displayName: "Test User",
      email: "test@example.com",
      primaryCountry: "SE",
      role: "editor",
      secondaryCountries: [],
      username: "test",
    },
  }),
}));

describe("MegaMenu route diagnostics entry", () => {
  beforeEach(() => {
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn(() => ({
        addEventListener: vi.fn(),
        addListener: vi.fn(),
        dispatchEvent: vi.fn(),
        matches: false,
        media: "(hover: hover)",
        onchange: null,
        removeEventListener: vi.fn(),
        removeListener: vi.fn(),
      })),
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("exposes diagnostics from the profile popover and mobile drawer", () => {
    render(
      <MemoryRouter initialEntries={["/dashboard"]}>
        <MegaMenu />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /Test User/ }));
    expect(
      screen.getByRole("link", { name: "Route Diagnostics" }).getAttribute("href"),
    ).toBe("/route-diagnostics");

    fireEvent.click(screen.getByRole("button", { name: "展开主导航" }));
    expect(
      screen.getByRole("link", { name: "路由诊断" }).getAttribute("href"),
    ).toBe("/route-diagnostics");
  });
});
