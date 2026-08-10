// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { OAuthGate } from "../../App";
import { RequireRole } from "../../components/RequireRole";
import { AuthProvider, useAuth } from "../../contexts/AuthContext";
import { LoginPage } from "../../pages/LoginPage";
import { fetchAuthEndpoint } from "../../utils/authFallback";

vi.mock("../../utils/authFallback", () => ({
  fetchAuthEndpoint: vi.fn(),
}));

function useOrigin(
  hostname: string,
  port: string,
  search = "",
  pathname = "/",
  hash = "",
  replace = vi.fn(),
): void {
  vi.stubGlobal("location", {
    hostname,
    origin: port ? `http://${hostname}:${port}` : `https://${hostname}`,
    port,
    pathname,
    search,
    hash,
    replace,
  });
}

function AuthState(): React.ReactElement {
  const { token, user } = useAuth();
  return (
    <div>
      <span data-testid="username">{user?.username ?? "none"}</span>
      <span data-testid="role">{user?.role ?? "none"}</span>
      <span data-testid="token">{token ?? "none"}</span>
    </div>
  );
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  vi.clearAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("Candidate auth bootstrap", () => {
  it("does not create an admin identity without an application login", () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin("candidate.ojeur.cloud", "");

    render(<AuthProvider><AuthState /></AuthProvider>);

    expect(screen.getByTestId("username").textContent).toBe("anonymous");
    expect(screen.getByTestId("role").textContent).toBe("viewer");
    expect(screen.getByTestId("token").textContent).toBe("none");
    expect(fetchAuthEndpoint).not.toHaveBeenCalled();
  });

  it("sends an unauthenticated Candidate route to the existing login page", async () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin("candidate.ojeur.cloud", "");

    render(
      <MemoryRouter initialEntries={["/"]}>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<div>JATO login page</div>} />
            <Route
              path="/"
              element={<RequireRole><div>Candidate app</div></RequireRole>}
            />
          </Routes>
        </AuthProvider>
      </MemoryRouter>,
    );

    expect(await screen.findByText("JATO login page")).toBeTruthy();
    expect(screen.queryByText("Candidate app")).toBeNull();
  });

  it("uses the username/password login page without unavailable Candidate OAuth", () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin("candidate.ojeur.cloud", "");

    render(
      <MemoryRouter initialEntries={["/login"]}>
        <AuthProvider><LoginPage /></AuthProvider>
      </MemoryRouter>,
    );

    expect(screen.getByLabelText("Username")).toBeTruthy();
    expect(screen.getByLabelText("Password")).toBeTruthy();
    expect(screen.queryByText("Continue with Google")).toBeNull();
  });

  it("keeps Google login visible on the Active origin", () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin("www.ojeur.cloud", "");

    render(
      <MemoryRouter initialEntries={["/login"]}>
        <AuthProvider><LoginPage /></AuthProvider>
      </MemoryRouter>,
    );

    expect(screen.getByText("Continue with Google")).toBeTruthy();
  });

  it("ignores unsupported OAuth callback credentials on Candidate", () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin(
      "candidate.ojeur.cloud",
      "",
      "?token=active-token&username=admin&role=admin",
    );

    render(<AuthProvider><AuthState /></AuthProvider>);

    expect(screen.getByTestId("username").textContent).toBe("anonymous");
    expect(screen.getByTestId("role").textContent).toBe("viewer");
    expect(screen.getByTestId("token").textContent).toBe("none");
    expect(localStorage.getItem("jato_auth_token")).toBeNull();
  });

  it("rejects Candidate OAuth credentials before providers can store them", () => {
    const replace = vi.fn();
    useOrigin(
      "candidate.ojeur.cloud",
      "",
      "?token=active-token&username=admin&role=admin&country=DK",
      "/product/order-genius",
      "#bom",
      replace,
    );

    render(<OAuthGate><div>Candidate app</div></OAuthGate>);

    expect(screen.queryByText("Candidate app")).toBeNull();
    expect(localStorage.getItem("jato_auth_token")).toBeNull();
    expect(localStorage.getItem("jato_user_name")).toBeNull();
    expect(localStorage.getItem("jato_user_role")).toBeNull();
    expect(replace).toHaveBeenCalledWith(
      "/product/order-genius?country=DK#bom",
    );
  });

  it("validates a stored Candidate session against auth/me immediately", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    localStorage.setItem("jato_auth_token", "candidate-token");
    localStorage.setItem("jato_user_name", "admin");
    localStorage.setItem("jato_user_role", "admin");
    vi.mocked(fetchAuthEndpoint).mockResolvedValue(Response.json({
      username: "admin",
      role: "admin",
      primaryCountry: null,
      secondaryCountries: [],
      preferredLandingPage: null,
      profileComplete: false,
    }));

    render(<AuthProvider><AuthState /></AuthProvider>);

    expect(screen.getByTestId("username").textContent).toBe("admin");
    expect(screen.getByTestId("role").textContent).toBe("admin");
    expect(screen.getByTestId("token").textContent).toBe("candidate-token");

    await waitFor(() => expect(fetchAuthEndpoint).toHaveBeenCalledTimes(1));
    const [, init] = vi.mocked(fetchAuthEndpoint).mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(headers.get("X-User-Name")).toBe("admin");
    expect(headers.get("X-Auth-Token")).toBe("candidate-token");
  });

  it("preserves Active stored identity and does not refresh without a token", () => {
    vi.stubEnv("VITE_AUTH_TOKEN", "");
    useOrigin("www.ojeur.cloud", "");
    localStorage.setItem("jato_user_name", "active-user");
    localStorage.setItem("jato_user_role", "editor");

    render(<AuthProvider><AuthState /></AuthProvider>);

    expect(screen.getByTestId("username").textContent).toBe("active-user");
    expect(screen.getByTestId("role").textContent).toBe("editor");
    expect(fetchAuthEndpoint).not.toHaveBeenCalled();
  });

  it("clears an invalid Candidate session instead of granting access", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    localStorage.setItem("jato_auth_token", "invalid-token");
    localStorage.setItem("jato_user_name", "admin");
    localStorage.setItem("jato_user_role", "admin");
    vi.mocked(fetchAuthEndpoint).mockResolvedValue(
      new Response("Unauthorized", { status: 401 }),
    );

    render(<AuthProvider><AuthState /></AuthProvider>);

    await waitFor(() => expect(fetchAuthEndpoint).toHaveBeenCalledTimes(1));
    await waitFor(() => {
      expect(screen.getByTestId("username").textContent).toBe("none");
    });
    expect(localStorage.getItem("jato_auth_token")).toBeNull();
    expect(screen.getByTestId("role").textContent).toBe("none");
    expect(screen.getByTestId("token").textContent).toBe("none");
  });
});
