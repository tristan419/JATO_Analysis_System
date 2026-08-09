// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AuthProvider, useAuth } from "../../contexts/AuthContext";
import { fetchAuthEndpoint } from "../../utils/authFallback";

vi.mock("../../utils/authFallback", () => ({
  fetchAuthEndpoint: vi.fn(),
}));

function useOrigin(hostname: string, port: string, search = ""): void {
  vi.stubGlobal("location", {
    hostname,
    origin: port ? `http://${hostname}:${port}` : `https://${hostname}`,
    port,
    pathname: "/",
    search,
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
  it("starts as Candidate admin and refreshes auth/me without a token", async () => {
    useOrigin("candidate.ojeur.cloud", "");
    localStorage.setItem("jato_auth_token", "stale-active-token");
    localStorage.setItem("jato_user_name", "old-user");
    localStorage.setItem("jato_user_role", "viewer");
    vi.mocked(fetchAuthEndpoint).mockResolvedValue(Response.json({
      username: "candidate",
      role: "admin",
      primaryCountry: null,
      secondaryCountries: [],
      preferredLandingPage: null,
      profileComplete: false,
    }));

    render(<AuthProvider><AuthState /></AuthProvider>);

    expect(screen.getByTestId("username").textContent).toBe("candidate");
    expect(screen.getByTestId("role").textContent).toBe("admin");
    expect(screen.getByTestId("token").textContent).toBe("none");
    expect(localStorage.getItem("jato_auth_token")).toBeNull();

    await waitFor(() => expect(fetchAuthEndpoint).toHaveBeenCalledTimes(1));
    const [, init] = vi.mocked(fetchAuthEndpoint).mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(headers.get("X-User-Name")).toBe("candidate");
    expect(headers.has("X-Auth-Token")).toBe(false);
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

  it("ignores OAuth credentials on the Candidate origin", async () => {
    useOrigin("candidate.ojeur.cloud", "", "?token=active-token&username=active-user&role=viewer");
    const replaceState = vi.fn();
    vi.stubGlobal("history", { replaceState });
    vi.mocked(fetchAuthEndpoint).mockResolvedValue(Response.json({
      username: "candidate",
      role: "admin",
      primaryCountry: null,
      secondaryCountries: [],
      preferredLandingPage: null,
      profileComplete: false,
    }));

    render(<AuthProvider><AuthState /></AuthProvider>);

    await waitFor(() => expect(fetchAuthEndpoint).toHaveBeenCalledTimes(1));
    expect(localStorage.getItem("jato_auth_token")).toBeNull();
    expect(screen.getByTestId("username").textContent).toBe("candidate");
    expect(screen.getByTestId("role").textContent).toBe("admin");
    expect(replaceState).toHaveBeenCalledWith({}, "", "/");
  });
});
