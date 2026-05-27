import { useState, type FormEvent } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";

export function LoginPage() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const redirect = searchParams.get("redirect") || "/";

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (!username.trim() || !password) return;
    setError("");
    setSubmitting(true);
    try {
      await login(username.trim(), password);
      navigate(redirect, { replace: true });
    } catch (err) {
      setError((err as Error).message || "Login failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleOAuth(provider: "google") {
    try {
      const res = await fetch(
        `/v1/auth/${provider}/auth-url?redirect=${encodeURIComponent(redirect)}`,
      );
      if (!res.ok) throw new Error("Not available");
      const { url } = await res.json();
      window.location.href = url;
    } catch {
      setError(`${provider} sign in unavailable`);
    }
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "#ffffff",
        fontFamily:
          'BMWTypeNextLatin, Helvetica, Arial, "PingFang SC", "Microsoft YaHei", sans-serif',
        color: "#262626",
      }}
    >
      <form
        onSubmit={handleSubmit}
        style={{
          width: 400,
          padding: "40px",
          border: "1px solid #e5e7eb",
          borderRadius: 0,
          background: "#ffffff",
        }}
      >
        {/* Brand */}
        <div style={{ marginBottom: 32 }}>
          <span
            style={{
              fontSize: 12,
              fontWeight: 700,
              letterSpacing: "0.08em",
              color: "#1c69d4",
              textTransform: "uppercase",
            }}
          >
            JATO Analysis System
          </span>
          <h1
            style={{
              fontSize: 32,
              fontWeight: 400,
              color: "#262626",
              margin: "4px 0 0",
              lineHeight: 1.3,
            }}
          >
            Sign in
          </h1>
          <p style={{ fontSize: 14, color: "#757575", margin: "4px 0 0" }}>
            Market Intelligence Control Deck
          </p>
        </div>

        {/* Error */}
        {error && (
          <div
            style={{
              padding: "8px 16px",
              background: "#fef2f2",
              border: "1px solid #fecaca",
              borderRadius: 0,
              color: "#dc2626",
              fontSize: 13,
              marginBottom: 16,
            }}
          >
            {error}
          </div>
        )}

        {/* Username */}
        <label style={{ display: "block", marginBottom: 16 }}>
          <span
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: "#262626",
              display: "block",
              marginBottom: 4,
            }}
          >
            Username
          </span>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus
            autoComplete="username"
            style={{
              width: "100%",
              padding: "8px 12px",
              borderRadius: 0,
              border: "1px solid #d1d5db",
              background: "#ffffff",
              color: "#262626",
              fontSize: 16,
              fontWeight: 400,
              outline: "none",
              boxSizing: "border-box",
              lineHeight: 1.15,
            }}
          />
        </label>

        {/* Password */}
        <label style={{ display: "block", marginBottom: 24 }}>
          <span
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: "#262626",
              display: "block",
              marginBottom: 4,
            }}
          >
            Password
          </span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            style={{
              width: "100%",
              padding: "8px 12px",
              borderRadius: 0,
              border: "1px solid #d1d5db",
              background: "#ffffff",
              color: "#262626",
              fontSize: 16,
              fontWeight: 400,
              outline: "none",
              boxSizing: "border-box",
              lineHeight: 1.15,
            }}
          />
        </label>

        {/* Submit */}
        <button
          type="submit"
          disabled={submitting}
          style={{
            width: "100%",
            padding: "10px 0",
            borderRadius: 0,
            border: 0,
            background: submitting ? "#94a3b8" : "#1c69d4",
            color: "#ffffff",
            fontSize: 16,
            fontWeight: 700,
            cursor: submitting ? "default" : "pointer",
            lineHeight: 1.2,
            textTransform: "uppercase",
            marginBottom: 16,
          }}
        >
          {submitting ? "Signing in..." : "Sign in"}
        </button>

        {/* Divider */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            marginBottom: 16,
          }}
        >
          <div style={{ flex: 1, height: 1, background: "#e5e7eb" }} />
          <span style={{ fontSize: 12, color: "#757575" }}>or</span>
          <div style={{ flex: 1, height: 1, background: "#e5e7eb" }} />
        </div>

        {/* Google */}
        <button
          type="button"
          onClick={() => handleOAuth("google")}
          style={{
            width: "100%",
            padding: "10px 0",
            borderRadius: 0,
            border: "1px solid #d1d5db",
            background: "#ffffff",
            color: "#262626",
            fontSize: 16,
            fontWeight: 400,
            cursor: "pointer",
            lineHeight: 1.15,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 8,
          }}
        >
          <span style={{ color: "#4285f4", fontWeight: 700 }}>G</span>
          Continue with Google
        </button>
      </form>
    </div>
  );
}
