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

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "#0f172a",
        fontFamily: "system-ui, -apple-system, sans-serif",
      }}
    >
      <form
        onSubmit={handleSubmit}
        style={{
          width: 360,
          padding: "36px 32px",
          borderRadius: 16,
          background: "rgba(30,41,59,0.8)",
          backdropFilter: "blur(20px)",
          border: "1px solid rgba(255,255,255,0.06)",
          boxShadow: "0 4px 32px rgba(0,0,0,0.3)",
        }}
      >
        <h1
          style={{
            fontSize: 20,
            fontWeight: 700,
            color: "#f1f5f9",
            margin: "0 0 4px",
          }}
        >
          JATO Control Deck
        </h1>
        <p style={{ fontSize: 12, color: "#64748b", margin: "0 0 24px" }}>
          Sign in to your account
        </p>

        {error && (
          <div
            style={{
              padding: "8px 12px",
              borderRadius: 8,
              background: "rgba(239,68,68,0.12)",
              color: "#fca5a5",
              fontSize: 12,
              marginBottom: 16,
            }}
          >
            {error}
          </div>
        )}

        <label style={{ display: "block", marginBottom: 16 }}>
          <span style={{ fontSize: 11, color: "#94a3b8", display: "block", marginBottom: 4 }}>
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
              borderRadius: 8,
              border: "1px solid rgba(255,255,255,0.08)",
              background: "rgba(15,23,42,0.6)",
              color: "#e2e8f0",
              fontSize: 14,
              outline: "none",
              boxSizing: "border-box",
            }}
          />
        </label>

        <label style={{ display: "block", marginBottom: 20 }}>
          <span style={{ fontSize: 11, color: "#94a3b8", display: "block", marginBottom: 4 }}>
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
              borderRadius: 8,
              border: "1px solid rgba(255,255,255,0.08)",
              background: "rgba(15,23,42,0.6)",
              color: "#e2e8f0",
              fontSize: 14,
              outline: "none",
              boxSizing: "border-box",
            }}
          />
        </label>

        <button
          type="submit"
          disabled={submitting}
          style={{
            width: "100%",
            padding: "10px 0",
            borderRadius: 10,
            border: 0,
            background: submitting ? "#334155" : "#3b82f6",
            color: "#fff",
            fontSize: 14,
            fontWeight: 600,
            cursor: submitting ? "default" : "pointer",
          }}
        >
          {submitting ? "Signing in..." : "Sign in"}
        </button>

        <div style={{ display: "flex", alignItems: "center", gap: 12, margin: "16px 0" }}>
          <div style={{ flex: 1, height: 1, background: "rgba(255,255,255,0.06)" }} />
          <span style={{ fontSize: 10, color: "#475569" }}>OR</span>
          <div style={{ flex: 1, height: 1, background: "rgba(255,255,255,0.06)" }} />
        </div>

        <button
          type="button"
          onClick={async () => {
            try {
              const res = await fetch("/v1/auth/feishu/auth-url?redirect=" + encodeURIComponent(redirect));
              if (!res.ok) throw new Error("Feishu not available");
              const { url } = await res.json();
              window.location.href = url;
            } catch {
              setError("飞书登录暂不可用");
            }
          }}
          style={{
            width: "100%",
            padding: "10px 0",
            borderRadius: 10,
            border: "1px solid rgba(255,255,255,0.08)",
            background: "rgba(255,255,255,0.04)",
            color: "#e2e8f0",
            fontSize: 14,
            fontWeight: 500,
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 8,
          }}
        >
          <span style={{ color: "#3370ff", fontSize: 16 }}>▷</span>
          飞书登录
        </button>

        <button
          type="button"
          onClick={async () => {
            try {
              const res = await fetch("/v1/auth/google/auth-url?redirect=" + encodeURIComponent(redirect));
              if (!res.ok) throw new Error("Google not available");
              const { url } = await res.json();
              window.location.href = url;
            } catch {
              setError("Google sign in unavailable");
            }
          }}
          style={{
            width: "100%",
            padding: "10px 0",
            borderRadius: 10,
            border: "1px solid rgba(255,255,255,0.08)",
            background: "rgba(255,255,255,0.04)",
            color: "#e2e8f0",
            fontSize: 14,
            fontWeight: 500,
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 8,
            marginTop: 8,
          }}
        >
          <span style={{ color: "#4285f4", fontWeight: 700, fontSize: 15 }}>G</span>
          Sign in with Google
        </button>
      </form>
    </div>
  );
}
