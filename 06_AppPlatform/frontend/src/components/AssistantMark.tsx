import type { CSSProperties } from "react";

interface AssistantMarkProps {
  active?: boolean;
  size?: number;
}

export function AssistantMark({ active = false, size = 64 }: AssistantMarkProps) {
  const wrapperStyle: CSSProperties = {
    width: size,
    height: size,
  };

  return (
    <div
      className={`assistant-mark${active ? " is-active" : ""}`}
      style={wrapperStyle}
      aria-hidden
    >
      <svg
        viewBox="0 0 72 72"
        width={size}
        height={size}
        role="presentation"
        focusable="false"
      >
        <defs>
          <linearGradient id="assistant-mark-shell" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="#173d68" />
            <stop offset="100%" stopColor="#0f2440" />
          </linearGradient>
          <linearGradient id="assistant-mark-core" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="#60a5fa" />
            <stop offset="100%" stopColor="#22c55e" />
          </linearGradient>
        </defs>
        <circle cx="36" cy="36" r="32" fill="rgba(15, 23, 42, 0.12)" />
        <rect x="10" y="10" width="52" height="52" rx="18" fill="url(#assistant-mark-shell)" />
        <rect className="assistant-mark__glow" x="19" y="19" width="34" height="34" rx="12" fill="rgba(255,255,255,0.08)" />
        <path
          className="assistant-mark__beam"
          d="M24 45 L31 28 L36 40 L42 24 L49 45"
          fill="none"
          stroke="url(#assistant-mark-core)"
          strokeWidth="4.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <circle cx="24" cy="45" r="2.5" fill="#93c5fd" />
        <circle cx="49" cy="45" r="2.5" fill="#86efac" />
      </svg>
    </div>
  );
}
