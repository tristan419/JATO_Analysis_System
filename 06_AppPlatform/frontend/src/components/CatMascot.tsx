import type { CSSProperties } from "react";

interface CatMascotProps {
  /** When true the chat panel is open — cat shows curious face */
  chatOpen?: boolean;
  size?: number;
}

export type CatMascotMode = "idle" | "curious";

export function resolveCatMascotMode(chatOpen: boolean): CatMascotMode {
  return chatOpen ? "curious" : "idle";
}

export function CatMascot({ chatOpen = false, size = 64 }: CatMascotProps) {
  const mode = resolveCatMascotMode(chatOpen);
  const wrapperStyle: CSSProperties = {
    width: size,
    height: size,
  };

  return (
    <div
      className={`cat-mascot cat-mascot--${mode}`}
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
          <linearGradient id="cat-body-gradient" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="#24537d" />
            <stop offset="100%" stopColor="#17395a" />
          </linearGradient>
        </defs>
        <ellipse cx="36" cy="63" rx="18" ry="5" fill="rgba(15, 23, 42, 0.14)" />
        <path
          className="cat-tail"
          d="M54 50 C63 42, 65 31, 58 25 C54 22, 51 25, 52 30 C55 37, 55 43, 48 49"
          fill="none"
          stroke="#163754"
          strokeWidth="4"
          strokeLinecap="round"
        />
        <ellipse className="cat-body" cx="37" cy="46" rx="16" ry="14" fill="url(#cat-body-gradient)" />
        <ellipse cx="31" cy="45" rx="4.5" ry="6" fill="rgba(255, 255, 255, 0.08)" />
        <g className="cat-head">
          <path className="cat-ear--left" d="M24 30 L30 17 L35 30 Z" fill="#17395a" />
          <path className="cat-ear-inner--left" d="M27 29 L30 21 L33 29 Z" fill="#f1a7a1" />
          <path className="cat-ear--right" d="M37 30 L42 17 L48 30 Z" fill="#17395a" />
          <path className="cat-ear-inner--right" d="M39 29 L42 21 L45 29 Z" fill="#f1a7a1" />
          <circle cx="36" cy="33" r="13" fill="url(#cat-body-gradient)" />
          <ellipse cx="31" cy="38" rx={chatOpen ? 3.8 : 3.2} ry={chatOpen ? 4.2 : 3.5} fill="#ffffff" />
          <ellipse cx="41" cy="38" rx={chatOpen ? 3.8 : 3.2} ry={chatOpen ? 4.2 : 3.5} fill="#ffffff" />
          <circle cx="31" cy="38" r={chatOpen ? 2.1 : 1.7} fill="#101828" />
          <circle cx="41" cy="38" r={chatOpen ? 2.1 : 1.7} fill="#101828" />
          <circle cx="32" cy="37" r="0.8" fill="#ffffff" />
          <circle cx="42" cy="37" r="0.8" fill="#ffffff" />
          <path d="M34.5 42.5 Q36 43.7 37.5 42.5" fill="none" stroke="#0f172a" strokeWidth="1.5" strokeLinecap="round" />
          <path d="M36 42.8 L36 45.2" fill="none" stroke="#0f172a" strokeWidth="1.4" strokeLinecap="round" />
          <ellipse className="cat-yawn-mouth" cx="36" cy="46.5" rx="0" ry="0" fill="#f97316" opacity="0.85" />
          <g className="cat-whiskers" stroke="#0f172a" strokeWidth="1.2" strokeLinecap="round">
            <path d="M24 42 L30 41" />
            <path d="M24.5 45 L30.5 44.6" />
            <path d="M42 41 L48 42" />
            <path d="M41.5 44.6 L47.5 45" />
          </g>
        </g>
        <circle className="cat-yarn" cx="18" cy="58" r="4.5" fill="#38bdf8" opacity="0.85" />
        <path d="M18 53.5 C21 51, 23 49, 26 49" fill="none" stroke="#38bdf8" strokeWidth="1.4" strokeLinecap="round" opacity="0.8" />
      </svg>
    </div>
  );
}
