type LoadingSurfaceMode = "banner" | "overlay" | "inline";

interface LoadingSurfaceProps {
  label: string;
  detail?: string;
  mode?: LoadingSurfaceMode;
  kicker?: string;
  className?: string;
}

export function LoadingSurface({
  label,
  detail,
  mode = "banner",
  kicker,
  className,
}: LoadingSurfaceProps) {
  const classes = ["loading-surface", `loading-surface-${mode}`, className]
    .filter(Boolean)
    .join(" ");
  const effectiveKicker = kicker ?? (mode === "inline" ? "Refreshing" : "Loading");

  return (
    <div className={classes} role="status" aria-live="polite">
      <div className="loading-surface-orb" aria-hidden="true">
        <span className="loading-surface-orb-fill" />
        <span className="loading-surface-orb-core" />
      </div>
      <div className="loading-surface-copy">
        <span className="loading-surface-kicker">{effectiveKicker}</span>
        <strong className="loading-surface-label">{label}</strong>
        {detail ? <span className="loading-surface-detail">{detail}</span> : null}
      </div>
    </div>
  );
}