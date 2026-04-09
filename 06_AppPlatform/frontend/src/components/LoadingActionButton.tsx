import type { ButtonHTMLAttributes, ReactNode } from "react";

type ButtonVariant = "primary" | "secondary" | "accent" | "ghost" | "danger";
type ButtonSize = "default" | "sm";

interface LoadingActionButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  children: ReactNode;
  loading?: boolean;
  loadingLabel?: ReactNode;
  variant?: ButtonVariant;
  size?: ButtonSize;
}

export function LoadingActionButton({
  children,
  loading = false,
  loadingLabel,
  variant = "primary",
  size = "default",
  className,
  disabled,
  type = "button",
  ...rest
}: LoadingActionButtonProps) {
  const classes = [
    "btn",
    `btn-${variant}`,
    size === "sm" ? "btn-sm" : "",
    "btn-liquid",
    loading ? "is-loading" : "",
    className ?? "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <button
      {...rest}
      type={type}
      className={classes}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
    >
      <span className="btn-liquid-label">{loading ? loadingLabel ?? children : children}</span>
      {loading ? <span className="btn-liquid-loader" aria-hidden="true" /> : null}
    </button>
  );
}