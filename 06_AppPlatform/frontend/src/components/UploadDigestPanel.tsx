import type { ReactNode } from "react";

export interface UploadDigestMetric {
  label: string;
  value: ReactNode;
  tone?: "neutral" | "success" | "warning" | "danger";
}

interface UploadDigestPanelProps {
  title: string;
  subtitle?: string;
  metrics: UploadDigestMetric[];
  warnings?: string[];
  errors?: string[];
  children?: ReactNode;
  footer?: ReactNode;
}

export function UploadDigestPanel({
  title,
  subtitle,
  metrics,
  warnings = [],
  errors = [],
  children,
  footer,
}: UploadDigestPanelProps) {
  return (
    <section className="upload-digest-panel">
      <header className="upload-digest-head">
        <div>
          <span className="upload-digest-eyebrow">Upload Digest</span>
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </header>
      <div className="upload-digest-metrics">
        {metrics.map((metric) => (
          <div
            key={metric.label}
            className={`upload-digest-metric upload-digest-metric-${metric.tone ?? "neutral"}`}
          >
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </div>
        ))}
      </div>
      {errors.length > 0 ? (
        <div className="upload-digest-alert upload-digest-alert-danger">
          {errors.slice(0, 20).map((error, index) => <div key={`${error}-${index}`}>{error}</div>)}
        </div>
      ) : null}
      {warnings.length > 0 ? (
        <div className="upload-digest-alert upload-digest-alert-warning">
          {warnings.slice(0, 20).map((warning, index) => <div key={`${warning}-${index}`}>{warning}</div>)}
        </div>
      ) : null}
      {children ? <div className="upload-digest-body">{children}</div> : null}
      {footer ? <footer className="upload-digest-footer">{footer}</footer> : null}
    </section>
  );
}
