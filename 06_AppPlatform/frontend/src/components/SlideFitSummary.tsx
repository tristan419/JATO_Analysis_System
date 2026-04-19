import type { SlideFitAssessment } from "../utils/slideFit";

const STATUS_LABEL: Record<SlideFitAssessment["status"], string> = {
  safe: "Fit Safe",
  compress: "Need Trim",
  split: "Need Split",
};

export function SlideFitSummary({ assessment }: { assessment: SlideFitAssessment }) {
  return (
    <section className={`slide-fit-summary slide-fit-summary--${assessment.status}`}>
      <div className="slide-fit-summary-head">
        <span className={`slide-fit-badge slide-fit-badge--${assessment.status}`}>
          {STATUS_LABEL[assessment.status]}
        </span>
        <span className="slide-fit-summary-text">{assessment.summary}</span>
      </div>
      {assessment.recommendedActions.length > 0 ? (
        <div className="slide-fit-actions">
          {assessment.recommendedActions.slice(0, 3).map((action) => (
            <span key={action} className="market-scan-toolbar-chip slide-fit-action-chip">
              {action}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}
