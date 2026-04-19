import { useEffect, useMemo, useState } from "react";

import { buildCountryChatLoadingPlan } from "../contexts/countryChatHelpers";

export function CountryChatPendingMessage({
  question,
  compact = false,
}: {
  question: string;
  compact?: boolean;
}) {
  const plan = useMemo(
    () => buildCountryChatLoadingPlan(question),
    [question],
  );
  const [activeStep, setActiveStep] = useState(0);

  useEffect(() => {
    setActiveStep(0);
    if (plan.steps.length < 2) {
      return undefined;
    }
    const timerId = window.setInterval(() => {
      setActiveStep((current) => Math.min(current + 1, plan.steps.length - 1));
    }, compact ? 1200 : 1500);
    return () => window.clearInterval(timerId);
  }, [compact, plan.steps]);

  return (
    <div className={`copilot-loading${compact ? " is-compact" : ""}`}>
      <div className="copilot-loading-kicker">{plan.label}</div>
      <div className="copilot-loading-current">{plan.steps[activeStep] ?? plan.label}</div>
      <div className="copilot-loading-steps">
        {plan.steps.map((step, index) => (
          <span
            key={step}
            className={[
              "copilot-loading-step",
              index < activeStep ? "is-done" : "",
              index === activeStep ? "is-active" : "",
            ].filter(Boolean).join(" ")}
          >
            {index + 1}. {step}
          </span>
        ))}
      </div>
    </div>
  );
}
