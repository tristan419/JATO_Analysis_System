import { useEffect, useState } from "react";

function getStepDelay(target: number) {
  if (target <= 1) {
    return 320;
  }
  return Math.max(120, Math.round(880 / target));
}

export function LoopingCountStrip({
  current,
  total,
  label,
  meta,
  pauseMs = 2000,
}: {
  current: number;
  total: number;
  label: string;
  meta: string;
  pauseMs?: number;
}) {
  const safeCurrent = Math.max(0, Math.trunc(current));
  const safeTotal = Math.max(0, Math.trunc(total));
  const [displayCurrent, setDisplayCurrent] = useState(0);

  useEffect(() => {
    setDisplayCurrent(0);
  }, [safeCurrent]);

  useEffect(() => {
    if (safeCurrent <= 0) {
      setDisplayCurrent(0);
      return undefined;
    }

    if (displayCurrent < safeCurrent) {
      const timer = window.setTimeout(() => {
        setDisplayCurrent((value) => Math.min(value + 1, safeCurrent));
      }, getStepDelay(safeCurrent));
      return () => window.clearTimeout(timer);
    }

    const timer = window.setTimeout(() => {
      setDisplayCurrent(0);
    }, pauseMs);
    return () => window.clearTimeout(timer);
  }, [displayCurrent, pauseMs, safeCurrent]);

  return (
    <div className="review-toolbar-kpi-strip review-toolbar-kpi-strip--centered">
      <span className="review-toolbar-kpi-label">{label}</span>
      <strong className="review-toolbar-kpi-value review-toolbar-kpi-value--animated">
        <span>{displayCurrent}</span>
        <span className="review-toolbar-kpi-divider">/</span>
        <span>{safeTotal}</span>
      </strong>
      <span className="review-toolbar-kpi-meta">{meta}</span>
    </div>
  );
}