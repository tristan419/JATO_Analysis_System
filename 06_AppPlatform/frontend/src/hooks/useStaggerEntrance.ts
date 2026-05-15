import { useEffect, useRef, type RefObject } from "react";
import { animate, stagger } from "animejs";

interface StaggerOptions {
  selector?: string;
  staggerDelay?: number;
  duration?: number;
  translateY?: number;
}

export function useStaggerEntrance(
  containerRef: RefObject<HTMLElement | null>,
  active: boolean,
  options: StaggerOptions = {},
) {
  const {
    selector = ".kpi-card",
    staggerDelay = 60,
    duration = 500,
    translateY: y = 16,
  } = options;

  const ranRef = useRef(false);

  useEffect(() => {
    if (!active) {
      ranRef.current = false;
      return;
    }
    if (!containerRef.current) return;

    const container = containerRef.current;

    // Retry up to 5 frames in case the children haven't rendered yet
    let retries = 0;
    let rafId = 0;

    function tryAnimate() {
      const targets = container.querySelectorAll(selector);
      if (targets.length === 0 && retries < 5) {
        retries++;
        rafId = requestAnimationFrame(tryAnimate);
        return;
      }
      if (targets.length === 0 || ranRef.current) return;

      ranRef.current = true;

      try {
        animate(targets, {
          translateY: [y, 0],
          opacity: [0, 1],
          delay: stagger(staggerDelay),
          duration,
          ease: "outExpo",
        });
      } catch {
        // Silently ignore — animation is decorative, never break the page
      }
    }

    rafId = requestAnimationFrame(tryAnimate);

    return () => {
      cancelAnimationFrame(rafId);
    };
  }, [active, containerRef, selector, staggerDelay, duration, y]);
}
