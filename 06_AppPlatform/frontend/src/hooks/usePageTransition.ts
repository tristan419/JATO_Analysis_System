import { useEffect, useRef } from "react";
import { animate } from "animejs";

/**
 * Fades in the target element whenever `trigger` changes.
 * Uses requestAnimationFrame to ensure the DOM has the new content before animating.
 *
 * Typical use: animate a tab content area when the active tab changes.
 *
 * Network/server note: zero additional bytes — anime.js is already in the vendor bundle.
 * The animation runs entirely on the GPU (opacity + transform), no layout thrashing.
 */
export function usePageTransition(
  trigger: string,
  selector: string,
  duration = 280,
) {
  const prevRef = useRef(trigger);
  const mountedRef = useRef(false);

  useEffect(() => {
    // Skip the initial mount — only animate on subsequent changes
    if (!mountedRef.current) {
      mountedRef.current = true;
      return;
    }
    if (prevRef.current === trigger) return;
    prevRef.current = trigger;

    // Defer to the next frame so the DOM has the new content
    const rafId = requestAnimationFrame(() => {
      const el = document.querySelector(selector);
      if (!el) return;

      try {
        animate(el as HTMLElement, {
          opacity: [0, 1],
          duration,
          ease: "outQuad",
        });
      } catch {
        /* decorative only — never break the page */
      }
    });

    return () => cancelAnimationFrame(rafId);
  }, [trigger, selector, duration]);
}
