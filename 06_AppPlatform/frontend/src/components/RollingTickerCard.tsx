import { useEffect, useMemo, useState } from "react";

const DEFAULT_ITEM_HEIGHT_PX = 16;
const REEL_ONLY_ITEM_HEIGHT_PX = 32;

function normalizeItems(items: string[], emptyLabel: string) {
  const uniqueItems = Array.from(
    new Set(items.map((item) => item.trim()).filter(Boolean)),
  );
  return uniqueItems.length > 0 ? uniqueItems : [emptyLabel];
}

export function RollingTickerCard({
  emptyLabel = "No active items",
  items,
  pauseMs = 1000,
  title,
  variant = "default",
}: {
  emptyLabel?: string;
  items: string[];
  pauseMs?: number;
  title: string;
  variant?: "default" | "reel-only";
}) {
  const itemHeightPx = variant === "reel-only"
    ? REEL_ONLY_ITEM_HEIGHT_PX
    : DEFAULT_ITEM_HEIGHT_PX;
  const normalizedItems = useMemo(
    () => normalizeItems(items, emptyLabel),
    [emptyLabel, items],
  );
  const [activeIndex, setActiveIndex] = useState(0);
  const [phase, setPhase] = useState<"idle" | "sliding">("idle");
  const [offsetPx, setOffsetPx] = useState(0);
  const [trackItems, setTrackItems] = useState<string[]>(() => [normalizedItems[0]]);

  useEffect(() => {
    setActiveIndex(0);
    setPhase("idle");
    setOffsetPx(0);
    setTrackItems([normalizedItems[0]]);
  }, [normalizedItems]);

  useEffect(() => {
    if (normalizedItems.length <= 1 || phase !== "idle") {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      const nextIndex = (activeIndex + 1) % normalizedItems.length;
      setTrackItems([
        normalizedItems[activeIndex],
        normalizedItems[nextIndex],
      ]);
      setOffsetPx(0);

      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          setPhase("sliding");
          setOffsetPx(itemHeightPx);
        });
      });
    }, pauseMs);

    return () => window.clearTimeout(timer);
  }, [activeIndex, itemHeightPx, normalizedItems, pauseMs, phase]);

  function handleTransitionEnd() {
    if (phase !== "sliding" || trackItems.length < 2 || normalizedItems.length <= 1) {
      return;
    }

    const finalItem = trackItems[trackItems.length - 1];
    const finalIndex = normalizedItems.indexOf(finalItem);
    setActiveIndex(finalIndex >= 0 ? finalIndex : 0);
    setPhase("idle");
    setOffsetPx(0);
    setTrackItems([finalItem]);
  }

  const visibleTrackItems = trackItems.length > 0
    ? trackItems
    : [normalizedItems[activeIndex % normalizedItems.length]];

  return (
    <div className={`rolling-ticker-card${variant === "reel-only" ? " is-reel-only" : ""}`}>
      {variant !== "reel-only" && (
        <div className="rolling-ticker-head">
          <span className="rolling-ticker-label">{title}</span>
          <span className="rolling-ticker-count">{normalizedItems.length}</span>
        </div>
      )}
      <div className="rolling-ticker-window" aria-label={title}>
        <div
          className={`rolling-ticker-track${phase === "sliding" ? " is-sliding" : ""}`}
          onTransitionEnd={handleTransitionEnd}
          style={{
            transform: `translateY(-${offsetPx}px)`,
            transitionDuration: phase === "idle" ? "0ms" : "460ms",
            transitionTimingFunction: "cubic-bezier(0.22, 1, 0.36, 1)",
          }}
        >
          {visibleTrackItems.map((item, index) => (
            <span key={`${item}-${index}`} className="rolling-ticker-item">
              {item}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}