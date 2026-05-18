import { useEffect, useRef } from "react";

import {
  getHorizontalNavigationDirectionFromKey,
  shouldIgnorePageNavigationTarget,
} from "../utils/pageNavigation";

type DeckSubpageItem<Key extends string> = {
  key: Key;
  code: string;
  label: string;
  sublabel: string;
};

interface DeckSubpageNavProps<Key extends string> {
  items: DeckSubpageItem<Key>[];
  activeKey: Key;
  onSelect: (key: Key) => void;
  ariaLabel: string;
  tabsClassName: string;
}

export function DeckSubpageNav<Key extends string>({
  items,
  activeKey,
  onSelect,
  ariaLabel,
  tabsClassName,
}: DeckSubpageNavProps<Key>) {
  const stripRef = useRef<HTMLDivElement>(null);

  // Auto-scroll active tab into view on change
  useEffect(() => {
    const strip = stripRef.current;
    if (!strip) return;
    const activeButton = strip.querySelector<HTMLButtonElement>(
      ".market-scan-tab.is-active",
    );
    if (activeButton) {
      activeButton.scrollIntoView({ inline: "center", behavior: "smooth" });
    }
  }, [activeKey]);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        event.defaultPrevented
        || event.altKey
        || event.ctrlKey
        || event.metaKey
        || event.shiftKey
        || event.repeat
      ) {
        return;
      }
      const direction = getHorizontalNavigationDirectionFromKey(event.key);
      if (direction === null || shouldIgnorePageNavigationTarget(event.target)) {
        return;
      }
      const activeIndex = items.findIndex((item) => item.key === activeKey);
      if (activeIndex === -1) return;
      const targetIndex = activeIndex + (direction < 0 ? -1 : 1);
      if (targetIndex < 0 || targetIndex >= items.length) return;
      event.preventDefault();
      onSelect(items[targetIndex].key);
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activeKey, items, onSelect]);

  return (
    <nav className={tabsClassName} aria-label={ariaLabel} ref={stripRef}>
      {items.map((item) => (
        <button
          key={item.key}
          type="button"
          className={`market-scan-tab${activeKey === item.key ? " is-active" : ""}`}
          onClick={() => onSelect(item.key)}
        >
          <span className="market-scan-tab-code">{item.code}</span>
          <span className="market-scan-tab-copy">
            <strong>{item.label}</strong>
            <span>{item.sublabel}</span>
          </span>
        </button>
      ))}
    </nav>
  );
}
