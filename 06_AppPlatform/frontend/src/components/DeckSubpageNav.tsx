import { useEffect, useMemo } from "react";

import {
  getAdjacentKeyedItem,
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
  const previousItem = useMemo(
    () => getAdjacentKeyedItem(items, activeKey, -1),
    [activeKey, items],
  );
  const nextItem = useMemo(
    () => getAdjacentKeyedItem(items, activeKey, 1),
    [activeKey, items],
  );

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
      const target = direction < 0 ? previousItem : nextItem;
      if (!target) {
        return;
      }
      event.preventDefault();
      onSelect(target.key);
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [nextItem, onSelect, previousItem]);

  return (
    <div className="deck-subpage-nav">
      <button
        type="button"
        className="deck-subpage-step"
        onClick={() => previousItem && onSelect(previousItem.key)}
        disabled={!previousItem}
        aria-label={previousItem ? `上一页：${previousItem.code} ${previousItem.label}` : "已经是第一页"}
      >
        <span className="deck-subpage-step-arrow">←</span>
        <span className="deck-subpage-step-copy">
          <span className="deck-subpage-step-meta">上一页</span>
          <strong>{previousItem?.sublabel ?? "Start"}</strong>
        </span>
      </button>
      <nav className={tabsClassName} aria-label={ariaLabel}>
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
      <button
        type="button"
        className="deck-subpage-step deck-subpage-step--next"
        onClick={() => nextItem && onSelect(nextItem.key)}
        disabled={!nextItem}
        aria-label={nextItem ? `下一页：${nextItem.code} ${nextItem.label}` : "已经是最后一页"}
      >
        <span className="deck-subpage-step-copy">
          <span className="deck-subpage-step-meta">下一页</span>
          <strong>{nextItem?.sublabel ?? "End"}</strong>
        </span>
        <span className="deck-subpage-step-arrow">→</span>
      </button>
    </div>
  );
}
