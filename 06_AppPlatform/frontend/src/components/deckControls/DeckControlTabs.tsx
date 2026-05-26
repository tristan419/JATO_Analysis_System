import type { CSSProperties } from "react";

export interface DeckControlTabItem<Key extends string> {
  key: Key;
  label: string;
  caption: string;
}

interface DeckControlTabsProps<Key extends string> {
  tabs: Array<DeckControlTabItem<Key>>;
  activeKey: Key;
  onChange: (key: Key) => void;
  ariaLabel: string;
  className?: string;
  tabClassName?: string;
}

type DeckControlTabsStyle = CSSProperties & {
  "--deck-tab-count": number;
};

export function DeckControlTabs<Key extends string>({
  tabs,
  activeKey,
  onChange,
  ariaLabel,
  className,
  tabClassName,
}: DeckControlTabsProps<Key>) {
  const tabsClassName = ["deck-control-tabs", className].filter(Boolean).join(" ");
  const buttonClassName = ["deck-control-tab", tabClassName].filter(Boolean).join(" ");
  const style: DeckControlTabsStyle = { "--deck-tab-count": tabs.length };

  return (
    <div className={tabsClassName} role="tablist" aria-label={ariaLabel} style={style}>
      {tabs.map((tab) => {
        const active = tab.key === activeKey;
        return (
          <button
            key={tab.key}
            type="button"
            className={`${buttonClassName}${active ? " is-active" : ""}`}
            onClick={() => onChange(tab.key)}
            role="tab"
            aria-selected={active}
          >
            <span>{tab.label}</span>
            <small>{tab.caption}</small>
          </button>
        );
      })}
    </div>
  );
}
