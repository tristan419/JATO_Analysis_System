import type { ReactNode } from "react";

import { LoadingSurface } from "./LoadingSurface";

export type PageBannerTone = "error" | "warning" | "info" | "success";

export interface PageBannerItem {
  id?: string;
  tone: PageBannerTone;
  title?: ReactNode;
  message: ReactNode;
  action?: ReactNode;
}

interface PageLoadingShellProps {
  label: string;
  detail?: string;
  kicker?: string;
}

interface PageBannerStackProps {
  items: PageBannerItem[];
}

function hasBannerMessage(item: PageBannerItem) {
  return item.message !== null && item.message !== undefined && item.message !== false;
}

export function PageLoadingShell({ label, detail, kicker }: PageLoadingShellProps) {
  return (
    <div className="page-loading-shell">
      <LoadingSurface mode="overlay" label={label} detail={detail} kicker={kicker} />
    </div>
  );
}

export function PageBannerStack({ items }: PageBannerStackProps) {
  const visibleItems = items.filter(hasBannerMessage);
  if (!visibleItems.length) {
    return null;
  }

  return (
    <div className="page-banner-stack" role="status" aria-live="polite">
      {visibleItems.map((item, index) => (
        <section key={item.id ?? index} className={`page-banner page-banner--${item.tone}`}>
          <div className="page-banner-copy">
            {item.title ? <strong className="page-banner-title">{item.title}</strong> : null}
            <div className="page-banner-message">{item.message}</div>
          </div>
          {item.action ? <div className="page-banner-action">{item.action}</div> : null}
        </section>
      ))}
    </div>
  );
}
