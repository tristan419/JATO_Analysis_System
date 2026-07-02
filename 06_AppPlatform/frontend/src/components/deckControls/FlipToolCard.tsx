import type { CSSProperties, ReactNode } from "react";

interface FlipToolCardProps {
  flipped: boolean;
  front: ReactNode;
  back: ReactNode;
  ariaLabel?: string;
  height?: CSSProperties["height"];
  minHeight?: CSSProperties["minHeight"];
  className?: string;
  innerClassName?: string;
  frontClassName?: string;
  backClassName?: string;
  style?: CSSProperties;
  innerStyle?: CSSProperties;
  frontStyle?: CSSProperties;
  backStyle?: CSSProperties;
}

export function FlipToolCard({
  flipped,
  front,
  back,
  ariaLabel,
  height,
  minHeight = height,
  className,
  innerClassName,
  frontClassName,
  backClassName,
  style,
  innerStyle,
  frontStyle,
  backStyle,
}: FlipToolCardProps) {
  const cardClassName = ["deck-flip-card", className, flipped ? "is-flipped" : ""].filter(Boolean).join(" ");
  const innerClasses = ["deck-flip-inner", innerClassName].filter(Boolean).join(" ");
  const frontClasses = ["deck-flip-face", "deck-flip-front", frontClassName].filter(Boolean).join(" ");
  const backClasses = ["deck-flip-face", "deck-flip-back", backClassName].filter(Boolean).join(" ");
  const sizeStyle: CSSProperties = { height, minHeight };

  return (
    <article
      className={cardClassName}
      aria-label={ariaLabel}
      style={{ ...sizeStyle, ...style }}
    >
      <div className={innerClasses} style={{ ...sizeStyle, ...innerStyle }}>
        <section
          className={frontClasses}
          aria-hidden={flipped}
          inert={flipped}
          style={frontStyle}
        >
          {front}
        </section>
        <section
          className={backClasses}
          aria-hidden={!flipped}
          inert={!flipped}
          style={backStyle}
        >
          {back}
        </section>
      </div>
    </article>
  );
}
