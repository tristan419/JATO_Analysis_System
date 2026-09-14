import { useEffect, useState } from "react";

interface AstrBotNativeFrameProps {
  src: string;
  title: string;
}

export function AstrBotNativeFrame({ src, title }: AstrBotNativeFrameProps) {
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    setLoaded(false);
  }, [src]);

  return (
    <div className="astrbot-native-frame-shell" aria-label={title}>
      {!loaded ? (
        <div className="astrbot-native-loading" role="status" aria-live="polite">
          <span className="astrbot-loading-dot" aria-hidden="true" />
          <span>Loading AstrBot console</span>
        </div>
      ) : null}
      <iframe
        key={src}
        title={title}
        className="astrbot-native-frame"
        src={src}
        onLoad={() => setLoaded(true)}
        allow="clipboard-read; clipboard-write"
      />
    </div>
  );
}
