import { useCallback, useEffect, useMemo, useState, type CSSProperties, type RefCallback } from "react";

type FixedCanvasPreviewOptions = {
  width: number;
  height: number;
  exporting: boolean;
};

type FixedCanvasPreviewState = {
  shellRef: RefCallback<HTMLDivElement>;
  scale: number;
  scaleBoxStyle: CSSProperties;
  frameStyle: CSSProperties;
};

function roundPreviewScale(value: number): number {
  return Math.round(value * 10000) / 10000;
}

export function useFixedCanvasPreview({
  width,
  height,
  exporting,
}: FixedCanvasPreviewOptions): FixedCanvasPreviewState {
  const [shellElement, setShellElement] = useState<HTMLDivElement | null>(null);
  const [scale, setScale] = useState(1);
  const shellRef = useCallback((node: HTMLDivElement | null) => {
    setShellElement(node);
  }, []);

  useEffect(() => {
    if (exporting) {
      return;
    }
    if (!shellElement || width <= 0) {
      return;
    }

    const updateScale = () => {
      const availableWidth = shellElement.clientWidth;
      const nextScale = availableWidth > 0 ? Math.min(1, availableWidth / width) : 1;
      const normalizedScale = roundPreviewScale(Number.isFinite(nextScale) ? nextScale : 1);
      setScale((current) => (Math.abs(current - normalizedScale) > 0.0001 ? normalizedScale : current));
    };

    updateScale();
    const resizeObserver = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(updateScale);
    resizeObserver?.observe(shellElement);
    window.addEventListener("resize", updateScale);

    return () => {
      resizeObserver?.disconnect();
      window.removeEventListener("resize", updateScale);
    };
  }, [exporting, shellElement, width]);

  const activeScale = exporting ? 1 : scale;

  return useMemo(
    () => ({
      shellRef,
      scale: activeScale,
      scaleBoxStyle: {
        width: `${width * activeScale}px`,
        height: `${height * activeScale}px`,
      },
      frameStyle: {
        width: `${width}px`,
        height: `${height}px`,
        minHeight: 0,
        aspectRatio: "auto",
        transform: exporting ? undefined : `scale(${activeScale})`,
        transformOrigin: "top left",
        willChange: exporting ? undefined : "transform",
      },
    }),
    [activeScale, exporting, height, shellRef, width],
  );
}
