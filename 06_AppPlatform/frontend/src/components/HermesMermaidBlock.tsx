import { useEffect, useId, useRef, useState } from "react";
import type { HermesMermaidBlock as HermesMermaidBlockType } from "../types/hermes";

interface Props {
  block: HermesMermaidBlockType;
  maxHeight?: number;
}

export function HermesMermaidBlock({ block, maxHeight = 500 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const id = useId();
  const [error, setError] = useState<string | null>(null);
  const renderedRef = useRef(false);

  useEffect(() => {
    if (!containerRef.current || renderedRef.current) return;
    renderedRef.current = true;

    let cancelled = false;
    import("mermaid")
      .then((mermaid) => {
        if (cancelled) return;
        mermaid.default.initialize({ startOnLoad: false, securityLevel: "strict" });
        return mermaid.default.run({ nodes: [containerRef.current!] });
      })
      .then(() => {
        if (!cancelled) setError(null);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [block.raw]);

  if (error) {
    return (
      <div className="hermes-mermaid-block" style={{ maxHeight, overflow: "auto" }}>
        <div style={{ color: "#ef4444", fontSize: 12, marginBottom: 8 }}>
          Diagram render error: {error}
        </div>
        <pre style={{ fontSize: 11, whiteSpace: "pre-wrap", color: "#64748b" }}>
          {block.raw}
        </pre>
      </div>
    );
  }

  return (
    <div className="hermes-mermaid-block" style={{ maxHeight, overflow: "auto" }}>
      <div ref={containerRef} id={id} className="mermaid">
        {block.raw}
      </div>
    </div>
  );
}
