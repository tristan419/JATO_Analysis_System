import React, { useEffect, useRef, useState } from "react";
import Spline from "@splinetool/react-spline";
import type { Application } from "@splinetool/runtime";

class SplineErrorBoundary extends React.Component<{ children: React.ReactNode, onError: () => void }, { hasError: boolean }> {
  constructor(props: any) {
    super(props);
    this.state = { hasError: false };
  }
  static getDerivedStateFromError() {
    return { hasError: true };
  }
  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error("Spline rendering error:", error, errorInfo);
    this.props.onError();
  }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{ position: "absolute", inset: 0, background: "#333", display: "flex", alignItems: "center", justifyContent: "center", color: "#fff", fontSize: "10px", textAlign: "center", padding: "4px" }}>
          Ask
        </div>
      );
    }
    return this.props.children;
  }
}

interface CatMascotProps {
  /** When true the chat panel is open — cat shows curious face */
  chatOpen?: boolean;
  size?: number;
}

/**
 * 3D Animated interactable cat mascot powered by Spline.
 */
export function CatMascot({ chatOpen = false, size = 64 }: CatMascotProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const splineAppRef = useRef<Application | null>(null);

  function onLoad(splineApp: Application) {
    splineAppRef.current = splineApp;
    setLoading(false);
    
    // IMPORTANT: If you want to try and change the letter "m" to "O&J" dynamically via code,
    // it only works if "m" is a TEXT object in the Spline scene, not a 3D geometry shape.
    // If it *is* a Text object named "m" in the layer list:
    // const mText = splineApp.findObjectByName('m');
    // if (mText) { mText.text = "O&J"; }
  }

  useEffect(() => {
    if (!splineAppRef.current) return;
    try {
      if (chatOpen) {
        splineAppRef.current.emitEvent("mouseDown", "Cat"); 
        splineAppRef.current.setVariable?.("isCurious", true);
      } else {
        splineAppRef.current.emitEvent("mouseUp", "Cat");
        splineAppRef.current.setVariable?.("isCurious", false);
      }
    } catch (e) {
      console.warn("Spline event trigger failed:", e);
    }
  }, [chatOpen]);

  // Please replace this URL with your EXPORTED .splinecode URL!
  // To get it: Open the Community File -> Click Remix -> Export -> Public URL
  // "https://app.spline.design/community/file/7b0fc82a-d762-4170-83f9-cb7f26de3594" is NOT a splinecode URL!
  const SPLINE_URL = "https://prod.spline.design/6Wq1Q7YGyMsqfall/scene.splinecode"; // Placeholder valid URL

  return (
    <div
      className="cat-mascot-spline"
      style={{
        width: size,
        height: size,
        position: "relative",
        borderRadius: "50%",
        overflow: "hidden",
        backgroundColor: loading && !error ? "transparent" : undefined
      }}
      aria-hidden
    >
      {loading && !error && (
        <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,0.7)", fontSize: "12px", background: "#1a1a1a" }}>
          ...
        </div>
      )}
      
      {error && (
        <div style={{ position: "absolute", inset: 0, background: "#333" }} />
      )}

      <SplineErrorBoundary onError={() => { setError(true); setLoading(false); }}>
        <Spline
          scene={SPLINE_URL}
          onLoad={onLoad}
          onError={() => {
            setError(true);
            setLoading(false);
          }}
          style={{
            width: "100%",
            height: "100%",
            display: error ? "none" : "block",
            pointerEvents: "none" // Allow interaction but handled by wrapper
          }}
        />
      </SplineErrorBoundary>
    </div>
  );
}
