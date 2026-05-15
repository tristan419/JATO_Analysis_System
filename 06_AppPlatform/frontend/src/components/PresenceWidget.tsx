import { useCallback, useEffect, useRef, useState } from "react";
import { animate } from "animejs";
import { usePresence, type PresenceUser } from "../hooks/usePresence";

const W = 160;
const HEADER_H = 34;
const ROW_H = 30;
const MAX_ROWS = 6;
const INITIAL_TOP = 8;
const SNAP_THRESHOLD = 60;
const VISIBLE_HINT = 20;

const ROLE_DOT: Record<string, string> = {
  admin: "#ef4444",
  editor: "#f59e0b",
  viewer: "#3b82f6",
  anonymous: "#64748b",
};

function edgeSnap(x: number) {
  const margin = VISIBLE_HINT - W;
  if (x < SNAP_THRESHOLD) return margin;
  if (x > window.innerWidth - W - SNAP_THRESHOLD)
    return window.innerWidth - VISIBLE_HINT;
  return Math.max(0, Math.min(x, window.innerWidth - W));
}

export function PresenceWidget() {
  const { online, samePage, users } = usePresence();
  const [expanded, setExpanded] = useState(false);
  const [snapped, setSnapped] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [pos, setPos] = useState(() => ({
    x: window.innerWidth - W - 12,
    y: INITIAL_TOP,
  }));
  const rootRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<{
    sx: number; sy: number; ox: number; oy: number; moved: boolean;
  } | null>(null);

  const visibleRows = Math.min(users.length, MAX_ROWS);
  const listH = expanded ? visibleRows * ROW_H + 8 : 0;
  const totalH = HEADER_H + listH;

  /* spring expand / collapse */
  useEffect(() => {
    if (!rootRef.current) return;
    try {
      animate(rootRef.current, {
        height: totalH,
        duration: 350,
        ease: expanded ? "outBack" : "inOutCubic",
      });
    } catch { /* decorative */ }
  }, [expanded, totalH]);

  /* unsnap on expand */
  useEffect(() => {
    if (!expanded || !snapped) return;
    const mid = (window.innerWidth - W) / 2;
    setPos((p) => ({ x: Math.max(8, mid), y: p.y }));
    setSnapped(false);
  }, [expanded, snapped]);

  /* ── Drag ── */
  const onDown = useCallback((e: React.PointerEvent) => {
    if (expanded) return;
    let originX = pos.x;
    if (snapped) {
      // slide to fully visible near the same edge, not center
      originX = pos.x < 0 ? 8 : window.innerWidth - W - 8;
      setPos((p) => ({ x: originX, y: p.y }));
      setSnapped(false);
    }
    dragRef.current = {
      sx: e.clientX, sy: e.clientY,
      ox: originX,
      oy: pos.y,
      moved: false,
    };
    setDragging(true);
    e.currentTarget.setPointerCapture(e.pointerId);
  }, [expanded, pos, snapped]);

  const onMove = useCallback((e: React.PointerEvent) => {
    if (!dragRef.current) return;
    const dx = e.clientX - dragRef.current.sx;
    const dy = e.clientY - dragRef.current.sy;
    if (Math.abs(dx) > 2 || Math.abs(dy) > 2) dragRef.current.moved = true;
    setPos({
      x: dragRef.current.ox + dx,
      y: Math.max(0, Math.min(dragRef.current.oy + dy, window.innerHeight - HEADER_H)),
    });
  }, []);

  const onUp = useCallback((e: React.PointerEvent) => {
    if (!dragRef.current) return;
    const wasDrag = dragRef.current.moved;
    dragRef.current = null;
    setDragging(false);
    e.currentTarget.releasePointerCapture(e.pointerId);
    if (!wasDrag) {
      setExpanded((v) => !v);
      if (snapped) setSnapped(false);
    } else {
      const sx = edgeSnap(pos.x);
      setPos((p) => ({ x: sx, y: p.y }));
      setSnapped(sx <= VISIBLE_HINT - W || sx >= window.innerWidth - VISIBLE_HINT);
    }
  }, [pos, snapped]);

  const isOff = online === 0;

  return (
    <div
      ref={rootRef}
      className={`pw-island${expanded ? " is-expanded" : ""}${dragging ? " is-dragging" : ""}${snapped ? " is-snapped" : ""}`}
      onPointerDown={onDown}
      onPointerMove={onMove}
      onPointerUp={onUp}
      style={{
        position: "fixed",
        left: pos.x,
        top: pos.y,
        zIndex: 9000,
        width: W,
        height: HEADER_H,
        borderRadius: 18,
        background: "#0d0d0d",
        border: "1px solid rgba(255,255,255,0.08)",
        boxShadow: dragging
          ? "0 12px 40px rgba(0,0,0,0.6)"
          : "0 2px 12px rgba(0,0,0,0.35)",
        color: "#f5f5f7",
        fontSize: 12,
        fontFamily: '-apple-system, "SF Pro Text", system-ui, sans-serif',
        userSelect: "none",
        cursor: expanded ? "default" : "grab",
        overflow: "hidden",
        opacity: snapped && !dragging ? 0.45 : 1,
        transition: dragging
          ? "none"
          : "left 0.35s cubic-bezier(0.22,1,0.36,1), top 0.35s cubic-bezier(0.22,1,0.36,1), opacity 0.3s ease, box-shadow 0.3s ease",
        touchAction: "none",
      }}
    >
      {/* Header */}
      <div
        onClick={() => expanded && setExpanded(false)}
        style={{
          display: "flex",
          alignItems: "center",
          height: HEADER_H,
          padding: "0 12px",
          gap: 8,
          cursor: expanded ? "pointer" : "inherit",
        }}
      >
        <span className="presence-dot" style={{
          width: 7, height: 7, borderRadius: "50%",
          background: isOff ? "#64748b" : "#30d158",
          flexShrink: 0,
          boxShadow: isOff ? "0 0 2px rgba(100,116,139,0.4)" : "0 0 7px rgba(48,209,88,0.55)",
        }} />
        <span style={{ fontWeight: 600, fontSize: 11, letterSpacing: "0.05em" }}>LIVE</span>
        <span style={{ color: "#98989e", fontSize: 11 }}>{isOff ? "offline" : online}</span>
        {samePage > 0 && !expanded && (
          <span style={{ color: "#52525b", fontSize: 10 }}>· {samePage}h</span>
        )}
        <span style={{ marginLeft: "auto", fontSize: 10, color: "#52525b",
          transition: "transform 0.3s", transform: expanded ? "rotate(180deg)" : undefined }}>▾</span>
      </div>

      {/* Vertical user list */}
      {expanded && (
        <div style={{
          borderTop: "1px solid rgba(255,255,255,0.06)",
          padding: "4px 8px",
          maxHeight: MAX_ROWS * ROW_H,
          overflowY: users.length > MAX_ROWS ? "auto" : "hidden",
        }}>
          {users.length === 0 ? (
            <div style={{ color: "#64748b", padding: "8px 0", textAlign: "center", fontSize: 11 }}>
              No one online
            </div>
          ) : (
            users.map((u: PresenceUser) => (
              <div key={`${u.user_name}-${u.current_page}`} style={{
                display: "flex", alignItems: "center", gap: 6,
                height: ROW_H, padding: "0 4px", fontSize: 11,
              }}>
                <span style={{
                  width: 6, height: 6, borderRadius: "50%",
                  background: ROLE_DOT[u.role] || "#64748b",
                  flexShrink: 0,
                }} />
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {u.user_name}
                </span>
                <span style={{ color: "#64748b", fontSize: 10, maxWidth: 64, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {u.current_page}
                </span>
                <span style={{ color: "#475569", fontSize: 10, flexShrink: 0 }}>{u.last_seen_ago_s}s</span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}
