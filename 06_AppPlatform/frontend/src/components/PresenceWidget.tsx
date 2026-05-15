import { useCallback, useEffect, useRef, useState } from "react";
import { animate } from "animejs";
import { usePresence, type PresenceUser } from "../hooks/usePresence";

/* ── Sizing ── */
const H = 36;
const W_COLLAPSED = 130;
const W_EXPANDED = 280;
const INITIAL_TOP = 8;
const SNAP_THRESHOLD = 60;
const VISIBLE_HINT = 20;

/* ── Role chip ── */
const ROLE: Record<string, { label: string; bg: string }> = {
  admin: { label: "Admin", bg: "#7f1d1d" },
  editor: { label: "Editor", bg: "#78350f" },
  viewer: { label: "Viewer", bg: "#1e3a5f" },
  anonymous: { label: "Guest", bg: "#334155" },
};

/* ── Edge snap ── */
function edgeSnap(x: number, w: number) {
  const margin = VISIBLE_HINT - w;
  if (x < SNAP_THRESHOLD) return margin; // snap left
  if (x > window.innerWidth - w - SNAP_THRESHOLD)
    return window.innerWidth - VISIBLE_HINT; // snap right
  return Math.max(0, Math.min(x, window.innerWidth - w));
}

/* ── Widget ── */

export function PresenceWidget() {
  const { online, samePage, users } = usePresence();
  const [expanded, setExpanded] = useState(false);
  const [snapped, setSnapped] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [pos, setPos] = useState(() => ({
    x: window.innerWidth - W_COLLAPSED - 12,
    y: INITIAL_TOP,
  }));
  const widgetW = expanded ? W_EXPANDED : W_COLLAPSED;
  const rootRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<{
    sx: number;
    sy: number;
    ox: number;
    oy: number;
    moved: boolean;
  } | null>(null);

  /* spring expand / collapse */
  useEffect(() => {
    if (!rootRef.current) return;
    try {
      animate(rootRef.current, {
        width: expanded ? W_EXPANDED : W_COLLAPSED,
        duration: 400,
        ease: expanded ? "outBack" : "inOutCubic",
      });
    } catch {
      /* decorative */
    }
  }, [expanded]);

  /* shift into view when expanding from snapped edge */
  useEffect(() => {
    if (!expanded || !snapped) return;
    // unsnap so the wider expanded pill is fully visible
    const mid = (window.innerWidth - W_EXPANDED) / 2;
    setPos((p) => ({ x: Math.max(8, mid), y: p.y }));
    setSnapped(false);
  }, [expanded, snapped]);

  /* ── Drag handlers ── */
  const onDown = useCallback(
    (e: React.PointerEvent) => {
      if (expanded) return;
      if (snapped) {
        // unsnap on grab
        const mid = (window.innerWidth - W_COLLAPSED) / 2;
        setPos((p) => ({ x: mid, y: p.y }));
        setSnapped(false);
      }
      dragRef.current = {
        sx: e.clientX,
        sy: e.clientY,
        ox: snapped
          ? (window.innerWidth - W_COLLAPSED) / 2
          : pos.x,
        oy: pos.y,
        moved: false,
      };
      setDragging(true);
      e.currentTarget.setPointerCapture(e.pointerId);
    },
    [expanded, pos, snapped],
  );

  const onMove = useCallback((e: React.PointerEvent) => {
    if (!dragRef.current) return;
    const dx = e.clientX - dragRef.current.sx;
    const dy = e.clientY - dragRef.current.sy;
    if (Math.abs(dx) > 2 || Math.abs(dy) > 2) dragRef.current.moved = true;
    setPos({
      x: dragRef.current.ox + dx,
      y: Math.max(0, Math.min(dragRef.current.oy + dy, window.innerHeight - H)),
    });
  }, []);

  const onUp = useCallback(
    (e: React.PointerEvent) => {
      if (!dragRef.current) return;
      const wasDrag = dragRef.current.moved;
      dragRef.current = null;
      setDragging(false);
      e.currentTarget.releasePointerCapture(e.pointerId);
      if (!wasDrag) {
        setExpanded((v) => !v);
        if (snapped) setSnapped(false);
      } else {
        const sx = edgeSnap(pos.x, widgetW);
        setPos((p) => ({ x: sx, y: p.y }));
        setSnapped(
          sx <= VISIBLE_HINT - widgetW ||
            sx >= window.innerWidth - VISIBLE_HINT,
        );
      }
    },
    [pos, snapped, widgetW],
  );

  /* ── Render ── */
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
        width: widgetW,
        height: H,
        borderRadius: 20,
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
      {/* ── Collapsed content ── */}
      <div
        className="pw-island-row"
        onClick={() => expanded && setExpanded(false)}
        style={{
          display: "flex",
          alignItems: "center",
          height: H,
          padding: "0 14px",
          gap: 8,
          cursor: expanded ? "pointer" : "inherit",
          whiteSpace: "nowrap",
        }}
      >
        {/* Breathing dot */}
        <span
          className="presence-dot"
          style={{
            width: 7,
            height: 7,
            borderRadius: "50%",
            background: isOff ? "#64748b" : "#30d158",
            flexShrink: 0,
            boxShadow: isOff
              ? "0 0 2px rgba(100,116,139,0.4)"
              : "0 0 7px rgba(48,209,88,0.55)",
          }}
        />

        {/* Compact label */}
        {!expanded && (
          <>
            <span style={{ fontWeight: 600, fontSize: 11, letterSpacing: "0.05em" }}>
              LIVE
            </span>
            <span style={{ color: "#98989e", fontSize: 11 }}>
              {isOff ? "offline" : online}
            </span>
          </>
        )}

        {/* Expanded: horizontal user chips */}
        {expanded && (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              flex: 1,
              overflow: "hidden",
            }}
          >
            <span style={{ fontWeight: 600, fontSize: 11, letterSpacing: "0.05em", flexShrink: 0 }}>
              LIVE
            </span>
            {users.length === 0 ? (
              <span style={{ color: "#98989e", fontSize: 11 }}>no one online</span>
            ) : (
              users.map((u: PresenceUser) => (
                <span
                  key={`${u.user_name}-${u.current_page}`}
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 4,
                    padding: "2px 8px",
                    borderRadius: 10,
                    background: "rgba(255,255,255,0.06)",
                    fontSize: 10,
                    flexShrink: 0,
                    whiteSpace: "nowrap",
                  }}
                >
                  <span
                    style={{
                      width: 5,
                      height: 5,
                      borderRadius: "50%",
                      background: ROLE[u.role]?.bg ?? "#334155",
                      flexShrink: 0,
                    }}
                  />
                  {u.user_name}
                </span>
              ))
            )}
          </div>
        )}

        {/* Chevon + same-page hint */}
        {!expanded && samePage > 0 && (
          <span style={{ color: "#52525b", fontSize: 10, flexShrink: 0 }}>
            · {samePage}h
          </span>
        )}
        <span
          style={{
            marginLeft: expanded ? "auto" : undefined,
            fontSize: 10,
            color: "#52525b",
            flexShrink: 0,
            transition: "transform 0.3s",
            transform: expanded ? "rotate(180deg)" : undefined,
          }}
        >
          ▾
        </span>
      </div>
    </div>
  );
}
