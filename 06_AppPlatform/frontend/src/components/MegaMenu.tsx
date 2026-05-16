import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import { useAuth } from "../contexts/AuthContext";
import {
  filterMenuByRole,
  getActiveMegaMenuId,
  MEGA_MENU_ITEMS,
  type MegaMenuItem,
  type MegaMenuSubItem,
} from "../utils/pageNavigation";
import { AssistantMark } from "./AssistantMark";

function MegaMenuPanel({
  item,
  open,
  onClose,
}: {
  item: MegaMenuItem & { type: "dropdown" | "mega" };
  open: boolean;
  onClose: () => void;
}) {
  if (!open) return null;

  if (item.type === "dropdown") {
    return (
      <div className="mega-menu-panel mega-menu-panel--sm" role="menu">
        {item.items.map((sub) => (
          <MegaMenuPanelLink key={sub.to} sub={sub} onClick={onClose} />
        ))}
      </div>
    );
  }

  return (
    <div className="mega-menu-panel mega-menu-panel--lg" role="menu">
      {item.groups.map((group) => (
        <div key={group.title} className="mega-menu-column">
          <h3 className="mega-menu-column-title">{group.title}</h3>
          {group.items.map((sub) => (
            <MegaMenuPanelLink key={sub.to} sub={sub} onClick={onClose} />
          ))}
        </div>
      ))}
    </div>
  );
}

function MegaMenuPanelLink({
  sub,
  onClick,
}: {
  sub: MegaMenuSubItem;
  onClick: () => void;
}) {
  const location = useLocation();
  const active =
    location.pathname === sub.to || location.pathname.startsWith(`${sub.to}/`);

  return (
    <Link
      to={sub.to}
      className={`mega-menu-panel-link${active ? " active" : ""}`}
      role="menuitem"
      onClick={onClick}
    >
      <span className="mega-menu-panel-link-label">{sub.label}</span>
      <span className="mega-menu-panel-link-sublabel">{sub.sublabel}</span>
    </Link>
  );
}

function MegaMenuDropdown({
  item,
  open,
  onToggle,
  onClose,
}: {
  item: MegaMenuItem & { type: "dropdown" | "mega" };
  open: boolean;
  onToggle: () => void;
  onClose: () => void;
}) {
  const location = useLocation();
  const activeId = getActiveMegaMenuId(location.pathname);
  const isActive = activeId === item.id;
  const ref = useRef<HTMLDivElement>(null);
  const hoverSupported = useRef(window.matchMedia("(hover: hover)").matches);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    }
    if (open) {
      document.addEventListener("mousedown", handleClick);
      return () => document.removeEventListener("mousedown", handleClick);
    }
  }, [open, onClose]);

  useEffect(() => {
    if (!open) return;
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onClose();
        (document.activeElement as HTMLElement)?.blur();
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [open, onClose]);

  return (
    <div
      className={`mega-menu-dropdown${open ? " is-open" : ""}`}
      ref={ref}
      onMouseEnter={() => {
        if (hoverSupported.current && !open) onToggle();
      }}
      onMouseLeave={() => {
        if (hoverSupported.current && open) onClose();
      }}
    >
      <button
        className={`mega-menu-trigger${isActive ? " active" : ""}`}
        type="button"
        aria-haspopup="true"
        aria-expanded={open}
        onClick={onToggle}
      >
        <span className="mega-menu-trigger-label">{item.label}</span>
        <span className="mega-menu-trigger-sublabel">{item.sublabel}</span>
        <span className="mega-menu-chevron" aria-hidden="true" />
      </button>
      <MegaMenuPanel item={item} open={open} onClose={onClose} />
    </div>
  );
}

export function MegaMenu() {
  const location = useLocation();
  const { user, logout } = useAuth();
  const [openId, setOpenId] = useState<string | null>(null);
  const [navOpen, setNavOpen] = useState(false);
  const activeId = getActiveMegaMenuId(location.pathname);

  const closeAll = useCallback(() => setOpenId(null), []);

  const filteredItems = useMemo(
    () => filterMenuByRole(MEGA_MENU_ITEMS, user?.role ?? "viewer"),
    [user?.role],
  );

  useEffect(() => {
    setNavOpen(false);
    setOpenId(null);
  }, [location.pathname]);

  function toggleDropdown(id: string) {
    setOpenId((prev) => (prev === id ? null : id));
  }

  return (
    <>
      <button
        type="button"
        className={`top-bar-menu-toggle${navOpen ? " is-open" : ""}`}
        aria-expanded={navOpen}
        aria-controls="primary-navigation"
        aria-label={navOpen ? "收起主导航" : "展开主导航"}
        onClick={() => setNavOpen((c) => !c)}
      >
        <span aria-hidden="true" />
        <span aria-hidden="true" />
        <span aria-hidden="true" />
      </button>

      <nav className="mega-menu" aria-label="Primary">
        {filteredItems.map((item) => {
          if (item.type === "link") {
            return (
              <Link
                key={item.id}
                to={item.to}
                className={`mega-menu-link${activeId === item.id ? " active" : ""}`}
              >
                <span className="mega-menu-link-label">{item.label}</span>
                <span className="mega-menu-link-sublabel">{item.sublabel}</span>
              </Link>
            );
          }
          return (
            <MegaMenuDropdown
              key={item.id}
              item={item}
              open={openId === item.id}
              onToggle={() => toggleDropdown(item.id)}
              onClose={closeAll}
            />
          );
        })}

        <div className="mega-menu-user">
          <Link
            to="/copilot"
            className="mega-menu-ai-btn"
            aria-label="Country Assistant"
            title="Country Assistant"
          >
            <AssistantMark size={22} />
          </Link>
          {user ? (
            <>
              <span className="mega-menu-username">{user.username}</span>
              <span className="mega-menu-role">{user.role}</span>
              <button
                type="button"
                className="mega-menu-signout"
                onClick={logout}
              >
                Sign out
              </button>
            </>
          ) : (
            <Link to="/login" className="mega-menu-signin">
              Sign in
            </Link>
          )}
        </div>
      </nav>

      {navOpen && (
        <div
          className="mega-menu-overlay"
          onClick={() => setNavOpen(false)}
          aria-hidden="true"
        />
      )}
      <aside
        id="primary-navigation"
        className={`mega-menu-drawer${navOpen ? " is-open" : ""}`}
        aria-hidden={!navOpen}
      >
        <div className="mega-menu-drawer-inner">
          {filteredItems.map((item) => {
            if (item.type === "link") {
              return (
                <Link
                  key={item.id}
                  to={item.to}
                  className={`mega-menu-drawer-link${activeId === item.id ? " active" : ""}`}
                  onClick={() => setNavOpen(false)}
                >
                  <span className="mega-menu-drawer-link-label">{item.label}</span>
                  <span className="mega-menu-drawer-link-sublabel">{item.sublabel}</span>
                </Link>
              );
            }
            return (
              <DrawerAccordion
                key={item.id}
                item={item}
                activeId={activeId}
                onNavigate={() => setNavOpen(false)}
              />
            );
          })}
          <div className="mega-menu-drawer-user">
            <Link to="/copilot" className="mega-menu-drawer-ai" onClick={() => setNavOpen(false)}>
              <AssistantMark size={20} />
              <span>Country Assistant</span>
            </Link>
            {user ? (
              <div className="mega-menu-drawer-auth">
                <span className="mega-menu-username">{user.username}</span>
                <span className="mega-menu-role">{user.role}</span>
                <button type="button" className="mega-menu-signout" onClick={() => { logout(); setNavOpen(false); }}>
                  Sign out
                </button>
              </div>
            ) : (
              <Link to="/login" className="mega-menu-signin" onClick={() => setNavOpen(false)}>
                Sign in
              </Link>
            )}
          </div>
        </div>
      </aside>
    </>
  );
}

function DrawerAccordion({
  item,
  activeId,
  onNavigate,
}: {
  item: MegaMenuItem & { type: "dropdown" | "mega" };
  activeId: string | null;
  onNavigate: () => void;
}) {
  const [expanded, setExpanded] = useState(false);

  const items: MegaMenuSubItem[] =
    item.type === "dropdown" ? item.items : item.groups.flatMap((g) => g.items);

  const groups = item.type === "mega" ? item.groups : null;

  return (
    <div className="mega-menu-drawer-accordion">
      <button
        type="button"
        className={`mega-menu-drawer-accordion-trigger${activeId === item.id ? " active" : ""}`}
        aria-expanded={expanded}
        onClick={() => setExpanded((e) => !e)}
      >
        <span className="mega-menu-drawer-accordion-label">{item.label}</span>
        <span className="mega-menu-drawer-accordion-sublabel">{item.sublabel}</span>
        <span className={`mega-menu-chevron${expanded ? " is-open" : ""}`} aria-hidden="true" />
      </button>
      <div className={`mega-menu-drawer-accordion-panel${expanded ? " is-open" : ""}`}>
        {groups
          ? groups.map((group) => (
              <div key={group.title} className="mega-menu-drawer-group">
                <span className="mega-menu-drawer-group-title">{group.title}</span>
                {group.items.map((sub) => (
                  <Link key={sub.to} to={sub.to} className="mega-menu-drawer-sublink" onClick={onNavigate}>
                    {sub.label} / {sub.sublabel}
                  </Link>
                ))}
              </div>
            ))
          : items.map((sub) => (
              <Link key={sub.to} to={sub.to} className="mega-menu-drawer-sublink" onClick={onNavigate}>
                {sub.label} / {sub.sublabel}
              </Link>
            ))}
      </div>
    </div>
  );
}
