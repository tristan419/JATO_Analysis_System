import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from "react";
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

const RoleUpgradeModal = lazy(() =>
  import("./RoleUpgradeModal").then((module) => ({ default: module.RoleUpgradeModal }))
);

function isSubItemActive(to: string, location: { pathname: string; search: string }): boolean {
  const [targetPath, targetSearch = ""] = to.split("?");
  if (location.pathname !== targetPath && !location.pathname.startsWith(`${targetPath}/`)) return false;
  const currentParams = new URLSearchParams(location.search);
  if (!targetSearch) return currentParams.get("mode") !== "hero-product";
  const targetParams = new URLSearchParams(targetSearch);
  for (const [key, value] of targetParams.entries()) {
    if (currentParams.get(key) !== value) return false;
  }
  return true;
}

function MegaMenuPanel({
  item, open, onClose,
}: { item: MegaMenuItem & { type: "dropdown" | "mega" }; open: boolean; onClose: () => void }) {
  if (!open) return null;
  if (item.type === "dropdown") {
    return (
      <div className="mega-menu-panel mega-menu-panel--sm" role="menu">
        {item.items.map((sub) => <MegaMenuPanelLink key={sub.to} sub={sub} onClick={onClose} />)}
      </div>
    );
  }
  return (
    <div className="mega-menu-panel mega-menu-panel--lg" role="menu">
      {item.groups.map((group) => (
        <div key={group.title} className="mega-menu-column">
          <h3 className="mega-menu-column-title">{group.title}</h3>
          {group.items.map((sub) => <MegaMenuPanelLink key={sub.to} sub={sub} onClick={onClose} />)}
        </div>
      ))}
    </div>
  );
}

function MegaMenuPanelLink({ sub, onClick }: { sub: MegaMenuSubItem; onClick: () => void }) {
  const location = useLocation();
  const active = isSubItemActive(sub.to, location);
  return (
    <Link to={sub.to} className={`mega-menu-panel-link${active ? " active" : ""}`} role="menuitem" onClick={onClick}>
      <span className="mega-menu-panel-link-label">{sub.label}</span>
      <span className="mega-menu-panel-link-sublabel">{sub.sublabel}</span>
    </Link>
  );
}

function MegaMenuDropdown({
  item, open, onToggle, onClose,
}: { item: MegaMenuItem & { type: "dropdown" | "mega" }; open: boolean; onToggle: () => void; onClose: () => void }) {
  const location = useLocation();
  const activeId = getActiveMegaMenuId(location.pathname);
  const isActive = activeId === item.id;
  const ref = useRef<HTMLDivElement>(null);
  const hoverSupported = useRef(window.matchMedia("(hover: hover)").matches);
  const [flipLeft, setFlipLeft] = useState(false);

  useEffect(() => {
    function handleClick(e: MouseEvent) { if (ref.current && !ref.current.contains(e.target as Node)) onClose(); }
    if (open) { document.addEventListener("mousedown", handleClick); return () => document.removeEventListener("mousedown", handleClick); }
  }, [open, onClose]);

  useEffect(() => {
    if (!open) return;
    function handleKey(e: KeyboardEvent) { if (e.key === "Escape") { onClose(); (document.activeElement as HTMLElement)?.blur(); } }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [open, onClose]);

  // Flip panel leftwards when it overflows the right edge of the viewport
  useEffect(() => {
    if (!open) { setFlipLeft(false); return; }
    const timer = requestAnimationFrame(() => {
      const panel = ref.current?.querySelector(".mega-menu-panel--lg, .mega-menu-panel") as HTMLElement | null;
      if (!panel) return;
      const rect = panel.getBoundingClientRect();
      setFlipLeft(rect.right > window.innerWidth);
    });
    return () => cancelAnimationFrame(timer);
  }, [open]);

  const dropdownClass = [
    "mega-menu-dropdown",
    open ? "is-open" : "",
    flipLeft ? "flip-left" : "",
  ].filter(Boolean).join(" ");

  return (
    <div className={dropdownClass} ref={ref}
      onMouseEnter={() => { if (hoverSupported.current && !open) onToggle(); }}
      onMouseLeave={() => { if (hoverSupported.current && open) onClose(); }}>
      <button type="button" className={`mega-menu-trigger${isActive ? " active" : ""}`} aria-haspopup="true" aria-expanded={open} onClick={onToggle}>
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
  const [showUpgrade, setShowUpgrade] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const profileRef = useRef<HTMLDivElement | null>(null);
  const activeId = getActiveMegaMenuId(location.pathname);
  const closeAll = useCallback(() => setOpenId(null), []);

  const filteredItems = useMemo(
    () => filterMenuByRole(MEGA_MENU_ITEMS, user?.role ?? "viewer"),
    [user?.role],
  );

  useEffect(() => { setNavOpen(false); setOpenId(null); }, [location.pathname]);

  useEffect(() => {
    if (!profileOpen) return;
    function handleClick(e: MouseEvent) {
      if (profileRef.current && !profileRef.current.contains(e.target as Node)) {
        setProfileOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [profileOpen]);

  function toggleDropdown(id: string) { setOpenId((prev) => (prev === id ? null : id)); }

  return (
    <>
      <button type="button" className={`top-bar-menu-toggle${navOpen ? " is-open" : ""}`} aria-expanded={navOpen} aria-controls="primary-navigation" aria-label={navOpen ? "收起主导航" : "展开主导航"} onClick={() => setNavOpen((c) => !c)}>
        <span aria-hidden="true" /><span aria-hidden="true" /><span aria-hidden="true" />
      </button>

      <nav className="mega-menu" aria-label="Primary">
        {filteredItems.map((item) => {
          if (item.type === "link") {
            return <Link key={item.id} to={item.to} className={`mega-menu-link${activeId === item.id ? " active" : ""}`}><span className="mega-menu-link-label">{item.label}</span><span className="mega-menu-link-sublabel">{item.sublabel}</span></Link>;
          }
          return <MegaMenuDropdown key={item.id} item={item} open={openId === item.id} onToggle={() => toggleDropdown(item.id)} onClose={closeAll} />;
        })}

        <div className="mega-menu-user">
          <Link to="/copilot" className="mega-menu-ai-btn" aria-label="Country Assistant" title="Country Assistant"><AssistantMark size={22} /></Link>
          {user ? (
            <div className="mega-menu-profile-trigger" ref={profileRef}>
              <button
                type="button"
                className="mega-menu-profile-btn"
                onClick={() => setProfileOpen((v) => !v)}
                aria-expanded={profileOpen}
              >
                {user.avatarUrl ? (
                  <img src={user.avatarUrl} alt="" className="mega-menu-avatar" referrerPolicy="no-referrer" />
                ) : (
                  <span className="mega-menu-avatar mega-menu-avatar-fallback">{user.displayName?.[0] ?? user.username[0]}</span>
                )}
                <span className="mega-menu-username">{user.displayName ?? user.username}</span>
              </button>
              {profileOpen && (
                <div className="mega-menu-profile-popover">
                  <div className="mega-menu-profile-popover-header">
                    {user.avatarUrl ? (
                      <img src={user.avatarUrl} alt="" className="mega-menu-profile-popover-avatar" referrerPolicy="no-referrer" />
                    ) : (
                      <span className="mega-menu-profile-popover-avatar mega-menu-profile-popover-avatar-fallback">{user.displayName?.[0] ?? user.username[0]}</span>
                    )}
                    <div>
                      <div className="mega-menu-profile-popover-name">{user.displayName ?? user.username}</div>
                      <div className="mega-menu-profile-popover-email">{user.email ?? user.username}</div>
                    </div>
                  </div>
                  <div className="mega-menu-profile-popover-body">
                    <div className="mega-menu-profile-popover-row">
                      <span>Role</span>
                      <span className="mega-menu-role">{user.role}</span>
                    </div>
                    {user.primaryCountry ? (
                      <div className="mega-menu-profile-popover-row">
                        <span>Country</span>
                        <span>{user.primaryCountry}{user.secondaryCountries.length > 0 ? ` +${user.secondaryCountries.length}` : ""}</span>
                      </div>
                    ) : null}
                  </div>
                  <div className="mega-menu-profile-popover-actions">
                    <Link to="/account/profile" className="mega-menu-profile-popover-action" onClick={() => setProfileOpen(false)}>Edit Profile</Link>
                    {user.role === "viewer" && (
                      <button type="button" className="mega-menu-profile-popover-action" onClick={() => { setProfileOpen(false); setShowUpgrade(true); }}>Request Upgrade</button>
                    )}
                    <button type="button" className="mega-menu-profile-popover-action mega-menu-profile-popover-signout" onClick={logout}>Sign Out</button>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <Link to="/login" className="mega-menu-signin">Sign in</Link>
          )}
        </div>
      </nav>

      {navOpen && <div className="mega-menu-overlay" onClick={() => setNavOpen(false)} aria-hidden="true" />}
      <aside id="primary-navigation" className={`mega-menu-drawer${navOpen ? " is-open" : ""}`} aria-hidden={!navOpen}>
        <div className="mega-menu-drawer-inner">
          {filteredItems.map((item) => {
            if (item.type === "link") {
              return <Link key={item.id} to={item.to} className={`mega-menu-drawer-link${activeId === item.id ? " active" : ""}`} onClick={() => setNavOpen(false)}><span className="mega-menu-drawer-link-label">{item.label}</span><span className="mega-menu-drawer-link-sublabel">{item.sublabel}</span></Link>;
            }
            return <DrawerAccordion key={item.id} item={item} activeId={activeId} onNavigate={() => setNavOpen(false)} />;
          })}
          <div className="mega-menu-drawer-user">
            <Link to="/copilot" className="mega-menu-drawer-ai" onClick={() => setNavOpen(false)}><AssistantMark size={20} /><span>Country Assistant</span></Link>
            {user ? (
              <div className="mega-menu-drawer-auth">
                {user.avatarUrl ? (
                  <img src={user.avatarUrl} alt="" className="mega-menu-avatar" referrerPolicy="no-referrer" style={{ width: 32, height: 32 }} />
                ) : null}
                <span className="mega-menu-username">{user.displayName ?? user.username}</span>
                <span className="mega-menu-role">{user.role}</span>
                {user.email ? <span style={{ fontSize: 11, color: "#94a3b8" }}>{user.email}</span> : null}
                <Link to="/account/profile" className="mega-menu-signin" onClick={() => setNavOpen(false)}>国家偏好</Link>
                <button type="button" className="mega-menu-signout" onClick={() => { logout(); setNavOpen(false); }}>Sign out</button>
              </div>
            ) : (
              <Link to="/login" className="mega-menu-signin" onClick={() => setNavOpen(false)}>Sign in</Link>
            )}
          </div>
        </div>
      </aside>

      {showUpgrade && (
        <Suspense fallback={null}>
          <RoleUpgradeModal currentRole={user?.role ?? "viewer"} onClose={() => setShowUpgrade(false)} />
        </Suspense>
      )}
    </>
  );
}

function DrawerAccordion({
  item, activeId, onNavigate,
}: { item: MegaMenuItem & { type: "dropdown" | "mega" }; activeId: string | null; onNavigate: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const items: MegaMenuSubItem[] = item.type === "dropdown" ? item.items : item.groups.flatMap((g) => g.items);
  const groups = item.type === "mega" ? item.groups : null;
  return (
    <div className="mega-menu-drawer-accordion">
      <button type="button" className={`mega-menu-drawer-accordion-trigger${activeId === item.id ? " active" : ""}`} aria-expanded={expanded} onClick={() => setExpanded((e) => !e)}>
        <span className="mega-menu-drawer-accordion-label">{item.label}</span>
        <span className="mega-menu-drawer-accordion-sublabel">{item.sublabel}</span>
        <span className={`mega-menu-chevron${expanded ? " is-open" : ""}`} aria-hidden="true" />
      </button>
      <div className={`mega-menu-drawer-accordion-panel${expanded ? " is-open" : ""}`}>
        {groups
          ? groups.map((group) => (
              <div key={group.title} className="mega-menu-drawer-group">
                <span className="mega-menu-drawer-group-title">{group.title}</span>
                {group.items.map((sub) => <Link key={sub.to} to={sub.to} className="mega-menu-drawer-sublink" onClick={onNavigate}>{sub.label} / {sub.sublabel}</Link>)}
              </div>))
          : items.map((sub) => <Link key={sub.to} to={sub.to} className="mega-menu-drawer-sublink" onClick={onNavigate}>{sub.label} / {sub.sublabel}</Link>)}
      </div>
    </div>
  );
}
