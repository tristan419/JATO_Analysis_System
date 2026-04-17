import { NavLink } from "react-router-dom";

const ADMIN_TOOL_LINKS = [
  { to: "/specification", label: "02 规格明细" },
  { to: "/data-management", label: "03 数据总览" },
  { to: "/engineering", label: "05 配置导入" },
  { to: "/review", label: "06 匹配审核" },
  { to: "/msrp/monthly-update", label: "07 JATO 月更" },
];

export function AdminToolsNav() {
  return (
    <details className="msrp-admin-links">
      <summary>管理工具</summary>
      <nav>
        {ADMIN_TOOL_LINKS.map((item) => (
          <NavLink key={item.to} to={item.to}>
            {item.label}
          </NavLink>
        ))}
      </nav>
    </details>
  );
}
