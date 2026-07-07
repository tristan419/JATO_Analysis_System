"""Status-flow configuration for Order Genius vehicle allocation."""

from __future__ import annotations

from typing import Any


DEFAULT_LOGISTICS_STATUS_FLOW: list[dict[str, Any]] = [
    {
        "key": "pending",
        "labelEn": "Ordered",
        "labelZh": "已下单",
        "order": 10,
        "color": "#2563eb",
        "icon": "clipboard-list",
        "terminal": False,
        "allowedTransitions": ["in_production", "ready_for_shipping"],
    },
    {
        "key": "in_production",
        "labelEn": "In production",
        "labelZh": "生产中",
        "order": 20,
        "color": "#7c3aed",
        "icon": "factory",
        "terminal": False,
        "allowedTransitions": ["ready_for_shipping"],
    },
    {
        "key": "ready_for_shipping",
        "labelEn": "Ready for shipping",
        "labelZh": "待发运",
        "order": 30,
        "color": "#0891b2",
        "icon": "package-check",
        "terminal": False,
        "allowedTransitions": ["on_vessel"],
    },
    {
        "key": "on_vessel",
        "labelEn": "In shipping",
        "labelZh": "海运途中",
        "order": 40,
        "color": "#0f766e",
        "icon": "ship",
        "terminal": False,
        "allowedTransitions": ["arrived_at_port"],
    },
    {
        "key": "arrived_at_port",
        "labelEn": "Arrived at port",
        "labelZh": "到港",
        "order": 50,
        "color": "#ca8a04",
        "icon": "anchor",
        "terminal": False,
        "allowedTransitions": ["in_warehouse", "ready_for_pickup"],
    },
    {
        "key": "in_warehouse",
        "labelEn": "In stock",
        "labelZh": "总代库存",
        "order": 60,
        "color": "#65a30d",
        "icon": "warehouse",
        "terminal": False,
        "allowedTransitions": ["ready_for_pickup", "delivered"],
    },
    {
        "key": "ready_for_pickup",
        "labelEn": "In commission",
        "labelZh": "经销商库存",
        "order": 70,
        "color": "#ea580c",
        "icon": "store",
        "terminal": False,
        "allowedTransitions": ["delivered"],
    },
    {
        "key": "delivered",
        "labelEn": "In traffic",
        "labelZh": "已交付并注册",
        "order": 80,
        "color": "#dc2626",
        "icon": "car-front",
        "terminal": True,
        "allowedTransitions": [],
    },
]

DEFAULT_ALLOCATION_STATUS_FLOW: list[dict[str, Any]] = [
    {
        "key": "unallocated",
        "labelEn": "Unallocated",
        "labelZh": "未分配",
        "order": 10,
        "color": "#64748b",
        "icon": "circle",
        "terminal": False,
        "allowedTransitions": ["reserved", "allocated", "cancelled"],
    },
    {
        "key": "reserved",
        "labelEn": "Reserved",
        "labelZh": "已预留",
        "order": 20,
        "color": "#2563eb",
        "icon": "bookmark",
        "terminal": False,
        "allowedTransitions": ["allocated", "cancelled"],
    },
    {
        "key": "allocated",
        "labelEn": "Allocated",
        "labelZh": "已分配",
        "order": 30,
        "color": "#0f766e",
        "icon": "check-circle",
        "terminal": False,
        "allowedTransitions": ["delivered", "cancelled"],
    },
    {
        "key": "delivered",
        "labelEn": "Delivered",
        "labelZh": "已交付",
        "order": 40,
        "color": "#16a34a",
        "icon": "badge-check",
        "terminal": True,
        "allowedTransitions": [],
    },
    {
        "key": "cancelled",
        "labelEn": "Cancelled",
        "labelZh": "已取消",
        "order": 90,
        "color": "#dc2626",
        "icon": "x-circle",
        "terminal": True,
        "allowedTransitions": [],
    },
]


STATUS_FLOW_OVERRIDES: dict[tuple[str | None, str | None], dict[str, Any]] = {
    (
        "DK",
        "NCG",
    ): {
        "source": "ordering_account",
        "logistics": {
            "pending": {
                "labelEn": "Ordered",
                "labelZh": "已下单",
                "color": "#2563eb",
                "icon": "clipboard-list",
            },
            "on_vessel": {
                "labelEn": "In shipping",
                "labelZh": "海运途中",
                "color": "#0891b2",
                "icon": "ship",
            },
            "in_warehouse": {
                "labelEn": "In stock",
                "labelZh": "总代库存",
                "color": "#16a34a",
                "icon": "warehouse",
            },
            "ready_for_pickup": {
                "labelEn": "In commission",
                "labelZh": "经销商库存",
                "color": "#ea580c",
                "icon": "store",
            },
            "delivered": {
                "labelEn": "In traffic",
                "labelZh": "已交付并注册",
                "color": "#dc2626",
                "icon": "car-front",
            },
        },
    },
}


def _clean_scope(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().upper()
    return text or None


def _clone_status_flow(flow: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **step,
            "allowedTransitions": list(step.get("allowedTransitions", [])),
        }
        for step in flow
    ]


def _apply_status_flow_overrides(
    flow: list[dict[str, Any]],
    overrides: dict[str, dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not overrides:
        return _clone_status_flow(flow)
    merged: list[dict[str, Any]] = []
    for step in flow:
        key = str(step.get("key") or "")
        override = overrides.get(key, {})
        merged.append(
            {
                **step,
                **override,
                "allowedTransitions": list(override.get("allowedTransitions", step.get("allowedTransitions", []))),
            }
        )
    return merged


def _status_flow_override_for(
    country_code: str | None,
    ordering_account_code: str | None,
) -> dict[str, Any] | None:
    candidates = [
        (country_code, ordering_account_code),
        (country_code, None),
        (None, ordering_account_code),
    ]
    for key in candidates:
        override = STATUS_FLOW_OVERRIDES.get(key)
        if override:
            return override
    return None


def get_vehicle_status_flow_config(country_code: str | None = None, ordering_account_code: str | None = None) -> dict[str, Any]:
    """Return the status-flow contract consumed by the PI allocation UI."""
    country = _clean_scope(country_code)
    ordering_account = _clean_scope(ordering_account_code)
    override = _status_flow_override_for(country, ordering_account) or {}
    return {
        "countryCode": country,
        "orderingAccountCode": ordering_account,
        "source": override.get("source", "default"),
        "logistics": _apply_status_flow_overrides(DEFAULT_LOGISTICS_STATUS_FLOW, override.get("logistics")),
        "allocation": _apply_status_flow_overrides(DEFAULT_ALLOCATION_STATUS_FLOW, override.get("allocation")),
    }
