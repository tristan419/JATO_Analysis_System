#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


DEFAULT_COUNTRIES = [
    "丹麦",
    "克罗地亚",
    "匈牙利",
    "奥地利",
    "希腊",
    "德国",
    "意大利",
    "挪威",
    "捷克",
    "斯洛伐克",
    "斯洛文尼亚",
    "比利时",
    "法国",
    "波兰",
    "瑞典",
    "瑞士",
    "罗马尼亚",
    "芬兰",
    "荷兰",
    "葡萄牙",
    "西班牙",
]
DEFAULT_POWERTRAINS = ["ICE", "HEV", "BEV", "MHEV", "PHEV"]
DEFAULT_GROUP_BYS = ["动总规整", "国家", "四驱占比", "Business/Private 占比"]
DEFAULT_GRAINS = ["month", "year"]
DEFAULT_USER_ROLES = ["viewer", "order_filler", "editor", "admin"]
DEFAULT_SHARE_SPLIT_BY = ["segment", "powertrain"]
SHARE_GROUP_BYS = {"四驱占比", "Business/Private 占比"}
SERVER_CACHE_HEADER = "x-jato-server-cache"
EDGE_CACHE_HEADER = "x-jato-edge-cache"


@dataclass(frozen=True)
class PrewarmRequest:
    label: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class PrewarmAttempt:
    label: str
    attempt: int
    user_role: str
    status: int
    seconds: float
    rows: int | None
    server_cache: str
    edge_cache: str


def split_csv(value: str | None, default: list[str]) -> list[str]:
    raw = value or ""
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or list(default)


def normalize_origin(value: str) -> str:
    return value.strip().rstrip("/") or "http://127.0.0.1:8000"


def api_url(origin: str, prefix: str, path: str) -> str:
    clean_prefix = "/" + prefix.strip("/")
    clean_path = "/" + path.strip("/")
    return urljoin(f"{normalize_origin(origin)}/", f"{clean_prefix}{clean_path}")


def build_prewarm_requests(
    countries: list[str],
    powertrains: list[str],
    group_bys: list[str],
    grains: list[str],
    top_n: int,
    include_others: bool,
    share_split_by: list[str] | None = None,
) -> list[PrewarmRequest]:
    filters = {
        "国家": countries,
        "动总规整": powertrains,
    }
    requests: list[PrewarmRequest] = []
    split_values = [
        value.strip().lower()
        for value in (share_split_by or [])
        if value.strip().lower() in {"segment", "powertrain"}
    ]
    for grain in dict.fromkeys(grains):
        normalized_grain = "year" if grain.strip().lower() == "year" else "month"
        for group_by in dict.fromkeys(group_bys):
            group_split_values: list[str | None] = [None]
            if group_by in SHARE_GROUP_BYS:
                group_split_values.extend(dict.fromkeys(split_values))
            for split_value in group_split_values:
                payload = {
                    "filters": filters,
                    "grain": normalized_grain,
                    "group_by": group_by,
                    "top_n": max(1, int(top_n)),
                    "include_others": bool(include_others),
                }
                label = f"{normalized_grain}:{group_by}"
                if split_value is not None:
                    payload["share_split_by"] = split_value
                    label = f"{label}:{split_value}"
                requests.append(PrewarmRequest(label=label, payload=payload))
    return requests


def auth_headers(token: str, user_name: str, user_role: str) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-User-Name": user_name or "prewarm",
        "X-User-Role": user_role or "viewer",
    }
    if token:
        headers["X-Auth-Token"] = token
    return headers


def post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any], float]:
    started = time.perf_counter()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            elapsed = time.perf_counter() - started
            parsed = json.loads(raw) if raw else {}
            return response.status, dict(response.headers), parsed, elapsed
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach {url}: {exc.reason}") from exc


def header_value(headers: dict[str, str], name: str) -> str:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return str(value)
    return ""


def run_prewarm(
    url: str,
    requests: list[PrewarmRequest],
    token: str,
    user_name: str,
    user_roles: list[str],
    repeat: int,
    timeout: float,
) -> list[PrewarmAttempt]:
    attempts: list[PrewarmAttempt] = []
    roles = user_roles or DEFAULT_USER_ROLES
    for user_role in dict.fromkeys(roles):
        headers = auth_headers(token, user_name, user_role)
        for item in requests:
            for attempt in range(1, max(1, int(repeat)) + 1):
                status, response_headers, payload, seconds = post_json(
                    url,
                    item.payload,
                    headers,
                    timeout,
                )
                rows = payload.get("rows")
                attempts.append(
                    PrewarmAttempt(
                        label=item.label,
                        attempt=attempt,
                        user_role=user_role,
                        status=status,
                        seconds=seconds,
                        rows=int(rows) if isinstance(rows, int | float) else None,
                        server_cache=header_value(response_headers, SERVER_CACHE_HEADER),
                        edge_cache=header_value(response_headers, EDGE_CACHE_HEADER),
                    )
                )
    return attempts


def validate_attempts(
    attempts: list[PrewarmAttempt],
    require_server_cache: bool,
    require_repeat_hit: bool,
) -> list[str]:
    errors: list[str] = []
    if require_server_cache:
        missing = [item for item in attempts if not item.server_cache]
        if missing:
            errors.append("missing X-JATO-Server-Cache on one or more responses")

    if require_repeat_hit:
        latest_by_label: dict[tuple[str, str], PrewarmAttempt] = {}
        for attempt in attempts:
            latest_by_label[(attempt.user_role, attempt.label)] = attempt
        cold_labels = [
            f"{item.user_role}:{item.label}"
            for item in latest_by_label.values()
            if item.server_cache.upper() == "MISS"
        ]
        if cold_labels:
            errors.append(
                "repeat request still returned MISS for: "
                + ", ".join(sorted(cold_labels))
            )
    return errors


def print_attempts(attempts: list[PrewarmAttempt]) -> None:
    for item in attempts:
        print(
            json.dumps(
                {
                    "label": item.label,
                    "attempt": item.attempt,
                    "userRole": item.user_role,
                    "status": item.status,
                    "seconds": round(item.seconds, 3),
                    "rows": item.rows,
                    "serverCache": item.server_cache or "-",
                    "edgeCache": item.edge_cache or "-",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prewarm Dashboard grouped time-series cache through "
            "POST /v1/analysis/time-series-grouped."
        )
    )
    parser.add_argument(
        "--origin",
        default=os.getenv("JATO_API_ORIGIN", "http://127.0.0.1:8000"),
        help="API origin, for example http://127.0.0.1:8000 or https://www.ojeur.cloud.",
    )
    parser.add_argument("--prefix", default=os.getenv("JATO_API_PREFIX", "/v1"))
    parser.add_argument("--token", default=os.getenv("APP_AUTH_TOKEN", os.getenv("VITE_AUTH_TOKEN", "")))
    parser.add_argument("--user-name", default=os.getenv("VITE_USER_NAME", "prewarm"))
    parser.add_argument("--user-role", default=os.getenv("VITE_USER_ROLE", "viewer"))
    parser.add_argument(
        "--user-roles",
        default=os.getenv(
            "JATO_PREWARM_USER_ROLES",
            os.getenv("APP_GROUPED_TIME_SERIES_PREWARM_SCOPES", ""),
        ),
    )
    parser.add_argument("--countries", default=os.getenv("JATO_PREWARM_COUNTRIES", ""))
    parser.add_argument("--powertrains", default=os.getenv("JATO_PREWARM_POWERTRAINS", ""))
    parser.add_argument("--group-by", default=os.getenv("JATO_PREWARM_GROUP_BY", ""))
    parser.add_argument("--grains", default=os.getenv("JATO_PREWARM_GRAINS", ""))
    parser.add_argument("--share-split-by", default=os.getenv("JATO_PREWARM_SHARE_SPLIT_BY", ""))
    parser.add_argument("--top-n", type=int, default=int(os.getenv("JATO_PREWARM_TOP_N", "10")))
    parser.add_argument("--include-others", action="store_true")
    parser.add_argument("--repeat", type=int, default=int(os.getenv("JATO_PREWARM_REPEAT", "2")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("JATO_PREWARM_TIMEOUT", "60")))
    parser.add_argument("--require-server-cache", action="store_true")
    parser.add_argument("--require-repeat-hit", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    url = api_url(args.origin, args.prefix, "/analysis/time-series-grouped")
    requests = build_prewarm_requests(
        countries=split_csv(args.countries, DEFAULT_COUNTRIES),
        powertrains=split_csv(args.powertrains, DEFAULT_POWERTRAINS),
        group_bys=split_csv(args.group_by, DEFAULT_GROUP_BYS),
        grains=split_csv(args.grains, DEFAULT_GRAINS),
        top_n=args.top_n,
        include_others=args.include_others,
        share_split_by=split_csv(args.share_split_by, DEFAULT_SHARE_SPLIT_BY),
    )
    attempts = run_prewarm(
        url=url,
        requests=requests,
        token=args.token,
        user_name=args.user_name,
        user_roles=split_csv(
            args.user_roles,
            split_csv(args.user_role, DEFAULT_USER_ROLES),
        ),
        repeat=args.repeat,
        timeout=args.timeout,
    )
    print_attempts(attempts)
    errors = validate_attempts(
        attempts,
        require_server_cache=args.require_server_cache,
        require_repeat_hit=args.require_repeat_hit,
    )
    if errors:
        raise SystemExit("; ".join(errors))


if __name__ == "__main__":
    main()
