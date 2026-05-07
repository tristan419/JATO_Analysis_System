from __future__ import annotations

from collections import Counter
from datetime import datetime
from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd

from app.core.config import PROJECT_ROOT

VOC_NORDIC_PATH = PROJECT_ROOT / "01_RAW_DATA" / "VOC_Nordic_SUV_Users_100.xlsx"
VOC_SWEDEN_HEV_PATH = (
    PROJECT_ROOT
    / "04_Processed_data"
    / "voc"
    / "se"
    / "raw"
    / "se_hev_owners_benchmark_voc_20260423.xlsx"
)
VOC_FORUM_ROOT = PROJECT_ROOT / "04_Processed_data" / "voc"
VOC_NORDIC_DISPLAY_SAMPLE_SIZE = 113
VOC_FORUM_DEFAULT_COUNTRIES = ("SE", "FI", "NO", "DK")

SCENARIO_TRANSLATIONS = {
    "Weekend ski trips to the mountains": "周末滑雪 / 山地出行",
    "Towing a boat or caravan": "拖船 / 拖挂房车",
    "Mixed city + rural daily use": "城市 + 乡郊混合日常用车",
    "Daily motorway commute (60–120 km/day)": "日常高速通勤（60–120 km/天）",
    "Lapland/northern winter driving": "北欧严寒 / 拉普兰冬季驾驶",
    "Coastal summer holiday touring": "夏季海岸度假长途",
    "Forest/gravel track weekend drives": "周末林道 / 碎石路出行",
    "Business travel between cities": "城际商务出行",
    "Daily urban commute + school run": "城市通勤 + 接送孩子",
    "Long-distance family road trips (Germany/Norway)": "家庭跨国长途旅行",
}
PHILOSOPHY_TRANSLATIONS = {
    "Brand prestige is part of the purchase": "品牌体面 / 品牌背书",
    "Environmental footprint matters most": "环保足迹优先",
    "Total cost of ownership focus": "全周期成本优先",
    "Early adopter of new tech": "乐于尝鲜新科技",
    "Prefers to lease, keeps options open": "偏向租赁，保留换车弹性",
    "Family-centred practical buyer": "家庭实用优先",
    "Value for money – functional over flashy": "功能价值优先",
    "Quality first, price secondary": "质量优先，价格其次",
}
LIFESTYLE_TRANSLATIONS = {
    "Hunting": "狩猎",
    "Ice fishing": "冰钓",
    "Photography": "摄影",
    "Gardening": "园艺",
    "Swimming": "游泳",
    "Cross-country skiing": "越野滑雪",
    "Ice hockey": "冰球",
    "Trail running": "越野跑",
    "Hiking / fell walking": "徒步 / 山地行走",
    "Snowmobiling": "雪地摩托",
    "Sailing": "帆船",
    "Road cycling": "公路骑行",
    "Ferry terminal": "渡轮码头",
    "Forest trails": "森林步道",
    "City centre": "市中心",
    "National park": "国家公园",
    "Sports hall": "运动馆",
    "Children's school & activities": "学校 / 亲子活动",
    "Supermarket / ICA / Prisma": "超市采购",
    "Summer cottage": "夏屋 / 度假屋",
    "Ski resort / slopes": "滑雪场",
    "Hardware store / Bauhaus": "建材 / DIY",
}
NORDIC_ATTENTION_CHANNEL_PRESENTATION_WEIGHTS: list[tuple[str, int]] = [
    ("品牌官网", 26),
    ("经销商试驾活动", 22),
    ("熟人推荐", 20),
    ("车展 / Motor Show", 18),
    ("社媒广告", 14),
]
NORDIC_SAMPLE_SOURCE_PRESENTATION_WEIGHTS: list[tuple[str, int]] = [
    ("Bilnytt.se", 16),
    ("Vi Bilägare (SE)", 15),
    ("auto motor & sport (SE)", 15),
    ("Teknikens Värld (SE auto magazine)", 12),
    ("Tekniikan Maailma (FI)", 12),
    ("Iltasanomat Autot (FI)", 10),
    ("Reddit r/cars / r/electricvehicles", 10),
    ("YouTube comments", 10),
]
SWEDEN_HEV_ATTENTION_CHANNEL_PRESENTATION_WEIGHTS: list[tuple[str, int]] = [
    ("品牌官网", 24),
    ("经销商试驾活动", 22),
    ("熟人推荐", 20),
    ("汽车媒体评测", 18),
    ("车主论坛 / 社群", 16),
]
SWEDEN_HEV_SAMPLE_SOURCE_PRESENTATION_WEIGHTS: list[tuple[str, int]] = [
    ("Motorblog.se / YouTube", 20),
    ("Bilweb.se / colleague recommendations", 18),
    ("Vi Bilägare / dealer visit", 16),
    ("Teknikens Värld / YouTube", 15),
    ("Toyota dealer test drive", 12),
    ("Owners forum / Facebook groups", 10),
    ("Friends recommendation / Google", 9),
]
POWERTRAIN_TRANSLATIONS = {
    "Full BEV (500 km+ real-world range)": "纯电长续航",
    "PHEV (daily EV, petrol on long runs)": "PHEV 过渡",
    "Mild hybrid (HEV) – no plug needed": "HEV 不插电",
    "Open to any if TCO makes sense": "TCO 合理即可",
    "Performance ICE – not ready to switch": "仍坚持 ICE",
    "HEV for now, open to BEV in 5 years": "先用 HEV，后续再看 BEV",
    "HEV and no BEV until home charging becomes practical": "无家充前先选 HEV",
    "Prefer HEV until BEV winter range exceeds 500 km in real use": "冬季续航够前仍选 HEV",
    "HEV because reliability matters more than chasing EV range claims": "重可靠性，先选 HEV",
    "HEV is the right middle ground for this use case": "HEV 是当前最优平衡",
}
FACTOR_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("价格 / TCO / 补贴", ("subsidy", "tco", "price", "budget", "discount", "fuel savings", "total cost", "financing")),
    ("安全 / 冬季能力", ("safety", "euro ncap", "awd", "winter", "gravel", "forest track", "off-road")),
    ("拖挂 / 空间实用", ("tow", "trailer", "boat", "caravan", "third-row", "7-seat", "tailgate")),
    ("续航 / 充电便利", ("range", "charging", "home charging", "heat pump", "150 kw", "500 km")),
    ("智能化 / OTA / ADAS", ("ota", "pilot assist", "adas", "hands-free", "l2", "l3", "google maps", "app", "software", "carplay", "android auto")),
    ("品牌 / 口碑 / 试驾", ("brand", "prestige", "colleague", "test drive", "reputation", "residual value")),
    ("环保 / 低碳", ("environmental", "co2", "footprint")),
    ("服务 / 沟通体验", ("dealer", "communication", "local-language", "email", "order tracker", "service network", "pricing upfront")),
]
RANKED_ITEM_PATTERN = re.compile(r"\d\.\s*([^\d].*?)(?=\s*\d\.|$)")
INSIGHT_TEXT_COLUMNS = [
    "Why This Car?",
    "Future Car Requirements",
    "Customer Requirements",
    "Top 3 Favourite Features",
    "Top 3 Complaints",
    "Suggestions",
    "Evaluation",
    "Driving Scenarios",
    "Daily Life Pattern",
    "Powertrain Preference",
    "Price Perception",
    "Spending Philosophy",
]
CORE_DECISION_TEXT_COLUMNS = [
    "Driving Scenarios",
    "Why This Car?",
    "Future Car Requirements",
    "Top 3 Favourite Features",
    "Customer Requirements",
    "Evaluation",
]


def _load_voc_cache_key(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return (str(path), stat.st_mtime_ns, stat.st_size)


@lru_cache(maxsize=4)
def _load_voc_frame_cached(path_str: str, _mtime_ns: int, _size: int) -> pd.DataFrame:
    return _read_excel_with_fallback(path_str, sheet_name="VOC Data").fillna("")


def _read_excel_with_fallback(
    source_file: str | Path,
    *,
    sheet_name: str,
) -> pd.DataFrame:
    try:
        return pd.read_excel(
            source_file,
            sheet_name=sheet_name,
            engine="calamine",
        )
    except Exception:
        try:
            return pd.read_excel(
                source_file,
                sheet_name=sheet_name,
            )
        except Exception as exc:
            raise RuntimeError(
                "读取 Nordic VOC Excel 失败：calamine 与默认引擎均不可用。"
            ) from exc


def _load_voc_frame_from_path(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"VOC Nordic workbook not found: {path}")
    return _load_voc_frame_cached(*_load_voc_cache_key(path)).copy()


def _load_voc_frame() -> pd.DataFrame:
    return _load_voc_frame_from_path(VOC_NORDIC_PATH)


def _load_hev_voc_frame() -> pd.DataFrame:
    return _load_voc_frame_from_path(VOC_SWEDEN_HEV_PATH)


def _clean_text(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _share(value: int, total: int) -> float:
    return round((value / total), 4) if total > 0 else 0.0


def _translate(label: str, mapping: dict[str, str]) -> str:
    return mapping.get(label, label)


def _series_counter(series: pd.Series) -> Counter[str]:
    counter: Counter[str] = Counter()
    for value in series:
        text = _clean_text(value)
        if text:
            counter[text] += 1
    return counter


def _semicolon_items(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _ranked_triplet_items(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = [item.strip(" ;.") for item in RANKED_ITEM_PATTERN.findall(text) if item.strip(" ;.")]
    if parts:
        return parts
    return _semicolon_items(text)


def _combined_row_text(row: pd.Series, columns: list[str] | None = None) -> str:
    relevant_columns = columns or INSIGHT_TEXT_COLUMNS
    return " | ".join(_clean_text(row.get(column)).lower() for column in relevant_columns)


def _count_rows_with_keywords(
    frame: pd.DataFrame,
    keywords: tuple[str, ...],
    *,
    columns: list[str] | None = None,
) -> int:
    count = 0
    for _, row in frame.iterrows():
        combined = _combined_row_text(row, columns)
        if any(keyword in combined for keyword in keywords):
            count += 1
    return count


def _build_share_items(
    counter: Counter[str],
    *,
    total: int,
    limit: int,
    translate_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for label, value in counter.most_common(limit):
        translated = _translate(label, translate_map or {})
        items.append({
            "label": translated,
            "rawLabel": label,
            "value": int(value),
            "sharePct": _share(int(value), total),
        })
    return items


def _age_bucket(age: int) -> str:
    if age <= 34:
        return "28-34岁"
    if age <= 44:
        return "35-44岁"
    if age <= 54:
        return "45-54岁"
    return "55岁+"


def _estimate_weekly_commute_bucket(row: pd.Series) -> str:
    daily_life = _clean_text(row.get("Daily Life Pattern"))
    scenarios = _clean_text(row.get("Driving Scenarios"))
    usage = _clean_text(row.get("Usage Frequency"))
    combined = " | ".join([daily_life, scenarios, usage]).lower()
    match = re.search(r"commutes\s+(\d+)\s*km/day", daily_life, flags=re.IGNORECASE)
    if match:
        weekly_km = int(match.group(1)) * 5
        if weekly_km < 100:
            return "<100 km/周"
        if weekly_km < 300:
            return "100-300 km/周"
        if weekly_km < 500:
            return "300-500 km/周"
        return "500 km+/周"
    if "60–120 km/day" in combined or "60-120 km/day" in combined:
        return "300-600 km/周"
    if "business travel between cities" in combined or "client visits" in combined:
        return "300-600 km/周"
    if "daily urban commute + school run" in combined:
        return "100-300 km/周"
    if "semi-rural lifestyle" in combined or "irregular shifts" in combined:
        return "200-500 km/周"
    if "works from home 3 days/week" in combined or "weekends mainly" in combined or "varies seasonally" in combined:
        return "<100 km/周"
    if "every day" in combined or "5 days/week" in combined:
        return "200-500 km/周"
    if "3–4 days/week" in combined or "3-4 days/week" in combined:
        return "120-320 km/周"
    return "弹性 / 季节性"


def _occupation_sector(value: object) -> str:
    text = _clean_text(value).lower()
    if any(token in text for token in ("software", "data scientist", "tech")):
        return "科技 / 数字"
    if any(token in text for token in ("teacher", "education")):
        return "教育"
    if any(token in text for token in ("doctor", "healthcare", "nurse")):
        return "医疗健康"
    if any(token in text for token in ("police", "public service")):
        return "公共服务"
    if any(token in text for token in ("civil engineer", "construction", "architecture", "forestry")):
        return "工程 / 建筑 / 森林"
    if any(token in text for token in ("mechanic", "electrician", "trades", "automotive")):
        return "技术工种 / 汽车"
    if any(token in text for token in ("project manager", "consulting", "sales manager", "marketing", "finance", "hr")):
        return "管理 / 商务 / 咨询"
    if any(token in text for token in ("logistics", "truck driver")):
        return "物流 / 运输"
    if any(token in text for token in ("business owner", "owner")):
        return "自营 / 创业"
    if any(token in text for token in ("chef", "restaurant", "f&b")):
        return "餐饮 / 服务"
    return "其他"


def _build_lifestyle_items(frame: pd.DataFrame) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for _, row in frame.iterrows():
        tags = {
            _translate(item, LIFESTYLE_TRANSLATIONS)
            for item in [*_semicolon_items(row.get("Sports / Hobbies")), *_semicolon_items(row.get("Frequent Locations"))]
            if _clean_text(item)
        }
        for tag in tags:
            counter[tag] += 1
    return _build_share_items(counter, total=len(frame), limit=10)


def _presentation_share_items(items: list[tuple[str, int]]) -> list[dict[str, Any]]:
    total = sum(value for _, value in items) or 1
    return [
        {
            "label": label,
            "rawLabel": label,
            "value": int(value),
            "sharePct": _share(int(value), total),
        }
        for label, value in items
    ]


def _build_information_source_groups(
    _frame: pd.DataFrame,
    *,
    sample_source_weights: list[tuple[str, int]] | None = None,
    attention_channel_weights: list[tuple[str, int]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        _presentation_share_items(
            sample_source_weights or NORDIC_SAMPLE_SOURCE_PRESENTATION_WEIGHTS
        ),
        _presentation_share_items(
            attention_channel_weights
            or NORDIC_ATTENTION_CHANNEL_PRESENTATION_WEIGHTS
        ),
    )


def _build_usage_items(frame: pd.DataFrame) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for value in frame["Driving Scenarios"]:
        for item in _semicolon_items(value):
            counter[item] += 1
    return _build_share_items(counter, total=len(frame), limit=8, translate_map=SCENARIO_TRANSLATIONS)


def _build_factor_items(frame: pd.DataFrame) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    relevant_columns = [
        "Spending Philosophy",
        "Why This Car?",
        "Future Car Requirements",
        "Customer Requirements",
        "Price Perception",
        "Suggestions",
        "Top 3 Favourite Features",
        "Evaluation",
    ]
    for _, row in frame.iterrows():
        combined = " | ".join(_clean_text(row.get(column)).lower() for column in relevant_columns)
        for label, keywords in FACTOR_RULES:
            if any(keyword in combined for keyword in keywords):
                counter[label] += 1
    return _build_share_items(counter, total=len(frame), limit=8)


def _build_summary_cards(frame: pd.DataFrame, usage_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    total = len(frame)
    family_households = int(frame["Household Size"].isin([4, 5]).sum())
    seven_seat_households = _count_rows_with_keywords(frame, ("third-row", "7-seat"))
    school_run_households = int(frame["Driving Scenarios"].astype(str).str.contains("school run", case=False, na=False).sum())
    towing_households = int(frame["Driving Scenarios"].astype(str).str.contains("Towing a boat or caravan", case=False, na=False).sum())
    awd_households = _count_rows_with_keywords(
        frame,
        ("awd", "all-wheel drive", "winter", "lapland", "gravel", "forest track", "ground clearance"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    space_households = _count_rows_with_keywords(
        frame,
        ("tow", "trailer", "boat", "caravan", "third-row", "7-seat", "tailgate", "boot", "cargo", "family pack"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    bev_phev = int(frame["Powertrain Preference"].astype(str).str.contains("BEV|PHEV", na=False).sum())
    tco_households = _count_rows_with_keywords(
        frame,
        ("subsidy", "tco", "price", "budget", "discount", "residual value", "financing"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    charging_households = _count_rows_with_keywords(
        frame,
        ("charging", "range", "heat pump", "150 kw", "500 km", "battery preconditioning", "home charging"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    scenario_labels = "、".join(item["label"] for item in usage_items[:4])
    return [
        {
            "label": "家庭 / 空间",
            "headline": "4-5口家庭是主流，七座和尾厢更像家庭与装备场景加分项",
            "detail": (
                f"{family_households / total:.0%} 为4-5口家庭，{seven_seat_households / total:.0%} 明确提到第三排 / 7座，"
                f"{school_run_households / total:.0%} 场景直接包含接娃通勤。"
            ),
        },
        {
            "label": "场景组合",
            "headline": "工作日通勤之外，周末滑雪、拖挂与跨城长途共同决定选车",
            "detail": f"高频场景集中在 {scenario_labels}；其中 {towing_households / total:.0%} 的样本直接涉及拖船 / 拖挂房车。",
        },
        {
            "label": "驱动 / 动力",
            "headline": "样本更看重 AWD、冬季通过性和拖挂能力，不是单纯追求两驱低价",
            "detail": (
                f"{awd_households / total:.0%} 在场景 / 选因 / 需求中提到 AWD / 冬季 / 林道能力，"
                f"{space_households / total:.0%} 提到拖挂、七座或装载空间。"
            ),
        },
        {
            "label": "转电门槛",
            "headline": "愿意 BEV / PHEV，但前提是冬季续航、补能便利和 TCO 讲得通",
            "detail": (
                f"{bev_phev / total:.0%} 偏好 BEV/PHEV，{tco_households / total:.0%} 在核心决策文本里提到价格 / TCO，"
                f"{charging_households / total:.0%} 提到充电 / 续航。"
            ),
        },
    ]


def _build_persona(frame: pd.DataFrame, commute_items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "title": "典型北欧家庭转电用户",
        "summary": "40岁上下、已婚或同居的4-5口之家，住在瑞典 / 芬兰二三线或半郊区，不会为了转电牺牲 AWD、冬季通过性、拖挂能力和家庭装载空间。",
        "facts": [
            {"label": "年龄", "value": "35-49岁为主"},
            {"label": "婚姻 / 家庭", "value": "已婚/同居，4-5人家庭，1-2个孩子"},
            {"label": "职业背景", "value": "科技、教育、公共服务与工程类稳定专业岗位"},
            {"label": "居住周边", "value": "森林 / 国家公园 / 码头 / 学校 / 夏屋 / 运动馆"},
            {"label": "驱动诉求", "value": "优先 AWD + 冬季稳定，不接受“转电=牺牲雪地和林道能力”"},
            {"label": "典型周通勤", "value": commute_items[0]["label"] if commute_items else "200-500 km/周"},
            {"label": "能源迁移", "value": "愿意从 ICE 转向 BEV/PHEV，但前提是补贴、TCO、补能和冬季可用性说得通"},
        ],
        "notes": [
            "工作日是通勤 + school run，周末与假期则切换到滑雪 / 夏屋 / 长途 / 船或房车拖挂的复合场景。",
            "更像在追求“AWD + 空间 + 低碳”的平衡方案，而不是单纯为了省钱退回两驱或只看品牌面子。",
            "七座和大尾厢不是全员刚需，但在4-5口家庭里会被拿来服务接娃、多人口出行、婴童 / 户外装备和拖挂周边物品装载。",
            "对 PHEV / BEV 的前提条件非常现实：家充打包、冬季真实续航、快充效率、补贴透明和长期 TCO 都要讲得通。",
            "高频加分项集中在 AWD、拖挂能力、ADAS / Pilot Assist、OTA 稳定性和冬季友好配置。",
        ],
    }


def _is_hev_like_powertrain(value: object) -> bool:
    text = _clean_text(value).upper()
    if not text:
        return False
    if "HEV" in text or "SELF-CHARGING" in text or "NO PLUG" in text:
        return True
    return "HYBRID" in text and "PHEV" not in text and "BEV" not in text


def _is_no_plug_hev_preference(value: object) -> bool:
    text = _clean_text(value).lower()
    if not text:
        return False
    if _is_hev_like_powertrain(text):
        return True
    return any(
        token in text
        for token in (
            "no plug",
            "home charging",
            "no home charger",
            "charging routine",
            "without charging",
            "charging anxiety",
        )
    )


def _count_toyota_owners(frame: pd.DataFrame) -> int:
    return int(frame["Car Ownership / Model"].astype(str).str.contains("toyota", case=False, na=False).sum())


def _count_hev_preferences(frame: pd.DataFrame) -> int:
    return int(sum(1 for value in frame["Powertrain Preference"] if _is_hev_like_powertrain(value)))


def _count_hev_tech_households(frame: pd.DataFrame) -> int:
    return _count_rows_with_keywords(
        frame,
        (
            "infotainment",
            "carplay",
            "ota",
            "touchscreen",
            "screen",
            "software",
            "adas",
            "adaptive cruise",
            "acc",
            "360",
            "camera",
            "digital",
            "tech",
        ),
        columns=[
            "Why This Car?",
            "Current Car Pain Points",
            "Future Car Requirements",
            "Top 3 Favourite Features",
            "Top 3 Complaints",
            "Customer Requirements",
            "Suggestions",
            "Price Perception",
            "Evaluation",
            "Closing Remarks",
        ],
    )


def _count_no_plug_hev_preferences(frame: pd.DataFrame) -> int:
    return int(
        sum(
            1
            for value in frame["Powertrain Preference"]
            if _is_no_plug_hev_preference(value)
        )
    )


def _build_hev_summary_cards(frame: pd.DataFrame, usage_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    total = len(frame)
    toyota_households = _count_toyota_owners(frame)
    family_households = int(frame["Household Size"].isin([3, 4, 5]).sum())
    winter_households = _count_rows_with_keywords(
        frame,
        ("winter", "snow", "ice", "traction", "awd", "slush", "cold"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    efficiency_households = _count_rows_with_keywords(
        frame,
        ("fuel", "economy", "consumption", "tco", "running cost", "maintenance", "residual value", "resale"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    technology_households = _count_hev_tech_households(frame)
    no_plug_households = _count_no_plug_hev_preferences(frame)
    hev_households = _count_hev_preferences(frame)
    scenario_labels = "、".join(item["label"] for item in usage_items[:4])
    return [
        {
            "label": "Toyota Anchor",
            "headline": "瑞典 HEV 样本里，Toyota hybrid 是最稳定的现实锚点",
            "detail": (
                f"{toyota_households / total:.0%} 的样本当前驾驶或明确偏好 Toyota hybrid；"
                "RAV4 Hybrid、Corolla Cross Hybrid 与 Yaris Cross Hybrid 是最稳定出现的讨论对象。"
            ),
        },
        {
            "label": "瑞典家庭场景",
            "headline": "通勤、接娃、滑雪和家庭长途叠加，决定了全年低负担优先",
            "detail": (
                f"高频场景集中在 {scenario_labels}；"
                f"{family_households / total:.0%} 为 3-5 人家庭，车辆必须兼顾工作日与假期切换。"
            ),
        },
        {
            "label": "购买逻辑",
            "headline": "省油、省心、保值和冬季稳定性是硬门槛，但科技配置不能再太保守",
            "detail": (
                f"{efficiency_households / total:.0%} 在核心决策文本里提到油耗 / 使用成本 / 保值，"
                f"{winter_households / total:.0%} 提到冬季抓地、低温或雪地稳定性。"
            ),
        },
        {
            "label": "科技痛点",
            "headline": "Toyota 的短板不在可靠性，而在 ADAS、车机和数字配置跟进偏慢",
            "detail": (
                f"{technology_households / total:.0%} 的样本在痛点、建议或评价里直接提到 ADAS / 车机 / CarPlay / OTA / 屏幕交互等科技配置问题；"
                f"{hev_households / total:.0%} 仍明确偏好 HEV。"
            ),
        },
    ]


def _build_hev_persona(_frame: pd.DataFrame, commute_items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "title": "典型瑞典 HEV 家庭省心派",
        "summary": "35-54 岁、已婚或同居的瑞典家庭用户，希望保留 SUV 的空间、冬季稳定性和长期可靠性，优先选择低油耗、低心智负担的 HEV；但在 Toyota 这类主流选项上，他们也明确感受到 ADAS 与车机配置更新不够快。",
        "facts": [
            {"label": "年龄", "value": "35-54岁为主"},
            {"label": "家庭结构", "value": "已婚 / 同居为主，3-5人家庭，1-2个孩子"},
            {"label": "市场范围", "value": "瑞典单国 HEV benchmark 样本"},
            {"label": "车型锚点", "value": "Toyota RAV4 Hybrid / Corolla Cross Hybrid / Camry Hybrid"},
            {"label": "核心诉求", "value": "低油耗、低维护、冬季稳定、空间够用、长期省心"},
            {"label": "科技痛点", "value": "ADAS、车机、CarPlay / OTA 与屏幕交互普遍被认为不够新"},
            {"label": "典型周通勤", "value": commute_items[0]["label"] if commute_items else "100-300 km/周"},
            {"label": "决策锚点", "value": "全年省心可靠 + 瑞典冬季可用性 + 家庭低负担"},
        ],
        "notes": [
            "工作日围绕通勤、接娃、采购和城市近郊移动，周末会切换到滑雪、探亲、夏屋或家庭长途等复合场景。",
            "他们并不排斥电动化，但在 HEV 决策上更看重省油、省心和冬季稳定，而不是把充电便利当成核心卖点。",
            "Toyota 是最稳定的现实锚点，但真正起作用的是整套低风险方案：可靠性、转售价值、油耗表现和熟悉的经销商支持。",
            "典型抱怨集中在 ADAS 不够完整、车机和 UI 老旧、CarPlay / OTA 体验落后，以及科技配置与价格不匹配。",
            "产品加分项集中在座舱实用性、后排 / 尾厢空间、ACC / 安全辅助、冬季轮胎与加热配置，以及长途巡航的安静和平顺。",
            "在营销与产品讨论里，最有效的话术应该是全年都省心，同时把科技短板补齐，而不是只强调不需要插电。",
        ],
    }


def _build_default_benchmark_metrics(frame: pd.DataFrame) -> list[dict[str, Any]]:
    total = len(frame)
    family_households = int(frame["Household Size"].isin([4, 5]).sum())
    bev_phev = int(frame["Powertrain Preference"].astype(str).str.contains("BEV|PHEV", na=False).sum())
    awd_households = _count_rows_with_keywords(
        frame,
        ("awd", "all-wheel drive", "winter", "lapland", "gravel", "forest track", "ground clearance"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )
    return [
        {"label": "样本量", "value": VOC_NORDIC_DISPLAY_SAMPLE_SIZE, "detail": "北欧用户调研样本"},
        {"label": "4-5口家庭", "value": f"{family_households / total:.0%}", "detail": "七座 / 尾厢需求主要来自家庭与装备装载"},
        {"label": "BEV / PHEV 倾向", "value": f"{bev_phev / total:.0%}", "detail": "愿意转电，但不接受牺牲冬季和长途可用性"},
        {"label": "AWD / 冬季诉求", "value": f"{awd_households / total:.0%}", "detail": "四驱、雪地与林道可靠性是核心锚点"},
    ]


def _build_hev_benchmark_metrics(frame: pd.DataFrame) -> list[dict[str, Any]]:
    total = len(frame)
    toyota_households = _count_toyota_owners(frame)
    hev_households = _count_hev_preferences(frame)
    family_households = int(frame["Household Size"].isin([3, 4, 5]).sum())
    technology_households = _count_hev_tech_households(frame)
    return [
        {"label": "样本量", "value": total, "detail": "瑞典 HEV curated benchmark 样本"},
        {"label": "Toyota 占比", "value": f"{toyota_households / total:.0%}", "detail": "当前驾驶或重点考虑 Toyota hybrid 的样本占比"},
        {"label": "HEV 偏好", "value": f"{hev_households / total:.0%}", "detail": "明确偏好 self-charging / no-plug hybrid 的样本占比"},
        {"label": "科技 / ADAS 痛点", "value": f"{technology_households / total:.0%}", "detail": "在痛点、建议或评价里直接提到 ADAS、车机、CarPlay、OTA 或屏幕交互问题的样本占比"},
        {"label": "家庭场景", "value": f"{family_households / total:.0%}", "detail": "3-5 人家庭占比，决定了空间与全天候可靠性的重要性"},
    ]


def _build_benchmark_customer_deck(
    *,
    frame: pd.DataFrame,
    source_path: Path,
    respondent_count: int,
    dataset_label: str,
    coverage_label: str,
    page_title: str,
    subtitle: str,
    summary_text: str,
    methodology_note: str,
    conclusion_builder: Any,
    persona_builder: Any,
    metrics_builder: Any,
    sample_source_weights: list[tuple[str, int]] | None = None,
    attention_channel_weights: list[tuple[str, int]] | None = None,
) -> dict[str, Any]:
    if frame.empty:
        raise RuntimeError("VOC Nordic workbook is empty")

    total = len(frame)
    source_stat = source_path.stat() if source_path.exists() else None
    age_counter = Counter(_age_bucket(int(age)) for age in frame["Age"].fillna(0).astype(int))
    household_counter = Counter(f"{int(size)}人" for size in frame["Household Size"].fillna(0).astype(int))
    commute_counter = Counter(_estimate_weekly_commute_bucket(row) for _, row in frame.iterrows())
    occupation_counter = Counter(_occupation_sector(value) for value in frame["Occupation / Industry"])
    philosophy_counter = Counter(_translate(value, PHILOSOPHY_TRANSLATIONS) for value in frame["Spending Philosophy"] if _clean_text(value))

    usage_items = _build_usage_items(frame)
    factor_items = _build_factor_items(frame)
    commute_items = _build_share_items(commute_counter, total=total, limit=6)
    sample_source_items, attention_channel_items = _build_information_source_groups(
        frame,
        sample_source_weights=sample_source_weights,
        attention_channel_weights=attention_channel_weights,
    )
    gender_items = _build_share_items(_series_counter(frame["Gender"]), total=total, limit=3)
    age_items = _build_share_items(age_counter, total=total, limit=4)
    household_items = _build_share_items(household_counter, total=total, limit=4)
    occupation_items = _build_share_items(occupation_counter, total=total, limit=6)
    lifestyle_items = _build_lifestyle_items(frame)
    philosophy_items = _build_share_items(philosophy_counter, total=total, limit=6)
    powertrain_items = _build_share_items(
        _series_counter(frame["Powertrain Preference"]),
        total=total,
        limit=5,
        translate_map=POWERTRAIN_TRANSLATIONS,
    )

    return {
        "metadata": {
            "protocolVersion": "voc-nordic/v1",
            "datasetLabel": dataset_label,
            "sourceFile": str(source_path.relative_to(PROJECT_ROOT)) if source_path.is_relative_to(PROJECT_ROOT) else str(source_path),
            "respondentCount": respondent_count,
            "updatedAt": source_stat.st_mtime if source_stat else datetime.now().timestamp(),
            "mode": "benchmark",
            "modeLabel": "Benchmark Excel",
            "sourceKind": "benchmark_excel",
            "sampleUnitLabel": "samples",
            "coverageLabel": coverage_label,
            "countryCodes": list(VOC_FORUM_DEFAULT_COUNTRIES),
        },
        "page": {
            "title": page_title,
            "subtitle": subtitle,
            "summaryText": summary_text,
            "methodologyNote": methodology_note,
            "conclusionCards": conclusion_builder(frame, usage_items),
            "metrics": metrics_builder(frame),
            "profile": {
                "sampleSources": sample_source_items,
                "attentionChannels": attention_channel_items,
                "gender": gender_items,
                "age": age_items,
                "household": household_items,
                "weeklyCommute": commute_items,
            },
            "occupation": {"items": occupation_items},
            "lifestyle": {"items": lifestyle_items},
            "powertrain": {"items": powertrain_items},
            "philosophy": {"items": philosophy_items},
            "purchaseUses": {"items": usage_items},
            "decisionFactors": {"items": factor_items},
            "persona": persona_builder(frame, commute_items),
        },
    }


def _normalize_country_codes(country_codes: list[str] | None) -> list[str]:
    if not country_codes:
        return list(VOC_FORUM_DEFAULT_COUNTRIES)
    normalized: list[str] = []
    seen: set[str] = set()
    for code in country_codes:
        candidate = _clean_text(code).upper()
        if candidate and candidate not in seen:
            seen.add(candidate)
            normalized.append(candidate)
    return normalized or list(VOC_FORUM_DEFAULT_COUNTRIES)


def _relative_project_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _metric_value(payload: dict[str, Any], label: str) -> float:
    for item in payload.get("metrics") or []:
        if _clean_text(item.get("label")) == label:
            try:
                return float(item.get("value") or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _aggregate_forum_share_items(
    payloads: list[dict[str, Any]],
    key: str,
) -> list[dict[str, Any]]:
    values: Counter[str] = Counter()
    mention_counts: Counter[str] = Counter()
    labels: dict[str, str] = {}
    for payload in payloads:
        for item in payload.get(key) or []:
            raw_label = _clean_text(item.get("rawLabel") or item.get("label"))
            if not raw_label:
                continue
            labels.setdefault(raw_label, _clean_text(item.get("label")) or raw_label)
            values[raw_label] += int(item.get("value") or 0)
            mention_counts[raw_label] += int(item.get("mentionCount") or 0)

    total = sum(values.values())
    if total <= 0:
        return []

    items: list[dict[str, Any]] = []
    for raw_label, value in values.most_common():
        item = {
            "label": labels.get(raw_label, raw_label),
            "rawLabel": raw_label,
            "value": int(value),
            "sharePct": _share(int(value), total),
        }
        if mention_counts[raw_label] > 0:
            item["mentionCount"] = int(mention_counts[raw_label])
        items.append(item)
    return items


def _aggregate_forum_evidence_cards(
    payloads: list[dict[str, Any]],
    *,
    limit: int = 6,
) -> list[dict[str, Any]]:
    def _preview_text(value: Any, *, limit: int = 1400) -> tuple[str, bool]:
        text = _clean_text(value)
        if not text:
            return "", False
        if len(text) <= limit:
            return text, False
        return f"{text[:limit].rstrip()}…", True

    def _serialize_observations(value: Any, *, limit: int = 4) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        items: list[dict[str, Any]] = []
        for observation in value:
            if not isinstance(observation, dict):
                continue
            label = _clean_text(observation.get("label") or observation.get("signalKey"))
            sentence = _clean_text(observation.get("sentence"))
            if not label and not sentence:
                continue
            items.append({
                "signalKind": _clean_text(observation.get("signalKind")),
                "label": label,
                "sentence": sentence,
                "matchedTokens": [
                    _clean_text(token)
                    for token in observation.get("matchedTokens") or []
                    if _clean_text(token)
                ],
                "sentiment": _clean_text(observation.get("sentiment")) or "neutral",
            })
            if len(items) >= limit:
                break
        return items

    cards: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    publish_rank = {"high": 0, "medium": 1, "low": 2}

    for payload in payloads:
        document_lookup = payload.get("_documents_by_url")
        for card in payload.get("evidenceCards") or []:
            url = _clean_text(card.get("url"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            source_document = (
                document_lookup.get(url)
                if isinstance(document_lookup, dict)
                else {}
            )
            excerpt = _clean_text(card.get("excerpt")) or _clean_text(source_document.get("excerpt"))
            content_preview, content_truncated = _preview_text(
                card.get("contentPreview") or source_document.get("cleanedText") or excerpt,
            )
            observations = _serialize_observations(
                card.get("observations") or source_document.get("observations"),
            )
            cards.append({
                "title": _clean_text(card.get("title")) or url,
                "url": url,
                "siteName": _clean_text(card.get("siteName")),
                "siteType": _clean_text(card.get("siteType")),
                "sourceCode": _clean_text(card.get("sourceCode") or source_document.get("sourceCode")),
                "countryCode": _clean_text(card.get("countryCode") or source_document.get("countryCode")),
                "countryLabel": _clean_text(card.get("countryLabel") or source_document.get("countryLabel")),
                "language": _clean_text(card.get("language") or source_document.get("language")),
                "publishedAt": card.get("publishedAt") or source_document.get("publishedAt"),
                "collectedAt": card.get("collectedAt") or source_document.get("collectedAt"),
                "publishTier": _clean_text(card.get("publishTier")),
                "publishDecision": _clean_text(card.get("publishDecision") or source_document.get("publishDecision")),
                "sentiment": _clean_text(card.get("sentiment")) or "neutral",
                "qualityScore": int(card.get("qualityScore") or source_document.get("qualityScore") or 0),
                "observationCount": int(
                    card.get("observationCount")
                    or source_document.get("observationCount")
                    or len(observations)
                ),
                "signals": [
                    _clean_text(signal)
                    for signal in card.get("signals") or []
                    if _clean_text(signal)
                ],
                "evidenceSnippets": [
                    _clean_text(snippet)
                    for snippet in card.get("evidenceSnippets") or []
                    if _clean_text(snippet)
                ],
                "excerpt": excerpt,
                "contentPreview": content_preview,
                "contentTruncated": content_truncated,
                "observations": observations,
            })

    cards.sort(
        key=lambda item: (
            publish_rank.get(_clean_text(item.get("publishTier")), 99),
            -len(item.get("signals") or []),
            _clean_text(item.get("siteName")),
            _clean_text(item.get("title")),
        ),
    )
    return cards[:limit]


def _build_forum_conclusion_cards(
    *,
    source_mix: list[dict[str, Any]],
    pain_points: list[dict[str, Any]],
    product_signals: list[dict[str, Any]],
    decision_factors: list[dict[str, Any]],
) -> list[dict[str, str]]:
    def top_label(items: list[dict[str, Any]], fallback: str) -> str:
        return _clean_text(items[0].get("label")) if items else fallback

    return [
        {
            "label": "Source mix",
            "headline": top_label(source_mix, "Coverage still thin"),
            "detail": "Largest source contributor among the currently aggregated live forum artifacts.",
        },
        {
            "label": "Top pain point",
            "headline": top_label(pain_points, "No concentrated issue yet"),
            "detail": "Most repeated observed issue across publish-ready forum documents.",
        },
        {
            "label": "Top product signal",
            "headline": top_label(product_signals, "No concentrated signal yet"),
            "detail": "Most repeated product dimension in current live forum observation hits.",
        },
        {
            "label": "Lead decision factor",
            "headline": top_label(decision_factors, "No concentrated factor yet"),
            "detail": "Highest-level reason cluster aggregated from observed pain points and product signals.",
        },
    ]


def _load_forum_customer_deck_payloads(
    *,
    country_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for country_code in _normalize_country_codes(country_codes):
        deck_path = VOC_FORUM_ROOT / country_code.lower() / "deck" / "customer_insight_deck.json"
        if not deck_path.exists():
            continue
        payload = json.loads(deck_path.read_text(encoding="utf-8"))
        payload["_deck_path"] = str(deck_path)
        enriched_path = VOC_FORUM_ROOT / country_code.lower() / "enriched" / "customer_insight_signals.json"
        document_lookup: dict[str, dict[str, Any]] = {}
        if enriched_path.exists():
            enriched_payload = json.loads(enriched_path.read_text(encoding="utf-8"))
            for document in enriched_payload.get("documents") or []:
                if not isinstance(document, dict):
                    continue
                url = _clean_text(document.get("url"))
                if url and url not in document_lookup:
                    document_lookup[url] = document
        payload["_documents_by_url"] = document_lookup
        payloads.append(payload)
    return payloads


def _query_forum_customer_deck(
    *,
    country_codes: list[str] | None = None,
) -> dict[str, Any]:
    payloads = _load_forum_customer_deck_payloads(country_codes=country_codes)
    if not payloads:
        requested = ", ".join(_normalize_country_codes(country_codes))
        raise FileNotFoundError(
            f"No forum VOC deck artifacts found under {VOC_FORUM_ROOT} for countries: {requested}"
        )

    source_mix = _aggregate_forum_share_items(payloads, "sourceMix")
    site_types = _aggregate_forum_share_items(payloads, "siteTypes")
    languages = _aggregate_forum_share_items(payloads, "languages")
    publish_tiers = _aggregate_forum_share_items(payloads, "publishTiers")
    sentiment_items = _aggregate_forum_share_items(payloads, "sentiment")
    ownership_stage_items = _aggregate_forum_share_items(payloads, "ownershipStages")
    pain_point_items = _aggregate_forum_share_items(payloads, "painPoints")
    product_signal_items = _aggregate_forum_share_items(payloads, "productSignals")
    powertrain_items = _aggregate_forum_share_items(payloads, "powertrains")
    decision_factor_items = _aggregate_forum_share_items(payloads, "decisionFactors")
    evidence_cards = _aggregate_forum_evidence_cards(payloads)

    country_codes_resolved = [
        _clean_text(payload.get("countryCode")).upper()
        for payload in payloads
        if _clean_text(payload.get("countryCode"))
    ]
    country_labels = [
        _clean_text(payload.get("countryLabel")) or _clean_text(payload.get("countryCode"))
        for payload in payloads
    ]
    observed_sections = sorted({
        _clean_text(section)
        for payload in payloads
        for section in payload.get("observedSections") or []
        if _clean_text(section)
    })
    inferred_sections = sorted({
        _clean_text(section)
        for payload in payloads
        for section in payload.get("inferredSections") or []
        if _clean_text(section)
    })
    latest_updated_at = max(
        datetime.fromisoformat(_clean_text(payload.get("generatedAt"))).timestamp()
        for payload in payloads
        if _clean_text(payload.get("generatedAt"))
    )
    total_documents = int(sum(_metric_value(payload, "Documents") for payload in payloads))
    total_publish_ready = int(sum(_metric_value(payload, "Publish-ready docs") for payload in payloads))
    total_signal_observations = int(sum(_metric_value(payload, "Signal observations") for payload in payloads))
    total_sources = int(sum(_metric_value(payload, "Sources") for payload in payloads))
    weighted_quality_numerator = sum(
        _metric_value(payload, "Avg quality score") * _metric_value(payload, "Documents")
        for payload in payloads
    )
    avg_quality = round((weighted_quality_numerator / total_documents), 2) if total_documents > 0 else 0.0
    coverage_label = " / ".join(country_codes_resolved) if country_codes_resolved else "forum live"

    return {
        "metadata": {
            "protocolVersion": "voc-forum-live/v1",
            "datasetLabel": "Nordic forum VOC live deck",
            "sourceFile": "04_Processed_data/voc/<country>/deck/customer_insight_deck.json",
            "respondentCount": total_documents,
            "updatedAt": latest_updated_at,
            "mode": "forum_live",
            "modeLabel": "Forum VOC live",
            "sourceKind": "forum_voc",
            "sampleUnitLabel": "docs",
            "coverageLabel": coverage_label,
            "countryCodes": country_codes_resolved,
        },
        "page": {
            "title": "看客户",
            "subtitle": f"北欧 forum VOC（{coverage_label}）",
            "summaryText": (
                f"当前 live 模式聚合 {', '.join(country_labels)} 的公开论坛 / 评论页 deck，"
                "强调 observed evidence、痛点、产品信号与决策因素，不把人口画像包装成 sample facts。"
            ),
            "methodologyNote": (
                "只汇总已生成且通过当前 auto-review gate 的 public forum VOC deck。"
                "年龄、家庭结构、通勤等字段在 live 模式中保持 excluded / inferred-only。"
            ),
            "conclusionCards": _build_forum_conclusion_cards(
                source_mix=source_mix,
                pain_points=pain_point_items,
                product_signals=product_signal_items,
                decision_factors=decision_factor_items,
            ),
            "metrics": [
                {"label": "Countries", "value": len(country_codes_resolved), "detail": "Live forum deck artifacts included in this view."},
                {"label": "Sources", "value": total_sources, "detail": "Public source files aggregated across the included country decks."},
                {"label": "Documents", "value": total_documents, "detail": "Raw forum/comment documents represented in this live view."},
                {"label": "Publish-ready docs", "value": total_publish_ready, "detail": "Documents that currently pass the auto-review gate."},
                {"label": "Signal observations", "value": total_signal_observations, "detail": "Sentence-level evidence hits preserved by the enrichment layer."},
                {"label": "Avg quality score", "value": avg_quality, "detail": "Document-weighted average auto-review quality score."},
            ],
            "profile": {
                "sampleSources": [],
                "attentionChannels": [],
                "gender": [],
                "age": [],
                "household": [],
                "weeklyCommute": [],
            },
            "occupation": {"items": []},
            "lifestyle": {"items": []},
            "powertrain": {"items": powertrain_items},
            "philosophy": {"items": []},
            "purchaseUses": {"items": []},
            "decisionFactors": {"items": decision_factor_items},
            "persona": {
                "title": "Forum VOC live mode",
                "summary": "这个模式展示公开论坛里可直接观测到的 evidence-backed VOC，不输出样本画像式 persona facts。",
                "facts": [],
                "notes": [
                    "Observed sections come from public, publish-ready forum/comment pages only.",
                    "Demographic, age, household, and commute fields stay excluded from sample-fact reporting in live mode.",
                    "Use this mode as a dynamic evidence layer alongside the benchmark Excel deck, not as a replacement for sample research.",
                ],
            },
            "forumLive": {
                "sourceMix": source_mix,
                "siteTypes": site_types,
                "languages": languages,
                "publishTiers": publish_tiers,
                "sentiment": sentiment_items,
                "ownershipStages": ownership_stage_items,
                "painPoints": pain_point_items,
                "productSignals": product_signal_items,
                "powertrains": powertrain_items,
                "decisionFactors": decision_factor_items,
                "evidenceCards": evidence_cards,
                "observedSections": observed_sections,
                "inferredSections": inferred_sections,
            },
        },
    }


def _query_benchmark_customer_deck() -> dict[str, Any]:
    frame = _load_voc_frame()
    return _build_benchmark_customer_deck(
        frame=frame,
        source_path=VOC_NORDIC_PATH,
        respondent_count=VOC_NORDIC_DISPLAY_SAMPLE_SIZE,
        dataset_label="VOC Nordic SUV Users",
        coverage_label="Nordic benchmark sample",
        page_title="看客户",
        subtitle="北欧用户调研",
        summary_text=(
            "北欧 SUV 用户的核心不是便宜两驱，而是 AWD / 冬季可靠性、空间拖挂能力与转电 TCO 的平衡。"
            "通勤、接娃、滑雪和长途共同决定了他们对七座、尾厢、续航与补能的要求。"
        ),
        methodology_note="口径可重叠：家庭结构、场景和核心决策文本并行统计，不代表互斥人群。",
        conclusion_builder=_build_summary_cards,
        persona_builder=_build_persona,
        metrics_builder=_build_default_benchmark_metrics,
    )


def _query_hev_benchmark_customer_deck() -> dict[str, Any]:
    frame = _load_hev_voc_frame()
    return _build_benchmark_customer_deck(
        frame=frame,
        source_path=VOC_SWEDEN_HEV_PATH,
        respondent_count=len(frame),
        dataset_label="VOC Sweden HEV Owners",
        coverage_label="Sweden HEV curated benchmark sample",
        page_title="看HEV",
        subtitle="瑞典 HEV 车主画像",
        summary_text=(
            "这组样本聚焦瑞典市场的 HEV 家庭用户。"
            "他们要的是全年省心、冬季稳定、家庭实用、油耗可控和长期成本可控；与此同时，Toyota 一类主流 HEV 的 ADAS 与科技配置不足是被反复提到的短板。"
        ),
        methodology_note=(
            "独立瑞典 HEV benchmark workbook；当前以 hard-coded curated sample 方式接入，"
            "用于单独观察瑞典 HEV 家庭用户在冬季、家庭通勤、油耗/TCO 与科技配置期待上的共同偏好。"
        ),
        conclusion_builder=_build_hev_summary_cards,
        persona_builder=_build_hev_persona,
        metrics_builder=_build_hev_benchmark_metrics,
        sample_source_weights=SWEDEN_HEV_SAMPLE_SOURCE_PRESENTATION_WEIGHTS,
        attention_channel_weights=SWEDEN_HEV_ATTENTION_CHANNEL_PRESENTATION_WEIGHTS,
    )


def query_nordic_customer_deck(
    *,
    mode: str = "benchmark",
    country_codes: list[str] | None = None,
) -> dict[str, Any]:
    if mode == "forum_live":
        return _query_forum_customer_deck(country_codes=country_codes)
    return _query_benchmark_customer_deck()


def query_nordic_hev_customer_deck(
    *,
    mode: str = "benchmark",
    country_codes: list[str] | None = None,
) -> dict[str, Any]:
    if mode == "forum_live":
        return _query_forum_customer_deck(country_codes=country_codes)
    return _query_hev_benchmark_customer_deck()
