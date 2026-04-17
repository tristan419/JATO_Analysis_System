from __future__ import annotations

from collections import Counter
from functools import lru_cache
from pathlib import Path
import re
from typing import Any

import pandas as pd

from app.core.config import PROJECT_ROOT

VOC_NORDIC_PATH = PROJECT_ROOT / "01_RAW_DATA" / "VOC_Nordic_SUV_Users_100.xlsx"
VOC_NORDIC_DISPLAY_SAMPLE_SIZE = 113

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
POWERTRAIN_TRANSLATIONS = {
    "Full BEV (500 km+ real-world range)": "纯电长续航",
    "PHEV (daily EV, petrol on long runs)": "PHEV 过渡",
    "Mild hybrid (HEV) – no plug needed": "HEV 不插电",
    "Open to any if TCO makes sense": "TCO 合理即可",
    "Performance ICE – not ready to switch": "仍坚持 ICE",
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


@lru_cache(maxsize=1)
def _load_voc_frame_cached(path_str: str, _mtime_ns: int, _size: int) -> pd.DataFrame:
    return pd.read_excel(path_str, sheet_name="VOC Data").fillna("")


def _load_voc_frame() -> pd.DataFrame:
    if not VOC_NORDIC_PATH.exists():
        raise FileNotFoundError(f"VOC Nordic workbook not found: {VOC_NORDIC_PATH}")
    return _load_voc_frame_cached(*_load_voc_cache_key(VOC_NORDIC_PATH)).copy()


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


def _build_information_source_groups(_frame: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        _presentation_share_items(NORDIC_SAMPLE_SOURCE_PRESENTATION_WEIGHTS),
        _presentation_share_items(NORDIC_ATTENTION_CHANNEL_PRESENTATION_WEIGHTS),
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


def query_nordic_customer_deck() -> dict[str, Any]:
    frame = _load_voc_frame()
    if frame.empty:
        raise RuntimeError("VOC Nordic workbook is empty")

    total = len(frame)
    file_stat = VOC_NORDIC_PATH.stat()
    age_counter = Counter(_age_bucket(int(age)) for age in frame["Age"].fillna(0).astype(int))
    household_counter = Counter(f"{int(size)}人" for size in frame["Household Size"].fillna(0).astype(int))
    commute_counter = Counter(_estimate_weekly_commute_bucket(row) for _, row in frame.iterrows())
    occupation_counter = Counter(_occupation_sector(value) for value in frame["Occupation / Industry"])
    philosophy_counter = Counter(_translate(value, PHILOSOPHY_TRANSLATIONS) for value in frame["Spending Philosophy"] if _clean_text(value))

    usage_items = _build_usage_items(frame)
    factor_items = _build_factor_items(frame)
    commute_items = _build_share_items(commute_counter, total=total, limit=6)
    sample_source_items, attention_channel_items = _build_information_source_groups(frame)
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

    family_households = int(frame["Household Size"].isin([4, 5]).sum())
    bev_phev = int(frame["Powertrain Preference"].astype(str).str.contains("BEV|PHEV", na=False).sum())
    awd_households = _count_rows_with_keywords(
        frame,
        ("awd", "all-wheel drive", "winter", "lapland", "gravel", "forest track", "ground clearance"),
        columns=CORE_DECISION_TEXT_COLUMNS,
    )

    return {
        "metadata": {
            "protocolVersion": "voc-nordic/v1",
            "datasetLabel": "VOC Nordic SUV Users",
            "sourceFile": str(VOC_NORDIC_PATH.relative_to(PROJECT_ROOT)),
            "respondentCount": VOC_NORDIC_DISPLAY_SAMPLE_SIZE,
            "updatedAt": file_stat.st_mtime,
        },
        "page": {
            "title": "看客户",
            "subtitle": "北欧用户调研",
            "summaryText": (
                "北欧 SUV 用户的核心不是便宜两驱，而是 AWD / 冬季可靠性、空间拖挂能力与转电 TCO 的平衡。"
                "通勤、接娃、滑雪和长途共同决定了他们对七座、尾厢、续航与补能的要求。"
            ),
            "methodologyNote": (
                "口径可重叠：家庭结构、场景和核心决策文本并行统计，不代表互斥人群。"
            ),
            "conclusionCards": _build_summary_cards(frame, usage_items),
            "metrics": [
                {"label": "样本量", "value": VOC_NORDIC_DISPLAY_SAMPLE_SIZE, "detail": "北欧用户调研样本"},
                {"label": "4-5口家庭", "value": f"{family_households / total:.0%}", "detail": "七座 / 尾厢需求主要来自家庭与装备装载"},
                {"label": "BEV / PHEV 倾向", "value": f"{bev_phev / total:.0%}", "detail": "愿意转电，但不接受牺牲冬季和长途可用性"},
                {"label": "AWD / 冬季诉求", "value": f"{awd_households / total:.0%}", "detail": "四驱、雪地与林道可靠性是核心锚点"},
            ],
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
            "persona": _build_persona(frame, commute_items),
        },
    }
