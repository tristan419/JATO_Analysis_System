from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import chromadb
except ModuleNotFoundError as exc:
    chromadb = None
    _CHROMADB_IMPORT_ERROR = exc
else:
    _CHROMADB_IMPORT_ERROR = None

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DB_DIR = PROJECT_ROOT / "04_Processed_data" / "chroma_db"
DEFAULT_COLLECTION_NAME = "vehicle_wiki"
DEFAULT_MANIFEST_NAME = "vehicle_wiki_manifest.json"
DEFAULT_EMBED_DIMENSIONS = 256
NORMALIZED_MSRP_COLUMN = "MSRP规整"
NORMALIZED_MSRP_CURRENCY = "EUR"

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+")
_GROUP_COLUMNS = [
    "Countries",
    "Make",
    "Model",
    "Trim level",
    "Powertrain type",
    "Version name",
]
_TOKEN_ALIASES = {
    "msrp": "price",
    "price": "price",
    "pricing": "price",
    "售价": "price",
    "价格": "price",
    "定价": "price",
    "retail": "price",
    "base": "price",
    "dimension": "dimension",
    "dimensions": "dimension",
    "size": "dimension",
    "尺寸": "dimension",
    "长宽高": "dimension",
    "length": "length",
    "长度": "length",
    "width": "width",
    "宽度": "width",
    "height": "height",
    "高度": "height",
    "wheelbase": "wheelbase",
    "轴距": "wheelbase",
    "trim": "trim",
    "版型": "trim",
    "配置": "trim",
    "version": "version",
}


def _resolve_db_dir(db_path: str | Path | None = None) -> Path:
    if db_path is None:
        configured = str(os.getenv("APP_LOCAL_WIKI_DB_PATH", "")).strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return DEFAULT_DB_DIR
    return Path(db_path).expanduser().resolve()


def get_local_wiki_collection_name() -> str:
    configured = str(os.getenv("APP_LOCAL_WIKI_COLLECTION", "")).strip()
    return configured or DEFAULT_COLLECTION_NAME


def get_local_wiki_manifest_path(
    db_path: str | Path | None = None,
) -> Path:
    return _resolve_db_dir(db_path) / DEFAULT_MANIFEST_NAME


def clear_local_wiki_caches() -> None:
    _get_client.cache_clear()
    _get_collection.cache_clear()


def _require_chromadb() -> None:
    if chromadb is None:
        raise RuntimeError(
            "chromadb is not installed; local wiki features are unavailable"
        ) from _CHROMADB_IMPORT_ERROR


@lru_cache(maxsize=8)
def _get_client(db_dir: str) -> chromadb.PersistentClient:
    _require_chromadb()
    return chromadb.PersistentClient(path=db_dir)


def get_local_wiki_client(
    db_path: str | Path | None = None,
) -> chromadb.PersistentClient:
    resolved = _resolve_db_dir(db_path)
    resolved.mkdir(parents=True, exist_ok=True)
    return _get_client(str(resolved))


@lru_cache(maxsize=16)
def _get_collection(
    db_dir: str,
    collection_name: str,
):
    client = _get_client(db_dir)
    return client.get_collection(name=collection_name)


def get_local_wiki_collection(
    db_path: str | Path | None = None,
    collection_name: str | None = None,
):
    resolved = _resolve_db_dir(db_path)
    return _get_collection(
        str(resolved),
        collection_name or get_local_wiki_collection_name(),
    )


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _normalize_text(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _normalize_meta(value: Any, *, upper: bool = False) -> str:
    text = _normalize_text(value)
    return text.upper() if upper else text


def _format_number(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        number = float(value)
        if number.is_integer():
            return str(int(number))
        return f"{number:.2f}".rstrip("0").rstrip(".")
    text = str(value).strip()
    return text or "N/A"


def _tokenize(text: str) -> list[str]:
    lowered = str(text).lower().strip()
    tokens = [_TOKEN_ALIASES.get(tok, tok) for tok in _TOKEN_PATTERN.findall(lowered)]
    if not tokens:
        return []
    bigrams = [
        f"{tokens[idx]}::{tokens[idx + 1]}"
        for idx in range(len(tokens) - 1)
    ]
    return tokens + bigrams


def embed_text(
    text: str,
    *,
    dimensions: int = DEFAULT_EMBED_DIMENSIONS,
) -> list[float]:
    vector = [0.0] * max(8, int(dimensions))
    for token in _tokenize(text):
        digest = hashlib.blake2b(
            token.encode("utf-8"),
            digest_size=16,
        ).digest()
        index = int.from_bytes(digest[:4], "little") % len(vector)
        sign = -1.0 if digest[4] % 2 else 1.0
        weight = 1.0 + min(len(token), 12) / 12.0
        vector[index] += sign * weight

    norm = math.sqrt(sum(value * value for value in vector))
    if not norm:
        return vector
    return [value / norm for value in vector]


def _build_search_tags(row: pd.Series) -> str:
    values = [
        _normalize_meta(row.get("Make"), upper=True),
        _normalize_meta(row.get("Model"), upper=True),
        _normalize_text(row.get("Trim level")),
        _normalize_text(row.get("Version name")),
        _normalize_text(row.get("Powertrain type")),
        "price msrp 售价 价格 定价 retail base",
        "dimension 尺寸 长宽高 length width height wheelbase 轴距",
        "trim version 配置 版型",
    ]
    return " ".join(part for part in values if part)


def build_vehicle_documents(
    frame: pd.DataFrame,
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    available_group_cols = [
        column for column in _GROUP_COLUMNS if column in frame.columns
    ]
    if not available_group_cols:
        raise ValueError("frame 缺少构建 local wiki 所需的关键列")

    valid_frame = frame.dropna(subset=available_group_cols)
    if valid_frame.empty:
        return [], [], []

    documents: list[str] = []
    metadatas: list[dict[str, Any]] = []
    ids: list[str] = []

    grouped = valid_frame.groupby(available_group_cols, dropna=False)
    for group_key, group_frame in grouped:
        row = group_frame.iloc[0]
        key_parts = (
            list(group_key)
            if isinstance(group_key, tuple)
            else [group_key]
        )
        raw_key = "|".join(_normalize_text(part) for part in key_parts)
        doc_id = hashlib.blake2b(
            raw_key.encode("utf-8"),
            digest_size=12,
        ).hexdigest()

        country = _normalize_meta(row.get("Countries"))
        brand = _normalize_meta(row.get("Make"), upper=True)
        model = _normalize_meta(row.get("Model"), upper=True)
        trim = _normalize_text(row.get("Trim level"))
        version = _normalize_text(row.get("Version name"))
        powertrain = _normalize_text(row.get("Powertrain type"))
        currency = _normalize_text(row.get("Currency"))
        normalized_msrp = _format_number(row.get(NORMALIZED_MSRP_COLUMN))
        base_price = _format_number(row.get("Base price"))
        retail_price = _format_number(row.get("Retail price"))
        length = _format_number(row.get("length (mm)"))
        width = _format_number(row.get("width (mm)"))
        height = _format_number(row.get("height (mm)"))
        wheelbase = _format_number(row.get("wheelbase (mm)"))
        body_type = _normalize_text(row.get("Body type"))
        fuel_type = _normalize_text(row.get("Fuel type"))
        transmission = _normalize_text(row.get("Transmission type"))
        drive = _normalize_text(row.get("Driven wheels"))
        battery_range = _format_number(row.get("Battery range"))
        seating = _format_number(row.get("Seating capacity"))
        cargo = _format_number(row.get("cargo volume (l)"))

        document_lines = [
            "## Vehicle Specification Fact Sheet",
            f"- CountryMarket: {country or 'N/A'}",
            f"- Brand: {brand or 'N/A'}",
            f"- Model: {model or 'N/A'}",
            f"- Trim Level: {trim or 'N/A'}",
            f"- Version Name: {version or 'N/A'}",
            f"- Powertrain: {powertrain or 'N/A'}",
            f"- Body Type: {body_type or 'N/A'}",
            f"- Fuel Type: {fuel_type or 'N/A'}",
            f"- Transmission: {transmission or 'N/A'}",
            f"- Driven Wheels: {drive or 'N/A'}",
            (
                "- Dimensions: "
                f"Length {length} mm | Width {width} mm | "
                f"Height {height} mm | Wheelbase {wheelbase} mm"
            ),
            (
                "- Price: "
                f"MSRP {normalized_msrp} {NORMALIZED_MSRP_CURRENCY} (normalized) | "
                f"Base {base_price} {currency} | "
                f"Retail {retail_price} {currency}"
            ),
            f"- Battery Range: {battery_range} km",
            f"- Seating Capacity: {seating}",
            f"- Cargo Volume: {cargo} L",
            f"- Search Tags: {_build_search_tags(row)}",
        ]

        documents.append("\n".join(document_lines))
        metadatas.append(
            {
                "country": country,
                "brand": brand,
                "model": model,
                "trim": trim,
                "powertrain": powertrain,
                "version": version,
            }
        )
        ids.append(doc_id)

    return documents, metadatas, ids


def build_where_clause(
    *,
    country: str = "",
    brand: str = "",
    model: str = "",
) -> dict[str, Any] | None:
    filters: list[dict[str, Any]] = []
    normalized_country = _normalize_meta(country)
    normalized_brand = _normalize_meta(brand, upper=True)
    normalized_model = _normalize_meta(model, upper=True)

    if normalized_country:
        filters.append({"country": {"$eq": normalized_country}})
    if normalized_brand:
        filters.append({"brand": {"$eq": normalized_brand}})
    if normalized_model:
        filters.append({"model": {"$eq": normalized_model}})

    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return {"$and": filters}


def build_vehicle_wiki_from_dataframe(
    frame: pd.DataFrame,
    *,
    source_path: str | Path | None = None,
    db_path: str | Path | None = None,
    collection_name: str | None = None,
    batch_size: int = 1000,
) -> dict[str, Any]:
    documents, metadatas, ids = build_vehicle_documents(frame)
    if not documents:
        raise ValueError("没有可写入 vehicle_wiki 的文档")

    resolved_db_dir = _resolve_db_dir(db_path)
    resolved_db_dir.mkdir(parents=True, exist_ok=True)
    resolved_collection = collection_name or get_local_wiki_collection_name()

    clear_local_wiki_caches()
    client = get_local_wiki_client(resolved_db_dir)
    try:
        client.delete_collection(name=resolved_collection)
    except Exception:
        pass

    collection = client.get_or_create_collection(name=resolved_collection)
    for start in range(0, len(documents), max(1, int(batch_size))):
        stop = min(start + max(1, int(batch_size)), len(documents))
        batch_docs = documents[start:stop]
        batch_embeddings = [embed_text(document) for document in batch_docs]
        collection.upsert(
            ids=ids[start:stop],
            documents=batch_docs,
            metadatas=metadatas[start:stop],
            embeddings=batch_embeddings,
        )

    actual_count = collection.count()
    if actual_count != len(documents):
        raise RuntimeError(
            "vehicle_wiki 建库后文档数量异常: "
            f"expected={len(documents)} actual={actual_count}"
        )

    manifest = {
        "collectionName": resolved_collection,
        "documentCount": actual_count,
        "embeddingKind": "deterministic-hash-v1",
        "embeddingDimensions": DEFAULT_EMBED_DIMENSIONS,
        "dbPath": str(resolved_db_dir),
        "sourcePath": str(source_path) if source_path else None,
    }
    manifest_path = get_local_wiki_manifest_path(resolved_db_dir)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    clear_local_wiki_caches()
    return manifest


def query_local_wiki_documents(
    query: str,
    *,
    country: str = "",
    brand: str = "",
    model: str = "",
    limit: int = 5,
    db_path: str | Path | None = None,
    collection_name: str | None = None,
) -> list[str]:
    normalized_query = str(query).strip()
    if not normalized_query:
        return []

    resolved_db_dir = _resolve_db_dir(db_path)
    resolved_collection = collection_name or get_local_wiki_collection_name()
    try:
        collection = get_local_wiki_collection(
            resolved_db_dir,
            resolved_collection,
        )
    except Exception as exc:
        log.warning("Local wiki collection unavailable: %s", exc)
        return []

    query_text = " ".join(
        part
        for part in [normalized_query, country, brand, model]
        if str(part).strip()
    )
    where_clause = build_where_clause(
        country=country,
        brand=brand,
        model=model,
    )

    try:
        result = collection.query(
            query_embeddings=[embed_text(query_text)],
            n_results=max(1, int(limit)),
            where=where_clause,
            include=["documents", "distances", "metadatas"],
        )
    except Exception as exc:
        log.warning("Local wiki query failed: %s", exc)
        return []

    documents = result.get("documents") or [[]]
    return [
        str(document)
        for document in documents[0]
        if str(document).strip()
    ]
