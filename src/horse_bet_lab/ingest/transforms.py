from __future__ import annotations

import re
from datetime import date

NULL_TOKENS = {"", " ", "00000000", "0000", "-", "NULL", "null"}


def decode_text(raw_bytes: bytes) -> str:
    for encoding in ("cp932", "utf-8"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="strict")


def decode_text_lossy(raw_bytes: bytes) -> str:
    for encoding in ("cp932", "utf-8"):
        try:
            return raw_bytes.decode(encoding, errors="replace")
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="replace")


def normalize_missing(value: str) -> str | None:
    stripped = value.strip()
    if stripped in NULL_TOKENS:
        return None
    return stripped


def to_text(value: str) -> str | None:
    return normalize_missing(value)


def to_int(value: str) -> int | None:
    normalized = normalize_missing(value)
    if normalized is None:
        return None
    return int(normalized)


def to_float(value: str) -> float | None:
    normalized = normalize_missing(value)
    if normalized is None:
        return None
    return float(normalized)


def to_date(value: str) -> date | None:
    normalized = normalize_missing(value)
    if normalized is None:
        return None
    return date.fromisoformat(f"{normalized[0:4]}-{normalized[4:6]}-{normalized[6:8]}")


def to_bac_entry_count(value: str) -> int | None:
    matches = re.findall(r"\d+", value)
    if not matches:
        return None
    return int(matches[-1])


def to_bac_race_name(value: str) -> str | None:
    normalized = normalize_missing(value)
    if normalized is None:
        return None
    cleaned = re.sub(r"[0-9 ]+$", "", normalized).strip()
    if not cleaned:
        return None
    return cleaned
