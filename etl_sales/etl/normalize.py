from __future__ import annotations

import re
from typing import Dict, Iterable, Mapping, Optional, Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode

HEADER_SANITIZE_RE = re.compile(r"[^0-9a-zA-Z_]+")
ARTICUL_DIGITS_RE = re.compile(r"\d")
ARTICUL_PATTERN = re.compile(r"^(?P<p1>\d{4})[- ]?(?P<p2>\d{3})[- ]?(?P<p3>\d{2})$")


def normalize_header(name: str) -> str:
    normalized = unidecode(str(name)).strip().lower().replace(" ", "_")
    normalized = HEADER_SANITIZE_RE.sub("_", normalized)
    normalized = normalized.strip("_")
    return normalized


def build_alias_lookup(alias_map: Mapping[str, Iterable[str]]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for canonical, aliases in alias_map.items():
        lookup[normalize_header(canonical)] = canonical
        for alias in aliases:
            lookup[normalize_header(alias)] = canonical
    return lookup


def map_columns(columns: Iterable[str], alias_lookup: Mapping[str, str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return mapping original->canonical and original->other for unknown."""
    canonical_map: Dict[str, str] = {}
    other_map: Dict[str, str] = {}
    used: Dict[str, int] = {}
    for column in columns:
        normalized = normalize_header(column)
        canonical = alias_lookup.get(normalized)
        if canonical:
            if canonical in used:
                used[canonical] += 1
                canonical_name = f"{canonical}_{used[canonical]}"
            else:
                used[canonical] = 0
                canonical_name = canonical
            canonical_map[column] = canonical_name
        else:
            safe = normalize_header(column) or "column"
            other_name = f"Other_{safe}"
            if other_name in other_map.values():
                suffix = 1
                while f"{other_name}_{suffix}" in other_map.values():
                    suffix += 1
                other_name = f"{other_name}_{suffix}"
            other_map[column] = other_name
    return canonical_map, other_map


def normalize_articul(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    text = unidecode(str(value)).strip()
    if not text:
        return None
    digits = "".join(ARTICUL_DIGITS_RE.findall(text))
    if len(digits) < 9:
        return None
    digits = digits[:9]
    match = ARTICUL_PATTERN.match(digits)
    if match:
        groups = match.groupdict()
        return f"{groups['p1']}-{groups['p2']}-{groups['p3']}"
    if len(digits) == 9:
        return f"{digits[0:4]}-{digits[4:7]}-{digits[7:9]}"
    return None


def normalize_articul_series(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    normalized = series.apply(normalize_articul)
    invalid_mask = normalized.isna() & series.notna() & series.astype(str).str.strip().ne("")
    return normalized, invalid_mask


def clean_articul_store(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().replace({"nan": None, "None": None})


def coerce_int(series: pd.Series) -> pd.Series:
    result = pd.to_numeric(series.replace({"-": None, "": None}), errors="coerce").fillna(0)
    return result.astype(int)


def coerce_float(series: pd.Series) -> pd.Series:
    series = series.astype(str).str.replace(",", ".", regex=False)
    result = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return result.astype(float)


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = np.nan
    return df


def reorder_columns(df: pd.DataFrame, preferred_order: Iterable[str]) -> pd.DataFrame:
    preferred = [column for column in preferred_order if column in df.columns]
    others = [column for column in df.columns if column not in preferred]
    return df[preferred + others]
