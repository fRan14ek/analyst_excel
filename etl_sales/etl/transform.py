from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping

import pandas as pd
from loguru import logger

from . import normalize


@dataclass
class ReportContext:
    start_date: dt.date
    end_date: dt.date
    report_week: str
    file_path: Path
    platform: str
    fail_on_invalid_articul: bool = False


@dataclass
class TransformResult:
    dataframe: pd.DataFrame
    invalid_articuls: pd.DataFrame
    other_columns: Dict[str, str]


CANONICAL_COLUMNS = [
    "id_key",
    "articul_product",
    "articul_store",
    "playground",
    "ordered",
    "ordered_for_the_amount",
    "report_period_start",
    "report_period_end",
    "report_week",
    "file_source",
]


REQUIRED_TYPES = {
    "ordered": normalize.coerce_int,
    "ordered_for_the_amount": normalize.coerce_float,
}


def apply_column_mappings(df: pd.DataFrame, alias_map: Mapping[str, Iterable[str]]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    alias_lookup = normalize.build_alias_lookup(alias_map)
    canonical_map, other_map = normalize.map_columns(df.columns, alias_lookup)
    rename_map: Dict[str, str] = {}
    rename_map.update(canonical_map)
    rename_map.update(other_map)
    logger.debug("Column rename map built: {map}", map=rename_map)
    renamed_df = df.rename(columns=rename_map)
    return renamed_df, other_map


def _create_invalid_articul_df(df: pd.DataFrame, invalid_mask: pd.Series) -> pd.DataFrame:
    if invalid_mask.sum() == 0:
        return pd.DataFrame(columns=df.columns)
    invalid_rows = df.loc[invalid_mask].copy()
    return invalid_rows


def prepare_dataframe(
    df: pd.DataFrame,
    alias_map: Mapping[str, Iterable[str]],
    context: ReportContext,
) -> TransformResult:
    logger.info("Preparing dataframe for platform {platform} from file {file}", platform=context.platform, file=context.file_path)
    working_df, other_map = apply_column_mappings(df, alias_map)

    working_df = normalize.ensure_columns(working_df, CANONICAL_COLUMNS)

    for column, transformer in REQUIRED_TYPES.items():
        if column in working_df.columns:
            try:
                working_df[column] = transformer(working_df[column])
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Failed to convert column {column} in file {file}: {exc}", column=column, file=context.file_path, exc=exc)
                working_df[column] = transformer(pd.Series())
        else:
            working_df[column] = transformer(pd.Series())

    if "articul_store" in working_df.columns:
        working_df["articul_store"] = normalize.clean_articul_store(working_df["articul_store"])

    if "articul_product" not in working_df.columns:
        working_df["articul_product"] = None

    normalized_articuls, invalid_mask = normalize.normalize_articul_series(working_df["articul_product"])
    working_df["articul_product"] = normalized_articuls
    invalid_df = _create_invalid_articul_df(df.assign(articul_product_normalized=normalized_articuls), invalid_mask)

    if context.fail_on_invalid_articul and not invalid_df.empty:
        raise ValueError(f"Invalid articuls detected in file {context.file_path}")

    working_df["playground"] = context.platform
    working_df["report_period_start"] = pd.to_datetime(context.start_date)
    working_df["report_period_end"] = pd.to_datetime(context.end_date)
    working_df["report_week"] = str(context.report_week)
    working_df["file_source"] = str(context.file_path)

    ordered_columns = [col for col in working_df.columns if col.startswith("Other_")]
    ordered_columns.sort()

    working_df = normalize.reorder_columns(working_df, CANONICAL_COLUMNS + ordered_columns)

    other_columns_mapping = {rename: original for original, rename in other_map.items()}

    return TransformResult(dataframe=working_df, invalid_articuls=invalid_df, other_columns=other_columns_mapping)


def assign_incremental_ids(df: pd.DataFrame, existing_df: pd.DataFrame, id_column: str = "id_key") -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df
    if existing_df is not None and not existing_df.empty and id_column in existing_df.columns:
        start_id = int(existing_df[id_column].max()) + 1
    else:
        start_id = 1
    if id_column in df.columns and df[id_column].notna().any():
        missing_mask = df[id_column].isna() | (df[id_column] == 0)
        count_missing = missing_mask.sum()
        df.loc[missing_mask, id_column] = range(start_id, start_id + count_missing)
    else:
        df[id_column] = range(start_id, start_id + len(df))
    df[id_column] = df[id_column].astype(int)
    return df
