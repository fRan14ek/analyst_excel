from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd
from loguru import logger

from .io import read_input_table


def load_product_master(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        logger.warning("Product lookup file not found at {path}", path=path)
        return pd.DataFrame(columns=["articul_product", "name_product"])
    df = read_input_table(path)
    if "articul_product" not in df.columns:
        raise ValueError("Product lookup must contain articul_product column")
    if "name_product" not in df.columns:
        logger.warning("Product lookup missing name_product column. A placeholder will be used.")
        df["name_product"] = None
    df = df[["articul_product", "name_product"]]
    df["articul_product"] = df["articul_product"].astype(str)
    return df.drop_duplicates(subset=["articul_product"])


def enrich_report(report_df: pd.DataFrame, product_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if report_df.empty:
        return report_df, pd.DataFrame(columns=report_df.columns)
    enriched = report_df.merge(product_df, on="articul_product", how="left")
    unmatched_mask = enriched["name_product"].isna() & enriched["articul_product"].notna()
    unmatched = enriched.loc[unmatched_mask, ["articul_product", "articul_store", "playground", "report_week"]].drop_duplicates()
    return enriched, unmatched
