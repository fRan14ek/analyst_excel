from __future__ import annotations

from typing import Iterable, Tuple

import pandas as pd


def dedupe_against_existing(
    new_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    key_columns: Iterable[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if new_df.empty:
        return new_df, new_df

    key_columns = list(key_columns)
    new_unique = new_df.drop_duplicates(subset=key_columns, keep="last")

    if existing_df is None or existing_df.empty:
        return new_unique, pd.DataFrame(columns=new_df.columns)

    existing_keys = existing_df[key_columns].drop_duplicates()
    merged = new_unique.merge(existing_keys.assign(__is_duplicate=True), on=key_columns, how="left")

    duplicates = merged[merged["__is_duplicate"].eq(True)].drop(columns=["__is_duplicate"]).copy()
    deduped = merged[merged["__is_duplicate"].ne(True)].drop(columns=["__is_duplicate"]).copy()

    return deduped, duplicates
