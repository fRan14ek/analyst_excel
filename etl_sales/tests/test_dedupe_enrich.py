from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from etl_sales.etl.dedupe import dedupe_against_existing
from etl_sales.etl.enrich import enrich_report


def test_dedupe_against_existing() -> None:
    existing = pd.DataFrame(
        {
            "articul_product": ["1234-567-89"],
            "articul_store": ["store"],
            "report_period_start": ["2025-09-08"],
            "playground": ["OZ"],
            "id_key": [1],
        }
    )
    new_data = pd.DataFrame(
        {
            "articul_product": ["1234-567-89", "1234-567-90"],
            "articul_store": ["store", "store"],
            "report_period_start": ["2025-09-08", "2025-09-08"],
            "playground": ["OZ", "OZ"],
        }
    )

    deduped, duplicates = dedupe_against_existing(
        new_data,
        existing,
        ["articul_product", "articul_store", "report_period_start", "playground"],
    )

    assert len(deduped) == 1
    assert deduped.iloc[0]["articul_product"] == "1234-567-90"
    assert len(duplicates) == 1
    assert duplicates.iloc[0]["articul_product"] == "1234-567-89"


def test_enrich_report_merges_product_name() -> None:
    report_df = pd.DataFrame(
        {
            "articul_product": ["1234-567-89", "1234-567-90"],
            "articul_store": ["store", "store"],
            "playground": ["OZ", "OZ"],
            "report_week": ["202536", "202536"],
        }
    )
    product_df = pd.DataFrame(
        {
            "articul_product": ["1234-567-89"],
            "name_product": ["Товар"]
        }
    )

    enriched, unmatched = enrich_report(report_df, product_df)

    assert "name_product" in enriched.columns
    assert enriched.loc[enriched["articul_product"] == "1234-567-89", "name_product"].iloc[0] == "Товар"
    assert len(unmatched) == 1
    assert unmatched.iloc[0]["articul_product"] == "1234-567-90"
