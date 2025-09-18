from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from etl_sales.etl.normalize import normalize_articul
from etl_sales.etl.transform import ReportContext, assign_incremental_ids, prepare_dataframe
from etl_sales.etl.io import read_yaml


def test_prepare_dataframe_maps_columns(tmp_path: Path) -> None:
    data = pd.DataFrame(
        {
            "Артикул товара": ["1234 567 89"],
            "Артикул магазина": [" store-01 "],
            "Количество заказов": ["10"],
            "Сумма продаж": ["1 234,56"],
            "Промо": ["yes"],
        }
    )
    alias_map = read_yaml(Path("etl_sales/mappings/columns_aliases_OZ.yaml"))
    context = ReportContext(
        start_date=dt.date(2025, 9, 8),
        end_date=dt.date(2025, 9, 14),
        report_week="202536",
        file_path=tmp_path / "sample.xlsx",
        platform="OZ",
    )

    result = prepare_dataframe(data, alias_map, context)

    assert "articul_product" in result.dataframe.columns
    assert result.dataframe.loc[0, "articul_product"] == "1234-567-89"
    assert result.dataframe.loc[0, "articul_store"] == "store-01"
    assert result.dataframe.loc[0, "ordered"] == 10
    assert abs(result.dataframe.loc[0, "ordered_for_the_amount"] - 1234.56) < 0.001
    assert "Other_promo" in result.dataframe.columns
    assert result.dataframe.loc[0, "playground"] == "OZ"
    assert str(result.dataframe.loc[0, "report_week"]) == "202536"
    assert result.dataframe.loc[0, "file_source"] == str(context.file_path)
    assert result.invalid_articuls.empty


def test_normalize_articul() -> None:
    assert normalize_articul("1234 567 89") == "1234-567-89"
    assert normalize_articul("123456789") == "1234-567-89"
    assert normalize_articul("abc") is None


def test_assign_incremental_ids() -> None:
    existing = pd.DataFrame({"id_key": [1, 2, 3]})
    new_data = pd.DataFrame({"id_key": [None, None], "articul_product": ["1234-567-89", "1234-567-90"]})

    assigned = assign_incremental_ids(new_data, existing)

    assert assigned["id_key"].tolist() == [4, 5]
