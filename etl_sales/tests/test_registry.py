from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from etl_sales.etl.registry import ColumnRegistry


def test_registry_registers_new_columns(tmp_path: Path) -> None:
    registry_path = tmp_path / "columns_registry.xlsx"
    registry = ColumnRegistry(registry_path)

    added = registry.register("OZ", {"Other_new_column": "Новая колонка"}, Path("/tmp/source.xlsx"))
    assert added == 1

    registry.flush()

    stored = pd.read_excel(registry_path, sheet_name="OZ")
    assert stored.iloc[0]["mapped_name"] == "Other_new_column"
    assert stored.iloc[0]["original_name"] == "Новая колонка"
