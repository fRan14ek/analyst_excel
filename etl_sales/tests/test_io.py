from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from etl_sales.etl.io import load_base_sheets, write_workbook


def test_write_and_load_workbook(tmp_path: Path) -> None:
    data = {
        "OZ": pd.DataFrame({"id_key": [1], "articul_product": ["1234-567-89"]}),
        "WB": pd.DataFrame({"id_key": [2], "articul_product": ["1234-567-90"]}),
    }
    path = tmp_path / "base.xlsx"

    write_workbook(path, data)

    sheets = load_base_sheets(path, ["OZ", "WB", "REPORT"])

    assert "OZ" in sheets
    assert "WB" in sheets
    assert sheets["OZ"].iloc[0]["id_key"] == 1
    assert sheets["REPORT"].empty
