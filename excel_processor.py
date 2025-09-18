"""Utilities for extracting data from an Excel workbook and materialising it
as SQLite databases.

The module exposes a small command line interface that accepts the path to an
``.xlsx`` file and produces one SQLite database per sheet together with a JSON
dictionary describing the workbook's structure.  The entry point can be used as
``python excel_processor.py <path-to-workbook>``.

To keep the solution self-contained the workbook is parsed directly using the
standard :mod:`zipfile` and :mod:`xml.etree.ElementTree` modules.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import sqlite3
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, List, Sequence, Tuple
from xml.etree import ElementTree as ET


@dataclass
class SheetData:
    """Represent the data extracted from a workbook sheet."""

    name: str
    original_columns: List[str]
    normalized_columns: List[str]
    rows: List[Sequence[Any]]
    row_count: int
    database_path: Path | None = field(default=None)


def _normalise_column_names(raw_columns: Iterable[Any]) -> List[str]:
    """Return normalised column names suitable for SQLite.

    ``raw_columns`` usually corresponds to the first row of a sheet.  Missing
    values are replaced with ``column_<index>`` and all other names are
    converted to snake case.  If duplicate names occur, numerical suffixes are
    appended to keep them unique.
    """

    normalised: List[str] = []
    seen: set[str] = set()

    for index, value in enumerate(raw_columns, start=1):
        if value is None or (isinstance(value, str) and value.strip() == ""):
            candidate = f"column_{index}"
        else:
            candidate = str(value)

        candidate = re.sub(r"\W+", "_", candidate, flags=re.UNICODE).strip("_")
        candidate = candidate.lower() or f"column_{index}"

        suffix = 1
        unique_candidate = candidate
        while unique_candidate in seen:
            suffix += 1
            unique_candidate = f"{candidate}_{suffix}"

        seen.add(unique_candidate)
        normalised.append(unique_candidate)

    return normalised


def _quote_sql_identifier(value: str) -> str:
    """Return ``value`` quoted for use as an SQLite identifier."""

    return f'"{value.replace("\"", "\"\"")}"'


def _safe_file_component(name: str) -> str:
    """Return a filesystem-safe representation of ``name``.

    The resulting string contains only alphanumeric characters and ``_``.  It is
    guaranteed to be non-empty.
    """

    simplified = re.sub(r"\W+", "_", name, flags=re.UNICODE).strip("_")
    simplified = simplified or "sheet"
    return simplified


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _column_index_from_reference(cell_reference: str) -> int:
    letters = [char for char in cell_reference if char.isalpha()]
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - 64)
    return max(index, 1)


def _read_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    try:
        data = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []

    root = ET.fromstring(data)
    shared: List[str] = []
    for si in root.findall(f"{{{MAIN_NS}}}si"):
        text_parts: List[str] = []
        for child in si.iter():
            if child.tag == f"{{{MAIN_NS}}}t":
                text_parts.append(child.text or "")
        shared.append("".join(text_parts))
    return shared


def _read_workbook_relationships(archive: zipfile.ZipFile) -> dict[str, str]:
    try:
        data = archive.read("xl/_rels/workbook.xml.rels")
    except KeyError:
        return {}

    root = ET.fromstring(data)
    relationships: dict[str, str] = {}
    for relationship in root.findall(f"{{{PKG_REL_NS}}}Relationship"):
        relationships[relationship.attrib["Id"]] = relationship.attrib["Target"]
    return relationships


def _parse_cell_value(cell: ET.Element, shared_strings: Sequence[str]) -> Any:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        inline = cell.find(f"{{{MAIN_NS}}}is")
        if inline is None:
            return None
        text_parts = [
            node.text or ""
            for node in inline.iter()
            if node.tag == f"{{{MAIN_NS}}}t"
        ]
        return "".join(text_parts)

    value_element = cell.find(f"{{{MAIN_NS}}}v")
    if value_element is None:
        return None

    raw_value = value_element.text or ""
    if cell_type == "s":
        try:
            index = int(raw_value)
        except ValueError:
            return None
        if 0 <= index < len(shared_strings):
            return shared_strings[index]
        return None
    if cell_type == "b":
        return raw_value == "1"
    if cell_type == "str":
        return raw_value

    if raw_value == "":
        return None

    if raw_value.isdigit() or (raw_value.startswith("-") and raw_value[1:].isdigit()):
        try:
            return int(raw_value)
        except ValueError:
            pass
    try:
        return float(raw_value)
    except ValueError:
        return raw_value


def _read_sheet(
    archive: zipfile.ZipFile, sheet_path: str, shared_strings: Sequence[str]
) -> List[List[Any]]:
    try:
        data = archive.read(sheet_path)
    except KeyError:
        return []

    root = ET.fromstring(data)
    sheet_data = root.find(f"{{{MAIN_NS}}}sheetData")
    if sheet_data is None:
        return []

    rows: List[List[Any]] = []
    for row in sheet_data.findall(f"{{{MAIN_NS}}}row"):
        row_values: List[Any] = []
        current_column = 1
        for cell in row.findall(f"{{{MAIN_NS}}}c"):
            reference = cell.attrib.get("r")
            if reference:
                column_index = _column_index_from_reference(reference)
                while current_column < column_index:
                    row_values.append(None)
                    current_column += 1

            row_values.append(_parse_cell_value(cell, shared_strings))
            current_column += 1

        rows.append(row_values)

    return rows


def _load_workbook_data(workbook_path: Path) -> List[Tuple[str, List[List[Any]]]]:
    with zipfile.ZipFile(workbook_path, "r") as archive:
        shared_strings = _read_shared_strings(archive)
        relationships = _read_workbook_relationships(archive)
        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))

        sheets_element = workbook_root.find(f"{{{MAIN_NS}}}sheets")
        if sheets_element is None:
            return []

        sheets: List[Tuple[str, List[List[Any]]]] = []
        for sheet in sheets_element.findall(f"{{{MAIN_NS}}}sheet"):
            name = sheet.attrib.get("name", "Sheet")
            relationship_id = sheet.attrib.get(f"{{{REL_NS}}}id")
            if not relationship_id:
                continue
            target = relationships.get(relationship_id)
            if not target:
                continue
            sheet_path = posixpath.normpath(str(PurePosixPath("xl") / target))
            rows = _read_sheet(archive, sheet_path, shared_strings)
            sheets.append((name, rows))
    return sheets


def extract_workbook_details(workbook_path: Path) -> List[SheetData]:
    """Read ``workbook_path`` and return structured sheet data."""

    sheet_contents = _load_workbook_data(workbook_path)

    sheets: List[SheetData] = []
    for sheet_name, rows in sheet_contents:
        if not rows:
            original_columns: List[str] = []
            normalised_columns: List[str] = []
            data_rows: List[Sequence[Any]] = []
        else:
            max_length = max(len(row) for row in rows)
            padded_rows = [row + [None] * (max_length - len(row)) for row in rows]

            header = padded_rows[0]
            original_columns = ["" if value is None else str(value) for value in header]
            normalised_columns = _normalise_column_names(header)
            body_rows = padded_rows[1:]
            data_rows = [tuple(row[: len(normalised_columns)]) for row in body_rows]

        sheets.append(
            SheetData(
                name=sheet_name,
                original_columns=original_columns,
                normalized_columns=normalised_columns,
                rows=data_rows,
                row_count=len(data_rows),
            )
        )

    return sheets


def create_sheet_databases(sheets: List[SheetData], output_dir: Path) -> None:
    """Create one SQLite database per sheet.

    The database will contain a single table named ``sheet_data`` which mirrors
    the sheet's columns and data.  The resulting database path is stored in the
    ``SheetData`` objects under ``database_path``.
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    for index, sheet in enumerate(sheets, start=1):
        filename = f"{index:02d}_{_safe_file_component(sheet.name)}.db"
        database_path = output_dir / filename

        with sqlite3.connect(database_path) as connection:
            if not sheet.normalized_columns:
                connection.execute("CREATE TABLE IF NOT EXISTS sheet_data (placeholder INTEGER)")
            else:
                columns_sql = ", ".join(
                    f"{_quote_sql_identifier(column)} NUMERIC" for column in sheet.normalized_columns
                )
                connection.execute(f"CREATE TABLE IF NOT EXISTS sheet_data ({columns_sql})")

                if sheet.rows:
                    placeholders = ", ".join(["?"] * len(sheet.normalized_columns))
                    insert_sql = f"INSERT INTO sheet_data VALUES ({placeholders})"
                    connection.executemany(insert_sql, sheet.rows)

        sheet.database_path = database_path


def create_sheet_dictionary(
    workbook_path: Path, sheets: List[SheetData], output_dir: Path, filename: str = "sheet_dictionary.json"
) -> Path:
    """Persist a JSON dictionary describing all sheets.

    The resulting JSON contains the number of sheets together with column and
    row information for each individual sheet.
    """

    dictionary = {
        "workbook": str(workbook_path),
        "sheet_count": len(sheets),
        "sheets": {
            sheet.name: {
                "row_count": sheet.row_count,
                "original_columns": sheet.original_columns,
                "normalized_columns": sheet.normalized_columns,
                "database": str(sheet.database_path) if sheet.database_path else None,
            }
            for sheet in sheets
        },
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    dictionary_path = output_dir / filename
    dictionary_path.write_text(json.dumps(dictionary, indent=2, ensure_ascii=False), encoding="utf-8")

    return dictionary_path


def process_workbook(workbook_path: Path, output_dir: Path | None = None) -> Path:
    """High-level helper that orchestrates the full workflow."""

    workbook_path = workbook_path.expanduser().resolve()
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    if output_dir is None:
        output_dir = workbook_path.parent / "output"

    sheets = extract_workbook_details(workbook_path)
    databases_dir = output_dir / "databases"
    create_sheet_databases(sheets, databases_dir)

    dictionary_path = create_sheet_dictionary(
        workbook_path=workbook_path, sheets=sheets, output_dir=output_dir, filename="sheet_dictionary.json"
    )
    return dictionary_path


def _parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract Excel data into SQLite databases and JSON metadata. "
            "By default the workbook is looked up in the 'base' directory, but explicit paths are accepted."
        )
    )
    parser.add_argument(
        "workbook",
        type=Path,
        help="Excel workbook (.xlsx) located in 'base' or provided via an explicit relative/absolute path.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where databases and the dictionary file will be created. Defaults to a sibling 'output' directory.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_arguments(argv)
    dictionary_path = process_workbook(args.workbook, output_dir=args.output_dir)
    print(f"Dictionary created at {dictionary_path}")


if __name__ == "__main__":
    main()
