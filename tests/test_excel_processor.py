from __future__ import annotations

import json
import sqlite3
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Sequence, Tuple
from xml.etree import ElementTree as ET

import excel_processor


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
DOC_PROPS_APP_NS = "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
DOC_PROPS_VT_NS = "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"
CORE_NS = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
DC_NS = "http://purl.org/dc/elements/1.1/"
DCTERMS_NS = "http://purl.org/dc/terms/"
DCMITYPE_NS = "http://purl.org/dc/dcmitype/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_NS = "http://www.w3.org/XML/1998/namespace"


def _column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result or "A"


def _build_shared_strings(sheets: Sequence[Tuple[str, Sequence[Sequence[object]]]]) -> Tuple[List[str], dict[str, int]]:
    values: List[str] = []
    lookup: dict[str, int] = {}
    for _, rows in sheets:
        for row in rows:
            for value in row:
                if isinstance(value, str) and value not in lookup:
                    lookup[value] = len(values)
                    values.append(value)
    return values, lookup


def _to_xml_bytes(element: ET.Element) -> bytes:
    return ET.tostring(element, encoding="utf-8", xml_declaration=True)


def _create_shared_strings_xml(strings: Sequence[str]) -> bytes:
    sst = ET.Element("sst", {"xmlns": MAIN_NS, "count": str(len(strings)), "uniqueCount": str(len(strings))})
    for value in strings:
        si = ET.SubElement(sst, "si")
        text = ET.SubElement(si, "t")
        if value != value.strip():
            text.set(f"{{{XML_NS}}}space", "preserve")
        text.text = value
    return _to_xml_bytes(sst)


def _create_workbook_xml(sheet_names: Sequence[str]) -> bytes:
    workbook = ET.Element("workbook", {"xmlns": MAIN_NS, "xmlns:r": REL_NS})
    sheets_element = ET.SubElement(workbook, "sheets")
    for index, name in enumerate(sheet_names, start=1):
        ET.SubElement(
            sheets_element,
            "sheet",
            {"name": name, "sheetId": str(index), f"{{{REL_NS}}}id": f"rId{index}"},
        )
    return _to_xml_bytes(workbook)


def _create_workbook_relationships_xml(sheet_count: int) -> bytes:
    relationships = ET.Element("Relationships", {"xmlns": PKG_REL_NS})
    for index in range(1, sheet_count + 1):
        ET.SubElement(
            relationships,
            "Relationship",
            {
                "Id": f"rId{index}",
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet",
                "Target": f"worksheets/sheet{index}.xml",
            },
        )
    return _to_xml_bytes(relationships)


def _create_sheet_xml(rows: Sequence[Sequence[object]], lookup: dict[str, int]) -> bytes:
    worksheet = ET.Element("worksheet", {"xmlns": MAIN_NS})
    sheet_data = ET.SubElement(worksheet, "sheetData")
    for row_index, row in enumerate(rows, start=1):
        row_element = ET.SubElement(sheet_data, "row", {"r": str(row_index)})
        for column_index, value in enumerate(row, start=1):
            cell_reference = f"{_column_letter(column_index)}{row_index}"
            cell_attributes = {"r": cell_reference}
            cell_element = ET.SubElement(row_element, "c", cell_attributes)
            if isinstance(value, str):
                cell_element.set("t", "s")
                ET.SubElement(cell_element, "v").text = str(lookup[value])
            elif value is None:
                continue
            else:
                ET.SubElement(cell_element, "v").text = str(value)
    return _to_xml_bytes(worksheet)


def _create_content_types_xml(sheet_count: int) -> bytes:
    types = ET.Element("Types", {"xmlns": "http://schemas.openxmlformats.org/package/2006/content-types"})
    ET.SubElement(
        types,
        "Default",
        {"Extension": "rels", "ContentType": "application/vnd.openxmlformats-package.relationships+xml"},
    )
    ET.SubElement(types, "Default", {"Extension": "xml", "ContentType": "application/xml"})
    ET.SubElement(
        types,
        "Override",
        {
            "PartName": "/xl/workbook.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
        },
    )
    for index in range(1, sheet_count + 1):
        ET.SubElement(
            types,
            "Override",
            {
                "PartName": f"/xl/worksheets/sheet{index}.xml",
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
            },
        )
    ET.SubElement(
        types,
        "Override",
        {
            "PartName": "/xl/styles.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml",
        },
    )
    ET.SubElement(
        types,
        "Override",
        {
            "PartName": "/xl/sharedStrings.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml",
        },
    )
    ET.SubElement(
        types,
        "Override",
        {
            "PartName": "/docProps/core.xml",
            "ContentType": "application/vnd.openxmlformats-package.core-properties+xml",
        },
    )
    ET.SubElement(
        types,
        "Override",
        {
            "PartName": "/docProps/app.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.extended-properties+xml",
        },
    )
    return _to_xml_bytes(types)


def _create_root_relationships_xml() -> bytes:
    relationships = ET.Element("Relationships", {"xmlns": PKG_REL_NS})
    ET.SubElement(
        relationships,
        "Relationship",
        {
            "Id": "rId1",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
            "Target": "xl/workbook.xml",
        },
    )
    ET.SubElement(
        relationships,
        "Relationship",
        {
            "Id": "rId2",
            "Type": "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
            "Target": "docProps/core.xml",
        },
    )
    ET.SubElement(
        relationships,
        "Relationship",
        {
            "Id": "rId3",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
            "Target": "docProps/app.xml",
        },
    )
    return _to_xml_bytes(relationships)


def _create_core_properties_xml() -> bytes:
    core = ET.Element(
        f"{{{CORE_NS}}}coreProperties",
        {
            "xmlns:dc": DC_NS,
            "xmlns:dcterms": DCTERMS_NS,
            "xmlns:dcmitype": DCMITYPE_NS,
            "xmlns:xsi": XSI_NS,
        },
    )

    ET.SubElement(core, f"{{{DC_NS}}}creator").text = "Tests"
    ET.SubElement(core, f"{{{DC_NS}}}title").text = "Sample"
    ET.SubElement(core, f"{{{DC_NS}}}subject").text = ""
    ET.SubElement(core, f"{{{DC_NS}}}description").text = ""
    ET.SubElement(core, f"{{{DC_NS}}}language").text = "ru-RU"
    ET.SubElement(core, f"{{{CORE_NS}}}lastModifiedBy").text = "Tests"

    created = ET.SubElement(core, f"{{{DCTERMS_NS}}}created")
    created.set(f"{{{XSI_NS}}}type", "dcterms:W3CDTF")
    created.text = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    modified = ET.SubElement(core, f"{{{DCTERMS_NS}}}modified")
    modified.set(f"{{{XSI_NS}}}type", "dcterms:W3CDTF")
    modified.text = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return _to_xml_bytes(core)


def _create_app_properties_xml() -> bytes:
    properties = ET.Element(
        f"{{{DOC_PROPS_APP_NS}}}Properties",
        {"xmlns:vt": DOC_PROPS_VT_NS},
    )
    ET.SubElement(properties, f"{{{DOC_PROPS_APP_NS}}}Application").text = "Python"
    return _to_xml_bytes(properties)


def _create_styles_xml() -> bytes:
    styles = ET.Element("styleSheet", {"xmlns": MAIN_NS})
    fonts = ET.SubElement(styles, "fonts", {"count": "1"})
    font = ET.SubElement(fonts, "font")
    ET.SubElement(font, "sz", {"val": "11"})
    ET.SubElement(font, "color", {"theme": "1"})
    ET.SubElement(font, "name", {"val": "Calibri"})
    ET.SubElement(font, "family", {"val": "2"})

    fills = ET.SubElement(styles, "fills", {"count": "2"})
    ET.SubElement(ET.SubElement(fills, "fill"), "patternFill", {"patternType": "none"})
    ET.SubElement(ET.SubElement(fills, "fill"), "patternFill", {"patternType": "gray125"})

    borders = ET.SubElement(styles, "borders", {"count": "1"})
    border = ET.SubElement(borders, "border")
    ET.SubElement(border, "left")
    ET.SubElement(border, "right")
    ET.SubElement(border, "top")
    ET.SubElement(border, "bottom")
    ET.SubElement(border, "diagonal")

    cell_style_xfs = ET.SubElement(styles, "cellStyleXfs", {"count": "1"})
    ET.SubElement(cell_style_xfs, "xf", {"numFmtId": "0", "fontId": "0", "fillId": "0", "borderId": "0"})

    cell_xfs = ET.SubElement(styles, "cellXfs", {"count": "1"})
    ET.SubElement(cell_xfs, "xf", {"numFmtId": "0", "fontId": "0", "fillId": "0", "borderId": "0", "xfId": "0"})

    cell_styles = ET.SubElement(styles, "cellStyles", {"count": "1"})
    ET.SubElement(cell_styles, "cellStyle", {"name": "Normal", "xfId": "0", "builtinId": "0"})
    return _to_xml_bytes(styles)


def _write_sample_workbook(path: Path) -> None:
    sheets = [
        ("Первая страница", [["Имя", "Количество"], ["Алиса", 3], ["Боб", 5]]),
        ("Вторая", [["Категория", "Описание", "Вес"], ["A", "Первый", 10.5], ["B", "", None]]),
    ]

    shared_strings, lookup = _build_shared_strings(sheets)

    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", _create_content_types_xml(len(sheets)))
        archive.writestr("_rels/.rels", _create_root_relationships_xml())
        archive.writestr("xl/workbook.xml", _create_workbook_xml([sheet[0] for sheet in sheets]))
        archive.writestr("xl/_rels/workbook.xml.rels", _create_workbook_relationships_xml(len(sheets)))
        archive.writestr("xl/sharedStrings.xml", _create_shared_strings_xml(shared_strings))
        archive.writestr("xl/styles.xml", _create_styles_xml())

        for index, (_, rows) in enumerate(sheets, start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", _create_sheet_xml(rows, lookup))

        archive.writestr("docProps/core.xml", _create_core_properties_xml())
        archive.writestr("docProps/app.xml", _create_app_properties_xml())


class ExcelProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.workbook_path = Path(self.temp_dir.name) / "sample.xlsx"
        _write_sample_workbook(self.workbook_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_process_workbook_creates_expected_artifacts(self) -> None:
        output_dir = Path(self.temp_dir.name) / "output"
        dictionary_path = excel_processor.process_workbook(self.workbook_path, output_dir=output_dir)

        self.assertTrue(dictionary_path.exists(), "Dictionary JSON should be created")

        with dictionary_path.open("r", encoding="utf-8") as handle:
            dictionary = json.load(handle)

        self.assertEqual(dictionary["sheet_count"], 2)
        self.assertCountEqual(dictionary["sheets"].keys(), ["Первая страница", "Вторая"])

        first_sheet_info = dictionary["sheets"]["Первая страница"]
        self.assertEqual(first_sheet_info["row_count"], 2)
        self.assertEqual(first_sheet_info["original_columns"], ["Имя", "Количество"])
        self.assertEqual(first_sheet_info["normalized_columns"], ["имя", "количество"])

        second_sheet_info = dictionary["sheets"]["Вторая"]
        self.assertEqual(second_sheet_info["row_count"], 2)
        self.assertEqual(second_sheet_info["normalized_columns"], ["категория", "описание", "вес"])

        first_db_path = Path(first_sheet_info["database"])
        self.assertTrue(first_db_path.exists())
        with sqlite3.connect(first_db_path) as connection:
            rows = connection.execute("SELECT * FROM sheet_data ORDER BY rowid").fetchall()

        self.assertEqual(rows, [("Алиса", 3), ("Боб", 5)])

        second_db_path = Path(second_sheet_info["database"])
        self.assertTrue(second_db_path.exists())
        with sqlite3.connect(second_db_path) as connection:
            rows = connection.execute("SELECT * FROM sheet_data ORDER BY rowid").fetchall()

        self.assertEqual(rows, [("A", "Первый", 10.5), ("B", "", None)])


if __name__ == "__main__":  # pragma: no cover - convenience for local execution
    unittest.main()
