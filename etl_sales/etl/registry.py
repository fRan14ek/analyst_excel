from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping

import pandas as pd
from loguru import logger


@dataclass
class RegistryEntry:
    mapped_name: str
    original_name: str
    first_seen_date: dt.date
    first_seen_file: str


@dataclass
class ColumnRegistry:
    path: Path
    _data: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        if self.path.exists():
            try:
                existing = pd.read_excel(self.path, sheet_name=None)
                self._data = existing
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to read existing registry at {path}: {exc}", path=self.path, exc=exc)
                self._data = {}

    def register(self, platform: str, column_mapping: Mapping[str, str], file_path: Path) -> int:
        if not column_mapping:
            return 0
        platform_sheet = self._data.get(platform, pd.DataFrame(columns=["mapped_name", "original_name", "first_seen_date", "first_seen_file"]))
        recorded_originals = set(str(value) for value in platform_sheet["original_name"].astype(str).tolist()) if not platform_sheet.empty else set()
        new_rows = []
        today = dt.date.today()
        for mapped_name, original_name in column_mapping.items():
            if original_name in recorded_originals:
                continue
            new_rows.append(
                {
                    "mapped_name": mapped_name,
                    "original_name": original_name,
                    "first_seen_date": today,
                    "first_seen_file": str(file_path),
                }
            )
        if new_rows:
            updated_sheet = pd.concat([platform_sheet, pd.DataFrame(new_rows)], ignore_index=True)
            self._data[platform] = updated_sheet
            logger.info("Registered {count} new columns for platform {platform}", count=len(new_rows), platform=platform)
        return len(new_rows)

    def flush(self) -> None:
        if not self._data:
            return
        with pd.ExcelWriter(self.path, engine="openpyxl") as writer:
            for platform, df in self._data.items():
                df.to_excel(writer, sheet_name=platform, index=False)
