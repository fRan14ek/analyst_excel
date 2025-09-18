from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping, Optional

import pandas as pd


@dataclass
class PlatformMetrics:
    files_processed: int = 0
    rows_read: int = 0
    rows_loaded: int = 0
    duplicates: int = 0
    invalid_articuls: int = 0
    new_columns: int = 0


@dataclass
class RunStats:
    by_platform: Dict[str, PlatformMetrics] = field(default_factory=dict)
    unmatched_products: int = 0
    registry_new_columns: int = 0
    output_report_path: Optional[Path] = None
    output_parquet_path: Optional[Path] = None
    base_path: Optional[Path] = None
    duplicates_path: Optional[Path] = None
    invalid_path: Optional[Path] = None
    unmatched_path: Optional[Path] = None

    def for_platform(self, platform: str) -> PlatformMetrics:
        if platform not in self.by_platform:
            self.by_platform[platform] = PlatformMetrics()
        return self.by_platform[platform]

    def total_files(self) -> int:
        return sum(metric.files_processed for metric in self.by_platform.values())

    def total_loaded(self) -> int:
        return sum(metric.rows_loaded for metric in self.by_platform.values())

    def total_duplicates(self) -> int:
        return sum(metric.duplicates for metric in self.by_platform.values())

    def to_markdown(self) -> str:
        lines = ["# Run Summary", "", f"Дата запуска: {dt.datetime.now():%Y-%m-%d %H:%M:%S}", ""]
        lines.append("## Метрики по площадкам")
        lines.append("")
        lines.append("| Площадка | Файлы | Прочитано строк | Загрузка | Дубликаты | Неверные артикулы | Новые колонки |")
        lines.append("|---|---|---|---|---|---|---|")
        for platform, metric in self.by_platform.items():
            lines.append(
                f"| {platform} | {metric.files_processed} | {metric.rows_read} | {metric.rows_loaded} | "
                f"{metric.duplicates} | {metric.invalid_articuls} | {metric.new_columns} |"
            )
        lines.append("")
        lines.append("## Итого")
        lines.append("")
        lines.append(f"- Всего файлов: {self.total_files()}")
        lines.append(f"- Загружено строк: {self.total_loaded()}")
        lines.append(f"- Удалено дублей: {self.total_duplicates()}")
        lines.append(f"- Несопоставленные товары: {self.unmatched_products}")
        lines.append(f"- Новые колонки в реестре: {self.registry_new_columns}")
        if self.output_report_path:
            lines.append(f"- Итоговый отчёт: {self.output_report_path}")
        if self.output_parquet_path:
            lines.append(f"- Parquet: {self.output_parquet_path}")
        if self.base_path:
            lines.append(f"- Обновлённая база: {self.base_path}")
        if self.duplicates_path:
            lines.append(f"- Файл дублей: {self.duplicates_path}")
        if self.invalid_path:
            lines.append(f"- Неверные артикулы: {self.invalid_path}")
        if self.unmatched_path:
            lines.append(f"- Не найденные в мастере: {self.unmatched_path}")
        return "\n".join(lines)


def build_report(platform_frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    frames = [df.assign(playground=platform) if "playground" not in df.columns else df for platform, df in platform_frames.items() if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)
