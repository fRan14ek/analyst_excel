from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text
from loguru import logger

from .dedupe import dedupe_against_existing
from .enrich import enrich_report, load_product_master
from .io import (
    ensure_directories,
    list_platform_files,
    load_base_sheets,
    load_config,
    prompt_save_path,
    read_input_table,
    read_yaml,
    timestamped_filename,
    write_workbook,
)
from .registry import ColumnRegistry
from .report import RunStats, build_report
from .transform import ReportContext, assign_incremental_ids, prepare_dataframe


app = typer.Typer(add_completion=False, help="ETL для еженедельной загрузки отчётов маркетплейсов")
console = Console()


def _compute_week(start: dt.date, week: Optional[str]) -> str:
    if week:
        return str(week)
    iso = start.isocalendar()
    return f"{iso.year}{iso.week:02d}"


def _configure_logging(log_dir: Path) -> Path:
    ensure_directories([log_dir])
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    log_path = log_dir / timestamped_filename("run", ".log")
    logger.add(log_path, level="DEBUG")
    return log_path


def _load_alias_map(mapping_path: Path) -> Dict[str, List[str]]:
    if not mapping_path.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_path}")
    return read_yaml(mapping_path)


def _render_summary(stats: RunStats) -> None:
    table = Table(title="Итоги обработки")
    table.add_column("Площадка")
    table.add_column("Файлов")
    table.add_column("Строк")
    table.add_column("Загружено")
    table.add_column("Дубликаты")
    table.add_column("Неверные артикулы")
    table.add_column("Новые колонки")
    for platform, metric in stats.by_platform.items():
        table.add_row(
            platform,
            str(metric.files_processed),
            str(metric.rows_read),
            str(metric.rows_loaded),
            str(metric.duplicates),
            str(metric.invalid_articuls),
            str(metric.new_columns),
        )
    console.print(table)
    totals = Text(
        f"Всего файлов: {stats.total_files()}, загружено строк: {stats.total_loaded()}, дубликатов: {stats.total_duplicates()}"
    )
    console.print(totals)


@app.command("load-week")
def load_week(
    start: dt.date = typer.Option(..., help="Дата начала отчётной недели"),
    end: Optional[dt.date] = typer.Option(None, help="Дата окончания отчётной недели (по умолчанию +6 дней)"),
    base: Optional[Path] = typer.Option(None, help="Путь к базе Excel"),
    week: Optional[str] = typer.Option(None, help="Номер недели в формате YYYYWW"),
    save_to: Optional[Path] = typer.Option(None, help="Каталог для сохранения отчёта"),
    dry_run: bool = typer.Option(False, help="Не записывать файлы"),
    fail_on_invalid_articul: bool = typer.Option(False, help="Остановить обработку при неверных артикулах"),
    no_export_parquet: bool = typer.Option(False, help="Не экспортировать parquet"),
    platform: Optional[str] = typer.Option(None, help="Обработать только выбранную площадку"),
    config_path: Path = typer.Option(Path("etl_sales/config.yaml"), help="Путь к конфигурационному файлу"),
) -> None:
    config = load_config(config_path)
    base_path = Path(base) if base else config.paths.base_file
    output_dir = Path(save_to) if save_to else config.paths.output_dir
    logs_dir = config.paths.logs_dir
    input_dir = config.paths.input_dir
    registry_path = config.paths.columns_registry
    product_lookup_path = config.paths.lookup_product

    log_path = _configure_logging(logs_dir)
    console.print(f"Лог-файл: {log_path}")

    report_end = end or (start + dt.timedelta(days=6))
    report_week = _compute_week(start, week)

    selected_platforms: Iterable[str]
    if platform:
        selected_platforms = [platform]
    else:
        selected_platforms = config.processing.default_platforms

    selected_platforms = list(selected_platforms)

    ensure_directories([output_dir, logs_dir])

    registry = ColumnRegistry(registry_path)
    stats = RunStats()

    platform_data: Dict[str, pd.DataFrame] = {}
    invalid_records: List[pd.DataFrame] = []
    duplicates_records: List[pd.DataFrame] = []

    for plt in selected_platforms:
        metrics = stats.for_platform(plt)
        alias_path = config.mappings.aliases.get(plt)
        if not alias_path:
            console.print(f"[yellow]Нет файла соответствий для площадки {plt}. Пропуск.[/yellow]")
            continue
        alias_map = _load_alias_map(alias_path)
        files = list_platform_files(input_dir, plt)
        metrics.files_processed = len(files)
        platform_frames: List[pd.DataFrame] = []
        for file_path in files:
            try:
                df = read_input_table(file_path)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Не удалось прочитать файл {file}: {exc}", file=file_path, exc=exc)
                continue
            if df.empty:
                logger.warning("Файл {file} пустой и будет пропущен", file=file_path)
                continue
            metrics.rows_read += len(df)
            context = ReportContext(
                start_date=start,
                end_date=report_end,
                report_week=report_week,
                file_path=file_path,
                platform=plt,
                fail_on_invalid_articul=fail_on_invalid_articul,
            )
            try:
                result = prepare_dataframe(df, alias_map, context)
            except ValueError as exc:
                logger.error("Ошибка при обработке файла {file}: {exc}", file=file_path, exc=exc)
                if fail_on_invalid_articul:
                    raise typer.Exit(code=1)
                else:
                    continue
            new_columns = registry.register(plt, result.other_columns, file_path)
            metrics.new_columns += new_columns
            stats.registry_new_columns += new_columns
            if not result.invalid_articuls.empty:
                invalid_records.append(result.invalid_articuls.assign(playground=plt, source_file=str(file_path)))
                metrics.invalid_articuls += len(result.invalid_articuls)
            platform_frames.append(result.dataframe)
        if platform_frames:
            platform_data[plt] = pd.concat(platform_frames, ignore_index=True, sort=False)
        else:
            platform_data[plt] = pd.DataFrame()

    required_sheets = list(selected_platforms)
    if "REPORT" not in required_sheets:
        required_sheets.append("REPORT")
    base_sheets = load_base_sheets(base_path, required_sheets)

    key_columns = ["articul_product", "articul_store", "report_period_start", "playground"]

    for plt, new_df in platform_data.items():
        metrics = stats.for_platform(plt)
        existing_df = base_sheets.get(plt, pd.DataFrame())
        if new_df.empty:
            continue
        deduped, duplicates = dedupe_against_existing(new_df, existing_df, key_columns)
        if not duplicates.empty:
            duplicates_records.append(duplicates.assign(playground=plt))
            metrics.duplicates += len(duplicates)
        deduped = assign_incremental_ids(deduped, existing_df, config.processing.id_column)
        metrics.rows_loaded += len(deduped)
        updated_df = pd.concat([existing_df, deduped], ignore_index=True, sort=False)
        base_sheets[plt] = updated_df

    report_df = build_report({plt: base_sheets.get(plt, pd.DataFrame()) for plt in config.processing.default_platforms})
    product_df = load_product_master(product_lookup_path)
    enriched_report, unmatched = enrich_report(report_df, product_df)
    stats.unmatched_products = len(unmatched)

    date_tag = start.strftime("%Y%m%d")
    report_filename = f"report_{report_week}.xlsx"
    report_path = output_dir / report_filename
    parquet_path = output_dir / f"report_{report_week}.parquet"
    invalid_path = output_dir / f"invalid_articuls_{date_tag}.xlsx"
    duplicates_path = output_dir / f"duplicates_{date_tag}.xlsx"
    unmatched_path = output_dir / f"unmatched_products_{date_tag}.xlsx"
    summary_path = output_dir / f"run_summary_{date_tag}.md"

    stats.output_report_path = report_path
    stats.output_parquet_path = None
    stats.base_path = base_path

    if invalid_records:
        invalid_combined = pd.concat(invalid_records, ignore_index=True, sort=False)
        stats.invalid_path = invalid_path
    else:
        invalid_combined = pd.DataFrame()
    if duplicates_records:
        duplicates_combined = pd.concat(duplicates_records, ignore_index=True, sort=False)
        stats.duplicates_path = duplicates_path
    else:
        duplicates_combined = pd.DataFrame()
    if not unmatched.empty:
        stats.unmatched_path = unmatched_path

    _render_summary(stats)

    if dry_run:
        console.print("[yellow]Режим dry-run: файлы не будут записаны.[/yellow]")
        return

    final_path = prompt_save_path(report_path)
    report_path = final_path
    stats.output_report_path = report_path

    ensure_directories([report_path.parent])

    report_sheets = {plt: base_sheets.get(plt, pd.DataFrame()) for plt in config.processing.default_platforms}
    report_sheets["REPORT"] = enriched_report
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        for sheet_name, df in report_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    base_sheets["REPORT"] = enriched_report

    write_workbook(base_path, base_sheets)

    if not invalid_combined.empty:
        with pd.ExcelWriter(invalid_path, engine="openpyxl") as writer:
            invalid_combined.to_excel(writer, index=False)
    if not duplicates_combined.empty:
        with pd.ExcelWriter(duplicates_path, engine="openpyxl") as writer:
            duplicates_combined.to_excel(writer, index=False)
    if not unmatched.empty:
        with pd.ExcelWriter(unmatched_path, engine="openpyxl") as writer:
            unmatched.to_excel(writer, index=False)

    if config.processing.enable_parquet and not no_export_parquet and not enriched_report.empty:
        enriched_report.to_parquet(parquet_path, index=False)
        stats.output_parquet_path = parquet_path

    registry.flush()

    summary_content = stats.to_markdown()
    summary_path.write_text(summary_content, encoding="utf-8")

    console.print(f"[green]Отчёт сохранён в {report_path}[/green]")
    console.print(f"Итоги сохранены в {summary_path}")


if __name__ == "__main__":
    app()
