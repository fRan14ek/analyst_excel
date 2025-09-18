# ETL Sales Processor

Этот проект предоставляет CLI-утилиту для агрегации еженедельных Excel/CSV отчётов
по площадкам Ozon, Wildberries и Яндекс.Маркет в единую базу данных Excel и сводный
отчёт.

## Структура проекта

```
etl_sales/
  config.yaml
  mappings/
  data/
  etl/
  tests/
```

Основной код находится в пакете `etl_sales/etl`. Команда CLI регистрирована в
модуле `etl_sales/etl/cli.py` и запускается через `python -m etl_sales.etl.cli`.

## Пример запуска

```bash
python -m etl_sales.etl.cli load-week \
  --start 2025-09-08 \
  --end 2025-09-14 \
  --base etl_sales/data/base.xlsx \
  --week 202536 \
  --save-to etl_sales/data/output/
```

Поддерживаются флаги `--dry-run`, `--fail-on-invalid-articul`, `--no-export-parquet`
и `--platform`.

## Тестирование

```bash
pytest
```
