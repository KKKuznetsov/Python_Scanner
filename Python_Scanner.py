# -*- coding: utf-8 -*-
"""
Python Scanner -> PostgreSQL (ops.file_registry)

Что делает:
1) Рекурсивно обходит root_dir и собирает список файлов.
2) Надёжно парсит путь вида:
   <root_dir> / (Сети|Дистрибьюторы) / YYYY / MM / Client / ReportType / file.ext
3) Пишет новые файлы в ops.file_registry со статусом NEW.
4) Не падает на "кривых" путях — такие файлы пропускает с предупреждением.
5) Сам создаёт схему/таблицу/индексы при первом запуске (в точности как в согласованном DDL).

Зависимости: psycopg2
Установка: python -m pip install psycopg2-binary
"""

import os
from datetime import datetime
import psycopg2

# === НАСТРОЙКИ ===
ROOT_DIR = r"C:\Users\user\Desktop\Поставщики данных"
SUPPLIERS = {"Сети", "Дистрибьюторы"}
ALLOWED_EXT = {".csv", ".xlsx", ".xls"}  # set() если нужны любые файлы

DB = dict(
    host="localhost",
    port=5432,
    database="etl_demo",
    user="postgres",
    password="Ваш пароль",
)

# === ФУНКЦИИ ===
def ensure_schema(conn):
    """Создаёт схему/таблицу/индексы, если их ещё нет (синхронно с вашим DDL)."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS ops;

    CREATE TABLE IF NOT EXISTS ops.file_registry (
        id             BIGSERIAL PRIMARY KEY,
        file_path      TEXT NOT NULL,
        uploaded_at    TIMESTAMP NOT NULL,
        status         TEXT NOT NULL CHECK (status IN ('NEW','PROCESSING','ERROR','CREATED','DELETE')),
        data_provider  TEXT NOT NULL CHECK (data_provider IN ('Сеть','Дистрибьютор')),
        report_year    SMALLINT NOT NULL CHECK (report_year >= 2000),
        report_month   SMALLINT NOT NULL CHECK (report_month BETWEEN 1 AND 12),
        client_name    TEXT NOT NULL,
        report_type    TEXT NOT NULL,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        error_reason   TEXT
    );

    -- Антидубли по пути
    CREATE UNIQUE INDEX IF NOT EXISTS uq_file_registry_file_path
        ON ops.file_registry(file_path);

    -- Индексы как в согласованном DDL
    CREATE INDEX IF NOT EXISTS idx_file_registry_status
        ON ops.file_registry(status);
    CREATE INDEX IF NOT EXISTS idx_file_registry_provider
        ON ops.file_registry(data_provider);
    CREATE INDEX IF NOT EXISTS idx_file_registry_period
        ON ops.file_registry(report_year, report_month);
    CREATE INDEX IF NOT EXISTS idx_file_registry_client
        ON ops.file_registry(client_name);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()  # фиксируем изменения


def should_skip(path: str) -> bool:
    """Фильтруем временные/неподходящие файлы по расширению."""
    name = os.path.basename(path)
    if name.startswith("~$") or name.lower().endswith(".tmp"):
        return True
    if not ALLOWED_EXT:
        return False
    return os.path.splitext(name)[1].lower() not in ALLOWED_EXT


def parse_file_info(full_path: str):
    """Парсим путь к файлу и достаём метаданные."""
    rel = os.path.relpath(full_path, ROOT_DIR)
    parts = rel.split(os.sep)

    supplier_idx = next((i for i, p in enumerate(parts) if p in SUPPLIERS), None)
    if supplier_idx is None:
        raise ValueError(f"Не найден поставщик ('Сети'/'Дистрибьюторы') в пути: {rel}")

    if len(parts) < supplier_idx + 6:
        raise ValueError(f"Слишком короткий путь для разбора: {rel}")

    supplier_folder = parts[supplier_idx]
    year_str = parts[supplier_idx + 1]
    month_str = parts[supplier_idx + 2]
    client = parts[supplier_idx + 3]
    report_type = parts[supplier_idx + 4]

    try:
        year = int(year_str)
        month = int(month_str)
    except ValueError:
        raise ValueError(f"Год/месяц не числа: year='{year_str}', month='{month_str}' в пути: {rel}")

    if not (1 <= month <= 12):
        raise ValueError(f"Некорректный месяц {month} в пути: {rel}")

    data_provider = "Сеть" if supplier_folder == "Сети" else "Дистрибьютор"

    return data_provider, year, month, client, report_type


def scan_and_insert():
    """Сканирует дерево папок и записывает новые файлы в БД."""
    total, added, skipped, bad = 0, 0, 0, 0

    with psycopg2.connect(**DB) as conn:
        ensure_schema(conn)

        with conn.cursor() as cur:
            for dirpath, _, filenames in os.walk(ROOT_DIR):
                for fname in filenames:
                    full = os.path.join(dirpath, fname)
                    total += 1

                    if should_skip(full):
                        skipped += 1
                        continue

                    try:
                        data_provider, year, month, client, report_type = parse_file_info(full)
                    except Exception as e:
                        bad += 1
                        print(f"[WARN] Пропускаю: {full} :: {e}")
                        continue

                    uploaded_at = datetime.fromtimestamp(os.path.getmtime(full))

                    cur.execute(
                        """
                        INSERT INTO ops.file_registry
                          (file_path, uploaded_at, status, data_provider, report_year, report_month, client_name, report_type)
                        VALUES
                          (%s, %s, 'NEW', %s, %s, %s, %s, %s)
                        ON CONFLICT (file_path) DO NOTHING;
                        """,
                        (full, uploaded_at, data_provider, year, month, client, report_type),
                    )
                    if cur.rowcount == 1:
                        added += 1

        conn.commit()  # фиксируем все вставки

    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"Всего просмотрено файлов: {total} | Добавлено новых: {added} | "
        f"Пропущено по фильтру: {skipped} | Пропущено из-за структуры: {bad}"
    )


def main():
    if not os.path.isdir(ROOT_DIR):
        raise SystemExit(f"ROOT_DIR не найден или не директория: {ROOT_DIR}")
    scan_and_insert()


if __name__ == "__main__":
    main()
