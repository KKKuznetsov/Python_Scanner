# -*- coding: utf-8 -*-
"""
Scanner -> PostgreSQL (ops.file_registry)

1) Рекурсивно обходит root_dir и собирает файлы.
2) Парсит путь вида:
   <root> / (Сети|Дистрибьюторы) / YYYY / MM / Client / ReportType / file.ext
3) Пишет новые файлы в ops.file_registry со статусом NEW (идемпотентно).
4) Некорректные пути пропускает с предупреждением.

Зависимости: psycopg2-binary
Установка:  python -m pip install psycopg2-binary
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
import psycopg2

SUPPLIERS = {"Сети", "Дистрибьюторы"}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True,
                   help="Корневая папка с данными (например, 'C:/Users/user/Desktop/Поставщики данных')")
    p.add_argument("--ext", default=".csv,.xlsx,.xls",
                   help="Разрешённые расширения через запятую. Пусто — любые (пример: '.csv,.xlsx')")
    p.add_argument("--dry-run", action="store_true",
                   help="Только анализ и вывод, без записи в БД")
    # Параметры БД (совместим с compose)
    p.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--db-port", type=int, default=int(os.environ.get("DB_PORT", 5432)))
    p.add_argument("--db-name", default=os.environ.get("DB_NAME", "appdb"))
    p.add_argument("--db-user", default=os.environ.get("DB_USER", "app"))
    p.add_argument("--db-pass", default=os.environ.get("DB_PASS", "secret"))
    return p.parse_args()

def is_allowed(name: str, allowed_exts):
    if name.startswith("~$") or name.lower().endswith(".tmp"):
        return False
    if not allowed_exts:
        return True
    ext = os.path.splitext(name)[1].lower()
    return ext in allowed_exts

def parse_file_info(rel_parts):
    """rel_parts = относительные сегменты пути от root (список строк)."""
    supplier_idx = next((i for i, p in enumerate(rel_parts) if p in SUPPLIERS), None)
    if supplier_idx is None:
        raise ValueError("Не найден поставщик ('Сети'/'Дистрибьюторы') в пути")

    # ожидаем минимум: supplier / YYYY / MM / client / report_type / filename
    if len(rel_parts) < supplier_idx + 6:
        raise ValueError("Слишком короткий путь для разбора (ожидается >= 6 сегментов после поставщика)")

    supplier_folder = rel_parts[supplier_idx]
    year_str = rel_parts[supplier_idx + 1]
    month_str = rel_parts[supplier_idx + 2]
    client = rel_parts[supplier_idx + 3]
    report_type = rel_parts[supplier_idx + 4]

    try:
        year = int(year_str)
        month = int(month_str)
    except ValueError:
        raise ValueError(f"Год/месяц не числа: year='{year_str}', month='{month_str}'")

    if not (1 <= month <= 12):
        raise ValueError(f"Некорректный месяц {month}")

    data_provider = "Сеть" if supplier_folder == "Сети" else "Дистрибьютор"
    return data_provider, year, month, client, report_type

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    root = Path(args.root).resolve()
    if not root.is_dir():
        sys.exit(f"ROOT_DIR не найден или не директория: {root}")

    allowed_exts = set(e.strip().lower() for e in args.ext.split(",")) if args.ext.strip() else set()

    total = added = skipped = bad = 0

    if args.dry_run:
        logging.info("Режим dry-run: БД не изменяется.")

    conn = None
    try:
        if not args.dry_run:
            conn = psycopg2.connect(
                host=args.db_host, port=args.db_port,
                dbname=args.db_name, user=args.db_user, password=args.db_pass
            )
            conn.autocommit = False

        for dirpath, _, filenames in os.walk(root):
            for fname in filenames:
                total += 1
                if not is_allowed(fname, allowed_exts):
                    skipped += 1
                    continue

                full_path = Path(dirpath) / fname
                rel = full_path.relative_to(root)
                rel_parts = list(rel.parts)

                try:
                    data_provider, year, month, client, report_type = parse_file_info(rel_parts)
                except Exception as e:
                    bad += 1
                    logging.warning("Пропускаю: %s :: %s", rel.as_posix(), e)
                    continue

                # mtime -> UTC (TIMESTAMPTZ)
                mtime = datetime.fromtimestamp(full_path.stat().st_mtime, tz=timezone.utc)

                if args.dry_run:
                    if added < 20:
                        print(rel.as_posix())
                    added += 1
                    continue

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ops.file_registry
                          (file_path, uploaded_at, status, data_provider, report_year, report_month, client_name, report_type)
                        VALUES
                          (%s, %s, 'NEW', %s, %s, %s, %s, %s)
                        ON CONFLICT (file_path) DO NOTHING;
                        """,
                        # ВАЖНО: сохраняем относительный путь (портативнее, чем абсолютный)
                        (rel.as_posix(), mtime, data_provider, year, month, client, report_type),
                    )
                    if cur.rowcount == 1:
                        added += 1

        if conn:
            conn.commit()

    finally:
        if conn:
            conn.close()

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] Всего файлов: {total} | Добавлено (новых/учтённых): {added} | "
          f"Пропущено по фильтрам: {skipped} | Пропущено из-за структуры: {bad}")

if __name__ == "__main__":
    main()
