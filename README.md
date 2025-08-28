# Python_Scanner

## Описание
Скрипт `Python_Scanner.py` рекурсивно проходит по локальному хранилищу, извлекает метаданные файлов и заносит их в таблицу `ops.file_registry`.

**Алгоритм работы:**
1. Рекурсивный обход `root_dir`.
2. Проверка структуры пути: <root_dir> / (Сети|Дистрибьюторы) / YYYY / MM / Client / ReportType / file.ext
3. Фильтрация файлов по расширениям (`.csv, .xlsx, .xls` по умолчанию).
4. Запись новых файлов в `ops.file_registry` со статусом `NEW`.
5. Пропуск «битых» или некорректных путей с логом-предупреждением.
6. Идемпотентность: повторный запуск не дублирует уже учтённые файлы.

## Установка и запуск

### Требования
- Python 3.9+
- PostgreSQL (таблица `ops.file_registry` уже создана — см. проект [Создание таблицы file_registry](https://github.com/KKKuznetsov/Create_table_file_registry_PostgreSQL))

### Установка зависимостей

- pip install psycopg2-binary

## Быстрый старт

1) Тестовый запуск (dry-run, без записи в БД)
- Через командную строку переходим в папку, где лежит сканер
- запускаем команду, пример:
python Python_Scanner.py --root "C:/Users/user/Desktop/Поставщики данных" --dry-run

2) Запись в БД
- Через командную строку переходим в папку, где лежит сканер
- запускаем команду, пример:
- python Python_Scanner.py --root "C:/Users/user/Desktop/Поставщики данных" --db-user app --db-pass secret

Любой параметр можно не указывать, если он совпадает со значением по умолчанию:
host=localhost, port=5432, dbname=appdb, user=app, pass=secret.

