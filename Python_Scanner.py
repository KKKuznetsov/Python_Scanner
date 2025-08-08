import os
import psycopg2
from datetime import datetime

# === Настройки ===
# Корневая папка, где лежит структура данных (можно заменить на путь к SFTP, если он смонтирован в систему)
root_dir = r"C:\Users\user\Desktop\Поставщики данных"

# Параметры подключения к базе PostgreSQL
db_config = {
    "host": "localhost",         # Адрес сервера базы данных
    "database": "etl_demo",      # Имя базы данных
    "user": "postgres",          # Имя пользователя
    "password": "your_password"  # Пароль пользователя
}

# === Функции ===

def parse_file_info(file_path):
    """
    Разбор пути к файлу по структуре:
    Поставщики данных / (Сети | Дистрибьюторы) / YYYY / MM / Client / ReportType / file.ext
    
    Возвращает:
    - Поставщик данных ("Сети" или "Дистрибьюторы")
    - Год отчета (int)
    - Месяц отчета (int)
    - Название клиента (str)
    - Тип отчета (str)
    """
    # Убираем корневую часть пути и делим оставшийся путь на сегменты
    parts = file_path.replace(root_dir, "").strip("\\/").split(os.sep)
    data_provider = parts[1]              # Папка "Сети" или "Дистрибьюторы"
    year = int(parts[2])                  # Год
    month = int(parts[3])                 # Месяц
    client = parts[4]                     # Клиент
    report_type = parts[5]                 # Тип отчета
    return data_provider, year, month, client, report_type

def get_all_files():
    """
    Рекурсивно обходит все папки начиная с root_dir
    и возвращает список полных путей ко всем найденным файлам.
    """
    file_list = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            file_list.append(full_path)
    return file_list

def get_existing_files(cursor):
    """
    Получает список уже учтённых файлов из таблицы file_registry.
    Возвращает set с путями для быстрого поиска.
    """
    cursor.execute("SELECT file_path FROM ops.file_registry;")
    return {row[0] for row in cursor.fetchall()}

# === Основная логика ===

def main():
    # Подключаемся к базе данных
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Получаем список всех файлов в структуре
    all_files = get_all_files()
    
    # Получаем список уже записанных в базу файлов
    existing_files = get_existing_files(cur)

    # Находим новые файлы (те, которых нет в таблице)
    new_files = [f for f in all_files if f not in existing_files]

    # Если новые файлы есть — записываем их в базу
    for file_path in new_files:
        data_provider, year, month, client, report_type = parse_file_info(file_path)
        uploaded_at = datetime.fromtimestamp(os.path.getmtime(file_path))  # Дата изменения файла (как дата загрузки)

        cur.execute("""
            INSERT INTO ops.file_registry
            (file_path, uploaded_at, status, data_provider, report_year, report_month, client_name, report_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            file_path,
            uploaded_at,
            "NEW",          # Новый файл всегда получает статус NEW
            data_provider,
            year,
            month,
            client,
            report_type
        ))

    # Фиксируем изменения в базе
    conn.commit()
    cur.close()
    conn.close()

    # Логируем результат
    print(f"[{datetime.now()}] Добавлено новых файлов: {len(new_files)}")

# === Запуск ===
if __name__ == "__main__":
    main()
