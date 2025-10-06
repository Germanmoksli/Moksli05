"""
Этот скрипт создаёт структуру базы данных для апарт‑отеля.  Поддерживается только PostgreSQL.
"""

import argparse
import os
from datetime import datetime

# PostgreSQL support only. SQLite has been removed.
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


def create_tables(connection) -> None:
    """Создать все таблицы для системы управления апарт‑отелем."""

    cursor = connection.cursor()

    # Таблица гостей
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS guests (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT,
            extra_phone TEXT,
            email TEXT,
            notes TEXT,
            birth_date DATE,
            photo TEXT
        );
        """
    )

    # Важно создать пользователей и квартиры до таблиц, которые используют их внешние ключи.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            name TEXT,
            contact_info TEXT,
            photo TEXT
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            room_number TEXT NOT NULL UNIQUE,
            capacity INTEGER,
            notes TEXT,
            listing_url TEXT,
            residential_complex TEXT,
            owner_id INTEGER,
            FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE SET NULL
        );
        """
    )

    # После создания users и rooms зафиксируем изменения,
    # чтобы таблицы точно существовали для следующих внешних ключей.
    connection.commit()

    # Таблица бронирований
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bookings (
            id SERIAL PRIMARY KEY,
            guest_id INTEGER NOT NULL,
            room_id INTEGER NOT NULL,
            check_in_date DATE NOT NULL,
            check_out_date DATE NOT NULL,
            status TEXT NOT NULL DEFAULT 'booked',
            total_amount REAL,
            paid_amount REAL,
            notes TEXT,
            FOREIGN KEY (guest_id) REFERENCES guests(id) ON DELETE CASCADE,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        """
    )

    # Платежи
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            booking_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            date DATE NOT NULL,
            method TEXT,
            status TEXT,
            notes TEXT,
            FOREIGN KEY (booking_id) REFERENCES bookings(id) ON DELETE CASCADE
        );
        """
    )

    # Расходы
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            date DATE NOT NULL,
            description TEXT
        );
        """
    )

    # Задачи по уборке
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaning_tasks (
            id SERIAL PRIMARY KEY,
            room_id INTEGER NOT NULL,
            scheduled_date TIMESTAMP NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        """
    )

    # Запросы на регистрацию (ожидают одобрения владельцем)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS registration_requests (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Чёрный список телефонов
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            phone TEXT PRIMARY KEY
        );
        """
    )

    # Сообщения (простой чат между сотрудниками/владельцами)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    # Финальный коммит для фиксации всех созданных таблиц
    connection.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create an aparthotel management database. Only PostgreSQL is supported."
        )
    )
    parser.add_argument(
        "--db-url",
        type=str,
        help="PostgreSQL database URL.  If omitted, DATABASE_URL environment variable will be used if set.",
    )
    args = parser.parse_args()

    db_url = args.db_url or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "Error: a PostgreSQL database URL must be provided via --db-url or DATABASE_URL."
        )
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if psycopg2 is None:
        raise SystemExit(
            "Error: psycopg2 package is required to connect to PostgreSQL but is not installed."
        )
    try:
        connection = psycopg2.connect(db_url, sslmode=os.environ.get("PGSSLMODE", "require"))
    except Exception as exc:
        raise SystemExit(f"Failed to connect to PostgreSQL database: {exc}")
    try:
        create_tables(connection)
        connection.commit()
    finally:
        connection.close()
    print(f"Database schema created successfully on PostgreSQL: {db_url}")


if __name__ == "__main__":
    main()
