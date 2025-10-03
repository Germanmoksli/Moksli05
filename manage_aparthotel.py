"""
Interactive command‑line tool for managing an apart‑hotel database.

This script connects directly to a PostgreSQL database and uses the same
schema as the Flask web application.  Support for SQLite has been
removed in order to avoid conflicts between differing SQL dialects.
All operations such as adding guests, rooms, bookings, payments,
expenses and cleaning tasks are performed through psycopg2.  The
database connection URL must be provided via the ``--db-url`` option
or through the ``DATABASE_URL`` environment variable.  If the target
database does not contain the required tables, they will be created
automatically by importing and calling ``create_tables`` from
``create_database.py``.

To run the script:

    python manage_aparthotel.py --db-url postgresql://user:pass@host:port/dbname

If you omit ``--db-url``, the script will attempt to use
``DATABASE_URL`` from the environment.  Should the connection or
initialisation fail an error will be displayed.
"""

import argparse
import os
from datetime import datetime

# psycopg2 is required for PostgreSQL connectivity.  We import it here
# so that a clear error is raised if the dependency is missing.
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore

from create_database import create_tables


def ensure_db_exists(db_url: str):
    """
    Connect to the specified PostgreSQL database and ensure the schema exists.

    Args:
        db_url: PostgreSQL connection URL.  If the URL uses the
            deprecated ``postgres://`` scheme it will be normalised
            to ``postgresql://``.  SSL mode is configured via the
            ``PGSSLMODE`` environment variable (default: ``require``).

    Returns:
        An open psycopg2 connection with autocommit enabled.
    """
    if not db_url:
        raise SystemExit(
            "A database URL must be provided via --db-url or the DATABASE_URL environment variable."
        )
    # Normalise the scheme for psycopg2
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if psycopg2 is None:
        raise SystemExit(
            "psycopg2 is required to run this script but is not installed. Install psycopg2 or psycopg2-binary."
        )
    try:
        conn = psycopg2.connect(db_url, sslmode=os.environ.get("PGSSLMODE", "require"))
        try:
            conn.autocommit = True
        except Exception:
            pass
        # Create tables if they do not exist
        create_tables(conn)
        return conn
    except Exception as e:
        raise SystemExit(f"Failed to connect to database: {e}")


def prompt(prompt_text: str) -> str:
    """Prompt the user for input and return the entered value."""
    return input(prompt_text).strip()


def add_guest(conn) -> None:
    name = prompt("Введите имя гостя: ")
    phone = prompt("Введите телефон (можно оставить пустым): ")
    email = prompt("Введите email (можно оставить пустым): ")
    notes = prompt("Примечания (можно оставить пустым): ")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO guests (name, phone, email, notes) VALUES (%s, %s, %s, %s)",
            (name, phone or None, email or None, notes or None),
        )
    print("Гость добавлен успешно!\n")


def add_room(conn) -> None:
    room_number = prompt("Введите номер комнаты: ")
    capacity_str = prompt("Введите вместимость комнаты (число, можно оставить пустым): ")
    notes = prompt("Примечания (можно оставить пустым): ")
    capacity = int(capacity_str) if capacity_str else None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rooms (room_number, capacity, notes) VALUES (%s, %s, %s)",
                (room_number, capacity, notes or None),
            )
        print("Комната добавлена успешно!\n")
    except Exception:
        print("Ошибка: номер комнаты уже существует или другая ошибка при добавлении!\n")


def choose_guest(conn) -> int:
    """Prompt the user to select a guest and return the guest ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, phone FROM guests ORDER BY id")
        guests = cur.fetchall()
    if not guests:
        print("Нет доступных гостей. Сначала добавьте гостя.\n")
        return -1
    print("\nВыберите гостя:")
    for idx, (guest_id, name, phone) in enumerate(guests, start=1):
        phone_str = f" ({phone})" if phone else ""
        print(f"{idx}. {name}{phone_str}")
    sel = prompt("Введите номер из списка: ")
    try:
        sel_idx = int(sel)
        if 1 <= sel_idx <= len(guests):
            return guests[sel_idx - 1][0]
    except ValueError:
        pass
    print("Некорректный выбор.\n")
    return -1


def choose_room(conn) -> int:
    """Prompt the user to select a room and return the room ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, room_number FROM rooms ORDER BY id")
        rooms = cur.fetchall()
    if not rooms:
        print("Нет доступных комнат. Сначала добавьте комнату.\n")
        return -1
    print("\nВыберите комнату:")
    for idx, (room_id, room_number) in enumerate(rooms, start=1):
        print(f"{idx}. Комната {room_number}")
    sel = prompt("Введите номер из списка: ")
    try:
        sel_idx = int(sel)
        if 1 <= sel_idx <= len(rooms):
            return rooms[sel_idx - 1][0]
    except ValueError:
        pass
    print("Некорректный выбор.\n")
    return -1


def add_booking(conn) -> None:
    print("\nСоздание бронирования")  # Display heading
    guest_id = choose_guest(conn)
    if guest_id == -1:
        return
    room_id = choose_room(conn)
    if room_id == -1:
        return
    check_in = prompt("Введите дату заезда (формат ГГГГ-ММ-ДД): ")
    check_out = prompt("Введите дату выезда (формат ГГГГ-ММ-ДД): ")
    status = prompt("Статус (по умолчанию 'booked'): ") or "booked"
    total_amount_str = prompt("Общая сумма (можно оставить пустым): ")
    paid_amount_str = prompt("Уплаченная сумма (можно оставить пустым): ")
    notes = prompt("Примечания (можно оставить пустым): ")
    total_amount = float(total_amount_str) if total_amount_str else None
    paid_amount = float(paid_amount_str) if paid_amount_str else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bookings (
                guest_id, room_id, check_in_date, check_out_date,
                status, total_amount, paid_amount, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                guest_id,
                room_id,
                check_in,
                check_out,
                status,
                total_amount,
                paid_amount,
                notes or None,
            ),
        )
    print("Бронирование добавлено успешно!\n")


def add_payment(conn) -> None:
    print("\nЗапись платежа")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, guest_id, room_id, check_in_date, check_out_date FROM bookings ORDER BY id"
        )
        bookings = cur.fetchall()
    if not bookings:
        print("Нет существующих бронирований. Сначала создайте бронирование.\n")
        return
    print("Выберите бронирование, для которого вносится платеж:")
    for idx, (bid, guest_id, room_id, check_in, check_out) in enumerate(bookings, start=1):
        print(f"{idx}. Бронь {bid} (гость ID {guest_id}, комната ID {room_id}, {check_in}–{check_out})")
    sel = prompt("Введите номер из списка: ")
    try:
        sel_idx = int(sel)
        if 1 <= sel_idx <= len(bookings):
            booking_id = bookings[sel_idx - 1][0]
        else:
            raise ValueError
    except ValueError:
        print("Некорректный выбор.\n")
        return
    amount_str = prompt("Введите сумму платежа: ")
    try:
        amount = float(amount_str)
    except ValueError:
        print("Некорректная сумма.\n")
        return
    date = prompt("Введите дату платежа (ГГГГ-ММ-ДД). Оставьте пустым для текущей даты: ")
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    method = prompt("Метод оплаты (наличные, карта и т.д., можно пусто): ")
    status = prompt("Статус платежа (можно пусто): ")
    notes = prompt("Примечания (можно пусто): ")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments (booking_id, amount, date, method, status, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (booking_id, amount, date, method or None, status or None, notes or None),
        )
    print("Платеж записан!\n")


def add_expense(conn) -> None:
    print("\nЗапись расхода")
    category = prompt("Категория расхода (например, уборка, ремонт): ")
    amount_str = prompt("Сумма: ")
    try:
        amount = float(amount_str)
    except ValueError:
        print("Некорректная сумма.\n")
        return
    date = prompt("Дата (ГГГГ-ММ-ДД, оставьте пустым для текущей даты): ")
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    description = prompt("Описание (можно пусто): ")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO expenses (category, amount, date, description) VALUES (%s, %s, %s, %s)",
            (category, amount, date, description or None),
        )
    print("Расход добавлен!\n")


def schedule_cleaning(conn) -> None:
    print("\nПланирование уборки")
    room_id = choose_room(conn)
    if room_id == -1:
        return
    date_str = prompt("Введите дату и время уборки (ГГГГ-ММ-ДД ЧЧ:ММ): ")
    # We accept the string as is; SQLite will store it as text/datetime.
    status = prompt("Статус (по умолчанию 'scheduled'): ") or "scheduled"
    notes = prompt("Примечания (можно пусто): ")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO cleaning_tasks (room_id, scheduled_date, status, notes) VALUES (%s, %s, %s, %s)",
            (room_id, date_str, status, notes or None),
        )
    print("Уборка запланирована!\n")


def view_bookings(conn) -> None:
    print("\nСписок бронирований:")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.id, g.name, r.room_number, b.check_in_date, b.check_out_date,
                   b.status, b.total_amount, b.paid_amount
            FROM bookings AS b
            JOIN guests AS g ON b.guest_id = g.id
            JOIN rooms AS r ON b.room_id = r.id
            ORDER BY b.check_in_date DESC
            """
        )
        bookings = cur.fetchall()
    if not bookings:
        print("Пока нет бронирований.\n")
        return
    for row in bookings:
        (
            booking_id,
            guest_name,
            room_number,
            check_in_date,
            check_out_date,
            status,
            total_amount,
            paid_amount,
        ) = row
        print(
            f"ID {booking_id}: {guest_name} в комнате {room_number} "
            f"{check_in_date}–{check_out_date}, статус: {status}, "
            f"общая сумма: {total_amount or 0}, оплачено: {paid_amount or 0}"
        )
    print()


def view_expenses(conn) -> None:
    print("\nСписок расходов:")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, category, amount, date, description FROM expenses ORDER BY date DESC"
        )
        expenses = cur.fetchall()
    if not expenses:
        print("Пока нет расходов.\n")
        return
    for row in expenses:
        exp_id, category, amount, date, description = row
        desc_str = f" ({description})" if description else ""
        print(f"ID {exp_id}: {date} – {category}: {amount}{desc_str}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Command‑line interface for managing an apart‑hotel database via PostgreSQL."
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (can also be provided via the DATABASE_URL environment variable)",
    )
    args = parser.parse_args()

    # Establish a database connection and ensure tables exist
    conn = ensure_db_exists(args.db_url)

    # Main menu loop
    while True:
        print(
            """
Меню:
 1. Добавить гостя
 2. Добавить комнату
 3. Создать бронирование
 4. Записать платеж
 5. Записать расход
 6. Запланировать уборку
 7. Просмотреть бронирования
 8. Просмотреть расходы
 9. Выход
"""
        )
        choice = prompt("Выберите действие (1–9): ")
        if choice == "1":
            add_guest(conn)
        elif choice == "2":
            add_room(conn)
        elif choice == "3":
            add_booking(conn)
        elif choice == "4":
            add_payment(conn)
        elif choice == "5":
            add_expense(conn)
        elif choice == "6":
            schedule_cleaning(conn)
        elif choice == "7":
            view_bookings(conn)
        elif choice == "8":
            view_expenses(conn)
        elif choice == "9":
            print("Выход. До свидания!")
            break
        else:
            print("Некорректный выбор. Попробуйте снова.\n")


if __name__ == "__main__":
    main()