"""
Interactive command‑line tool for managing an apart‑hotel database.

This script builds upon the SQLite database schema created by ``create_database.py``.
It provides a simple text‑based menu to perform common operations:

    1. Add a guest
    2. Add a room
    3. Make a booking
    4. Record a payment
    5. Log an expense
    6. Schedule a cleaning task
    7. View bookings
    8. View expenses
    9. Exit

The script is designed for users with no programming background. Follow the
prompts to enter data. Each action updates the underlying SQLite database
(``aparthotel.db`` by default). You can run the script via:

    python manage_aparthotel.py

Optionally, specify a custom database file with ``--db-file``:

    python manage_aparthotel.py --db-file my_database.db

If the specified database file does not exist, the script will attempt to
initialize it by creating the necessary tables.
"""

import argparse
import os
# SQLite support has been removed.  Use the Flask application's
# PostgreSQL connection helper instead.
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore
from app import get_db_connection
from datetime import datetime


# SQLite database filename constant removed.  DATABASE_URL should be used instead.



def ensure_db_exists(_unused: str = ""):
    """
    Return a PostgreSQL connection using the Flask application's helper.

    The DATABASE_URL environment variable must be set.  Schema creation is
    handled automatically by the application; SQLite support has been removed.
    """
    # Delegate connection handling to the Flask app's get_db_connection.
    conn = get_db_connection()
    return conn


def prompt(prompt_text: str) -> str:
    """Prompt the user for input and return the entered value."""
    return input(prompt_text).strip()


def add_guest(conn) -> None:
    name = prompt("Введите имя гостя: ")
    phone = prompt("Введите телефон (можно оставить пустым): ")
    email = prompt("Введите email (можно оставить пустым): ")
    notes = prompt("Примечания (можно оставить пустым): ")
    with conn:
        conn.execute(
            "INSERT INTO guests (name, phone, email, notes) VALUES (?, ?, ?, ?)",
            (name, phone or None, email or None, notes or None),
        )
    print("Гость добавлен успешно!\n")


def add_room(conn) -> None:
    room_number = prompt("Введите номер комнаты: ")
    capacity_str = prompt("Введите вместимость комнаты (число, можно оставить пустым): ")
    notes = prompt("Примечания (можно оставить пустым): ")
    capacity = int(capacity_str) if capacity_str else None
    try:
        with conn:
            conn.execute(
                "INSERT INTO rooms (room_number, capacity, notes) VALUES (?, ?, ?)",
                (room_number, capacity, notes or None),
            )
        print("Комната добавлена успешно!\n")
    except Exception:
        # Unique constraint violations raise different exceptions in psycopg2; catch generic
        print("Ошибка: номер комнаты уже существует!\n")


def choose_guest(conn) -> int:
    """Prompt the user to select a guest and return the guest ID."""
    guests = conn.execute("SELECT id, name, phone FROM guests ORDER BY id").fetchall()
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
    rooms = conn.execute("SELECT id, room_number FROM rooms ORDER BY id").fetchall()
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
    with conn:
        conn.execute(
            """
            INSERT INTO bookings (guest_id, room_id, check_in_date, check_out_date,
                                  status, total_amount, paid_amount, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    bookings = conn.execute(
        "SELECT id, guest_id, room_id, check_in_date, check_out_date FROM bookings ORDER BY id"
    ).fetchall()
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
    with conn:
        conn.execute(
            """
            INSERT INTO payments (booking_id, amount, date, method, status, notes)
            VALUES (?, ?, ?, ?, ?, ?)
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
    with conn:
        conn.execute(
            "INSERT INTO expenses (category, amount, date, description) VALUES (?, ?, ?, ?)",
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
    with conn:
        conn.execute(
            "INSERT INTO cleaning_tasks (room_id, scheduled_date, status, notes) VALUES (?, ?, ?, ?)",
            (room_id, date_str, status, notes or None),
        )
    print("Уборка запланирована!\n")


def view_bookings(conn) -> None:
    print("\nСписок бронирований:")
    bookings = conn.execute(
        """
        SELECT b.id, g.name, r.room_number, b.check_in_date, b.check_out_date,
               b.status, b.total_amount, b.paid_amount
        FROM bookings AS b
        JOIN guests AS g ON b.guest_id = g.id
        JOIN rooms AS r ON b.room_id = r.id
        ORDER BY b.check_in_date DESC
        """
    ).fetchall()
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
    expenses = conn.execute(
        "SELECT id, category, amount, date, description FROM expenses ORDER BY date DESC"
    ).fetchall()
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
        description="Command‑line interface for managing an apart‑hotel database (PostgreSQL only)."
    )
    parser.add_argument(
        "--db-url",
        type=str,
        help="PostgreSQL database URL.  If omitted, the DATABASE_URL environment variable will be used."
    )
    args = parser.parse_args()

    # Determine the database URL
    db_url = args.db_url or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "DATABASE_URL must be provided via --db-url or environment; SQLite support has been removed."
        )
    # Set the environment variable so app.get_db_connection picks it up
    os.environ["DATABASE_URL"] = db_url
    # Ensure the database exists / connect
    conn = ensure_db_exists(db_url)

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