"""
This script sets up a simple SQLite database for an apartment‑hotel (apart‑hotel)
management system. It defines several tables to handle reservations,
guests, rooms, payments, expenses, cleaning tasks, and users with different
roles (owner, manager, maid). Running this script will create a SQLite
database file named ``aparthotel.db`` in the current directory (unless a
different file name is supplied via the `--db-file` command line argument).

You do not need any external packages to run this script; it relies solely
on Python's standard library.

Usage:

    python create_database.py

You can also specify a custom database file name:

    python create_database.py --db-file my_database.db

The database schema includes the following tables:

    - guests
    - rooms
    - bookings
    - payments
    - expenses
    - cleaning_tasks
    - users

Each table is created with columns appropriate to manage an apart‑hotel.
"""

import argparse
import os
import sqlite3
from datetime import datetime


def create_tables(connection: sqlite3.Connection) -> None:
    """Create all tables needed for the apart‑hotel management system.

    Args:
        connection: An open SQLite connection. Tables will be created within
            this database. If tables already exist, this function will not
            recreate them.
    """
    cursor = connection.cursor()

    # Enable foreign key support (disabled by default in SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Table: guests
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS guests (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            notes TEXT
        );
        """
    )

    # Table: rooms
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            room_number TEXT NOT NULL UNIQUE,
            capacity INTEGER,
            notes TEXT,
            -- URL of the external listing/advertisement for this object (optional)
            listing_url TEXT,
            -- Residential complex (ЖК) identifier or name for filtering
            residential_complex TEXT
        );
        """
    )

    # Table: bookings
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

    # Table: payments
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

    # Table: expenses
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

    # Table: cleaning_tasks
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaning_tasks (
            id SERIAL PRIMARY KEY,
            room_id INTEGER NOT NULL,
            scheduled_date DATETIME NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        """
    )

    # Table: users
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

    # Table: registration requests (pending user signups awaiting owner approval)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS registration_requests (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Commit changes
    connection.commit()

    # Table: blacklist for storing sanitized phone numbers of guests in a blacklist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            phone TEXT PRIMARY KEY
        )
        """
    )

    # Table: messages for employee chat. Stores a simple log of chat
    # messages with the user_id of the author, the message text and
    # a timestamp as an ISO string.  Messages are deleted when the
    # corresponding user is removed (ON DELETE CASCADE).
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an aparthotel management database (SQLite).")
    parser.add_argument(
        "--db-file",
        type=str,
        default="aparthotel.db",
        help="Filename for the SQLite database (default: aparthotel.db)",
    )
    args = parser.parse_args()

    db_file_path = os.path.abspath(args.db_file)

    # Connect to the SQLite database (this will create the file if it doesn't exist)
    connection = sqlite3.connect(db_file_path)
    try:
        create_tables(connection)
    finally:
        connection.close()

    print(f"Database created successfully at: {db_file_path}")


if __name__ == "__main__":
    main()