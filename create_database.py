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
from datetime import datetime

# PostgreSQL support only. SQLite has been removed.
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


def create_tables(connection) -> None:
    """Create all tables needed for the apart‑hotel management system.

    Args:
        connection: An open SQLite connection. Tables will be created within
            this database. If tables already exist, this function will not
            recreate them.
    """
    cursor = connection.cursor()
    # Foreign keys are enforced by default in PostgreSQL.  SQLite support has been removed.

    # Table: guests
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


    # NOTE: We intentionally create users and rooms before any tables
    # that reference them via foreign keys.  In PostgreSQL, the referenced
    # table must exist at the time a foreign key is declared.  If you
    # create bookings before rooms, or rooms before users, the database
    # would raise an UndefinedTable error.  The order below prevents
    # such errors.

    # Table: users (needs to be created before rooms)
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

    # Table: rooms (references users via owner_id)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            -- Object name (user-defined name for the listing).  Formerly
            -- called room_number; we retain the same column name for
            -- compatibility but the label in the UI now reads "Название объекта".
            room_number TEXT NOT NULL UNIQUE,
            capacity INTEGER,
            notes TEXT,
            -- URL of the external listing/advertisement for this object (optional)
            listing_url TEXT,
            -- Residential complex (ЖК) identifier or name for filtering
            residential_complex TEXT
            -- Additional columns are added below using ALTER TABLE to
            -- support upgrades to existing databases.
        );
        """
    )
    # If the rooms table already existed, it may be missing new columns.  Use
    # ALTER TABLE IF NOT EXISTS to add them without raising errors.  Not
    # every PostgreSQL version supports IF NOT EXISTS for ADD COLUMN (9.6+
    # does), but it avoids duplicate column errors if the migration
    # runs multiple times.
    alter_statements = [
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS owner_id INTEGER",  # owner reference; foreign key can be added separately if needed
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS num_rooms INTEGER",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS floor INTEGER",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS floors_total INTEGER",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS area_total REAL",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS area_kitchen REAL",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS condition TEXT",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS kitchen_studio BOOLEAN",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS country TEXT",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS city TEXT",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS street TEXT",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS house_number TEXT",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS latitude REAL",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS longitude REAL",
        "ALTER TABLE rooms ADD COLUMN IF NOT EXISTS price_per_night REAL"
    ]
    for stmt in alter_statements:
        try:
            cursor.execute(stmt)
        except Exception:
            # Ignore errors (e.g. unsupported IF NOT EXISTS); continue to next
            pass
    # Try to add a foreign key constraint on owner_id to users(id).
    # If the constraint already exists or the syntax is unsupported, ignore the error.
    try:
        cursor.execute(
            "ALTER TABLE rooms ADD CONSTRAINT fk_rooms_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE SET NULL"
        )
    except Exception:
        pass
    # Commit after creating/updating rooms so that subsequent foreign key constraints
    # (e.g., bookings referencing rooms) do not fail due to uncommitted DDL.
    connection.commit()

    # Table: bookings (references guests and rooms)
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

    # Table: payments (references bookings)
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

    # Table: cleaning_tasks (references rooms)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaning_tasks (
            id SERIAL PRIMARY KEY,
            room_id INTEGER NOT NULL,
            -- Scheduled date/time of the cleaning task.  Use TIMESTAMP for PostgreSQL.
            scheduled_date TIMESTAMP NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        """
    )

    # Table: room_photos (stores uploaded images for each room)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS room_photos (
            id SERIAL PRIMARY KEY,
            room_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
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
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Table: blacklist for storing sanitized phone numbers of guests in a blacklist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            phone TEXT PRIMARY KEY
        );
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

    # Table: favorites.  Stores apartments (rooms) favorited by users.  A composite
    # primary key is used so that each user can only favorite a room once.  When
    # either the user or room is deleted, the corresponding favorite record is
    # removed automatically via ON DELETE CASCADE.  This table enables the
    # “Избранное” feature in the public listings and user profile.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER NOT NULL,
            room_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, room_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        """
    )

    # All remaining tables have been created.  Commit final changes so that
    # they persist.  If autocommit is enabled on the PostgreSQL connection
    # this call will have no effect, but it's harmless.  Without this
    # explicit commit, some deployments (e.g., Render) might not finalize
    # DDL statements.
    connection.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create an aparthotel management database.  By default this script "
            "initializes a local SQLite database, but you can specify a "
            "PostgreSQL database URL via --db-url or the DATABASE_URL "
            "environment variable.  When using PostgreSQL, the tables are "
            "created on the remote server."
        )
    )
    # Remove --db-file option; only --db-url or DATABASE_URL may be used.  SQLite support has been removed.
    parser.add_argument(
        "--db-url",
        type=str,
        help=(
            "PostgreSQL database URL.  If omitted, the DATABASE_URL environment variable will be used if set."
        ),
    )
    args = parser.parse_args()

    # Determine PostgreSQL URL from command-line option or environment.  SQLite fallback has been removed.
    db_url = args.db_url or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "Error: a PostgreSQL database URL must be provided via --db-url or the DATABASE_URL environment variable; SQLite support has been removed."
        )
    # Normalize URL scheme if necessary
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if psycopg2 is None:
        raise SystemExit(
            "Error: psycopg2 package is required to connect to PostgreSQL but is not installed."
        )
    try:
        # Default to requiring SSL unless PGSSLMODE is set differently
        connection = psycopg2.connect(db_url, sslmode=os.environ.get("PGSSLMODE", "require"))
    except Exception as exc:
        raise SystemExit(f"Failed to connect to PostgreSQL database: {exc}")
    try:
        create_tables(connection)
        # Ensure DDL changes are committed
        connection.commit()
    finally:
        connection.close()
    print(f"Database schema created successfully on PostgreSQL: {db_url}")


if __name__ == "__main__":
    main()