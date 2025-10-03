"""
Database schema initialisation for the aparthotel management system.

This script connects to a PostgreSQL database and creates all of the
tables required by the application.  Unlike earlier versions of this
project, support for SQLite has been removed.  A PostgreSQL connection
URL must be provided via the ``--db-url`` command‑line option or the
``DATABASE_URL`` environment variable.  The tables created by this
script mirror those used by the Flask application (``app.py``).

Usage:

    python create_database.py --db-url postgresql://user:password@host:port/dbname

If the ``--db-url`` flag is omitted the script will look for a
``DATABASE_URL`` environment variable.  The schema includes tables
for guests, rooms, bookings, payments, expenses, cleaning tasks,
users, registration requests, and supporting metadata like a
blacklist.  All DDL statements are executed with autocommit enabled
so that they run outside of explicit transactions.  If the database
already contains these tables the ``CREATE TABLE IF NOT EXISTS``
clauses will ensure nothing is dropped or overwritten.
"""

import argparse
import os
from datetime import datetime

# psycopg2 is required for PostgreSQL support.  Fail fast if it is not
# available so that users immediately know why their database cannot be
# initialised.  We import within a try/except to provide a clearer
# message when the package is missing.
try:
    import psycopg2  # type: ignore
except Exception as e:  # pragma: no cover - import error handling
    psycopg2 = None  # type: ignore


def create_tables(connection) -> None:
    """
    Create all tables needed for the apart‑hotel management system on
    PostgreSQL.  This function assumes that the supplied ``connection``
    object is a ``psycopg2`` connection.  ``PRAGMA`` directives and
    other SQLite‑specific operations have been removed.  Each table is
    created with ``CREATE TABLE IF NOT EXISTS`` to allow repeated
    execution without errors.  ``SERIAL`` columns are used for
    auto‑incrementing primary keys.

    Args:
        connection: An open PostgreSQL connection.  Tables will be
            created within this database.  If tables already exist,
            this function will not recreate them.
    """
    cursor = connection.cursor()

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
            listing_url TEXT,
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
            scheduled_date TIMESTAMP NOT NULL,
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

    # Table: registration requests
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

    # Table: blacklist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            phone TEXT PRIMARY KEY,
            reason TEXT,
            added_at TIMESTAMP
        );
        """
    )

    # Table: messages (simple message log used by other scripts)
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

    connection.commit()

def main() -> None:
    """
    Parse command‑line arguments and create the database schema.  Only
    PostgreSQL is supported; if the connection URL is missing or
    psycopg2 is unavailable the function will terminate with a
    descriptive error message.  Autocommit mode is enabled so that
    table creation happens immediately without needing an explicit
    commit.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Initialise the aparthotel management schema on PostgreSQL. "
            "SQLite is no longer supported.  Provide the PostgreSQL connection "
            "URL via --db-url or the DATABASE_URL environment variable."
        )
    )
    parser.add_argument(
        "--db-url",
        type=str,
        help=(
            "PostgreSQL database URL.  Overrides the DATABASE_URL environment variable when provided."
        ),
    )
    args = parser.parse_args()
    # Determine PostgreSQL URL from argument or environment
    db_url = args.db_url or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit(
            "A PostgreSQL connection URL must be provided via --db-url or the DATABASE_URL environment variable."
        )
    # Normalise legacy scheme used by some providers (postgres:// → postgresql://)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    # Ensure psycopg2 is installed
    if psycopg2 is None:
        raise SystemExit(
            "psycopg2 is required to initialise the schema but is not installed.  "
            "Install psycopg2 or psycopg2-binary and try again."
        )
    try:
        # Connect using SSL by default; PGSSLMODE can override.  Autocommit
        # ensures each statement commits automatically, similar to SQLite's default.
        connection = psycopg2.connect(db_url, sslmode=os.environ.get("PGSSLMODE", "require"))
        try:
            connection.autocommit = True
        except Exception:
            pass
        create_tables(connection)
        print(f"Database schema created successfully on PostgreSQL: {db_url}")
    except Exception as exc:
        raise SystemExit(f"Failed to connect or create schema on PostgreSQL: {exc}")
    finally:
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()