"""
Flask web application for managing an apart‑hotel with a Bootstrap‑styled
interface.

This version uses Jinja templates stored in the ``templates`` directory
and Bootstrap via CDN to provide a more polished look. It allows users
to list and add guests, rooms, and bookings. Additional functionality can
be added following this pattern.

To run the app:
    1. Install Flask if not already installed: ``pip3 install flask``
    2. Ensure the database ``aparthotel.db`` exists (run ``create_database.py`` if needed)
    3. Execute this script: ``python3 app.py``
    4. Visit http://127.0.0.1:5000/ in your browser.
"""

import os
import sqlite3

# Try to import psycopg2 for optional PostgreSQL support.  If the
# import fails (e.g. the package is not installed), the application
# will silently fall back to SQLite.  psycopg2 is used when the
# DATABASE_URL environment variable is provided.  psycopg2-binary
# should be added to your requirements file if you plan to run the
# application against PostgreSQL.
try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore
    psycopg2_extra = None  # type: ignore
import random
import smtplib
from email.mime.text import MIMEText
from flask import Flask, render_template, request, redirect, url_for, flash, session, abort, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from werkzeug.utils import secure_filename
from datetime import date, timedelta
from datetime import datetime  # For timestamp handling and filters
import re
import uuid  # For generating unique payment identifiers



# Configurations
DB_DEFAULT_FILE = "aparthotel.db"

app = Flask(__name__)
app.secret_key = "change_this_secret_key"  # Needed for flashing messages

# ----------------------------------------------------------------------------
# Load environment variables from a local configuration file if present
#
# To simplify configuration for non‑technical users, attempt to read one of
# several candidate files in the application directory and inject any
# ``KEY=VALUE`` definitions into ``os.environ``. This allows the
# application to pick up SMTP settings (MAIL_SERVER, MAIL_PORT, etc.) without
# requiring a manual ``export`` step or a specific filename. The following
# filenames are checked, in order, and the first existing one is used:
#   1. ``.env``
#   2. ``moksli_env.env``
#   3. ``moksli_env_fixed.env``
# Lines beginning with ``#`` are ignored. If a variable is already set in
# the environment, the value from the file does not override it. Only
# simple ``KEY=VALUE`` assignments are supported.

def _load_env_file() -> None:
    candidates = ['.env', 'moksli_env.env', 'moksli_env_fixed.env']
    base_dir = os.path.dirname(__file__)
    for name in candidates:
        path = os.path.join(base_dir, name)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '=' not in line:
                            continue
                        key, val = line.split('=', 1)
                        os.environ.setdefault(key.strip(), val.strip())
                return
            except Exception as e:
                print(f"Warning: Could not load environment file {name}: {e}")
                return
    return

_load_env_file()

# ----------------------------------------------------------------------------
# Email sending utility
#
# To enable email verification for user registration, configure the following
# environment variables in your hosting environment:
#   MAIL_SERVER   – hostname of the SMTP server (e.g., 'smtp.gmail.com')
#   MAIL_PORT     – port of the SMTP server (e.g., 465 for SSL or 587 for TLS)
#   MAIL_USERNAME – username/email for SMTP authentication
#   MAIL_PASSWORD – password or app password for SMTP authentication
#   MAIL_USE_TLS  – set to 'true' to use TLS (STARTTLS)
#   MAIL_USE_SSL  – set to 'true' to use SSL (default True)
#
# The send_verification_email function constructs a simple text email with
# a 6‑digit verification code and attempts to send it. It returns True on
# success and False on failure. If configuration is incomplete, it will
# log to stdout and return False.

def _send_verification_email_legacy(to_email: str, code: str) -> bool:
    """Send a verification code to the specified email address.

    Reads SMTP configuration from environment variables. Returns True if
    the email is sent successfully; otherwise returns False.
    """
    server = os.environ.get('MAIL_SERVER')
    port = int(os.environ.get('MAIL_PORT', '465'))
    username = os.environ.get('MAIL_USERNAME')
    password = os.environ.get('MAIL_PASSWORD')
    use_tls = os.environ.get('MAIL_USE_TLS', 'false').lower() == 'true'
    use_ssl = os.environ.get('MAIL_USE_SSL', 'true').lower() == 'true'
    if not server or not username or not password:
        # Missing configuration; do not attempt to send
        print("Email configuration is incomplete. Set MAIL_SERVER, MAIL_USERNAME, and MAIL_PASSWORD.")
        return False
    # Construct the email message
    subject = "Код подтверждения регистрации"
    body = f"Ваш код подтверждения: {code}"
    msg = MIMEText(body, _charset='utf-8')
    msg['Subject'] = subject
    msg['From'] = username
    msg['To'] = to_email
    try:
        if use_ssl:
            with smtplib.SMTP_SSL(server, port) as smtp:
                smtp.login(username, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(server, port) as smtp:
                if use_tls:
                    smtp.starttls()
                smtp.login(username, password)
                smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send verification email: {e}")
        return False

# --- Enhanced version below ---
# Redefine send_verification_email after the original definition to improve
# robustness. In Python, later definitions override earlier ones. This
# version adds fallback mechanisms for SMTP connection types and more
# detailed documentation.
def send_verification_email(to_email: str, code: str) -> bool:
    """
    Send a verification code to the specified email address.

    The SMTP connection settings are read from environment variables. If
    ``MAIL_SERVER``, ``MAIL_USERNAME`` or ``MAIL_PASSWORD`` are missing,
    the function will log a warning and return ``False`` without attempting
    to send anything. Connection security can be configured via
    ``MAIL_USE_TLS`` and ``MAIL_USE_SSL``. If the initial connection
    attempt fails, a fallback is attempted using the opposite security
    mode (e.g. try TLS when SSL fails) and finally a plain, unencrypted
    connection as a last resort. This helps accommodate SMTP servers that
    only support specific configurations. Returns ``True`` once the
    email has been successfully sent, or ``False`` if all attempts fail.
    """
    # Read SMTP configuration from the environment. The port defaults to
    # 465 (common for SMTP over SSL). The use_tls/use_ssl flags accept
    # values like "true"/"false" (case‑insensitive).
    server = os.environ.get('MAIL_SERVER')
    port = int(os.environ.get('MAIL_PORT', '465'))
    username = os.environ.get('MAIL_USERNAME')
    password = os.environ.get('MAIL_PASSWORD')
    use_tls = os.environ.get('MAIL_USE_TLS', 'false').lower() == 'true'
    use_ssl = os.environ.get('MAIL_USE_SSL', 'true').lower() == 'true'

    # Abort early if required settings are missing. Without a server
    # hostname or credentials we cannot send mail.
    if not server or not username or not password:
        print("Email configuration is incomplete. Set MAIL_SERVER, MAIL_USERNAME, and MAIL_PASSWORD.")
        return False

    # Construct the email contents. We explicitly set the encoding on
    # MIMEText to UTF‑8 so that Russian characters display correctly in
    # most mail clients.
    subject = "Код подтверждения регистрации"
    body = f"Ваш код подтверждения: {code}"
    msg = MIMEText(body, _charset='utf-8')
    msg['Subject'] = subject
    # Allow overriding the From header independently of the SMTP login.
    # If MAIL_FROM is set, it will be used as the visible sender address.
    mail_from = os.environ.get('MAIL_FROM', username)
    msg['From'] = mail_from
    msg['To'] = to_email

    def _attempt_send(use_ssl_flag: bool, use_tls_flag: bool) -> bool:
        """Internal helper to try sending with a specific security mode."""
        try:
            # Choose the appropriate SMTP constructor based on SSL usage
            if use_ssl_flag:
                smtp = smtplib.SMTP_SSL(server, port)
            else:
                smtp = smtplib.SMTP(server, port)
                # If TLS is requested on a non‑SSL connection, upgrade via STARTTLS
                if use_tls_flag:
                    smtp.starttls()
            # Log in and send the message
            smtp.login(username, password)
            smtp.send_message(msg)
            smtp.quit()
            return True
        except Exception as exc:
            # Log the exception to aid debugging. We deliberately do not
            # re‑raise here because the outer logic will try a fallback.
            print(f"Failed to send email (ssl={use_ssl_flag}, tls={use_tls_flag}): {exc}")
            return False

    # Try the configured mode first
    if _attempt_send(use_ssl, use_tls):
        return True

    # Fallback: if SSL was requested and failed, try TLS; if TLS was
    # requested and failed, try SSL. Otherwise, try both modes off. This
    # ensures there is at least one additional attempt before giving up.
    if use_ssl:
        # Try TLS connection
        if _attempt_send(False, True):
            return True
        # As a last resort, try a plain, unencrypted connection
        if _attempt_send(False, False):
            return True
    elif use_tls:
        # Try SSL connection on the same port
        if _attempt_send(True, False):
            return True
        # Then try plain connection
        if _attempt_send(False, False):
            return True
    else:
        # No security requested originally; try TLS then SSL
        if _attempt_send(False, True):
            return True
        if _attempt_send(True, False):
            return True

    # If all attempts failed, return False
    return False

# Jinja filter for formatting currency with thousands separator and two decimals
@app.template_filter('currency')
def format_currency(value):
    try:
        num = float(value)
        return f"{num:,.2f}".replace(',', ' ')
    except (TypeError, ValueError):
        return value


# Jinja filter to format a date string into Russian day-month format. The input
# should be a string in ISO format (YYYY-mm-dd). If parsing fails, the
# original value is returned. Example: '2025-09-26' becomes '26 сентября'.
@app.template_filter('russian_date')
def russian_date(date_str):
    """Convert a date string (YYYY-mm-dd) to 'D месяц' in Russian."""
    try:
        # Only consider the date part if a datetime string is passed
        date_part = date_str.split(' ')[0] if isinstance(date_str, str) else date_str
        dt = datetime.strptime(date_part, '%Y-%m-%d')
        months = [
            'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
        ]
        return f"{dt.day} {months[dt.month - 1]}"
    except Exception:
        return date_str

# Jinja filter to format phone numbers into a standardized display format.
# Given an input like '+77777777777' or '77777777777', returns a string
# formatted according to common conventions: e.g. '+7 (777) 777 77 77'.
# For US numbers (country code 1), formats as '+1 (XXX) XXX-XXXX'. For
# UK/Germany codes (44, 49) and Japan (81), uses 4‑3‑4 or 4‑3‑3 groupings.
# Other codes default to grouping the subscriber number in groups of three.
@app.template_filter('format_phone')
def format_phone(value: str) -> str:
    """Format an international phone number for display."""
    if not value:
        return ''
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', value)
    if not digits:
        return ''
    code = ''
    number = ''
    groups: list[int] = []
    # Determine country code and grouping rules
    # We handle specific codes first; others fallback to generic grouping
    if digits.startswith('7') and len(digits) > 1:
        # Kazakhstan/Russia: code is '7', subscriber number after
        code = digits[0]
        number = digits[1:]
        groups = [3, 3, 2, 2]
    elif digits.startswith('1') and len(digits) > 1:
        # USA/Canada: code '1'
        code = digits[0]
        number = digits[1:]
        groups = [3, 3, 4]
    elif digits.startswith('44') and len(digits) > 2:
        # UK
        code = digits[:2]
        number = digits[2:]
        groups = [4, 3, 4]
    elif digits.startswith('49') and len(digits) > 2:
        # Germany
        code = digits[:2]
        number = digits[2:]
        groups = [4, 3, 4]
    elif digits.startswith('81') and len(digits) > 2:
        # Japan
        code = digits[:2]
        number = digits[2:]
        groups = [4, 3, 3]
    else:
        # Generic fallback: treat first 1–3 digits as country code, rest as number.
        # Choose code length: if number is long enough, use 3‑digit code; else 2 or 1.
        if len(digits) > 3:
            code = digits[:3]
            number = digits[3:]
        elif len(digits) > 2:
            code = digits[:2]
            number = digits[2:]
        elif len(digits) > 1:
            code = digits[:1]
            number = digits[1:]
        else:
            # Too short to split; treat entire string as code
            code = digits
            number = ''
        # Group subscriber number into chunks of up to three digits
        rem = len(number)
        while rem > 0:
            chunk = 3 if rem >= 3 else rem
            groups.append(chunk)
            rem -= chunk
    # Build the formatted number
    parts = []
    idx = 0
    for g in groups:
        part = number[idx: idx + g]
        if not part:
            break
        parts.append(part)
        idx += g
    if not parts:
        return f"+{code}"
    formatted = f"+{code} ({parts[0]})"
    rest = parts[1:]
    if rest:
        # For US numbers, use a hyphen between last two groups
        if code == '1' and len(rest) == 2:
            formatted += f" {rest[0]}-{rest[1]}"
        else:
            formatted += ' ' + ' '.join(rest)
    return formatted

# Jinja filter to translate internal role codes to Russian labels for display.
@app.template_filter('role_ru')
def role_ru(role: str) -> str:
    """Translate a role identifier ('owner', 'manager', 'maid') into Russian for UI display."""
    mapping = {
        'owner': 'Владелец',
        'manager': 'Менеджер',
        'maid': 'Горничная',
    }
    # Fall back to the original value if no translation exists
    return mapping.get(role, role)


def get_db_connection():
    """
    Return a new database connection.  By default the application
    operates against a local SQLite database.  If the environment
    variable ``DATABASE_URL`` is defined and the psycopg2 package is
    available, a PostgreSQL connection will be created instead.  The
    returned object implements the same subset of the SQLite API used
    throughout this application so that the rest of the code can
    remain unchanged when switching between database backends.

    For SQLite, the existing behaviour (row_factory, PRAGMA foreign keys
    and automatic schema migrations) is preserved.  For PostgreSQL, a
    thin compatibility wrapper translates SQLite-style ``?`` parameter
    markers into ``%s``, implements ``lastrowid`` support via
    ``RETURNING id`` and emulates the ``PRAGMA table_info`` calls by
    querying PostgreSQL's information_schema.
    """
    db_url = os.environ.get("DATABASE_URL")

    # Require PostgreSQL: abort if DATABASE_URL is missing or psycopg2 is not installed.
    # SQLite support has been removed to prevent cross‑database conflicts.
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL environment variable must be set; SQLite support has been removed."
        )
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 package is required for PostgreSQL connections but is not installed."
        )
    # Normalize 'postgres://' scheme to 'postgresql://' for psycopg2.  Some
    # providers (including Render) still return a URL beginning with
    # 'postgres://'.  psycopg2 accepts 'postgresql://' and Render's
    # documentation recommends this scheme.  Without normalization, the
    # connection may fail on newer psycopg2 versions.
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    # Use PostgreSQL if DATABASE_URL is set and psycopg2 is available
    if db_url and psycopg2 is not None:
        try:
            # When connecting over the public internet, Render requires SSL.
            # If the URL does not include an sslmode parameter, supply one
            # explicitly using PGSSLMODE environment variable with a default
            # of 'require'.  Internal connections will silently ignore this
            # parameter if SSL isn't needed.
            pg_conn = psycopg2.connect(db_url, sslmode=os.environ.get("PGSSLMODE", "require"))
            # Enable autocommit so each statement runs in its own transaction.  Without
            # autocommit, a failed SQL statement would leave the connection in an
            # aborted transaction state (InFailedSqlTransaction) until an explicit
            # rollback, causing subsequent statements to fail.  Autocommit mirrors
            # SQLite's autocommit behaviour and improves resiliency when running
            # arbitrary queries from view functions.
            try:
                pg_conn.autocommit = True
            except Exception:
                pass
        except Exception as exc:
            # Remove SQLite fallback: raise a runtime error on PostgreSQL connection failure
            raise RuntimeError(f"Failed to connect to PostgreSQL: {exc}")
        if pg_conn is not None:
            # Automatically initialize the PostgreSQL schema if the core
            # ``users`` table does not exist.  This mirrors the behaviour of
            # ``create_database.py`` and allows first‑run deployments on
            # platforms like Render where there is no opportunity to run a
            # separate migration command.  The helper will create all core
            # tables if they are absent.  For subsequent connections the
            # initialization is skipped.
            def _initialize_pg_schema_if_needed(conn):
                try:
                    cur0 = conn.cursor()
                    cur0.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users');"
                    )
                    exists = cur0.fetchone()
                    cur0.close()
                    if exists and (exists[0] if isinstance(exists, tuple) else list(exists.values())[0]):
                        return
                    # Create tables.  These statements mirror create_database.py but omit SQLite‑specific PRAGMA calls.
                    statements = [
                        """
                        CREATE TABLE IF NOT EXISTS guests (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            phone TEXT,
                            -- Additional phone number for the guest (optional).  Allows storing a
                            -- secondary contact number without adding a separate table.
                            extra_phone TEXT,
                            email TEXT,
                            notes TEXT,
                            -- Date of birth for the guest.  Stored as DATE.  Optional.
                            birth_date DATE,
                            -- Filename of the uploaded guest photo stored in static/uploads.  Optional.
                            photo TEXT
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS rooms (
                            id SERIAL PRIMARY KEY,
                            room_number TEXT NOT NULL UNIQUE,
                            capacity INTEGER,
                            notes TEXT,
                            listing_url TEXT,
                            residential_complex TEXT
                        );
                        """,
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
                        """,
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
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS expenses (
                            id SERIAL PRIMARY KEY,
                            category TEXT NOT NULL,
                            amount REAL NOT NULL,
                            date DATE NOT NULL,
                            description TEXT
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS cleaning_tasks (
                            id SERIAL PRIMARY KEY,
                            room_id INTEGER NOT NULL,
                            scheduled_date TIMESTAMP NOT NULL,
                            status TEXT NOT NULL DEFAULT 'scheduled',
                            notes TEXT,
                            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
                        );
                        """,
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
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS registration_requests (
                            id SERIAL PRIMARY KEY,
                            username TEXT NOT NULL UNIQUE,
                            password_hash TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'pending',
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS blacklist (
                            phone TEXT PRIMARY KEY,
                            reason TEXT,
                            added_at TEXT
                        );
                        """,
                        # Chat rooms and related tables
                        """
                        CREATE TABLE IF NOT EXISTS chat_rooms (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            created_at TEXT DEFAULT CURRENT_TIMESTAMP
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS chat_room_members (
                            room_id INTEGER NOT NULL,
                            user_id INTEGER NOT NULL,
                            PRIMARY KEY (room_id, user_id),
                            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
                            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS messages (
                            id SERIAL PRIMARY KEY,
                            room_id INTEGER NOT NULL DEFAULT 1,
                            user_id INTEGER NOT NULL,
                            message TEXT NOT NULL,
                            timestamp TEXT NOT NULL,
                            file_name TEXT,
                            file_type TEXT,
                            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
                            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS user_last_seen (
                            user_id INTEGER PRIMARY KEY,
                            last_seen_message_id INTEGER,
                            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS user_room_last_seen (
                            user_id INTEGER NOT NULL,
                            room_id INTEGER NOT NULL,
                            last_seen_message_id INTEGER NOT NULL,
                            PRIMARY KEY (user_id, room_id),
                            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE
                        );
                        """,
                        # Guest comments and room status
                        """
                        CREATE TABLE IF NOT EXISTS guest_comments (
                            id SERIAL PRIMARY KEY,
                            guest_id INTEGER NOT NULL,
                            comment TEXT,
                            created_at TEXT,
                            FOREIGN KEY (guest_id) REFERENCES guests(id) ON DELETE CASCADE
                        );
                        """,
                        """
                        CREATE TABLE IF NOT EXISTS room_statuses (
                            room_id INTEGER NOT NULL,
                            date TEXT NOT NULL,
                            status TEXT NOT NULL,
                            PRIMARY KEY (room_id, date),
                            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
                        );
                        """,
                        # Subscriptions table with payment details
                        """
                        CREATE TABLE IF NOT EXISTS subscriptions (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER NOT NULL,
                            plan_name TEXT NOT NULL,
                            status TEXT NOT NULL,
                            price REAL,
                            created_at TEXT,
                            next_billing_date TEXT,
                            payment_id TEXT,
                            payment_url TEXT,
                            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                        );
                        """,
                    ]
                    cur = conn.cursor()
                    for stmt in statements:
                        cur.execute(stmt)
                    # Insert a default global chat room if none exists (id=1)
                    try:
                        cur.execute("SELECT id FROM chat_rooms WHERE id = 1")
                        row = cur.fetchone()
                        if not row:
                            cur.execute("INSERT INTO chat_rooms (id, name) VALUES (%s, %s)", (1, 'Общий чат'))
                    except Exception:
                        # If the table does not yet exist, ignore; it will be created
                        pass
                    conn.commit()
                    cur.close()
                except Exception as e:
                    # Log and continue; schema may be partially created
                    print(f"Error initializing PostgreSQL schema: {e}")
            # Run initialization once per connection
            _initialize_pg_schema_if_needed(pg_conn)

            # Define a wrapper class for PostgreSQL to emulate SQLite API
            class SQLiteCompatCursor:
                """
                A cursor wrapper that exposes a SQLite-like interface on
                top of a psycopg2 cursor.  It transparently converts
                parameter placeholders and adds a ``RETURNING id``
                clause to INSERT statements so that ``lastrowid`` can
                be provided.  ``PRAGMA table_info`` queries are
                emulated using information_schema when running on
                PostgreSQL.  If a PRAGMA is executed that has no
                meaning for PostgreSQL, the call is ignored and an
                empty result is returned.
                """

                def __init__(self, underlying_cursor):
                    self._cur = underlying_cursor
                    self.lastrowid = None

                def _translate_query(self, query: str) -> str:
                    """
                    Translate SQLite-specific query syntax into PostgreSQL-compatible SQL.

                    This helper normalizes PRAGMA statements, rewrites ``INSERT OR IGNORE``
                    into ``ON CONFLICT DO NOTHING`` and determines whether to append a
                    ``RETURNING id`` clause. The ``RETURNING id`` clause is only added
                    for tables that have an integer primary key column named ``id``. Tables
                    with composite or non-integer primary keys (e.g., ``user_room_last_seen``)
                    should not receive ``RETURNING id`` because such a column does not
                    exist, and doing so causes "UndefinedColumn" errors on PostgreSQL.
                    """
                    import re
                    # Normalize whitespace for easier matching
                    q = query.strip()
                    # Handle PRAGMA table_info(table_name)
                    if q.lower().startswith("pragma table_info"):
                        return q  # will be intercepted in execute
                    # Convert "INSERT OR IGNORE" into ON CONFLICT DO NOTHING
                    lower_q = q.lower()
                    if lower_q.startswith("insert or ignore into"):
                        # replace the first occurrence of "insert or ignore" with "insert"
                        # and append ON CONFLICT DO NOTHING just before any RETURNING clause
                        # Split by spaces to preserve table name and columns
                        parts = query.split(None, 3)  # e.g. ['INSERT', 'OR', 'IGNORE', 'INTO ...']
                        new_query = "INSERT " + parts[-1]
                        conflict_clause = " ON CONFLICT DO NOTHING"
                        # Insert conflict clause before any RETURNING keyword
                        if "RETURNING" in new_query or "returning" in new_query:
                            idx = new_query.lower().rfind("returning")
                            return new_query[:idx] + conflict_clause + " " + new_query[idx:]
                        else:
                            return new_query + conflict_clause
                    # If this is an INSERT without an explicit RETURNING clause, decide if we
                    # should add ``RETURNING id``. Only append for tables with an integer
                    # primary key named "id". Use a whitelist of such tables based on the
                    # schema defined in this application.
                    low = q.lower()
                    if low.startswith("insert") and "returning" not in low:
                        # Extract the table name following "insert into"
                        m = re.match(r"insert\s+(?:or\s+ignore\s+)?into\s+([\w\"\.]+)", q, re.IGNORECASE)
                        if m:
                            table_name = m.group(1).strip('"')
                            # Only append RETURNING id for tables that define an integer primary key "id".
                            id_tables = {
                                'guests', 'rooms', 'bookings', 'payments', 'expenses',
                                'cleaning_tasks', 'users', 'registration_requests',
                                'messages', 'chat_rooms', 'guest_comments', 'subscriptions'
                            }
                            # The table name might include schema qualification (e.g., public.chat_rooms)
                            simple_name = table_name.split('.')[-1]
                            if simple_name in id_tables:
                                return query + " RETURNING id"
                        # Otherwise return the query unchanged
                        return query
                    return query

                def _convert_placeholders(self, query: str) -> str:
                    """Replace SQLite-style '?' placeholders with psycopg2 '%s' placeholders."""
                    result_chars = []
                    for ch in query:
                        if ch == '?':
                            result_chars.append('%s')
                        else:
                            result_chars.append(ch)
                    return ''.join(result_chars)

                def execute(self, query: str, params=()):
                    # Handle PRAGMA table_info separately
                    q = query.strip()
                    if q.lower().startswith("pragma table_info"):
                        # Extract table name from PRAGMA table_info(table)
                        import re
                        m = re.search(r"pragma\s+table_info\s*\(([^)]+)\)", q, re.IGNORECASE)
                        table_name = m.group(1).strip(' \"\'') if m else None
                        # Query information_schema for column details
                        col_rows = []
                        if table_name:
                            try:
                                info_q = """
                                    SELECT ordinal_position - 1 AS cid,
                                           column_name AS name,
                                           data_type AS type,
                                           is_nullable,
                                           column_default,
                                           is_identity
                                    FROM information_schema.columns
                                    WHERE table_name = %s
                                    ORDER BY ordinal_position
                                """
                                self._cur.execute(info_q, (table_name,))
                                for row in self._cur.fetchall():
                                    # emulate sqlite3.Row: (cid, name, type, notnull, dflt_value, pk)
                                    cid = row[0]
                                    name = row[1]
                                    typ = row[2]
                                    notnull = 0 if row[3] == 'YES' else 1
                                    dflt = row[4]
                                    pk = 1 if row[5] == 'YES' else 0
                                    col_rows.append((cid, name, typ, notnull, dflt, pk))
                            except Exception:
                                # If schema query fails, return empty list
                                col_rows = []
                        # Create a fake cursor that returns these rows
                        self._results = col_rows
                        self.lastrowid = None
                        return self
                    # Otherwise translate query
                    translated = self._translate_query(query)
                    # Prepare to convert SQLite-style placeholders to psycopg2 format.
                    converted_query = translated
                    bound_params = params
                    # When params is a mapping, expand named parameters (e.g. :start_date)
                    if params and isinstance(params, dict):
                        import re
                        pattern = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")
                        names = pattern.findall(converted_query)
                        if names:
                            converted_query = pattern.sub("%s", converted_query)
                            # order values according to appearance of placeholders
                            bound_params = [params[name] for name in names]
                    # Convert any remaining '?' placeholders to '%s'
                    converted_query = self._convert_placeholders(converted_query)
                    # Execute the translated statement with bound parameters
                    self._cur.execute(converted_query, bound_params)
                    # Capture lastrowid if a RETURNING clause is present
                    if "returning" in converted_query.lower():
                        try:
                            returned = self._cur.fetchone()
                            if returned is not None:
                                if isinstance(returned, dict):
                                    self.lastrowid = returned.get('id')
                                else:
                                    self.lastrowid = returned[0]
                        except Exception:
                            self.lastrowid = None
                    else:
                        self.lastrowid = None
                    return self

                def executemany(self, query: str, param_list):
                    translated = self._translate_query(query)
                    converted_query = translated
                    params_to_use = param_list
                    # Support named parameters: if first param set is a mapping
                    if param_list and isinstance(param_list[0], dict):
                        import re
                        pattern = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")
                        names = pattern.findall(converted_query)
                        if names:
                            converted_query = pattern.sub("%s", converted_query)
                            new_params = []
                            for p in param_list:
                                new_params.append([p[name] for name in names])
                            params_to_use = new_params
                    # Convert any '?' to '%s'
                    converted_query = self._convert_placeholders(converted_query)
                    # Execute against the list of parameters
                    self._cur.executemany(converted_query, params_to_use)
                    self.lastrowid = None
                    return self

                def fetchone(self):
                    # If results were set manually (PRAGMA), return from that
                    if hasattr(self, '_results'):
                        if not self._results:
                            return None
                        return self._results.pop(0)
                    row = self._cur.fetchone()
                    return row

                def fetchall(self):
                    if hasattr(self, '_results'):
                        res = list(self._results)
                        self._results = []
                        return res
                    return self._cur.fetchall()

                def __iter__(self):
                    return iter(self.fetchall())

                @property
                def description(self):
                    return self._cur.description

                @property
                def rowcount(self):
                    return self._cur.rowcount

                def close(self):
                    try:
                        self._cur.close()
                    except Exception:
                        pass

            class SQLiteCompatConnection:
                """
                A connection wrapper that provides a subset of the SQLite3
                connection API on top of a PostgreSQL connection.  It
                exposes ``execute`` and ``cursor`` methods so that the
                rest of the application can remain unchanged.  It also
                implements context manager methods for ``with`` blocks.
                """

                def __init__(self, pg_connection):
                    self._conn = pg_connection
                    # We don't use row_factory when talking to PostgreSQL because
                    # psycopg2's RealDictCursor returns dictionaries directly.
                    self.row_factory = None

                def cursor(self):
                    # Use RealDictCursor so fetchall returns list of dicts
                    cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    return SQLiteCompatCursor(cur)

                def execute(self, query: str, params=()):
                    cur = self.cursor()
                    return cur.execute(query, params)

                def executemany(self, query: str, param_list):
                    cur = self.cursor()
                    return cur.executemany(query, param_list)

                def commit(self):
                    self._conn.commit()

                def rollback(self):
                    self._conn.rollback()

                def close(self):
                    try:
                        self._conn.close()
                    except Exception:
                        pass

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    """
                    Context manager exit handler that gracefully commits or
                    rolls back the transaction and closes the connection.

                    When an exception has occurred inside the ``with`` block
                    (``exc_type`` is not ``None``), attempt to roll back the
                    transaction.  Otherwise attempt to commit.  All operations
                    are wrapped in ``try/except`` blocks to suppress
                    ``psycopg2.InterfaceError`` exceptions that may arise if
                    the underlying PostgreSQL connection has already been
                    closed.  Finally, close the connection in a safe manner
                    regardless of whether commit/rollback succeeded.
                    """
                    try:
                        if exc_type:
                            # Roll back the transaction if an error occurred.  Ignore
                            # errors if the connection is already closed or
                            # autocommit is enabled.
                            try:
                                self.rollback()
                            except Exception:
                                pass
                        else:
                            # Commit the transaction on normal exit.  Ignore
                            # errors if the connection is already closed.
                            try:
                                self.commit()
                            except Exception:
                                pass
                    finally:
                        # Always attempt to close the connection.  Ignore any
                        # exceptions if the connection has already been closed.
                        try:
                            self.close()
                        except Exception:
                            pass

            # Wrap the raw psycopg2 connection in a SQLiteCompatConnection that
            # provides a subset of the sqlite3 API (execute/cursor/commit/rollback).
            wrapper_conn = SQLiteCompatConnection(pg_conn)
            # Ensure the bookings table includes a created_by column.  When
            # operating on PostgreSQL, our PRAGMA implementation uses
            # information_schema to introspect the schema.  If the column is
            # missing it will be added.  This helper is idempotent so it is
            # safe to call on every connection.  Suppress any error so the
            # application continues to run even if the schema update fails.
            try:
                ensure_booking_creator_column(wrapper_conn)
            except Exception:
                pass
            # Ensure that optional columns exist on the guests table.  Older
            # deployments may lack the extra_phone, birth_date or photo columns.
            # These helpers attempt to add the columns if they are missing.
            try:
                ensure_extra_phone_column(wrapper_conn)
            except Exception:
                pass
            try:
                ensure_birth_date_column(wrapper_conn)
            except Exception:
                pass
            try:
                ensure_guest_photo_column(wrapper_conn)
            except Exception:
                pass
            return wrapper_conn
    # No PostgreSQL connection could be established, and fallback to SQLite is disabled.
    # Raise an explicit error so that missing configuration is detected early.
    raise RuntimeError(
        "DATABASE_URL is not configured or the PostgreSQL driver is unavailable; unable to establish a database connection."
    )


# Simple login_required decorator. If a route requires the user to be
# authenticated, decorate the view function with @login_required.  It
# checks for a 'user_id' key in the session and redirects to the login
# page if not present.
def login_required(f):
    """Decorator to enforce that a user is logged in before accessing a view."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Skip authentication for static files which are served by Flask under the
        # 'static' endpoint. Without this check, loading CSS/JS would trigger
        # redirects to the login page.
        if request.endpoint == 'static':
            return f(*args, **kwargs)
        if not session.get('user_id'):
            # User is not authenticated, redirect to the login page
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


# Decorator to restrict access based on user roles. Accepts one or more allowed roles.
def roles_required(*allowed_roles):
    """
    A decorator to ensure that the current user has one of the specified roles.
    Usage: @roles_required('owner', 'manager') will allow access to owners and managers.
    """
    def decorator(view_func):
        @wraps(view_func)
        def wrapped_view(*args, **kwargs):
            user_role = session.get('user_role')
            if not user_role or user_role not in allowed_roles:
                # User is either not logged in or not in the allowed roles
                return abort(403)
            return view_func(*args, **kwargs)
        return wrapped_view
    return decorator


# Before every request, ensure the user is authenticated except for the
# login and registration pages. This provides a simple blanket check
# across the application so that you don't have to decorate each view
# individually. If the request endpoint is None (which can happen for
# static files) we allow it through.
@app.before_request
def require_login():
    """
    Enforce login for all routes except a small set of public endpoints.

    Prior to each request, this function checks whether the requested endpoint
    should be accessible without authentication. If the endpoint is not
    explicitly allowed and the user is not logged in, they are redirected
    to the login page. This provides a simple blanket access control for
    most of the application without requiring decorators on every view.

    Note: It is important to include any unauthenticated routes in the
    ``allowed_endpoints`` set. Failure to do so can result in the client
    receiving an unexpected HTML login page instead of the expected JSON
    response (for example, when requesting an API endpoint during
    registration). See issue where `/send_verification_code` returned
    a login redirect causing “Непредвиденный ответ от сервера”.
    """
    # Allow these endpoints for unauthenticated users.  The endpoint name
    # corresponds to the view function name (without blueprint prefix).
    allowed_endpoints = {
        'login',
        'register',
        'static',
        # Permit sending verification codes during registration without a session
        'send_verification_code',
    }
    endpoint = request.endpoint
    # Some endpoints may include blueprint names (e.g. "static"). Split on the
    # dot and inspect only the base name. If there is no endpoint (as with
    # serving a static file) then allow it.
    if endpoint:
        if endpoint.split('.')[0] in allowed_endpoints:
            return
    # For all other endpoints, if the user is not logged in, redirect to login.
    if not session.get('user_id'):
        return redirect(url_for('login'))


# Context processor to make current user information available in all
# templates. When a user is logged in, this returns their username,
# name, role and photo filename (if any) as template variables. These
# variables are used in the sidebar to display account info.
@app.context_processor
def inject_current_user():
    if session.get('user_id'):
        conn = get_db_connection()
        # Ensure the photo column exists before querying
        ensure_photo_column(conn)
        # Use a positional placeholder rather than the erroneous "%?".  The
        # SQLite API accepts "?" which our PostgreSQL compatibility layer
        # automatically converts into "%s" for psycopg2.  Without this
        # change psycopg2 would interpret "%?" as a literal percent sign
        # followed by an unknown placeholder and raise a syntax error.
        user = conn.execute(
            'SELECT id, username, role, name, photo FROM users WHERE id = ?',
            (session['user_id'],)
        ).fetchone()
        conn.close()
        if user:
            return dict(
                current_username=user['username'],
                current_user_name=user['name'],
                current_user_role=user['role'],
                user_photo=user['photo']
            )
    # If not logged in or user not found, return empty context
    return {}


# Context processor to inject counts of unread chat messages and pending registration
# requests into all templates. These values are used to display notification
# badges in the sidebar. For logged-in users, unread_message_count is the
# number of chat messages sent by other users that the current user has not
# yet viewed (based on the user_last_seen table). pending_request_count is
# the number of registration requests awaiting approval, which is relevant
# for owner accounts. If the user is not logged in, both counts are zero.
@app.context_processor
def inject_notification_counts():
    """
    Inject counts of unread chat rooms and pending registration requests into all templates.

    The unread indicator in the main sidebar should only appear when there is at least one
    chat room with unread messages. Instead of counting the total number of unread
    messages across all rooms, we compute the number of rooms for which there exists
    at least one message authored by someone else that the current user has not yet
    seen. If a room has never been opened by the user, its last seen ID defaults to
    the current maximum message ID for that room so that no unread indicator is shown
    until a new message arrives.
    """
    unread_message_count = 0
    pending_request_count = 0
    if session.get('user_id'):
        user_id = session['user_id']
        conn = get_db_connection()
        # Validate that the session user_id actually exists in the users table.  If the
        # user record has been deleted or the session is stale, skip unread/notification
        # calculation to avoid foreign key violations when inserting into
        # user_room_last_seen.  Without this check, attempting to insert a row for a
        # nonexistent user triggers a ForeignKeyViolation in PostgreSQL.
        try:
            user_exists_row = conn.execute(
                "SELECT 1 FROM users WHERE id = ?",
                (user_id,)
            ).fetchone()
        except Exception:
            user_exists_row = None
        if not user_exists_row:
            # User does not exist. Return zero counts and close connection.
            conn.close()
            return dict(unread_message_count=0, pending_request_count=pending_request_count)
        # Ensure necessary tables exist for chat and registration logic
        ensure_messages_table(conn)
        ensure_user_room_last_seen_table(conn)
        ensure_chat_rooms_table(conn)
        ensure_chat_room_members_table(conn)
        ensure_registration_requests_table(conn)
        # Fetch pending registration requests count for owners
        # Fetch pending registration requests count for owners.
        # When using psycopg2 with RealDictCursor the returned row is a mapping,
        # whereas sqlite3 returns a tuple.  To support both, extract the first
        # value regardless of type.
        row = conn.execute(
            "SELECT COUNT(*) FROM registration_requests WHERE status = 'pending'"
        ).fetchone()
        if row is None:
            pending_request_count = 0
        elif isinstance(row, dict):
            # RealDictCursor returns {'count': ...} or {'count(*)': ...}
            # Use the first value in the mapping.
            pending_request_count = list(row.values())[0]
        else:
            # Assume row is a sequence (tuple or list)
            pending_request_count = row[0]
        # Determine the rooms in which the user participates. Include the global room (id=1).
        rooms = conn.execute(
            """
            SELECT r.id
            FROM chat_rooms AS r
            JOIN chat_room_members AS m ON r.id = m.room_id
            WHERE m.user_id = ?
              AND (r.id = 1 OR EXISTS (SELECT 1 FROM messages WHERE room_id = r.id))
            ORDER BY r.id ASC
            """,
            (user_id,)
        ).fetchall()
        # Ensure the global chat (id=1) appears in the list even if not explicitly a member
        if not any(row['id'] == 1 for row in rooms):
            global_row = conn.execute('SELECT id FROM chat_rooms WHERE id = 1').fetchone()
            if global_row:
                rooms = [global_row] + rooms
        # Iterate over each room and determine if it has unread messages
        for r in rooms:
            room_id = r['id'] if isinstance(r, dict) else r[0]
            # Determine the last seen message ID for this user in this room
            threshold = 0
            row_seen = conn.execute(
                'SELECT last_seen_message_id FROM user_room_last_seen WHERE user_id = ? AND room_id = ?',
                (user_id, room_id)
            ).fetchone()
            if row_seen and row_seen['last_seen_message_id'] is not None:
                try:
                    threshold = int(row_seen['last_seen_message_id'])
                except Exception:
                    threshold = 0
            else:
                # If no record exists for this room/user, default the threshold to the
                # current maximum message ID so the room appears as read unless new
                # messages arrive. If the room has no messages, threshold stays at 0.
                row_max = conn.execute(
                    'SELECT MAX(id) AS max_id FROM messages WHERE room_id = ?',
                    (room_id,)
                ).fetchone()
                max_id = row_max['max_id'] if row_max and row_max['max_id'] is not None else 0
                threshold = int(max_id) if max_id else 0
                # Persist this initial last seen value for future queries. This uses
                # ON CONFLICT so that if a record already exists it will be updated.
                conn.execute(
                    'INSERT INTO user_room_last_seen (user_id, room_id, last_seen_message_id) VALUES (?, ?, ?) '
                    'ON CONFLICT(user_id, room_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id',
                    (user_id, room_id, threshold)
                )
                conn.commit()
            # Check if there are any messages beyond the threshold authored by someone else
            row_unread = conn.execute(
                'SELECT 1 FROM messages WHERE room_id = ? AND id > ? AND user_id != ? LIMIT 1',
                (room_id, threshold, user_id)
            ).fetchone()
            if row_unread:
                unread_message_count += 1
        conn.close()
    return dict(unread_message_count=unread_message_count,
                pending_request_count=pending_request_count)


# Route: register a new user (owner). This view presents a form to
# create a new account and handles form submission by validating
# input, checking for duplicates, hashing the password and storing the
# user in the database. After successful registration the user is
# prompted to log in.
@app.route('/register', methods=['GET', 'POST'])
def register():
    conn = get_db_connection()
    # Ensure the registration_requests table exists
    ensure_registration_requests_table(conn)
    if request.method == 'POST':
        # Use email as the login identifier
        email = (request.form.get('email') or '').strip()
        password = (request.form.get('password') or '').strip()
        verification_code_input = (request.form.get('verification_code') or '').strip()
        # Determine account type; default to employee if missing
        account_type = (request.form.get('account_type') or 'employee').strip().lower()
        # Basic validation: ensure email and password are provided
        if not email or not password:
            flash('Введите e‑mail и пароль.')
            conn.close()
            return redirect(url_for('register'))
        # Validate simple email format (must contain '@' and '.')
        if '@' not in email or '.' not in email:
            flash('Введите корректный e‑mail.')
            conn.close()
            return redirect(url_for('register'))
        # Ensure verification code is present
        if not verification_code_input:
            flash('Введите код подтверждения, который был отправлен на ваш e‑mail.')
            conn.close()
            return redirect(url_for('register'))
        # Check that a code was sent and matches the provided email
        code_expected = session.get('verification_code')
        email_expected = session.get('verification_email')
        if not code_expected or not email_expected:
            flash('Сначала запросите код подтверждения.')
            conn.close()
            return redirect(url_for('register'))
        if email_expected != email:
            flash('E‑mail не совпадает с тем, для которого был запрошен код.')
            conn.close()
            return redirect(url_for('register'))
        if verification_code_input != code_expected:
            flash('Неверный код подтверждения.')
            conn.close()
            return redirect(url_for('register'))
        # Check if email exists in users or pending requests
        # Use the correct SQLite placeholder.  The psycopg2 wrapper will
        # translate '?' into '%s' on PostgreSQL.  The previous version
        # mistakenly used "%?" which caused a syntax error in Postgres.
        existing_user = conn.execute('SELECT id FROM users WHERE username = ?', (email,)).fetchone()
        existing_req = conn.execute('SELECT id FROM registration_requests WHERE username = ?', (email,)).fetchone()
        if existing_user or existing_req:
            conn.close()
            flash('Пользователь с таким e‑mail уже существует или ожидает подтверждения.')
            return redirect(url_for('register'))
        # Determine if an owner already exists in the system.  When using
        # psycopg2 with RealDictCursor the row is a dict, but with sqlite3
        # it is a tuple.  Extract the first value safely.
        row_owner = conn.execute(
            "SELECT COUNT(*) FROM users WHERE role = 'owner'"
        ).fetchone()
        if row_owner is None:
            owner_exists = False
        elif isinstance(row_owner, dict):
            owner_exists = list(row_owner.values())[0] > 0
        else:
            owner_exists = row_owner[0] > 0
        # Decide the final role based on account type and whether an owner exists
        final_role = None
        if account_type == 'owner':
            if owner_exists:
                # Disallow creating another owner if one already exists
                flash('Владелец уже существует. Для регистрации сотрудников обратитесь к текущему владельцу.')
                conn.close()
                return redirect(url_for('register'))
            else:
                final_role = 'owner'
        else:
            # account_type is employee; if no owner exists, make this user the owner
            if not owner_exists:
                final_role = 'owner'
            else:
                final_role = None  # employee needs approval
        # All validations passed; remove verification code from session
        session.pop('verification_code', None)
        session.pop('verification_email', None)
        # Hash the password
        password_hash = generate_password_hash(password)
        if final_role == 'owner':
            # Directly create an owner user and add them to the global chat
            with conn:
                cur = conn.cursor()
                cur.execute(
                    'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                    (email, password_hash, 'owner')
                )
                new_user_id = cur.lastrowid
                # Ensure chat rooms and membership tables exist
                ensure_chat_rooms_table(conn)
                ensure_chat_room_members_table(conn)
                # Insert the new owner into the global chat (room 1)
                try:
                    cur.execute(
                        'INSERT OR IGNORE INTO chat_room_members (room_id, user_id) VALUES (1, ?)',
                        (new_user_id,)
                    )
                except Exception:
                    pass
            conn.close()
            flash('Регистрация завершена. Вы зарегистрированы как владелец.')
            return redirect(url_for('login'))
        else:
            # Create a pending registration request for employees
            with conn:
                conn.execute(
                    'INSERT INTO registration_requests (username, password_hash) VALUES (?, ?)',
                    (email, password_hash)
                )
            conn.close()
            flash('Ваш запрос на регистрацию отправлен. Дождитесь одобрения владельца.')
            return redirect(url_for('login'))
    # GET: render registration form
    conn.close()
    return render_template('register.html')


# Route: send verification code via email. This endpoint accepts a POST
# request with an 'email' parameter (JSON or form data), generates a
# 6‑digit verification code, stores it in the session, sends it to the
# specified email using the configured SMTP settings, and returns a
# JSON response indicating success or failure. It does not require
# authentication because it is part of the public registration flow.
@app.route('/send_verification_code', methods=['POST'])
def send_verification_code():
    """Handle requests to send a verification code to the provided email.

    This function attempts to parse the email address from JSON or form data,
    generates a six‑digit code, stores it in the session, sends the code via
    the configured SMTP server, and returns a JSON response. Any unexpected
    exceptions are caught to ensure a JSON error response is always returned.
    """
    try:
        # Try to read email from JSON body or form data
        email = None
        if request.is_json:
            try:
                email = request.get_json().get('email')
            except Exception:
                email = None
        if not email:
            email = (request.form.get('email') or '').strip()
        if not email:
            return jsonify({'success': False, 'message': 'E‑mail обязателен'}), 400
        # Generate a 6‑digit random code (zero‑padded)
        code = f"{random.randint(0, 999999):06d}"
        # Store the code and the email in session for later verification
        session['verification_code'] = code
        session['verification_email'] = email
        # Attempt to send the code via email
        sent = send_verification_email(email, code)
        if sent:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'message': 'Не удалось отправить код. Проверьте конфигурацию почты.'}), 500
    except Exception as e:
        # Log the exception for debugging purposes
        print(f"Error in send_verification_code: {e}")
        return jsonify({'success': False, 'message': 'Ошибка сервера при отправке кода.'}), 500


# Route: login. Presents a login form and handles authentication. On
# successful login the user's id and role are stored in the session and
# they are redirected to the homepage. On failure an error message is
# flashed.
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Use email as the login identifier
        email = (request.form.get('email') or '').strip()
        password = (request.form.get('password') or '').strip()
        if not email or not password:
            flash('Введите e‑mail и пароль.')
            return redirect(url_for('login'))
        conn = get_db_connection()
        # Ensure registration_requests table exists before querying it
        ensure_registration_requests_table(conn)
        # Correct placeholder usage for user lookup.  See comments in
        # register() above for details.
        user = conn.execute('SELECT * FROM users WHERE username = ?', (email,)).fetchone()
        if user and check_password_hash(user['password_hash'], password):
            # Store user id and role in the session
            session['user_id'] = user['id']
            session['user_role'] = user['role']
            conn.close()
            flash('Вход выполнен успешно!')
            return redirect(url_for('index'))
        else:
            # If no user or password mismatch, check pending registration requests
            pending = conn.execute('SELECT status FROM registration_requests WHERE username = ?', (email,)).fetchone()
            conn.close()
            if pending:
                status = pending['status']
                if status == 'pending':
                    flash('Ваш аккаунт ожидает одобрения администратора.')
                elif status == 'rejected':
                    flash('Ваша заявка на регистрацию была отклонена.')
                elif status == 'approved':
                    # The registration was approved but the credentials did not match a user.
                    # Suggest checking email and password instead of saying activation pending.
                    flash('Регистрация одобрена. Проверьте правильность e‑mail и пароля.')
                else:
                    flash('Неверные e‑mail или пароль.')
            else:
                flash('Неверные e‑mail или пароль.')
            return redirect(url_for('login'))
    # GET: render login form
    return render_template('login.html')


# Route: logout. Clears the user's session and redirects to login.
@app.route('/logout')
def logout():
    session.clear()
    flash('Вы вышли из системы.')
    return redirect(url_for('login'))


# Route to view and manage registration requests. Only accessible by owner.
@app.route('/registration_requests', methods=['GET', 'POST'])
@login_required
def registration_requests():
    # Only owners can manage registration requests
    if session.get('user_role') != 'owner':
        # Forbidden for non‑owners
        return abort(403)
    conn = get_db_connection()
    # Ensure the registration_requests table exists
    ensure_registration_requests_table(conn)
    if request.method == 'POST':
        # Read request id and action from form
        req_id = request.form.get('request_id')
        action = request.form.get('action')
        if not req_id:
            conn.close()
            return redirect(url_for('registration_requests'))
        # Approve registration: create user with selected role and mark request as approved
        if action == 'approve':
            role = request.form.get('role') or 'maid'
            # Sanitize role: allowed roles only
            if role not in ('owner', 'maid', 'manager'):
                role = 'maid'
            req = conn.execute('SELECT * FROM registration_requests WHERE id = ?', (req_id,)).fetchone()
            if req and req['status'] == 'pending':
                # Insert new user and add them to the global chat
                cur = conn.cursor()
                cur.execute(
                    'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                    (req['username'], req['password_hash'], role)
                )
                new_user_id = cur.lastrowid
                # Ensure chat room tables exist and add to global chat
                ensure_chat_rooms_table(conn)
                ensure_chat_room_members_table(conn)
                try:
                    cur.execute(
                        'INSERT OR IGNORE INTO chat_room_members (room_id, user_id) VALUES (1, ?)',
                        (new_user_id,)
                    )
                except Exception:
                    pass
                # Mark request as approved
                conn.execute(
                    'UPDATE registration_requests SET status = ? WHERE id = ?',
                    ('approved', req_id)
                )
                conn.commit()
                flash('Пользователь {} одобрен с ролью {}.'.format(req['username'], role))
        elif action == 'reject':
            # Mark request as rejected
            conn.execute(
                'UPDATE registration_requests SET status = ? WHERE id = ?',
                ('rejected', req_id)
            )
            conn.commit()
            flash('Запрос на регистрацию отклонён.')
        conn.close()
        return redirect(url_for('registration_requests'))
    # GET: show pending requests
    reqs = conn.execute(
        'SELECT * FROM registration_requests WHERE status = ?',
        ('pending',)
    ).fetchall()
    conn.close()
    # Pass list of roles to template for selection
    roles = ['owner', 'maid', 'manager']
    return render_template('registration_requests.html', requests=reqs, roles=roles)

# Ensure room_statuses table exists
def ensure_status_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS room_statuses (
            room_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            status TEXT NOT NULL,
            PRIMARY KEY (room_id, date),
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        )
        """
    )
    # Commit DDL on PostgreSQL so subsequent selects see the table
    try:
        conn.commit()
    except Exception:
        pass

# Ensure blacklist table exists for storing sanitized phone numbers that are blacklisted
def ensure_blacklist_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the blacklist table exists with the required columns. This table now stores
    the sanitized phone number along with a reason and timestamp for when the guest
    was added to the blacklist. If the table already exists without the extra
    columns, attempt to add them via ALTER TABLE. Any errors from adding
    existing columns are silently ignored.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            phone TEXT PRIMARY KEY,
            reason TEXT,
            added_at TEXT
        )
        """
    )
    # Attempt to add reason column if it does not exist
    try:
        conn.execute("ALTER TABLE blacklist ADD COLUMN reason TEXT")
    except Exception:
        pass
    # Attempt to add added_at column if it does not exist
    try:
        conn.execute("ALTER TABLE blacklist ADD COLUMN added_at TEXT")
    except Exception:
        pass

# Ensure that the guest_comments table exists. This table stores multiple
# comments for each guest, allowing a history of notes to be maintained.
def ensure_guest_comments_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS guest_comments (
            id SERIAL PRIMARY KEY ,
            guest_id INTEGER NOT NULL,
            comment TEXT,
            created_at TEXT,
            FOREIGN KEY (guest_id) REFERENCES guests(id) ON DELETE CASCADE
        )
        """
    )

# Ensure subscriptions table exists. This table stores subscription information for each owner.
def ensure_subscriptions_table(conn: sqlite3.Connection) -> None:
    """
    Create the subscriptions table if it does not exist. Each subscription is tied to a user (owner)
    and stores the plan name, status, price, and billing dates. This allows the application to
    manage access based on subscription status.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            plan_name TEXT NOT NULL,
            status TEXT NOT NULL,
            price REAL,
            created_at TEXT,
            next_billing_date TEXT,
            payment_id TEXT,
            payment_url TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    # Attempt to add new columns if the table exists without them. SQLite will
    # raise an exception if the column already exists; we ignore that error.
    try:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN payment_id TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN payment_url TEXT")
    except Exception:
        pass
    # Commit DDL changes so the new table/columns are visible immediately on PostgreSQL
    try:
        conn.commit()
    except Exception:
        pass


def initiate_kaspi_payment(plan_name: str, price: float) -> tuple[str, str]:
    """Simulate initiating a payment with Kaspi.

    This helper generates a unique payment identifier and constructs a URL
    pointing to the internal payment callback route. In a real integration
    you would send a request to Kaspi's API and use the returned URL.

    Args:
        plan_name: Name of the selected plan (e.g., 'standard' or 'premium').
        price: Price of the selected plan.

    Returns:
        A tuple consisting of (payment_id, payment_url).
    """
    payment_id = uuid.uuid4().hex  # Unique payment token
    # url_for requires a request context. When called inside a view this is fine.
    payment_url = url_for('payment_callback', payment_id=payment_id, _external=True)
    return payment_id, payment_url


# Ensure that the guests table has an extra_phone column. This allows storing
# an additional phone number for each guest. If the column already exists,
# attempting to add it will raise an exception which we silently ignore.
def ensure_extra_phone_column(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE guests ADD COLUMN extra_phone TEXT")
    except Exception:
        # The column likely already exists; ignore any error.
        pass

# Ensure that the guests table has a birth_date column.
def ensure_birth_date_column(conn: sqlite3.Connection) -> None:
    """
    Add a ``birth_date`` column to the ``guests`` table if it does not exist.

    The application stores the guest's birth date in a dedicated column.  On
    older deployments this column may be missing, causing ``UndefinedColumn``
    errors when reading or writing guest data.  This helper attempts to
    alter the ``guests`` table to add a ``birth_date DATE`` column.  If the
    column already exists or the table does not exist yet, any resulting
    exception is silently ignored.  Because ``ALTER TABLE`` statements are
    automatically committed on PostgreSQL when autocommit is enabled,
    explicit commits are not required.
    """
    try:
        conn.execute("ALTER TABLE guests ADD COLUMN birth_date DATE")
    except Exception:
        # Ignore failures if the column already exists or cannot be added
        pass

# Ensure that the guests table has a photo column.
def ensure_guest_photo_column(conn: sqlite3.Connection) -> None:
    """
    Add a ``photo`` column to the ``guests`` table if it does not exist.

    Guest photos are stored as filenames in the ``photo`` column.  In
    deployments where this column is absent, attempts to access guest
    photos will raise ``KeyError`` or ``UndefinedColumn`` errors.  This
    helper calls ``ALTER TABLE`` to add a ``photo TEXT`` column to the
    ``guests`` table.  Any error (for example if the column already
    exists) is suppressed so that the caller can proceed without
    disruption.
    """
    try:
        conn.execute("ALTER TABLE guests ADD COLUMN photo TEXT")
    except Exception:
        pass

# Ensure that the registration_requests table exists for pending user sign‑ups
def ensure_registration_requests_table(conn: sqlite3.Connection) -> None:
    """
    Create the registration_requests table if it does not already exist. This table stores
    pending registration entries awaiting approval by an owner.
    """
    conn.execute(
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


# Ensure that the users table has a 'photo' column for storing profile
# picture filenames. If the column does not exist, it will be added via
# ALTER TABLE. This function can be called before accessing or
# updating user photo data to lazily migrate the schema.
def ensure_photo_column(conn: sqlite3.Connection) -> None:
    """
    Ensure that the ``users`` table has a ``photo`` column.  This helper
    first checks if the table exists (via PRAGMA table_info) and only
    attempts to add the column if the table is present and the column is
    missing.  When using PostgreSQL, attempting to alter a non‑existent
    table would raise an error, so we skip the ALTER if no columns are
    returned.  Any exceptions during the ALTER are suppressed to
    accommodate concurrent deployments.
    """
    # Query the table info; when using PostgreSQL the compatibility layer
    # emulates PRAGMA via information_schema.  If the table does not
    # exist, the result will be an empty list.  Only proceed if there
    # are existing columns.
    cols = conn.execute("PRAGMA table_info(users)").fetchall()
    if not cols:
        # Table does not exist; nothing to do
        return
    # Each row returned by PRAGMA is either a tuple (from raw sqlite) or a mapping
    # object (e.g. sqlite3.Row or RealDictRow).  sqlite3.Row acts like a mapping
    # but does not implement ``get``; instead, column values can be accessed via
    # indexing with the column name.  Determine the column name accordingly.
    has_photo = False
    for row in cols:
        if isinstance(row, tuple):
            # For tuple rows, the column name is at index 1
            col_name = row[1]
        else:
            try:
                # sqlite3.Row and RealDictRow support dict-style access using []
                col_name = row['name']
            except Exception:
                # Fallback to None if name cannot be determined
                col_name = None
        if col_name == 'photo':
            has_photo = True
            break
    if not has_photo:
        try:
            conn.execute("ALTER TABLE users ADD COLUMN photo TEXT;")
        except Exception:
            # Ignore errors if another process has already added the column or if
            # the underlying database does not support ALTER TABLE in this way
            pass

# ---------------------------------------------------------------------------
# Chat rooms and memberships
#
# These helpers support the creation of separate chat rooms (private or
# group chats) and membership tracking. The default global chat uses
# room_id = 1 and is created automatically if it does not exist.
def ensure_chat_rooms_table(conn: sqlite3.Connection) -> None:
    """Create the chat_rooms table and ensure a default global room exists."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_rooms (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Ensure a default global chat room (id=1) exists
    try:
        row = conn.execute('SELECT id FROM chat_rooms WHERE id = 1').fetchone()
        if not row:
            conn.execute('INSERT INTO chat_rooms (id, name) VALUES (1, ?)', ('Общий чат',))
    except Exception:
        # Ignore if room already exists or insertion fails
        pass


def ensure_chat_room_members_table(conn: sqlite3.Connection) -> None:
    """Create the chat_room_members table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_room_members (
            room_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (room_id, user_id),
            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )


# Ensure messages table exists. This function creates a simple chat table
# used by the "Чат" feature. Each message stores the user_id of the
# author, the message text and a timestamp (string in ISO format).  It
# can be called whenever the chat view is accessed to lazily create
# the table if it doesn't already exist.
def ensure_messages_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the messages table exists with support for multiple chat rooms and
    optional file attachments. If the table pre‑exists without newer
    columns (room_id, file_name, file_type), they are added via ALTER TABLE.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            room_id INTEGER NOT NULL DEFAULT 1,
            user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            file_name TEXT,
            file_type TEXT,
            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    # Add missing columns if the table pre‑exists without them
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(messages)")
    cols = [row[1] for row in cur.fetchall()]
    if 'room_id' not in cols:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN room_id INTEGER DEFAULT 1")
        except Exception:
            pass
    if 'file_name' not in cols:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN file_name TEXT")
        except Exception:
            pass
    if 'file_type' not in cols:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN file_type TEXT")
        except Exception:
            pass

# Ensure the messages table has columns to store optional file attachments.
# Some earlier versions of the database may not include these columns,
# so this helper checks for them and adds them if necessary. The columns
# added are `file_name` (string) to store the uploaded filename and
# `file_type` (string) to store the MIME type or extension. The
# function runs harmlessly if the columns already exist.
def ensure_message_file_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(messages)")
    cols = [row[1] for row in cur.fetchall()]
    # Add file_name column if missing.  Wrap in try/except and rollback on
    # error.  Without the rollback, a failed ALTER TABLE would put the
    # connection into an aborted transaction state, preventing further
    # statements from executing until a rollback is issued.  See
    # psycopg2.errors.InFailedSqlTransaction.
    if 'file_name' not in cols:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN file_name TEXT")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    # Add file_type column if missing
    if 'file_type' not in cols:
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN file_type TEXT")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass


# A helper to ensure the table tracking the last seen chat message for each user
# exists. This table stores, for each user, the highest message ID they have
# viewed in the chat. It allows us to calculate the number of unread
# messages for each user by comparing the max message ID with the user's
# last seen ID. If the table doesn't exist it will be created.
def ensure_last_seen_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_last_seen (
            user_id INTEGER PRIMARY KEY,
            last_seen_message_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    # Explicitly commit DDL changes.  On PostgreSQL, DDL is transactional and
    # will not be visible to subsequent queries until committed.  Without
    # committing here, a following SELECT could fail with "relation does not
    # exist" even though the CREATE TABLE has been executed.  Wrapping in
    # try/except guards against errors on SQLite (where commit may be a no‑op).
    try:
        conn.commit()
    except Exception:
        pass


def ensure_user_room_last_seen_table(conn: sqlite3.Connection) -> None:
    """Ensure the user_room_last_seen table exists.

    This table stores the last seen message ID for each user in each room. It uses
    a composite primary key (user_id, room_id) so there is at most one record
    per user per room. If the table already exists, this function has no effect.
    """
    """Ensure the user_room_last_seen table exists.

    This helper creates the user_room_last_seen table if it does not already
    exist.  The table references chat_rooms(id), so it attempts to create
    the chat_rooms table first.  By explicitly ensuring chat_rooms exists
    before creating user_room_last_seen, we avoid errors on databases
    (notably PostgreSQL) that reject a CREATE TABLE referencing a missing
    foreign key table. If chat_rooms already exists this call is a no-op.
    """
    # Attempt to clear any aborted transaction state.  If a previous SQL error
    # left the connection in a failed transaction, subsequent statements will
    # raise ``InFailedSqlTransaction`` until a rollback.  This call does
    # nothing if the connection is not in a transaction or if rollback is
    # unsupported.
    try:
        conn.rollback()
    except Exception:
        pass
    # Ensure chat_rooms table exists to satisfy the foreign key constraint.  If
    # this call fails, we ignore the error and continue; the subsequent CREATE
    # may still succeed on SQLite.  On PostgreSQL, if chat_rooms is missing
    # CREATE TABLE will fail and we'll handle it below.
    try:
        ensure_chat_rooms_table(conn)
    except Exception:
        pass
    # Create the user_room_last_seen table with appropriate foreign keys.  Wrap
    # the execution and commit in a try/except so that any failure rolls back
    # the transaction.  Without rollback, a failed CREATE leaves the
    # connection in an aborted state and all subsequent commands will error.
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_room_last_seen (
                user_id INTEGER NOT NULL,
                room_id INTEGER NOT NULL,
                last_seen_message_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, room_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass




# Home page
@app.route("/")
def index():
    return render_template("index.html")


# Guests list
@app.route("/guests")
@login_required
@roles_required('owner', 'manager')
def list_guests():
    conn = get_db_connection()
    # Ensure blacklist table exists and fetch blacklisted phone digits
    ensure_blacklist_table(conn)
    black_rows = conn.execute("SELECT phone FROM blacklist").fetchall()
    blacklisted_digits = {row["phone"] for row in black_rows}
    import re
    # Check for search query in URL; filter guests by name or phone
    search_query = request.args.get("search", default="", type=str).strip()
    # Split the query into individual terms (e.g. first name, last name, patronymic)
    search_terms = []
    if search_query:
        # Use regex split to handle multiple spaces and trim empty tokens
        for term in re.split(r"\s+", search_query):
            term = term.strip().lower()
            if term:
                search_terms.append(term)
    # Extract only digits from the search query for phone number matching
    search_digits = re.sub(r"\D", "", search_query)
    # Fetch guests along with number of bookings
    # Order guests so that recently created records (with larger IDs) appear first.
    guests_raw = conn.execute(
        "SELECT g.*, COUNT(b.id) AS booking_count "
        "FROM guests AS g "
        "LEFT JOIN bookings AS b ON b.guest_id = g.id "
        "GROUP BY g.id "
        "ORDER BY g.id DESC"
    ).fetchall()
    guest_list = []
    for g in guests_raw:
        phone = g["phone"] or ""
        sanitized = re.sub(r"\D", "", phone)
        is_blacklisted = sanitized in blacklisted_digits
        guest_dict = {key: g[key] for key in g.keys()}
        guest_dict["blacklisted"] = is_blacklisted
        guest_dict["booking_count"] = g["booking_count"]
        # Apply search filter: if query exists, match against any term in the guest name or digits in the phone
        if search_terms:
            name_lower = (guest_dict["name"] or "").lower()
            # Determine if any search term appears in the guest's full name
            term_match = any(term in name_lower for term in search_terms)
            # Determine if the digit-only portion of the query appears in the sanitized phone number
            digits_match = bool(search_digits and search_digits in sanitized)
            # Skip this guest if neither the name nor the phone matches the search
            if not term_match and not digits_match:
                continue
        guest_list.append(guest_dict)
    conn.close()
    return render_template("guests.html", guests=guest_list, search_query=search_query)

# Delete guest
@app.route("/guests/delete/<int:guest_id>", methods=["POST"])
@login_required
@roles_required('owner', 'manager')
def delete_guest(guest_id: int):
    """Disabled guest deletion. This endpoint now only informs the user that
    deleting guests is not allowed and redirects back to the guests list.

    We intentionally avoid executing any DELETE statements to preserve guest
    records and their associated bookings. Even though the route still exists
    (to prevent broken links), it no longer performs any destructive action.
    """
    # Always redirect back to the guests list with a message. We do not look up
    # the guest or perform any deletion to ensure data safety.
    flash("Удаление гостей отключено.")
    return redirect(url_for("list_guests"))

# View guest detail by ID
@app.route("/guests/<int:guest_id>")
@login_required
@roles_required('owner', 'manager')
def view_guest(guest_id: int):
    """Display detailed information about a single guest, including their bookings."""
    conn = get_db_connection()
    guest = conn.execute("SELECT * FROM guests WHERE id = ?", (guest_id,)).fetchone()
    if not guest:
        conn.close()
        # If guest not found, render guest_info with no data
        return render_template('guest_info.html', guest=None, bookings=None, full_phone=None)
    # Fetch bookings for this guest, including room number and the name of the manager
    raw_bookings = conn.execute(
        """
        SELECT b.*, r.room_number,
               CASE
                 WHEN COALESCE(u.name, '') <> ''
                 THEN u.name
                 ELSE u.username
               END AS manager_name
        FROM bookings AS b
        JOIN rooms AS r ON b.room_id = r.id
        LEFT JOIN users AS u ON b.created_by = u.id
        WHERE b.guest_id = ?
        ORDER BY b.check_in_date DESC
        """,
        (guest_id,)
    ).fetchall()
    bookings = []
    for b in raw_bookings:
        # Convert to dict so we can add computed fields
        booking_dict = {key: b[key] for key in b.keys()}
        # Compute number of nights (difference between check_out_date and check_in_date)
        try:
            nights = (date.fromisoformat(b['check_out_date']) - date.fromisoformat(b['check_in_date'])).days
            # Ensure non-negative
            booking_dict['nights'] = nights if nights >= 0 else 0
        except Exception:
            booking_dict['nights'] = None
        bookings.append(booking_dict)
    # Determine if this guest's phone is blacklisted by sanitizing phone and checking in the blacklist table
    ensure_blacklist_table(conn)
    import re
    phone = guest['phone'] or ''
    sanitized = re.sub(r"\D", "", phone)
    is_blacklisted = False
    if sanitized:
        try:
            is_blacklisted = conn.execute(
                "SELECT 1 FROM blacklist WHERE phone = ?", (sanitized,)
            ).fetchone() is not None
        except Exception:
            is_blacklisted = False
    # If blacklisted, fetch the reason and date from the blacklist table
    blacklist_reason = None
    blacklist_added_at = None
    if is_blacklisted:
        try:
            row = conn.execute("SELECT reason, added_at FROM blacklist WHERE phone = ?", (sanitized,)).fetchone()
            if row:
                blacklist_reason = row["reason"]
                blacklist_added_at = row["added_at"]
        except Exception:
            pass
    # Prepare a display name using a more intuitive breakdown of the full name.
    # We assume the first word is the given name (Имя) and the rest constitute the patronymic
    # and/or family name. This way, a single given name will be shown correctly, and for
    # multiple words the first word remains as the given name. The remainder, if any,
    # will be displayed after it.
    full_name = guest['name'] or ''
    parts = full_name.split()
    if parts:
        first_name = parts[0]
        patronymic = ' '.join(parts[1:]) if len(parts) > 1 else ''
    else:
        first_name = ''
        patronymic = ''
    # Combine first name and the remainder for display
    display_name = first_name
    if patronymic:
        display_name += ' ' + patronymic
    # full_phone used by template for display; keep as stored phone
    full_phone = guest['phone'] or None
    # Fetch the most recent comment and all comments for this guest from guest_comments
    ensure_guest_comments_table(conn)
    # Query all comments; order by created_at descending
    try:
        # Use SQLite‑style placeholder.  Our database wrapper will
        # convert '?' into '%s' for psycopg2 when connected to
        # PostgreSQL.  The previous version used '%s' directly, which
        # SQLite does not understand and therefore raised an exception.
        comments_rows = conn.execute(
            "SELECT comment, created_at FROM guest_comments WHERE guest_id = ? ORDER BY created_at DESC",
            (guest_id,),
        ).fetchall()
    except Exception:
        comments_rows = []
    # Extract last comment if exists
    last_comment = comments_rows[0]['comment'] if comments_rows else None
    all_comments = [dict(comment=row['comment'], created_at=row['created_at']) for row in comments_rows]
    # Close connection after queries
    conn.close()
    return render_template(
        'guest_info.html',
        guest=guest,
        bookings=bookings,
        full_phone=full_phone,
        blacklisted=is_blacklisted,
        display_name=display_name,
        last_comment=last_comment,
        all_comments=all_comments,
        blacklist_reason=blacklist_reason,
        blacklist_added_at=blacklist_added_at,
    )

# Edit guest details
@app.route("/guests/edit/<int:guest_id>", methods=["GET", "POST"])
@login_required
@roles_required('owner', 'manager')
def edit_guest(guest_id: int):
    """
    Display a form for editing guest information and handle updates. Allows
    updating the name, phone, email, notes, birth date, and photo. If a
    new photo is uploaded, the old photo filename is replaced. After
    successful update, redirect back to the guest's detail page.
    """
    conn = get_db_connection()
    guest = conn.execute("SELECT * FROM guests WHERE id = ?", (guest_id,)).fetchone()
    if not guest:
        conn.close()
        flash("Гость не найден.")
        return redirect(url_for("list_guests"))
    # On POST, update the guest record
    if request.method == "POST":
        # Combine separate name components into a single full name. We expect
        # last_name, first_name, and patronymic fields from the form. If any
        # field is missing, it will be skipped when joining.
        last_name = request.form.get("last_name", "").strip()
        first_name = request.form.get("first_name", "").strip()
        patronymic = request.form.get("patronymic", "").strip()
        # Build the full name by joining non-empty parts with spaces
        name_parts = [part for part in [last_name, first_name, patronymic] if part]
        name = " ".join(name_parts) or None
        phone = request.form.get("phone", "").strip() or None
        # Дополнительный телефон может быть пустым; приводим к None, если поле пустое
        extra_phone = request.form.get("extra_phone", "").strip() or None
        email = request.form.get("email", "").strip() or None
        notes = request.form.get("notes", "").strip() or None
        birth_date = request.form.get("birth_date", "").strip() or None
        # Handle file upload for photo
        file = request.files.get("photo")
        # Default to existing photo filename if present.  When using PostgreSQL,
        # the guests table may not yet include a ``photo`` column; use getattr
        # style access and default to None if the key is missing.
        try:
            photo_filename = guest["photo"]
        except Exception:
            photo_filename = None
        if file and file.filename:
            # Save uploaded file to static/uploads
            filename = secure_filename(file.filename)
            upload_folder = os.path.join(app.static_folder, "uploads")
            # Ensure directory exists
            os.makedirs(upload_folder, exist_ok=True)
            file.save(os.path.join(upload_folder, filename))
            photo_filename = filename
        # Insert new comment into guest_comments if provided
        if notes:
            try:
                # Ensure comments table exists
                ensure_guest_comments_table(conn)
                # Insert comment with current timestamp.  The guest_comments table
                # defines columns (id, guest_id, comment, created_at).  Use
                # positional placeholders rather than the erroneous "%?" and include
                # the created_at timestamp.  Without specifying created_at,
                # the insert would fail because the column is non‑nullable on
                # PostgreSQL and you would lose the note timestamp on SQLite.
                timestamp = datetime.utcnow().isoformat()
                conn.execute(
                    """
                    INSERT INTO guest_comments (guest_id, comment, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (guest_id, notes, timestamp),
                )
            except Exception:
                # Ignore insert failures; the notes will simply not be saved.
                pass
        # Update guest record with the latest note
        with conn:
            # Ensure extra_phone column exists before updating
            ensure_extra_phone_column(conn)
            conn.execute(
                "UPDATE guests SET name = ?, phone = ?, extra_phone = ?, email = ?, notes = ?, birth_date = ?, photo = ? WHERE id = ?",
                (name, phone, extra_phone, email, notes, birth_date, photo_filename, guest_id),
            )
        conn.close()
        flash("Данные гостя обновлены.")
        return redirect(url_for("view_guest", guest_id=guest_id))
    # GET: show prepopulated form. Split the existing name into components so
    # that each input field is pre-filled. We assume the name is stored as
    # "LastName FirstName Patronymic", but if fewer parts exist, populate
    # accordingly.
    full_name = guest["name"] or ""
    parts = full_name.split()
    # Split the stored full name into components for the edit form. If only one
    # word is present, treat it as the given name; if two words, treat the first
    # as the given name and the second as the family name. For three or more
    # parts, assume the pattern "LastName FirstName Patronymic".
    if len(parts) == 0:
        last_name = ""
        first_name = ""
        patronymic = ""
    elif len(parts) == 1:
        first_name = parts[0]
        last_name = ""
        patronymic = ""
    elif len(parts) == 2:
        first_name = parts[0]
        last_name = parts[1]
        patronymic = ""
    else:
        # Three or more: interpret first as family name, second as given name, rest as patronymic
        last_name = parts[0]
        first_name = parts[1]
        patronymic = " ".join(parts[2:])
    # Determine if guest is blacklisted by sanitizing phone and checking in blacklist table
    ensure_blacklist_table(conn)
    import re
    phone_val = guest["phone"] or ""
    sanitized_phone = re.sub(r"\D", "", phone_val)
    blacklisted = False
    if sanitized_phone:
        try:
            blacklisted = conn.execute(
                "SELECT 1 FROM blacklist WHERE phone = ?", (sanitized_phone,)
            ).fetchone() is not None
        except Exception:
            blacklisted = False
    conn.close()
    # Also fetch all blacklisted phone numbers to provide to the phone mask script
    # Each entry in the blacklist table stores only sanitized digits
    bconn = get_db_connection()
    ensure_blacklist_table(bconn)
    rows = bconn.execute("SELECT phone FROM blacklist").fetchall()
    blacklisted_numbers = [row["phone"] for row in rows]
    bconn.close()
    # Compute sanitized digits from guest phone to pre-fill the phone input without non-digit characters
    phone_digits = re.sub(r"\D", "", phone_val)
    return render_template(
        "guest_edit_form.html",
        guest=guest,
        last_name=last_name,
        first_name=first_name,
        patronymic=patronymic,
        blacklisted=blacklisted,
        blacklisted_numbers=blacklisted_numbers,
        phone_digits=phone_digits,
    )

# Add guest's phone to blacklist
@app.route("/guests/blacklist/add/<int:guest_id>", methods=["POST"])
@login_required
@roles_required('owner', 'manager')
def add_guest_to_blacklist(guest_id: int):
    """
    Add the specified guest's phone number to the blacklist. A reason for
    blacklisting must be provided via form data. The reason and the timestamp
    of blacklisting are stored alongside the sanitized phone number in the
    blacklist table. If the reason is missing, the request is rejected with
    a flash message.
    """
    import re
    from datetime import datetime
    conn = get_db_connection()
    ensure_blacklist_table(conn)
    # Retrieve the provided reason from the form
    reason = (request.form.get("reason") or "").strip()
    if not reason:
        flash("Необходимо указать причину добавления в чёрный список.")
        conn.close()
        return redirect(url_for("list_guests"))
    # Fetch guest phone
    guest = conn.execute("SELECT phone FROM guests WHERE id = ?", (guest_id,)).fetchone()
    if not guest or not guest["phone"]:
        conn.close()
        flash("Гость не найден или телефон не указан.")
        return redirect(url_for("list_guests"))
    # Sanitize phone to digits only
    sanitized = re.sub(r"\D", "", guest["phone"])
    added_at = datetime.now().isoformat(timespec='seconds')
    try:
        with conn:
            # Insert or update the blacklist entry with reason and timestamp
            conn.execute(
                "INSERT INTO blacklist (phone, reason, added_at) VALUES (?, ?, ?) "
                "ON CONFLICT(phone) DO UPDATE SET reason = excluded.reason, added_at = excluded.added_at",
                (sanitized, reason, added_at),
            )
        flash("Гость занесён в чёрный список.")
    except Exception:
        flash("Не удалось занести гостя в чёрный список.")
    conn.close()
    return redirect(url_for("list_guests"))

# Remove guest's phone from blacklist
@app.route("/guests/blacklist/remove/<int:guest_id>", methods=["POST"])
@login_required
@roles_required('owner', 'manager')
def remove_guest_from_blacklist(guest_id: int):
    """Remove the specified guest's phone number from the blacklist."""
    import re
    conn = get_db_connection()
    ensure_blacklist_table(conn)
    guest = conn.execute("SELECT phone FROM guests WHERE id = ?", (guest_id,)).fetchone()
    if not guest or not guest["phone"]:
        conn.close()
        flash("Гость не найден или телефон не указан.")
        return redirect(url_for("list_guests"))
    sanitized = re.sub(r"\D", "", guest["phone"])
    with conn:
        conn.execute("DELETE FROM blacklist WHERE phone = ?", (sanitized,))
    conn.close()
    flash("Гость удалён из чёрного списка.")
    return redirect(url_for("list_guests"))


# Add guest
@app.route("/guests/add", methods=["GET", "POST"])
@login_required
@roles_required('owner', 'manager')
def add_guest():
    return_to = request.args.get('return_to') or request.form.get('return_to')
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip() or None
        # Дополнительный телефон может быть пустым. Приводим к None, если поле пустое
        extra_phone = request.form.get("extra_phone", "").strip() or None
        email = request.form.get("email", "").strip() or None
        notes = request.form.get("notes", "").strip() or None
        if not name:
            flash("Имя гостя обязательно.")
            # preserve return_to in redirect
            if return_to:
                return redirect(url_for("add_guest", return_to=return_to))
            return redirect(url_for("add_guest"))
        conn = get_db_connection()
        # Ensure the extra_phone column exists so that we can store the value
        ensure_extra_phone_column(conn)
        # Check if guest with same name or phone already exists
        duplicate = conn.execute(
            "SELECT id, name, phone FROM guests WHERE name = ? OR phone = ?",
            (name, phone),
        ).fetchone()
        if duplicate:
            # Guest exists; prepare form data for re-rendering
            existing_guest = dict(id=duplicate["id"], name=duplicate["name"], phone=duplicate["phone"])
            # Fetch bookings for the existing guest with room info and compute nights
            raw_bookings = conn.execute(
                """
                SELECT b.*, r.room_number
                FROM bookings AS b
                JOIN rooms AS r ON b.room_id = r.id
                WHERE b.guest_id = ?
                ORDER BY b.check_in_date DESC
                """,
                (duplicate["id"],)
            ).fetchall()
            existing_bookings = []
            for b in raw_bookings:
                booking_dict = {key: b[key] for key in b.keys()}
                try:
                    nights = (date.fromisoformat(b['check_out_date']) - date.fromisoformat(b['check_in_date'])).days
                    booking_dict['nights'] = nights if nights >= 0 else 0
                except Exception:
                    booking_dict['nights'] = None
                existing_bookings.append(booking_dict)
            conn.close()
            form_data = dict(name=name, phone=phone or "", extra_phone=extra_phone or "", email=email or "", notes=notes or "")
            # Render the add guest form with duplicate warning and options, plus existing bookings info
            return render_template(
                "guest_form.html",
                duplicate_guest=True,
                existing_guest=existing_guest,
                existing_bookings=existing_bookings,
                form_data=form_data,
                return_to=return_to,
            )
        # No duplicate; proceed with insertion
        with conn:
            cur = conn.execute(
                "INSERT INTO guests (name, phone, extra_phone, email, notes) VALUES (?, ?, ?, ?, ?)",
                (name, phone, extra_phone, email, notes),
            )
            new_id = cur.lastrowid
        conn.close()
        flash("Гость добавлен успешно!")
        # If return_to provided, redirect back with new guest id
        if return_to:
            sep = '&' if '?' in return_to else '?'
            return redirect(f"{return_to}{sep}guest_id={new_id}")
        return redirect(url_for("list_guests"))
    # GET request
    return render_template("guest_form.html", return_to=return_to)


# Rooms list
@app.route("/rooms")
@login_required
@roles_required('owner')
def list_rooms():
    conn = get_db_connection()
    rooms = conn.execute("SELECT * FROM rooms ORDER BY id").fetchall()
    conn.close()
    return render_template("rooms.html", rooms=rooms)


@app.route("/rooms/<int:room_id>/delete", methods=["POST"])
@login_required
@roles_required('owner')
def delete_room(room_id: int):
    """
    Delete a room along with its associated bookings and room status records.
    Only owners may perform this action.  After deletion a success or error
    message is flashed and the user is redirected to the list of rooms.
    """
    conn = get_db_connection()
    # Verify that the room exists
    room = conn.execute("SELECT id FROM rooms WHERE id = ?", (room_id,)).fetchone()
    if room is None:
        conn.close()
        flash("Квартира не найдена.", "danger")
        return redirect(url_for('list_rooms'))
    try:
        # Use a transaction to delete dependent records and the room itself
        with conn:
            conn.execute("DELETE FROM bookings WHERE room_id = ?", (room_id,))
            conn.execute("DELETE FROM room_statuses WHERE room_id = ?", (room_id,))
            conn.execute("DELETE FROM rooms WHERE id = ?", (room_id,))
        flash("Квартира успешно удалена.", "success")
    except Exception as e:
        # Roll back on error and display a message
        conn.rollback()
        flash(f"Ошибка при удалении квартиры: {e}", "danger")
    finally:
        conn.close()
    return redirect(url_for('list_rooms'))

def ensure_booking_creator_column(conn: sqlite3.Connection) -> None:
    """
    Ensure that the ``bookings`` table contains a ``created_by`` column.

    On startup, older databases may lack a ``created_by`` column used to
    associate a booking with the user who created it.  This helper
    introspects the table schema and adds the column if it is missing.  It
    works for both SQLite and PostgreSQL connections because the PRAGMA
    statement is intercepted by the PostgreSQL compatibility layer to query
    ``information_schema``.  The helper is idempotent: if the column
    already exists or the table does not yet exist, no action is taken.  Any
    exceptions encountered are suppressed to avoid breaking the caller.

    Parameters
    ----------
    conn : sqlite3.Connection or SQLiteCompatConnection
        An open database connection or its compatibility wrapper.
    """
    try:
        # Use PRAGMA to inspect columns.  On PostgreSQL the compatibility
        # layer will return tuples of (cid, name, type, notnull, dflt, pk).
        cur = conn.execute("PRAGMA table_info(bookings)")
        rows = cur.fetchall()
        # Extract column names regardless of row type (tuple or mapping)
        col_names = []
        for row in rows:
            try:
                # sqlite3.Row exposes row[1] as column name
                col_names.append(row[1])
            except Exception:
                # RealDictRow from psycopg2 exposes 'name'
                if isinstance(row, dict) and 'name' in row:
                    col_names.append(row['name'])
        if 'created_by' not in col_names:
            conn.execute("ALTER TABLE bookings ADD COLUMN created_by INTEGER")
            # Attempt to commit if supported.  In autocommit mode this is a no-op.
            try:
                conn.commit()
            except Exception:
                pass
    except Exception:
        # Silently ignore if introspection or alteration fails.
        pass


# Add room
@app.route("/rooms/add", methods=["GET", "POST"])
@login_required
@roles_required('owner')
def add_room():
    """
    Handle adding a new room (object).

    In the updated workflow, a room only requires a single field: the object
    name (stored in the ``room_number`` column). Capacity and notes are no
    longer collected from the user. Instead, they default to NULL in the
    database. Only users with the roles ``owner`` or ``manager`` can add
    rooms.  If a user submits a blank room name, an error message is shown.
    Duplicate names are also prevented via a unique constraint on the
    ``room_number`` column.  Successful creation redirects back to the
    list of rooms.
    """
    if request.method == "POST":
        # Fetch and sanitize the room name (object name)
        room_number = request.form.get("room_number", "").strip()
        # Optional external listing URL
        listing_url = request.form.get("listing_url", "").strip() or None
        # Residential complex (ЖК). None if not selected or blank.
        residential_complex = request.form.get("residential_complex")
        residential_complex = residential_complex.strip() if residential_complex else None
        # Ensure a name was provided
        if not room_number:
            flash("Название квартиры обязательно.")
            return redirect(url_for("add_room"))
        conn = get_db_connection()
        try:
            with conn:
                # Insert the room name, listing URL (if provided) and residential complex
                conn.execute(
                    "INSERT INTO rooms (room_number, listing_url, residential_complex) VALUES (?, ?, ?)",
                    (room_number, listing_url, residential_complex),
                )
        except Exception:
            # On any insertion error (e.g. duplicate room number), roll back the
            # transaction if necessary and inform the user.  When using
            # PostgreSQL the unique constraint violation is a different
            # exception type than sqlite3.IntegrityError, so catch all
            # exceptions here.
            conn.close()
            flash("Ошибка: квартира с таким названием уже существует.")
            return redirect(url_for("add_room"))
        conn.close()
        flash("Квартира добавлена успешно!")
        return redirect(url_for("list_rooms"))
    # Render the form for GET requests
    return render_template("room_form.html")

# Edit existing room (update name and listing URL)
@app.route("/rooms/<int:room_id>/edit", methods=["GET", "POST"])
@login_required
@roles_required('owner')
def edit_room(room_id: int):
    """
    Edit an existing room's details, including its name and external listing URL.
    Only owners and managers are allowed to perform edits.  On GET, render
    a form pre-filled with the room's current data.  On POST, validate
    inputs and update the database accordingly.
    """
    conn = get_db_connection()
    room = conn.execute(
        "SELECT id, room_number, listing_url, residential_complex FROM rooms WHERE id = ?",
        (room_id,)
    ).fetchone()
    if not room:
        conn.close()
        abort(404)
    if request.method == "POST":
        new_name = request.form.get("room_number", "").strip()
        new_link = request.form.get("listing_url", "").strip() or None
        # Update the residential complex (may be blank/None)
        new_complex = request.form.get("residential_complex")
        new_complex = new_complex.strip() if new_complex else None
        if not new_name:
            flash("Название квартиры обязательно.")
            return redirect(url_for("edit_room", room_id=room_id))
        try:
            with conn:
                conn.execute(
                    "UPDATE rooms SET room_number = ?, listing_url = ?, residential_complex = ? WHERE id = ?",
                    (new_name, new_link, new_complex, room_id),
                )
        except Exception:
            # Catch any update error, including unique constraint violations in PostgreSQL.
            conn.close()
            flash("Ошибка: квартира с таким названием уже существует.")
            return redirect(url_for("edit_room", room_id=room_id))
        conn.close()
        flash("Данные квартиры обновлены.")
        return redirect(url_for("list_rooms"))
    conn.close()
    return render_template("room_edit_form.html", room=room)


# Calendar view
import calendar

# Context processor to inject lists of guest names and phone numbers for autocomplete suggestions.
@app.context_processor
def inject_guest_autocomplete():
    """
    Provide global lists of guest names and phone numbers to templates. These lists
    can be used to populate datalist elements for autocomplete suggestions on
    inputs such as search fields, guest names and phone numbers. Filtering
    duplicates and empty values ensures the lists remain concise. If the
    database is large, you may consider implementing an asynchronous endpoint
    instead of passing all values.
    """
    try:
        conn = get_db_connection()
        # Ensure the extra_phone column exists before querying
        ensure_extra_phone_column(conn)
        # Collect distinct non-empty names
        name_rows = conn.execute(
            "SELECT DISTINCT name FROM guests WHERE name IS NOT NULL AND TRIM(name) != '' ORDER BY name"
        ).fetchall()
        phone_rows = conn.execute(
            "SELECT DISTINCT phone FROM guests WHERE phone IS NOT NULL AND TRIM(phone) != '' ORDER BY phone"
        ).fetchall()
        # Also include extra_phone values if present
        extra_rows = conn.execute(
            "SELECT DISTINCT extra_phone FROM guests WHERE extra_phone IS NOT NULL AND TRIM(extra_phone) != '' ORDER BY extra_phone"
        ).fetchall()
        conn.close()
        names = [row[0] for row in name_rows]
        phones = [row[0] for row in phone_rows] + [row[0] for row in extra_rows]
    except Exception:
        # On any error, return empty lists to avoid breaking templates
        names = []
        phones = []
    return dict(guest_names=names, guest_phones=phones)


@app.route("/calendar")
@app.route("/calendar/<int:year>/<int:month>")
@login_required
@roles_required('owner', 'manager')
def calendar_view(year=None, month=None):
    """
    Display a calendar grid for the specified month and year.
    Shows each room as a row and each day as a column with booking status.
    Includes navigation for previous/next month and year and a search form
    for available rooms within a date range.
    """
    today = date.today()
    year = year or today.year
    month = month or today.month

    # Determine which date's summary to display. If the user clicked a specific
    # day in the calendar, it will come via the ``selected_date`` query
    # parameter. Otherwise, default to today if it falls within the viewed
    # month/year; if not, use the first day of the month.  This ensures the
    # summary always reflects a valid date in the current calendar view.
    selected_date_str = request.args.get("selected_date") or None
    try:
        selected_date = date.fromisoformat(selected_date_str) if selected_date_str else None
    except ValueError:
        selected_date = None
    # If no explicit selection, choose a sensible default: if the current
    # month/year matches today's month/year, use today; otherwise use the
    # first day of the month.
    if selected_date is None:
        if year == today.year and month == today.month:
            selected_date = today
        else:
            selected_date = date(year, month, 1)

    # Optionally scroll to a specific date (isoformat string) provided via query
    scroll_date_str = request.args.get("scroll_date")
    try:
        scroll_date = date.fromisoformat(scroll_date_str) if scroll_date_str else None
    except ValueError:
        scroll_date = None

    # Compute previous and next month/year links
    prev_month = month - 1
    prev_year = year
    if prev_month < 1:
        prev_month = 12
        prev_year -= 1
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1

    # Prepare year navigation: show previous, current and next year relative to the viewed year
    # This ensures only three years are displayed in the navigation bar
    year_range = [year - 1, year, year + 1]

    # Prepare month navigation: show previous, current and next month relative to the viewed month.
    # When crossing year boundaries, adjust the year accordingly.
    # For example, if current month is January, previous month is December of the previous year.
    # Similarly, if current month is December, next month is January of the following year.
    prev_m = month - 1
    prev_y = year
    if prev_m < 1:
        prev_m = 12
        prev_y -= 1
    next_m = month + 1
    next_y = year
    if next_m > 12:
        next_m = 1
        next_y += 1
    # Build list of month links with associated year
    month_links = [
        {"month": prev_m, "year": prev_y},
        {"month": month,  "year": year},
        {"month": next_m, "year": next_y},
    ]

    # Month names for display (1-indexed); localised to Russian
    month_names = [
        "",  # placeholder for 0-index
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]

    # Abbreviations for weekdays (0=Monday, 6=Sunday)
    weekday_abbrs = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    # Build a list of dates for the days in this month
    cal = calendar.Calendar(firstweekday=0)  # Monday as first day
    month_days_iter = cal.itermonthdates(year, month)
    days_in_month = [d for d in month_days_iter if d.month == month]

    # Filter by residential complex (ЖК) if provided
    complex_filter = request.args.get("complex") or None

    # Handle search for available rooms
    # Determine if the user requested to show all free rooms across the selected complex via the "show_free" parameter.
    available_rooms = None
    show_free_flag = request.args.get("show_free")
    # Keep track of whether the free-room search originated from the show_free parameter (no date range).
    from_show_free = False
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    if show_free_flag and not (start_date_str and end_date_str):
        try:
            conn = get_db_connection()
            # Fetch rooms that have no bookings at all (i.e. completely free). Optionally filter by complex.
            if complex_filter:
                query = """
                    SELECT id, room_number, residential_complex
                    FROM rooms
                    WHERE residential_complex = ?
                      AND id NOT IN (SELECT DISTINCT room_id FROM bookings)
                    ORDER BY room_number
                """
                available_rooms = conn.execute(query, (complex_filter,)).fetchall()
            else:
                query = """
                    SELECT id, room_number, residential_complex
                    FROM rooms
                    WHERE id NOT IN (SELECT DISTINCT room_id FROM bookings)
                    ORDER BY room_number
                """
                available_rooms = conn.execute(query).fetchall()
            conn.close()
            # Mark that the available rooms list is the result of a show_free search
            from_show_free = True
        except Exception:
            # On any error, treat as no available rooms
            available_rooms = []
    elif start_date_str and end_date_str:
        try:
            start_date_obj = date.fromisoformat(start_date_str)
            end_date_obj = date.fromisoformat(end_date_str)
            if start_date_obj <= end_date_obj:
                conn = get_db_connection()
                # Select rooms without conflicting bookings in the given date range, optionally filtered by residential complex
                if complex_filter:
                    query = """
                        SELECT id, room_number, residential_complex
                        FROM rooms
                        WHERE residential_complex = ?
                          AND id NOT IN (
                            SELECT room_id FROM bookings
                            -- A booking overlaps the searched range if it does not end before the range starts
                            -- and does not start on/after the range ends. We treat check_out as free, so use <= and >=.
                            WHERE NOT (date(check_out_date) <= date(?) OR date(check_in_date) >= date(?))
                        )
                        ORDER BY room_number
                    """
                    available_rooms = conn.execute(query, (complex_filter, start_date_str, end_date_str)).fetchall()
                else:
                    query = """
                        SELECT id, room_number, residential_complex
                        FROM rooms
                        WHERE id NOT IN (
                            SELECT room_id FROM bookings
                            -- A booking overlaps the searched range if it does not end before the range starts
                            -- and does not start on/after the range ends. We treat check_out as free, so use <= and >=.
                            WHERE NOT (date(check_out_date) <= date(?) OR date(check_in_date) >= date(?))
                        )
                        ORDER BY room_number
                    """
                    available_rooms = conn.execute(query, (start_date_str, end_date_str)).fetchall()
                conn.close()
        except ValueError:
            # invalid date format; ignore search
            available_rooms = []

    # Load rooms and bookings overlapping this month
    conn = get_db_connection()
    # Fetch list of residential complexes for filter dropdown (unique non-null values)
    complexes_rows = conn.execute(
        "SELECT DISTINCT residential_complex FROM rooms WHERE residential_complex IS NOT NULL ORDER BY residential_complex"
    ).fetchall()
    # Build a simple list of complexes (strings). We'll pass this to the template.
    # When using SQLite, each row is tuple-like and supports index access.  When using
    # PostgreSQL via psycopg2's RealDictCursor, each row is a mapping.  Use the column
    # name when available and fall back to positional index otherwise.
    complexes: list[str] = []
    for row in complexes_rows:
        try:
            val = row["residential_complex"]
        except Exception:
            # sqlite3.Row supports integer indexing; RealDictRow does not
            val = row[0] if row else None
        if val:
            complexes.append(val)

    # Include listing_url and residential_complex so templates can link and filter; optionally filter by complex
    if complex_filter:
        rooms = conn.execute(
            "SELECT id, room_number, listing_url, residential_complex FROM rooms WHERE residential_complex = ? ORDER BY room_number",
            (complex_filter,),
        ).fetchall()
    else:
        rooms = conn.execute(
            "SELECT id, room_number, listing_url, residential_complex FROM rooms ORDER BY room_number",
        ).fetchall()
    # Fetch bookings overlapping this month and include guest_id and amounts
    bookings = conn.execute(
        """
        SELECT id, room_id, guest_id, check_in_date, check_out_date, status, total_amount, paid_amount
        FROM bookings
        WHERE (
            (date(check_in_date) <= date(?) AND date(check_out_date) >= date(?))
            OR (date(check_in_date) BETWEEN date(?) AND date(?))
            OR (date(check_out_date) BETWEEN date(?) AND date(?))
        )
        """,
        (
            f"{year}-{month:02d}-01",
            f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}",
            f"{year}-{month:02d}-01",
            f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}",
            f"{year}-{month:02d}-01",
            f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}",
        ),
    ).fetchall()
    # Load custom statuses for this month
    first_day = f"{year}-{month:02d}-01"
    last_day = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
    ensure_status_table(conn)
    # Query custom statuses between the first and last day.  Avoid the
    # ``date(?)`` syntax which causes a type mismatch on PostgreSQL when
    # comparing a text column with a date literal.  Since dates are stored in
    # ISO format (YYYY-MM-DD), simple string comparison works for range
    # filtering across both SQLite and PostgreSQL.
    statuses = conn.execute(
        "SELECT room_id, date, status FROM room_statuses WHERE date >= ? AND date <= ?",
        (first_day, last_day),
    ).fetchall()
    # Map statuses to dates per room
    status_map = {(int(s["room_id"]), date.fromisoformat(s["date"])): s["status"] for s in statuses}
    conn.close()

    # Build a dictionary of guest names for tooltips
    # Use a fresh connection since the previous conn may be closed at this point
    with get_db_connection() as gn_conn:
        guest_names = {row["id"]: row["name"] for row in gn_conn.execute("SELECT id, name FROM guests").fetchall()}

    # Map bookings to each date per room_id
    booking_map = {}
    # Helper to coerce values returned from the database into ``datetime.date`` objects.
    # PostgreSQL returns ``datetime.date`` while SQLite returns strings; psycopg2 might
    # return ``datetime.datetime`` when using timestamp columns.  This helper normalises
    # those types into a plain date.  It assumes the value is either a date, datetime or
    # ISO‑formatted string; otherwise ``date.fromisoformat`` will raise.
    def _to_date(val: object) -> date:
        if isinstance(val, date) and not isinstance(val, datetime):
            return val
        if isinstance(val, datetime):
            return val.date()
        return date.fromisoformat(str(val))
    for b in bookings:
        # Convert sqlite Row to a mutable dict so we can compute per‑day rate
        b_dict = dict(b)
        try:
            check_in = _to_date(b_dict.get("check_in_date"))
            check_out = _to_date(b_dict.get("check_out_date"))
        except Exception:
            # Skip bookings with invalid date values rather than causing a 500 error
            continue
        # Number of nights: difference in days (exclusive of check‑out date). Use 1 as fallback to avoid division by zero.
        nights = (check_out - check_in).days or 1
        # Compute price per day based on total_amount, if provided
        if b_dict.get("total_amount"):
            try:
                b_dict["rate_per_day"] = (b_dict["total_amount"] / nights)
            except Exception:
                b_dict["rate_per_day"] = None
        else:
            b_dict["rate_per_day"] = None


        # Populate booking_map for each day spanned by booking.
        # For consistency with user expectations, we treat the check‑out date as occupied as well,
        # so bookings spanning multiple days include both the check‑in and check‑out dates. This
        # differs from the pricing logic (which still charges per night), but ensures that a booking
        # from 28 сентября по 3 октября appears in the October calendar.
        # Populate booking_map for each day spanned by booking.
        # A booking from check_in to check_out is treated as occupying nights from
        # check_in up to but not including check_out. If check_in >= check_out
        # (zero nights or invalid range), mark only the check_in date.
        if check_in >= check_out:
            booking_map[(b_dict["room_id"], check_in)] = b_dict
        else:
            current = check_in
            while current < check_out:
                booking_map[(b_dict["room_id"], current)] = b_dict
                current += timedelta(days=1)

    # Prepare structure for template: list of rooms with day statuses
    calendar_data = []
    for room in rooms:
        row = {"room": room, "days": []}
        for d in days_in_month:
            b = booking_map.get((room["id"], d))
            if b:
                # When a booking exists on this date, prefer any custom room status set via the room_statuses table
                # (e.g. "occupied", "vacant", etc.). If no custom status is set, fall back to the booking's
                # own status only if it matches one of the occupancy states; otherwise treat as booked.  This allows
                # managers to change the occupancy status (e.g. from "booked" to "occupied") for a date with an
                # existing booking via the dropdown in the calendar, without being overridden by the booking's
                # deposit status (paid/withheld/returned).
                if (room["id"], d) in status_map:
                    # Custom status overrides everything
                    status_for_calendar = status_map[(room["id"], d)]
                else:
                    # Use the booking's status only if it represents a room occupancy state; otherwise default to booked
                    st = b.get("status") or "booked"
                    if st in ["occupied", "vacant", "booked", "ready", "cleaning", "hourly"]:
                        status_for_calendar = st
                    else:
                        status_for_calendar = "booked"
                row["days"].append({"date": d, "status": status_for_calendar, "booking": b})
            else:
                # For dates without a booking, use the custom status if present; otherwise mark past dates as vacant
                # and future dates as ready.
                if (room["id"], d) in status_map:
                    custom_status = status_map[(room["id"], d)]
                    row["days"].append({"date": d, "status": custom_status, "booking": None})
                else:
                    status_for_calendar = "vacant" if d < today else "ready"
                    row["days"].append({"date": d, "status": status_for_calendar, "booking": None})
        calendar_data.append(row)

    # Compute summary counts for the selected date.  We only count statuses
    # occurring on ``selected_date``, rather than across the entire month.
    summary_counts = {"occupied": 0, "vacant": 0, "booked": 0, "ready": 0, "cleaning": 0, "hourly": 0}
    for row in calendar_data:
        for cell in row["days"]:
            # Only include the status for the selected date
            if cell["date"] == selected_date:
                st = cell["status"]
                # Treat any unknown status (e.g., paid/returned) as booked for summary purposes
                if st not in summary_counts:
                    st_key = "booked"
                else:
                    st_key = st
                summary_counts[st_key] += 1

    # If a search for free rooms was performed, filter calendar_data to only include free rooms
    # available_rooms is None when no search or invalid date range
    if available_rooms is not None:
        # Build a set of room ids that are free
        free_room_ids = {room_row["id"] for room_row in available_rooms}
        calendar_data = [row for row in calendar_data if row["room"]["id"] in free_room_ids]

    return render_template(
        "calendar.html",
        year=year,
        month=month,
        prev_year=prev_year,
        prev_month=prev_month,
        next_year=next_year,
        next_month=next_month,
        year_range=year_range,
        month_names=month_names,
        weekday_abbrs=weekday_abbrs,
        days=days_in_month,
        rooms=calendar_data,
        available_rooms=available_rooms,
        start_date=start_date_str,
        end_date=end_date_str,
        summary_counts=summary_counts,
        guest_names=guest_names,
        today=today,
        scroll_date=scroll_date,
        complexes=complexes,
        selected_complex=complex_filter,
        month_links=month_links,
        selected_date=selected_date,
        from_show_free=from_show_free,
    )

# Bookings list
@app.route("/bookings")
@login_required
@roles_required('owner', 'manager')
def list_bookings():
    """Display a list of all bookings. Only owners and managers may view bookings."""
    conn = get_db_connection()
    # Retrieve bookings along with the name of the user who created each booking
    raw_rows = conn.execute(
        """
        SELECT b.id, g.name AS guest_name, r.room_number,
               b.check_in_date, b.check_out_date, b.status,
               b.total_amount, b.paid_amount,
               -- Compute manager_name: use first_name and last_name if available, otherwise username
               CASE
                 WHEN COALESCE(u.name, '') <> ''
                 THEN u.name
                 ELSE u.username
               END AS manager_name
        FROM bookings AS b
        JOIN guests AS g ON b.guest_id = g.id
        JOIN rooms AS r ON b.room_id = r.id
        LEFT JOIN users AS u ON b.created_by = u.id
        ORDER BY b.check_in_date DESC
        """
    ).fetchall()
    conn.close()
    return render_template("bookings.html", bookings=raw_rows)


# --------------------------------------------------
# Депозиты

@app.route("/deposits")
@login_required
@roles_required('owner', 'manager')
def deposits():
    """
    Страница со списком депозитов. Отображаются две вкладки:
    "Текущие" (депозиты со статусами "paid" и "withheld") и
    "Возвращённые" (статус "returned"). В каждой строке можно
    поменять статус депозита с помощью выпадающего списка.
    """
    conn = get_db_connection()
    # Текущие депозиты: не возвращённые
    # Include the manager name for each deposit entry
    deposits_current = conn.execute(
        """
        SELECT b.id, g.name AS guest_name, r.room_number,
               b.check_in_date, b.check_out_date, b.status,
               b.paid_amount,
               CASE
                 WHEN COALESCE(u.name, '') <> ''
                 THEN u.name
                 ELSE u.username
               END AS manager_name
        FROM bookings AS b
        JOIN guests AS g ON b.guest_id = g.id
        JOIN rooms AS r ON b.room_id = r.id
        LEFT JOIN users AS u ON b.created_by = u.id
        WHERE b.paid_amount IS NOT NULL AND b.paid_amount > 0
          AND b.status IN ('paid', 'withheld')
        ORDER BY b.check_in_date DESC
        """
    ).fetchall()
    # Возвращённые депозиты
    deposits_returned = conn.execute(
        """
        SELECT b.id, g.name AS guest_name, r.room_number,
               b.check_in_date, b.check_out_date, b.status,
               b.paid_amount,
               CASE
                 WHEN COALESCE(u.name, '') <> ''
                 THEN u.name
                 ELSE u.username
               END AS manager_name
        FROM bookings AS b
        JOIN guests AS g ON b.guest_id = g.id
        JOIN rooms AS r ON b.room_id = r.id
        LEFT JOIN users AS u ON b.created_by = u.id
        WHERE b.paid_amount IS NOT NULL AND b.paid_amount > 0
          AND b.status = 'returned'
        ORDER BY b.check_in_date DESC
        """
    ).fetchall()
    conn.close()
    return render_template(
        "deposits.html",
        deposits_current=deposits_current,
        deposits_returned=deposits_returned,
    )


@app.route("/deposits/update/<int:booking_id>", methods=["POST"])
@login_required
@roles_required('owner', 'manager')
def update_deposit_status(booking_id: int):
    """
    Обновить статус депозита для конкретного бронирования. Принимает новый
    статус из формы и сохраняет его в таблице bookings. После обновления
    перенаправляет обратно на страницу депозитов.
    """
    new_status = (request.form.get("deposit_status") or "paid").strip()
    # Допустимые статусы для депозита
    valid_statuses = {"paid", "withheld", "returned"}
    if new_status not in valid_statuses:
        flash("Недопустимый статус депозита.")
        return redirect(url_for('deposits'))
    conn = get_db_connection()
    with conn:
        conn.execute(
            "UPDATE bookings SET status = ? WHERE id = ?",
            (new_status, booking_id),
        )
    conn.close()
    flash("Статус депозита обновлён.")
    return redirect(url_for('deposits'))


# --------------------------------------------------
# Dashboard

@app.route('/dashboard')
@login_required
@roles_required('owner', 'manager')
def dashboard():
    """
    Display a high‑level dashboard summarizing key performance metrics for the
    selected date range.  Owners and managers can use this view to quickly
    assess occupancy, revenue and other indicators, and drill down into
    underlying bookings.  The dashboard supports a simple date range filter
    (start and end dates) via query parameters; if omitted, it defaults to
    today.

    Metrics shown:
      * Occupancy: percentage of sold nights over available nights.
      * ADR (Average Daily Rate): total revenue divided by sold nights.
      * RevPAR (Revenue per Available Room): total revenue divided by
        available nights.
      * Total revenue: sum of booking total_amounts in period.
      * Check‑ins and check‑outs counts.
      * Deposit statuses (counts by status).

    The view also renders a 14‑day pick‑up chart showing daily occupancy
    and revenue for the next two weeks starting from the selected start date.
    """
    from datetime import datetime, timedelta, date
    # Parse date range from query parameters; default to today
    # Read optional quick‑select period (1d, week, month, year) from query
    period_sel = request.args.get('period')
    start_date = None
    end_date = None
    # If a quick‑select period is specified, compute start_date and end_date accordingly
    if period_sel in {"1d", "week", "month", "year"}:
        end_date = date.today()
        if period_sel == "1d":
            start_date = end_date
        elif period_sel == "week":
            start_date = end_date - timedelta(days=6)
        elif period_sel == "month":
            start_date = end_date - timedelta(days=29)
        elif period_sel == "year":
            start_date = end_date - timedelta(days=364)
    else:
        # Otherwise, fall back to explicit start/end query parameters
        start_str = request.args.get('start')
        end_str = request.args.get('end')
        try:
            if start_str:
                start_date = date.fromisoformat(start_str)
            else:
                start_date = date.today()
            if end_str:
                end_date = date.fromisoformat(end_str)
            else:
                end_date = date.today()
        except Exception:
            # Fallback to today if parsing fails
            start_date = date.today()
            end_date = date.today()
    # Ensure start_date <= end_date
    if start_date and end_date and start_date > end_date:
        start_date, end_date = end_date, start_date
    # Compute inclusive range end by adding one day for calculations
    period_days = (end_date - start_date).days + 1
    # Connect to DB
    conn = get_db_connection()
    # Number of rooms (units) currently in the system
    rooms_count_row = conn.execute("SELECT COUNT(*) as cnt FROM rooms").fetchone()
    rooms_count = rooms_count_row['cnt'] if rooms_count_row else 0
    # Compute sold nights: sum of overlapping nights for each booking in the period.  To
    # maintain compatibility across SQLite and PostgreSQL, perform the overlap
    # calculation in Python rather than relying on database‑specific date
    # functions.  For each booking whose date range intersects the selected
    # period, clamp the booking's check‑in and check‑out to the period
    # boundaries and sum the resulting day counts.
    sold_nights = 0
    try:
        # Determine the end boundary as end_date + 1 day.  We treat the end
        # boundary as exclusive when computing nights.
        period_end_excl = end_date + timedelta(days=1)
        # Fetch all bookings that overlap the period.  Use simple range
        # comparison without named parameters so that the placeholder syntax is
        # translated correctly for PostgreSQL.  On SQLite the '?' markers are
        # accepted directly.  The compatibility layer will convert them to
        # '%s' when running on psycopg2.
        overlapping = conn.execute(
            "SELECT check_in_date, check_out_date FROM bookings WHERE check_out_date > ? AND check_in_date < ?",
            (start_date.isoformat(), period_end_excl.isoformat()),
        ).fetchall()
        for row in overlapping:
            # Support both dict‑like rows (from psycopg2 RealDictRow) and
            # tuple‑like rows (from sqlite3).  Retrieve by key if possible,
            # otherwise by position.
            try:
                check_in_str = row['check_in_date']  # type: ignore[index]
                check_out_str = row['check_out_date']  # type: ignore[index]
            except Exception:
                check_in_str = row[0]  # type: ignore[index]
                check_out_str = row[1]  # type: ignore[index]
            try:
                chk_in = date.fromisoformat(check_in_str)
                chk_out = date.fromisoformat(check_out_str)
            except Exception:
                # Skip rows with invalid dates
                continue
            # Compute the overlap interval [overlap_start, overlap_end_excl)
            # The booking occupies nights from chk_in up to but not including
            # chk_out.  Clamp the start and end to the selected period.
            overlap_start = max(chk_in, start_date)
            overlap_end_excl = min(chk_out, period_end_excl)
            nights = (overlap_end_excl - overlap_start).days
            if nights > 0:
                sold_nights += nights
    except Exception:
        # If any error occurs (e.g. missing table), leave sold_nights as 0.
        sold_nights = 0
    # Available nights: rooms_count * number of days in the period
    nights_available = rooms_count * period_days
    occupancy = (sold_nights / nights_available * 100) if nights_available > 0 else 0
    # Total revenue from bookings overlapping the period
    total_revenue_row = conn.execute(
        """
        SELECT COALESCE(SUM(b.total_amount), 0) AS total_rev
        FROM bookings AS b
        WHERE b.check_out_date > :start_date
          AND b.check_in_date < :end_plus
        """,
        {
            'start_date': start_date.isoformat(),
            'end_plus': (end_date + timedelta(days=1)).isoformat(),
        }
    ).fetchone()
    total_revenue = total_revenue_row['total_rev'] if total_revenue_row else 0.0
    # ADR: average revenue per sold night
    adr = (total_revenue / sold_nights) if sold_nights > 0 else 0
    # RevPAR: revenue per available room
    revpar = (total_revenue / nights_available) if nights_available > 0 else 0
    # Check‑ins and check‑outs for start_date; these metrics are useful for today
    check_in_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM bookings WHERE check_in_date = :date",
        {'date': start_date.isoformat()}
    ).fetchone()['cnt']
    check_out_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM bookings WHERE check_out_date = :date",
        {'date': start_date.isoformat()}
    ).fetchone()['cnt']
    # Deposit statuses within period (counts)
    deposit_rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM bookings
        WHERE paid_amount IS NOT NULL AND paid_amount > 0
          AND check_out_date > :start_date AND check_in_date < :end_plus
        GROUP BY status
        """,
        {
            'start_date': start_date.isoformat(),
            'end_plus': (end_date + timedelta(days=1)).isoformat(),
        }
    ).fetchall()
    deposit_counts = {row['status']: row['count'] for row in deposit_rows}
    # Build pick‑up data for the next 14 days (starting from start_date)
    pickup_dates = []
    pickup_occ = []
    pickup_revenue = []
    for i in range(14):
        day = start_date + timedelta(days=i)
        next_day = day + timedelta(days=1)
        pickup_dates.append(day.strftime('%d.%m'))
        # Compute nights sold for this day by scanning bookings.  A night is sold
        # if a booking covers the date (check_in_date <= day < check_out_date).
        sold_for_day = 0
        revenue_for_day = 0.0
        try:
            # Fetch all bookings that could overlap this day once per iteration
            day_rows = conn.execute(
                "SELECT check_in_date, check_out_date, total_amount FROM bookings WHERE check_in_date <= ? AND check_out_date > ?",
                (day.isoformat(), day.isoformat())
            ).fetchall()
        except Exception:
            day_rows = []
        for row in day_rows:
            # Support both dict-like and tuple rows
            try:
                ci = row['check_in_date']
                co = row['check_out_date']
                total_amt = row['total_amount']
            except Exception:
                ci = row[0]
                co = row[1]
                total_amt = row[2]
            try:
                ci_date = date.fromisoformat(ci)
                co_date = date.fromisoformat(co)
            except Exception:
                continue
            if ci_date <= day < co_date:
                sold_for_day += 1
                # Allocate revenue proportionally by nights
                nights_total = (co_date - ci_date).days
                if nights_total > 0 and total_amt is not None:
                    revenue_for_day += float(total_amt) / nights_total
        occ_for_day = (sold_for_day / rooms_count * 100) if rooms_count > 0 else 0
        pickup_occ.append(round(occ_for_day, 2))
        pickup_revenue.append(round(revenue_for_day, 2))
    # -------------------------------------------------------------------------
    # Per-room statistics: compute nights sold, revenue and booking count in Python.
    room_stats = []
    # Fetch bookings overlapping the selected period
    try:
        all_bookings = conn.execute(
            "SELECT room_id, check_in_date, check_out_date, total_amount FROM bookings WHERE check_out_date > ? AND check_in_date < ?",
            (start_date.isoformat(), (end_date + timedelta(days=1)).isoformat())
        ).fetchall()
    except Exception:
        all_bookings = []
    # Initialize aggregates by room
    agg_by_room: dict = {}
    for row in all_bookings:
        # Extract fields from either dict-like or tuple rows
        try:
            room_id = row['room_id']
            ci = row['check_in_date']
            co = row['check_out_date']
            total_amt = row['total_amount']
        except Exception:
            room_id = row[0]
            ci = row[1]
            co = row[2]
            total_amt = row[3]
        try:
            ci_date = date.fromisoformat(ci)
            co_date = date.fromisoformat(co)
        except Exception:
            continue
        # Compute overlap with selected period [start_date, end_date + 1)
        overlap_start = max(ci_date, start_date)
        overlap_end = min(co_date, end_date + timedelta(days=1))
        nights_overlap = (overlap_end - overlap_start).days
        if nights_overlap <= 0:
            continue
        if room_id not in agg_by_room:
            agg_by_room[room_id] = {
                'nights_sold': 0,
                'revenue': 0.0,
                'booking_count': 0,
            }
        agg = agg_by_room[room_id]
        agg['nights_sold'] += nights_overlap
        agg['booking_count'] += 1
        nights_total = (co_date - ci_date).days
        if nights_total > 0 and total_amt is not None:
            agg['revenue'] += float(total_amt) / nights_total * nights_overlap
    # Deposit counts by room and status (keep original SQL; no JULIANDAY)
    deposit_by_room_rows = conn.execute(
        """
        SELECT room_id, status, COUNT(*) AS count
        FROM bookings
        WHERE paid_amount IS NOT NULL AND paid_amount > 0
          AND check_out_date > ? AND check_in_date < ?
        GROUP BY room_id, status
        """,
        (start_date.isoformat(), (end_date + timedelta(days=1)).isoformat())
    ).fetchall()
    deposit_by_room: dict = {}
    for row in deposit_by_room_rows:
        try:
            rid = row['room_id']
            status = row['status']
            cnt = row['count']
        except Exception:
            rid = row[0]
            status = row[1]
            cnt = row[2]
        if rid not in deposit_by_room:
            deposit_by_room[rid] = {}
        deposit_by_room[rid][status] = cnt
    # Fetch all rooms to ensure rooms with no bookings are included
    room_list = conn.execute(
        "SELECT id, room_number FROM rooms ORDER BY room_number"
    ).fetchall()
    conn.close()
    for r in room_list:
        try:
            rid = r['id']
            room_number = r['room_number']
        except Exception:
            rid = r[0]
            room_number = r[1]
        agg = agg_by_room.get(rid)
        nights_sold_r = agg['nights_sold'] if agg else 0
        revenue_r = float(agg['revenue']) if agg else 0.0
        booking_count_r = agg['booking_count'] if agg else 0
        nights_available_r = period_days  # each room available each day
        empty_nights_r = nights_available_r - nights_sold_r
        occupancy_r = (nights_sold_r / nights_available_r * 100) if nights_available_r > 0 else 0
        adr_r = (revenue_r / nights_sold_r) if nights_sold_r > 0 else 0
        deposit_counts_r = deposit_by_room.get(rid, {})
        room_stats.append({
            'room_number': room_number,
            'nights_sold': nights_sold_r,
            'empty_nights': empty_nights_r,
            'occupancy': occupancy_r,
            'revenue': revenue_r,
            'adr': adr_r,
            'booking_count': booking_count_r,
            'deposit_counts': deposit_counts_r,
        })
    # Render dashboard with both overall metrics and per‑room stats
    return render_template(
        'dashboard.html',
        start_date=start_date,
        end_date=end_date,
        period=period_sel,
        occupancy=occupancy,
        sold_nights=sold_nights,
        nights_available=nights_available,
        adr=adr,
        revpar=revpar,
        total_revenue=total_revenue,
        check_in_count=check_in_count,
        check_out_count=check_out_count,
        deposit_counts=deposit_counts,
        pickup_dates=pickup_dates,
        pickup_occ=pickup_occ,
        pickup_revenue=pickup_revenue,
        room_stats=room_stats,
    )


# --------------------------------------------------
# Проверка гостя по номеру телефона

@app.route("/verify_guest")
@login_required
@roles_required('owner', 'manager')
def verify_guest():
    """
    Проверка гостя по номеру телефона. Принимает GET‑параметры
    ``country_code`` и ``phone``, формирует полный номер (без
    разделителей) и ищет гостя в базе. Если гость найден, на странице
    выводится информация о нём и все его бронирования. Если не
    найден, пользователь увидит соответствующее сообщение.
    """
    import re
    # Получаем код страны и введённый номер
    country_code = (request.args.get('country_code') or '').strip()
    phone_raw = (request.args.get('phone') or '').strip()
    # Удаляем все символы, кроме цифр
    code_digits = re.sub(r'\D', '', country_code)
    phone_digits = re.sub(r'\D', '', phone_raw)
    # Полный набор цифр для сравнения
    full_digits = code_digits + phone_digits
    conn = get_db_connection()
    # Ищем гостя, у которого после удаления всех нецифровых символов
    # телефон совпадает с full_digits
    target_guest = None
    guests = conn.execute("SELECT * FROM guests").fetchall()
    for g in guests:
        phone_db = g['phone'] or ''
        sanitized_db = re.sub(r'\D', '', phone_db)
        # Consider several matching strategies:
        # 1) Exact match to full digits (country code + number)
        # 2) Exact match to just the subscriber number (without country code)
        # 3) DB number ends with the subscriber number (to handle numbers stored without country code)
        if not phone_digits:
            continue
        if (
            sanitized_db == full_digits
            or sanitized_db == phone_digits
            or sanitized_db.endswith(phone_digits)
        ):
            target_guest = g
            break
    bookings = []
    if target_guest:
        # Получаем все бронирования этого гостя, включая номер квартиры
        raw_bookings = conn.execute(
            """
            SELECT b.*, r.room_number
            FROM bookings AS b
            JOIN rooms AS r ON b.room_id = r.id
            WHERE b.guest_id = ?
            ORDER BY b.check_in_date DESC
            """,
            (target_guest['id'],),
        ).fetchall()
        for b in raw_bookings:
            booking_dict = {key: b[key] for key in b.keys()}
            try:
                nights = (date.fromisoformat(b['check_out_date']) - date.fromisoformat(b['check_in_date'])).days
                booking_dict['nights'] = nights if nights >= 0 else 0
            except Exception:
                booking_dict['nights'] = None
            bookings.append(booking_dict)
    conn.close()
    # Формируем номер с ведущим знаком плюс для отображения
    full_phone = '+' + full_digits if full_digits else None
    return render_template('guest_info.html', guest=target_guest, bookings=bookings, full_phone=full_phone)


# Add booking
@app.route("/bookings/add", methods=["GET", "POST"])
@login_required
@roles_required('owner', 'manager')
def add_booking():
    """Create a new booking. Accepts optional scroll_date to return to calendar after creation."""
    conn = get_db_connection()
    # Ensure extra_phone column exists before any operations
    ensure_extra_phone_column(conn)
    rooms = conn.execute("SELECT id, room_number FROM rooms ORDER BY room_number").fetchall()
    # Determine scroll_date and optional guest_id from query or form for returning to calendar
    scroll_date = request.args.get("scroll_date") or request.form.get("scroll_date")
    # Pre-calculate the year and month from scroll_date so the calendar can be restored after adding/canceling
    scroll_year = None
    scroll_month = None
    if scroll_date:
        try:
            sd_tmp = date.fromisoformat(scroll_date)
            scroll_year = sd_tmp.year
            scroll_month = sd_tmp.month
        except Exception:
            pass
    provided_guest_id = request.args.get("guest_id") or request.form.get("guest_id")
    if request.method == "POST":
        # Handle form submission: create a booking, optionally creating a new guest
        import re
        # Validate room selection
        try:
            room_id = int(request.form.get("room_id"))
        except (TypeError, ValueError):
            flash("Выберите квартиру.")
            conn.close()
            return redirect(url_for("add_booking", scroll_date=scroll_date, guest_id=provided_guest_id))
        # Determine guest to use (existing or new)
        guest_id = None
        if provided_guest_id:
            try:
                guest_id = int(provided_guest_id)
            except (TypeError, ValueError):
                guest_id = None
        if guest_id is None:
            # No existing guest_id was supplied. We will attempt to find an existing guest
            # based on the provided phone number before creating a new one. This prevents
            # duplicate guest entries when the same person makes multiple bookings.
            country_code = (request.form.get("country_code") or "").strip()
            phone_raw = (request.form.get("phone") or "").strip()
            guest_name_input = (request.form.get("guest_name") or "").strip()
            # Strip non-digit characters
            code_digits = re.sub(r"\D", "", country_code)
            phone_digits = re.sub(r"\D", "", phone_raw)
            # Ensure required phone data
            if not code_digits or not phone_digits:
                flash("Введите код страны и номер телефона.")
                conn.close()
                return redirect(url_for("add_booking", scroll_date=scroll_date))
            # International phone length between 7 and 15 digits
            full_digits = code_digits + phone_digits
            if len(full_digits) < 7 or len(full_digits) > 15:
                flash("Введите корректный номер телефона.")
                conn.close()
                return redirect(url_for("add_booking", scroll_date=scroll_date))
            # Determine if this phone corresponds to an existing guest
            phone_clean = '+' + full_digits
            # Look for a matching guest by sanitized phone digits. We compare the full digits,
            # the subscriber number without the country code, or any phone ending with the
            # subscriber number to catch numbers stored without country codes.
            try:
                existing_rows = conn.execute("SELECT id, phone FROM guests").fetchall()
            except Exception:
                existing_rows = []
            found_guest_id = None
            for row in existing_rows:
                db_phone = row["phone"] or ""
                sanitized_db = re.sub(r"\D", "", db_phone)
                if (
                    sanitized_db == full_digits
                    or sanitized_db == phone_digits
                    or sanitized_db.endswith(phone_digits)
                ):
                    found_guest_id = row["id"]
                    break
            if found_guest_id:
                # Use the existing guest instead of creating a new one
                guest_id = found_guest_id
                # If the user provided an extra phone number, update it
                extra_phone_input = (request.form.get("extra_phone") or "").strip()
                if extra_phone_input:
                    try:
                        with get_db_connection() as upd_conn:
                            ensure_extra_phone_column(upd_conn)
                            upd_conn.execute(
                                "UPDATE guests SET extra_phone = ? WHERE id = ?",
                                (extra_phone_input, guest_id),
                            )
                    except Exception:
                        pass
            else:
                # No existing guest found; create a new one.  Do **not** use
                # a context manager on the existing connection here because
                # ``with conn`` will close the connection upon exit via
                # SQLiteCompatConnection.__exit__.  Closing the connection at
                # this point would render it unusable for subsequent booking
                # insertion operations later in this function.  Instead,
                # insert the guest using the existing open connection and
                # rely on autocommit (enabled on PostgreSQL) or call commit
                # explicitly if supported.  See issue where using ``with
                # conn`` here caused a "connection already closed" error when
                # saving a booking.
                new_guest_name = guest_name_input if guest_name_input else phone_clean
                extra_phone_input = (request.form.get("extra_phone") or "").strip()
                cur = conn.execute(
                    "INSERT INTO guests (name, phone, extra_phone, email, notes) VALUES (?, ?, ?, ?, ?)",
                    (new_guest_name, phone_clean, extra_phone_input or None, None, None),
                )
                # Capture the new guest ID from the cursor.  When using
                # PostgreSQL, our SQLiteCompatCursor will extract the value
                # from the RETURNING clause automatically.  On SQLite,
                # lastrowid provides the inserted row's ID.
                guest_id = cur.lastrowid
                # Explicitly commit if supported.  On PostgreSQL this is a
                # no‑op when autocommit is enabled, but on SQLite it
                # persists the change.
                try:
                    conn.commit()
                except Exception:
                    pass
        # If booking is for an existing guest, update extra_phone if provided
        if guest_id is not None and provided_guest_id:
            extra_phone_input = (request.form.get("extra_phone") or "").strip()
            if extra_phone_input:
                try:
                    with get_db_connection() as upd_conn:
                        ensure_extra_phone_column(upd_conn)
                        upd_conn.execute(
                            "UPDATE guests SET extra_phone = ? WHERE id = ?",
                            (extra_phone_input, guest_id),
                        )
                except Exception:
                    pass
        # Retrieve and validate dates
        check_in = request.form.get("check_in")
        check_out = request.form.get("check_out")
        if not check_in or not check_out:
            flash("Заполните даты заезда и выезда.")
            conn.close()
            return redirect(url_for("add_booking", scroll_date=scroll_date, guest_id=guest_id))
        try:
            nights = (date.fromisoformat(check_out) - date.fromisoformat(check_in)).days
        except ValueError:
            flash("Неверный формат даты.")
            conn.close()
            return redirect(url_for("add_booking", scroll_date=scroll_date, guest_id=guest_id))
        if nights < 0:
            flash("Дата выезда должна быть позже даты заезда.")
            conn.close()
            return redirect(url_for("add_booking", scroll_date=scroll_date, guest_id=guest_id))
        # Deposit status and monetary fields
        deposit_status = (request.form.get("deposit_status") or "paid").strip() or "paid"
        rate_str = (request.form.get("total_amount") or "").strip()
        deposit_amount_str = (request.form.get("paid_amount") or "").strip()
        notes = (request.form.get("notes") or "").strip() or None
        rate = float(rate_str) if rate_str else None
        deposit_amount = float(deposit_amount_str) if deposit_amount_str else None
        # Calculate total amount
        total_amount = (rate * nights) if (rate is not None) else None
        # Insert booking into database. After insertion, automatically mark the room
        # as booked for each date in the reservation range.  Avoid using
        # ``with conn`` on the existing connection because that will close
        # the connection and cause subsequent operations to fail.  Instead,
        # execute the insert and status updates directly and commit at the
        # end.  Autocommit is enabled on PostgreSQL, but an explicit commit
        # call is harmless and ensures SQLite persists the changes.
        # Insert booking record.  Record the ID of the user who created it via
        # session['user_id'] in the created_by column.
        conn.execute(
            """
            INSERT INTO bookings (
                guest_id, room_id, check_in_date, check_out_date,
                status, total_amount, paid_amount, notes, created_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guest_id,
                room_id,
                check_in,
                check_out,
                deposit_status,
                total_amount,
                deposit_amount,
                notes,
                session.get('user_id')  # record which user created this booking
            ),
        )
        # Automatically update the room_statuses table to mark the room as
        # booked for every date spanned by this booking (inclusive). This
        # prevents stale statuses (e.g. "ready" or "vacant") from
        # overriding the booking on the calendar.
        # Ensure the status table exists before updating
        ensure_status_table(conn)
        try:
            start_dt = date.fromisoformat(check_in)
            end_dt = date.fromisoformat(check_out)
        except Exception:
            start_dt = None
            end_dt = None
        if start_dt and end_dt:
            # Mark each date of the booking range as booked, excluding the check‑out day.
            # If check_in and check_out are the same day (zero nights), mark that single date.
            if start_dt >= end_dt:
                conn.execute(
                    """
                    INSERT INTO room_statuses (room_id, date, status)
                    VALUES (?, ?, 'booked')
                    ON CONFLICT(room_id, date) DO UPDATE SET status = 'booked'
                    """,
                    (room_id, start_dt.isoformat()),
                )
            else:
                current_dt = start_dt
                while current_dt < end_dt:
                    conn.execute(
                        """
                        INSERT INTO room_statuses (room_id, date, status)
                        VALUES (?, ?, 'booked')
                        ON CONFLICT(room_id, date) DO UPDATE SET status = 'booked'
                        """,
                        (room_id, current_dt.isoformat()),
                    )
                    current_dt += timedelta(days=1)
        # Commit the transaction explicitly.  This is a no‑op when autocommit
        # is enabled on PostgreSQL but ensures persistence on SQLite.
        try:
            conn.commit()
        except Exception:
            pass
        # Close the connection now that all operations on it are complete
        conn.close()
        flash("Бронирование создано успешно!")
        # After creating the booking, redirect back to the calendar if scroll_date
        # was provided so the user sees the updated booking in context. If no
        # scroll_date is present, redirect to the bookings list as before.
        if scroll_date:
            try:
                sd = date.fromisoformat(scroll_date)
                return redirect(url_for("calendar_view", year=sd.year, month=sd.month, scroll_date=scroll_date))
            except ValueError:
                pass
        return redirect(url_for("list_bookings"))
    # Handle GET: determine defaults and blacklisted numbers
    selected_room_id = request.args.get("room_id", type=int)
    default_check_in = request.args.get("check_in")
    default_check_out = request.args.get("check_out")
    conn.close()
    # Fetch blacklisted numbers (sanitized digits) for phone validation
    bconn = get_db_connection()
    ensure_blacklist_table(bconn)
    black_rows = bconn.execute("SELECT phone FROM blacklist").fetchall()
    # Prepare list of digits for blacklist
    blacklisted_numbers = [row["phone"] for row in black_rows]
    # Fetch existing bookings for provided_guest_id, if any
    existing_bookings = []
    guest_name = None
    if provided_guest_id:
        try:
            gid = int(provided_guest_id)
            # Retrieve guest's name for display
            g_row = bconn.execute("SELECT name FROM guests WHERE id = ?", (gid,)).fetchone()
            if g_row:
                guest_name = g_row['name']
            raw_b = bconn.execute(
                """
                SELECT b.*, r.room_number
                FROM bookings AS b
                JOIN rooms AS r ON b.room_id = r.id
                WHERE b.guest_id = ?
                ORDER BY b.check_in_date DESC
                """,
                (gid,),
            ).fetchall()
            for b in raw_b:
                bd = {key: b[key] for key in b.keys()}
                try:
                    nights = (date.fromisoformat(b['check_out_date']) - date.fromisoformat(b['check_in_date'])).days
                    bd['nights'] = nights if nights >= 0 else 0
                except Exception:
                    bd['nights'] = None
                existing_bookings.append(bd)
        except Exception:
            pass
    bconn.close()
    # Build autocomplete data for names and phones. This will be used by the
    # booking_form template to auto-fill phone or name when a match is found.
    data_conn = get_db_connection()
    ensure_extra_phone_column(data_conn)
    guest_rows = data_conn.execute(
        "SELECT id, name, phone, extra_phone FROM guests WHERE (phone IS NOT NULL AND TRIM(phone) != '') OR (extra_phone IS NOT NULL AND TRIM(extra_phone) != '')"
    ).fetchall()
    guest_autocomplete_data = []
    for row in guest_rows:
        name = row["name"]
        # Primary phone
        phone = row["phone"]
        if phone:
            guest_autocomplete_data.append({"name": name, "phone": phone})
        # Additional phone
        extra_phone = row.get("extra_phone") if hasattr(row, "get") else row["extra_phone"]
        if extra_phone:
            guest_autocomplete_data.append({"name": name, "phone": extra_phone})
    data_conn.close()
    # Render booking form; pass guest_id and existing bookings to preserve context
    return render_template(
        "booking_form.html",
        rooms=rooms,
        selected_room_id=selected_room_id,
        default_check_in=default_check_in,
        default_check_out=default_check_out,
        scroll_date=scroll_date,
        scroll_year=scroll_year,
        scroll_month=scroll_month,
        blacklisted_numbers=blacklisted_numbers,
        guest_id=provided_guest_id,
        existing_bookings=existing_bookings,
        guest_name=guest_name,
        guest_autocomplete_data=guest_autocomplete_data,
    )


# Removed legacy dashboard route. The new dashboard with advanced metrics and charts
# is defined earlier in this file. Keeping duplicate route definitions may lead to
# undefined variables in templates. This stub has been removed.

# Set or update a custom status for a room on a specific date
# Allow any authenticated user to change the occupancy status of a room on the calendar.
# Previously this endpoint required the user to have either the 'owner' or 'manager' role,
# which prevented other staff (e.g. receptionists or maids) from updating the room's status.
# Removing the roles_required decorator leaves only @login_required, so anyone who is logged
# in can freely update statuses.  If you need more granular control, adjust the
# ``roles_required`` arguments accordingly (e.g., include 'maid' or 'employee').
@app.route("/calendar/status/<int:room_id>/<date_str>", methods=["GET", "POST"])
@login_required
# @roles_required('owner', 'manager')  # removed to allow all authenticated users
def set_room_status(room_id, date_str):
    """Display or update status for a room on a particular date."""
    conn = get_db_connection()
    ensure_status_table(conn)
    # Validate date format
    try:
        date_obj = date.fromisoformat(date_str)
    except ValueError:
        conn.close()
        flash("Неверная дата.")
        return redirect(url_for("calendar_view"))
    if request.method == "POST":
        # Retrieve the desired status from the form. Default to "ready" if none provided.
        status = request.form.get("status", "ready").strip()
        # Determine which dates should be updated.  If the selected date belongs to a booking,
        # update the entire booking range (nights), otherwise only the selected date.
        dates_to_update: set[str] = set()
        try:
            # Fetch bookings overlapping the specified date.  Use plain comparisons on
            # text columns (ISO formatted dates) to remain portable across SQLite and
            # PostgreSQL.
            bookings = conn.execute(
                """
                SELECT check_in_date, check_out_date
                FROM bookings
                WHERE room_id = ?
                  AND date(check_in_date) <= date(?)
                  AND date(check_out_date) >= date(?)
                """,
                (room_id, date_str, date_str),
            ).fetchall()
        except Exception:
            bookings = []
        # If the date overlaps one or more bookings, collect all occupied nights
        if bookings:
            for b in bookings:
                try:
                    start = date.fromisoformat(b["check_in_date"])
                    end = date.fromisoformat(b["check_out_date"])
                except Exception:
                    continue
                # A zero‑night stay (check_in >= check_out) occupies the single day; otherwise
                # mark each night from start inclusive up to (but excluding) end.
                if start and end:
                    if start >= end:
                        dates_to_update.add(start.isoformat())
                    else:
                        current = start
                        while current < end:
                            dates_to_update.add(current.isoformat())
                            current += timedelta(days=1)
        else:
            # No overlapping booking: update just the selected date
            dates_to_update.add(date_str)
        # Upsert the status for each target date.  Avoid using a context manager on the
        # connection to prevent the wrapper from closing the connection prematurely.
        for dstr in dates_to_update:
            try:
                conn.execute(
                    """
                    INSERT INTO room_statuses (room_id, date, status)
                    VALUES (?, ?, ?)
                    ON CONFLICT(room_id, date) DO UPDATE SET status = excluded.status
                    """,
                    (room_id, dstr, status),
                )
            except Exception:
                # Ignore individual errors to prevent a partial failure from aborting all updates
                pass

        # If the chosen status represents an occupancy state (e.g., 'occupied', 'vacant', etc.),
        # also update the booking's own status for any bookings that overlap the selected date.
        # Without this, bookings retain the value 'booked', 'paid', etc., which causes the
        # calendar to continue displaying the old status.  Updating the booking ensures that
        # the occupancy state persists across page reloads.  Deposit-related statuses (paid,
        # withheld, returned) can still be managed separately via the booking edit form.
        occupancy_states = {"occupied", "vacant", "booked", "ready", "cleaning", "hourly"}
        if status in occupancy_states:
            try:
                conn.execute(
                    """
                    UPDATE bookings
                    SET status = ?
                    WHERE room_id = ?
                      AND date(check_in_date) <= date(?)
                      AND date(check_out_date) >= date(?)
                    """,
                    (status, room_id, date_str, date_str),
                )
            except Exception:
                pass
        # Explicitly commit the transaction.  On PostgreSQL this is a no‑op when
        # autocommit is enabled, but on SQLite it ensures persistence.
        try:
            conn.commit()
        except Exception:
            pass
        # Close the connection now that we are finished
        conn.close()
        flash("Статус обновлён.")
        # Redirect back to the calendar for the month/year of the selected date
        return redirect(url_for(
            "calendar_view",
            year=date_obj.year,
            month=date_obj.month,
            scroll_date=date_obj.isoformat(),
        ))
    # GET: fetch current status if exists
    cur_status = conn.execute(
        "SELECT status FROM room_statuses WHERE room_id = ? AND date = ?",
        (room_id, date_str),
    ).fetchone()
    conn.close()
    current_status = cur_status["status"] if cur_status else None
    # List of status options
    status_options = [
        ("occupied", "Заселено"),
        ("vacant", "Не заселено"),
        ("booked", "Бронь"),
        ("ready", "Готова"),
        ("cleaning", "Уборка"),
        ("hourly", "Часовики"),
    ]
    # Extract year and month for navigation/back links
    year = date_obj.year
    month = date_obj.month
    return render_template(
        "set_status.html",
        room_id=room_id,
        date=date_str,
        current_status=current_status,
        status_options=status_options,
        year=year,
        month=month,
    )

# Edit booking
@app.route("/bookings/edit/<int:booking_id>", methods=["GET", "POST"])
@login_required
@roles_required('owner', 'manager')
def edit_booking(booking_id):
    """Edit an existing booking record.
    Accepts optional scroll_date to return to calendar near the date of the booking."""
    conn = get_db_connection()
    # Fetch current booking
    booking = conn.execute(
        "SELECT * FROM bookings WHERE id = ?",
        (booking_id,)
    ).fetchone()
    if not booking:
        conn.close()
        flash("Бронирование не найдено.")
        return redirect(url_for("list_bookings"))
    guests = conn.execute("SELECT id, name FROM guests ORDER BY name").fetchall()
    rooms = conn.execute("SELECT id, room_number FROM rooms ORDER BY room_number").fetchall()
    # Fetch details for the currently booked guest to prepopulate the name and phone fields.
    # If the guest table has phone and extra_phone columns, retrieve them. Use empty strings if missing.
    guest_name = ''
    guest_phone_full = ''
    guest_extra_phone = ''
    guest_country_code = ''
    guest_phone_digits = ''
    try:
        guest = conn.execute(
            "SELECT name, phone, extra_phone FROM guests WHERE id = ?",
            (booking["guest_id"],),
        ).fetchone()
        if guest:
            guest_name = guest["name"] or ''
            guest_phone_full = guest["phone"] or ''
            guest_extra_phone = guest["extra_phone"] or ''
            # Extract only digits from the phone
            import re as _re
            digits_only = _re.sub(r"\D", "", guest_phone_full or '')
            # Attempt to split into country code (1-3 digits) and subscriber number
            # Assume phone numbers start with country code; we attempt longest known codes (3 digits), then 2, then 1
            country_code_digits = ''
            phone_digits = ''
            if digits_only:
                # Determine the code by trying known lengths
                # Start from 3 digits down to 1
                for l in (3, 2, 1):
                    if len(digits_only) > l:
                        possible_code = digits_only[:l]
                        # Use possible code as country code; break at first match
                        country_code_digits = possible_code
                        phone_digits = digits_only[l:]
                        break
                if not country_code_digits:
                    country_code_digits = digits_only
                    phone_digits = ''
                guest_country_code = '+' + country_code_digits
                guest_phone_digits = phone_digits
            # Format the phone for display based on common masks
            def _format_phone(code_digits: str, number: str) -> str:
                """Format a subscriber number using common grouping rules based on the country code."""
                groups: list[int] = []
                if code_digits == '7':
                    groups = [3, 3, 2, 2]
                elif code_digits == '1':
                    groups = [3, 3, 4]
                elif code_digits in ('44', '49'):
                    groups = [4, 3, 4]
                elif code_digits == '81':
                    groups = [4, 3, 3]
                else:
                    # Fallback: group remaining digits in chunks of up to 3
                    remaining = len(number)
                    while remaining > 0:
                        chunk = 3 if remaining >= 3 else remaining
                        groups.append(chunk)
                        remaining -= chunk
                num = number or ''
                idx = 0
                parts: list[str] = []
                for g in groups:
                    part = num[idx: idx + g]
                    if not part:
                        break
                    parts.append(part)
                    idx += g
                if not parts:
                    return ''
                first = parts[0]
                formatted = f"({first})"
                rest = parts[1:]
                if rest:
                    if code_digits == '1' and len(rest) == 2:
                        # US numbers: hyphen between last two groups
                        formatted += f" {rest[0]}-{rest[1]}"
                    else:
                        formatted += ' ' + ' '.join(rest)
                return formatted
            # Pre-format phone for display (used to initialise input field). If digits only, compute formatted.
            phone_display = ''
            if guest_country_code and guest_phone_digits:
                phone_display = _format_phone(guest_country_code.lstrip('+'), guest_phone_digits)
    except Exception:
        # If any error occurs, leave the variables empty
        pass

    # Determine the name of the manager who created this booking (if any).  We
    # retrieve the first and last names from the users table; if both are
    # missing or blank, we fall back to the username.  If the booking has no
    # created_by value, manager_name remains None.
    manager_name = None
    try:
        creator_id = booking.get('created_by') if isinstance(booking, dict) else booking['created_by']
    except Exception:
        creator_id = None
    if creator_id:
        try:
            row = conn.execute(
                "SELECT first_name, last_name, username FROM users WHERE id = ?",
                (creator_id,),
            ).fetchone()
            if row:
                first = (row['first_name'] or '').strip() if row['first_name'] is not None else ''
                last = (row['last_name'] or '').strip() if row['last_name'] is not None else ''
                if first or last:
                    # Compose a display name using available first and last names
                    manager_name = (first + ' ' + last).strip()
                else:
                    manager_name = row['username']
        except Exception:
            manager_name = None
    # Determine rate (price per night) and total price for display.
    rate_value = None
    total_price_value = None
    try:
        # Compute nights between check_in and check_out
        nights = (date.fromisoformat(booking['check_out_date']) - date.fromisoformat(booking['check_in_date'])).days
        if nights <= 0:
            nights = 1
        total_price_value = booking['total_amount']
        if total_price_value is not None:
            rate_value = total_price_value / nights
    except Exception:
        rate_value = booking['total_amount']
        total_price_value = booking['total_amount']
    # Determine scroll_date from query or form
    scroll_date = request.args.get("scroll_date") or request.form.get("scroll_date")
    # Pre-calculate scroll_year and scroll_month for returning to calendar after editing
    scroll_year = None
    scroll_month = None
    if scroll_date:
        try:
            sd_tmp = date.fromisoformat(scroll_date)
            scroll_year = sd_tmp.year
            scroll_month = sd_tmp.month
        except Exception:
            pass
    if request.method == "POST":
        try:
            guest_id = int(request.form.get("guest_id"))
            room_id = int(request.form.get("room_id"))
        except (TypeError, ValueError):
            flash("Пожалуйста, выберите гостя и квартиру.")
            conn.close()
            return redirect(url_for("edit_booking", booking_id=booking_id, scroll_date=scroll_date))
        # Дополнительный телефон может быть пустым; обновим запись гостя, если поле заполнено.
        extra_phone = (request.form.get("extra_phone") or "").strip()
        check_in = request.form.get("check_in")
        check_out = request.form.get("check_out")
        # Статус депозита: заменяем поле status на deposit_status; по умолчанию "paid"
        deposit_status = (request.form.get("deposit_status") or "paid").strip() or "paid"
        total_amount_str = request.form.get("total_amount", "").strip()
        paid_amount_str = request.form.get("paid_amount", "").strip()
        notes = request.form.get("notes", "").strip() or None
        total_amount = float(total_amount_str) if total_amount_str else None
        paid_amount = float(paid_amount_str) if paid_amount_str else None
        if not check_in or not check_out:
            flash("Заполните даты заезда и выезда.")
            conn.close()
            return redirect(url_for("edit_booking", booking_id=booking_id, scroll_date=scroll_date))
        with conn:
            conn.execute(
                """
                UPDATE bookings
                SET guest_id = ?, room_id = ?, check_in_date = ?, check_out_date = ?,
                    status = ?, total_amount = ?, paid_amount = ?, notes = ?
                WHERE id = ?
                """,
                (
                    guest_id,
                    room_id,
                    check_in,
                    check_out,
                    deposit_status,
                    total_amount,
                    paid_amount,
                    notes,
                    booking_id,
                ),
            )
            # Если дополнительный телефон указан, обновляем его у гостя
            if extra_phone:
                try:
                    ensure_extra_phone_column(conn)
                    conn.execute(
                        "UPDATE guests SET extra_phone = ? WHERE id = ?",
                        (extra_phone, guest_id),
                    )
                except Exception:
                    pass
            # After updating the booking, ensure the room statuses reflect the
            # updated reservation range.  We only mark dates as "booked" when there
            # is no existing custom status for the date.  This prevents overriding
            # statuses that managers have manually set (e.g. "occupied").
            ensure_status_table(conn)
            try:
                new_start_dt = date.fromisoformat(check_in)
                new_end_dt = date.fromisoformat(check_out)
            except Exception:
                new_start_dt = None
                new_end_dt = None
            if new_start_dt and new_end_dt:
                def _mark_booked(d: date):
                    try:
                        # Attempt to insert a row only if it does not already exist.
                        conn.execute(
                            """
                            INSERT INTO room_statuses (room_id, date, status)
                            VALUES (?, ?, 'booked')
                            ON CONFLICT(room_id, date) DO NOTHING
                            """,
                            (room_id, d.isoformat()),
                        )
                    except Exception:
                        pass
                if new_start_dt >= new_end_dt:
                    _mark_booked(new_start_dt)
                else:
                    current_dt = new_start_dt
                    while current_dt < new_end_dt:
                        _mark_booked(current_dt)
                        current_dt += timedelta(days=1)
        conn.close()
        flash("Бронирование обновлено успешно!")
        # Redirect back to calendar if scroll_date provided
        if scroll_date:
            try:
                sd = date.fromisoformat(scroll_date)
                return redirect(url_for("calendar_view", year=sd.year, month=sd.month, scroll_date=scroll_date))
            except ValueError:
                pass
        return redirect(url_for("list_bookings"))
    # GET request
    conn.close()
    return render_template(
        "booking_edit_form.html",
        booking=booking,
        guests=guests,
        rooms=rooms,
        scroll_date=scroll_date,
        scroll_year=scroll_year,
        scroll_month=scroll_month,
        guest_name=guest_name,
        guest_country_code=guest_country_code,
        guest_phone_digits=guest_phone_digits,
        phone_display=phone_display,
        guest_extra_phone=guest_extra_phone,
        selected_room_id=booking["room_id"],
        rate_value=rate_value,
        total_price_value=total_price_value,
        manager_name=manager_name,
    )


# Delete booking
@app.route("/bookings/delete/<int:booking_id>", methods=["POST"])
@login_required
@roles_required('owner', 'manager')
def delete_booking(booking_id):
    """Delete a booking record and optionally return to the calendar near the original dates.

    This route accepts an optional ``scroll_date`` parameter via query string or form data
    to return the user back to the month in the calendar view they were previously looking at.

    On successful deletion, a flash message is shown and the user is redirected to either the
    bookings list or back to the calendar if a ``scroll_date`` is provided.
    """
    # Determine scroll_date from query or form for redirecting back to calendar
    scroll_date = request.args.get("scroll_date") or request.form.get("scroll_date")
    conn = get_db_connection()
    # Before deletion, fetch booking details to reset room statuses after removal.
    booking_row = conn.execute(
        "SELECT room_id, check_in_date, check_out_date FROM bookings WHERE id = ?",
        (booking_id,),
    ).fetchone()
    # Perform deletion and update statuses within one transaction.
    with conn:
        # If the booking exists, update the room statuses to "ready" for the occupied dates
        if booking_row:
            ensure_status_table(conn)
            try:
                # Convert stored strings to date objects. These values are used both for
                # clearing room statuses and for redirecting the user back to the
                # appropriate month in the calendar.
                start_dt = date.fromisoformat(booking_row["check_in_date"])
                end_dt = date.fromisoformat(booking_row["check_out_date"])
            except Exception:
                start_dt = None
                end_dt = None
            if start_dt and end_dt:
                if start_dt >= end_dt:
                    # Single‑day booking: mark just the start date as ready
                    conn.execute(
                        """
                        INSERT INTO room_statuses (room_id, date, status)
                        VALUES (?, ?, 'ready')
                        ON CONFLICT(room_id, date) DO UPDATE SET status = 'ready'
                        """,
                        (booking_row["room_id"], start_dt.isoformat()),
                    )
                else:
                    current_dt = start_dt
                    # Mark each date except the check‑out day as ready
                    while current_dt < end_dt:
                        conn.execute(
                            """
                            INSERT INTO room_statuses (room_id, date, status)
                            VALUES (?, ?, 'ready')
                            ON CONFLICT(room_id, date) DO UPDATE SET status = 'ready'
                            """,
                            (booking_row["room_id"], current_dt.isoformat()),
                        )
                        current_dt += timedelta(days=1)
        # Delete booking; cascade rules ensure related records like payments are removed
        conn.execute("DELETE FROM bookings WHERE id = ?", (booking_id,))
    conn.close()
    flash("Бронирование удалено успешно!")
    # Decide where to redirect after deletion. We always try to return to the calendar.
    # First, prefer the explicit scroll_date passed in the request (if any). If present
    # and valid, derive the year and month from it. If no scroll_date is provided, fall
    # back to the booking's original check‑in date. As a last resort, redirect to the
    # base calendar without parameters.
    target_date = None
    if scroll_date:
        try:
            target_date = date.fromisoformat(scroll_date)
        except Exception:
            target_date = None
    if target_date is None and booking_row and start_dt:
        # Use the check‑in date of the deleted booking for the calendar navigation
        target_date = start_dt
    if target_date:
        return redirect(url_for("calendar_view", year=target_date.year, month=target_date.month, scroll_date=target_date.isoformat()))
    # Default fallback: redirect to calendar view without specifying a date
    return redirect(url_for("calendar_view"))


# ---------------------------------------------------------------------------
# Employees (users) management
#
# This view displays all registered users in the system. It queries the
# 'users' table and passes the results to a dedicated template. Access is
# restricted by the global `require_login` hook, so only authenticated
# users can see this page. If finer‑grained role control is desired, an
# additional check on the current user's role (e.g. owner vs manager) could
# be implemented here.
@app.route("/employees", methods=['GET', 'POST'])
@login_required
@roles_required('owner', 'manager')
def list_employees():
    """Display and manage employees and pending registration requests.

    Owners and managers can view all registered employees. Owners may also
    approve or reject pending registration requests directly from this page.
    Registration request actions are handled via POST submissions to this
    endpoint. After processing, the page is reloaded to reflect the updated
    lists. """
    conn = get_db_connection()
    # Ensure registration requests table exists so queries won't fail
    ensure_registration_requests_table(conn)
    # If this is a POST and the current user is an owner, handle a registration request
    if request.method == 'POST' and session.get('user_role') == 'owner':
        req_id = request.form.get('request_id')
        action = request.form.get('action')
        if req_id:
            if action == 'approve':
                role = request.form.get('role') or 'maid'
                # Sanitize role
                if role not in ('owner', 'maid', 'manager'):
                    role = 'maid'
                # Fetch request
                cur_req = conn.execute('SELECT * FROM registration_requests WHERE id = ?', (req_id,)).fetchone()
                if cur_req and cur_req['status'] == 'pending':
                    # Insert new user and add them to the global chat
                    cur = conn.cursor()
                    cur.execute(
                        'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                        (cur_req['username'], cur_req['password_hash'], role)
                    )
                    new_user_id = cur.lastrowid
                    # Add to global chat room
                    ensure_chat_rooms_table(conn)
                    ensure_chat_room_members_table(conn)
                    try:
                        cur.execute(
                            'INSERT OR IGNORE INTO chat_room_members (room_id, user_id) VALUES (1, ?)',
                            (new_user_id,)
                        )
                    except Exception:
                        pass
                    # Mark request approved
                    conn.execute(
                        'UPDATE registration_requests SET status = ? WHERE id = ?',
                        ('approved', req_id)
                    )
                    conn.commit()
                    flash('Пользователь {} одобрен с ролью {}.'.format(cur_req['username'], role))
            elif action == 'reject':
                # Reject the request
                conn.execute(
                    'UPDATE registration_requests SET status = ? WHERE id = ?',
                    ('rejected', req_id)
                )
                conn.commit()
                flash('Запрос на регистрацию отклонён.')
        # After processing, redirect to this page to refresh lists
        conn.close()
        return redirect(url_for('list_employees'))
    # GET: fetch lists of employees and pending requests
    users = conn.execute(
        "SELECT id, username, role, name, contact_info FROM users ORDER BY id"
    ).fetchall()
    # Fetch pending registration requests only for owners
    reqs = []
    roles_list = []
    if session.get('user_role') == 'owner':
        reqs = conn.execute(
            'SELECT * FROM registration_requests WHERE status = ?',
            ('pending',)
        ).fetchall()
        roles_list = ['owner', 'maid', 'manager']
    conn.close()
    return render_template('employees.html', users=users, requests=reqs, roles=roles_list)

# Route to fire (delete) an employee. Only accessible by owner. Cannot fire oneself.
@app.route('/employees/<int:user_id>/fire', methods=['POST'])
@login_required
def fire_employee(user_id: int):
    # Only owners are allowed to fire employees
    if session.get('user_role') != 'owner':
        return abort(403)
    current_user_id = session.get('user_id')
    # Prevent owner from deleting their own account
    if user_id == current_user_id:
        flash('Вы не можете уволить самого себя.')
        return redirect(url_for('list_employees'))
    conn = get_db_connection()
    # Check if the user exists
    target = conn.execute('SELECT id, username FROM users WHERE id = ?', (user_id,)).fetchone()
    if not target:
        conn.close()
        flash('Сотрудник не найден.')
        return redirect(url_for('list_employees'))
    # Delete the user (cascade will remove chat messages)
    with conn:
        conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.close()
    flash('Сотрудник {} уволен.'.format(target['username']))
    return redirect(url_for('list_employees'))

# ---------------------------------------------------------------------------
# Change employee role
#
# This route allows the owner to change the role of an existing user.  Only
# owners have access and owners cannot change their own role to avoid locking
# themselves out.  The new role must be one of the allowed roles defined in
# the system.
@app.route('/employees/<int:user_id>/update_role', methods=['POST'])
@login_required
def update_employee_role(user_id: int):
    """Update the role of a given employee. Accessible only to owners."""
    # Only owners can change roles
    if session.get('user_role') != 'owner':
        return abort(403)
    current_user_id = session.get('user_id')
    # Owners cannot change their own role to avoid removing their own access
    if user_id == current_user_id:
        flash('Вы не можете менять свою собственную роль.')
        return redirect(url_for('list_employees'))
    # Fetch the target user and ensure they exist
    conn = get_db_connection()
    target = conn.execute('SELECT id, username, role FROM users WHERE id = ?', (user_id,)).fetchone()
    if not target:
        conn.close()
        flash('Сотрудник не найден.')
        return redirect(url_for('list_employees'))
    # Determine desired new role from form data
    new_role = (request.form.get('new_role') or '').strip()
    allowed_roles = ('owner', 'manager', 'maid')
    if new_role not in allowed_roles:
        conn.close()
        flash('Недопустимая роль.')
        return redirect(url_for('list_employees'))
    # If the role is unchanged, simply return
    if new_role == target['role']:
        conn.close()
        flash('Роль сотрудника {} не изменилась.'.format(target['username']))
        return redirect(url_for('list_employees'))
    # Update the user's role
    with conn:
        conn.execute('UPDATE users SET role = ? WHERE id = ?', (new_role, user_id))
    conn.close()
    flash('Роль сотрудника {} изменена на {}.'.format(target['username'], new_role))
    return redirect(url_for('list_employees'))

# ---------------------------------------------------------------------------
# Subscription management
#
# This route allows the owner to create and manage their subscription. It
# presents a simple form to select a plan and simulates activation of the
# subscription. In a real implementation you would integrate with a payment
# gateway and update the subscription status based on callbacks.
@app.route('/subscribe', methods=['GET', 'POST'])
@login_required
def subscribe():
    """Allow the owner to create a subscription for the service."""
    # Only owners can manage subscriptions
    if session.get('user_role') != 'owner':
        return abort(403)
    conn = get_db_connection()
    ensure_subscriptions_table(conn)
    user_id = session.get('user_id')
    # Fetch the most recent subscription for this user (if any)
    current_sub = conn.execute(
        'SELECT * FROM subscriptions WHERE user_id = ? ORDER BY id DESC LIMIT 1',
        (user_id,)
    ).fetchone()
    if request.method == 'POST':
        # Read selected plan from the form; default to 'standard' if missing
        plan_name = (request.form.get('plan_name') or 'standard').strip()
        # Define simple pricing for demonstration purposes; in production this should
        # come from a configuration or database or environment settings
        price_lookup = {
            'standard': 1000.0,
            'premium': 2000.0
        }
        price = price_lookup.get(plan_name, 1000.0)
        # Prevent duplicate active subscriptions
        if current_sub and current_sub['status'] == 'active':
            conn.close()
            flash('У вас уже есть активная подписка.')
            return redirect(url_for('subscribe'))
        # Initiate payment with Kaspi (stub). Returns a payment ID and URL
        payment_id, payment_url = initiate_kaspi_payment(plan_name, price)
        today = date.today()
        # Insert subscription with pending status and payment details
        with conn:
            conn.execute(
                'INSERT INTO subscriptions (user_id, plan_name, status, price, created_at, payment_id, payment_url) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (user_id, plan_name, 'pending', price, today.isoformat(), payment_id, payment_url)
            )
        conn.close()
        # Redirect the user to the payment URL (simulated). In real use this
        # would redirect to the Kaspi payment page.
        return redirect(payment_url)
    # GET request: render subscription page
    conn.close()
    return render_template('subscribe.html', current_sub=current_sub)


# ---------------------------------------------------------------------------
# Payment callback simulation
#
# This route simulates receiving a payment confirmation from Kaspi. When
# integrating with the real Kaspi Pay service, this endpoint should be
# configured as the webhook or redirect URL in your Kaspi merchant
# settings. The payment_id is used to locate the corresponding subscription
# and update its status to active. After processing the payment, the user
# is redirected to the homepage with a flash message.
@app.route('/payment_callback/<payment_id>')
def payment_callback(payment_id: str):
    """
    Handle the payment callback by activating the pending subscription.

    Args:
        payment_id: The unique identifier associated with the pending payment.

    Returns:
        A redirect to the homepage with a flash message indicating the
        outcome of the payment processing.
    """
    conn = get_db_connection()
    ensure_subscriptions_table(conn)
    sub = conn.execute('SELECT * FROM subscriptions WHERE payment_id = ?', (payment_id,)).fetchone()
    if sub and sub['status'] == 'pending':
        # Mark the subscription as active and set next billing date to 30 days from today
        next_date = date.today() + timedelta(days=30)
        with conn:
            conn.execute(
                'UPDATE subscriptions SET status = ?, next_billing_date = ? WHERE id = ?',
                ('active', next_date.isoformat(), sub['id'])
            )
        conn.close()
        flash('Оплата получена, подписка активирована.')
        return redirect(url_for('index'))
    else:
        conn.close()
        flash('Платёж уже обработан или не найден.')
        return redirect(url_for('index'))


# ---------------------------------------------------------------------------
# Chat between employees
#
# The chat page allows authenticated users to exchange messages.  Messages
# authored by the current user are aligned to the right; messages from
# others appear on the left.  Each message stores the exact send time.  To
# ensure the underlying database table exists, this view calls
# ensure_messages_table() before performing any queries or inserts.
from datetime import datetime


# Chat between employees
@app.route("/chat", defaults={'room_id': 1}, methods=["GET", "POST"])
@app.route("/chat/<int:room_id>", methods=["GET", "POST"])
@login_required
def chat(room_id: int):
    """Display the chat interface for a specific room and handle sending messages with attachments.

    On POST, a new message (optionally with a file attachment) is saved to the
    database. On GET, messages are retrieved along with author information and
    read receipt state for rendering. The user's last seen message is updated
    so that read receipts can be calculated for other users.
    """
    conn = get_db_connection()
    # Ensure necessary tables exist
    ensure_chat_rooms_table(conn)
    ensure_chat_room_members_table(conn)
    ensure_messages_table(conn)
    ensure_message_file_columns(conn)
    ensure_last_seen_table(conn)
    ensure_user_room_last_seen_table(conn)
    try:
        ensure_photo_column(conn)
    except Exception:
        pass
    current_user_id = session.get('user_id')
    # Verify membership for non-global rooms
    if room_id != 1:
        membership = conn.execute(
            'SELECT 1 FROM chat_room_members WHERE room_id = ? AND user_id = ?',
            (room_id, current_user_id)
        ).fetchone()
        if not membership:
            conn.close()
            return abort(403)
    # Get room info
    room = conn.execute('SELECT * FROM chat_rooms WHERE id = ?', (room_id,)).fetchone()
    if not room:
        conn.close()
        return abort(404)
    if request.method == "POST":
        # Handle sending a new message with optional attachment
        message_text = (request.form.get("message") or "").strip()
        file_obj = request.files.get('file')
        if not message_text and (not file_obj or not file_obj.filename):
            flash("Введите текст сообщения или приложите файл.")
            conn.close()
            return redirect(url_for("chat", room_id=room_id))
        file_name = None
        file_type = None
        if file_obj and file_obj.filename:
            original_name = file_obj.filename
            safe_name = secure_filename(original_name)
            upload_dir = os.path.join(app.root_path, 'static', 'uploads', 'chat_files')
            os.makedirs(upload_dir, exist_ok=True)
            file_path = os.path.join(upload_dir, safe_name)
            file_obj.save(file_path)
            file_name = safe_name
            file_type = file_obj.content_type or ''
        user_id = current_user_id
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with conn:
            conn.execute(
                "INSERT INTO messages (room_id, user_id, message, timestamp, file_name, file_type) VALUES (?, ?, ?, ?, ?, ?)",
                (room_id, user_id, message_text, timestamp, file_name, file_type)
            )
        conn.close()
        return redirect(url_for("chat", room_id=room_id))
    # GET: display chat messages for the selected room
    # Determine the last seen message ID for the current user (global across rooms)
    last_seen_id = 0
    if current_user_id:
        row = conn.execute(
            'SELECT last_seen_message_id FROM user_last_seen WHERE user_id = ?',
            (current_user_id,)
        ).fetchone()
        if row and row['last_seen_message_id']:
            try:
                last_seen_id = int(row['last_seen_message_id'])
            except Exception:
                last_seen_id = 0
    # Fetch messages in this room ordered by id
    rows = conn.execute(
        """
        SELECT m.id, m.user_id, m.message, m.timestamp, m.file_name, m.file_type,
               u.username, u.name AS author_name, u.photo AS author_photo,
               COALESCE((SELECT MIN(last_seen_message_id)
                         FROM user_last_seen uls
                         WHERE uls.user_id != m.user_id), 0) AS min_last_seen_except_sender
        FROM messages m
        JOIN users u ON m.user_id = u.id
        WHERE m.room_id = ?
        ORDER BY m.id ASC
        """,
        (room_id,)
    ).fetchall()
    message_list = []
    for row in rows:
        msg = dict(row)
        msg['show_ticks'] = (current_user_id is not None and msg['user_id'] == current_user_id)
        msg['read_by_all'] = False
        try:
            min_last_seen = int(msg.get('min_last_seen_except_sender') or 0)
        except Exception:
            min_last_seen = 0
        if msg['show_ticks'] and min_last_seen >= msg['id']:
            msg['read_by_all'] = True
        message_list.append(msg)
    first_unread_id = None
    if current_user_id and last_seen_id:
        for m in message_list:
            if m['id'] > last_seen_id and m['user_id'] != current_user_id:
                first_unread_id = m['id']
                break
    if current_user_id and message_list:
        # Record the ID of the latest message seen in this room.
        max_id = message_list[-1]['id']
        cur = conn.cursor()
        # Update the global last seen message for the user across all rooms.
        cur.execute(
            "INSERT INTO user_last_seen (user_id, last_seen_message_id) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id",
            (current_user_id, max_id)
        )
        # Also record the last seen message for the specific room so unread indicators can
        # be computed accurately on a per‑room basis. Without this, indicators would
        # continue to show until you send a message.
        cur.execute(
            "INSERT INTO user_room_last_seen (user_id, room_id, last_seen_message_id) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id, room_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id",
            (current_user_id, room_id, max_id)
        )
        conn.commit()
        # Update the local last_seen_id variable so unread status is calculated using
        # the most recent message the user has seen. Without this, the unread
        # indicator could persist until a new message is sent.
        try:
            last_seen_id = int(max_id)
        except Exception:
            pass
    # Fetch list of chat rooms for sidebar navigation
    raw_rooms = conn.execute(
        """
        SELECT r.id, r.name
        FROM chat_rooms AS r
        JOIN chat_room_members AS m ON r.id = m.room_id
        WHERE m.user_id = ?
          AND (r.id = 1 OR EXISTS (SELECT 1 FROM messages WHERE room_id = r.id))
        ORDER BY r.id ASC
        """,
        (current_user_id,)
    ).fetchall()
    rooms = list(raw_rooms)
    # Ensure the global chat appears in the list even if the user is not recorded as a member
    if not any(r['id'] == 1 for r in rooms):
        global_room_row = conn.execute('SELECT id, name FROM chat_rooms WHERE id = 1').fetchone()
        if global_room_row:
            rooms = [global_room_row] + rooms

    # Build a list of rooms with a display name for private chats. For two‑person rooms, show
    # only the other participant's name; otherwise use the stored room name.
    processed_rooms = []
    for r in rooms:
        display_name = r['name']
        participants = conn.execute(
            "SELECT u.id, u.name, u.username FROM chat_room_members m JOIN users u ON m.user_id = u.id WHERE m.room_id = ?",
            (r['id'],)
        ).fetchall()
        if len(participants) == 2:
            other = None
            for p in participants:
                if p['id'] != current_user_id:
                    other = p
                    break
            if other:
                display_name = other['name'] or other['username']
        r_dict = dict(r)
        r_dict['display_name'] = display_name
        processed_rooms.append(r_dict)

    # Determine unread status for each room using per‑room last seen records.
    unread_rooms = {}
    for r in processed_rooms:
        threshold = 0
        row_seen = conn.execute(
            "SELECT last_seen_message_id FROM user_room_last_seen WHERE user_id = ? AND room_id = ?",
            (current_user_id, r['id'])
        ).fetchone()
        if row_seen and row_seen['last_seen_message_id'] is not None:
            try:
                threshold = int(row_seen['last_seen_message_id'])
            except Exception:
                threshold = 0
        else:
            row_max = conn.execute(
                "SELECT MAX(id) AS max_id FROM messages WHERE room_id = ?",
                (r['id'],)
            ).fetchone()
            max_id = row_max['max_id'] if row_max and row_max['max_id'] is not None else 0
            threshold = int(max_id) if max_id else 0
            conn.execute(
                "INSERT INTO user_room_last_seen (user_id, room_id, last_seen_message_id) VALUES (?, ?, ?) "
                "ON CONFLICT(user_id, room_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id",
                (current_user_id, r['id'], threshold)
            )
            conn.commit()
        row_unread = conn.execute(
            "SELECT 1 FROM messages WHERE room_id = ? AND id > ? AND user_id != ? LIMIT 1",
            (r['id'], threshold, current_user_id)
        ).fetchone()
        unread_rooms[r['id']] = bool(row_unread)
    if room_id in unread_rooms:
        unread_rooms[room_id] = False

    # Determine the display name for the current room
    room_display_name = room['name']
    participants = conn.execute(
        "SELECT u.id, u.name, u.username FROM chat_room_members m JOIN users u ON m.user_id = u.id WHERE m.room_id = ?",
        (room_id,)
    ).fetchall()
    if len(participants) == 2:
        for p in participants:
            if p['id'] != current_user_id:
                room_display_name = p['name'] or p['username']
                break
    conn.close()
    return render_template(
        "chat.html",
        messages=message_list,
        first_unread_id=first_unread_id,
        room=room,
        room_display_name=room_display_name,
        rooms=processed_rooms,
        unread_rooms=unread_rooms
    )

# ---------------------------------------------------------------------------
# Chat rooms listing and creation
#
# The chat_rooms view presents the list of rooms the current user belongs to and
# provides a simple interface to start new private or group chats.  Users can
# select one or more colleagues from the staff list.  When a single user is
# selected (and no group name is provided) a private chat will either reuse
# an existing room or create a new two‑person room.  When multiple users are
# selected, the form requires a non‑empty group name.  All selected users,
# plus the current user, become members of the newly created room.
#
# After creation, users are redirected straight into the newly created chat.

@app.route('/chat_rooms', methods=['GET', 'POST'])
@login_required
def chat_rooms():
    """List the chat rooms the current user participates in and allow creation of new rooms."""
    conn = get_db_connection()
    ensure_chat_rooms_table(conn)
    ensure_chat_room_members_table(conn)
    ensure_messages_table(conn)
    current_user_id = session.get('user_id')

    # Handle creation of new chat on POST
    if request.method == 'POST':
        # Grab the selected colleague IDs from the form.  They come as strings.
        selected_ids = request.form.getlist('members')
        group_name = (request.form.get('group_name') or '').strip()
        # Convert to integers and remove any invalid entries
        participants = set()
        for sid in selected_ids:
            try:
                participants.add(int(sid))
            except Exception:
                pass
        # Always include the current user as a participant
        participants.add(current_user_id)
        # Remove the current user if someone accidentally selected themselves
        participants.discard(current_user_id)
        # If no other participants were selected, nothing to do
        if not participants:
            flash('Выберите хотя бы одного сотрудника для создания чата.')
            return redirect(url_for('chat_rooms'))
        # Build full participant set including current user
        all_participants = set(participants)
        all_participants.add(current_user_id)
        # Handle private chat (exactly two participants)
        if len(all_participants) == 2 and not group_name:
            # Determine the other user's ID
            other_id = next(iter(participants))
            # Try to find an existing two‑person room containing exactly this pair
            row = conn.execute(
                """
                SELECT crm.room_id
                FROM chat_room_members AS crm
                WHERE crm.user_id IN (?, ?)
                GROUP BY crm.room_id
                HAVING COUNT(*) = 2 AND
                       (SELECT COUNT(*) FROM chat_room_members WHERE room_id = crm.room_id) = 2 AND
                       crm.room_id != 1
                """,
                (current_user_id, other_id)
            ).fetchone()
            if row:
                # Existing room found; redirect to it
                conn.close()
                return redirect(url_for('chat', room_id=row['room_id']))
            # Create a new private room with a simple name
            # Fetch names to construct a human‑readable title
            # Correct the placeholder when fetching a single user by id.
            other_user = conn.execute('SELECT name FROM users WHERE id = ?', (other_id,)).fetchone()
            other_name = other_user['name'] if other_user and other_user['name'] else 'Собеседник'
            room_name = f'Диалог с {other_name}'
            cur = conn.cursor()
            cur.execute('INSERT INTO chat_rooms (name) VALUES (?)', (room_name,))
            new_room_id = cur.lastrowid
            # Insert both participants into the membership table
            for uid in all_participants:
                cur.execute('INSERT INTO chat_room_members (room_id, user_id) VALUES (?, ?)', (new_room_id, uid))
            conn.commit()
            conn.close()
            return redirect(url_for('chat', room_id=new_room_id))
        # Otherwise treat as a group chat; require a group name
        if not group_name:
            flash('Для группового чата укажите название группы.')
            return redirect(url_for('chat_rooms'))
        # Create new group room
        cur = conn.cursor()
        cur.execute('INSERT INTO chat_rooms (name) VALUES (?)', (group_name,))
        new_room_id = cur.lastrowid
        for uid in all_participants:
            cur.execute('INSERT INTO chat_room_members (room_id, user_id) VALUES (?, ?)', (new_room_id, uid))
        conn.commit()
        conn.close()
        flash(f'Групповой чат "{group_name}" создан.')
        return redirect(url_for('chat', room_id=new_room_id))

    # GET: show existing chat rooms and colleague list for creation
    # Fetch chat rooms the user is a member of, including the global chat (id=1)
    rooms = conn.execute(
        """
        SELECT r.id, r.name
        FROM chat_rooms AS r
        JOIN chat_room_members AS m ON r.id = m.room_id
        WHERE m.user_id = ?
          AND (r.id = 1 OR EXISTS (SELECT 1 FROM messages WHERE room_id = r.id))
        ORDER BY r.id ASC
        """,
        (current_user_id,)
    ).fetchall()
    # Ensure the global chat (id=1) appears in the list even if the user is not recorded as a member.
    if not any(row['id'] == 1 for row in rooms):
        global_room = conn.execute('SELECT id, name FROM chat_rooms WHERE id = 1').fetchone()
        if global_room:
            rooms = [global_room] + rooms
    # Fetch list of employees excluding the current user
    colleagues = conn.execute(
        """
        SELECT id, name, username, role
        FROM users
        WHERE id != ?
        ORDER BY name
        """,
        (current_user_id,)
    ).fetchall()
    conn.close()
    return render_template('chat_rooms.html', rooms=rooms, colleagues=colleagues)


# ---------------------------------------------------------------------------
# Start a private chat from a user's profile
#
# This route is used when a user clicks the "Написать" button on an employee's
# profile page. It attempts to find an existing one-on-one chat between the
# current user and the target user. If such a room exists, the user is
# redirected directly into that chat. Otherwise a new chat room is created
# (with both users as members) but it will remain hidden from the chat list
# until the first message is sent because the room has no messages. After
# creation the user is redirected to the chat page for the new room so they
# can compose a message.

@app.route('/start_chat/<int:user_id>')
@login_required
def start_chat(user_id: int):
    conn = get_db_connection()
    ensure_chat_rooms_table(conn)
    ensure_chat_room_members_table(conn)
    ensure_messages_table(conn)
    # Validate the target user exists
    target = conn.execute('SELECT id, name, username FROM users WHERE id = ?', (user_id,)).fetchone()
    if not target:
        conn.close()
        return abort(404)
    current_user_id = session.get('user_id')
    if not current_user_id:
        conn.close()
        return abort(403)
    # Prevent starting a chat with yourself
    if user_id == current_user_id:
        conn.close()
        flash('Нельзя начать чат с самим собой.')
        return redirect(request.referrer or url_for('chat'))
    # Check if a private chat (exactly two members) between current and target exists
    existing_room = conn.execute(
        '''
        SELECT r.id
        FROM chat_rooms AS r
        JOIN chat_room_members AS m1 ON r.id = m1.room_id
        JOIN chat_room_members AS m2 ON r.id = m2.room_id
        WHERE m1.user_id = ? AND m2.user_id = ?
          AND r.id != 1
        GROUP BY r.id
        HAVING COUNT(*) = 2
        ''',
        (current_user_id, user_id)
    ).fetchone()
    if existing_room:
        room_id = existing_room['id']
        conn.close()
        return redirect(url_for('chat', room_id=room_id))
    # Create a new room name based on participants' display names
    names = conn.execute(
        'SELECT COALESCE(name, username) AS display_name FROM users WHERE id IN (?, ?)',
        (current_user_id, user_id)
    ).fetchall()
    name_list = sorted(row['display_name'] for row in names)
    room_name = ', '.join(name_list)
    # Create the room and add both users as members
    cur = conn.cursor()
    cur.execute('INSERT INTO chat_rooms (name) VALUES (?)', (room_name,))
    new_room_id = cur.lastrowid
    cur.execute('INSERT INTO chat_room_members (room_id, user_id) VALUES (?, ?)', (new_room_id, current_user_id))
    cur.execute('INSERT INTO chat_room_members (room_id, user_id) VALUES (?, ?)', (new_room_id, user_id))
    conn.commit()
    conn.close()
    return redirect(url_for('chat', room_id=new_room_id))

# ---------------------------------------------------------------------------
# Delete a chat room and all its messages
#
# Participants of a chat can delete it via POST. The global chat (id=1) is
# protected from deletion. Deleting a chat removes all its messages,
# memberships and last-seen records, then redirects to the chat list.

@app.route('/chat/delete/<int:room_id>', methods=['POST'])
@login_required
def delete_chat(room_id: int):
    """
    Remove the current user from a chat room.

    When a user chooses to delete a chat, the conversation should remain for
    other participants until they also delete it. This handler removes the
    user's membership in the chat and clears their per‑room last seen record.
    If the user was the last remaining member, the entire room and its
    messages are deleted. The global chat (room_id=1) cannot be deleted by
    individual users.
    """
    conn = get_db_connection()
    # Ensure necessary tables exist
    ensure_chat_rooms_table(conn)
    ensure_chat_room_members_table(conn)
    ensure_messages_table(conn)
    ensure_user_room_last_seen_table(conn)
    current_user_id = session.get('user_id')
    # Prevent deletion of the global chat
    if room_id == 1:
        conn.close()
        abort(403)
    # Verify that the user is a member of this room
    membership = conn.execute(
        'SELECT 1 FROM chat_room_members WHERE room_id = ? AND user_id = ?',
        (room_id, current_user_id)
    ).fetchone()
    if not membership:
        conn.close()
        abort(403)
    # Count how many members remain in this room
    row_member_count = conn.execute(
        'SELECT COUNT(*) FROM chat_room_members WHERE room_id = ?',
        (room_id,)
    ).fetchone()
    if row_member_count is None:
        member_count = 0
    elif isinstance(row_member_count, dict):
        member_count = list(row_member_count.values())[0]
    else:
        member_count = row_member_count[0]
    with conn:
        # Remove the current user from the membership table
        conn.execute(
            'DELETE FROM chat_room_members WHERE room_id = ? AND user_id = ?',
            (room_id, current_user_id)
        )
        # Remove the last seen record for this user in this room
        conn.execute(
            'DELETE FROM user_room_last_seen WHERE user_id = ? AND room_id = ?',
            (current_user_id, room_id)
        )
        # If the user was the last member, remove the entire chat room and its messages
        if member_count <= 1:
            conn.execute('DELETE FROM messages WHERE room_id = ?', (room_id,))
            conn.execute('DELETE FROM chat_rooms WHERE id = ?', (room_id,))
            # Also remove any remaining per‑room last seen entries (should be none)
            conn.execute('DELETE FROM user_room_last_seen WHERE room_id = ?', (room_id,))
    conn.close()
    flash('Чат удалён.')
    return redirect(url_for('chat_rooms'))

# ---------------------------------------------------------------------------
# Mark a chat as seen via an asynchronous request
#
# The front‑end may navigate between chat rooms very quickly. When a user
# opens a chat room but immediately clicks on another one, the full page
# render for the first chat might be cancelled before the server updates
# their last‑seen message. As a result, the unread indicator can persist
# incorrectly in the sidebar. To ensure that a room is marked as read as
# soon as the user views it (even if they don't send a message), we
# expose this lightweight POST endpoint. It records the maximum message ID
# in the specified room for the current user in both the global and
# per‑room "last seen" tables. The client should call this endpoint on
# page load and when the user leaves or hides the chat page.

@app.route('/chat/<int:room_id>/seen', methods=['POST', 'GET'])
@login_required
def mark_chat_seen(room_id: int):
    """Mark the current chat as read for the logged‑in user.

    The client calls this endpoint via `fetch` or `navigator.sendBeacon`
    whenever the chat page is loaded or hidden. It updates the
    `user_last_seen` and `user_room_last_seen` tables with the highest
    message ID in the given room. If the room doesn't exist or the user
    isn't a member of the room (except for the global room with ID=1),
    nothing is updated. A 204 response is always returned to the client
    so that no additional handling is required on the front‑end.
    """
    # Get the current user ID from the session. If unavailable, return early.
    current_user_id = session.get('user_id')
    if not current_user_id:
        return ('', 204)
    conn = get_db_connection()
    # Ensure necessary tables exist before updating them
    ensure_messages_table(conn)
    ensure_last_seen_table(conn)
    ensure_user_room_last_seen_table(conn)
    ensure_chat_room_members_table(conn)
    # Verify that the user is a member of this room (skip check for the global chat)
    if room_id != 1:
        membership = conn.execute(
            'SELECT 1 FROM chat_room_members WHERE room_id = ? AND user_id = ?',
            (room_id, current_user_id)
        ).fetchone()
        if not membership:
            conn.close()
            return ('', 204)
    # Determine the highest message ID in the room. If there are no messages,
    # default to zero so that the last seen entry is set but won't mark
    # anything unread.
    row = conn.execute(
        'SELECT MAX(id) AS max_id FROM messages WHERE room_id = ?',
        (room_id,)
    ).fetchone()
    try:
        max_id = int(row['max_id']) if row and row['max_id'] is not None else 0
    except Exception:
        max_id = 0
    with conn:
        # Update the global last seen table for the user
        conn.execute(
            'INSERT INTO user_last_seen (user_id, last_seen_message_id) VALUES (?, ?) '
            'ON CONFLICT(user_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id',
            (current_user_id, max_id)
        )
        # Update the per‑room last seen table
        conn.execute(
            'INSERT INTO user_room_last_seen (user_id, room_id, last_seen_message_id) VALUES (?, ?, ?) '
            'ON CONFLICT(user_id, room_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id',
            (current_user_id, room_id, max_id)
        )
    conn.close()
    # Respond with 204 No Content. This avoids triggering any redirects on the client.
    return ('', 204)


# ---------------------------------------------------------------------------
# Account page for the logged‑in user
#
# This page allows the user to view and update their profile information
# (name and contact info) and upload a profile photo. A logout link is
# also provided on this page. The profile photo is stored in the
# 'static/uploads' directory relative to the application root, and only
# the filename is stored in the database.
@app.route("/account", methods=["GET", "POST"])
def account():
    """
    Display and update the profile for the currently logged‑in user.  When a POST request
    is received, update the user's name, contact information and optional avatar image.
    When handling avatar uploads, save the file into the ``static/uploads`` directory
    relative to the application root and store a path relative to the ``static`` folder
    (e.g. ``uploads/filename.jpg``) in the database.  Avoid using a context manager on
    the connection so the wrapper does not close the connection prematurely.
    """
    # Ensure the user is authenticated; if not, redirect to login
    if not session.get('user_id'):
        return redirect(url_for('login'))
    user_id = session['user_id']
    conn = get_db_connection()
    # Ensure the photo column exists on the users table before any updates
    ensure_photo_column(conn)
    # Always fetch the current user record up front so that both GET and POST
    # branches have access to the existing contact_info and photo.  Avoid
    # closing the connection here since it will be reused further down.
    try:
        user_row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    except Exception:
        user_row = None
    if request.method == 'POST':
        # Extract profile fields
        name = (request.form.get('name') or '').strip() or None
        # Pull country code and phone digits from the form; remove all non‑digit characters
        country_code = (request.form.get('country_code') or '').strip()
        raw_phone = (request.form.get('phone') or '').strip()
        phone_digits = re.sub(r'\D', '', raw_phone)
        # Determine the new contact_info value.  If digits were provided, always
        # compose a contact string combining the selected country code and digits.
        # Otherwise, fall back to the existing contact_info so that omitting the
        # phone field does not wipe out the existing number.
        contact: str | None
        if phone_digits:
            contact = f"{country_code}{phone_digits}"
        else:
            # Preserve existing contact_info if available
            contact = user_row['contact_info'] if user_row else None
        # Handle uploaded photo.  Save the file into ``static/uploads`` and build a
        # relative path for storage in the database.  When saving, ensure the
        # directory exists and optionally prefix the filename with a timestamp to
        # avoid collisions.
        photo_file = request.files.get('photo')
        photo_path: str | None = None
        if photo_file and photo_file.filename:
            filename = secure_filename(photo_file.filename)
            # Prefix the filename with a timestamp to avoid name clashes
            timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            unique_name = f"{timestamp}_{filename}"
            upload_dir = os.path.join(app.root_path, 'static', 'uploads')
            os.makedirs(upload_dir, exist_ok=True)
            file_path = os.path.join(upload_dir, unique_name)
            try:
                photo_file.save(file_path)
                # Store the relative path (under static) in the DB
                photo_path = f'uploads/{unique_name}'
            except Exception:
                # If saving fails, ignore the upload and continue without updating the photo
                photo_path = None
        # Perform the update.  Do not use a context manager on ``conn`` so that
        # the wrapper does not close the connection before we are done.  Commit
        # explicitly so changes persist on SQLite and remain harmless on PostgreSQL.
        try:
            if photo_path:
                conn.execute(
                    'UPDATE users SET name=?, contact_info=?, photo=? WHERE id=?',
                    (name, contact, photo_path, user_id)
                )
                # Update the session variable so the navigation bar updates immediately
                session['user_photo'] = photo_path
            else:
                conn.execute(
                    'UPDATE users SET name=?, contact_info=? WHERE id=?',
                    (name, contact, user_id)
                )
            conn.commit()
        except Exception:
            # Ignore update errors but log if necessary
            pass
        # Close connection after update
        conn.close()
        flash('Профиль обновлён.')
        return redirect(url_for('account'))
    # GET: display current profile.  Compute phone_country_code and phone_digits
    user = user_row
    # Close connection now that we no longer need it for GET
    conn.close()
    # Determine the default phone code and digits for edit form.  If the user
    # already has a contact_info value, parse it to extract the country code
    # and subscriber number.  Otherwise, fall back to Kazakhstan code (+7)
    # with an empty subscriber number.  Use the same grouping logic as
    # format_phone but omit the leading plus sign from the returned number.
    phone_country_code = '+7'
    phone_digits_display = ''
    try:
        import re as _re
        if user and user['contact_info']:
            digits_only = _re.sub(r'\D', '', user['contact_info'])
            if digits_only:
                code_digits = ''
                number_digits = ''
                # Determine code based on common prefixes
                if digits_only.startswith('7') and len(digits_only) > 1:
                    code_digits = '7'
                    number_digits = digits_only[1:]
                elif digits_only.startswith('1') and len(digits_only) > 1:
                    code_digits = '1'
                    number_digits = digits_only[1:]
                elif digits_only.startswith('44') and len(digits_only) > 2:
                    code_digits = '44'
                    number_digits = digits_only[2:]
                elif digits_only.startswith('49') and len(digits_only) > 2:
                    code_digits = '49'
                    number_digits = digits_only[2:]
                elif digits_only.startswith('81') and len(digits_only) > 2:
                    code_digits = '81'
                    number_digits = digits_only[2:]
                else:
                    # Fallback: treat up to first three digits as code
                    if len(digits_only) > 3:
                        code_digits = digits_only[:3]
                        number_digits = digits_only[3:]
                    elif len(digits_only) > 2:
                        code_digits = digits_only[:2]
                        number_digits = digits_only[2:]
                    elif len(digits_only) > 1:
                        code_digits = digits_only[:1]
                        number_digits = digits_only[1:]
                    else:
                        code_digits = digits_only
                        number_digits = ''
                if code_digits:
                    phone_country_code = '+' + code_digits
                # Format the subscriber number according to common groupings for display
                def _format_subscriber(code: str, number: str) -> str:
                    groups: list[int] = []
                    if code == '7':
                        groups = [3, 3, 2, 2]
                    elif code == '1':
                        groups = [3, 3, 4]
                    elif code in ('44', '49'):
                        groups = [4, 3, 4]
                    elif code == '81':
                        groups = [4, 3, 3]
                    else:
                        # Fallback: group remaining digits in chunks of up to 3
                        rem = len(number)
                        while rem > 0:
                            g = 3 if rem >= 3 else rem
                            groups.append(g)
                            rem -= g
                    parts: list[str] = []
                    idx = 0
                    for g in groups:
                        part = number[idx: idx + g]
                        if not part:
                            break
                        parts.append(part)
                        idx += g
                    if not parts:
                        return ''
                    first = parts[0]
                    formatted = f"({first})"
                    rest = parts[1:]
                    if rest:
                        if code == '1' and len(rest) == 2:
                            formatted += f" {rest[0]}-{rest[1]}"
                        else:
                            formatted += ' ' + ' '.join(rest)
                    return formatted
                phone_digits_display = _format_subscriber(code_digits, number_digits)
    except Exception:
        # On any error while parsing the phone, retain defaults
        phone_country_code = '+7'
        phone_digits_display = ''
    return render_template('account.html', user=user, phone_country_code=phone_country_code, phone_digits=phone_digits_display)


# ---------------------------------------------------------------------------
# Employee profile viewing
#
# This route displays the profile of a specific employee in read‑only mode.
# Any authenticated user can view profiles. We ensure the photo column exists
# for avatars and show basic fields such as username, name, role and
# contact information.
@app.route('/employee/<int:user_id>')
@login_required
def employee_profile(user_id: int):
    conn = get_db_connection()
    # Ensure the users table has a photo column for avatars
    try:
        ensure_photo_column(conn)
    except Exception:
        pass
    # Use correct placeholder for id lookup; see comments above.
    user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    if not user:
        flash('Сотрудник не найден.')
        # Fallback: redirect back to chat. Alternatively could redirect to employees list.
        return redirect(url_for('chat'))
    return render_template('employee_info.html', user=user)



if __name__ == "__main__":
    # Optionally specify database file through environment variable APARTHOTEL_DB_FILE
    app.run(debug=True)