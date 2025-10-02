#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script patches your existing Flask project's app.py to fix
SQL placeholder issues and a malformed INSERT in guest_comments.

What it does:
1) Replaces '%?' → '?' in common WHERE/VALUES positions
2) Fixes SELECT on guest_comments: '%s' → '?'
3) Fixes INSERT into guest_comments: adds missing comma and created_at

Usage:
    python fix_app_sql_placeholders.py /path/to/app.py
"""
import sys, re, io, os

def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_app_sql_placeholders.py /path/to/app.py")
        sys.exit(1)
    path = sys.argv[1]
    if not os.path.isfile(path):
        print("File not found:", path)
        sys.exit(2)

    with io.open(path, "r", encoding="utf-8") as f:
        s = f.read()

    original = s

    # 1) Replace bad %? placeholders with ?
    s = s.replace(" WHERE id = %?", " WHERE id = ?")
    s = s.replace(" WHERE username = %?", " WHERE username = ?")
    s = s.replace(" VALUES (%?, %?)", " VALUES (?, ?)")

    # 2) Fix guest_comments SELECT: %s -> ?
    import re
    s = re.sub(
        r"SELECT\s+comment,\s*created_at\s+FROM\s+guest_comments\s+WHERE\s+guest_id\s*=\s*%s",
        "SELECT comment, created_at FROM guest_comments WHERE guest_id = ?",
        s,
        flags=re.IGNORECASE
    )

    # 3) Fix guest_comments INSERT: add comma and created_at
    s = s.replace(
        "INSERT INTO guest_comments (guest_id comment)\n                    VALUES (?, ?)",
        "INSERT INTO guest_comments (guest_id, comment, created_at)\n                    VALUES (?, ?, ?)"
    )

    if s == original:
        print("No changes made (file already patched or patterns not found).");
    else:
        # Backup
        backup = path + ".bak"
        with io.open(backup, "w", encoding="utf-8") as f:
            f.write(original)
        with io.open(path, "w", encoding="utf-8") as f:
            f.write(s)
        print("Patched:", path)
        print("Backup saved to:", backup)

if __name__ == "__main__":
    main()
