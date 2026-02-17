"""Database access and manipulation."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable


PRACTICES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS practices (
  organisation_code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  name_norm TEXT,
  postcode TEXT,
  postcode_norm TEXT,
  town TEXT,
  status TEXT
);
"""

INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_practices_postcode_norm ON practices(postcode_norm);",
    "CREATE INDEX IF NOT EXISTS idx_practices_name_norm ON practices(name_norm);",
]


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.execute(PRACTICES_TABLE_SQL)
    for stmt in INDEXES_SQL:
        con.execute(stmt)
    con.commit()


def upsert_practices(
    con: sqlite3.Connection, rows: Iterable[dict[str, str | None]]
) -> None:
    """
    Upsert practice rows into sqlite.

    Expected keys in each row:
      organisation_code, name, name_norm, postcode, postcode_norm, town, status
    """
    con.executemany(
        """
        INSERT INTO practices (
          organisation_code, name, name_norm, postcode, postcode_norm, town, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(organisation_code) DO UPDATE SET
          name=excluded.name,
          name_norm=excluded.name_norm,
          postcode=excluded.postcode,
          postcode_norm=excluded.postcode_norm,
          town=excluded.town,
          status=excluded.status
        """,
        [
            (
                r.get("organisation_code"),
                r.get("name"),
                r.get("name_norm"),
                r.get("postcode"),
                r.get("postcode_norm"),
                r.get("town"),
                r.get("status"),
            )
            for r in rows
        ],
    )
    con.commit()


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    """Read a normal headered CSV into dict rows."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def read_csv_rows(path: Path) -> list[list[str]]:
    """Read a CSV into raw rows (positional), for headerless exports."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))
