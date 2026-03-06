"""Database access and manipulation."""

from __future__ import annotations

import csv
import sqlite3
from collections.abc import Callable, Iterable
from pathlib import Path


PRACTICES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS practices (
  organisation_code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  name_norm TEXT,
  postcode TEXT,
  postcode_norm TEXT,
  town TEXT,
  status TEXT,
  nation TEXT
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


def _migrate(con: sqlite3.Connection) -> None:
    """Apply any schema migrations needed for existing databases."""
    existing = {row[1] for row in con.execute("PRAGMA table_info(practices)")}
    if "nation" not in existing:
        con.execute("ALTER TABLE practices ADD COLUMN nation TEXT")
        con.commit()


def init_db(con: sqlite3.Connection) -> None:
    con.execute(PRACTICES_TABLE_SQL)
    for stmt in INDEXES_SQL:
        con.execute(stmt)
    con.commit()
    _migrate(con)


_UPSERT_SQL = """
INSERT INTO practices (
  organisation_code, name, name_norm, postcode, postcode_norm, town, status, nation
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(organisation_code) DO UPDATE SET
  name=excluded.name,
  name_norm=excluded.name_norm,
  postcode=excluded.postcode,
  postcode_norm=excluded.postcode_norm,
  town=excluded.town,
  status=excluded.status,
  nation=excluded.nation
"""


def upsert_practices(
    con: sqlite3.Connection,
    rows: Iterable[dict[str, str | None]],
    on_progress: Callable[[int, int], None] | None = None,
    chunk_size: int = 500,
) -> None:
    """
    Upsert practice rows into sqlite.

    Expected keys in each row:
      organisation_code, name, name_norm, postcode, postcode_norm, town, status, nation

    on_progress: optional callback(rows_completed, total_rows)
    """
    all_rows = list(rows)
    total = len(all_rows)
    completed = 0

    for i in range(0, total, chunk_size):
        chunk = all_rows[i : i + chunk_size]
        con.executemany(
            _UPSERT_SQL,
            [
                (
                    r.get("organisation_code"),
                    r.get("name"),
                    r.get("name_norm"),
                    r.get("postcode"),
                    r.get("postcode_norm"),
                    r.get("town"),
                    r.get("status"),
                    r.get("nation"),
                )
                for r in chunk
            ],
        )
        completed += len(chunk)
        if on_progress is not None:
            on_progress(completed, total)

    con.commit()


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    """Read a normal headered CSV into dict rows."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def read_csv_rows(path: Path) -> list[list[str]]:
    """Read a CSV into raw rows (positional), for headerless exports."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))
