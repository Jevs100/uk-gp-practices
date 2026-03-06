"""Main interface for querying GP practice data."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

from .db import connect, init_db, upsert_practices
from .models import Practice
from .paths import csv_path, db_path as default_db_path
from .sources.base import Source


DEFAULT_MAX_AGE = timedelta(days=1)

_INTERNAL_COLUMNS = frozenset({"name_norm", "postcode_norm"})


def _row_to_practice(row: sqlite3.Row) -> Practice:
    raw = {k: row[k] for k in row.keys() if k not in _INTERNAL_COLUMNS}
    return Practice(
        organisation_code=row["organisation_code"],
        name=row["name"],
        postcode=row["postcode"],
        town=row["town"],
        status=row["status"],
        nation=row["nation"],
        raw=raw,
    )


@dataclass(slots=True)
class PracticeIndex:
    """
    Main query interface for GP practice data.

    Backed by:
      - downloaded source CSV files (one per nation)
      - local SQLite database for fast lookups

    Can be used as a context manager::

        with PracticeIndex.auto_update() as idx:
            practice = idx.get("A81001")
    """

    db_file: Path
    _con_cache: sqlite3.Connection | None = field(default=None, repr=False)

    @classmethod
    def auto_update(
        cls,
        sources: list[Source] | None = None,
        max_age: timedelta = DEFAULT_MAX_AGE,
    ) -> "PracticeIndex":
        from .sources import ALL_SOURCES

        if sources is None:
            sources = ALL_SOURCES  # type: ignore[assignment]

        idx = cls(db_file=default_db_path())
        idx._ensure_schema()
        for source in sources:
            idx.update_if_needed(source=source, max_age=max_age)
        return idx

    def _ensure_schema(self) -> None:
        init_db(self._con())

    def _con(self) -> sqlite3.Connection:
        if self._con_cache is None:
            con = connect(self.db_file)
            con.row_factory = sqlite3.Row
            self._con_cache = con
        return self._con_cache

    def close(self) -> None:
        """Close the underlying database connection."""
        if self._con_cache is not None:
            self._con_cache.close()
            self._con_cache = None

    def __enter__(self) -> "PracticeIndex":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def update_if_needed(
        self, source: Source, max_age: timedelta = DEFAULT_MAX_AGE
    ) -> bool:
        """
        Download + rebuild if the CSV is missing or older than max_age.
        Returns True if an update happened.
        """
        csvf = csv_path(source.nation)
        if csvf.exists():
            mtime = datetime.fromtimestamp(csvf.stat().st_mtime, tz=timezone.utc)
            if datetime.now(tz=timezone.utc) - mtime < max_age:
                return False

        self.update(source=source)
        return True

    def update(self, source: Source) -> None:
        """Download a source CSV and upsert into SQLite."""
        csvf = csv_path(source.nation)
        source.download(csvf)
        self.load_source(csvf, source=source)

    def load_source(
        self,
        csv_file: str | Path,
        source: Source,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """
        Parse a local CSV via the given source and upsert into SQLite.

        on_progress: optional callback(rows_completed, total_rows)
        Returns the number of rows ingested.
        """
        con = self._con()
        init_db(con)
        prepared = source.parse(Path(csv_file))
        upsert_practices(con, prepared, on_progress=on_progress)
        return len(prepared)

    def load_csv(
        self,
        csv_file: str | Path,
        report: str = "epraccur",
        on_progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """
        Backward-compatible wrapper — loads an epraccur CSV using EnglandSource.

        Prefer load_source() for new code.
        """
        from .sources.england import EnglandSource

        return self.load_source(csv_file, source=EnglandSource(), on_progress=on_progress)

    def get(self, organisation_code: str) -> Practice | None:
        """Fetch a single practice by its ODS organisation code."""
        code = organisation_code.strip()
        con = self._con()
        row = con.execute(
            "SELECT * FROM practices WHERE organisation_code = ?",
            (code,),
        ).fetchone()
        if not row:
            return None
        return _row_to_practice(row)

    def search(
        self,
        name: str | None = None,
        postcode: str | None = None,
        town: str | None = None,
        status: str | None = None,
        nation: str | None = None,
        limit: int = 25,
    ) -> list[Practice]:
        """Search practices by name, postcode, town, and/or nation."""
        from .normalise import normalize_name, normalize_postcode

        clauses: list[str] = []
        params: list[Any] = []

        if nation:
            clauses.append("nation = ?")
            params.append(nation.lower())

        if status:
            clauses.append("status = ?")
            params.append(status)

        if postcode:
            pn = normalize_postcode(postcode)
            clauses.append("postcode_norm = ?")
            params.append(pn)

        if town:
            clauses.append("LOWER(town) LIKE ?")
            params.append(f"%{town.strip().lower()}%")

        if name:
            nn = normalize_name(name)
            clauses.append("name_norm LIKE ?")
            params.append(f"%{nn}%")

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM practices{where} LIMIT ?"
        params.append(int(limit))

        con = self._con()
        rows = con.execute(sql, params).fetchall()
        return [_row_to_practice(r) for r in rows]
