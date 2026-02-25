"""Main interface for querying GP practice data."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

from .db import connect, init_db, read_csv_rows, upsert_practices
from .download import download_report
from .models import Practice
from .normalise import normalize_name, normalize_postcode
from .paths import csv_path, db_path as default_db_path


DEFAULT_REPORT = "epraccur"
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
        raw=raw,
    )


@dataclass(slots=True)
class PracticeIndex:
    """
    Main query interface for GP practice data.

    Backed by:
      - downloaded NHS ODS DSE CSV report(s)
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
        report: str = DEFAULT_REPORT,
        max_age: timedelta = DEFAULT_MAX_AGE,
    ) -> "PracticeIndex":
        idx = cls(db_file=default_db_path())
        idx._ensure_schema()
        idx.update_if_needed(report=report, max_age=max_age)
        return idx

    def _ensure_schema(self) -> None:
        con = self._con()
        init_db(con)

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
        self, report: str = DEFAULT_REPORT, max_age: timedelta = DEFAULT_MAX_AGE
    ) -> bool:
        """
        Download + rebuild if the CSV is missing or older than max_age.
        Returns True if an update happened.
        """
        csvf = csv_path(report)
        if csvf.exists():
            mtime = datetime.fromtimestamp(csvf.stat().st_mtime, tz=timezone.utc)
            if datetime.now(tz=timezone.utc) - mtime < max_age:
                return False

        self.update(report=report)
        return True

    def update(self, report: str = DEFAULT_REPORT) -> None:
        """
        Download a report CSV and upsert into SQLite.
        """
        csvf = csv_path(report)
        download_report(report=report, dest=csvf)
        self.load_csv(csvf, report=report)

    def get(self, organisation_code: str) -> Practice | None:
        """
        Fetch a single practice by its ODS organisation code.
        """
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
        limit: int = 25,
    ) -> list[Practice]:
        """
        Search practices by name and/or postcode and/or town.

        Note: this is simple LIKE matching for v0.1.
        Fuzzy matching can come later as an optional dependency.
        """
        clauses: list[str] = []
        params: list[object] = []

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

    def _prepare_rows_epraccur(self, rows: list[list[str]]) -> list[dict[str, Any]]:
        """
        Parse headerless epraccur CSV by positional columns.

        Observed column mapping:
        0 = organisation_code
        1 = name
        7 = town
        9 = postcode
        12 = status
        """
        prepared: list[dict[str, Any]] = []

        for r in rows:
            if not r or len(r) < 13:
                continue

            code = (r[0] or "").strip()
            name = (r[1] or "").strip()
            town = (r[7] or "").strip() or None
            postcode = (r[9] or "").strip() or None
            status = (r[12] or "").strip() or None

            if not code or not name:
                continue

            prepared.append(
                {
                    "organisation_code": code,
                    "name": name,
                    "name_norm": normalize_name(name),
                    "postcode": postcode,
                    "postcode_norm": normalize_postcode(postcode),
                    "town": town,
                    "status": status,
                }
            )

        return prepared

    def load_csv(
        self,
        csv_file: str | Path,
        report: str = DEFAULT_REPORT,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """
        Load a local CSV file into the SQLite database.

        on_progress: optional callback(rows_completed, total_rows)
        Returns the number of rows ingested.
        """
        csvf = Path(csv_file)
        con = self._con()
        init_db(con)

        if report.lower() == "epraccur":
            rows = read_csv_rows(csvf)
            prepared = self._prepare_rows_epraccur(rows)
        else:
            raise ValueError(f"Unsupported report for local CSV load: {report}")

        upsert_practices(con, prepared, on_progress=on_progress)
        return len(prepared)
