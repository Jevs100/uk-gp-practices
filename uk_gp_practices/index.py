"""Main interface for querying GP practice data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

from .db import connect, init_db, read_csv, upsert_practices
from .download import download_report
from .models import Practice
from .normalise import normalize_name, normalize_postcode
from .paths import csv_path, db_path as default_db_path


DEFAULT_REPORT = "epraccur"
DEFAULT_MAX_AGE = timedelta(days=1)


@dataclass(slots=True)
class PracticeIndex:
    """
    Main query interface for GP practice data.

    Backed by:
      - downloaded NHS ODS DSE CSV report(s)
      - local SQLite database for fast lookups
    """

    db_file: Path

    @classmethod
    def auto_update(
        cls,
        report: str = DEFAULT_REPORT,
        max_age: timedelta = DEFAULT_MAX_AGE,
    ) -> "PracticeIndex":
        idx = cls(db_file=default_db_path())
        idx.update_if_needed(report=report, max_age=max_age)
        return idx

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

        raw_rows = read_csv(csvf)
        prepared: list[dict[str, Any]] = []

        # Defensive mapping because CSV headers vary a bit across reports.
        for r in raw_rows:
            code = (
                r.get("Organisation Code")
                or r.get("organisation_code")
                or r.get("Org Code")
                or ""
            ).strip()
            name = (
                r.get("Name") or r.get("Organisation Name") or r.get("name") or ""
            ).strip()
            postcode = (r.get("Postcode") or r.get("postcode") or "").strip() or None
            town = (
                r.get("Town") or r.get("town") or r.get("City") or ""
            ).strip() or None
            status = (r.get("Status") or r.get("status") or "").strip() or None

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

        con = connect(self.db_file)
        try:
            init_db(con)
            upsert_practices(con, prepared)
        finally:
            con.close()

    def _con(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_file))
        con.row_factory = sqlite3.Row
        # Ensure schema exists even if DB file is new
        init_db(con)
        return con

    def get(self, organisation_code: str) -> Practice | None:
        """
        Fetch a single practice by its ODS organisation code.
        """
        code = organisation_code.strip()
        con = self._con()
        try:
            row = con.execute(
                "SELECT * FROM practices WHERE organisation_code = ?",
                (code,),
            ).fetchone()
            if not row:
                return None
            return Practice(
                organisation_code=row["organisation_code"],
                name=row["name"],
                postcode=row["postcode"],
                town=row["town"],
                status=row["status"],
                raw=dict(row),
            )
        finally:
            con.close()

    def search(
        self,
        name: str | None = None,
        postcode: str | None = None,
        town: str | None = None,
        status: str | None = "Active",
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
        try:
            rows = con.execute(sql, params).fetchall()
            return [
                Practice(
                    organisation_code=r["organisation_code"],
                    name=r["name"],
                    postcode=r["postcode"],
                    town=r["town"],
                    status=r["status"],
                    raw=dict(r),
                )
                for r in rows
            ]
        finally:
            con.close()

    def _prepare_rows(self, raw_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
        def norm_key(k: str) -> str:
            return "".join(ch for ch in k.strip().lower() if ch.isalnum())

        def get_any(row: dict[str, str], *candidates: str) -> str:
            # Build a normalized-key dict once per row
            normed = {norm_key(k): v for k, v in row.items()}
            for c in candidates:
                v = normed.get(norm_key(c))
                if v is not None:
                    return v
            return ""

        prepared: list[dict[str, Any]] = []

        for r in raw_rows:
            code = get_any(
                r,
                "Organisation Code",
                "Org Code",
                "ORG_CODE",
                "ORGANISATION_CODE",
                "CODE",
                "ODS_CODE",
            ).strip()

            name = get_any(
                r,
                "Name",
                "Organisation Name",
                "ORG_NAME",
                "ORGANISATION_NAME",
                "PRACTICE_NAME",
            ).strip()

            postcode = (
                get_any(
                    r,
                    "Postcode",
                    "POSTCODE",
                    "POST_CODE",
                    "ZIP",
                ).strip()
                or None
            )

            town = (
                get_any(
                    r,
                    "Town",
                    "CITY",
                    "POST_TOWN",
                    "POSTTOWN",
                ).strip()
                or None
            )

            status = (
                get_any(
                    r,
                    "Status",
                    "STATUS",
                    "CURRENT_STATUS",
                ).strip()
                or None
            )

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
