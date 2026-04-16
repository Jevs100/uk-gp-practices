"""England and Wales GP practice source backed by NHS ODS epraccur."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..db import read_csv_rows
from ..download import DownloadResult, download_report
from ..normalise import normalize_name, normalize_postcode


class EnglandSource:
    """
    Download and parse the NHS ODS epraccur report for England and Wales.

    The epraccur CSV is headerless; columns are positional:
      0  = organisation_code
      1  = name
      7  = town
      9  = postcode
      12 = status
    """

    nation = "england"
    _report = "epraccur"

    def download(
        self,
        dest: Path,
        on_progress: Callable[[int, int | None], None] | None = None,
    ) -> DownloadResult:
        return download_report(report=self._report, dest=dest, on_progress=on_progress)

    def parse(self, path: Path) -> list[dict[str, Any]]:
        rows = read_csv_rows(path)
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
                    "nation": self.nation,
                }
            )

        return prepared
