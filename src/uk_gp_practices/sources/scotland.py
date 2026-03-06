"""Scotland GP practice data source — Public Health Scotland (NHS Scotland Open Data)."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from ..db import read_csv_dicts
from ..download import DownloadResult, download_url
from ..normalise import normalize_name, normalize_postcode


class ScotlandSource:
    """
    Downloads and parses the NHS Scotland GP practice contact details CSV.

    The CSV has headers; relevant columns:
      PracticeCode    → organisation_code
      GPPracticeName  → name
      Postcode        → postcode
      AddressLine3    → town (falls back to last non-empty of AddressLine1–AddressLine4)
    """

    nation = "scotland"
    _DATASET_ID = "f23655c3-6e23-4103-a511-a80d998adb90"
    _CKAN_API = "https://www.opendata.nhs.scot/api/3/action/package_show"
    _ENV_OVERRIDE = "UK_GP_PRACTICES_SCOTLAND_URL"

    def _latest_url(self) -> str:
        resp = httpx.get(
            self._CKAN_API,
            params={"id": self._DATASET_ID},
            timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        data = resp.json()
        resources = data["result"]["resources"]
        # Find the most recent CSV resource (sorted by last_modified descending)
        csv_resources = [
            r for r in resources
            if r.get("format", "").upper() == "CSV"
        ]
        if not csv_resources:
            raise RuntimeError("No CSV resources found in Scotland CKAN dataset")
        csv_resources.sort(key=lambda r: r.get("last_modified") or r.get("created", ""), reverse=True)
        return csv_resources[0]["url"]

    def download(
        self,
        dest: Path,
        on_progress: Callable[[int, int | None], None] | None = None,
    ) -> DownloadResult:
        url = os.getenv(self._ENV_OVERRIDE) or self._latest_url()
        return download_url(url, dest, on_progress=on_progress)

    def parse(self, path: Path) -> list[dict[str, Any]]:
        rows = read_csv_dicts(path)
        prepared: list[dict[str, Any]] = []

        for r in rows:
            code = (r.get("PracticeCode") or "").strip()
            name = (r.get("GPPracticeName") or "").strip()
            postcode = (r.get("Postcode") or "").strip() or None

            # Town: prefer AddressLine3, fall back to last non-empty address line
            town = (r.get("AddressLine3") or "").strip() or None
            if not town:
                for field in ("AddressLine4", "AddressLine2", "AddressLine1"):
                    candidate = (r.get(field) or "").strip()
                    if candidate:
                        town = candidate
                        break

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
                    "status": None,
                    "nation": self.nation,
                }
            )

        return prepared
