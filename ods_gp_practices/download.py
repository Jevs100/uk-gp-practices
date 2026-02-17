from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx


DSE_REPORT_URL = "https://odsdatasearchandexport.nhs.uk/api/getReport"


@dataclass(frozen=True, slots=True)
class DownloadResult:
    report: str
    path: Path
    bytes_written: int


def download_report(report: str, dest: Path, timeout: float = 60.0) -> DownloadResult:
    """
    Download an NHS ODS Data Search & Export (DSE) report as a CSV file.

    Example report codes:
        - epraccur (GP Practices / Prescribing Cost Centres)
        - egpcur   (GP practitioners)

    Writes the raw CSV bytes to `dest`.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    params = {"report": report}

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        resp = client.get(DSE_REPORT_URL, params=params)
        resp.raise_for_status()
        data = resp.content

    dest.write_bytes(data)

    return DownloadResult(report=report, path=dest, bytes_written=len(data))
