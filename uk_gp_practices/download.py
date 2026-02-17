from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import time

import httpx


DEFAULT_DSE_REPORT_URL = "https://odsdatasearchandexport.nhs.uk/api/getReport"


@dataclass(frozen=True, slots=True)
class DownloadResult:
    report: str
    path: Path
    bytes_written: int
    url: str


def download_report(
    report: str,
    dest: Path,
    timeout: float = 60.0,
    retries: int = 4,
    backoff_seconds: float = 0.6,
) -> DownloadResult:
    """
    Download an NHS ODS Data Search & Export (DSE) report as a CSV file.

    Retries on transient network errors.
    Allows overriding the URL via env var UK_GP_PRACTICES_DSE_URL for testing.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    url = os.getenv("UK_GP_PRACTICES_DSE_URL", DEFAULT_DSE_REPORT_URL)
    params = {"report": report}

    last_exc: Exception | None = None

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for attempt in range(1, retries + 2):
            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                data = resp.content
                dest.write_bytes(data)
                return DownloadResult(
                    report=report, path=dest, bytes_written=len(data), url=str(resp.url)
                )
            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.RemoteProtocolError,
                httpx.NetworkError,
            ) as exc:
                last_exc = exc
                if attempt > retries + 1:
                    break
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    raise RuntimeError(
        f"Failed to download report '{report}' from {url}. "
        f"This is usually DNS/network/firewall. Original error: {last_exc!r}"
    )
