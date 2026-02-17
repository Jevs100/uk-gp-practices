from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import time

import httpx


# Prefer the www host. Some networks/DNS resolvers fail on the non-www host.
DEFAULT_DSE_REPORT_URLS = [
    "https://www.odsdatasearchandexport.nhs.uk/api/getReport",
    "https://odsdatasearchandexport.nhs.uk/api/getReport",
]


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
    retries: int = 3,
    backoff_seconds: float = 0.6,
) -> DownloadResult:
    """
    Download an NHS ODS Data Search & Export predefined report as a CSV.

    Override endpoint (single URL) with:
        UK_GP_PRACTICES_DSE_URL
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    override = os.getenv("UK_GP_PRACTICES_DSE_URL")
    candidate_urls = [override] if override else DEFAULT_DSE_REPORT_URLS

    params = {"report": report}
    last_exc: Exception | None = None

    for base_url in candidate_urls:
        if not base_url:
            continue

        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            for attempt in range(1, retries + 2):
                try:
                    resp = client.get(base_url, params=params)
                    resp.raise_for_status()
                    data = resp.content
                    dest.write_bytes(data)
                    return DownloadResult(
                        report=report,
                        path=dest,
                        bytes_written=len(data),
                        url=str(resp.url),
                    )
                except (
                    httpx.ConnectError,
                    httpx.ReadTimeout,
                    httpx.RemoteProtocolError,
                    httpx.NetworkError,
                ) as exc:
                    last_exc = exc
                    time.sleep(backoff_seconds * (2 ** (attempt - 1)))
                except httpx.HTTPStatusError as exc:
                    # If we reached the server but got a real HTTP error, don't keep retrying other hosts forever.
                    raise RuntimeError(
                        f"Failed to download report '{report}' from {base_url}: {exc.response.status_code}"
                    ) from exc

    raise RuntimeError(
        f"Failed to download report '{report}'. Tried: {', '.join([u for u in candidate_urls if u])}. "
        f"Last error: {last_exc!r}"
    )
