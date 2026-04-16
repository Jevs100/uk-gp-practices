"""Protocol implemented by each downloadable GP practice data source."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from ..download import DownloadResult


@runtime_checkable
class Source(Protocol):
    """
    Download and parse GP practice records from one source/nation.

    Implementors must provide:
      - nation: identifier string (e.g. "england", "scotland", "northern_ireland")
      - download(): fetch the remote data to a local file
      - parse(): convert the local file into normalised row dicts
    """

    nation: str

    def download(
        self,
        dest: Path,
        on_progress: Callable[[int, int | None], None] | None = None,
    ) -> DownloadResult: ...

    def parse(self, path: Path) -> list[dict[str, Any]]: ...
