"""Paths for storing cached data and reports."""

from __future__ import annotations

from pathlib import Path
from platformdirs import user_cache_dir


APP_NAME = "uk-gp-practices"


def cache_dir() -> Path:
    """
    Returns the cache directory for the package.

    Example (Linux):
        ~/.cache/uk-gp-practices/
    """
    p = Path(user_cache_dir(APP_NAME))
    p.mkdir(parents=True, exist_ok=True)
    return p


def db_path() -> Path:
    """Default SQLite database path."""
    return cache_dir() / "practices.sqlite3"


def csv_path(report: str) -> Path:
    """Path for storing a downloaded report CSV file."""
    safe = report.replace("/", "_").replace("\\", "_").replace("..", "_")
    return cache_dir() / f"{safe}.csv"
