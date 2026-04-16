"""Data models for normalized GP practice records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any


@dataclass(frozen=True, slots=True)
class Practice:
    """
    Normalized GP practice record returned by PracticeIndex queries.

    raw contains the database row fields used to build the normalized model.
    """

    organisation_code: str
    name: str

    postcode: str | None
    town: str | None
    status: str | None
    nation: str | None = None

    raw: Optional[dict[str, Any]] = None
