"""Models representing GP practice data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any


@dataclass(frozen=True, slots=True)
class Practice:
    """
    Represents a UK GP Practice (surgery) record.

    This is based on NHS ODS Data Search & Export (DSE) CSV reports such as `epraccur`.
    """

    organisation_code: str
    name: str

    postcode: str | None
    town: str | None
    status: str | None

    raw: Optional[dict[str, Any]] = None
