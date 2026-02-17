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

    postcode: Optional[str] = None
    town: Optional[str] = None
    status: Optional[str] = None


    raw: Optional[dict[str, Any]] = None
