"""Tools for normalizing data for searching."""

from __future__ import annotations

import re


_ws = re.compile(r"\s+")


def normalize_postcode(value: str | None) -> str | None:
    """
    Normalize a UK postcode into a consistent format for searching.

    Example:
        " sw1a  1aa " -> "SW1A1AA"
    """
    if not value:
        return None

    v = _ws.sub("", value.strip().upper())
    return v or None


def normalize_name(value: str | None) -> str | None:
    """
    Normalize a name for case-insensitive searching.

    Example:
        "  Castle   Medical " -> "castle medical"
    """
    if not value:
        return None

    v = _ws.sub(" ", value.strip())
    return v.lower() or None
