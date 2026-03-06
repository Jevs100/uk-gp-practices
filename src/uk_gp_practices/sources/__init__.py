from .base import Source
from .england import EnglandSource
from .scotland import ScotlandSource

ALL_SOURCES = [EnglandSource(), ScotlandSource()]

__all__ = ["Source", "EnglandSource", "ScotlandSource", "ALL_SOURCES"]
