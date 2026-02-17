"""UK GP Practices ODS data handling."""

from .index import PracticeIndex
from .models import Practice

__all__ = ["Practice", "PracticeIndex"]
