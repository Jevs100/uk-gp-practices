"""Query cached UK GP practice records from supported public NHS sources."""

from importlib.metadata import version, PackageNotFoundError

from .index import PracticeIndex
from .models import Practice

try:
    __version__ = version("uk-gp-practices")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["Practice", "PracticeIndex", "__version__"]
