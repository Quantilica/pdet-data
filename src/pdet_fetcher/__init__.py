from importlib.metadata import PackageNotFoundError, version

from quantilica.core.logging import get_logger

try:
    __version__ = version("pdet-fetcher")
except PackageNotFoundError:
    __version__ = "0.0.0"

logger = get_logger(__name__)


from .fetch import (
    connect,
    fetch_caged,
    fetch_caged_2020,
    fetch_caged_2020_docs,
    fetch_caged_docs,
    fetch_rais,
    fetch_rais_docs,
    list_caged,
    list_caged_2020,
    list_caged_2020_docs,
    list_caged_docs,
    list_rais,
    list_rais_docs,
)
from .wrangling import convert_caged, convert_rais, extract_columns_for_dataset

__all__ = [
    "__version__",
    "connect",
    "convert_caged",
    "convert_rais",
    "extract_columns_for_dataset",
    "fetch_caged",
    "fetch_caged_2020",
    "fetch_caged_2020_docs",
    "fetch_caged_docs",
    "fetch_rais",
    "fetch_rais_docs",
    "list_caged",
    "list_caged_2020",
    "list_caged_2020_docs",
    "list_caged_docs",
    "list_rais",
    "list_rais_docs",
]
