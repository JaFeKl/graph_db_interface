import logging
from .graph_db_interface import GraphDB, SPARQLQuery


LOGGER = logging.getLogger(__name__)


handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))

LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)

__all__ = [
    "GraphDB",
    "SPARQLQuery"
]