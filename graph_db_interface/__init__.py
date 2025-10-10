import logging
LOGGER = logging.getLogger(__name__)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))

LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)

from .sparql_query import SPARQLQuery
from .graph_db import GraphDB

__all__ = [
    "GraphDB",
    "SPARQLQuery"
]