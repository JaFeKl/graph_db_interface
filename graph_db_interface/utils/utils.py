import logging
import re
from typing import Union
from rdflib import URIRef, Literal, XSD


LOGGER = logging.getLogger(__name__)


def ensure_absolute(iri: str):
    """Ensure the IRI is in absolute form enclosed in <>.

    If the IRI is already absolute (i.e., enclosed in <>), it returns as is.
    Otherwise, it wraps the IRI in <>.

    Args:
        iri (str): The input IRI.

    Returns:
        str: The absolute IRI in <> format.
    """
    iri = iri.strip()

    # Check if already enclosed in <>
    if iri.startswith("<") and iri.endswith(">"):
        return iri

    # Validate and convert to absolute IRI using rdflib
    uri = URIRef(iri)  # Ensures it's a valid IRI
    return f"<{uri}>"


def strip_angle_brackets(iri: str) -> str:
    if iri.startswith("<") and iri.endswith(">"):
        clean_iri = iri[1:-1]  # Remove first and last character
    else:
        clean_iri = iri  # Keep it unchanged
    return clean_iri


def to_xsd_literal(value):
    return Literal(value)


def from_xsd_literal(value: str, datatype: str):
    literal = Literal(value, datatype=datatype)
    return literal.value  # If it's not a literal, return as is (e.g., IRI or variable)


def get_local_name(iri: str):
    iri = URIRef(iri)
    # If there's a fragment (i.e., the part after '#')
    if iri.fragment:
        return iri.fragment

    # Otherwise, split by '/' and return the last segment
    return iri.split("/")[-1]


def extract_where_clause(query: str) -> str:
    # Regex to match everything from WHERE to the closing brace of the WHERE clause
    match = re.search(r"\bWHERE\s*{(.+?)}", query, re.DOTALL)

    if match:
        # Return the content inside the curly braces, which is the WHERE clause
        return match.group(0).strip()

    return "No WHERE clause found"


def insert_before_where_clause(query: str, from_statement: str) -> str:
    # Find the position of the WHERE clause
    match = re.search(r"\bWHERE\s*{", query, re.DOTALL)

    if match:
        # Insert the FROM statement before the WHERE clause
        where_pos = match.start()  # Start of the WHERE clause
        query_with_from = query[:where_pos] + f"{from_statement}\n" + query[where_pos:]
        return query_with_from

    LOGGER.warning(
        "Unable to insert clause before WHERE, no WHERE found in the given query."
        " Returning the original query..."
    )
    return query


def escape_string_literal(value: Union[str, Literal]) -> str:
    if (
        isinstance(value, Literal)
        and isinstance(value.value, str)
        # Try to prevent double escaping.
        and not '\\"' in value
    ):
        value = value.replace('"', '\\"')
        return Literal(f'"{value}"', datatype=XSD.string)

    return value
