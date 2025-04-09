import logging
import re
from typing import Union, Dict, Optional, Any
from rdflib import URIRef, Literal, XSD, Dataset
from rdflib.plugins.sparql.processor import prepareQuery
from urllib.parse import urlparse
from pyparsing import ParseBaseException
from graph_db_interface.exceptions import InvalidIRIError


LOGGER = logging.getLogger(__name__)


def validate_query(query: str) -> bool:
    """Validate a SPARQL query.

    Args:
        query (str): The SPARQL query to validate.

    Returns:
        bool: True if the query is valid, False otherwise.
    """
    try:
        # Attempt to prepare the query
        prepareQuery(query)
        return True
    except Exception as e:
        LOGGER.error(f"Invalid SPARQL query: {e}")
        return False


def validate_update_query(query: str) -> bool:
    try:
        g = Dataset()
        g.update(query)
        return True
    except ParseBaseException as e:
        LOGGER.error(f"Parse error: {e}")
        return False
    except Exception as e:
        # Could be other issues (like malformed URIs)
        LOGGER.error(f"General error: {e}")
        return False


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

    return f"<{iri}>"


def is_absolute(iri: str) -> bool:
    """Check if the IRI is in absolute form.

    Args:
        iri (str): The input IRI.

    Returns:
        bool: True if the IRI is absolute, False otherwise.
    """
    return iri.startswith("<") and iri.endswith(">")


def strip_angle_brackets(iri: str) -> str:
    """Strip the angle brackets from the IRI if present.

    Args:
        iri (str): The input IRI.

    Returns:
        str: The IRI without angle brackets.
    """
    # Remove angle brackets if they exist
    return iri[1:-1] if is_absolute(iri) else iri


def to_literal(value, datatype=None, as_string: bool = False) -> Union[Literal, str]:
    """Convert a Python value to its corresponding XSD literal representation."""
    if isinstance(value, str) and datatype is None:
        datatype = XSD.string
    literal = Literal(value, datatype=datatype)
    literal = escape_string_literal(literal)
    if as_string:
        return literal.n3()
    return literal


def from_xsd_literal(value: str, datatype: str):
    """
    Convert a string value to its corresponding Python type based on the XSD datatype.
    """
    literal = Literal(value, datatype=datatype)
    return literal.toPython()


def convert_query_result_to_python_type(result_binding: dict) -> Any:
    """Convert a SPARQL query result binding to its corresponding Python type."""
    type = result_binding.get("type")
    if type == "literal":
        return from_xsd_literal(result_binding["value"], result_binding["datatype"])
    else:
        # If no datatype is provided, return the value as is
        return result_binding["value"]


def get_local_name(iri: str):
    iri = URIRef(strip_angle_brackets(iri))
    # If there's a fragment (i.e., the part after '#')
    if iri.fragment:
        return iri.fragment

    # Otherwise, split by '/' and return the last segment
    return iri.split("/")[-1]


def escape_string_literal(value: Union[str, Literal]) -> Union[Literal, str]:
    if (
        isinstance(value, Literal)
        and isinstance(value.value, str)
        # Try to prevent double escaping.
        and not '\\"' in value
    ):
        value = value.replace('"', '\\"')
        return Literal(f'"{value}"', datatype=XSD.string)

    return value


def is_iri(value: str) -> bool:
    """Checks if the provided value is a valid IRI."""
    stripped = strip_angle_brackets(value)
    parseresult = urlparse(stripped)
    if not parseresult.scheme or not parseresult.netloc:
        return False
    return True


def is_shorthand_iri(value: str, prefixes: Optional[Dict[str, str]] = None) -> bool:
    """
    Checks if the provided value is in the form of a shorthand IRI (prefix:localName).

    A shorthand IRI consists of a prefix and a local name separated by a colon (":").
    This function verifies if the given value matches this format and if a dict of prefixes
    is given in the provided dictionary of prefixes.

        value (str): The string to check if it is a shorthand IRI.
        prefixes (Optional[Dict[str, str]]): A dictionary mapping prefixes to their full IRIs.

        bool: True if the value is in the form of a valid shorthand IRI, False otherwise.
    """
    if is_iri(value):
        return False
    elif ":" in value:
        # Check if value can be splitted exactly in two parts
        if len(value.split(":")) != 2:
            return False
        prefix = value.split(":")[0]
        if prefixes:
            # Check if the prefix exists in the provided prefixes dictionary
            if prefix in prefixes:
                return True
            else:
                LOGGER.warning(
                    f"Prefix '{prefix}' not found in the provided prefixes dictionary."
                )
                return False
        else:
            # If no prefixes are provided, just check the format
            return True
    else:
        return False


def prepare_object(
    obj: Any, as_string: bool = False, ensure_iri: bool = False
) -> Union[str, Literal]:
    """Prepare an object for SPARQL queries."""
    if ensure_iri:
        if not isinstance(obj, str):
            raise InvalidIRIError(
                f"Provided object '{obj}' is not a string. Cannot be a valid IRI."
            )
        if is_iri(obj):
            return ensure_absolute(obj)
        elif is_shorthand_iri(obj):
            return obj
        else:
            raise InvalidIRIError(
                f"Provided object '{obj}' is not a valid IRI. Ensure 'ensure_iri' is set correctly."
            )

    if isinstance(obj, str):
        if is_iri(obj):
            return ensure_absolute(obj)
        elif is_shorthand_iri(obj):
            return obj

    if isinstance(obj, Literal):
        obj: Literal = escape_string_literal(obj)
        if as_string:
            obj = obj.n3()
        else:
            return obj

    return to_literal(obj, as_string=as_string)


def prepare_subject_or_predicate(
    subject_or_predicate: str, ensure_iri: bool = True
) -> str:
    if is_iri(subject_or_predicate):
        return ensure_absolute(subject_or_predicate)
    elif is_shorthand_iri(subject_or_predicate):
        return subject_or_predicate
    else:
        if ensure_iri is True:
            raise InvalidIRIError(
                f"Provided subject or predicate '{subject_or_predicate}' is not a valid IRI."
            )
        else:
            return subject_or_predicate


def encapsulate_named_graph(named_graph: Optional[str], content: str) -> str:
    """
    Encapsulates the given content within a named graph block if a named graph is provided.

    Args:
        named_graph (Optional[str]): The IRI of the named graph. If None, the content is returned as is.
        content (str): The SPARQL content to encapsulate.

    Returns:
        str: The encapsulated content or the original content if no named graph is provided.
    """
    if named_graph:
        named_graph = ensure_absolute(named_graph)
        return f"""
GRAPH {named_graph} {{
    {content}
}}"""
    return content
