import logging
from typing import Union, Dict, Optional, Any
from rdflib import URIRef, Literal, XSD, Dataset
from rdflib.plugins.sparql.processor import prepareQuery
from urllib.parse import urlparse
from graph_db_interface.exceptions import (
    InvalidInputError,
    InvalidIRIError,
    InvalidQueryError,
)


LOGGER = logging.getLogger(__name__)


def validate_query(query: str):
    try:
        # Attempt to prepare the query
        prepareQuery(query)
        return True
    except Exception as e:
        error_message = f"SPAQRQL query validation failed: {e}"
        LOGGER.error(error_message)
        raise InvalidQueryError(error_message)


def validate_update_query(query: str):
    try:
        g = Dataset()
        g.update(query)
        return True
    except Exception as e:
        error_message = f"SPAQRQL update query validation failed: {e}"
        LOGGER.error(error_message)
        raise InvalidQueryError(error_message)


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
    # literal = escape_string_literal(literal)
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


def prepare_subject(sub: str, ensure_iri: bool = True) -> str:
    """
    Prepares and validates a subject string, ensuring it conforms to IRI (Internationalized Resource Identifier)
    standards if required.

    Args:
        sub (str): The subject string to validate and prepare.
        ensure_iri (bool, optional): If True, ensures the subject is a valid IRI. Defaults to True.

    Returns:
        str: The prepared subject string, either as an absolute IRI or as provided if valid.

    Raises:
        InvalidInputError: If the provided subject is not a string.
        InvalidIRIError: If the subject is not a valid IRI and `ensure_iri` is True.
    """
    if not type(sub) == str:
        raise InvalidInputError(f"Provided subject '{sub}' is not a string.")
    if is_iri(sub):
        return ensure_absolute(sub)
    elif is_shorthand_iri(sub):
        return sub
    else:
        if ensure_iri is True:
            raise InvalidIRIError(
                f"Provided subject '{sub}' is not a valid IRI. Ensure 'ensure_iri' is set correctly."
            )
        else:
            return sub


def prepare_predicate(pred: str, ensure_iri: bool = True) -> str:
    """
    Prepares a predicate string by validating and optionally ensuring it is an IRI (Internationalized Resource Identifier).

    Args:
        pred (str): The predicate to be validated and processed.
        ensure_iri (bool, optional): If True, ensures the predicate is a valid IRI. Defaults to True.

    Returns:
        str: The processed predicate, either as an absolute IRI or as provided if valid.

    Raises:
        InvalidInputError: If the provided predicate is not a string.
        InvalidIRIError: If `ensure_iri` is True and the provided predicate is not a valid IRI.
    """
    if not type(pred) == str:
        raise InvalidInputError(f"Provided subject '{pred}' is not a string.")
    if is_iri(pred):
        return ensure_absolute(pred)
    elif is_shorthand_iri(pred):
        return pred
    else:
        if ensure_iri is True:
            raise InvalidIRIError(
                f"Provided predicate '{pred}' is not a valid IRI. Ensure 'ensure_iri' is set correctly."
            )
        else:
            return pred


def prepare_object(
    obj: Any, as_string: bool = False, ensure_iri: bool = False
) -> Union[str, Literal]:
    """
    Prepares an object for use in a graph database context by ensuring it is in the
    correct format, such as an IRI (Internationalized Resource Identifier) or a Literal.

    Args:
        obj (Any): The object to be prepared. It can be a string, Literal, or any other type.
        as_string (bool, optional): If True, converts a Literal object to its string representation.
            Defaults to False.
        ensure_iri (bool, optional): If True, ensures that the provided object is a valid IRI.
            Raises an InvalidIRIError if the object is not a valid IRI. Defaults to False.

    Returns:
        Union[str, Literal]: The prepared object. This can be:
            - A string representing an absolute or shorthand IRI.
            - A Literal object or its string representation if `as_string` is True.

    Raises:
        InvalidIRIError: If `ensure_iri` is True and the provided object is not a valid IRI.
    """
    if ensure_iri:
        if not type(obj) == str:
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

    if type(obj) == str:
        if is_iri(obj):
            return ensure_absolute(obj)
        else:
            return obj

    if type(obj) == Literal:
        # TODO: How to handle string escapes, obj: Literal = escape_string_literal(obj)
        if as_string:
            return obj.n3()
        else:
            return obj

    return to_literal(obj, as_string=as_string)


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
