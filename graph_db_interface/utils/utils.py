import logging
from typing import TypeAlias, Union, Optional, Any, Tuple
from rdflib import Literal, XSD, Dataset
from rdflib.plugins.sparql.processor import prepareQuery
from graph_db_interface.exceptions import (
    InvalidInputError,
    InvalidQueryError,
)
from graph_db_interface.utils.iri import IRI


LOGGER = logging.getLogger(__name__)

Triple: TypeAlias = Tuple[
    Union[str, IRI],
    Union[str, IRI],
    Union[str, IRI, Literal],
]

PartialTriple: TypeAlias = Tuple[
    Optional[Union[str, IRI]],
    Optional[Union[str, IRI]],
    Optional[Union[str, IRI, Literal]],
]


def sanitize_triple(
    triple: Triple,
    allow_partial: Optional[bool] = False,
) -> Triple:
    """
    Validates and converts the components of a triple to their appropriate types.

    - A valid triple must have three entries (subject, predicate, object) defined.
    - If allow_partial is false, no entry may be None. Otherweise, at least one entry
    must be not None.
    - Subject and predicate must be valid IRIs.
    - The object can be either an IRI or a Literal
        - If the object is of type IRI, URIRef, or str, it will be converted to IRI.
        - If an object of type str should be treated as a Literal, it must be
        explicitly converted to Literal before passing.
        - If the object is of any other type, it will be converted to a Literal.

    Args:
        triple (Triple): The triple to validate and convert.
        allow_partial (Optional[bool]): Whether to allow partial triples. Defaults to False.

    Returns:
        Triple: The validated and converted triple.

    Raises:
        InvalidInputError: If the triple does not have three or too many None entries.
        TypeError: If subject or predicate are not of type str, URIRef, or IRI.
        InvalidIRIError: If object is of type str or URIRef (except Literal) and not in a valid IRI format.
    """

    if allow_partial:
        if len(triple) != 3 or all(e is None for e in triple):
            error_message = f"Triple requires three components, at least one of which is not None: {triple}"
            LOGGER.error(error_message)
            raise InvalidInputError(error_message)
    else:
        if len(triple) != 3 or any(e is None for e in triple):
            error_message = (
                f"Triple requires three components, neither of which is None: {triple}"
            )
            LOGGER.error(error_message)
            raise InvalidInputError(error_message)

    sub, pred, obj = triple

    # Sub must be IRI
    if sub is not None and not isinstance(sub, IRI):
        sub = IRI(sub)

    # Pred must be IRI
    if pred is not None and not isinstance(pred, IRI):
        pred = IRI(pred)

    # Pred must be IRI or Literal
    # If str, try IRI first, then Literal. Otherwise, Literal.
    if obj is not None and not isinstance(obj, (IRI, Literal)):
        if isinstance(obj, str):
            try:
                obj = IRI(str(obj))
            except Exception as e:
                error_message = (
                    "Object is of type str but cannot be converted to IRI. If object "
                    + f"is a <Literal>, explicitly convert before passing: {obj} ({e})"
                )
                LOGGER.error(error_message)
                raise type(e)(error_message) from e
        else:
            obj = Literal(obj)
    return sub, pred, obj


def triple_to_string(
    triple: Triple,
    line_end: Optional[str] = None,
) -> str:
    """Convert a triple to its string representation suitable for SPARQL queries.

    Args:
        triple (Triple): The triple to convert.
    Returns:
        str: The string representation of the triple.
    """
    sub, pred, obj = triple
    return f"{sub.n3()} {pred.n3()} {obj.n3()}" + (f" {line_end}" if line_end else "")


def validate_query(query: str):
    """
    Validate a SPARQL SELECT/ASK query string by parsing it.

    Args:
        query (str): The SPARQL query to validate.

    Returns:
        bool: True if parsing succeeds.

    Raises:
        InvalidQueryError: If parsing fails.
    """
    try:
        # Attempt to prepare the query
        prepareQuery(query)
        return True
    except Exception as e:
        error_message = f"SPAQRQL query validation failed: {e}"
        LOGGER.error(error_message)
        raise InvalidQueryError(error_message)


def validate_update_query(
    query: str,
):
    """
    Validate a SPARQL UPDATE string by applying it to a temporary dataset.

    Args:
        query (str): The SPARQL UPDATE string to validate.

    Returns:
        bool: True if validation succeeds.

    Raises:
        InvalidQueryError: If validation fails.
    """
    try:
        g = Dataset()
        g.update(query)
        return True
    except Exception as e:
        error_message = f"SPAQRQL update query validation failed: {e}"
        LOGGER.error(error_message)
        raise InvalidQueryError(error_message)


def to_literal(
    value: Any,
    datatype: Optional[str] = None,
    as_string: Optional[bool] = False,
) -> Union[Literal, str]:
    """
    Convert a Python value to an XSD literal.

    Args:
        value (Any): The Python value to convert.
        datatype: (Optional[str]) Optional XSD datatype to use; inferred as `XSD.string` for strings.
        as_string (Optional[bool]): If True, return the N3 string form of the literal. Defaults to False.

    Returns:
        Union[Literal, str]: The `rdflib.Literal` or its N3 string form when `as_string=True`.
    """
    if isinstance(value, str) and datatype is None:
        datatype = XSD.string
    literal = Literal(value, datatype=datatype)
    # literal = escape_string_literal(literal)
    if as_string:
        return literal.n3()
    return literal


def from_xsd_literal(
    value: str,
    datatype: str,
):
    """
    Convert an XSD-typed string value to a Python value.

    Args:
        value (str): The lexical form of the literal.
        datatype (str): The XSD datatype IRI.

    Returns:
        Any: The converted Python value.
    """
    literal = Literal(value, datatype=datatype)
    return literal.toPython()


def convert_query_result_to_python_type(
    result_binding: dict,
) -> Any:
    """
    Convert a SPARQL binding entry to a Python value.

    Args:
        result_binding (dict): A single binding dict (e.g., `{ 'type': 'literal', ... }`).

    Returns:
        Any: A Python value converted from the binding, or the raw string when not typed.
    """
    type = result_binding.get("type")
    if type == "literal" and "datatype" in result_binding:
        return from_xsd_literal(result_binding["value"], result_binding["datatype"])
    elif type == "uri":
        return IRI(result_binding["value"])
    else:
        # If no datatype is provided, return the value as is
        return result_binding["value"]


def get_local_name(
    iri: str,
) -> str:
    """
    Extract the local name from an IRI.

    Prefers the fragment after `#` when present; otherwise returns the last path
    segment after `/`.

    Args:
        iri (str): The input IRI (full or shorthand acceptable).

    Returns:
        str: The local name component.
    """
    iri = IRI(iri)
    # If there's a fragment (i.e., the part after '#')
    if iri.fragment:
        return iri.fragment

    # Otherwise, split by '/' and return the last segment
    return iri.split("/")[-1]


def encapsulate_named_graph(
    named_graph: IRI,
    content: str,
) -> str:
    """
    Encapsulates the given content within a named graph block if a named graph is provided.

    Args:
        named_graph (IRI): The IRI of the named graph. If None, the content is returned as is.
        content (str): The SPARQL content to encapsulate.

    Returns:
        str: The encapsulated content or the original content if no named graph is provided.
    """
    if named_graph:
        return f"""GRAPH {named_graph.n3()} {{
    {content}
}}"""
    return content
