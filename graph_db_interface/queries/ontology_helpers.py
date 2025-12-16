# To be imported into ..graph_db.py GraphDB class
import uuid

from typing import List, Optional, Union, Callable, TYPE_CHECKING
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI
from graph_db_interface.exceptions import InvalidInputError
from rdflib import Namespace

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def iri_exists(
    self: "GraphDB",
    iri: Union[str, IRI],
    as_sub: Optional[bool] = False,
    as_pred: Optional[bool] = False,
    as_obj: Optional[bool] = False,
    include_explicit: Optional[bool] = True,
    include_implicit: Optional[bool] = True,
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Check if an IRI exists as subject, predicate, or object.

    Args:
        iri (Union[str, IRI]): The IRI to check for existence.
        as_sub (Optional[bool]): If True, check existence as subject. Defaults to False.
        as_pred (Optional[bool]): If True, check existence as predicate. Defaults to False.
        as_obj (Optional[bool]): If True, check existence as object. Defaults to False.
        include_explicit (Optional[bool]): Include explicit triples (`FROM onto:explicit`). Defaults to True.
        include_implicit (Optional[bool]): Include inferred triples (`FROM onto:implicit`). Defaults to True.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if the IRI exists based on the specified criteria, False otherwise.

    Raises:
        InvalidInputError: If none of `as_sub`, `as_pred`, or `as_obj` is True.
    """

    # Check if either as_subject, as_predicate, or as_object is True
    if not (as_sub or as_pred or as_obj):
        raise InvalidInputError(
            "At least one of as_sub, as_pred, or as_obj must be True"
        )

    iri = IRI(iri)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    # Define potential query parts
    where_clauses = []
    if as_sub:
        where_clauses.append(f"{{{iri.n3()} ?p ?o . }}")
    if as_pred:
        where_clauses.append(f"{{?s {iri.n3()} ?o . }}")
    if as_obj:
        where_clauses.append(f"{{?s ?p {iri.n3()} . }}")

    query = SPARQLQuery.ask(
        where_clauses=where_clauses,
        named_graph=named_graph,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )
    result = self.query(query=query, update=False)
    if result is not None and result["boolean"]:
        self.logger.debug(f"Found IRI {iri}")
        return True

    self.logger.debug(f"Unable to find IRI {iri}")
    return False


def new_iri(
    self: "GraphDB",
    base: Union[str, IRI],
    schema: Optional[Callable[[], str]] = lambda: str(uuid.uuid4()),
) -> IRI:
    """
    Generate a new unique IRI within the graph database's namespace.

    Args:
        base (Union[str, IRI]): The base IRI or namespace for the new IRI.
        schema (Optional[Callable[[], str]]): A callable that generates the unique part
            of the IRI. Defaults to a UUID4 string generator.

    Returns:
        IRI: A new unique IRI.
    """
    if schema() == schema():
        raise ValueError("Schema function must produce different values on each call")

    base = base.rstrip("#")
    if "#" in base:
        base, fragment_start = base.split("#", 1)
    else:
        fragment_start = ""

    def new() -> str:
        return IRI(value=fragment_start + schema(), base=base)

    iri = new()

    while self.iri_exists(iri, as_sub=True, as_pred=True, as_obj=True):
        iri = new()
    return iri


def new_blank_id(
    self: "GraphDB",
    schema: Optional[Callable[[], str]] = lambda: f"genid-{uuid.uuid4()}",
) -> str:
    """
    Generate a new unique blank node identifier.

    Args:
        schema (Optional[Callable[[], str]]): A callable that generates the blank node ID.
            Defaults to a `genid-<uuid4>` format.

    Returns:
        str: A new unique blank node identifier.
    """
    if schema() == schema():
        raise ValueError("Schema function must produce different values on each call")

    genid = schema()

    while genid in self._blank_ids:
        genid = schema()

    self._blank_ids.add(genid)
    return genid


def is_subclass(
    self: "GraphDB",
    subclass_iri: Union[str, IRI],
    class_iri: Union[str, IRI],
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Check whether one class is a subclass of another (`rdfs:subClassOf`).

    Asks for `subclass_iri rdfs:subClassOf class_iri`

    Args:
        subclass_iri (Union[str, IRI]): The IRI of the potential subclass.
        class_iri (Union[str, IRI]): The IRI of the potential superclass.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if `subclass_iri` is a subclass of `class_iri`, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph
    return self.triple_exists(
        (subclass_iri, "rdfs:subClassOf", class_iri), named_graph=named_graph
    )


def owl_is_named_individual(
    self: "GraphDB",
    iri: Union[str, IRI],
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Check if the given IRI corresponds to an OWL named individual.

    Asks for `iri rdf:type owl:NamedIndividual`.

    Args:
        iri (Union[str, IRI]): The IRI to check.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if the IRI is a named individual, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph
    return self.triple_exists(
        (iri, "rdf:type", "owl:NamedIndividual"), named_graph=named_graph
    )


def owl_get_classes_of_individual(
    self: "GraphDB",
    instance_iri: Union[str, IRI],
    ignored_prefixes: Optional[List[Namespace]] = None,
    local_name: Optional[bool] = False,
    include_explicit: Optional[bool] = True,
    include_implicit: Optional[bool] = False,
    named_graph: Optional[Union[str, IRI]] = None,
) -> List[Union[IRI, str]]:
    """
    Get the OWL classes associated with a given individual.

    Builds a SPARQL query that returns the classes for an instance IRI and
    optionally filters out results by prefix or returns local names only.

    Args:
        instance_iri (Union[str, IRI]): IRI of the individual to inspect.
        ignored_prefixes (Optional[List[Namespace]]): Prefixes/namespaces to ignore
            when collecting classes. Defaults to ["owl", "rdfs"].
        local_name (Optional[bool]): If True, return only the local names of the classes. Defaults to False.
        include_explicit (Optional[bool]): Include explicit triples. Defaults to True.
        include_implicit (Optional[bool]): Include inferred triples. Defaults to False.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        List[Union[IRI, str]]: Class IRIs, or local names if `local_name=True`.
    """
    instance_iri = IRI(instance_iri)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph
    ignored_prefixes = (
        ignored_prefixes if ignored_prefixes is not None else ["owl", "rdfs"]
    )

    if len(ignored_prefixes) > 0:
        filter_conditions = (
            "FILTER ("
            + " && ".join(
                [
                    f"!STRSTARTS(STR(?class), STR({prefix}:))"
                    for prefix in ignored_prefixes
                ]
            )
            + ")"
        )
    else:
        filter_conditions = ""

    query = SPARQLQuery.select(
        variables=["?class"],
        where_clauses=[
            f"?class rdf:type owl:Class .",
            f"{instance_iri.n3()} rdf:type ?class .",
            filter_conditions,
        ],
        named_graph=named_graph,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )
    results = self.query(query=query)

    if results is None:
        return []

    if local_name:
        classes = [
            utils.get_local_name(result["class"]["value"])
            for result in results["results"]["bindings"]
        ]
        return classes

    classes = [
        IRI(result["class"]["value"]) for result in results["results"]["bindings"]
    ]
    return classes
