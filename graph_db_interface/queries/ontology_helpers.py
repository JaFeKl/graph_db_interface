# To be imported into ..graph_db.py GraphDB class

from typing import List, Optional, TYPE_CHECKING
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def iri_exists(
    self: "GraphDB",
    iri: str,
    as_sub: bool = False,
    as_pred: bool = False,
    as_obj: bool = False,
    include_explicit: bool = True,
    include_implicit: bool = True,
) -> bool:
    """
    Checks if a given IRI exists in the graph database as a subject, predicate, or object.

    Args:
        iri (str): The IRI to check for existence.
        as_sub (bool, optional): If True, checks if the IRI exists as a subject. Defaults to False.
        as_pred (bool, optional): If True, checks if the IRI exists as a predicate. Defaults to False.
        as_obj (bool, optional): If True, checks if the IRI exists as an object. Defaults to False.
        include_explicit (bool, optional): If True, includes explicitly defined triples in the query. Defaults to True.
        include_implicit (bool, optional): If True, includes implicitly inferred triples in the query. Defaults to True.

    Returns:
        bool: True if the IRI exists in the graph database based on the specified criteria, False otherwise.

    Raises:
        InvalidInputError: If none of `as_sub`, `as_pred`, or `as_obj` is set to True.
    """

    # Check if either as_subject, as_predicate, or as_object is True
    if not (as_sub or as_pred or as_obj):
        raise InvalidInputError(
            "At least one of as_sub, as_pred, or as_obj must be True"
        )

    # Define potential query parts
    where_clauses = []
    if as_sub:
        sub = utils.prepare_subject(iri, ensure_iri=True)
        where_clauses.append(f"{{{sub} ?p ?o . }}")
    if as_pred:
        pred = utils.prepare_predicate(iri, ensure_iri=True)
        where_clauses.append(f"{{?s {pred} ?o . }}")
    if as_obj:
        obj = utils.prepare_object(iri, as_string=True)
        where_clauses.append(f"{{?s ?p {obj} . }}")

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )

    query.add_ask_block(
        where_clauses=where_clauses,
    )

    query_string = query.to_string(validate=True)

    result = self.query(
        query=query_string,
        update=False,
    )
    if result is not None and result["boolean"]:
        self.logger.debug(f"Found IRI {iri}")
        return True

    self.logger.debug(f"Unable to find IRI {iri}")
    return False


def is_subclass(self: "GraphDB", subclass_iri: str, class_iri: str) -> bool:
    """
    Determines whether a given class (subclass_iri) is a subclass of another class (class_iri)
    based on the "rdfs:subClassOf" relationship.

    Args:
        subclass_iri (str): The IRI of the potential subclass.
        class_iri (str): The IRI of the potential superclass.

    Returns:
        bool: True if subclass_iri is a subclass of class_iri, False otherwise.
    """
    return self.triple_exists(subclass_iri, "rdfs:subClassOf", class_iri)


def owl_is_named_individual(self: "GraphDB", iri: str) -> bool:
    """
    Checks if the given IRI corresponds to an OWL named individual.

    This method verifies whether the provided IRI is explicitly defined as
    an `owl:NamedIndividual` in the RDF graph by checking for the existence
    of the triple (IRI, rdf:type, owl:NamedIndividual). If the triple does
    not exist, a warning is logged.

    Args:
        iri (str): The IRI to be checked.

    Returns:
        bool: True if the IRI is a named individual, False otherwise.
    """
    if not self.triple_exists(iri, "rdf:type", "owl:NamedIndividual"):
        self.logger.debug(f"IRI {iri} is not a named individual!")
        return False
    return True


def owl_get_classes_of_individual(
    self: "GraphDB",
    instance_iri: str,
    ignored_prefixes: Optional[List[str]] = None,
    local_name: bool = False,
    include_explicit=True,
    include_implicit=False,
) -> List[str]:
    """
    Retrieves the OWL classes associated with a given individual (instance IRI)
    from a graph database.

    Args:
        instance_iri (str): The IRI of the individual whose classes are to be retrieved.
        ignored_prefixes (Optional[List[str]]): A list of prefixes to ignore when
        filtering classes. Defaults to ["owl", "rdfs"] if not provided.
        local_name (bool): If True, returns the local names of the classes
        (i.e., the part of the IRI after the last '#', '/', or ':').
        Defaults to False.
        include_explicit (bool): If True, includes explicitly defined triples in the query.
        Defaults to True.
        include_implicit (bool): If True, includes implicitly inferred triples in the query.
        Defaults to False.

    Returns:
        List[str]: A list of class IRIs or local names (depending on the value
        of `local_name`) associated with the given individual.

    Notes:
        - The method constructs a SPARQL query to retrieve the classes of the
            individual and applies optional filtering based on ignored prefixes.
        - If no results are found, an empty list is returned.
        - The `utils.get_local_name` function is used to extract the local name
            from the IRI if `local_name` is set to True.
    """
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

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )

    query.add_select_block(
        variables=["?class"],
        where_clauses=[
            f"?class rdf:type owl:Class .",
            f"{utils.prepare_subject(instance_iri)} rdf:type ?class .",
            filter_conditions,
        ],
    )

    query_string = query.to_string(validate=True)

    results = self.query(query=query_string)

    if results is None:
        return []

    classes = [result["class"]["value"] for result in results["results"]["bindings"]]
    if local_name is True:
        classes = [utils.get_local_name(iri) for iri in classes]
    return classes
