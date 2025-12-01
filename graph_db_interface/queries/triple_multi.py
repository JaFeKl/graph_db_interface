# To be imported into ..graph_db.py GraphDB class

from typing import List, Union, Any, Optional, Tuple, TYPE_CHECKING
from rdflib import Literal
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI
from graph_db_interface.utils.utils import Triple, PartialTriple
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triples_get(
    self: "GraphDB",
    triple: Optional[PartialTriple] = None,
    sub: Optional[Union[str, IRI]] = None,
    pred: Optional[Union[str, IRI]] = None,
    obj: Optional[Union[str, IRI, Literal]] = None,
    include_explicit: Optional[bool] = True,
    include_implicit: Optional[bool] = True,
) -> List[Tuple[IRI, IRI, Any]]:
    """
    Retrieve triples matching any combination of subject, predicate, or object.

    Args:
        triple (Optional[PartialTriple]): Combined (subject, predicate, object) filter tuple. Use this
            or individual `sub`/`pred`/`obj`.
        sub (Optional[Union[str, IRI]]): Subject filter (IRI/shorthand/string).
        pred (Optional[Union[str, IRI]]): Predicate filter (IRI/shorthand/string).
        obj (Optional[Union[str, IRI, Literal]]): Object filter (IRI/shorthand/Literal/string).
        include_explicit (Optional[bool]): Include explicit triples. Defaults to True.
        include_implicit (Optional[bool]): Include inferred triples. Defaults to True.

    Returns:
        List[Tuple[IRI, IRI, Any]]: Matching triples as `(subject, predicate, object)`, where the
        object is converted to an appropriate Python type when applicable.

    Raises:
        InvalidInputError: If neither or both of `triple` and any of `sub`/`pred`/`obj` are provided.
    """

    elems_given = sub is not None or pred is not None or obj is not None
    if (triple and elems_given) or (not triple and not elems_given):
        raise InvalidInputError(
            "Either 'triple' or 'sub/pred/obj' must be provided, not both."
        )

    sub, pred, obj = utils.sanitize_triple(
        triple or (sub, pred, obj), allow_partial=True
    )

    binds = []
    filter = []

    def _append_bind_and_filter(
        var: str,
        value: Union[IRI, Literal],
    ) -> None:
        if isinstance(value, IRI):
            binds.append(f"BIND({value.n3()} AS {var})")
        elif isinstance(value, Literal):
            filter.append(f"FILTER(?o={value.n3()})")
        else:
            raise Exception(
                f"Value must be either IRI or Literal, is type {type(value)}"
            )

    if sub is not None:
        _append_bind_and_filter("?s", sub)

    if pred is not None:
        _append_bind_and_filter("?p", pred)

    if obj is not None:
        _append_bind_and_filter("?o", obj)

    query = SPARQLQuery(
        named_graph=self.named_graph,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )
    query.add_select_block(
        variables=["?s", "?p", "?o"],
        where_clauses=binds + ["?s ?p ?o ."] + filter,
    )
    query_string = query.to_string(validate=True)
    if query_string is None:
        self.logger.error(
            "Unable to construct SPARQL query, returning empty list of triples"
        )
        return []

    results = self.query(query=query_string)
    converted_results = [
        (
            IRI(result["s"]["value"]),
            IRI(result["p"]["value"]),
            utils.convert_query_result_to_python_type(result["o"]),
        )
        for result in results["results"]["bindings"]
    ]
    return converted_results


def any_triple_exists(
    self: "GraphDB",
    triples: List[Triple],
    named_graph: Optional[IRI] = None,
) -> bool:
    """
    Check if any of the given triples exist.

    Args:
        triples (List[Triple]): Triples to check.
        named_graph (Optional[IRI]): Override the client's default named graph.

    Returns:
        bool: True if at least one exists, False otherwise.
    """

    if not triples:
        raise InvalidInputError(f"Cannot check existence of empty triple list.")

    for triple in triples:
        triple = utils.sanitize_triple(triple)

        if self.triple_exists(triple):
            self.logger.debug(
                f"At least this triple exists: ({utils.triple_to_string(triple)}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
            )
            return True

    self.logger.debug(
        f"None of the triples exists: ({triples}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
    )
    return False


def all_triple_exists(
    self: "GraphDB",
    triples: List[Triple],
    named_graph: Optional[IRI] = None,
) -> bool:
    """
    Check if all of the given triples exist.

    Args:
        triples (List[Triple]): Triples to check.
        named_graph (Optional[IRI]): Override the client's default named graph.

    Returns:
        bool: True if all exist, False otherwise.
    """

    if not triples:
        raise InvalidInputError(f"Cannot check existence of empty triple list.")

    triple_strings = []
    for triple in triples:
        triple = utils.sanitize_triple(triple)
        triple_strings.append(utils.triple_to_string(triple, "."))

    query = SPARQLQuery(named_graph=named_graph or self.named_graph)
    query.add_ask_block(where_clauses=triple_strings)
    query_string = query.to_string()
    if not query_string:
        raise InvalidInputError(
            f"Could not generate 'all_triple_exists' query for triples ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )

    ask_result = self.query(query=query_string, update=False)
    if ask_result is None:
        raise InvalidInputError(
            f"Could not query 'all_triple_exists' for triples ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )

    if ask_result["boolean"] is False:
        self.logger.debug(
            f"Not all of the triples exist: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"All of the triples exist: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triples_add(
    self: "GraphDB",
    triples_to_add: List[Triple],
    check_exist: Optional[bool] = True,
    named_graph: Optional[IRI] = None,
) -> bool:
    """
    Add multiple triples to the graph database.

    Args:
        triples_to_add (List[Triple]): Triples to add.
        check_exist (Optional[bool]): If True, abort when any triple already exists. Defaults to True.
        named_graph (Optional[IRI]): Override the client's default named graph.

    Returns:
        bool: True if all triples were added, False otherwise.
    """
    if not triples_to_add:
        return True

    validated_triples_to_add = [
        utils.sanitize_triple(triple) for triple in triples_to_add
    ]

    if check_exist and self.any_triple_exists(
        triples=validated_triples_to_add, named_graph=named_graph
    ):
        self.logger.warning(
            "At least one of the triples to add already exists in the graph."
        )
        return False

    triple_strings = [
        utils.triple_to_string(triple, ".") for triple in validated_triples_to_add
    ]

    query = SPARQLQuery(named_graph=named_graph or self.named_graph)
    query.add_insert_data_block(
        triples=validated_triples_to_add,
    )
    query_string = query.to_string()
    if not query_string:
        raise InvalidInputError(
            f"Could not generate 'triples_add' query for triples ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to add triples: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully added triples: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
    )
    return result


def triples_delete(
    self: "GraphDB",
    triples_to_delete: List[Triple],
    check_exist: Optional[bool] = True,
    named_graph: Optional[IRI] = None,
) -> bool:
    """
    Delete multiple triples from the graph database.

    Args:
        triples_to_delete (List[Triple]): Triples to delete.
        check_exist (Optional[bool]): If True, abort when any triple does not exist. Defaults to True.
        named_graph (Optional[IRI]): Override the client's default named graph.

    Returns:
        bool: True if all triples were deleted, False otherwise.
    """
    if not triples_to_delete:
        return True

    validated_triples_to_delete = [
        utils.sanitize_triple(triple) for triple in triples_to_delete
    ]

    if check_exist and not self.all_triple_exists(
        triples=validated_triples_to_delete, named_graph=named_graph
    ):
        self.logger.warning(
            "At least one of the triples to delete does not exist in the graph."
        )
        return False

    triple_strings = [
        utils.triple_to_string(triple, ".") for triple in validated_triples_to_delete
    ]

    query = SPARQLQuery(named_graph=named_graph or self.named_graph)
    query.add_delete_data_block(
        triples=validated_triples_to_delete,
    )
    query_string = query.to_string()
    if not query_string:
        raise InvalidInputError(
            f"Could not generate 'triples_delete' query for triples ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to delete triples: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully deleted triples: ({triple_strings}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triples_update(
    self: "GraphDB",
    old_triples: List[Triple],
    new_triples: List[Triple],
    check_exist: Optional[bool] = True,
    named_graph: Optional[IRI] = None,
) -> bool:
    """
    Update multiple RDF triples in the triplestore.

    Args:
        old_triples (List[Triple]): Triples to be replaced.
        new_triples (List[Triple]): Replacement triples (same length as `old_triples`).
        check_exist (Optional[bool]): If True, abort when any old triple does not exist. Defaults to True.
        named_graph (Optional[IRI]): Override the client's default named graph.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    if not old_triples and not new_triples:
        return True

    if len(old_triples) != len(new_triples):
        raise InvalidInputError("Old and new triples lists must have the same length.")

    validated_old_triples = [utils.sanitize_triple(triple) for triple in old_triples]
    validated_new_triples = [utils.sanitize_triple(triple) for triple in new_triples]

    if check_exist and not self.all_triple_exists(
        triples=validated_old_triples, named_graph=named_graph
    ):
        self.logger.warning(
            "At least one of the triples to update does not exist in the graph."
        )
        return False

    old_triple_strings = [
        utils.triple_to_string(triple, ".") for triple in validated_old_triples
    ]

    query = SPARQLQuery(named_graph=named_graph or self.named_graph)
    query.add_delete_insert_data_block(
        delete_triples=validated_old_triples,
        insert_triples=validated_new_triples,
        where_clauses=old_triple_strings,
    )
    query_string = query.to_string(validate=True)
    if not query_string:
        raise InvalidInputError(
            f"Could not generate 'triples_update' query for triples ({validated_old_triples}) -> ({validated_new_triples}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to update triples ({validated_old_triples}) -> ({validated_new_triples}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully updated triples ({validated_old_triples}) -> ({validated_new_triples}), named_graph: {named_graph or self.named_graph or "default"}, repository: {self._repository}"
    )
    return True
