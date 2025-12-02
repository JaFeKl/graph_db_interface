# To be imported into ..graph_db.py GraphDB class

from typing import Optional, Union, TYPE_CHECKING
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI
from graph_db_interface.utils.utils import (
    Triple,
    PartialTriple,
)
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triple_exists(
    self: "GraphDB",
    triple: Triple,
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Check whether a specific triple exists in the graph database.

    Args:
        triple (Triple): The triple `(subject, predicate, object)` to check.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if the triple exists, False otherwise.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    query = SPARQLQuery(named_graph=named_graph)
    query.add_ask_block(
        where_clauses=[
            utils.triple_to_string(triple, "."),
        ],
    )
    query_string = query.to_string()

    result = self.query(query=query_string)
    if result is None or result["boolean"] is False:
        self.logger.debug(
            f"Unable to find triple ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Found triple ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triple_add(
    self: "GraphDB",
    triple: Triple,
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Add a triple to the graph database.

    Args:
        triple (Triple): The triple `(subject, predicate, object)` to insert.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if the triple was inserted, False otherwise.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    query = SPARQLQuery(named_graph=named_graph)
    query.add_insert_data_block(
        triples=[triple],
    )
    query_string = query.to_string()
    if query_string is None:
        return False

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to insert triple: ({utils.triple_to_string(triple)}) named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully inserted triple: ({utils.triple_to_string(triple)}) named_graph: {named_graph or "default"}, repository: {self._repository}"
    )

    return True


def triple_delete(
    self: "GraphDB",
    triple: Triple,
    check_exist: Optional[bool] = True,
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Delete a single triple.

    A SPARQL DELETE operation in GraphDB can be successful even if the triple
    does not exist. When `check_exist=True`, the function verifies the triple is
    present before attempting deletion.

    Args:
        triple (Triple): The triple `(subject, predicate, object)` to delete.
        check_exist (Optional[bool]): Whether to verify existence prior to deletion. Defaults to True.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if deletion succeeded (or triple absent when `check_exist=False`), False otherwise.
    """
    triple = utils.sanitize_triple(triple)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if check_exist:
        if not self.triple_exists(triple, named_graph=named_graph):
            self.logger.warning("Unable to delete triple since it does not exist")
            return False
    query = SPARQLQuery(named_graph=named_graph)
    query.add_delete_data_block(
        triples=[triple],
    )
    query_string = query.to_string()

    if query_string is None:
        return False

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to delete triple: ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully deleted triple: ({utils.triple_to_string(triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triple_update(
    self: "GraphDB",
    old_triple: Triple,
    new_triple: Optional[PartialTriple] = None,
    new_sub: Optional[Union[str, IRI]] = None,
    new_pred: Optional[Union[str, IRI]] = None,
    new_obj: Optional[Union[str, IRI]] = None,
    check_exist: Optional[bool] = True,
    named_graph: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Update a triple by replacing any of its parts.

    Performs a SPARQL `DELETE ... INSERT ... WHERE` that replaces the old triple
    with a new triple built from provided parts.

    Args:
        old_triple (Triple): Existing triple to update.
        new_triple (Optional[PartialTriple]): Replacement values (subject/predicate/object). Use this
            or `new_sub`/`new_pred`/`new_obj`.
        new_sub (Optional[Union[str, IRI]]): Replacement subject.
        new_pred (Optional[Union[str, IRI]]): Replacement predicate.
        new_obj (Optional[Union[str, IRI]]): Replacement object.
        check_exist (Optional[bool]): If True, verify that `old_triple` exists before updating. Defaults to True.
        named_graph (Optional[Union[str, IRI]]): Override the client's default named graph.

    Returns:
        bool: True if the update succeeded, False otherwise.

    Raises:
        InvalidInputError: If neither or both of `new_triple` and any of `new_sub`/`new_pred`/`new_obj` are provided,
            or if input triples are incomplete/invalid.
    """
    elems_given = new_sub is not None or new_pred is not None or new_obj is not None
    if (new_triple and elems_given) or (not new_triple and not elems_given):
        raise InvalidInputError(
            "Either 'new triple' or 'new_sub/new_pred/new_obj' must be provided, not both."
        )

    old_triple = utils.sanitize_triple(old_triple)
    new_triple = utils.sanitize_triple(
        new_triple or (new_sub, new_pred, new_obj),
        allow_partial=True,
    )

    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if check_exist:
        if not self.triple_exists(old_triple, named_graph=named_graph):
            self.logger.warning(
                f"Triple does not exist: ({utils.triple_to_string(old_triple)})"
            )
            return False

    # Determine replacement variables
    update_triple = tuple(n if n else o for o, n in zip(old_triple, new_triple))

    query = SPARQLQuery(named_graph=named_graph)
    query.add_delete_insert_data_block(
        delete_triples=[old_triple],
        insert_triples=[update_triple],
        where_clauses=[utils.triple_to_string(old_triple, ".")],
    )
    query_string = query.to_string(validate=True)
    if query_string is None:
        return False

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(
            f"Failed to update triple to: ({utils.triple_to_string(update_triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully updated triple to: ({utils.triple_to_string(update_triple)}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True
