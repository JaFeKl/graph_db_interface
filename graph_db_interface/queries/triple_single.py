# To be imported into ..graph_db.py GraphDB class

from typing import Union, Any, Optional, TYPE_CHECKING
from rdflib import Literal
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triple_exists(
    self: "GraphDB",
    sub: str,
    pred: str,
    obj: Union[str, Literal],
) -> bool:
    """
    Checks if a specific triple exists in the graph database.

    Args:
        sub (str): The subject of the triple. It will be processed to ensure it is an IRI.
        pred (str): The predicate of the triple. It will be processed to ensure it is an IRI.
        obj (Union[str, Literal]): The object of the triple. It can be a string or a Literal and
            will be processed to ensure it is represented as a string.

    Returns:
        bool: True if the triple exists in the graph database, False otherwise.
    """
    sub = utils.prepare_subject(sub, ensure_iri=True)
    pred = utils.prepare_predicate(pred, ensure_iri=True)
    obj = utils.prepare_object(obj, as_string=True)

    query = SPARQLQuery(named_graph=self._named_graph, prefixes=self._prefixes)
    query.add_ask_block(
        where_clauses=[
            f"{sub} {pred} {obj} .",
        ],
    )
    query_string = query.to_string()

    result = self.query(query=query_string)
    if result is not None and result["boolean"]:
        self.logger.debug(f"Found triple {sub}, {pred}, {obj}")
        return True

    self.logger.debug(
        f"Unable to find triple {sub}, {pred}, {obj}, named_graph:"
        f" {self._named_graph}, repository: {self._repository}"
    )
    return False


def triple_add(
    self: "GraphDB",
    sub: str,
    pred: str,
    obj: Any,
    named_graph: Optional[str] = None,
) -> bool:
    """
    Adds a triple (subject, predicate, object) to the graph database.

    This method prepares the subject, predicate, and object to ensure they are
    in the correct format (e.g., IRI or string) and constructs a SPARQL query
    to insert the triple into the specified named graph.

    Args:
        sub (str): The subject of the triple. It will be processed to ensure it
            is a valid IRI.
        pred (str): The predicate of the triple. It will be processed to ensure
            it is a valid IRI.
        obj (Any): The object of the triple. It will be processed to ensure it
            is represented as a string.

    Returns:
        bool: True if the triple was successfully inserted into the graph
        database, False otherwise.
    """
    sub = utils.prepare_subject(sub, ensure_iri=True)
    pred = utils.prepare_predicate(pred, ensure_iri=True)
    obj = utils.prepare_object(obj, as_string=True)

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )
    query.add_insert_data_block(
        triples=[(sub, pred, obj)],
    )
    query_string = query.to_string()
    if query_string is None:
        return False

    if named_graph:
        old_named_graph = self._named_graph
        self.named_graph = named_graph
    result = self.query(query=query_string, update=True)
    if result:
        self.logger.debug(
            f"New triple inserted: {sub}, {pred}, {obj} named_graph:"
            f" {self._named_graph}, repository: {self._repository}"
        )

    if named_graph:
        self.named_graph = old_named_graph
    return result


def triple_delete(
    self: "GraphDB",
    sub: str,
    pred: str,
    obj: Union[str, Literal],
    check_exist: bool = True,
) -> bool:
    """Delete a single triple. A SPAQRL delete query will be successfull, even though the triple to delete does not exist in the first place.

    Args:
        subject (str): valid subject IRI
        predicate (str): valid predicate IRI
        object (str): valid object IRI
        named_graph (str, optional): The IRI of a named graph. Defaults to None.
        check_exist (bool, optional): Flag if you want to check if the triple exists before aiming to delete it. Defaults to True.

    Returns:
        bool: Returns True if query was successfull. False otherwise.
    """
    sub = utils.prepare_subject(sub, ensure_iri=True)
    pred = utils.prepare_predicate(pred, ensure_iri=True)
    obj = utils.prepare_object(obj, as_string=True)

    if check_exist:
        if not self.triple_exists(sub, pred, obj):
            self.logger.warning("Unable to delete triple since it does not exist")
            return False
    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )
    query.add_delete_data_block(
        triples=[(sub, pred, obj)],
    )
    query_string = query.to_string()

    if query_string is None:
        return False

    # Execute the SPARQL query
    result = self.query(query=query_string, update=True)
    if result:
        self.logger.debug(f"Successfully deleted triple: {sub} {pred} {obj}")
    else:
        self.logger.warning(f"Failed to delete triple: {sub} {pred} {obj}")

    return result


def triple_update(
    self: "GraphDB",
    sub_old: str,
    pred_old: str,
    obj_old: Union[str, Literal],
    sub_new: Optional[str] = None,
    pred_new: Optional[str] = None,
    obj_new: Optional[Union[str, Literal]] = None,
    check_exist: bool = True,
) -> bool:
    """
    Updates any part of an existing triple (subject, predicate, or object) in the RDF store.

    This function replaces the specified part of an existing triple using a SPARQL
    `DELETE ... INSERT ... WHERE` query.

    Args:
        old_subject (str, optional): The subject of the triple to be updated.
        old_predicate (str, optional): The predicate of the triple to be updated.
        old_object (str, optional): The object of the triple to be updated.
        new_subject (str, optional): The new subject to replace the old subject.
        new_predicate (str, optional): The new predicate to replace the old predicate.
        new_object (str, optional): The new object to replace the old object.
        named_graph (str, optional): The named graph where the triple update should be performed.
        check_exist (bool, optional): If `True`, checks if the old triple exists before updating.
                                    Defaults to `True`.

    Returns:
        bool: `True` if the update was successful, `False` otherwise.

    Raises:
        Any exceptions thrown by `self.query()` if the SPARQL update request fails.

    Example:
        ```python
        success = rdf_store.triple_update_any(
            old_subject="<http://example.org/oldSubject>",
            old_predicate="<http://example.org/oldPredicate>",
            old_object="<http://example.org/oldObject>",
            new_subject="<http://example.org/newSubject>"
        )
        ```
    """
    if not (sub_old and pred_old and obj_old):
        raise InvalidInputError(
            "All parts of the triple to update (sub_old, pred_old, obj_old) must be provided."
        )

    if sub_new is None and pred_new is None and obj_new is None:
        raise InvalidInputError(
            "At least one of sub_new, pred_new, or obj_new must be provided."
        )

    sub_old = utils.prepare_subject(sub_old, ensure_iri=True)
    pred_old = utils.prepare_predicate(pred_old, ensure_iri=True)
    obj_old = utils.prepare_object(obj_old, as_string=True)

    if check_exist:
        if not self.triple_exists(
            sub_old,
            pred_old,
            obj_old,
        ):
            self.logger.warning(f"Triple does not exist: {sub_old} {pred_old} {obj_old}")
            return False

    if sub_new is not None:
        sub_new = utils.prepare_subject(sub_new, ensure_iri=True)
    if pred_new is not None:
        pred_new = utils.prepare_predicate(pred_new, ensure_iri=True)
    if obj_new is not None:
        obj_new = utils.prepare_object(obj_new, as_string=True)

    # Determine replacement variables
    update_sub = sub_new if sub_new else sub_old
    update_pred = pred_new if pred_new else pred_old
    update_obj = obj_new if obj_new else obj_old

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )
    query.add_delete_insert_data_block(
        delete_triples=[(sub_old, pred_old, obj_old)],
        insert_triples=[(update_sub, update_pred, update_obj)],
        where_clauses=[f"{sub_old} {pred_old} {obj_old} ."],
    )
    query_string = query.to_string(validate=True)
    if query_string is None:
        return False

    result = self.query(query=query_string, update=True)

    if result:
        self.logger.debug(
            f"Successfully updated triple to: {update_sub} {update_pred}"
            f" {update_obj}, named_graph: {self._named_graph}, repository:"
            f" {self._repository}"
        )
    else:
        self.logger.warning(
            f"Failed to update triple to: {update_sub} {update_pred}"
            f" {update_obj}, named_graph: {self._named_graph}, repository:"
            f" {self._repository}"
        )

    return result
