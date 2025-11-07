# To be imported into ..graph_db.py GraphDB class

from typing import List, Union, Any, Optional, Tuple, TYPE_CHECKING
from rdflib import Literal
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triples_get(
    self: "GraphDB",
    sub: Optional[str] = None,
    pred: Optional[str] = None,
    obj: Optional[Any] = None,
    include_explicit: bool = True,
    include_implicit: bool = True,
) -> Union[List[Tuple], List[str]]:
    """
    Retrieve triples based on the specified subject, predicate, and/or object.

    Args:
        sub (Optional[str]): The subject of the triple. Can be an IRI, shorthand IRI, or a string.
        pred (Optional[str]): The predicate of the triple. Can be an IRI, shorthand IRI, or a string.
        obj (Optional[Any]): The object of the triple. Can be an IRI, shorthand IRI, Literal, or a string.
        include_explicit (bool): Whether to include explicitly defined triples. Defaults to True.
        include_implicit (bool): Whether to include implicitly inferred triples. Defaults to True.

    Returns:
        Union[List[Tuple], List[str]]: A list of triples matching the query. Each triple is represented as a tuple
        (subject, predicate, object), where the object is converted to its Python type if applicable.

    Raises:
        InvalidInputError: If none of the subject, predicate, or object is provided.
    """

    if sub is None and pred is None and obj is None:
        raise InvalidInputError(
            "At least one of subject, predicate, or object must be provided"
        )

    binds = []
    filter = []

    def append_bind_and_filter(var: str, value: str):
        if utils.is_iri(value):
            binds.append(f"BIND({utils.ensure_absolute(value)} AS {var})")
        elif utils.is_shorthand_iri(value):
            binds.append(f"BIND({value} AS {var})")
        elif isinstance(value, Literal):
            filter.append(f"FILTER(?o={value.n3()})")
        else:
            filter.append(f"FILTER(CONTAINS(STR({var}), '{value}'))")

    if sub is not None:
        sub = utils.prepare_subject(sub, ensure_iri=False)
        append_bind_and_filter("?s", sub)

    if pred is not None:
        pred = utils.prepare_predicate(pred, ensure_iri=False)
        append_bind_and_filter("?p", pred)

    if obj is not None:
        obj = utils.prepare_object(obj, ensure_iri=False)
        append_bind_and_filter("?o", obj)

    query = SPARQLQuery(
        named_graph=self._named_graph,  # type: ignore
        prefixes=self._prefixes,
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
            result["s"]["value"],
            result["p"]["value"],
            utils.convert_query_result_to_python_type(result["o"]),
        )
        for result in results["results"]["bindings"]
    ]
    return converted_results


def triples_add(
    self,
    triples_to_add: List[Tuple[str, str, Any]],
    check_exist: bool = True,
    named_graph: Optional[str] = None,
) -> bool:
    """
    Adds multiple triples to the graph database.

    Args:
        triples_to_add (List[Tuple[str, str, Any]]): A list of triples to add, where each triple is represented as a tuple (subject, predicate, object).
        check_exist (bool, optional): Flag to check if any of the triples already exists. Then, no triple will be added. In Defaults to True.

    Returns:
        bool: True if all triples were successfully added, False otherwise.
    """
    if not triples_to_add:
        raise InvalidInputError("The list of triples to add must not be empty.")

    prepared_triples = []

    for sub, pred, obj in triples_to_add:
        sub = utils.prepare_subject(sub, ensure_iri=True)
        pred = utils.prepare_predicate(pred, ensure_iri=True)
        obj = utils.prepare_object(obj, as_string=True)

        if not sub or not pred or not obj:
            raise InvalidInputError(f"Invalid triple: ({sub}, {pred}, {obj})")
            return False
        prepared_triples.append((sub, pred, obj))

    if check_exist:
        ask_query = SPARQLQuery(
            named_graph=self._named_graph,
            prefixes=self._prefixes,
        )
        ask_query.add_ask_block(
            where_clauses=[
                f"{sub} {pred} {obj} ." for sub, pred, obj in prepared_triples
            ],
        )
        ask_query_string = ask_query.to_string()
        if ask_query_string is None:
            return False
        ask_result = self.query(query=ask_query_string, update=False)
        if ask_result is not None and ask_result["boolean"]:
            self.logger.warning("One of the triples to add already exists in the graph.")
            return False

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )
    query.add_insert_data_block(
        triples=prepared_triples,
    )

    # if check_exist:
    #     query.add_insert_exists_block(
    #         triples=prepared_triples,
    #     )
    # else:
    #     query.add_insert_data_block(
    #         triples=prepared_triples,
    #     )

    query_string = query.to_string()
    if query_string is None:
        return False

    if named_graph:
        old_named_graph = self._named_graph
        self.named_graph = named_graph

    result = self.query(query=query_string, update=True)
    if not result:
        self.logger.warning(f"Failed to add triples: {prepared_triples}")
        return False

    if named_graph:
        self.named_graph = named_graph

    return result


def triples_delete(
    self,
    triples_to_delete: List[Tuple[str, str, Union[str, Literal]]],
    check_exist: bool = True,
) -> bool:
    """
    Delete multiple triples from the graph database.

    Args:
        triples_to_delete (List[Tuple[str, str, Union[str, Literal]]]): A list of triples to delete, where each triple is represented as a tuple (subject, predicate, object).
        check_exist (bool, optional): Flag to check if each triple exists before attempting to delete it. Defaults to True.

    Returns:
        bool: Returns True if all triples were successfully deleted, False otherwise.
    """
    if not triples_to_delete:
        raise InvalidInputError("The list of triples to delete must not be empty.")

    prepared_triples = []
    where_clauses = []
    for sub, pred, obj in triples_to_delete:
        sub = utils.prepare_subject(sub, ensure_iri=True)
        pred = utils.prepare_predicate(pred, ensure_iri=True)
        obj = utils.prepare_object(obj, as_string=True)

        if check_exist:

            if not self.triple_exists(sub, pred, obj):
                self.logger.warning(
                    f"Triple does not exist and cannot be deleted: {sub} {pred} {obj}"
                )
                return False
            where_clauses.append(f"{sub} {pred} {obj} .")
        prepared_triples.append((sub, pred, obj))

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )

    query.add_delete_data_block(
        triples=prepared_triples,
    )

    result = self.query(query=query.to_string(), update=True)
    if result:
        self.logger.debug(f"Successfully deleted triples: {prepared_triples}")
    else:
        self.logger.warning(f"Failed to delete triples: {prepared_triples}")

    return result


def triples_update(
    self,
    old_triples: List[Tuple[str, str, Union[str, Literal]]],
    new_triples: List[Tuple[str, str, Union[str, Literal]]],
    check_exist: bool = True,
) -> bool:
    """
    Update multiple RDF triples in the triplestore.

    Args:
        old_triples (List[Tuple[str, str, Union[str, Literal]]]): A list of triples to update, where each triple is represented as a tuple (subject, predicate, object).
        new_triples (List[Tuple[Optional[str], Optional[str], Optional[Union[str, Literal]]]]): A list of new triples to replace the old triples.
        check_exist (bool): Whether to check for the existence of old triples before updating.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    if not old_triples or not new_triples:
        raise InvalidInputError("Old and new triples lists must not be empty.")

    if len(old_triples) != len(new_triples):
        raise InvalidInputError("Old and new triples lists must have the same length.")

    delete_triples = []
    insert_triples = []
    where_clauses = []

    for triple in old_triples:
        if len(triple) != 3:
            raise InvalidInputError(
                "Each old triple must have exactly three elements (subject, predicate, object)."
            )
        sub_old, pred_old, obj_old = triple
        if check_exist:
            if not self.triple_exists(sub_old, pred_old, obj_old):
                self.logger.warning(f"Triple does not exist: {sub_old} {pred_old} {obj_old}")
                return False

        sub_old = utils.prepare_subject(sub_old, ensure_iri=True)
        pred_old = utils.prepare_predicate(pred_old, ensure_iri=True)
        obj_old = utils.prepare_object(obj_old, as_string=True)
        delete_triples.append((sub_old, pred_old, obj_old))
        where_clauses.append(f"{sub_old} {pred_old} {obj_old} .")

    for triple in new_triples:
        if len(triple) != 3:
            raise InvalidInputError(
                "Each new triple must have exactly three elements (subject, predicate, object)."
            )
        sub_new, pred_new, obj_new = triple

        if sub_new is not None:
            sub_new = utils.prepare_subject(sub_new, ensure_iri=True)
        if pred_new is not None:
            pred_new = utils.prepare_predicate(pred_new, ensure_iri=True)
        if obj_new is not None:
            obj_new = utils.prepare_object(obj_new, as_string=True)
        insert_triples.append((sub_new, pred_new, obj_new))

    query = SPARQLQuery(
        named_graph=self._named_graph,
        prefixes=self._prefixes,
    )
    query.add_delete_insert_data_block(
        delete_triples=delete_triples,
        insert_triples=insert_triples,
        where_clauses=where_clauses,
    )
    query_string = query.to_string(validate=True)
    if query_string is None:
        return False
    result = self.query(query=query_string, update=True)
    if result:
        self.logger.debug(
            f"Successfully updated triples {old_triples} -> {new_triples}, named_graph: {self._named_graph}, repository:"
            f" {self._repository}"
        )
    else:
        self.logger.warning(
            f"Failed to update triples {old_triples} -> {new_triples}, named_graph: {self._named_graph}, repository:"
            f" {self._repository}"
        )
    return result
