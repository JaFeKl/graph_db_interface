# To be imported into ..graph_db.py GraphDB class

from typing import Dict, List, Union, Any, Optional, Tuple, TYPE_CHECKING
from rdflib import BNode, Literal
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI
from graph_db_interface.utils.types import (
    Subject,
    Predicate,
    PartialTripleLike,
    SubjectLike,
    PredicateLike,
    ObjectLike,
    GraphNameLike,
    TriplesLike,
    Triple,
)
from graph_db_interface.exceptions import InvalidInputError

from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def triples_get(
    self: "GraphDB",
    triple: Optional[PartialTripleLike] = None,
    sub: Optional[SubjectLike] = None,
    pred: Optional[PredicateLike] = None,
    obj: Optional[ObjectLike] = None,
    include_explicit: Optional[bool] = True,
    include_implicit: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> List[Tuple[Subject, Predicate, Any]]:
    """
    Retrieve triples matching any combination of subject, predicate, or object.

    Args:
        triple (Optional[PartialTripleLike]): Combined (subject, predicate, object) filter tuple. Use this
            or individual `sub`/`pred`/`obj`.
        sub (Optional[SubjectLike]): Subject filter (IRI/shorthand/string).
        pred (Optional[PredicateLike]): Predicate filter (IRI/shorthand/string).
        obj (Optional[ObjectLike]): Object filter (IRI/shorthand/Literal/string).
        include_explicit (Optional[bool]): Include explicit triples. Defaults to True.
        include_implicit (Optional[bool]): Include inferred triples. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        List[Tuple[Subject, Predicate, Any]]: Matching triples as `(subject, predicate, object)`, where the
        object is converted to an appropriate Python type when applicable.

    Raises:
        InvalidInputError: If neither or both of `triple` and any of `sub`/`pred`/`obj` are provided.
    """

    elems_given = sub is not None or pred is not None or obj is not None
    if (triple and elems_given) or (not triple and not elems_given):
        raise InvalidInputError(
            "Either 'triple' or 'sub/pred/obj' must be provided, not both."
        )

    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph
    if named_graph is not None and not hasattr(named_graph, "n3"):
        named_graph = IRI(named_graph)

    sub, pred, obj = utils.sanitize_triple(
        triple or (sub, pred, obj), allow_partial=True
    )

    binds = []
    filter = []

    def _append_bind_and_filter(
        var: str,
        value: Union[IRI, BNode, Literal],
    ) -> None:
        if isinstance(value, (IRI, BNode)):
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

    query = SPARQLQuery.select(
        variables=["?s", "?p", "?o"],
        where_clauses=binds + ["?s ?p ?o ."] + filter,
        named_graph=named_graph,
        include_explicit=include_explicit,
        include_implicit=include_implicit,
    )
    results = self.query(query=query, convert_bindings=True)

    converted_results = [
        (result["s"], result["p"], result["o"])
        for result in results["results"]["bindings"]
    ]

    return converted_results


def any_triple_exists(
    self: "GraphDB",
    triples: TriplesLike,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Check if any of the given triples exist.

    Args:
        triples (TriplesLike): Triples to check.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if at least one exists, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if not triples:
        raise InvalidInputError(f"Cannot check existence of empty triple list.")

    validated_triples = [utils.sanitize_triple(triple) for triple in triples]

    triple_groups = utils.group_triples_by_bnode(validated_triples)

    # Build UNION patterns for ASK query
    union_patterns = []
    for group in triple_groups:
        group_pattern = " .\n    ".join(
            utils.triple_to_string(triple, "") for triple in group
        )
        union_patterns.append(f"{{\n    {group_pattern} .\n  }}")

    where_clause = "\n  UNION\n  ".join(union_patterns)

    query = SPARQLQuery.ask(
        where_clauses=[where_clause],
        named_graph=named_graph,
    )
    ask_result = self.query(query=query, update=False)
    if ask_result is None:
        raise InvalidInputError(
            f"Could not query 'any_triple_exists' for triples, named_graph: {named_graph or 'default'}, repository: {self._repository}"
        )

    if ask_result["boolean"] is True:
        self.logger.debug(
            f"At least one of the triples exists, named_graph: {named_graph or 'default'}, repository: {self._repository}"
        )
        return True

    self.logger.debug(
        f"None of the triples exists, named_graph: {named_graph or 'default'}, repository: {self._repository}"
    )
    return False


def all_triple_exists(
    self: "GraphDB",
    triples: TriplesLike,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Check if all of the given triples exist.

    Args:
        triples (TriplesLike): Triples to check.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if all exist, False otherwise.
    """
    self.logger.setLevel(10)
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

    if not triples:
        raise InvalidInputError(f"Cannot check existence of empty triple list.")

    triple_strings = []
    for triple in triples:
        triple = utils.sanitize_triple(triple)
        triple_strings.append(utils.triple_to_string(triple, "."))

    query = SPARQLQuery.ask(
        where_clauses=triple_strings,
        named_graph=named_graph,
    )
    ask_result = self.query(query=query, update=False)
    if ask_result is None:
        raise InvalidInputError(
            f"Could not query 'all_triple_exists' for triples ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )

    if ask_result["boolean"] is False:
        self.logger.debug(
            f"Not all of the triples exist: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"All of the triples exist: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triples_add(
    self: "GraphDB",
    triples_to_add: TriplesLike,
    check_exist: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Add multiple triples to the graph database.

    Args:
        triples_to_add (TriplesLike): Triples to add.
        check_exist (Optional[bool]): If True, abort when any triple already exists. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if all triples were added, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

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

    query = SPARQLQuery.insert_data(
        triples=validated_triples_to_add,
        named_graph=named_graph,
    )
    result = self.query(query=query, update=True)
    if not result:
        self.logger.warning(
            f"Failed to add triples: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully added triples: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return result


def triples_delete(
    self: "GraphDB",
    triples_to_delete: TriplesLike,
    check_exist: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Delete multiple triples from the graph database.

    Args:
        triples_to_delete (TriplesLike): Triples to delete.
        check_exist (Optional[bool]): If True, abort when any triple does not exist. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if all triples were deleted, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

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

    query = SPARQLQuery.delete_data(
        triples=validated_triples_to_delete,
        named_graph=named_graph,
    )
    result = self.query(query=query, update=True)
    if not result:
        self.logger.warning(
            f"Failed to delete triples: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully deleted triples: ({triple_strings}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True


def triples_update(
    self: "GraphDB",
    old_triples: TriplesLike,
    new_triples: TriplesLike,
    check_exist: Optional[bool] = True,
    named_graph: Optional[GraphNameLike] = None,
) -> bool:
    """
    Update multiple RDF triples in the triplestore.

    Args:
        old_triples (TriplesLike): Triples to be replaced.
        new_triples (TriplesLike): Replacement triples (same length as `old_triples`).
        check_exist (Optional[bool]): If True, abort when any old triple does not exist. Defaults to True.
        named_graph (Optional[GraphNameLike]): Override the client's default named graph.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    named_graph = IRI(named_graph) if named_graph is not None else self.named_graph

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

    def _render_term(
        term: Any,
        bn_map: Optional[Dict[BNode, str]] = None,
        prefix: str = "",
    ) -> str:
        if isinstance(term, BNode):
            if bn_map is None:
                raise InvalidInputError(
                    "Blank nodes in predicates are not supported for updates."
                )
            if term not in bn_map:
                bn_map[term] = f"?{prefix}{len(bn_map) + 1}"
            return bn_map[term]
        if hasattr(term, "n3"):
            return term.n3()
        return IRI(term).n3()

    def _build_patterns(
        triples: List[Triple],
        bn_map: Dict[BNode, str],
        prefix: str,
    ) -> List[str]:
        patterns: List[str] = []
        for subject, predicate, obj in triples:
            subj_str = _render_term(subject, bn_map, prefix)
            pred_str = _render_term(predicate)
            obj_str = _render_term(obj, bn_map, prefix)
            patterns.append(f"{subj_str} {pred_str} {obj_str} .")
        return patterns

    old_bn_var_map: Dict[BNode, str] = {}
    new_bn_var_map: Dict[BNode, str] = {}

    old_delete_patterns = _build_patterns(
        validated_old_triples, old_bn_var_map, "oldbn"
    )
    insert_patterns = _build_patterns(validated_new_triples, new_bn_var_map, "newbn")

    def _format_block(patterns: List[str]) -> str:
        if not patterns:
            return ""
        return "\n".join(f"  {pattern}" for pattern in patterns)

    delete_block = _format_block(old_delete_patterns)
    insert_block = _format_block(insert_patterns)

    where_patterns = list(dict.fromkeys(old_delete_patterns))
    where_block_parts: List[str] = []
    if where_patterns:
        where_block_parts.append(_format_block(where_patterns))
    if new_bn_var_map:
        where_block_parts.extend(
            f"  BIND(BNODE() AS {var})" for var in new_bn_var_map.values()
        )
    where_block = "\n".join(where_block_parts)

    graph_clause = f"WITH {named_graph.n3()}\n" if named_graph else ""

    query = f"""{graph_clause}DELETE {{
{delete_block}
}}
INSERT {{
{insert_block}
}}
WHERE {{
{where_block}
}}
""".strip()
    result = self.query(query=query, update=True)
    if not result:
        self.logger.warning(
            f"Failed to update triples ({validated_old_triples}) -> ({validated_new_triples}), named_graph: {named_graph or "default"}, repository: {self._repository}"
        )
        return False

    self.logger.debug(
        f"Successfully updated triples ({validated_old_triples}) -> ({validated_new_triples}), named_graph: {named_graph or "default"}, repository: {self._repository}"
    )
    return True
