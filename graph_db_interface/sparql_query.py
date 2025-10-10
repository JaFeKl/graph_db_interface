from enum import Enum
from typing import List, Optional, Dict, Tuple
from graph_db_interface.utils import utils

class SPARQLQueryType(Enum):
    """Enum for different SPARQL query types."""

    SELECT = "SELECT"
    SELECT_DISTINCT = "SELECT DISTINCT"
    SELECT_REDUCED = "SELECT REDUCED"
    CONSTRUCT = "CONSTRUCT"
    DESCRIBE = "DESCRIBE"
    ASK = "ASK"
    INSERT_DATA = "INSERT DATA"
    DELETE_DATA = "DELETE DATA"
    DELETE_INSERT = "DELETE/INSERT"

class SPARQLQuery:
    def __init__(
        self,
        named_graph: Optional[str] = None,
        prefixes: Optional[Dict[str, str]] = None,
        include_explicit: bool = True,
        include_implicit: bool = True,
    ):
        self._named_graph = named_graph
        self._prefixes = prefixes
        self._include_explicit = include_explicit
        self._include_implicit = include_implicit
        self._query_blocks = []

    def add_select_block(
        self,
        variables: List[str],
        where_clauses: List[str],
        select_type: SPARQLQueryType = SPARQLQueryType.SELECT,
    ) -> str:
        block_parts = []
        block_parts.append(
            f"{select_type.value} {self._create_variable_string(variables)}"
        )
        part = self._add_explicit_implicit()
        if self._named_graph:
            block_parts.append(f"FROM {utils.ensure_absolute(self._named_graph)}")
        if part:
            block_parts.append(part)
        block_parts.append(f"WHERE {{{self._combine_where_clauses(where_clauses)}}}")
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": select_type, "data": block})

    def add_ask_block(
        self,
        where_clauses: List[str],
    ) -> str:
        block_parts = []
        block_parts.append("ASK")
        part = self._add_explicit_implicit()
        if part:
            block_parts.append(part)
        block_parts.append(
            f"""
WHERE {{
    {utils.encapsulate_named_graph(self._named_graph, self._combine_where_clauses(where_clauses))
}}}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.ASK, "data": block})

    def add_insert_data_block(
        self,
        triples: List[Tuple[str]],
    ) -> str:
        block_parts = []
        data_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in triples
        )
        block_parts.append(
            f"""INSERT DATA {{
        {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.INSERT_DATA, "data": block})

    def add_delete_data_block(
        self,
        triples: List[Tuple[str]],
    ) -> str:
        block_parts = []
        data_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in triples
        )
        block_parts.append(
            f"""DELETE DATA {{
        {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.DELETE_DATA, "data": block})

    def add_delete_insert_data_block(
        self,
        delete_triples: List[Tuple[str]],
        insert_triples: List[Tuple[str]],
        where_clauses: List[str],
    ):
        block_parts = []
        if self._named_graph:
            block_parts.append(f"WITH {utils.ensure_absolute(self._named_graph)}")
        delete_triples_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in delete_triples
        )
        block_parts.append(f"DELETE {{{delete_triples_combined}}}")

        insert_triples_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in insert_triples
        )
        block_parts.append(f"INSERT {{{insert_triples_combined}}}")

        block_parts.append(f"WHERE {{{self._combine_where_clauses(where_clauses)}}}")
        block = "\n".join(block_parts)
        self._query_blocks.append(
            {"type": SPARQLQueryType.DELETE_INSERT, "data": block}
        )

    def _create_variable_string(self, variables: List[str]) -> str:
        """Create a string representation of the variables for the SELECT query."""
        return " ".join(variables) if variables else "*"

    def _combine_where_clauses(self, where_clauses: List[str]) -> str:
        if len(where_clauses) >= 1:
            return "\n".join(where_clauses)
        else:
            return ""

    def _get_prefix_string(self) -> str:
        return (
            "\n".join(
                f"PREFIX {prefix}: {iri}" for prefix, iri in self._prefixes.items()
            )
            + "\n"
        )

    def _add_explicit_implicit(self) -> Optional[str]:
        if self._include_explicit and not self._include_implicit:
            return "FROM onto:explicit"
        elif self._include_implicit and not self._include_explicit:
            return "FROM onto:implicit"
        return None

    def to_string(self, validate: bool = True) -> str:
        query_parts = []
        if self._prefixes:
            query_parts.append(self._get_prefix_string())

        for block in self._query_blocks:
            query_parts.append(block["data"])

        query = "\n".join(query_parts)
        if validate:
            if self._query_blocks[0]["type"] in (
                SPARQLQueryType.SELECT,
                SPARQLQueryType.SELECT_DISTINCT,
                SPARQLQueryType.SELECT_REDUCED,
                SPARQLQueryType.ASK,
            ):
                # Validate the select or ask query
                utils.validate_query(query)

            elif self._query_blocks[0]["type"] in (
                SPARQLQueryType.INSERT_DATA,
                SPARQLQueryType.DELETE_DATA,
                SPARQLQueryType.DELETE_INSERT,
            ):
                # Validate the update query
                utils.validate_update_query(query)
        return query