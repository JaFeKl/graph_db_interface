from enum import Enum
from typing import List, Optional, Union
from graph_db_interface.utils import utils
from graph_db_interface.utils.utils import Triple
from graph_db_interface.utils.iri import IRI


class SPARQLQueryType(Enum):
    """Enum for different SPARQL query types."""

    SELECT = "SELECT"
    SELECT_DISTINCT = "SELECT DISTINCT"
    SELECT_REDUCED = "SELECT REDUCED"
    CONSTRUCT = "CONSTRUCT"
    DESCRIBE = "DESCRIBE"
    ASK = "ASK"
    INSERT_DATA = "INSERT DATA"
    INSERT_EXISTS = "INSERT EXISTS"
    DELETE_DATA = "DELETE DATA"
    DELETE_INSERT = "DELETE/INSERT"


class SPARQLQuery:
    """
    Helper for composing and validating SPARQL queries.

    Builds SELECT/ASK and UPDATE blocks (INSERT/DELETE) with optional default
    named graph and control over explicit/implicit inference.
    """

    def __init__(
        self,
        named_graph: Optional[Union[str, IRI]] = None,
        include_explicit: Optional[bool] = True,
        include_implicit: Optional[bool] = True,
    ):
        self._named_graph = IRI(named_graph) if named_graph is not None else None
        self._include_explicit = include_explicit
        self._include_implicit = include_implicit
        self._query_blocks = []

    def add_select_block(
        self,
        variables: List[str],
        where_clauses: List[str],
        select_type: Optional[SPARQLQueryType] = SPARQLQueryType.SELECT,
    ) -> None:
        """
        Add a SELECT block to the query.

        Args:
            variables (List[str]): Variable names to project (e.g., ["?s", "?p", "?o"]).
            where_clauses (List[str]): WHERE patterns/filters to include.
            select_type (Optional[SPARQLQueryType]): SELECT variant (e.g., DISTINCT). Defaults to SELECT.
        """
        block_parts = []
        block_parts.append(
            f"{select_type.value} {self._create_variable_string(variables)}"
        )
        part = self._add_explicit_implicit()
        if self._named_graph:
            block_parts.append(f"FROM {self._named_graph.n3()}")
        if part:
            block_parts.append(part)
        block_parts.append(f"WHERE {{{self._combine_where_clauses(where_clauses)}}}")
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": select_type, "data": block})

    def add_ask_block(
        self,
        where_clauses: List[str],
    ) -> None:
        """
        Add an ASK block to the query.

        Args:
            where_clauses (List[str]): WHERE patterns/filters to include.
        """
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
        triples: List[Triple],
    ) -> None:
        """
        Add an INSERT DATA block comprised of triples.

        Args:
            triples (List[Triple]): Triples to insert.
        """
        block_parts = []
        data_combined = "\n".join(
            utils.triple_to_string(utils.sanitize_triple(triple), ".")
            for triple in triples
        )
        block_parts.append(
            f"""INSERT DATA {{
        {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.INSERT_DATA, "data": block})

    def add_insert_exists_block(
        self,
        triples: List[Triple],
    ) -> None:
        """
        Add an INSERT ... WHERE NOT EXISTS block.

        Args:
            triples (List[Triple]): Triples to insert when they do not already exist.
        """
        block_parts = []
        data_combined = "\n".join(
            utils.triple_to_string(utils.sanitize_triple(triple), ".")
            for triple in triples
        )
        block_parts.append(
            f"""INSERT {{
        {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}}
WHERE {{ FILTER NOT EXISTS {{
    {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}} }}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append(
            {"type": SPARQLQueryType.INSERT_EXISTS, "data": block}
        )

    def add_delete_data_block(
        self,
        triples: List[Triple],
    ) -> None:
        """
        Add a DELETE DATA block comprised of triples.

        Args:
            triples (List[Triple]): Triples to delete.
        """
        block_parts = []
        data_combined = "\n".join(
            utils.triple_to_string(utils.sanitize_triple(triple), ".")
            for triple in triples
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
        delete_triples: List[Triple],
        insert_triples: List[Triple],
        where_clauses: List[str],
    ) -> None:
        """
        Add a combined DELETE/INSERT block with WHERE.

        Args:
            delete_triples (List[Triple]): Triples to delete.
            insert_triples (List[Triple]): Triples to insert.
            where_clauses (List[str]): WHERE patterns selecting the triples to update.
        """
        block_parts = []
        if self._named_graph:
            block_parts.append(f"WITH {self._named_graph.n3()}")
        delete_triples_combined = "\n".join(
            utils.triple_to_string(utils.sanitize_triple(triple), ".")
            for triple in delete_triples
        )
        block_parts.append(f"DELETE {{{delete_triples_combined}}}")

        insert_triples_combined = "\n".join(
            utils.triple_to_string(utils.sanitize_triple(triple), ".")
            for triple in insert_triples
        )
        block_parts.append(f"INSERT {{{insert_triples_combined}}}")

        block_parts.append(f"WHERE {{{self._combine_where_clauses(where_clauses)}}}")
        block = "\n".join(block_parts)
        self._query_blocks.append(
            {"type": SPARQLQueryType.DELETE_INSERT, "data": block}
        )

    def _create_variable_string(
        self,
        variables: List[str],
    ) -> str:
        """
        Create a string representation of projected variables.

        Args:
            variables (List[str]): Variables to project.

        Returns:
            str: Space-separated variables or "*" if empty.
        """
        return " ".join(variables) if variables else "*"

    def _combine_where_clauses(
        self,
        where_clauses: List[str],
    ) -> str:
        """
        Combine WHERE clauses into a single string separated by newlines.

        Args:
            where_clauses (List[str]): WHERE clause strings.

        Returns:
            str: Combined WHERE content.
        """
        if len(where_clauses) >= 1:
            return "\n".join(where_clauses)
        else:
            return ""

    def _add_explicit_implicit(self) -> Optional[str]:
        """
        Generate a FROM clause for explicit/implicit inclusion.

        Returns:
            Optional[str]: A `FROM <...>` clause or `None` when both are included.
        """
        if self._include_explicit and not self._include_implicit:
            return f"FROM <http://www.ontotext.com/explicit>"
        elif self._include_implicit and not self._include_explicit:
            return f"FROM <http://www.ontotext.com/implicit>"
        return None

    def to_string(
        self,
        validate: Optional[bool] = True,
    ) -> str:
        """
        Compose the full SPARQL string and optionally validate it.

        Args:
            validate (Optional[bool]): If True, validate the query or update structure.

        Returns:
            str: The composed query string.

        Raises:
            InvalidQueryError: If validation of the composed query fails.
        """
        query_parts = []
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
                SPARQLQueryType.INSERT_EXISTS,
                SPARQLQueryType.DELETE_DATA,
                SPARQLQueryType.DELETE_INSERT,
            ):
                # Validate the update query
                utils.validate_update_query(query)
        return query
