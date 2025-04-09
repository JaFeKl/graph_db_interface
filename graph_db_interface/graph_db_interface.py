import logging
from enum import Enum
from base64 import b64encode
from typing import List, Union, Any, Optional, Dict, Tuple
import requests
from requests import Response
from rdflib import Literal
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidQueryError


LOGGER = logging.getLogger(__name__)


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
    ) -> str:
        block_parts = []
        block_parts.append(f"SELECT {self._create_variable_string(variables)}")
        part = self._add_explicit_implicit()
        if self._named_graph:
            block_parts.append(f"FROM {utils.ensure_absolute(self._named_graph)}")
        if part:
            block_parts.append(part)
        block_parts.append(f"WHERE {{{self._combine_where_clauses(where_clauses)}}}")
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.SELECT, "data": block})

    def add_ask_block(
        self,
        where_clauses: List[str],
    ) -> str:
        block_parts = []
        block_parts.append(
            f"""
ASK WHERE {{
    {utils.encapsulate_named_graph(self._named_graph, self._combine_where_clauses(where_clauses))
}}}
"""
        )
        block = "\n".join(block_parts)
        self._query_blocks.append({"type": SPARQLQueryType.ASK, "data": block})

    def add_insert_data_block(
        self,
        tiples: List[Tuple[str]],
    ) -> str:
        block_parts = []
        data_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in tiples
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
        tiples: List[Tuple[str]],
    ) -> str:
        block_parts = []
        data_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in tiples
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
        delete_tiples: List[Tuple[str]],
        insert_tiples: List[Tuple[str]],
        where_clauses: List[str],
    ):
        block_parts = []
        if self._named_graph:
            block_parts.append(f"WITH {utils.ensure_absolute(self._named_graph)}")
        delete_tiples_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in delete_tiples
        )
        block_parts.append(f"DELETE {{{delete_tiples_combined}}}")

        insert_tiples_combined = "\n".join(
            f"{triple[0]} {triple[1]} {triple[2]} ." for triple in insert_tiples
        )
        block_parts.append(f"INSERT {{{insert_tiples_combined}}}")

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

    def to_string(self, validate: bool = True) -> Optional[str]:
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
                if not utils.validate_query(query):
                    LOGGER.error(
                        "The constructed query is invalid. Please check the query syntax."
                    )
                    return None
            elif self._query_blocks[0]["type"] in (
                SPARQLQueryType.INSERT_DATA,
                SPARQLQueryType.DELETE_DATA,
                SPARQLQueryType.DELETE_INSERT,
            ):
                # Validate the update query
                if not utils.validate_update_query(query):
                    LOGGER.error(
                        "The constructed update query is invalid. Please check the query syntax."
                    )
                    return None
        return query


class GraphDB:
    """A GraphDB interface that abstracts SPARQL queries and provides a small set of commonly needed queries."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        repository: str,
        timeout: int = 60,
        use_gdb_token: bool = True,
        named_graph: Optional[str] = None,
    ):
        self._base_url = base_url
        self._username = username
        self._password = password
        self._timeout = timeout
        self._auth = None

        if use_gdb_token:
            self._auth = self._get_authentication_token(self._username, self._password)
        else:
            token = bytes(f"{self._username}:{self._password}", "utf-8")
            self._auth = f"Basic {b64encode(token).decode()}"

        self._repositories = self.get_list_of_repositories(only_ids=True)
        self.repository = repository

        self._prefixes = {}
        self._add_prefix("owl", "<http://www.w3.org/2002/07/owl#>")
        self._add_prefix("rdf", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
        self._add_prefix("rdfs", "<http://www.w3.org/2000/01/rdf-schema#>")
        self._add_prefix("onto", "<http://www.ontotext.com/>")

        self.named_graph = named_graph

        LOGGER.info(
            f"Using GraphDB repository '{self.repository}' as user '{self._username}'."
        )

    def _validate_repository(self, repository: str) -> str:
        """Validates if the repository is part of the RepositoryNames enum."""
        if repository not in self._repositories:
            raise ValueError(
                "Invalid repository name. Allowed values are:"
                f" {', '.join(list(self._repositories))}."
            )
        return repository

    @property
    def repository(self):
        """The currently selected respository in the Graph DB instance."""
        return self._repository

    @repository.setter
    def repository(self, value: str):
        self._repository = self._validate_repository(value)

    @property
    def named_graph(self):
        """The currently selected named graph in the Graph DB instance."""
        return self._named_graph

    @named_graph.setter
    def named_graph(self, value: Optional[str]):
        if value is not None:
            if utils.strip_angle_brackets(value) not in self.get_list_of_named_graphs():
                LOGGER.warning(
                    f"Passed named graph {value} does not exist in the repository."
                )
            self._named_graph = utils.ensure_absolute(value)
        else:
            self._named_graph = None

    def _make_request(
        self, method: str, endpoint: str, timeout: int = None, **kwargs
    ) -> Response:
        timeout = timeout if timeout is not None else self._timeout

        headers = kwargs.pop("headers", {})

        if self._auth is not None:
            headers["Authorization"] = self._auth

        return getattr(requests, method)(
            f"{self._base_url}/{endpoint}", headers=headers, timeout=timeout, **kwargs
        )

    def _get_authentication_token(self, username: str, password: str) -> str:
        """Obtain a GDB authentication token given your username and your password

        Args:
            username (str): username of your GraphDB account
            password (str): password of your GraphDB account

        Raises:
            ValueError: raised when no token could be successfully obtained

        Returns:
            str: gdb token
        """
        payload = {
            "username": username,
            "password": password,
        }
        response = self._make_request("post", "rest/login", json=payload)
        if response.status_code == 200:
            return response.headers.get("Authorization")

        LOGGER.error(
            f"Failed to obtain gdb token: {response.status_code}: {response.text}"
        )
        raise ValueError(
            "You were unable to obtain a token given your provided credentials."
            " Please make sure, that your provided credentials are valid."
        )

    def _add_prefix(self, prefix: str, iri: str):
        self._prefixes[prefix] = utils.ensure_absolute(iri)

    def _get_prefix_string(self) -> str:
        return (
            "\n".join(
                f"PREFIX {prefix}: {iri}" for prefix, iri in self._prefixes.items()
            )
            + "\n"
        )

    def _named_graph_string(self, named_graph: str = None) -> str:
        if named_graph:
            return f"GRAPH {named_graph}"

        return ""

    def construct_query(
        self,
        query_type: SPARQLQueryType,
        variables: Optional[List[str]] = None,
        where_clauses: List[str] = [],
        insert_or_delete_triples: List[Tuple[str]] = [],
        include_explicit: bool = True,
        include_implicit: bool = True,
    ) -> Optional[str]:
        """Construct a SPARQL query string based on the provided query type."""
        query_lines = []

        # Add prefixes if provided
        if self._prefixes:
            query_lines.append(self._get_prefix_string())

        # Query head
        if query_type in (
            SPARQLQueryType.SELECT,
            SPARQLQueryType.SELECT_DISTINCT,
            SPARQLQueryType.SELECT_REDUCED,
        ):
            var_str = " ".join(variables) if variables else "*"
            query_lines.append(f"{query_type.value} {var_str}")
            if self._named_graph:
                query_lines.append(f"FROM {utils.ensure_absolute(self._named_graph)}")

            if include_explicit and not include_implicit:
                query_lines.append("FROM onto:explicit")
            elif include_implicit and not include_explicit:
                query_lines.append("FROM onto:implicit")

            if len(where_clauses) > 1:
                where_clauses_combined = "\n".join(where_clauses)
                where_clause_string = f"""
WHERE {{
    {where_clauses_combined}
}}
"""
            else:
                where_clause_string = "WHERE { }"

            query_lines.append(where_clause_string)

        elif query_type == SPARQLQueryType.ASK:
            where_clauses_combined = " .\n".join(where_clauses)
            combined = f"""
{query_type.value} WHERE {{
    {utils.encapsulate_named_graph(self._named_graph, where_clauses_combined)}
    }}
"""
            query_lines.append(combined)

        elif query_type in (SPARQLQueryType.INSERT_DATA, SPARQLQueryType.DELETE_DATA):
            data_combined = "\n".join(
                f"{triple[0]} {triple[1]} {triple[2]} ."
                for triple in insert_or_delete_triples
            )
            full_clause = f"""
{query_type.value} {{
    {utils.encapsulate_named_graph(self._named_graph, data_combined)}
}}
"""
            query_lines.append(full_clause)

        query = "\n".join(query_lines)

        if query_type in (
            SPARQLQueryType.SELECT,
            SPARQLQueryType.SELECT_DISTINCT,
            SPARQLQueryType.SELECT_REDUCED,
            SPARQLQueryType.ASK,
        ):
            if not utils.validate_query(query):
                LOGGER.error(
                    "The constructed query is invalid. Please check the query syntax."
                )
                return None
        elif query_type in (SPARQLQueryType.INSERT_DATA, SPARQLQueryType.DELETE_DATA):
            if not utils.validate_update_query(query):
                LOGGER.error(
                    "The constructed update query is invalid. Please check the query syntax."
                )
                return None
        return query

    def query(
        self,
        query: str,
        update: bool = False,
    ) -> Optional[Union[Dict, bool]]:
        """
        Executes a SPARQL query with optional handling of explicit and implicit statements.

        This method sends a SPARQL query to the specified endpoint using either the GET or POST method.
        It also allows the inclusion of explicit and/or implicit statements based on the provided flags.

        Args:
            query (str):
                The SPARQL query string to be executed.

            update (bool, optional):
                If True, the /repositories/{repositoryID}/statements endpoint is being used with 'POST'
                If False, the /repositories/{repositoryID} endpoint is used with 'POST'
                Defaults to 'False'.

            include_explicit (bool, optional):
                If True, explicit statements are included in the query.
                If False, explicit statements are excluded. Defaults to True.

            include_implicit (bool, optional):
                If True, implicit statements are included in the query. Defaults to True.
                If False, implicit statements are excluded. Defaults to True.

        Returns:
            Optional[Union[Dict, bool]]:
                The result of the executed SPARQL query as a parsed JSON object for
                regular queries or a boolean for update queries.

        Notes:
            - The query is first prefixed with all prefixes defined using `_add_prefix`.
            - The `method` determines the RDF4J endpoint used for the query.
            - If an unsupported `method` is provided, an error is logged, and the query is ignored.
        """

        endpoint = f"repositories/{self._repository}"
        headers = {
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        }

        if update:
            endpoint += "/statements"
            headers["Content-Type"] = "application/sparql-update"
        response = self._make_request("post", endpoint, headers=headers, data=query)

        if not response.ok:
            status_code = response.status_code
            LOGGER.error(
                f"Error while querying GraphDB ({status_code}) - {response.text}"
            )
            return False if update else None

        return True if update else response.json()

    """ GraphDB Management """

    def get_list_of_named_graphs(self) -> Optional[List]:
        """Get a list of named graphs in the currently set repository.

        Returns:
            Optional[List]: List of named graph IRIs. Can be an empty list.
        """
        # TODO: This query is quite slow and should be optimized
        # SPARQL query to retrieve all named graphs
        query = """
        SELECT DISTINCT ?graph WHERE {
        GRAPH ?graph { ?s ?p ?o }
        }
        """

        results = self.query(query)

        if results is None:
            return []

        return [result["graph"]["value"] for result in results["results"]["bindings"]]

    def get_list_of_repositories(
        self, only_ids: bool = False
    ) -> Union[List[str], List[dict], None]:
        """Get a list of all existing repositories on the GraphDB instance.

        Returns:
            Optional[List[str]]: Returns a list of repository ids.
        """
        response = self._make_request("get", "rest/repositories")

        if response.status_code == 200:
            repositories = response.json()
            if only_ids:
                return [repo["id"] for repo in repositories]
            return repositories

        LOGGER.warning(
            f"Failed to list repositories: {response.status_code}: {response.text}"
        )
        return None

    """RDF4J REST API - SPARQL : SPARQL Query and Update execution"""

    """ GET """

    def iri_exists(
        self,
        iri: str,
        as_subject: bool = False,
        as_predicate: bool = False,
        as_object: bool = False,
        filters: dict = None,
        include_explicit: bool = True,
        include_implicit: bool = True,
        named_graph: str = None,
    ) -> bool:
        """Check if a given IRI exists.

        Args:
            iri (str): An IRI, e.g. absolute <http://example.org/subject> or prefixed, e.g. ex:subject
            as_subject (bool, optional): If the IRI should be searched for as a subject. Defaults to False.
            as_predicate (bool, optional): If the IRI should be searched for as a predicate. Defaults to False.
            as_object (bool, optional): If the IRI should be searched for as a object. Defaults to False.
            filters (dict, optional): A dictionary that maps list of IRIS to either 's', 'p', 'o' and defines if triples that match
                these cases should be ignored. Defaults to None. E.g. filters = {'p' = [<http://example.org/predicate>]}

            named_graph (str, optional): A specific named graph to query in. Defaults to None.

        Returns:
            bool: returns True if iri in the given triple positions exists, false otherwise.
        """

        # Define potential query parts
        clauses = []
        if as_subject:
            clauses.append(f"{{{iri} ?p ?o . }}")
        if as_predicate:
            clauses.append(f"{{?s {iri} ?o . }}")
        if as_object:
            clauses.append(f"{{?s ?p {iri} . }}")

        if not clauses:
            LOGGER.warning(
                "No clauses defined in which to search the IRI for, returning False"
            )
            return False

        # Generate FILTER conditions dynamically
        filter_conditions = []
        if filters:
            for var, values in filters.items():
                if values:
                    conditions = " && ".join([f"?{var} != {value}" for value in values])
                    filter_conditions.append(f"FILTER ({conditions})")

        filter_clause = " ".join(filter_conditions)
        query = (
            f"ASK WHERE {{ {self._named_graph_string(named_graph)} {{"
            f" {' UNION '.join(clauses)} {filter_clause} }} }}"
        )
        result = self.query(
            query=query,
            include_explicit=include_explicit,
            include_implicit=include_implicit,
        )
        if result is not None and result["boolean"]:
            LOGGER.debug(f"Found IRI {iri}")
            return True

        LOGGER.debug(f"Unable to find IRI {iri}")
        return False

    def triple_exists(
        self,
        subject: str,
        predicate: str,
        object: Union[str, Literal],
    ) -> bool:
        """Checks if a specified triple exists in the repository

        Args:
            subject (str): valid subject IRI
            predicate (str): valid predicate IRI
            object (str): valid object IRI
            named_graph (str, optional): A specific named graph to query in. Defaults to None.

        Returns:
            bool: Returns True when the given triple exists. False otherwise.
        """
        subject = utils.prepare_subject_or_predicate(subject, ensure_iri=True)
        predicate = utils.prepare_subject_or_predicate(predicate, ensure_iri=True)
        object = utils.prepare_object(object, as_string=True)

        query = SPARQLQuery(named_graph=self._named_graph, prefixes=self._prefixes)
        query.add_ask_block(
            where_clauses=[
                f"{subject} {predicate} {object} .",
            ],
        )
        query_string = query.to_string()

        result = self.query(query=query_string)
        if result is not None and result["boolean"]:
            LOGGER.debug(f"Found triple {subject}, {predicate}, {object}")
            return True

        LOGGER.debug(
            f"Unable to find triple {subject}, {predicate}, {object}, named_graph:"
            f" {self._named_graph}, repository: {self._repository}"
        )
        return False

    def triples_get(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        object: Optional[Any] = None,
        include_explicit: bool = True,
        include_implicit: bool = True,
    ) -> Union[List[Tuple], List[str]]:
        """
        We can either look for
            - Case 1: outgoing relations (subject given) or
            - Case 2: incoming relations (object given).

        Additionally, it can either be an absolute or relative IRI:
            - case 1: subject is a valid absolute or relative IRI, which means we only consider a single subject
            - case 2: subject is not an IRI and just a single string and we want to filter for it which might return multiple subjects

        The output is a list of triple as:
            [(subject, predicate, object), ...]
        or if only_predicates is True:
            [predicate, ...]
        """
        if subject is None and predicate is None and object is None:
            LOGGER.error(
                "At least one of subject, predicate, or object must be provided, returning empty list"
            )
            return []

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

        if subject is not None:
            append_bind_and_filter("?s", subject)

        if predicate is not None:
            append_bind_and_filter("?p", predicate)

        if object is not None:
            object = utils.prepare_object(object)
            append_bind_and_filter("?o", object)

        # query = self.construct_query(
        #     query_type=SPARQLQueryType.SELECT,
        #     variables=["?s", "?p", "?o"],
        #     where_clauses=binds + ["?s ?p ?o ."] + filter,
        #     include_explicit=include_explicit,
        #     include_implicit=include_implicit,
        # )

        # print(query)

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
        query_string = query.to_string()
        if query_string is None:
            LOGGER.error(
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

    """ POST """

    def triple_add(
        self,
        subject: str,
        predicate: str,
        object: Any,
    ) -> bool:
        """Add a single triple either to the default graph or to a named graph

        Args:
            subject (str): valid subject IRI
            predicate (str): valid predicate IRI
            object (str): valid object IRI
            named_graph (str, optional): The IRI of a named graph. Defaults to None.

        Returns:
            bool: Returns True if the triple was successfully added. Returns False otherwise.
        """
        subject = utils.prepare_subject_or_predicate(subject, ensure_iri=True)
        predicate = utils.prepare_subject_or_predicate(predicate, ensure_iri=True)
        object = utils.prepare_object(object, as_string=True)

        query = SPARQLQuery(
            named_graph=self._named_graph,
            prefixes=self._prefixes,
        )
        query.add_insert_data_block(
            tiples=[(subject, predicate, object)],
        )
        query_string = query.to_string()
        if query_string is None:
            return False

        result = self.query(query=query_string, update=True)
        if result:
            LOGGER.debug(
                f"New triple inserted: {subject}, {predicate}, {object} named_graph:"
                f" {self._named_graph}, repository: {self._repository}"
            )
        return result

    def triple_delete(
        self,
        subject: str,
        predicate: str,
        object: Union[str, Literal],
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
        subject = utils.prepare_subject_or_predicate(subject, ensure_iri=True)
        predicate = utils.prepare_subject_or_predicate(predicate, ensure_iri=True)
        object = utils.prepare_object(object, as_string=True)

        if check_exist:
            if not self.triple_exists(subject, predicate, object):
                LOGGER.warning("Unable to delete triple since it does not exist")
                return False
        query = SPARQLQuery(
            named_graph=self._named_graph,
            prefixes=self._prefixes,
        )
        query.add_delete_data_block(
            tiples=[(subject, predicate, object)],
        )
        query_string = query.to_string()

        if query_string is None:
            return False

        # Execute the SPARQL query
        result = self.query(query=query_string, update=True)
        if result:
            LOGGER.debug(f"Successfully deleted triple: {subject} {predicate} {object}")
        else:
            LOGGER.warning(f"Failed to delete triple: {subject} {predicate} {object}")

        return result

    def triple_update(
        self,
        old_subject: str = None,
        old_predicate: str = None,
        old_object: Union[str, Literal] = "?o",
        new_subject: str = None,
        new_predicate: str = None,
        new_object: Union[str, Literal] = None,
        named_graph: str = None,
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
        if not (old_subject and old_predicate and old_object):
            LOGGER.warning(
                "All parts of the old triple (subject, predicate, object) must be"
                " provided."
            )
            return False

        if new_subject is None and new_predicate is None and new_object is None:
            LOGGER.warning(
                "At least one of new_subject, new_predicate, or new_object must be"
                " provided."
            )
            return False

        old_subject = utils.prepare_subject_or_predicate(old_subject, ensure_iri=True)
        old_predicate = utils.prepare_subject_or_predicate(
            old_predicate, ensure_iri=True
        )
        old_object = utils.prepare_object(old_object, as_string=True)

        if check_exist:
            if not self.triple_exists(
                old_subject,
                old_predicate,
                old_object,
            ):
                LOGGER.warning(
                    f"Triple does not exist: {old_subject} {old_predicate} {old_object}"
                )
                return False

        if new_subject is not None:
            new_subject = utils.prepare_subject_or_predicate(
                new_subject, ensure_iri=True
            )
        if new_predicate is not None:
            new_predicate = utils.prepare_subject_or_predicate(
                new_predicate, ensure_iri=True
            )
        if new_object is not None:
            new_object = utils.prepare_object(new_object, as_string=True)

        # Determine replacement variables
        update_subject = new_subject if new_subject else old_subject
        update_predicate = new_predicate if new_predicate else old_predicate
        update_object = new_object if new_object else old_object

        query = SPARQLQuery(
            named_graph=self._named_graph,
            prefixes=self._prefixes,
        )
        query.add_delete_insert_data_block(
            delete_tiples=[(old_subject, old_predicate, old_object)],
            insert_tiples=[(update_subject, update_predicate, update_object)],
            where_clauses=[f"{old_subject} {old_predicate} {old_object} ."],
        )
        query_string = query.to_string()
        if query_string is None:
            return False

        print(query_string)
        # if named_graph:
        #     query = f"WITH {named_graph} " + query

        # LOGGER.debug(query)
        result = self.query(query=query_string, update=True)

        if result:
            LOGGER.debug(
                f"Successfully updated triple to: {update_subject} {update_predicate}"
                f" {update_object}, named_graph: {named_graph}, repository:"
                f" {self._repository}"
            )
        else:
            LOGGER.warning(
                f"Failed to update triple to: {update_subject} {update_predicate}"
                f" {update_object}, named_graph: {named_graph}, repository:"
                f" {self._repository}"
            )

        return result

    """RDF4J REST API - Graph Store : Named graph management"""

    def named_graph_add(
        self, content: str, graph_uri: str, content_type: str = "application/x-turtle"
    ):
        """
        Add statements to a directly referenced named graph. Overrides all existing statements in this graph.
        """
        response: Response = self._make_request(
            "put",
            f"repositories/{self._repository}/rdf-graphs/service",
            params={"graph": graph_uri},
            headers={"Content-Type": content_type},
            data=content,
        )
        if response.status_code == 204:
            LOGGER.debug(f"Named graph {graph_uri} created successfully!")
        else:
            LOGGER.warning(
                f"Failed to update named graph: {response.status_code} -"
                f" {response.text}"
            )
        return response

    def named_graph_delete(self, graph_uri: str):
        """
        Deletes the specified named graph from the triplestore.
        """
        response: Response = self._make_request(
            "delete",
            f"repositories/{self._repository}/rdf-graphs/service",
            params={"graph": graph_uri},
        )

        if response.status_code == 204:
            LOGGER.debug(f"Named graph {graph_uri} deleted successfully!")
        else:
            LOGGER.warning(
                f"Failed to delete named graph: {response.status_code} - {response.text}"
            )
        return response

    """ Convenience """

    def is_subclass(self, subclass_iri: str, class_iri: str) -> bool:
        return self.triple_exists(subclass_iri, "rdfs:subClassOf", class_iri)

    def owl_is_named_individual(self, iri: str) -> bool:
        if not self.triple_exists(iri, "rdf:type", "owl:NamedIndividual"):
            LOGGER.warning(f"IRI {iri} is not a named individual!")
            return False

        return True

    def owl_get_classes_of_individual(
        self,
        instance_iri: str,
        ignored_prefixes: Optional[List[str]] = None,
        local_name: bool = True,
    ) -> List[str]:
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

        query = f"""
        SELECT ?class
        WHERE {{
            ?class rdf:type owl:Class .
            {instance_iri} rdf:type ?class .
                {filter_conditions}
        }}
        """
        results = self.query(query=query)

        if results is None:
            return []

        classes = [
            result["class"]["value"] for result in results["results"]["bindings"]
        ]
        if local_name is True:
            classes = [utils.get_local_name(iri) for iri in classes]
        return classes
