import logging
from enum import Enum
from base64 import b64encode
from typing import List, Union, Any, Optional, Dict, Tuple
import requests
from requests import Response
from rdflib import Literal
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import (
    InvalidInputError,
    InvalidRepositoryError,
    AuthenticationError,
)


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
        select_type: SPARQLQueryType = SPARQLQueryType.SELECT,
    ) -> str:
        block_parts = []
        block_parts.append(
            f"{select_type.name} {self._create_variable_string(variables)}"
        )
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
            raise InvalidRepositoryError(
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
        raise AuthenticationError(
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

    def query(
        self,
        query: str,
        update: bool = False,
    ) -> Optional[Union[Dict, bool]]:
        """
        Executes a SPARQL query or update operation on the GraphDB repository.
        Args:
            query (str): The SPARQL query or update string to be executed.
            update (bool, optional): Indicates whether the query is an update operation.
                Defaults to False.
        Returns:
            Optional[Union[Dict, bool]]:
                - If `update` is False, returns the query result as a dictionary (parsed JSON).
                - If `update` is True, returns True if the update was successful.
                - Returns None if the query fails and `update` is False.
                - Returns False if the update fails and `update` is True.
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
            LOGGER.debug(f"Found IRI {iri}")
            return True

        LOGGER.debug(f"Unable to find IRI {iri}")
        return False

    def triple_exists(
        self,
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
            LOGGER.debug(f"Found triple {sub}, {pred}, {obj}")
            return True

        LOGGER.debug(
            f"Unable to find triple {sub}, {pred}, {obj}, named_graph:"
            f" {self._named_graph}, repository: {self._repository}"
        )
        return False

    def triples_get(
        self,
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
        sub: str,
        pred: str,
        obj: Any,
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
            tiples=[(sub, pred, obj)],
        )
        query_string = query.to_string()
        if query_string is None:
            return False

        result = self.query(query=query_string, update=True)
        if result:
            LOGGER.debug(
                f"New triple inserted: {sub}, {pred}, {obj} named_graph:"
                f" {self._named_graph}, repository: {self._repository}"
            )
        return result

    def triple_delete(
        self,
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
                LOGGER.warning("Unable to delete triple since it does not exist")
                return False
        query = SPARQLQuery(
            named_graph=self._named_graph,
            prefixes=self._prefixes,
        )
        query.add_delete_data_block(
            tiples=[(sub, pred, obj)],
        )
        query_string = query.to_string()

        if query_string is None:
            return False

        # Execute the SPARQL query
        result = self.query(query=query_string, update=True)
        if result:
            LOGGER.debug(f"Successfully deleted triple: {sub} {pred} {obj}")
        else:
            LOGGER.warning(f"Failed to delete triple: {sub} {pred} {obj}")

        return result

    def triple_update(
        self,
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
                LOGGER.warning(f"Triple does not exist: {sub_old} {pred_old} {obj_old}")
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
            delete_tiples=[(sub_old, pred_old, obj_old)],
            insert_tiples=[(update_sub, update_pred, update_obj)],
            where_clauses=[f"{sub_old} {pred_old} {obj_old} ."],
        )
        query_string = query.to_string(validate=True)
        if query_string is None:
            return False

        result = self.query(query=query_string, update=True)

        if result:
            LOGGER.debug(
                f"Successfully updated triple to: {update_sub} {update_pred}"
                f" {update_obj}, named_graph: {self._named_graph}, repository:"
                f" {self._repository}"
            )
        else:
            LOGGER.warning(
                f"Failed to update triple to: {update_sub} {update_pred}"
                f" {update_obj}, named_graph: {self._named_graph}, repository:"
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

    def owl_is_named_individual(self, iri: str) -> bool:
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
            LOGGER.warning(f"IRI {iri} is not a named individual!")
            return False
        return True

    def owl_get_classes_of_individual(
        self,
        instance_iri: str,
        ignored_prefixes: Optional[List[str]] = None,
        local_name: bool = True,
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
                Defaults to True.
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
            include_explicit=True,
            include_implicit=True,
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

        print(query_string)
        results = self.query(query=query_string)

        if results is None:
            return []

        classes = [
            result["class"]["value"] for result in results["results"]["bindings"]
        ]
        if local_name is True:
            classes = [utils.get_local_name(iri) for iri in classes]
        return classes
