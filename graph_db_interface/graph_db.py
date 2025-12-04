from __future__ import annotations

from base64 import b64encode
from typing import List, Union, Optional, Dict
from graph_db_interface.kafka.kafka_manager import KafkaManager
import requests
import logging
from requests import Response
from graph_db_interface.utils.graph_db_credentials import GraphDBCredentials
from graph_db_interface.utils.iri import IRI
from graph_db_interface.sparql_query import SPARQLQuery
from graph_db_interface.exceptions import (
    InvalidRepositoryError,
    AuthenticationError,
    GraphDbException,
)


class GraphDB:
    """
    High-level client for GraphDB repositories.

    Provides convenience methods for querying and updating data, manages
    authentication, repository selection, a default named graph, and a
    Kafka connector manager.
    """

    def __init__(
        self,
        credentials: GraphDBCredentials,
        timeout: int = 60,
        use_gdb_token: bool = True,
        named_graph: Optional[Union[str, IRI]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        if logger is None:
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = logger
        self._credentials = credentials
        self._timeout = timeout
        self._auth = None

        if use_gdb_token:
            self._auth = self._get_authentication_token(
                self._credentials.username, self._credentials.password
            )
        else:
            token = bytes(
                f"{self._credentials.username}:{self._credentials.password}", "utf-8"
            )
            self._auth = f"Basic {b64encode(token).decode()}"

        self._repositories = self.get_list_of_repositories(only_ids=True)

        self.repository = credentials.repository

        self.named_graph = named_graph
        self.kafka_manager = KafkaManager(db=self)

        self.logger.info(
            f"Using GraphDB repository '{self.repository}' as user '{self._credentials.username}'."
        )

    @classmethod
    def from_env(
        cls,
        logger: Optional[logging.Logger] = None,
    ) -> GraphDB:
        """
        Construct a client using environment-based credentials.

        Returns:
            GraphDB: A configured `GraphDB` instance using `GraphDBCredentials.from_env()`.
        """
        return cls(credentials=GraphDBCredentials.from_env(), logger=logger)

    from graph_db_interface.queries.named_graph import (
        get_list_of_named_graphs,
    )
    from graph_db_interface.queries.rdf4j.graph_store import (
        fetch_statements,
        import_statements,
        clear_graph,
    )

    from graph_db_interface.queries.triple_single import (
        triple_exists,
        triple_add,
        triple_delete,
        triple_update,
    )
    from graph_db_interface.queries.triple_multi import (
        triples_get,
        any_triple_exists,
        all_triple_exists,
        triples_add,
        triples_delete,
        triples_update,
    )
    from graph_db_interface.queries.ontology_helpers import (
        iri_exists,
        is_subclass,
        owl_is_named_individual,
        owl_get_classes_of_individual,
    )

    @property
    def repository(self) -> str:
        """
        The currently selected repository identifier.

        Returns:
            str: The active repository id.
        """
        return self._repository

    @repository.setter
    def repository(
        self,
        value: str,
    ):
        self._repository = self._validate_repository(value)

    @property
    def named_graph(self) -> Optional[IRI]:
        """
        The currently selected default named graph.

        Returns:
            Optional[IRI]: The default named graph as an `IRI`, or `None` if unset.
        """
        return self._named_graph

    @named_graph.setter
    def named_graph(
        self,
        value: Optional[Union[str, IRI]],
    ):
        if value is None:
            self._named_graph = None
            return

        value = IRI(value)

        if value not in self.get_list_of_named_graphs():
            self.logger.warning(
                f"Passed named graph {value} does not exist in the repository."
            )
        self._named_graph = value

    @property
    def named_graph_str(self) -> Optional[str]:
        """
        The selected default named graph as a string.

        Returns:
            Optional[str]: The IRI string of the default named graph, or `None`.
        """
        if self._named_graph is None:
            return None
        return str(self._named_graph)

    def get_list_of_repositories(
        self,
        only_ids: Optional[bool] = False,
    ) -> Union[List[str], List[dict], None]:
        """
        List repositories available on the GraphDB instance.

        Args:
            only_ids (Optional[bool]): When True, return only the repository ids. When False,
                return the full repository descriptor objects. Defaults to False.

        Returns:
            Union[List[str], List[dict], None]: A list of ids if `only_ids=True`, a list of
            repository descriptors otherwise, or `None` when the request fails.
        """
        response = self._make_request("get", "rest/repositories")

        if response.status_code == 200:
            repositories = response.json()
            if only_ids:
                return [repo["id"] for repo in repositories]
            return repositories

        self.logger.warning(
            f"Failed to list repositories: {response.status_code}: {response.text}"
        )
        return None

    def _validate_repository(
        self,
        repository: str,
    ) -> str:
        """
        Validate that the repository exists on the server.

        Args:
            repository (str): The repository identifier to validate.

        Returns:
            str: The validated repository identifier.

        Raises:
            InvalidRepositoryError: If the repository is not available.
        """
        if repository not in self._repositories:
            raise InvalidRepositoryError(
                "Invalid repository name. Allowed values are:"
                f" {', '.join(list(self._repositories))}."
            )
        return repository

    def _make_request(
        self,
        method: str,
        endpoint: str,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> Response:
        """
        Perform an authenticated HTTP request to the GraphDB REST API.

        Args:
            method (str): The HTTP method (e.g., "get", "post").
            endpoint (str): The REST endpoint path relative to `base_url`.
            timeout (Optional[int]): Request timeout in seconds; defaults to the client setting.
            **kwargs: Additional arguments forwarded to `requests`.

        Returns:
            Response: The `requests.Response` object.
        """
        timeout = timeout if timeout is not None else self._timeout

        headers = kwargs.pop("headers", {})

        if self._auth is not None:
            headers["Authorization"] = self._auth

        return getattr(requests, method)(
            f"{self._credentials.base_url}/{endpoint}",
            headers=headers,
            timeout=timeout,
            **kwargs,
        )

    def _get_authentication_token(
        self,
        username: str,
        password: str,
    ) -> str:
        """
        Obtain a GraphDB authentication token.

        Args:
            username (str): The GraphDB username.
            password (str): The GraphDB password.

        Returns:
            str: The `Authorization` header value (e.g., `Bearer ...`).

        Raises:
            AuthenticationError: If a token cannot be obtained with the provided credentials.
        """
        payload = {
            "username": username,
            "password": password,
        }
        response = self._make_request("post", "rest/login", json=payload)
        if response.status_code == 200:
            return response.headers.get("Authorization")

        self.logger.error(
            f"Failed to obtain gdb token: {response.status_code}: {response.text}"
        )
        raise AuthenticationError(
            "You were unable to obtain a token given your provided credentials."
            " Please make sure, that your provided credentials are valid."
        )

    def query(
        self,
        query: Union[SPARQLQuery, str],
        update: Optional[bool] = False,
    ) -> Optional[Union[Dict, bool]]:
        """
        Execute a SPARQL query or update against the repository.

        Args:
            query (Union[SPARQLQuery, str]): The SPARQL query/update string to execute.
            update (Optional[bool]): If True, perform an update; otherwise perform a read query.
                Defaults to False.

        Returns:
            Optional[Union[Dict, bool]]: If `update` is False, the parsed JSON result dict.
            If `update` is True, `True` on success. Returns `None` or `False` only when
            failures are handled upstream; otherwise an exception is raised.

        Raises:
            TypeError: If the `query` parameter is neither a `SPARQLQuery` nor a string.
            InvalidQueryError: If the SPARQL query is malformed.
            GraphDbException: If the HTTP request succeeds but the GraphDB API returns an error status.
        """
        if isinstance(query, SPARQLQuery):
            query = query.to_string()
        elif not isinstance(query, str):
            raise TypeError("Query must be a SPARQLQuery or a string.")

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
            self.logger.error(
                f"Error while querying GraphDB ({status_code}) - {response.text}"
            )
            raise GraphDbException(
                f"Error while querying GraphDB ({status_code}) - {response.text}"
            )

        self.logger.debug(
            f'Query\n"""\n{query}\n"""\nReturned\n{"Update successful (200)" if update else response.json()}'
        )

        return True if update else response.json()
