from base64 import b64encode
from typing import List, Union, Optional, Dict
import requests
from requests import Response
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import (
    InvalidRepositoryError,
    AuthenticationError,
    GraphDbException,
)

from . import LOGGER


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
        self.add_prefix("owl", "<http://www.w3.org/2002/07/owl#>")
        self.add_prefix("rdf", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
        self.add_prefix("rdfs", "<http://www.w3.org/2000/01/rdf-schema#>")
        self.add_prefix("onto", "<http://www.ontotext.com/>")

        self.named_graph = named_graph

        LOGGER.info(
            f"Using GraphDB repository '{self.repository}' as user '{self._username}'."
        )

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

    def _validate_repository(self, repository: str) -> str:
        """Validates if the repository is part of the RepositoryNames enum."""
        if repository not in self._repositories:
            raise InvalidRepositoryError(
                "Invalid repository name. Allowed values are:"
                f" {', '.join(list(self._repositories))}."
            )
        return repository

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

    def add_prefix(self, prefix: str, iri: str):
        self._prefixes[prefix] = utils.ensure_absolute(iri)

    def remove_prefix(self, prefix: str) -> bool:
        if prefix in self._prefixes:
            del self._prefixes[prefix]
            return True
        return False

    def get_prefixes(self) -> Dict[str, str]:
        return self._prefixes

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
            raise GraphDbException(
                f"Error while querying GraphDB ({status_code}) - {response.text}"
            )
            return False if update else None

        return True if update else response.json()
