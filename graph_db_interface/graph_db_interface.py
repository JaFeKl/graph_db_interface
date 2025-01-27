import logging
import requests
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
from typing import List, Optional
from enum import Enum


class RepositoryNames(Enum):
    REPO_PLAYGROUND = "Playground"
    REPO_DEVELOPMENT = "Development"
    REPO_PRODUCTION = "Production"


class GraphDBinterface():
    """A generic sparql interface to allow easy interactions with a knowledge graph
    """
    def __init__(self,
                 username: str,
                 password: str,
                 base_url: str = "https://graphdb.iam-mms.kit.edu",
                 repository: str = "Playground"):
        self._logger = logging.getLogger("sparql_interface")
        self._username = username
        self._password = password
        self._base_url = base_url
        self._token = self._get_authentication_token(self._username, self._password)
        self._header = {"Authorization": self._token, "Accept": "application/json"}
        self._repository = self._validate_repository(repository)
        self._initialize_sparql_wrapper()

    @staticmethod
    def _validate_repository(repository: str) -> str:
        """Validates if the repository is part of the RepositoryNames enum."""
        if repository not in [repo.value for repo in RepositoryNames]:
            raise ValueError(
                f"Invalid repository name. Allowed values are: {', '.join([repo.value for repo in RepositoryNames])}."
            )
        return repository

    @property
    def repository(self):
        return self._repository

    @repository.setter
    def repository(self, value: str):
        self._validate_repository(value)

    def _initialize_sparql_wrapper(self, endpoint: str = None):
        if endpoint:
            self.sparql = SPARQLWrapper(endpoint=endpoint)
        else:
            self.sparql = SPARQLWrapper(endpoint=f"{self._base_url}/repositories/{self.repository}/statements")
        self.sparql.setCredentials(self._username, self._password)
        self.sparql.setReturnFormat(JSON)

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
        headers = {'Content-Type': 'application/json'}
        payload = {
            "username": username,
            "password": password
        }
        response = requests.post(self._base_url + "/rest/login",
                                 headers=headers,
                                 json=payload)
        if response.status_code == 200:
            return response.headers.get("Authorization")
        else:
            self._logger.error(f"Failed to obtain gdb token: {response.status_code}: {response.text}")
            raise ValueError("You were unable to obtain a token given your provided credentials. Please make sure, that your provided credentials are valid.")

    def get_list_of_repositories(self) -> Optional[List[str]]:
        """Get a list of all existing repositories on the GraphDB instance.

        Returns:
            Optional[List[str]]: Returns a list of repository ids.
        """
        url = f"{self._base_url}/rest/repositories"
        response = requests.get(url, headers=self._header)

        if response.status_code == 200:
            repositories = response.json()
            return repositories
        else:
            self._logger.warning(f"Failed to list repositories: {response.status_code}: {response.text}")
            return None

    def get_list_of_named_graphs(self) -> List:
        """Get a list of named graphs in the currently set repository.

        Returns:
            List: List of named graph IRIs. Can be an empty list.
        """
        self.sparql.endpoint = f"{self._base_url}/repositories/{self._repository}"

        # SPARQL query to retrieve all named graphs
        query = """
        SELECT DISTINCT ?graph WHERE {
        GRAPH ?graph { ?s ?p ?o }
        }
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        return [result["graph"]["value"] for result in results["results"]["bindings"]]

    def _named_graph_string(self, named_graph: str = None) -> str:
        if named_graph:
            return f"GRAPH <{named_graph}>"
        else:
            return ""

    def triple_exists(self, subject: str, predicate: str, object: str, named_graph: str = None) -> bool:
        """Checks if a specified triple exists in the repository

        Args:
            subject (str): valid subject IRI
            predicate (str): valid predicate IRI
            object (str): valid object IRI
            named_graph (str, optional): A specific named graph to query in. Defaults to None.

        Returns:
            bool: Returns True when the given triple exists. False otherwise.
        """
        self.sparql.endpoint = f"{self._base_url}/repositories/{self._repository}"

        query = f"""
            ASK WHERE {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """
        self.sparql.setMethod(GET)
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        if results["boolean"] is True:
            self._logger.debug(f"Found triple {subject}, {predicate}, {object}")
            return True
        else:
            self._logger.debug(f"Unable to find triple {subject}, {predicate}, {object}, named_graph: {named_graph}, repository: {self._repository}")
            return False

    def triple_add(self, subject: str, predicate: str, object: str, named_graph: str = None) -> bool:
        """Add a single triple either to the default graph or to a named graph

        Args:
            subject (str): valid subject IRI
            predicate (str): valid predicate IRI
            object (str): valid object IRI
            named_graph (str, optional): The IRI of a named graph. Defaults to None.

        Returns:
            bool: Returns True if the triple was successfully added. Returns False otherwise.
        """
        self.sparql.endpoint = f"{self._base_url}/repositories/{self.repository}/statements"

        query = f"""
            INSERT DATA {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """

        # Set the query method to POST (for updates)
        self.sparql.setMethod(POST)
        self.sparql.setQuery(query)
        result = self.sparql.query()
        if result.response.status == 204:
            self._logger.debug(f"New triple inserted: {subject}, {predicate}, {object} named_graph: {named_graph}, repository: {self._repository}")
            return True
        else:
            return False

    def triple_delete(self, subject: str, predicate: str, object: str, named_graph: str = None, check_exist: bool = True) -> bool:
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
        self.sparql.endpoint = f"{self._base_url}/repositories/{self.repository}/statements"
        try:
            if check_exist:
                if not self.triple_exists(subject, predicate, object, named_graph):
                    self._logger.warning("Unable to delete triple since it does not exist")
                    return False
            if named_graph:
                query = f"""
                    DELETE WHERE {{
                        {self._named_graph_string(named_graph)} {{
                            {subject} {predicate} {object} .
                        }}
                    }}
                """
            else:
                query = f"""
                    DELETE WHERE {{
                            {subject} {predicate} {object} .
                        }}
                """
            self.sparql.setMethod(POST)
            self.sparql.setQuery(query)
            result = self.sparql.query()
            if result.response.status == 204:
                self._logger.debug(f"Successfully deleted triple: {subject} {predicate} {object}")
                return True
            else:
                return False

        except Exception as e:
            self._logger.error(f"Failed to delete triple, error: {e}")

    def triple_update(
            self,
            old_subject: str,
            old_predicate: str,
            old_object: str,
            new_subject: str,
            new_predicate: str,
            new_object: str,
            named_graph: str = None,
            check_exist: bool = True) -> bool:

        self.sparql.endpoint = f"{self._base_url}/repositories/{self.repository}/statements"

        try:
            if check_exist:
                if not self.triple_exists(old_subject, old_predicate, old_object, named_graph=named_graph):
                    self._logger.warning("Unable to update triple since it does not exist")
                    return False
            query = f"""
                DELETE {{
                    {old_subject} {old_predicate} {old_object} .
                }}
                INSERT {{
                    {new_subject} {new_predicate} {new_object} .
                }}
                WHERE {{
                    {old_subject} {old_predicate} {old_object} .
                }}
            """
            if named_graph:
                query = f"WITH <{named_graph}> " + query
            self._logger.debug(query)
            self.sparql.setMethod(POST)
            self.sparql.setQuery(query)
            result = self.sparql.query()
            if result.response.status == 204:
                self._logger.debug(f"Successfully updated triple to: {new_subject} {new_predicate} {new_object} named_graph: {named_graph}, repository: {self._repository}")
                return True
            else:
                return False

        except Exception as e:
            self._logger.error(f"Failed to update triple, error {e}")

    def triple_update_object(
            self,
            subject: str,
            predicate: str,
            new_object,
            named_graph: str = None) -> bool:

        self.sparql.endpoint = f"{self._base_url}/repositories/{self.repository}/statements"
        try:
            query = f"""
                DELETE {{
                    {subject} {predicate} ?oldObject .
                }}
                INSERT {{
                    {subject} {predicate} {new_object} .
                }}
                WHERE {{
                    {subject} {predicate} ?oldObject .
                }}
            """
            if named_graph:
                query = f"WITH <{named_graph}> " + query
            self._logger.debug(query)
            self.sparql.setMethod(POST)
            self.sparql.setQuery(query)
            result = self.sparql.query()
            if result.response.status == 204:
                self._logger.debug(f"Successfully updated triple to: {subject} {predicate} {new_object} named_graph: {named_graph}, repository: {self._repository}")
                return True
            else:
                return False
        except Exception as e:
            self._logger.error(f"Failed to update triple, error {e}")
