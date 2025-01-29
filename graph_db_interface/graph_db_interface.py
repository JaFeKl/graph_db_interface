import logging
import requests
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, QueryResult
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
        self._logger = logging.getLogger("graph_db_interface")
        self._username = username
        self._password = password
        self._base_url = base_url
        self._token = self._get_authentication_token(self._username, self._password)
        self._header = {"Authorization": self._token, "Accept": "application/json"}
        self._repository = self._validate_repository(repository)
        self._initialize_sparql_wrapper()
        self._logger.info(f"Connected to GraphDB. User: {self._username}, Repository: {self.repository}")

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

    def query(self, query: str, method=GET) -> QueryResult:
        if method == GET:
            self.sparql.endpoint = f"{self._base_url}/repositories/{self._repository}"
        elif method == POST:
            self.sparql.endpoint = f"{self._base_url}/repositories/{self.repository}/statements"
        else:
            self._logger.error(f"Cannot set query method {method}! Query is ignored.")
            return
        self.sparql.setMethod(method)
        self.sparql.setQuery(query)
        return self.sparql.query()

    def _named_graph_string(self, named_graph: str = None) -> str:
        if named_graph:
            return f"GRAPH {named_graph}"
        else:
            return ""

    def iri_exists(
            self,
            iri: str,
            as_subject: bool = False,
            as_predicate: bool = False,
            as_object: bool = False,
            use_predicate_filter: bool = True,
            named_graph: str = None) -> bool:
        """Check if a given IRI exists.

        Args:
            iri (str): An explicit IRI, e.g. <http://example.org/subject>
            as_subject (bool, optional): If the IRI should be used searched for as a subject. Defaults to False.
            as_predicate (bool, optional): If the IRI should be used searched for as a predicate. Defaults to False.
            as_object (bool, optional): If the IRI should be used searched for as a object. Defaults to False.
            use_predicate_filter (bool, optional): If a filter on a specific set of predicates should be applied. Defaults to True.
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
            self._logger.warning("No clauses defined in which to search the IRI for, returning False")
            return False

        # Filter for unwanted predicates
        if use_predicate_filter:
            filter = "FILTER (?p != <http://www.w3.org/2002/07/owl#sameAs> && ?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)"
        else:
            filter = ""

        query = f"ASK WHERE {{ {self._named_graph_string(named_graph)} {{ {' UNION '.join(clauses)} {filter}}} }}"
        result = self.query(query=query, method=GET)
        result = result.convert()
        if result["boolean"] is True:
            self._logger.debug(f"Found IRI {iri}")
            return True
        else:
            self._logger.debug(f"Unable to find IRI {iri}, check if you successfully")
            return False

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
        query = f"""
            ASK WHERE {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """
        results = self.query(query=query, method=GET)
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
        query = f"""
            INSERT DATA {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """
        result = self.query(query=query, method=POST)
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
        result = self.query(query=query, method=POST)
        if result.response.status == 204:
            self._logger.debug(f"Successfully deleted triple: {subject} {predicate} {object}")
            return True
        else:
            self._logger.warning(f"Failed to delete triple: {subject} {predicate} {object}")
            return False

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
        """
        Updates a triple in the RDF store by replacing an existing triple with a new one.

        This function performs a SPARQL `DELETE ... INSERT ... WHERE` update to remove an old triple
        and insert a new one. Optionally, it can check if the old triple exists before attempting
        the update.

        Args:
            old_subject (str): The subject of the triple to be updated.
            old_predicate (str): The predicate of the triple to be updated.
            old_object (str): The object of the triple to be updated.
            new_subject (str): The subject of the new triple to be inserted.
            new_predicate (str): The predicate of the new triple to be inserted.
            new_object (str): The object of the new triple to be inserted.
            named_graph (str, optional): The named graph where the triple update should be performed.
                                        Defaults to `None`, meaning the default graph is used.
            check_exist (bool, optional): If `True`, the function first checks if the old triple exists
                                        before attempting an update. Defaults to `True`.

        Returns:
            bool: `True` if the update was successful, `False` otherwise.

        Raises:
            Any exceptions thrown by `self.query()` if the SPARQL update request fails.

        Example:
            ```python
            success = rdf_store.triple_update(
                old_subject="<http://example.org/oldSubject>",
                old_predicate="<http://example.org/oldPredicate>",
                old_object="<http://example.org/oldObject>",
                new_subject="<http://example.org/newSubject>",
                new_predicate="<http://example.org/newPredicate>",
                new_object="<http://example.org/newObject>",
                named_graph="<http://example.org/graph>",
                check_exist=True
            )
            ```
        """

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
            query = f"WITH {named_graph} " + query
        result = self.query(query=query, method=POST)
        if result.response.status == 204:
            self._logger.debug(f"Successfully updated triple to: {new_subject} {new_predicate} {new_object} named_graph: {named_graph}, repository: {self._repository}")
            return True
        else:
            return False

    def triple_update_any(
            self,
            old_subject: str = None,
            old_predicate: str = None,
            old_object: str = None,
            new_subject: str = None,
            new_predicate: str = None,
            new_object: str = None,
            named_graph: str = None,
            check_exist: bool = True) -> bool:
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
            self._logger.warning("All parts of the old triple (subject, predicate, object) must be provided.")
            return False

        if new_subject is None and new_predicate is None and new_object is None:
            self._logger.warning("At least one of new_subject, new_predicate, or new_object must be provided.")
            return False

        if check_exist:
            if not self.triple_exists(old_subject, old_predicate, old_object, named_graph=named_graph):
                self._logger.warning(f"Triple does not exist: {old_subject} {old_predicate} {old_object}")
                return False

        # Determine replacement variables
        update_subject = new_subject if new_subject else old_subject
        update_predicate = new_predicate if new_predicate else old_predicate
        update_object = new_object if new_object else old_object

        # Construct the SPARQL query
        query = f"""
            DELETE {{
                {old_subject} {old_predicate} {old_object} .
            }}
            INSERT {{
                {update_subject} {update_predicate} {update_object} .
            }}
            WHERE {{
                {old_subject} {old_predicate} {old_object} .
            }}
        """

        if named_graph:
            query = f"WITH {named_graph} " + query

        self._logger.debug(query)
        result = self.query(query=query, method=POST)

        if result.response.status == 204:
            self._logger.debug(f"Successfully updated triple to: {update_subject} {update_predicate} {update_object}, "
                               f"named_graph: {named_graph}, repository: {self._repository}")
            return True
        else:
            self._logger.warning(f"Failed to update triple to: {update_subject} {update_predicate} {update_object}, "
                                 f"named_graph: {named_graph}, repository: {self._repository}, "
                                 f"status code: {result.response.status}")
            return False
