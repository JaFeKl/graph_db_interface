import logging
from typing import List, Union, Any, Optional
import requests
from requests import Response
from rdflib import Literal
from SPARQLWrapper import SPARQLWrapper, JSON, POST, QueryResult
from graph_db_interface.utils import utils


LOGGER = logging.getLogger(__name__)


class GraphDB:
    """A GraphDB interface that abstracts SPARQL queries to a small set of pre-defined class methods."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        repository: str,
        timeout: int = 60,
    ):
        self._base_url = base_url
        self._username = username
        self._password = password
        self._timeout = timeout
        self._token = self._get_authentication_token(self._username, self._password)
        self._header = {"Authorization": self._token, "Accept": "application/json"}
        self._repositories = self.get_list_of_repositories(only_ids=True)
        self._repository = self._validate_repository(repository)
        self._initialize_sparql_wrapper()

        self._prefixes = {}
        self._add_prefix("owl", "<http://www.w3.org/2002/07/owl#>")
        self._add_prefix("rdf", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
        self._add_prefix("rdfs", "<http://www.w3.org/2000/01/rdf-schema#>")
        self._add_prefix("onto", "<http://www.ontotext.com/>")

        LOGGER.info(
            f"Connected to GraphDB. User: {self._username}, Repository:"
            f" {self.repository}"
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
        return self._repository

    @repository.setter
    def repository(self, value: str):
        self._validate_repository(value)

    def _initialize_sparql_wrapper(self, endpoint: str = None):
        if endpoint:
            self.sparql = SPARQLWrapper(endpoint=endpoint)
        else:
            self.sparql = SPARQLWrapper(
                endpoint=f"{self._base_url}/repositories/{self.repository}/statements"
            )
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
        headers = {"Content-Type": "application/json"}
        payload = {
            "username": username,
            "password": password,
        }
        response = requests.post(
            f"{self._base_url}/rest/login",
            headers=headers,
            json=payload,
            timeout=self._timeout,
        )
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
        self._prefixes[prefix] = iri

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

    """ GraphDB Management """

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

    def get_list_of_repositories(
        self, only_ids: bool = False
    ) -> Union[List[str], List[dict], None]:
        """Get a list of all existing repositories on the GraphDB instance.

        Returns:
            Optional[List[str]]: Returns a list of repository ids.
        """
        url = f"{self._base_url}/rest/repositories"
        response = requests.get(url, headers=self._header, timeout=self._timeout)

        if response.status_code == 200:
            repositories = response.json()
            if only_ids:
                return [repo["id"] for repo in repositories]
            return repositories

        LOGGER.warning(
            f"Failed to list repositories: {response.status_code}: {response.text}"
        )
        return None

    """ Utility """

    def _set_explicit(self, query: str) -> str:
        return utils.insert_before_where_clause(
            query=query, from_statement="FROM onto:explicit"
        )

    def _set_implicit(self, query: str) -> str:
        return utils.insert_before_where_clause(
            query=query, from_statement="FROM onto:implicit"
        )

    """RDF4J REST API - SPARQL : SPARQL Query and Update execution"""

    def query(
        self,
        query: str,
        update: bool = False,
        include_explicit: bool = True,
        include_implicit: bool = True,
    ) -> QueryResult:
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
            QueryResult:
                The result of the executed SPARQL query. Defined in the SPARQLWrapper package.
                This object encapsulates the query result and may include data such as bindings,
                errors, or status information.

        Notes:
            - The query is first prefixed with all prefixes defined using `_add_prefix`.
            - The `method` determines the RDF4J endpoint used for the query.
            - If an unsupported `method` is provided, an error is logged, and the query is ignored.
        """
        if update is False:
            self.sparql.endpoint = f"{self._base_url}/repositories/{self._repository}"
        else:
            self.sparql.endpoint = (
                f"{self._base_url}/repositories/{self.repository}/statements"
            )

        # add prefixes in front of Query
        query = f"""
        {self._get_prefix_string()}
        {query}
        """
        if include_explicit and not include_implicit:
            query = self._set_explicit(query)
        elif include_implicit and not include_explicit:
            query = self._set_implicit(query)
        self.sparql.setMethod(POST)
        self.sparql.setQuery(query)
        return self.sparql.query()

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
            update=False,
            include_explicit=include_explicit,
            include_implicit=include_implicit,
        )
        result = result.convert()
        if result["boolean"] is True:
            LOGGER.debug(f"Found IRI {iri}")
            return True

        LOGGER.debug(f"Unable to find IRI {iri}")
        return False

    def triple_exists(
        self,
        subject: str,
        predicate: str,
        object: Union[str, Literal],
        named_graph: str = None,
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
        query = f"""
            ASK WHERE {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """
        results = self.query(query=query, update=False)
        results = self.sparql.query().convert()
        if results["boolean"] is True:
            LOGGER.debug(f"Found triple {subject}, {predicate}, {object}")
            return True

        LOGGER.debug(
            f"Unable to find triple {subject}, {predicate}, {object}, named_graph:"
            f" {named_graph}, repository: {self._repository}"
        )
        return False

    def triple_get_subjects(self, predicate: str, object: str) -> List[str]:
        query = f"""
        SELECT ?subject
        WHERE {{
            ?subject {predicate} {object} .
        }}
        """
        results = self.query(query=query, update=False).convert()
        return [result["subject"]["value"] for result in results["results"]["bindings"]]

    def triple_get_predicates(self, subject: str, object: str) -> List[str]:
        query = f"""
        SELECT ?predicate
        WHERE {{
            {subject} ?predicate {object} .
        }}
        """
        results = self.query(query=query, update=False).convert()
        return [
            result["predicate"]["value"] for result in results["results"]["bindings"]
        ]

    def triple_get_objects(self, subject: str, predicate: str) -> List[Any]:
        query = f"""
        SELECT ?object
        WHERE {{
            {subject} {predicate} ?object .
        }}
        """
        results = self.query(query=query, update=False).convert()

        converted_results = []  # given the values are literals, we try to convert them

        for result in results["results"]["bindings"]:
            obj = result["object"]
            obj_value = obj["value"]
            obj_type = obj.get("datatype")
            if obj_type:
                converted_results.append(utils.from_xsd_literal(obj_value, obj_type))
            else:
                converted_results.append(obj_value)

        return converted_results

    """ POST """

    def triple_add(
        self,
        subject: str,
        predicate: str,
        object: Union[str, Literal],
        named_graph: str = None,
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
        query = f"""
            INSERT DATA {{
                {self._named_graph_string(named_graph)} {{
                    {subject} {predicate} {object} .
                }}
            }}
        """
        result = self.query(query=query, update=True)
        if result.response.status == 204:
            LOGGER.debug(
                f"New triple inserted: {subject}, {predicate}, {object} named_graph:"
                f" {named_graph}, repository: {self._repository}"
            )
            return True

        return False

    def triple_delete(
        self,
        subject: str,
        predicate: str,
        object: Union[str, Literal],
        named_graph: str = None,
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
        if check_exist:
            if not self.triple_exists(subject, predicate, object, named_graph):
                LOGGER.warning("Unable to delete triple since it does not exist")
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
        result = self.query(query=query, update=True)
        if result.response.status == 204:
            LOGGER.debug(f"Successfully deleted triple: {subject} {predicate} {object}")
            return True

        LOGGER.warning(f"Failed to delete triple: {subject} {predicate} {object}")
        return False

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

        if check_exist:
            if not self.triple_exists(
                old_subject, old_predicate, old_object, named_graph=named_graph
            ):
                LOGGER.warning(
                    f"Triple does not exist: {old_subject} {old_predicate} {old_object}"
                )
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

        LOGGER.debug(query)
        result = self.query(query=query, update=True)

        if result.response.status == 204:
            LOGGER.debug(
                f"Successfully updated triple to: {update_subject} {update_predicate}"
                f" {update_object}, named_graph: {named_graph}, repository:"
                f" {self._repository}"
            )
            return True

        LOGGER.warning(
            f"Failed to update triple to: {update_subject} {update_predicate}"
            f" {update_object}, named_graph: {named_graph}, repository:"
            f" {self._repository}, status code: {result.response.status}"
        )
        return False

    """RDF4J REST API - Graph Store : Named graph management"""

    def named_graph_add(
        self, content: str, graph_uri: str, content_type: str = "application/x-turtle"
    ):
        """
        Add statements to a directly referenced named graph. Overrides all existing statements in this graph.
        """

        headers = {"Content-Type": content_type}
        endpoint = (
            f"{self._base_url}/repositories/{self._repository}/rdf-graphs/service?"
            f"graph={graph_uri}"
        )
        response: Response = requests.put(
            endpoint,
            headers=headers,
            auth=(self._username, self._password),
            data=content,
            timeout=self._timeout,
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

        endpoint = (
            f"{self._base_url}/repositories/{self._repository}/rdf-graphs/service"
            f"?graph={graph_uri}"
        )
        response: Response = requests.delete(
            endpoint, auth=(self._username, self._password), timeout=self._timeout
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
        results = self.query(query=query, update=False).convert()
        classes = [
            result["class"]["value"] for result in results["results"]["bindings"]
        ]
        if local_name is True:
            classes = [utils.get_local_name(iri) for iri in classes]
        return classes
