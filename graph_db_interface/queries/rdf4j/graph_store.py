from requests import Response
from typing import TYPE_CHECKING, Optional, Tuple
from rdflib import Graph

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def fetch_statements(
    self: "GraphDB",
    graph_uri: Optional[str] = None,
) -> Tuple[Response, Graph]:
    """
    Fetch the contents of either a explicit named or the default graph.
    If graph_uri is None, the default graph is fetched.
    """
    default_graph = True if graph_uri is None else False
    g = Graph()
    endpoint = f"repositories/{self._repository}/rdf-graphs/service"
    if graph_uri is None:
        graph_uri = "http://www.openrdf.org/schema/sesame#nil"

    response: Response = self._make_request(
        "get",
        endpoint,
        params={"graph": graph_uri} if graph_uri else None,
        headers={"Content-Type": "application/x-turtle"},
    )
    if response.status_code == 200:
        if not default_graph:
            self.logger.debug(f"Named graph {graph_uri} fetched successfully!")
        else:
            self.logger.debug("Default graph fetched successfully!")
        g.parse(data=response.text, format="nt")
    else:
        if not default_graph:
            self.logger.warning(
                f"Failed to fetch named graph: {response.status_code} -"
                f" {response.text}"
            )
        else:
            self.logger.warning(
                f"Failed to fetch default graph: {response.status_code} -"
                f" {response.text}"
            )
    return response, g


def import_statements(
    self: "GraphDB",
    content: str,
    overwrite: bool = False,
    graph_uri: Optional[str] = None,
    content_type: str = "application/x-turtle",
):
    default_graph = True if graph_uri is None else False
    if default_graph:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service?default"
    else:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service"

    method = "put" if overwrite else "post"

    response: Response = self._make_request(
        method,
        endpoint,
        params={"graph": graph_uri} if graph_uri else None,
        headers={"Content-Type": content_type},
        data=content,
    )
    if response.status_code == 204:
        if not default_graph:
            self.logger.debug(
                f"Statements imported to named graph {graph_uri} successfully!"
            )
        else:
            self.logger.debug("Statements imported to default graph successfully!")
    else:
        if not default_graph:
            self.logger.warning(
                f"Failed to import statements to named graph: {response.status_code} -"
                f" {response.text}"
            )
        else:
            self.logger.warning(
                f"Failed to import statements to default graph: {response.status_code} -"
                f" {response.text}"
            )
    return response


def clear_graph(self: "GraphDB", graph_uri: Optional[str] = None):
    """
    Deletes the specified named graph from the triplestore.
    """
    default_graph = True if graph_uri is None else False
    if default_graph:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service?default"
    else:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service"

    response: Response = self._make_request(
        "delete",
        endpoint,
        params={"graph": graph_uri} if graph_uri else None,
    )

    if response.status_code == 204:
        if not default_graph:
            self.logger.debug(f"Named graph {graph_uri} cleared successfully!")
        else:
            self.logger.debug(f"Default graph cleared successfully!")
    else:
        if not default_graph:
            self.logger.warning(
                f"Failed to clear named graph: {response.status_code} - {response.text}"
            )
        else:
            self.logger.warning(
                f"Failed to clear default graph: {response.status_code} - {response.text}"
            )
    return response
