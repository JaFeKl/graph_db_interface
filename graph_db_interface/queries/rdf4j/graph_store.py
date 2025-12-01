from requests import Response
from typing import TYPE_CHECKING, Optional, Union
from rdflib import Graph
from graph_db_interface.utils.iri import IRI

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def fetch_statements(
    self: "GraphDB",
    graph_iri: Optional[Union[str, IRI]] = None,
) -> Optional[Graph]:
    """
    Fetch statements from a named or the default graph.

    Queries the RDF4J Graph Store endpoint and parses the response into an
    `rdflib.Graph`. When `graph_iri` is None, the default graph is fetched.

    Args:
        graph_iri (Optional[Union[str, IRI]]): The named graph IRI to fetch; when
            None, the default graph is fetched.

    Returns:
        Optional[Graph]: The parsed graph on success; `None` when the request fails.

    Raises:
        requests.exceptions.RequestException: If the underlying HTTP request fails.
    """
    graph_iri_str = (
        str(IRI(graph_iri)) if graph_iri else "http://www.openrdf.org/schema/sesame#nil"
    )
    g = Graph()
    endpoint = f"repositories/{self._repository}/rdf-graphs/service"

    response: Response = self._make_request(
        "get",
        endpoint,
        params={"graph": graph_iri_str},
        headers={"Content-Type": "application/x-turtle"},
    )

    if response.status_code != 200:
        self.logger.warning(
            f"Failed to fetch named graph: {response.status_code} - {response.text}"
            if graph_iri_str
            else f"Failed to fetch default graph: {response.status_code} - {response.text}"
        )
        return None

    self.logger.debug(
        f"Named graph '{graph_iri_str}' fetched successfully!"
        if graph_iri_str
        else "Default graph fetched successfully!"
    )
    g.parse(data=response.text, format="nt")
    return g


def import_statements(
    self: "GraphDB",
    content: str,
    overwrite: Optional[bool] = False,
    graph_iri: Optional[Union[str, IRI]] = None,
    content_type: Optional[str] = "application/x-turtle",
) -> bool:
    """
    Import RDF statements into a named or the default graph.

    Sends content to the RDF4J Graph Store endpoint using POST (append) or PUT
    (overwrite). When `graph_iri` is None, the default graph is targeted.

    Args:
        content (str): RDF content to import.
        overwrite (Optional[bool]): Use PUT to overwrite existing content. Defaults to False.
        graph_iri (Optional[Union[str, IRI]]): Target named graph IRI; default graph when None.
        content_type (Optional[str]): MIME type of `content` (e.g., 'application/x-turtle').
            Defaults to 'application/x-turtle'.

    Returns:
        bool: True on success (HTTP 204), False otherwise.

    Raises:
        requests.exceptions.RequestException: If the underlying HTTP request fails.
    """
    graph_iri_str = str(IRI(graph_iri)) if graph_iri else None
    if graph_iri_str:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service"
    else:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service?default"

    method = "put" if overwrite else "post"

    response: Response = self._make_request(
        method,
        endpoint,
        params={"graph": graph_iri_str} if graph_iri_str else None,
        headers={"Content-Type": content_type},
        data=content,
    )

    if response.status_code != 204:
        self.logger.warning(
            f"Failed to import statements to named graph: {response.status_code} - {response.text}"
            if graph_iri_str
            else f"Failed to import statements to default graph: {response.status_code} - {response.text}"
        )
        return False

    self.logger.debug(
        f"Named graph {graph_iri_str} imported successfully!"
        if graph_iri_str
        else "Default graph imported successfully!"
    )
    return True


def clear_graph(
    self: "GraphDB",
    graph_iri: Optional[Union[str, IRI]] = None,
) -> bool:
    """
    Clear a named graph or the default graph.

    Deletes the specified named graph from the triplestore; when `graph_iri` is
    None, clears the default graph.

    Args:
        graph_iri (Optional[Union[str, IRI]]): IRI of the named graph to clear;
            default graph when None.

    Returns:
        bool: True on success (HTTP 204), False otherwise.

    Raises:
        requests.exceptions.RequestException: If the underlying HTTP request fails.
    """
    graph_iri_str = str(IRI(graph_iri)) if graph_iri else None
    if graph_iri_str:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service"
    else:
        endpoint = f"repositories/{self._repository}/rdf-graphs/service?default"

    response: Response = self._make_request(
        "delete",
        endpoint,
        params={"graph": graph_iri_str} if graph_iri_str else None,
    )

    if response.status_code != 204:
        self.logger.warning(
            f"Failed to clear named graph: {response.status_code} - {response.text}"
            if graph_iri_str
            else f"Failed to clear default graph: {response.status_code} - {response.text}"
        )
        return False

    self.logger.debug(
        f"Named graph {graph_iri_str} cleared successfully!"
        if graph_iri_str
        else "Default graph cleared successfully!"
    )
    return True
