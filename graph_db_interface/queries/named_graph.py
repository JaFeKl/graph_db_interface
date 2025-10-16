# To be imported into ..graph_db.py GraphDB class

from typing import List, Optional
from requests import Response

from .. import LOGGER

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
