# To be imported into ..graph_db.py GraphDB class

from typing import List, TYPE_CHECKING
from graph_db_interface.utils.iri import IRI

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


def get_list_of_named_graphs(
    self: "GraphDB",
) -> List[IRI]:
    """
    Get the list of named graphs in the current repository.

    Returns:
        List[IRI]: List of named graph IRIs. Can be an empty list.

    Raises:
        GraphDbException: If the underlying request to GraphDB fails.
    """
    # TODO: This query is quite slow and should be optimized
    # SPARQL query to retrieve all named graphs

    query = """
SELECT DISTINCT ?graph WHERE {
    GRAPH ?graph { ?s ?p ?o }
}
    """
    results = self.query(query, convert_bindings=True)
    if results is None:
        return []
    return [result["graph"] for result in results["results"]["bindings"]]
