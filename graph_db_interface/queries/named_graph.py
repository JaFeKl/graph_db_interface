# To be imported into ..graph_db.py GraphDB class

from typing import List, Optional


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
