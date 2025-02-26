import os
import graph_db_interface.utils.utils as utils
from graph_db_interface.graph_db_interface import GraphDB

REQUIRED_ENV_VARS = ["GRAPHDB_URL", "GRAPHDB_USER", "GRAPHDB_PASSWORD", "GRAPHDB_REPOSITORY"]
utils.check_env_vars(REQUIRED_ENV_VARS)

GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_USER = os.getenv("GRAPHDB_USER")
GRAPHDB_PASSWORD = os.getenv("GRAPHDB_PASSWORD")
GRAPHDB_REPOSITORY = os.getenv("GRAPHDB_REPOSITORY")

db = GraphDB(
    base_url=GRAPHDB_URL,
    username=GRAPHDB_USER,
    password=GRAPHDB_PASSWORD,
    repository=GRAPHDB_REPOSITORY)

test_ttl_data = """@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Alice a foaf:Person ;
        foaf:name "Alice" ;
        foaf:knows ex:Bob .

ex:Bob a foaf:Person ;
    foaf:name "Bob2" .
"""


def test_add_and_delete_named_graph():
    response = db.named_graph_add(test_ttl_data, "https://my_test_graph", "application/x-turtle")
    assert (response.status_code == 204)

    response = db.named_graph_delete("https://my_test_graph")
    assert (response.status_code == 204)
