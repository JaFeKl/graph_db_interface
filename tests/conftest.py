import os
import sys
import pytest
from graph_db_interface import GraphDB, GraphDBCredentials


@pytest.fixture(scope="session")
def db() -> GraphDB:
    """Fixture to create a GraphDB client."""
    for env_var in [
        "GRAPHDB_URL",
        "GRAPHDB_USERNAME",
        "GRAPHDB_PASSWORD",
        "GRAPHDB_TEST_REPOSITORY",
    ]:
        if os.getenv(env_var) is None:
            print(f"Missing environment variable '{env_var}'.", file=sys.stderr)
            sys.exit(1)

    credentials = GraphDBCredentials(
        base_url=os.getenv("GRAPHDB_URL"),
        username=os.getenv("GRAPHDB_USERNAME"),
        password=os.getenv("GRAPHDB_PASSWORD"),
        repository=os.getenv("GRAPHDB_TEST_REPOSITORY"),
    )

    db = GraphDB(credentials=credentials)

    for graph in [
        None,
        "http://example.org/named_graph",
        "http://example.org/local_named_graph",
    ]:
        db.clear_graph(graph)

    return db
