import json
import os
import sys
import pytest
from graph_db_interface import GraphDB


@pytest.fixture(scope="session")
def db() -> GraphDB:
    """Fixture to create a GraphDB client."""
    for env_var in [
        "GRAPHDB_URL",
        "GRAPHDB_USER",
        "GRAPHDB_PASSWORD",
        "GRAPHDB_REPOSITORY",
    ]:
        if os.getenv(env_var) is None:
            print(f"Missing environment variable '{env_var}'.", file=sys.stderr)
            sys.exit(1)

    return GraphDB(
        base_url=os.getenv("GRAPHDB_URL"),
        username=os.getenv("GRAPHDB_USER"),
        password=os.getenv("GRAPHDB_PASSWORD"),
        repository=os.getenv("GRAPHDB_REPOSITORY"),
    )
