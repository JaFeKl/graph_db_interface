import json
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
        "GRAPHDB_REPOSITORY",
    ]:
        if os.getenv(env_var) is None:
            print(f"Missing environment variable '{env_var}'.", file=sys.stderr)
            sys.exit(1)

    credentials = GraphDBCredentials.from_env()

    return GraphDB(credentials=credentials)
