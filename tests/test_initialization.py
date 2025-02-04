import pytest
import os
import graph_db_interface.utils.utils as utils
from graph_db_interface.graph_db_interface import GraphDB

REQUIRED_ENV_VARS = ["GRAPHDB_URL", "GRAPHDB_USER", "GRAPHDB_PASSWORD", "GRAPHDB_REPOSITORY"]
utils.check_env_vars(REQUIRED_ENV_VARS)

GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_USER = os.getenv("GRAPHDB_USER")
GRAPHDB_PASSWORD = os.getenv("GRAPHDB_PASSWORD")
GRAPHDB_REPOSITORY = os.getenv("GRAPHDB_REPOSITORY")


def test_credentials_valid():
    """Test successfull initialization
    """
    try:
        GraphDB(
            base_url=GRAPHDB_URL,
            username=GRAPHDB_USER,
            password=GRAPHDB_PASSWORD,
            repository=GRAPHDB_REPOSITORY)
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")


def test_credentials_invalid():
    """Test invalid credentials used
    """
    with pytest.raises(ValueError):
        GraphDB(
            base_url=GRAPHDB_URL,
            username=GRAPHDB_USER,
            password="SomeWrongPassword",
            repository=GRAPHDB_REPOSITORY)


def test_invalid_repository():
    """Test an invalid selected repository
    """
    with pytest.raises(ValueError):
        GraphDB(
            base_url=GRAPHDB_URL,
            username=GRAPHDB_USER,
            password=GRAPHDB_PASSWORD,
            repository="SomeWrongRepository")
