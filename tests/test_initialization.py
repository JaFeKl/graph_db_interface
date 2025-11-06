import os
import pytest
from graph_db_interface import GraphDB, GraphDBCredentials
from graph_db_interface.exceptions import AuthenticationError, InvalidRepositoryError


def test_credentials_valid(db: GraphDB):
    """Test successfull initialization"""
    try:
        credentials = GraphDBCredentials(
            base_url=db._base_url,
            username=db._username,
            password=db._password,
            repository=db._repository,
        )
        GraphDB(credentials=credentials)
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")

def test_credentials_from_environment(db: GraphDB):
    """Test successfull initialization"""
    if os.getenv("GRAPHDB_URL") is None \
        or os.getenv("GRAPHDB_USERNAME") is None \
        or os.getenv("GRAPHDB_PASSWORD") is None \
        or os.getenv("GRAPHDB_REPOSITORY") is None:
        pytest.skip("One or more environment variables are not set")

    try:
        credentials = GraphDBCredentials.from_env()
        GraphDB(credentials=credentials)
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")
        
    try:
        GraphDB.from_env()
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")


def test_credentials_from_class(db: GraphDB):
    """Test successfull initialization"""
    try:
        credentials = GraphDBCredentials(
            base_url=db._base_url,
            username=db._username,
            password=db._password,
            repository=db._repository
        )
        GraphDB(
            *credentials
        )
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")


def test_credentials_invalid(db: GraphDB):
    """Test invalid credentials used"""

    credentials = GraphDBCredentials(
        base_url=db._base_url,
        username=db._username,
        password="SomeWrongPassword",
        repository=db._repository,
    )

    with pytest.raises(AuthenticationError):
        GraphDB(credentials=credentials)


def test_invalid_repository(db: GraphDB):
    """Test an invalid selected repository"""
    credentials = GraphDBCredentials(
        base_url=db._base_url,
        username=db._username,
        password=db._password,
        repository="SomeWrongRepository",
    )

    with pytest.raises(InvalidRepositoryError):
        GraphDB(credentials=credentials)
