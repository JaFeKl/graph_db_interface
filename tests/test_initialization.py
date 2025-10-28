import pytest
from graph_db_interface import GraphDB, GraphDBCredentials
from graph_db_interface.exceptions import AuthenticationError, InvalidRepositoryError


def test_credentials_valid(db: GraphDB):
    """Test successfull initialization"""
    try:
        GraphDB(
            base_url=db._base_url,
            username=db._username,
            password=db._password,
            repository=db._repository,
        )
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
    with pytest.raises(AuthenticationError):
        GraphDB(
            base_url=db._base_url,
            username=db._username,
            password="SomeWrongPassword",
            repository=db._repository,
        )


def test_invalid_repository(db: GraphDB):
    """Test an invalid selected repository"""
    with pytest.raises(InvalidRepositoryError):
        GraphDB(
            base_url=db._base_url,
            username=db._username,
            password=db._password,
            repository="SomeWrongRepository",
        )
