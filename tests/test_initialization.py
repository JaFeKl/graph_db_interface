import pytest
from graph_db_interface import GraphDB


def test_credentials_valid(db):
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


def test_credentials_invalid(db):
    """Test invalid credentials used"""
    with pytest.raises(ValueError):
        GraphDB(
            base_url=db._base_url,
            username=db._username,
            password="SomeWrongPassword",
            repository=db._repository,
        )


def test_invalid_repository(db):
    """Test an invalid selected repository"""
    with pytest.raises(ValueError):
        GraphDB(
            base_url=db._base_url,
            username=db._username,
            password=db._password,
            repository="SomeWrongRepository",
        )
