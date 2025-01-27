import pytest
from graph_db_interface.graph_db_interface import GraphDBinterface


def test_credentials_valid():
    """Test successfull initialization
    """
    try:
        GraphDBinterface(
            username="test",
            password="test123"
        )
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")


def test_credentials_invalid():
    """Test invalid credentials used
    """
    with pytest.raises(ValueError):
        GraphDBinterface(
            username="test",
            password="test1234")


def test_invalid_repository():
    """Test an invalid selected repository
    """
    with pytest.raises(ValueError):
        GraphDBinterface(
            username="test",
            password="test123",
            repository="Test"
        )
