from graph_db_interface.utils import utils
from graph_db_interface.exceptions import (
    InvalidIRIError,
    InvalidInputError,
    InvalidQueryError,
)
from graph_db_interface.utils.iri import IRI
import pytest
from rdflib import Literal
import datetime


def test_validate_query():
    valid_query = """
    SELECT *
    WHERE {
        ?s ?p ?o .
    }
    """
    invalid_query = """
    SELECT *
    WHERE {
        ?s ?p
    }
    """
    utils.validate_query(valid_query)
    with pytest.raises(InvalidQueryError):
        utils.validate_query(invalid_query)


def test_validate_update_query():
    valid_query = """
    DELETE DATA {
        GRAPH <http://example.org/named_graph> {
            <http://example.org#subject> <http://example.org#predicate> "object" .
        }
    }
    """
    invalid_query = """
    DELETE DATA {
        GRAPH <http://example.org/named_graph> {
            <http://example.org#subject> <http://example.org#predicate>
        }
    }
    """
    utils.validate_update_query(valid_query)
    with pytest.raises(InvalidQueryError):
        utils.validate_update_query(invalid_query)


def test_to_literal():
    literal_str = utils.to_literal(42, as_string=True)
    assert literal_str == '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'

    literal_str = utils.to_literal(True, as_string=True)
    assert literal_str == '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'

    literal_str = utils.to_literal(42.5, as_string=True)
    assert literal_str == '"42.5"^^<http://www.w3.org/2001/XMLSchema#double>'

    literal_str = utils.to_literal("Hello", as_string=True)
    assert literal_str == '"Hello"^^<http://www.w3.org/2001/XMLSchema#string>'

    literal_str = utils.to_literal(False, as_string=True)
    assert literal_str == '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'

    literal_str = utils.to_literal(-123, as_string=True)
    assert literal_str == '"-123"^^<http://www.w3.org/2001/XMLSchema#integer>'

    literal_str = utils.to_literal(0.0, as_string=True)
    assert literal_str == '"0.0"^^<http://www.w3.org/2001/XMLSchema#double>'


def test_from_xsd_literal():
    value = utils.from_xsd_literal("42", "http://www.w3.org/2001/XMLSchema#integer")
    assert value == 42

    value = utils.from_xsd_literal("true", "http://www.w3.org/2001/XMLSchema#boolean")
    assert value is True

    value = utils.from_xsd_literal("42.5", "http://www.w3.org/2001/XMLSchema#float")
    assert value == 42.5

    value = utils.from_xsd_literal("42.5", "http://www.w3.org/2001/XMLSchema#double")
    assert value == 42.5

    value = utils.from_xsd_literal("Hello", "http://www.w3.org/2001/XMLSchema#string")
    assert value == "Hello"

    value = utils.from_xsd_literal("-123", "http://www.w3.org/2001/XMLSchema#integer")
    assert value == -123

    value = utils.from_xsd_literal("false", "http://www.w3.org/2001/XMLSchema#boolean")
    assert value is False

    value = utils.from_xsd_literal("0.0", "http://www.w3.org/2001/XMLSchema#double")
    assert value == 0.0

    value = utils.from_xsd_literal(
        "2023-01-01", "http://www.w3.org/2001/XMLSchema#date"
    )
    assert value == datetime.date(2023, 1, 1)

    value = utils.from_xsd_literal(
        "2023-01-01T12:34:56", "http://www.w3.org/2001/XMLSchema#dateTime"
    )
    assert value == datetime.datetime(2023, 1, 1, 12, 34, 56)


def test_convert_query_result_to_python_type():
    result_dict = {"type": "uri", "value": "http://example.org#object2"}
    result_dict2 = {
        "datatype": "http://www.w3.org/2001/XMLSchema#double",
        "type": "literal",
        "value": "0.5",
    }
    converted_result = utils.convert_query_result_to_python_type(result_dict)
    assert converted_result == "http://example.org#object2"
    converted_result = utils.convert_query_result_to_python_type(result_dict2)
    assert converted_result == 0.5


def test_get_local_name():
    iri = "<http://example.org#subject>"
    local_name = utils.get_local_name(iri)
    assert local_name == "subject"

    iri = "http://example.org#predicate"
    local_name = utils.get_local_name(iri)
    assert local_name == "predicate"

    iri = "<http://example.org#object"
    local_name = utils.get_local_name(iri)
    assert local_name == "object"


def test_encapsulate_named_graph():
    named_graph_str = "<http://example.org/named_graph>"
    named_graph = IRI(named_graph_str)
    content = """SELECT *
    WHERE {
        ?s ?p ?o .
    }"""
    expected_result = f"""
GRAPH {named_graph_str} {{
    SELECT *
    WHERE {{
        ?s ?p ?o .
    }}
}}
"""
    result = utils.encapsulate_named_graph(named_graph, content)
    assert expected_result.strip() == result.strip()
