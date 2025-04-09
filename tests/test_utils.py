from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidIRIError
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

    assert utils.validate_query(valid_query) is True
    assert utils.validate_query(invalid_query) is False


def test_validate_update_query():
    valid_query = """
    DELETE DATA {
        GRAPH <http://example.org/graph> {
            <http://example.org/subject> <http://example.org/predicate> "object" .
        }
    }
    """
    invalid_query = """
    DELETE DATA {
        GRAPH <http://example.org/graph> {
            <http://example.org/subject> <http://example.org/predicate>
        }
    }
    """
    assert utils.validate_update_query(valid_query) is True
    assert utils.validate_update_query(invalid_query) is False


def test_ensure_absolute():
    iri = "http://www.sfb1574.kit.edu/core"
    absolute_iri = utils.ensure_absolute("http://www.sfb1574.kit.edu/core")
    assert f"<{iri}>" == absolute_iri


def test_is_absolute():
    absolute_iri = "<http://www.sfb1574.kit.edu/core>"
    relative_iri = "core:Resource"
    assert utils.is_absolute(absolute_iri) is True
    assert utils.is_absolute(relative_iri) is False


def test_strip_angle_brackets():
    iri = "http://www.sfb1574.kit.edu/core"
    absolute_iri = f"<{iri}>"
    assert iri == utils.strip_angle_brackets(absolute_iri)
    assert iri == utils.strip_angle_brackets(iri)


# def test_to_literal():
#     literal_str = utils.to_literal(42, as_string=True)
#     assert literal_str == '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'

#     literal_str = utils.to_literal(True, as_string=True)
#     assert literal_str == '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'

#     literal_str = utils.to_literal(42.5, as_string=True)
#     assert literal_str == '"42.5"^^<http://www.w3.org/2001/XMLSchema#double>'

#     literal_str = utils.to_literal("Hello", as_string=True)
#     assert literal_str == '"Hello"^^<http://www.w3.org/2001/XMLSchema#string>'

#     literal_str = utils.to_literal(False, as_string=True)
#     assert literal_str == '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'

#     literal_str = utils.to_literal(-123, as_string=True)
#     assert literal_str == '"-123"^^<http://www.w3.org/2001/XMLSchema#integer>'

#     literal_str = utils.to_literal(0.0, as_string=True)
#     assert literal_str == '"0.0"^^<http://www.w3.org/2001/XMLSchema#double>'


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
    result_dict = {"type": "uri", "value": "http://example.org/object2"}
    result_dict2 = {
        "datatype": "http://www.w3.org/2001/XMLSchema#double",
        "type": "literal",
        "value": "0.5",
    }
    converted_result = utils.convert_query_result_to_python_type(result_dict)
    assert converted_result == "http://example.org/object2"
    converted_result = utils.convert_query_result_to_python_type(result_dict2)
    assert converted_result == 0.5


def test_get_local_name():
    iri = "<http://example.org/subject>"
    local_name = utils.get_local_name(iri)
    assert local_name == "subject"

    iri = "http://example.org/predicate"
    local_name = utils.get_local_name(iri)
    assert local_name == "predicate"

    iri = "<http://example.org/object"
    local_name = utils.get_local_name(iri)
    assert local_name == "object"


def test_escape_string_literal():
    value = utils.escape_string_literal('Hello "World"')
    assert value == 'Hello "World"'

    value = utils.escape_string_literal(Literal('Hello "World"'))
    assert value.value == '"Hello \\"World\\""'


def test_is_iri():
    assert utils.is_iri("http://example.org/subject") is True
    assert utils.is_iri("example.org/subject") is False
    assert utils.is_iri("0.5") is False
    assert utils.is_iri("<http://example.org/subject>") is True
    assert utils.is_iri("http://example.org/subject#fragment") is True
    assert utils.is_iri("http://example.org/subject?query=param") is True


def test_is_shorthand_iri():
    assert utils.is_shorthand_iri("ex:subject") is True
    assert (
        utils.is_shorthand_iri("ex:subject", prefixes={"ex": "http://example.org/"})
        is True
    )
    assert (
        utils.is_shorthand_iri("subject", prefixes={"ex": "http://example.org/"})
        is False
    )
    assert utils.is_shorthand_iri("http://example.org/subject") is False
    assert utils.is_shorthand_iri("ex:subject", prefixes={}) is True


def test_prepare_object():
    # provide absolute IRI
    assert (
        utils.prepare_object("<http://example.org/subject>")
        == "<http://example.org/subject>"
    )
    # provide IRI, should be turned into absolute IRI
    assert (
        utils.prepare_object("http://example.org/subject")
        == "<http://example.org/subject>"
    )
    # provide shorthand IRI, should be kept as is
    assert utils.prepare_object("ex:subject", ensure_iri=False) == "ex:subject"

    # 
    assert (
        utils.prepare_object(0.5, as_string=True)
        == '"0.5"^^<http://www.w3.org/2001/XMLSchema#double>'
    )
    assert utils.prepare_object(0.5, as_string=False) == Literal(
        0.5, datatype="http://www.w3.org/2001/XMLSchema#double"
    )
    with pytest.raises(InvalidIRIError):
        utils.prepare_object(0.5, ensure_iri=True)

    # assert utils.prepare_object("0.5", as_string=True) == "0.5"

    # assert (
    #     utils.prepare_object(Literal("Hello"), as_string=True)
    #     == '"\\"Hello\\""^^<http://www.w3.org/2001/XMLSchema#string>'
    # )
    # assert utils.prepare_object(Literal(42)) == Literal(42)


def test_encapsulate_named_graph():
    named_graph = "<http://example.org/graph>"
    content = """SELECT *
    WHERE {
        ?s ?p ?o .
    }"""
    expected_result = f"""
GRAPH {named_graph} {{
    SELECT *
    WHERE {{
        ?s ?p ?o .
    }}
}}
"""
    result = utils.encapsulate_named_graph(named_graph, content)
    assert expected_result.strip() == result.strip()
