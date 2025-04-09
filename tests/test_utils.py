from graph_db_interface.utils import utils
from graph_db_interface.exceptions import (
    InvalidIRIError,
    InvalidInputError,
    InvalidQueryError,
)
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
    utils.validate_update_query(valid_query)
    with pytest.raises(InvalidQueryError):
        utils.validate_update_query(invalid_query)


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


def test_prepare_subject():
    # provide absolute IRI
    assert (
        utils.prepare_subject("<http://example.org/subject>", ensure_iri=True)
        == "<http://example.org/subject>"
    )
    assert (
        utils.prepare_subject("<http://example.org/subject>", ensure_iri=False)
        == "<http://example.org/subject>"
    )

    # provide IRI, should be turned into absolute IRI
    assert (
        utils.prepare_subject("http://example.org/subject", ensure_iri=True)
        == "<http://example.org/subject>"
    )
    assert (
        utils.prepare_subject("http://example.org/subject", ensure_iri=False)
        == "<http://example.org/subject>"
    )

    # provide shorthand IRI, should be kept as is
    assert utils.prepare_subject("ex:subject", ensure_iri=True) == "ex:subject"
    assert utils.prepare_subject("ex:subject", ensure_iri=False) == "ex:subject"

    # simple strings should be returned as is
    assert utils.prepare_subject("Hello", ensure_iri=False) == "Hello"
    with pytest.raises(InvalidIRIError):
        utils.prepare_subject("Hello", ensure_iri=True)

    # Literals should not be provided as subjects
    with pytest.raises(InvalidInputError):
        utils.prepare_subject(Literal("Hello"), ensure_iri=True)
    with pytest.raises(InvalidInputError):
        utils.prepare_subject(Literal(42.5), ensure_iri=True)
    with pytest.raises(InvalidInputError):
        utils.prepare_subject(Literal(42.5), ensure_iri=False)


def test_prepare_predicate():
    # provide absolute IRI
    assert (
        utils.prepare_predicate("<http://example.org/predicate>", ensure_iri=True)
        == "<http://example.org/predicate>"
    )
    assert (
        utils.prepare_predicate("<http://example.org/predicate>", ensure_iri=False)
        == "<http://example.org/predicate>"
    )

    # provide IRI, should be turned into absolute IRI
    assert (
        utils.prepare_predicate("http://example.org/predicate", ensure_iri=True)
        == "<http://example.org/predicate>"
    )
    assert (
        utils.prepare_predicate("http://example.org/predicate", ensure_iri=False)
        == "<http://example.org/predicate>"
    )

    # provide shorthand IRI, should be kept as is
    assert utils.prepare_predicate("ex:predicate", ensure_iri=True) == "ex:predicate"
    assert utils.prepare_predicate("ex:predicate", ensure_iri=False) == "ex:predicate"

    # simple strings should be returned as is
    assert utils.prepare_predicate("Hello", ensure_iri=False) == "Hello"
    with pytest.raises(InvalidIRIError):
        utils.prepare_predicate("Hello", ensure_iri=True)

    # Literals should not be provided as predicates
    with pytest.raises(InvalidInputError):
        utils.prepare_predicate(Literal("Hello"), ensure_iri=True)
    with pytest.raises(InvalidInputError):
        utils.prepare_predicate(Literal(42.5), ensure_iri=True)
    with pytest.raises(InvalidInputError):
        utils.prepare_predicate(Literal(42.5), ensure_iri=False)


def test_prepare_object():
    # provide absolute IRI
    assert (
        utils.prepare_object("<http://example.org/subject>", ensure_iri=True)
        == "<http://example.org/subject>"
    )
    assert (
        utils.prepare_object("<http://example.org/subject>", ensure_iri=False)
        == "<http://example.org/subject>"
    )

    # provide IRI, should be turned into absolute IRI
    assert (
        utils.prepare_object("http://example.org/subject", ensure_iri=True)
        == "<http://example.org/subject>"
    )
    assert (
        utils.prepare_object("http://example.org/subject", ensure_iri=False)
        == "<http://example.org/subject>"
    )

    # provide shorthand IRI, should be kept as is
    assert utils.prepare_object("ex:subject", ensure_iri=True) == "ex:subject"
    assert utils.prepare_object("ex:subject", ensure_iri=False) == "ex:subject"

    # Literals
    assert utils.prepare_object(Literal(42)) == Literal(42)
    assert (
        utils.prepare_object(Literal(42), as_string=True)
        == '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'
    )
    with pytest.raises(InvalidIRIError):
        utils.prepare_object(Literal(42), ensure_iri=True)

    # Simple strings should not be converted since they might be used for filtering
    assert utils.prepare_object("Hello", as_string=True) == "Hello"
    assert utils.prepare_object("Hello", as_string=False) == "Hello"
    with pytest.raises(InvalidIRIError):
        utils.prepare_object("Hello", ensure_iri=True)

    # Standard Python types
    assert (
        utils.prepare_object(42.5, as_string=True)
        == '"42.5"^^<http://www.w3.org/2001/XMLSchema#double>'
    )
    assert utils.prepare_object(42.5, as_string=False) == Literal(42.5)
    with pytest.raises(InvalidIRIError):
        utils.prepare_object(42.5, ensure_iri=True)


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
