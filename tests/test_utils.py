from graph_db_interface.utils import utils


def test_extract_where_clause():
    where_clause = "WHERE { <http://example.org/subject> ?p ?o . }"
    query = f"""
SELECT *
{where_clause}
"""
    assert where_clause == utils.extract_where_clause(query=query)


def test_insert_before_where_clause():
    where_clause = "WHERE { <http://example.org/subject> ?p ?o . }"
    query = f"""
SELECT *
{where_clause}
"""
    from_clause = "FROM <http://www.ontotext.com/explicit>"
    expected_query = f"""
SELECT *
{from_clause}
{where_clause}
"""
    result = utils.insert_before_where_clause(query=query, from_statement=from_clause)
    assert expected_query == result


def test_ensure_absolute():
    iri = "http://www.sfb1574.kit.edu/core"
    absolute_iri = utils.ensure_absolute("http://www.sfb1574.kit.edu/core")
    assert f"<{iri}>" == absolute_iri


def test_strip_angle_brackets():
    iri = "http://www.sfb1574.kit.edu/core"
    absolute_iri = f"<{iri}>"
    assert iri == utils.strip_angle_brackets(absolute_iri)


def test_to_xsd_literal():
    literal = utils.to_xsd_literal(42)
    assert isinstance(literal.value, int)

    literal = utils.to_xsd_literal(True)
    assert isinstance(literal.value, bool)

    literal = utils.to_xsd_literal(42.5)
    assert isinstance(literal.value, float)


def test_from_xsd_literal():
    value = utils.from_xsd_literal("42", "http://www.w3.org/2001/XMLSchema#integer")
    assert value == 42

    value = utils.from_xsd_literal("true", "http://www.w3.org/2001/XMLSchema#boolean")
    assert value is True

    value = utils.from_xsd_literal("42.5", "http://www.w3.org/2001/XMLSchema#float")
    assert value == 42.5

    value = utils.from_xsd_literal("42.5", "http://www.w3.org/2001/XMLSchema#double")
    assert value == 42.5
