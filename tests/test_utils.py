import graph_db_interface.utils.utils as utils


def test_extract_where_clause():
    where_clause = "WHERE { <http://example.org/subject> ?p ?o . }"
    query = f"""
SELECT *
{where_clause}
"""
    assert (where_clause == utils.extract_where_clause(query=query))


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
    assert (expected_query == result)
