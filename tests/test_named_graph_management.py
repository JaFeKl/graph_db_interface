TEST_TTL_DATA = """@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Alice a foaf:Person ;
        foaf:name "Alice" ;
        foaf:knows ex:Bob .

ex:Bob a foaf:Person ;
    foaf:name "Bob2" .
"""


def test_add_and_delete_named_graph(db):
    response = db.named_graph_add(
        TEST_TTL_DATA, "https://my_test_graph", "application/x-turtle"
    )
    assert response.status_code == 204

    response = db.named_graph_delete("https://my_test_graph")
    assert response.status_code == 204
