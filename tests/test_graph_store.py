from graph_db_interface import GraphDB

TEST_TTL_DATA = """@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Alice a foaf:Person ;
        foaf:name "Alice" ;
        foaf:knows ex:Bob .

ex:Bob a foaf:Person ;
    foaf:name "Bob" .
"""

UPDATED_TTL_DATA = """@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Alice a foaf:Person ;
        foaf:name "Alice" ;
        foaf:knows ex:Bob .

ex:Bob2 a foaf:Person ;
    foaf:name "Bob2" .

ex:Peter a foaf:Person ;
    foaf:name "Peter" .
"""

GRAPH_IRI = "http://test_named_graph"


def test_named_graph(db: GraphDB):

    # first fetch the test named graph to ensure it is empty
    graph = db.fetch_statements(
        graph_iri=GRAPH_IRI,
    )
    assert graph is not None
    assert len(graph) == 0

    # now we add some data to it
    success = db.import_statements(
        content=TEST_TTL_DATA,
        overwrite=False,
        graph_iri=GRAPH_IRI,
        content_type="application/x-turtle",
    )
    assert success

    # fetch the named graph again to ensure data was added
    graph = db.fetch_statements(
        graph_iri=GRAPH_IRI,
    )
    assert graph is not None
    assert len(graph) == 5

    # now we import new data with overwrite=True
    success = db.import_statements(
        content=UPDATED_TTL_DATA,
        overwrite=True,
        graph_iri=GRAPH_IRI,
        content_type="application/x-turtle",
    )
    assert success

    # fetch the named graph again to ensure data was updated
    graph = db.fetch_statements(
        graph_iri=GRAPH_IRI,
    )
    assert graph is not None
    assert len(graph) == 7

    # now we add data again with overwrite=False
    success = db.import_statements(
        content=TEST_TTL_DATA,
        overwrite=False,
        graph_iri=GRAPH_IRI,
        content_type="application/x-turtle",
    )
    assert success

    # fetch the named graph again to ensure data was appended
    graph = db.fetch_statements(
        graph_iri=GRAPH_IRI,
    )
    assert graph is not None
    assert len(graph) == 9

    # clear the named graph
    success = db.clear_graph(
        graph_iri=GRAPH_IRI,
    )
    assert success

    # fetch the named graph again to ensure it is empty
    graph = db.fetch_statements(
        graph_iri=GRAPH_IRI,
    )
    assert graph is not None
    assert len(graph) == 0


def test_default_graph(db: GraphDB):
    # first fetch the default graph
    graph = db.fetch_statements(
        graph_iri=None,
    )
    assert graph is not None
    triples_in_default_graph = int(len(graph))
    print(f"Default graph has {triples_in_default_graph} triples.")

    # now we add some data to it
    success = db.import_statements(
        content=TEST_TTL_DATA,
        overwrite=False,
        graph_iri=None,
        content_type="application/x-turtle",
    )
    assert success

    expected_num_triples = triples_in_default_graph + 5
    # fetch the default graph again to ensure data was added
    graph = db.fetch_statements(
        graph_iri=None,
    )
    assert graph is not None
    assert int(len(graph)) == int(expected_num_triples)

    # now we import new data with overwrite=True
    success = db.import_statements(
        content=UPDATED_TTL_DATA,
        overwrite=True,
        graph_iri=None,
        content_type="application/x-turtle",
    )
    assert success

    # fetch the default graph again to ensure data was updated
    graph = db.fetch_statements(
        graph_iri=None,
    )
    assert graph is not None
    assert len(graph) == 7

    # now we add data again with overwrite=False
    success = db.import_statements(
        content=TEST_TTL_DATA,
        overwrite=False,
        graph_iri=None,
        content_type="application/x-turtle",
    )
    assert success

    # fetch the default graph again to ensure data was appended
    graph = db.fetch_statements(
        graph_iri=None,
    )
    assert graph is not None
    assert len(graph) == 9

    # clear the default graph
    success = db.clear_graph(
        graph_iri=None,
    )
    assert success

    # fetch the default graph again to ensure it is empty
    graph = db.fetch_statements(
        graph_iri=None,
    )
    assert graph is not None
    assert len(graph) == 0
