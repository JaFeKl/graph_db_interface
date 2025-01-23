import pytest
from graph_db_interface.src.graph_db_interface.graph_db_interface import GraphDBinterface

interface = GraphDBinterface(username="test", password="test123")

subject = "<http://example.org/subject>"
predicate = "<http://example.org/predicate>"
object = "<http://example.org/object>"

new_subject = "<http://example.org/new_subject>"
new_predicate = "<http://example.org/new_predicate>"
new_object = "<http://example.org/new_object>"

named_graph = "https://my_named_test_graph"


@pytest.mark.parametrize("named_graph", [
    None,         # Test1: All operations are performed on the default graph
    named_graph   # Test2: All operations are performed on a named graph
])
def test_manipulate_graph(named_graph):
    # try deleting a triple when it does not exist
    result = interface.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is False)

    # add a new triple to the default graph
    result = interface.triple_add(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    # try to update the a triple and change its object
    result = interface.triple_update(subject, predicate, object, subject, predicate, new_object, named_graph=named_graph)
    assert (result is True)

    # the old triple should not exist anymore
    result = interface.triple_exists(subject, predicate, object, named_graph=named_graph)
    assert (result is False)

    # but the updated triple should exist!
    result = interface.triple_exists(subject, predicate, new_object, named_graph=named_graph)
    assert (result is True)

    # Now we can delete the triple
    result = interface.triple_delete(subject, predicate, new_object, named_graph=named_graph)
    assert (result is True)
