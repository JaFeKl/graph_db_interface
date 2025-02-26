import pytest
import os
import graph_db_interface.utils.utils as utils
from rdflib import Literal, XSD
from graph_db_interface.graph_db_interface import GraphDB


REQUIRED_ENV_VARS = ["GRAPHDB_URL", "GRAPHDB_USER", "GRAPHDB_PASSWORD", "GRAPHDB_REPOSITORY"]
utils.check_env_vars(REQUIRED_ENV_VARS)

GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_USER = os.getenv("GRAPHDB_USER")
GRAPHDB_PASSWORD = os.getenv("GRAPHDB_PASSWORD")
GRAPHDB_REPOSITORY = os.getenv("GRAPHDB_REPOSITORY")

db = GraphDB(
    base_url=GRAPHDB_URL,
    username=GRAPHDB_USER,
    password=GRAPHDB_PASSWORD,
    repository=GRAPHDB_REPOSITORY)

subject = "<http://example.org/subject>"
predicate = "<http://example.org/predicate>"
object = Literal(0.5, datatype=XSD.double)  # data value

new_subject = "<http://example.org/new_subject>"
new_predicate = "<http://example.org/new_predicate>"
new_object = Literal(0.7, datatype=XSD.double)

named_graph = "<https://my_named_test_graph>"


@pytest.mark.parametrize("named_graph", [
    None,         # Test1: All operations are performed on the default graph
    named_graph   # Test2: All operations are performed on a named graph
])
def test_manipulate_graph(named_graph):
    # try deleting a triple when it does not exist
    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is False)

    # add a new triple to the default graph
    result = db.triple_add(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    # try to update the full triple and change its object
    result = db.triple_update(
        old_subject=subject,
        old_predicate=predicate,
        old_object=object,
        new_subject=new_subject,
        new_predicate=new_predicate,
        new_object=new_object,
        named_graph=named_graph)

    assert (result is True)

    # the old triple should not exist anymore
    result = db.triple_exists(subject, predicate, object, named_graph=named_graph)
    assert (result is False)

    # but the updated triple should exist!
    result = db.triple_exists(new_subject, new_predicate, new_object, named_graph=named_graph)
    assert (result is True)

    # only update the subject of the triple
    result = db.triple_update(
        old_subject=new_subject,
        old_predicate=new_predicate,
        old_object=new_object,
        new_subject=subject, named_graph=named_graph)
    assert (result is True)

    # only update the predicate of the triple
    result = db.triple_update(
        old_subject=subject,
        old_predicate=new_predicate,
        old_object=new_object,
        new_predicate=predicate,
        named_graph=named_graph)
    assert (result is True)

    # only update the object of the triple
    result = db.triple_update(
        old_subject=subject,
        old_predicate=predicate,
        old_object="?o",
        new_object=object,
        named_graph=named_graph)
    assert (result is True)

    # # Now we can delete the triple
    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is True)


@pytest.mark.parametrize("named_graph", [
    None,         # Test1: All operations are performed on the default graph
    named_graph   # Test2: All operations are performed on a named graph
])
def test_iri_exists(named_graph):
    # add a new triple to the default graph
    result = db.triple_add(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    # does not specify any part of a triple to look for
    result = db.iri_exists(iri=subject, named_graph=named_graph)
    assert (result is False)

    # IRI should exist like this
    result = db.iri_exists(
        subject,
        as_subject=True,
        # filters=filters,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph)
    assert (result is True)

    # IRI should not exist when applying a filter since there are only implicit triple that satisfy the filter
    filters = {}
    filters["p"] = {predicate}
    result = db.iri_exists(
        subject,
        as_subject=True,
        filters=filters,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph)
    assert (result is False)

    result = db.iri_exists(subject, as_subject=True, as_predicate=True, named_graph=named_graph)
    assert (result is True)

    result = db.iri_exists(predicate, as_predicate=True, named_graph=named_graph)
    assert (result is True)

    result = db.iri_exists(subject, as_object=True, include_explicit=True, include_implicit=False, named_graph=named_graph)
    assert (result is False)

    result = db.iri_exists(subject, as_predicate=True, include_explicit=True, include_implicit=False, named_graph=named_graph)
    assert (result is False)

    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is True)


@pytest.mark.parametrize("named_graph", [
    None,         # Test1: All operations are performed on the default graph
    named_graph   # Test2: All operations are performed on a named graph
])
def test_triple_gets(named_graph):
    result = db.triple_add(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    list_of_subjects = db.triple_get_subjects(predicate, object)
    assert (list_of_subjects == [utils.strip_angle_brackets(subject)])

    list_of_predicates = db.triple_get_predicates(subject, object)
    assert (list_of_predicates == [utils.strip_angle_brackets(predicate)])

    list_of_objects = db.triple_get_objects(subject, predicate)
    assert (list_of_objects == [object.toPython()])

    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is True)


@pytest.mark.parametrize("named_graph", [
    None,         # Test1: All operations are performed on the default graph
    named_graph   # Test2: All operations are performed on a named graph
])
def test_convenience_functions(named_graph):
    subject = "<http://example.org/instance>"
    predicate = "rdfs:subClassOf"
    object = "<http://example.org/myClass>"

    result = db.triple_add(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    result = db.triple_add(subject, "rdf:type", "owl:NamedIndividual", named_graph=named_graph)
    assert (result is True)

    result = db.triple_add(subject, "rdf:type", object, named_graph=named_graph)
    assert (result is True)

    result = db.triple_add(object, "rdf:type", "owl:Class", named_graph=named_graph)
    assert (result is True)

    result = db.is_subclass(subject, object)
    assert (result is True)

    result = db.is_subclass(subject, "<http://example.org/someNonExistingClass>")
    assert (result is False)

    result = db.owl_is_named_individual(subject)
    assert (result is True)

    result = db.owl_is_named_individual(predicate)
    assert (result is False)

    classes = db.owl_get_classes_of_individual(subject, local_name=False)
    assert (classes == [utils.strip_angle_brackets(object)])

    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert (result is True)

    result = db.triple_delete(subject, "rdf:type", "owl:NamedIndividual", named_graph=named_graph)
    assert (result is True)

    result = db.triple_delete(subject, "rdf:type", object, named_graph=named_graph)
    assert (result is True)

    result = db.triple_delete(object, "rdf:type", "owl:Class", named_graph=named_graph)
    assert (result is True)
