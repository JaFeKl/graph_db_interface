import pytest
from rdflib import Literal, XSD
from graph_db_interface.utils import utils


SUBJECT = "<http://example.org/subject>"
PREDICATE = "<http://example.org/predicate>"
OBJECT = Literal(0.5, datatype=XSD.double)  # data value

NEW_SUBJECT = "<http://example.org/new_subject>"
NEW_PREDICATE = "<http://example.org/new_predicate>"
NEW_OBJECT = Literal('string with "quotes"', datatype=XSD.string)

NAMED_GRAPH = "<https://my_named_test_graph>"


@pytest.mark.parametrize(
    "named_graph",
    [
        None,  # Test1: All operations are performed on the default graph
        NAMED_GRAPH,  # Test2: All operations are performed on a named graph
    ],
)
def test_manipulate_graph(named_graph, db):
    # try deleting a triple when it does not exist
    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is False

    # add a new triple to the default graph
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True

    # try to update the full triple and change its object
    result = db.triple_update(
        old_subject=SUBJECT,
        old_predicate=PREDICATE,
        old_object=OBJECT,
        new_subject=NEW_SUBJECT,
        new_predicate=NEW_PREDICATE,
        new_object=NEW_OBJECT,
        named_graph=named_graph,
    )

    assert result is True

    # the old triple should not exist anymore
    result = db.triple_exists(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is False

    # but the updated triple should exist!
    result = db.triple_exists(
        NEW_SUBJECT, NEW_PREDICATE, NEW_OBJECT, named_graph=named_graph
    )
    assert result is True

    # only update the subject of the triple
    result = db.triple_update(
        old_subject=NEW_SUBJECT,
        old_predicate=NEW_PREDICATE,
        old_object=NEW_OBJECT,
        new_subject=SUBJECT,
        named_graph=named_graph,
    )
    assert result is True

    # only update the predicate of the triple
    result = db.triple_update(
        old_subject=SUBJECT,
        old_predicate=NEW_PREDICATE,
        old_object=NEW_OBJECT,
        new_predicate=PREDICATE,
        named_graph=named_graph,
    )
    assert result is True

    # only update the object of the triple
    result = db.triple_update(
        old_subject=SUBJECT,
        old_predicate=PREDICATE,
        old_object="?o",
        new_object=OBJECT,
        named_graph=named_graph,
    )
    assert result is True

    # # Now we can delete the triple
    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True


@pytest.mark.parametrize(
    "named_graph",
    [
        None,  # Test1: All operations are performed on the default graph
        NAMED_GRAPH,  # Test2: All operations are performed on a named graph
    ],
)
def test_iri_exists(named_graph, db):
    # add a new triple to the default graph
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True

    # does not specify any part of a triple to look for
    result = db.iri_exists(iri=SUBJECT, named_graph=named_graph)
    assert result is False

    # IRI should exist like this
    result = db.iri_exists(
        SUBJECT,
        as_subject=True,
        # filters=filters,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is True

    # IRI should not exist when applying a filter since there are only implicit triple that satisfy the filter
    filters = {}
    filters["p"] = {PREDICATE}
    result = db.iri_exists(
        SUBJECT,
        as_subject=True,
        filters=filters,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is False

    result = db.iri_exists(
        SUBJECT, as_subject=True, as_predicate=True, named_graph=named_graph
    )
    assert result is True

    result = db.iri_exists(PREDICATE, as_predicate=True, named_graph=named_graph)
    assert result is True

    result = db.iri_exists(
        SUBJECT,
        as_object=True,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is False

    result = db.iri_exists(
        SUBJECT,
        as_predicate=True,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is False

    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True


@pytest.mark.parametrize(
    "named_graph",
    [
        None,  # Test1: All operations are performed on the default graph
        NAMED_GRAPH,  # Test2: All operations are performed on a named graph
    ],
)
def test_triple_gets(named_graph, db):
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True

    list_of_subjects = db.triple_get_subjects(PREDICATE, OBJECT)
    assert list_of_subjects == [utils.strip_angle_brackets(SUBJECT)]

    list_of_predicates = db.triple_get_predicates(SUBJECT, OBJECT)
    assert list_of_predicates == [utils.strip_angle_brackets(PREDICATE)]

    list_of_objects = db.triple_get_objects(SUBJECT, PREDICATE)
    assert list_of_objects == [OBJECT.toPython()]

    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT, named_graph=named_graph)
    assert result is True


@pytest.mark.parametrize(
    "named_graph",
    [
        None,  # Test1: All operations are performed on the default graph
        NAMED_GRAPH,  # Test2: All operations are performed on a named graph
    ],
)
def test_convenience_functions(named_graph, db):
    subject = "<http://example.org/instance>"
    predicate = "rdfs:subClassOf"
    object = "<http://example.org/myClass>"

    result = db.triple_add(subject, predicate, object, named_graph=named_graph)
    assert result is True

    result = db.triple_add(
        subject, "rdf:type", "owl:NamedIndividual", named_graph=named_graph
    )
    assert result is True

    result = db.triple_add(subject, "rdf:type", object, named_graph=named_graph)
    assert result is True

    result = db.triple_add(object, "rdf:type", "owl:Class", named_graph=named_graph)
    assert result is True

    result = db.is_subclass(subject, object)
    assert result is True

    result = db.is_subclass(subject, "<http://example.org/someNonExistingClass>")
    assert result is False

    result = db.owl_is_named_individual(subject)
    assert result is True

    result = db.owl_is_named_individual(predicate)
    assert result is False

    classes = db.owl_get_classes_of_individual(subject, local_name=False)
    assert classes == [utils.strip_angle_brackets(object)]

    result = db.triple_delete(subject, predicate, object, named_graph=named_graph)
    assert result is True

    result = db.triple_delete(
        subject, "rdf:type", "owl:NamedIndividual", named_graph=named_graph
    )
    assert result is True

    result = db.triple_delete(subject, "rdf:type", object, named_graph=named_graph)
    assert result is True

    result = db.triple_delete(object, "rdf:type", "owl:Class", named_graph=named_graph)
    assert result is True
