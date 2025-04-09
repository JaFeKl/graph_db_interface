import pytest
from rdflib import Literal, XSD
from graph_db_interface import GraphDB
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils import utils


SUBJECT = "<http://example.org/subject>"
PREDICATE = "<http://example.org/predicate>"
OBJECT = Literal(0.5, datatype=XSD.double)  # data value

NEW_SUBJECT = "<http://example.org/new_subject>"
NEW_PREDICATE = "<http://example.org/new_predicate>"
NEW_OBJECT = Literal('string with "quotes"', datatype=XSD.string)

NAMED_GRAPH = "<https://my_named_test_graph>"


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    named_graph = request.param
    # We once set a named graph and once we don't
    db.named_graph = named_graph


def test_add_and_delete_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # try to delete the triple again
    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT)
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT, check_exist=False)
    assert result is True


def test_update_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # Input errors
    with pytest.raises(InvalidInputError):
        db.triple_update(sub_old=SUBJECT, pred_old=PREDICATE, obj_old=None)

    # Nothing to update
    with pytest.raises(InvalidInputError):
        db.triple_update(sub_old=SUBJECT, pred_old=PREDICATE, obj_old=OBJECT)

    # try to update the full triple and change its object
    result = db.triple_update(
        sub_old=SUBJECT,
        pred_old=PREDICATE,
        obj_old=OBJECT,
        sub_new=NEW_SUBJECT,
        pred_new=NEW_PREDICATE,
        obj_new=NEW_OBJECT,
    )
    assert result is True

    result = db.triple_delete(NEW_SUBJECT, NEW_PREDICATE, NEW_OBJECT)
    assert result is True


def test_update_triple_only_subject(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # only update the subject of the triple
    result = db.triple_update(
        sub_old=SUBJECT,
        pred_old=PREDICATE,
        obj_old=OBJECT,
        sub_new=NEW_SUBJECT,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(NEW_SUBJECT, PREDICATE, OBJECT)
    assert result is True


def test_update_triple_only_predicate(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # only update the predicate of the triple
    result = db.triple_update(
        sub_old=SUBJECT,
        pred_old=PREDICATE,
        obj_old=OBJECT,
        pred_new=NEW_PREDICATE,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT, NEW_PREDICATE, OBJECT)
    assert result is True


def test_update_triple_only_object(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # only update the object of the triple
    result = db.triple_update(
        sub_old=SUBJECT,
        pred_old=PREDICATE,
        obj_old=OBJECT,
        obj_new=NEW_OBJECT,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT, PREDICATE, NEW_OBJECT)
    assert result is True


def test_iri_exists(db: GraphDB):
    # add a new triple to the default graph
    result = db.triple_add(SUBJECT, PREDICATE, OBJECT)
    assert result is True

    # does not specify any part of a triple to look for
    with pytest.raises(InvalidInputError):
        db.iri_exists(iri=SUBJECT)

    # IRI should exist like this
    result = db.iri_exists(
        iri=SUBJECT,
        as_sub=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is True

    result = db.iri_exists(SUBJECT, as_sub=True, as_pred=True)
    assert result is False

    result = db.iri_exists(PREDICATE, as_pred=True)
    assert result is True

    result = db.iri_exists(
        SUBJECT,
        as_obj=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.iri_exists(
        SUBJECT,
        as_pred=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.triple_delete(SUBJECT, PREDICATE, OBJECT)
    assert result is True


def test_convenience_functions(db: GraphDB):
    subject = "<http://example.org/instance>"
    predicate = "rdfs:subClassOf"
    object = "<http://example.org/myClass>"

    result = db.triple_add(subject, predicate, object)
    assert result is True

    result = db.triple_add(subject, "rdf:type", "owl:NamedIndividual")
    assert result is True

    result = db.triple_add(subject, "rdf:type", object)
    assert result is True

    result = db.triple_add(object, "rdf:type", "owl:Class")
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

    classes = db.owl_get_classes_of_individual(subject, local_name=True)
    assert classes == [utils.get_local_name(object)]

    classes = db.owl_get_classes_of_individual(object, local_name=False)
    assert classes == []

    result = db.triple_delete(subject, predicate, object)
    assert result is True

    result = db.triple_delete(subject, "rdf:type", "owl:NamedIndividual")
    assert result is True

    result = db.triple_delete(subject, "rdf:type", object)
    assert result is True

    result = db.triple_delete(object, "rdf:type", "owl:Class")
    assert result is True
