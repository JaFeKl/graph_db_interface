import pytest
from rdflib import Literal, XSD
from graph_db_interface import GraphDB
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils import utils

NAMED_GRAPH = "http://example.org/named_graph"

SUB_1 = "http://example.org#subject_1"
PRED_1 = "http://example.org#predicate_1"
OBJ_1 = 0.5

SUB_2 = "http://example.org#subject_2"
PRED_2 = "http://example.org#predicate_2"
OBJ_2 = 42

NEW_SUB_1 = "http://example.org#new_subject"
NEW_PRED_1 = "http://example.org#new_predicate"
NEW_OBJ_1 = Literal('string with "quotes"', datatype=XSD.string)

NEW_SUB_2 = "http://example.org#new_subject_2"
NEW_PRED_2 = "http://example.org#new_predicate_2"
NEW_OBJ_2 = True


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    named_graph = request.param
    # We once set a named graph and once we don't
    db.named_graph = named_graph


def test_add_and_delete_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # try to delete the triple
    result = db.triple_delete((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # try to delete the triple again
    result = db.triple_delete((SUB_1, PRED_1, OBJ_1))
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triple_delete((SUB_1, PRED_1, OBJ_1), check_exist=False)
    assert result is True


def test_add_and_delete_multiple_triples(db: GraphDB):
    # add multiple triples
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    # try to delete the triples
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    # try to delete the triples again
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        check_exist=False,
    )
    assert result is True


def test_update_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # Input errors
    # no new given
    with pytest.raises(InvalidInputError):
        db.triple_update(old_triple=(SUB_1, PRED_1, OBJ_1))

    # both new given
    with pytest.raises(InvalidInputError):
        db.triple_update(
            old_triple=(SUB_1, PRED_1, OBJ_1),
            new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            new_sub=NEW_SUB_1,
        )

    # old is incomplete
    with pytest.raises(InvalidInputError):
        db.triple_update(
            old_triple=(SUB_1, PRED_1, None),
            new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        )

    # try to update the full triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
    )
    assert result is True

    # try to update the individual entries
    result = db.triple_update(
        old_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1), new_sub=SUB_1
    )
    assert result is True

    result = db.triple_update(
        old_triple=(SUB_1, NEW_PRED_1, NEW_OBJ_1),
        new_pred=PRED_1,
    )
    assert result is True

    result = db.triple_update(old_triple=(SUB_1, PRED_1, NEW_OBJ_1), new_obj=OBJ_1)
    assert result is True

    # adressing via individual arguments
    result = db.triple_update(
        (SUB_1, PRED_1, OBJ_1),
        new_sub=NEW_SUB_1,
        new_pred=NEW_PRED_1,
        new_obj=NEW_OBJ_1,
    )
    assert result is True

    # Cleanup
    result = db.triple_delete((NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1))
    assert result is True


def test_update_triple_only_subject(db: GraphDB):
    # Add a new triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # only update the subject of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(NEW_SUB_1, None, None),
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete((NEW_SUB_1, PRED_1, OBJ_1))
    assert result is True


def test_update_triple_only_predicate(db: GraphDB):
    # Add a new triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # only update the predicate of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(None, NEW_PRED_1, None),
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete((SUB_1, NEW_PRED_1, OBJ_1))
    assert result is True


def test_update_triple_only_object(db: GraphDB):
    # Add a new triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # only update the object of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(None, None, NEW_OBJ_1),
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete((SUB_1, PRED_1, NEW_OBJ_1))
    assert result is True


def test_update_multiple_triples(db: GraphDB):
    # add multiple triples
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    # update multiple triples
    result = db.triples_update(
        old_triples=[
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        new_triples=[
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            (NEW_SUB_2, NEW_PRED_2, NEW_OBJ_2),
        ],
    )
    assert result is True

    # try to delete the new triples
    result = db.triples_delete(
        [
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            (NEW_SUB_2, NEW_PRED_2, NEW_OBJ_2),
        ]
    )
    assert result is True


def test_iri_exists(db: GraphDB):
    # add a new triple to the default graph
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # does not specify any part of a triple to look for
    with pytest.raises(InvalidInputError):
        db.iri_exists(iri=SUB_1)

    # IRI should exist like this
    result = db.iri_exists(
        iri=SUB_1,
        as_sub=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is True

    result = db.iri_exists(SUB_1, as_sub=True, as_pred=True)
    assert result is False

    result = db.iri_exists(PRED_1, as_pred=True)
    assert result is True

    result = db.iri_exists(
        SUB_1,
        as_obj=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.iri_exists(
        SUB_1,
        as_pred=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.triple_delete((SUB_1, PRED_1, OBJ_1))
    assert result is True


def test_triple_exists(db: GraphDB):
    # Test return on empty DB
    result = db.triple_exists((SUB_1, PRED_1, OBJ_1))
    assert result is False

    # Add triple
    result = db.triple_add((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # Test if triple is now found
    result = db.triple_exists((SUB_1, PRED_1, OBJ_1))
    assert result is True

    # Test if modified triple is not found
    result = db.triple_exists((SUB_2, PRED_1, OBJ_1))
    assert result is False

    result = db.triple_exists((SUB_1, PRED_2, OBJ_1))
    assert result is False

    result = db.triple_exists((SUB_1, PRED_1, OBJ_2))
    assert result is False

    # Cleanup
    result = db.triple_delete((SUB_1, PRED_1, OBJ_1))
    assert result is True


def test_multi_triple_exists(db: GraphDB):
    # Test return on empty DB
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is False

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is False

    # Add first and unrelated triple
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        ]
    )
    assert result is True

    # One triple now matches
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is False

    # Add second triple
    result = db.triple_add((SUB_2, PRED_2, OBJ_2))
    assert result is True

    # One triple now matches
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ]
    )
    assert result is True

    # Cleanup
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        ]
    )
    assert result is True


def test_convenience_functions(db: GraphDB):
    sub = "http://example.org#instance"
    pred = "rdfs:subClassOf"
    obj = "http://example.org#myClass"

    result = db.triple_add((sub, pred, obj))
    assert result is True

    result = db.triple_add((sub, "rdf:type", "owl:NamedIndividual"))
    assert result is True

    result = db.triple_add((sub, "rdf:type", obj))
    assert result is True

    result = db.triple_add((obj, "rdf:type", "owl:Class"))
    assert result is True

    result = db.is_subclass(sub, obj)
    assert result is True

    result = db.is_subclass(sub, "http://example.org#someNonExistingClass")
    assert result is False

    result = db.owl_is_named_individual(sub)
    assert result is True

    result = db.owl_is_named_individual(pred)
    assert result is False

    classes = db.owl_get_classes_of_individual(sub, local_name=False)
    assert classes == [obj]

    classes = db.owl_get_classes_of_individual(sub, local_name=True)
    assert classes == [utils.get_local_name(obj)]

    classes = db.owl_get_classes_of_individual(obj, local_name=False)
    assert classes == []

    result = db.triple_delete((sub, pred, obj))
    assert result is True

    result = db.triple_delete((sub, "rdf:type", "owl:NamedIndividual"))
    assert result is True

    result = db.triple_delete((sub, "rdf:type", obj))
    assert result is True

    result = db.triple_delete((obj, "rdf:type", "owl:Class"))
    assert result is True
