from typing import Tuple
import pytest
from rdflib import Literal, XSD
from graph_db_interface import GraphDB
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils import utils

NAMED_GRAPH = "http://example.org/named_graph"

SUBJECT_1 = "<http://example.org/subject_1>"
PREDICATE_1 = "<http://example.org/predicate_1>"
OBJECT_1 = Literal(0.5, datatype=XSD.double)  # data value

SUBJECT_2 = "<http://example.org/subject_2>"
PREDICATE_2 = "<http://example.org/predicate_2>"
OBJECT_2 = Literal(42, datatype=XSD.integer)  # data value


NEW_SUBJECT_1 = "<http://example.org/new_subject>"
NEW_PREDICATE_1 = "<http://example.org/new_predicate>"
NEW_OBJECT_1 = Literal('string with "quotes"', datatype=XSD.string)

NEW_SUBJECT_2 = "<http://example.org/new_subject_2>"
NEW_PREDICATE_2 = "<http://example.org/new_predicate_2>"
NEW_OBJECT_2 = Literal(True, datatype=XSD.boolean)

from typing import Any

from typing import List, Union

LIST_OF_TRIPLES: List[Tuple[str, str, Union[str, Literal]]] = [
    (SUBJECT_1, PREDICATE_1, OBJECT_1),
    (SUBJECT_2, PREDICATE_2, OBJECT_2),
]
LIST_OF_NEW_TRIPLES: List[Tuple[str, str, Union[str, Literal]]] = [
    (NEW_SUBJECT_1, NEW_PREDICATE_1, NEW_OBJECT_1),
    (NEW_SUBJECT_2, NEW_PREDICATE_2, NEW_OBJECT_2),
]


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    named_graph = request.param
    # We once set a named graph and once we don't
    db.named_graph = named_graph


def test_add_and_delete_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # try to delete the triple again
    result = db.triple_delete(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triple_delete(SUBJECT_1, PREDICATE_1, OBJECT_1, check_exist=False)
    assert result is True


def test_add_and_delete_multiple_triples(db: GraphDB):
    # add multiple triples
    result = db.triples_add(LIST_OF_TRIPLES)
    assert result is True

    # try to delete the triples
    result = db.triples_delete(LIST_OF_TRIPLES)
    assert result is True

    # try to delete the triples again
    result = db.triples_delete(LIST_OF_TRIPLES)
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triples_delete(LIST_OF_TRIPLES, check_exist=False)
    assert result is True


def test_update_triple(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # Input errors
    with pytest.raises(InvalidInputError):
        db.triple_update(sub_old=SUBJECT_1, pred_old=PREDICATE_1, obj_old=None)

    # Nothing to update
    with pytest.raises(InvalidInputError):
        db.triple_update(sub_old=SUBJECT_1, pred_old=PREDICATE_1, obj_old=OBJECT_1)

    # try to update the full triple and change its object
    result = db.triple_update(
        sub_old=SUBJECT_1,
        pred_old=PREDICATE_1,
        obj_old=OBJECT_1,
        sub_new=NEW_SUBJECT_1,
        pred_new=NEW_PREDICATE_1,
        obj_new=NEW_OBJECT_1,
    )
    assert result is True

    result = db.triple_delete(NEW_SUBJECT_1, NEW_PREDICATE_1, NEW_OBJECT_1)
    assert result is True


def test_update_triple_only_subject(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # only update the subject of the triple
    result = db.triple_update(
        sub_old=SUBJECT_1,
        pred_old=PREDICATE_1,
        obj_old=OBJECT_1,
        sub_new=NEW_SUBJECT_1,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(NEW_SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True


def test_update_triple_only_predicate(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # only update the predicate of the triple
    result = db.triple_update(
        sub_old=SUBJECT_1,
        pred_old=PREDICATE_1,
        obj_old=OBJECT_1,
        pred_new=NEW_PREDICATE_1,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT_1, NEW_PREDICATE_1, OBJECT_1)
    assert result is True


def test_update_triple_only_object(db: GraphDB):
    # Add a new triple
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # only update the object of the triple
    result = db.triple_update(
        sub_old=SUBJECT_1,
        pred_old=PREDICATE_1,
        obj_old=OBJECT_1,
        obj_new=NEW_OBJECT_1,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(SUBJECT_1, PREDICATE_1, NEW_OBJECT_1)
    assert result is True


def test_update_multiple_triples(db: GraphDB):
    # add multiple triples
    result = db.triples_add(LIST_OF_TRIPLES)
    assert result is True

    # update multiple triples
    result = db.triples_update(
        old_triples=LIST_OF_TRIPLES,
        new_triples=LIST_OF_NEW_TRIPLES,
    )
    assert result is True

    # try to delete the new triples
    result = db.triples_delete(LIST_OF_NEW_TRIPLES)
    assert result is True


def test_iri_exists(db: GraphDB):
    # add a new triple to the default graph
    result = db.triple_add(SUBJECT_1, PREDICATE_1, OBJECT_1)
    assert result is True

    # does not specify any part of a triple to look for
    with pytest.raises(InvalidInputError):
        db.iri_exists(iri=SUBJECT_1)

    # IRI should exist like this
    result = db.iri_exists(
        iri=SUBJECT_1,
        as_sub=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is True

    result = db.iri_exists(SUBJECT_1, as_sub=True, as_pred=True)
    assert result is False

    result = db.iri_exists(PREDICATE_1, as_pred=True)
    assert result is True

    result = db.iri_exists(
        SUBJECT_1,
        as_obj=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.iri_exists(
        SUBJECT_1,
        as_pred=True,
        include_explicit=True,
        include_implicit=False,
    )
    assert result is False

    result = db.triple_delete(SUBJECT_1, PREDICATE_1, OBJECT_1)
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


def test_prefix_management(db: GraphDB):
    """Test add_prefix, remove_prefix, and get_prefixes functionality"""

    # Get initial prefixes (should include default prefixes: owl, rdf, rdfs, onto)
    initial_prefixes = db.get_prefixes()
    assert isinstance(initial_prefixes, dict)
    assert "owl" in initial_prefixes
    assert "rdf" in initial_prefixes
    assert "rdfs" in initial_prefixes
    assert "onto" in initial_prefixes
    initial_count = len(initial_prefixes)

    # Test add_prefix with full IRI
    db.add_prefix("ex", "<http://example.org/>")
    prefixes = db.get_prefixes()
    assert "ex" in prefixes
    assert prefixes["ex"] == "<http://example.org/>"
    assert len(prefixes) == initial_count + 1

    # Test add_prefix with IRI without angle brackets (should add them)
    db.add_prefix("test", "http://test.org/")
    prefixes = db.get_prefixes()
    assert "test" in prefixes
    assert prefixes["test"] == "<http://test.org/>"
    assert len(prefixes) == initial_count + 2

    # Test overwriting an existing prefix
    db.add_prefix("ex", "<http://example.com/>")
    prefixes = db.get_prefixes()
    assert prefixes["ex"] == "<http://example.com/>"
    assert len(prefixes) == initial_count + 2  # Count shouldn't increase

    # Test remove_prefix for existing prefix
    result = db.remove_prefix("ex")
    assert result is True
    prefixes = db.get_prefixes()
    assert "ex" not in prefixes
    assert len(prefixes) == initial_count + 1

    # Test remove_prefix for non-existing prefix
    result = db.remove_prefix("nonexistent")
    assert result is False

    # Test remove_prefix for another existing prefix
    result = db.remove_prefix("test")
    assert result is True
    prefixes = db.get_prefixes()
    assert "test" not in prefixes
    assert len(prefixes) == initial_count

    # Verify default prefixes are still intact
    assert "owl" in prefixes
    assert "rdf" in prefixes
    assert "rdfs" in prefixes
    assert "onto" in prefixes
