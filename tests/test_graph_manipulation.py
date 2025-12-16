import random
import pytest
from rdflib import Literal, XSD
from graph_db_interface import GraphDB
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils import utils
from graph_db_interface.utils.iri import IRI

GLOBAL_NAMED_GRAPH = "http://example.org/named_graph"
LOCAL_NAMED_GRAPH = "http://example.org/local_named_graph"

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


@pytest.fixture(params=[None, LOCAL_NAMED_GRAPH], scope="module")
def named_graph(request) -> str:
    # Provide a per-test local override for the named graph
    return request.param


@pytest.fixture(params=[None, GLOBAL_NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    # Set or unset the global named graph on the DB client
    db.named_graph = request.param


def test_add_and_delete_triple(db: GraphDB, named_graph: str):
    # Add a new triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triple again
    result = db.triple_delete(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triple_delete(
        (SUB_1, PRED_1, OBJ_1),
        check_exist=False,
        named_graph=named_graph,
    )
    assert result is True


def test_add_and_delete_multiple_triples(db: GraphDB, named_graph: str):
    # add multiple triples
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triples
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triples again
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is False

    # if we dont check for existence it shoud return True
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        check_exist=False,
        named_graph=named_graph,
    )
    assert result is True


def test_update_triple(db: GraphDB, named_graph: str):
    # Add a new triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # Input errors
    # no new given
    with pytest.raises(InvalidInputError):
        db.triple_update(
            old_triple=(SUB_1, PRED_1, OBJ_1),
            named_graph=named_graph,
        )

    # both new given
    with pytest.raises(InvalidInputError):
        db.triple_update(
            old_triple=(SUB_1, PRED_1, OBJ_1),
            new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            new_sub=NEW_SUB_1,
            named_graph=named_graph,
        )

    # old is incomplete
    with pytest.raises(InvalidInputError):
        db.triple_update(
            old_triple=(SUB_1, PRED_1, None),
            new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            named_graph=named_graph,
        )

    # try to update the full triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # try to update the individual entries
    result = db.triple_update(
        old_triple=(NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        new_sub=SUB_1,
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_update(
        old_triple=(SUB_1, NEW_PRED_1, NEW_OBJ_1),
        new_pred=PRED_1,
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, NEW_OBJ_1),
        new_obj=OBJ_1,
        named_graph=named_graph,
    )
    assert result is True

    # adressing via individual arguments
    result = db.triple_update(
        (SUB_1, PRED_1, OBJ_1),
        new_sub=NEW_SUB_1,
        new_pred=NEW_PRED_1,
        new_obj=NEW_OBJ_1,
        named_graph=named_graph,
    )
    assert result is True

    # Cleanup
    result = db.triple_delete(
        (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_update_triple_only_subject(db: GraphDB, named_graph: str):
    # Add a new triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # only update the subject of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(NEW_SUB_1, None, None),
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(
        (NEW_SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_update_triple_only_predicate(db: GraphDB, named_graph: str):
    # Add a new triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # only update the predicate of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(None, NEW_PRED_1, None),
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(
        (SUB_1, NEW_PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_update_triple_only_object(db: GraphDB, named_graph: str):
    # Add a new triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # only update the object of the triple
    result = db.triple_update(
        old_triple=(SUB_1, PRED_1, OBJ_1),
        new_triple=(None, None, NEW_OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the triple
    result = db.triple_delete(
        (SUB_1, PRED_1, NEW_OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_update_multiple_triples(db: GraphDB, named_graph: str):
    # add multiple triples
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
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
        named_graph=named_graph,
    )
    assert result is True

    # try to delete the new triples
    result = db.triples_delete(
        [
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
            (NEW_SUB_2, NEW_PRED_2, NEW_OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True


def test_iri_exists(db: GraphDB, named_graph: str):
    # add a new triple to the default graph
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # does not specify any part of a triple to look for
    with pytest.raises(InvalidInputError):
        db.iri_exists(
            iri=SUB_1,
            named_graph=named_graph,
        )

    # IRI should exist like this
    result = db.iri_exists(
        iri=SUB_1,
        as_sub=True,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is True

    result = db.iri_exists(
        SUB_1,
        as_sub=True,
        as_pred=True,
        named_graph=named_graph,
    )
    assert result is False

    result = db.iri_exists(
        PRED_1,
        as_pred=True,
        named_graph=named_graph,
    )
    assert result is True

    result = db.iri_exists(
        SUB_1,
        as_obj=True,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is False

    result = db.iri_exists(
        SUB_1,
        as_pred=True,
        include_explicit=True,
        include_implicit=False,
        named_graph=named_graph,
    )
    assert result is False

    result = db.triple_delete(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_triple_exists(db: GraphDB, named_graph: str):
    # Test return on empty DB
    result = db.triple_exists(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is False

    # Add triple
    result = db.triple_add(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # Test if triple is now found
    result = db.triple_exists(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True

    # Test if modified triple is not found
    result = db.triple_exists(
        (SUB_2, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is False

    result = db.triple_exists(
        (SUB_1, PRED_2, OBJ_1),
        named_graph=named_graph,
    )
    assert result is False

    result = db.triple_exists(
        (SUB_1, PRED_1, OBJ_2),
        named_graph=named_graph,
    )
    assert result is False

    # Cleanup
    result = db.triple_delete(
        (SUB_1, PRED_1, OBJ_1),
        named_graph=named_graph,
    )
    assert result is True


def test_multi_triple_exists(db: GraphDB, named_graph: str):
    # Test return on empty DB
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is False

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is False

    # Add first and unrelated triple
    result = db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        ],
        named_graph=named_graph,
    )
    assert result is True

    # One triple now matches
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is False

    # Add second triple
    result = db.triple_add(
        (SUB_2, PRED_2, OBJ_2),
        named_graph=named_graph,
    )
    assert result is True

    # One triple now matches
    result = db.any_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True

    result = db.all_triple_exists(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
        ],
        named_graph=named_graph,
    )
    assert result is True

    # Cleanup
    result = db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUB_2, PRED_2, OBJ_2),
            (NEW_SUB_1, NEW_PRED_1, NEW_OBJ_1),
        ],
        named_graph=named_graph,
    )
    assert result is True


def test_convenience_functions(db: GraphDB, named_graph: str):
    sub = "http://example.org#instance"
    pred = "rdfs:subClassOf"
    obj = "http://example.org#myClass"

    result = db.triple_add(
        (sub, pred, obj),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_add(
        (sub, "rdf:type", "owl:NamedIndividual"),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_add(
        (sub, "rdf:type", obj),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_add(
        (obj, "rdf:type", "owl:Class"),
        named_graph=named_graph,
    )
    assert result is True

    result = db.is_subclass(
        sub,
        obj,
        named_graph=named_graph,
    )
    assert result is True

    result = db.is_subclass(
        sub,
        "http://example.org#someNonExistingClass",
        named_graph=named_graph,
    )
    assert result is False

    result = db.owl_is_named_individual(
        sub,
        named_graph=named_graph,
    )
    assert result is True

    result = db.owl_is_named_individual(
        pred,
        named_graph=named_graph,
    )
    assert result is False

    classes = db.owl_get_classes_of_individual(
        sub,
        local_name=False,
        named_graph=named_graph,
    )
    assert classes == [obj]

    classes = db.owl_get_classes_of_individual(
        sub,
        local_name=True,
        named_graph=named_graph,
    )
    assert classes == [utils.get_local_name(obj)]

    classes = db.owl_get_classes_of_individual(
        obj,
        local_name=False,
        named_graph=named_graph,
    )
    assert classes == []

    result = db.triple_delete(
        (sub, pred, obj),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_delete(
        (sub, "rdf:type", "owl:NamedIndividual"),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_delete(
        (sub, "rdf:type", obj),
        named_graph=named_graph,
    )
    assert result is True

    result = db.triple_delete(
        (obj, "rdf:type", "owl:Class"),
        named_graph=named_graph,
    )
    assert result is True


def test_iri_generation(db: GraphDB, named_graph: str):
    valid_schema = lambda: str(random.randint(0, 1000000))
    invalid_schema = lambda: "fixed"

    # Generate a new IRI
    iri1 = db.new_iri(
        base="http://example.org",
    )
    assert isinstance(iri1, IRI)
    assert iri1.onto == "http://example.org"

    # Ensure uniqueness
    iri2 = db.new_iri(
        base="http://example.org",
    )
    assert iri1 != iri2

    # Generate with fragment base
    iri3 = db.new_iri(
        base="http://example.org#ClassName_",
    )
    assert isinstance(iri3, IRI)
    assert iri3.onto == "http://example.org"
    assert iri3.fragment.startswith("ClassName_")

    # Ensure uniqueness
    iri4 = db.new_iri(
        base="http://example.org#ClassName_",
    )
    assert iri3 != iri4

    # Generate with schema
    iri5 = db.new_iri(
        base="http://example.org",
        schema=valid_schema,
    )
    assert isinstance(iri5, IRI)
    assert iri5.onto == "http://example.org"
    assert iri1 != iri5

    iri6 = db.new_iri(
        base="http://example.org",
        schema=valid_schema,
    )
    assert iri5 != iri6

    # Invalid schema that does not produce unique IRIs
    with pytest.raises(ValueError):
        db.new_iri(
            base="http://example.org",
            schema=invalid_schema,
        )

    # Generate blank node IDs
    genid1 = db.new_blank_id()
    assert isinstance(genid1, str)

    genid2 = db.new_blank_id()
    assert genid1 != genid2

    # Generate with schema
    genid3 = db.new_blank_id(
        schema=valid_schema,
    )
    assert isinstance(genid3, str)
    assert genid1 != genid3

    genid4 = db.new_blank_id(
        schema=valid_schema,
    )
    assert genid3 != genid4

    # Invalid schema that does not produce unique blank IDs
    with pytest.raises(ValueError):
        db.new_blank_id(
            schema=invalid_schema,
        )
