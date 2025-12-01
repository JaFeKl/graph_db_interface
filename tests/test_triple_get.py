from graph_db_interface import GraphDB
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils.iri import IRI
from rdflib import Literal, XSD
import pytest

NAMED_GRAPH = "https://my_named_test_graph"

SUBJECT1 = "http://example.org#subject1"
PREDICATE1 = "http://example.org#predicate1"
OBJECT1 = 0.5

SUBJECT2 = "http://example.org#subject2"
PREDICATE2 = "http://example.org#predicate2"
OBJECT2 = "http://example.org#object2"


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    """Fixture to set up the test environment. Is called twice"""
    named_graph = request.param

    # We once set a named graph and once we don't
    db.named_graph = named_graph

    db.triples_add(
        [
            (SUBJECT1, PREDICATE1, OBJECT1),
            (SUBJECT2, PREDICATE2, OBJECT2),
        ]
    )
    yield
    db.triples_delete(
        [
            (SUBJECT1, PREDICATE1, OBJECT1),
            (SUBJECT2, PREDICATE2, OBJECT2),
        ]
    )


def test_wrong_input(db: GraphDB):
    # Neither sub, pred, obj given
    with pytest.raises(InvalidInputError):
        db.triples_get()

    # Both triple and explicit iri given
    with pytest.raises(InvalidInputError):
        db.triples_get((SUBJECT1, PREDICATE1, OBJECT1), sub=SUBJECT1)


def test_triple_set_subjects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(sub=SUBJECT1, include_implicit=False)
    result_triples_wrong = db.triples_get(sub=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]
    assert result_triples_wrong == []

    # enclosed absolute IRI
    result_triples = db.triples_get(sub=SUBJECT1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        sub=f"ex:{utils.get_local_name(SUBJECT1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]


def test_triple_set_predicates(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(pred=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]

    # enclosed absolute IRI
    result_triples = db.triples_get(pred=f"<{PREDICATE1}>", include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        pred=f"ex:{utils.get_local_name(PREDICATE1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]


def test_triple_set_objects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(obj=OBJECT2, include_implicit=False)
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # enclosed absolute IRI
    result_triples = db.triples_get(obj=f"<{OBJECT2}>", include_implicit=False)
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        obj=f"ex:{utils.get_local_name(OBJECT2)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    if db.named_graph is not None:
        # Object as a Python basic type
        result_triples = db.triples_get(obj=OBJECT1, include_implicit=False)
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]

        # Object as a rdflib Literal
        result_triples = db.triples_get(
            obj=Literal(OBJECT1, datatype=XSD.double), include_implicit=False
        )
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1)]
