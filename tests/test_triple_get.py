from graph_db_interface import GraphDB
from graph_db_interface.utils import utils
from rdflib import Literal, XSD
import pytest

SUBJECT1 = "http://example.org/subject1"
PREDICATE1 = "http://example.org/predicate1"
OBJECT1 = Literal(0.5, datatype=XSD.double)  # data value

SUBJECT2 = "http://example.org/subject2"
PREDICATE2 = "http://example.org/predicate2"
OBJECT2 = "http://example.org/object2"

NAMED_GRAPH = "<https://my_named_test_graph>"


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    """Fixture to set up the test environment. Is called twice"""
    named_graph = request.param

    # We once set a named graph and once we don't
    db.named_graph = named_graph

    db.triple_add(SUBJECT1, PREDICATE1, OBJECT1)
    db.triple_add(SUBJECT2, PREDICATE2, OBJECT2)
    yield
    db.triple_delete(SUBJECT1, PREDICATE1, OBJECT1, check_exist=True)
    db.triple_delete(SUBJECT2, PREDICATE2, OBJECT2, check_exist=True)


def test_wrong_input(db: GraphDB):
    result_triples = db.triples_get()
    assert result_triples == []


def test_triple_set_subjects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(subject=SUBJECT1, include_implicit=False)
    result_triples_wrong = db.triples_get(subject=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]
    assert result_triples_wrong == []

    # enclosed absolute IRI
    result_triples = db.triples_get(
        subject=utils.ensure_absolute(SUBJECT1), include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # shorthand IRI
    db._add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        subject=f"ex:{utils.get_local_name(SUBJECT1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(
        subject="example.org/subject", include_implicit=False
    )
    assert sorted(result_triples) == sorted(
        [(SUBJECT1, PREDICATE1, OBJECT1.toPython()), (SUBJECT2, PREDICATE2, OBJECT2)]
    )


def test_triple_set_predicates(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(predicate=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # enclosed absolute IRI
    result_triples = db.triples_get(
        predicate=utils.ensure_absolute(PREDICATE1), include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # shorthand IRI
    db._add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        predicate=f"ex:{utils.get_local_name(PREDICATE1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(
        predicate="example.org/predicate", include_implicit=False
    )
    assert sorted(result_triples) == sorted(
        [(SUBJECT1, PREDICATE1, OBJECT1.toPython()), (SUBJECT2, PREDICATE2, OBJECT2)]
    )


def test_triple_set_objects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(object=OBJECT2, include_implicit=False)
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # enclosed absolute IRI
    result_triples = db.triples_get(
        object=utils.ensure_absolute(OBJECT2), include_implicit=False
    )
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # shorthand IRI
    db._add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        object=f"ex:{utils.get_local_name(OBJECT2)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(object="example.org/object", include_implicit=False)
    assert sorted(result_triples) == sorted([(SUBJECT2, PREDICATE2, OBJECT2)])

    if db.named_graph is not None:

        # Object as a rdflib Literal
        result_triples = db.triples_get(object=OBJECT1, include_implicit=False)
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

        # Object as a Python basic type
        result_triples = db.triples_get(
            object=OBJECT1.toPython(), include_implicit=False
        )
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]
    pass
