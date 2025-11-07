from graph_db_interface import GraphDB
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError
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
    with pytest.raises(InvalidInputError):
        db.triples_get()


def test_triple_set_subjects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(sub=SUBJECT1, include_implicit=False)
    result_triples_wrong = db.triples_get(sub=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]
    assert result_triples_wrong == []

    # enclosed absolute IRI
    result_triples = db.triples_get(
        sub=utils.ensure_absolute(SUBJECT1), include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # shorthand IRI
    db.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        sub=f"ex:{utils.get_local_name(SUBJECT1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(sub="example.org/subject", include_implicit=False)
    assert sorted(result_triples) == sorted(
        [(SUBJECT1, PREDICATE1, OBJECT1.toPython()), (SUBJECT2, PREDICATE2, OBJECT2)]
    )


def test_triple_set_predicates(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(pred=PREDICATE1, include_implicit=False)
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # enclosed absolute IRI
    result_triples = db.triples_get(
        pred=utils.ensure_absolute(PREDICATE1), include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # shorthand IRI
    db.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        pred=f"ex:{utils.get_local_name(PREDICATE1)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(
        pred="example.org/predicate", include_implicit=False
    )
    assert sorted(result_triples) == sorted(
        [(SUBJECT1, PREDICATE1, OBJECT1.toPython()), (SUBJECT2, PREDICATE2, OBJECT2)]
    )


def test_triple_set_objects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(obj=OBJECT2, include_implicit=False)
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # enclosed absolute IRI
    result_triples = db.triples_get(
        obj=utils.ensure_absolute(OBJECT2), include_implicit=False
    )
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # shorthand IRI
    db.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        obj=f"ex:{utils.get_local_name(OBJECT2)}", include_implicit=False
    )
    assert result_triples == [(SUBJECT2, PREDICATE2, OBJECT2)]

    # No iri but just a substring of the IRI
    result_triples = db.triples_get(obj="example.org/object", include_implicit=False)
    assert sorted(result_triples) == sorted([(SUBJECT2, PREDICATE2, OBJECT2)])

    if db.named_graph is not None:

        # Object as a rdflib Literal
        result_triples = db.triples_get(obj=OBJECT1, include_implicit=False)
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]

        # Object as a Python basic type
        result_triples = db.triples_get(obj=OBJECT1.toPython(), include_implicit=False)
        assert result_triples == [(SUBJECT1, PREDICATE1, OBJECT1.toPython())]
    pass
