from graph_db_interface import GraphDB
from graph_db_interface.utils import utils
from graph_db_interface.exceptions import InvalidInputError
from graph_db_interface.utils.iri import IRI
from rdflib import Literal, XSD
import pytest

NAMED_GRAPH = "https://my_named_test_graph"

SUB_1 = "http://example.org#subject1"
PRED_1 = "http://example.org#predicate1"
OBJ_1 = 0.5

SUBJ_2 = "http://example.org#subject2"
PRED_2 = "http://example.org#predicate2"
OBJ_2 = "http://example.org#object2"


@pytest.fixture(params=[None, NAMED_GRAPH], scope="module", autouse=True)
def setup(request, db: GraphDB):
    """Fixture to set up the test environment. Is called twice"""
    named_graph = request.param

    # We once set a named graph and once we don't
    db.named_graph = named_graph

    db.triples_add(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUBJ_2, PRED_2, OBJ_2),
        ]
    )
    yield
    db.triples_delete(
        [
            (SUB_1, PRED_1, OBJ_1),
            (SUBJ_2, PRED_2, OBJ_2),
        ]
    )


def test_wrong_input(db: GraphDB):
    # Neither sub, pred, obj given
    with pytest.raises(InvalidInputError):
        db.triples_get()

    # Both triple and explicit iri given
    with pytest.raises(InvalidInputError):
        db.triples_get((SUB_1, PRED_1, OBJ_1), sub=SUB_1)


def test_triple_set_subjects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(sub=SUB_1, include_implicit=False)
    result_triples_wrong = db.triples_get(sub=PRED_1, include_implicit=False)
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]
    assert result_triples_wrong == []

    # enclosed absolute IRI
    result_triples = db.triples_get(sub=SUB_1, include_implicit=False)
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        sub=f"ex:{utils.get_local_name(SUB_1)}", include_implicit=False
    )
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]


def test_triple_set_predicates(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(pred=PRED_1, include_implicit=False)
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]

    # enclosed absolute IRI
    result_triples = db.triples_get(pred=f"<{PRED_1}>", include_implicit=False)
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        pred=f"ex:{utils.get_local_name(PRED_1)}", include_implicit=False
    )
    assert result_triples == [(SUB_1, PRED_1, OBJ_1)]


def test_triple_set_objects(db: GraphDB):
    # Unenclosed absolute IRI
    result_triples = db.triples_get(obj=OBJ_2, include_implicit=False)
    assert result_triples == [(SUBJ_2, PRED_2, OBJ_2)]

    # enclosed absolute IRI
    result_triples = db.triples_get(obj=f"<{OBJ_2}>", include_implicit=False)
    assert result_triples == [(SUBJ_2, PRED_2, OBJ_2)]

    # shorthand IRI
    IRI.add_prefix("ex", "http://example.org/")
    result_triples = db.triples_get(
        obj=f"ex:{utils.get_local_name(OBJ_2)}", include_implicit=False
    )
    assert result_triples == [(SUBJ_2, PRED_2, OBJ_2)]

    if db.named_graph is not None:
        # Object as a Python basic type
        result_triples = db.triples_get(obj=OBJ_1, include_implicit=False)
        assert result_triples == [(SUB_1, PRED_1, OBJ_1)]

        # Object as a rdflib Literal
        result_triples = db.triples_get(
            obj=Literal(OBJ_1, datatype=XSD.double), include_implicit=False
        )
        assert result_triples == [(SUB_1, PRED_1, OBJ_1)]
