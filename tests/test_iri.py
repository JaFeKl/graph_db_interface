from graph_db_interface import IRI
from graph_db_interface.exceptions import InvalidIRIError
from rdflib import URIRef, Literal

import pytest


def test_init_formats_onto():
    # Pure onto, without wrappers
    ref_iri = IRI("http://www.w3.org/2002/07/owl")
    assert ref_iri == "http://www.w3.org/2002/07/owl"

    # Pure onto, with #
    iri = IRI("http://www.w3.org/2002/07/owl#")
    assert iri == ref_iri

    # Pure onto, with <>
    iri = IRI("<http://www.w3.org/2002/07/owl>")
    assert iri == ref_iri

    # Pure onto, with <> and #
    iri = IRI("<http://www.w3.org/2002/07/owl#>")
    assert iri == ref_iri

    # Explititly empty name
    iri = IRI("", "http://www.w3.org/2002/07/owl")
    assert iri == ref_iri

    # Pure onto, duplication
    iri = IRI(None, "http://www.w3.org/2002/07/owl")
    assert iri == ref_iri

    # Pure onto, using prefix
    iri = IRI("owl:")
    assert iri == ref_iri

    # Pure onto, malformed IRI
    with pytest.raises(InvalidIRIError):
        IRI("www.w3.org/2002/07/owl")

    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07:owl#")

    # Pure onto, using unknown prefix
    with pytest.raises(InvalidIRIError):
        IRI("unknown_prefix:")


def test_init_formats_full():
    # Full IRI, without wrappers
    ref_iri = IRI("http://www.w3.org/2002/07/owl#Class")
    assert ref_iri == "http://www.w3.org/2002/07/owl#Class"

    # Full IRI, with <>
    iri = IRI("<http://www.w3.org/2002/07/owl#Class>")
    assert iri == ref_iri

    # Full IRI, using prefix
    iri = IRI("owl:Class")
    assert iri == ref_iri

    # Full IRI, duplication
    iri = IRI("http://www.w3.org/2002/07/owl#Class")
    iri = IRI(iri)
    assert iri == ref_iri

    # Single value, using wrong type
    with pytest.raises(TypeError):
        IRI(123)

    with pytest.raises(TypeError):
        IRI(Literal("http://www.w3.org/2002/07/owl#Class"))

    # Full onto IRI, using ':' instead of '#'
    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07/owl:Class")

    # Unknown prefix
    with pytest.raises(InvalidIRIError):
        IRI("unknown_prefix:Class")

    # Two prefixes
    with pytest.raises(InvalidIRIError):
        IRI("owl:owl:Class")

    with pytest.raises(InvalidIRIError):
        IRI("owl:http://www.w3.org/2002/07/owl#Class")

    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07/owl#owl:Class")

    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07/owl#http://www.w3.org/2002/07/owl#Class")

    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07/owl#www.w3.org/2002/07/owl#Class")

    # mixed : and #
    with pytest.raises(InvalidIRIError):
        IRI("owl:owl#Class")

    with pytest.raises(InvalidIRIError):
        IRI("owl#owl:Class")

    # Prefix, using '#' instead of ':'
    with pytest.raises(InvalidIRIError):
        IRI("owl#Class")


def test_init_formats_two_arguments():
    # Full IRI, without wrappers
    ref_iri = IRI("Class", "http://www.w3.org/2002/07/owl")
    assert ref_iri == "http://www.w3.org/2002/07/owl#Class"

    # Full IRI, with <>
    iri = IRI("Class", "<http://www.w3.org/2002/07/owl>")
    assert iri == ref_iri

    # Full IRI, with #
    iri = IRI("Class", "http://www.w3.org/2002/07/owl#")
    assert iri == ref_iri

    # Full IRI, with # and <>
    iri = IRI("Class", "<http://www.w3.org/2002/07/owl#>")
    assert iri == ref_iri

    # Full IRI, using prefix
    iri = IRI("Class", "owl")
    assert iri == ref_iri

    # Full IRI, using prefix with :
    iri = IRI("Class", "owl:")
    assert iri == ref_iri

    iri = IRI("Class", "owl")
    iri2 = IRI(iri)
    assert iri2 == ref_iri

    # Full iri, using partial iri
    iri1 = IRI("http://www.w3.org/2002/07/owl")
    iri2 = IRI("Class", iri1)
    assert iri2 == ref_iri

    # Transparent cases
    iri1 = IRI(ref_iri, None)
    assert iri1 == ref_iri

    iri1 = IRI(None, ref_iri)
    assert iri1 == ref_iri

    # Two values, wrong type
    with pytest.raises(TypeError):
        IRI("Class", 123)

    with pytest.raises(TypeError):
        IRI(123, "owl:")

    with pytest.raises(TypeError):
        IRI("Class", Literal("http://www.w3.org/2002/07/owl"))

    with pytest.raises(TypeError):
        IRI(Literal("Class"), "owl:")

    # Empty IRI
    with pytest.raises(InvalidIRIError):
        IRI("", "")

    with pytest.raises(InvalidIRIError):
        IRI(None, None)

    # Unknown prefix
    with pytest.raises(InvalidIRIError):
        IRI("Class", "abc:")

    # Two prefixes
    with pytest.raises(InvalidIRIError):
        IRI("Class", "owl:owl:")

    with pytest.raises(InvalidIRIError):
        IRI("Class", "http://www.w3.org/2002/07/owl#http://www.w3.org/2002/07/owl#")

    with pytest.raises(InvalidIRIError):
        IRI("owl:Class", "owl:")

    with pytest.raises(InvalidIRIError):
        IRI("owl:Class", "http://www.w3.org/2002/07/owl#")

    with pytest.raises(InvalidIRIError):
        IRI("http://www.w3.org/2002/07/owl#Class", "owl:")

    with pytest.raises(InvalidIRIError):
        iri = IRI("Class", "owl")
        IRI(iri, "owl")


def test_prefix_management():
    """Test add_prefix, remove_prefix, and get_prefixes functionality"""

    # Get initial prefixes (should include default prefixes: owl, rdf, rdfs, onto)
    initial_prefixes = IRI.get_prefixes()
    assert isinstance(initial_prefixes, dict)
    assert all(
        prefix in initial_prefixes
        for prefix in [
            "owl",
            "rdf",
            "rdfs",
            "kafka",
            "kafka-inst",
        ]
    )
    initial_count = len(initial_prefixes)

    # Test add_prefix
    IRI.add_prefix("ex", "http://example.org#")
    prefixes = IRI.get_prefixes()
    assert "ex" in prefixes
    assert prefixes["ex"] == "http://example.org"
    assert len(prefixes) == initial_count + 1

    # Test add_prefix with IRI with angle brackets (should remove them)
    IRI.add_prefix("test", "<http://test.org#>")
    prefixes = IRI.get_prefixes()
    assert "test" in prefixes
    assert prefixes["test"] == "http://test.org"
    assert len(prefixes) == initial_count + 2

    # Test overwriting an existing prefix
    IRI.add_prefix("ex", "http://example.com")
    prefixes = IRI.get_prefixes()
    assert prefixes["ex"] == "http://example.com"
    assert len(prefixes) == initial_count + 2  # Count shouldn't increase

    # Test remove_prefix for existing prefix
    result = IRI.remove_prefix("ex")
    assert result is True
    prefixes = IRI.get_prefixes()
    assert "ex" not in prefixes
    assert len(prefixes) == initial_count + 1

    # Test remove_prefix for non-existing prefix
    result = IRI.remove_prefix("nonexistent")
    assert result is False

    # Test remove_prefix for another existing prefix
    result = IRI.remove_prefix("test")
    assert result is True
    prefixes = IRI.get_prefixes()
    assert "test" not in prefixes
    assert len(prefixes) == initial_count

    # Verify default prefixes are still intact
    assert all(
        prefix in initial_prefixes
        for prefix in [
            "owl",
            "rdf",
            "rdfs",
            "kafka",
            "kafka-inst",
        ]
    )


def test_eq():
    iri_ref = IRI("http://example.org#Test")

    iri2 = IRI("http://example.org#Test")
    assert iri_ref == iri2

    iri3 = IRI("http://example.org#AnotherTest")
    assert iri_ref != iri3

    str_iri = "http://example.org#Test"
    assert iri_ref == str_iri

    str_iri_different = "http://example.org#Different"
    assert iri_ref != str_iri_different

    str_not_iri = "NotAnIRI"
    assert iri_ref != str_not_iri

    literal_iri = Literal("http://example.org#Test")
    assert iri_ref != literal_iri

    uri_ref_iri = URIRef("http://example.org#Test")
    assert iri_ref == uri_ref_iri

    uri_ref_iri_different = URIRef("http://example.org#Different")
    assert iri_ref != uri_ref_iri_different

    not_a_string_or_iri = 12345
    assert iri_ref != not_a_string_or_iri


def test_hash():
    iri1 = IRI("http://example.org#Test")
    iri2 = IRI("http://example.org#Test")
    iri3 = IRI("http://example.org#AnotherTest")

    assert hash(iri1) == hash(iri2)
    assert hash(iri1) != hash(iri3)


def test_short():
    iri1 = IRI("http://www.w3.org/2002/07/owl#Class")
    iri3 = IRI("http://unknown.org#Entity")

    assert iri1.short == "owl:Class"
    assert iri3.short == "http://unknown.org#Entity"
