from typing import Any, TypeAlias, Union, Set, List, Optional, Tuple
from rdflib import Literal, BNode

from graph_db_interface.utils.iri import IRI

GraphName: TypeAlias = IRI

Subject: TypeAlias = Union[IRI, BNode]
Predicate: TypeAlias = IRI
Object: TypeAlias = Union[IRI, BNode, Literal]

Triple: TypeAlias = Tuple[Subject, Predicate, Object]
PartialTriple: TypeAlias = Tuple[
    Optional[Subject], Optional[Predicate], Optional[Object]
]

Triples: TypeAlias = Union[Set[Triple], List[Triple], Tuple[Triple]]
PartialTriples: TypeAlias = Union[
    Set[PartialTriple], List[PartialTriple], Tuple[PartialTriple]
]


IRILike: TypeAlias = Union[str, IRI]
BNodeLike: TypeAlias = Union[str, BNode]
LiteralLike: TypeAlias = Union[Any, Literal]

GraphNameLike: TypeAlias = IRILike

SubjectLike: TypeAlias = Union[IRILike, BNodeLike]
PredicateLike: TypeAlias = IRILike
ObjectLike: TypeAlias = Union[IRILike, BNodeLike, LiteralLike]

TripleLike: TypeAlias = Tuple[SubjectLike, PredicateLike, ObjectLike]
PartialTripleLike: TypeAlias = Tuple[
    Optional[SubjectLike], Optional[PredicateLike], Optional[ObjectLike]
]

TriplesLike: TypeAlias = Union[Set[TripleLike], List[TripleLike], Tuple[TripleLike]]
PartialTriplesLike: TypeAlias = Union[
    Set[PartialTripleLike], List[PartialTripleLike], Tuple[PartialTripleLike]
]
