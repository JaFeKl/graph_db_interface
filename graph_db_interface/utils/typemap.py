from typing import Any, Callable, Dict, Union
import decimal
import datetime
import rdflib.xsd_datetime
from rdflib.term import XSDToPython, _XSD_PFX, _RDF_XMLLITERAL
from graph_db_interface.utils.iri import IRI
import xml.dom.minidom

# Extends rdflib.term.XSDToPython
STR_MAP = lambda v: str(v)
IRI_MAP = lambda v: IRI(v)

XSDToPythonMapper: Dict[IRI, Callable[[str], Any]] = XSDToPython | {
    IRI("string", _XSD_PFX): STR_MAP,
    IRI("normalizedString", _XSD_PFX): STR_MAP,
    IRI("token", _XSD_PFX): STR_MAP,
    IRI("language", _XSD_PFX): STR_MAP,
    IRI("anyURI", _XSD_PFX): IRI_MAP,
    IRI(_RDF_XMLLITERAL): STR_MAP,
}

XSDToPythonTypes: Dict[IRI, type] = {
    IRI("time", _XSD_PFX): datetime.time,
    IRI("date", _XSD_PFX): datetime.date,
    IRI("gYear", _XSD_PFX): datetime.date,
    IRI("gYearMonth", _XSD_PFX): datetime.date,
    IRI("dateTime", _XSD_PFX): datetime.datetime,
    IRI("duration", _XSD_PFX): Union[rdflib.xsd_datetime.Duration, datetime.timedelta],
    IRI("dayTimeDuration", _XSD_PFX): datetime.timedelta,
    IRI("yearMonthDuration", _XSD_PFX): Union[
        rdflib.xsd_datetime.Duration, datetime.timedelta
    ],
    IRI("hexBinary", _XSD_PFX): bytes,
    IRI("string", _XSD_PFX): str,
    IRI("normalizedString", _XSD_PFX): str,
    IRI("token", _XSD_PFX): str,
    IRI("language", _XSD_PFX): str,
    IRI("boolean", _XSD_PFX): bool,
    IRI("decimal", _XSD_PFX): decimal.Decimal,
    IRI("integer", _XSD_PFX): int,
    IRI("nonPositiveInteger", _XSD_PFX): int,
    IRI("long", _XSD_PFX): int,
    IRI("nonNegativeInteger", _XSD_PFX): int,
    IRI("negativeInteger", _XSD_PFX): int,
    IRI("int", _XSD_PFX): int,
    IRI("unsignedLong", _XSD_PFX): int,
    IRI("positiveInteger", _XSD_PFX): int,
    IRI("short", _XSD_PFX): int,
    IRI("unsignedInt", _XSD_PFX): int,
    IRI("byte", _XSD_PFX): int,
    IRI("unsignedShort", _XSD_PFX): int,
    IRI("unsignedByte", _XSD_PFX): int,
    IRI("float", _XSD_PFX): float,
    IRI("double", _XSD_PFX): float,
    IRI("base64Binary", _XSD_PFX): bytes,
    IRI("anyURI", _XSD_PFX): IRI,
    _RDF_XMLLITERAL: xml.dom.minidom.Document,
}
