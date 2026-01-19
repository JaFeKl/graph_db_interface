from __future__ import annotations

from graph_db_interface.exceptions import InvalidIRIError

from typing import Optional, Any, Dict
from rdflib import URIRef, Literal
from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
import regex as re


class IRI(URIRef):
    """
    Lightweight IRI wrapper with prefix support and validation.

    The stored format is a full IRI string, eg. `http://example.org#Name`.
    Prefixes are resolved from the provided dictionary or class-level registry.

    Valid input formats are:
    - Full IRIs without fragments: `http://example.org`
    - Prefixes without fragments: `prefix:`
    - Full IRI with fragment: `http://example.org#Name`
    - Prefixes with fragments: `prefix:Name`

    Validation supports:
    - `str`, `rdflib.URIRef`, `IRI` or other subclass of `str` inputs.
        `rdflib.Literal` or non-string inputs raise `TypeError`.
        If desired, convert to string explicitly before passing.
    - Separate base and name: `base="http://example.org", value="Name"`.
    - Angle brackets `<...>`.
    - Trailing `#`, `\` or `:` from value and base.
    """

    PREFIXES = {
        "owl": "http://www.w3.org/2002/07/owl",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema",
        "xsd": "http://www.w3.org/2001/XMLSchema",
        "kafka": "http://www.ontotext.com/connectors/kafka",
        "kafka-inst": "http://www.ontotext.com/connectors/kafka/instance",
    }
    PREFIXES_INV = {v: k for k, v in PREFIXES.items()}

    # Valid schemes for full IRIs, end with '://'
    SCHEMES = {"http://", "https://"}

    def __new__(
        cls,
        value: str,
        base: Optional[str] = None,
        prefixes: Optional[Dict[str, str]] = None,
    ) -> IRI:
        """
        Create a new IRI instance with normalization and validation.

        Args:
            value (str): Full IRI, shorthand (`prefix:name`), or local name.
            base (Optional[str]): Optional base IRI or prefix to combine with `value`.
            prefixes (Optional[Dict[str, str]]): Additional prefix mappings to use.

        Returns:
            IRI: The normalized IRI instance.

        Raises:
            TypeError: If inputs are not strings or are of type `rdflib.Literal`.
                If desired, convert to string explicitly before passing.
            InvalidIRIError: If inputs cannot be resolved into a valid IRI.
        """
        iri = IRI._sanitize(value, base, prefixes)
        return super().__new__(cls, iri)

    @property
    def short(self) -> str:
        """
        Return a prefixed form if the base is registered.

        Returns:
            str: `prefix:name` if the base IRI is known; otherwise the full IRI.
        """
        onto, fragment = str(self).rsplit("#", 1)
        if onto in IRI.PREFIXES_INV:
            return f"{IRI.PREFIXES_INV[onto]}:{fragment}"
        return self.n3()

    @property
    def lined(self) -> str:
        """
        Convert IRI to valid Python identifier using Punycode-style markers.

        Encoding: _ → __, : → _c_, / → _s_, . → _d_, # → _h_

        Human-readable and fully reversible. Markers mnemonically represent their chars:
        _c_ = colon, _s_ = slash, _d_ = dot, _h_ = hash.

        Example: https://example.com#Property → https_c_example_d_com_h_Property
        """
        result = []
        for char in str(self):
            if char == "_":
                result.append("__")
            elif char == ":":
                result.append("_c_")
            elif char == "/":
                result.append("_s_")
            elif char == ".":
                result.append("_d_")
            elif char == "#":
                result.append("_h_")
            else:
                result.append(char)
        return "".join(result)

    @classmethod
    def from_lined(cls, lined: str) -> IRI:
        """
        Decode a lined identifier back to full IRI.

        Reverses the encoding applied by `lined` property.
        Decode order: _c_→:, _s_→/, _d_→., _h_→#, then __→_.

        Example: https_c_example_d_com_h_Property → https://example.com#Property
        """
        iri_str = (
            lined.replace("_c_", ":")
            .replace("_s_", "/")
            .replace("_d_", ".")
            .replace("_h_", "#")
            .replace("__", "_")
        )
        return cls(iri_str)

    @property
    def onto(self) -> str:
        """
        Return the ontology/base part of the IRI (before the '#').

        Returns:
            str: The base IRI.
        """
        return str(self).rsplit("#", 1)[0]

    def __hash__(self) -> int:
        """
        Hash based on the string form to ensure stable behavior in sets/dicts.

        Returns:
            int: The hash value.
        """
        return str(self).__hash__()

    def __eq__(
        self,
        other: Any,
    ) -> bool:
        """
        Compare IRIs by string value with tolerant string inputs.

        Args:
            other (Any): Another `IRI`, `URIRef`, or string to compare.

        Returns:
            bool: True if the IRIs resolve to the same string form.
        """
        # Includes IRI
        if isinstance(other, URIRef):
            return str(self) == str(other)
        # Sanitize string before comparison
        if isinstance(other, str):
            try:
                return self == IRI(other)
            except:
                return False
        return False

    @classmethod
    def add_prefix(
        cls,
        prefix: str,
        iri: str,
    ) -> None:
        """
        Register or overwrite a prefix mapping.

        Args:
            prefix (str): Prefix label (e.g., "ex").
            iri (str): Base IRI to map (with or without angle brackets or trailing '#').
        """
        iri = str(IRI(iri))
        cls.PREFIXES[prefix] = iri
        cls.PREFIXES_INV[iri] = prefix

    @classmethod
    def remove_prefix(
        cls,
        prefix: str,
    ) -> bool:
        """
        Remove a registered prefix mapping.

        Args:
            prefix (str): The prefix label to remove.

        Returns:
            bool: True if the prefix existed and was removed, False otherwise.
        """
        if prefix in cls.PREFIXES:
            del cls.PREFIXES_INV[cls.PREFIXES[prefix]]
            del cls.PREFIXES[prefix]
            return True
        return False

    @classmethod
    def get_prefixes(cls) -> Dict[str, str]:
        """
        Get a copy of the current prefix registry.

        Returns:
            Dict[str, str]: Mapping of prefix to base IRI (without trailing '#').
        """
        return cls.PREFIXES.copy()

    @classmethod
    def _sanitize(
        cls,
        value: str,
        base: Optional[str],
        prefixes: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Normalize and validate IRI inputs into a canonical string form.

        Args:
            value (str): Full IRI, shorthand (`prefix:name`), or local name.
            base (Optional[str]): Base IRI or prefix to combine with `value`.
            prefixes (Optional[Dict[str, str]]): Additional prefix mappings to use.

        Returns:
            str: The normalized IRI string.

        Raises:
            TypeError: If inputs are not strings or are of type `rdflib.Literal`.
                If desired, convert to string explicitly before passing.
            InvalidIRIError: If inputs cannot be resolved into a valid IRI.
        """
        if value is None and base is None:
            raise InvalidIRIError("Invalid IRI: both value and base cannot be None")

        # Shortcut known good IRIs
        if isinstance(value, IRI) and base is None:
            return str(value)

        if isinstance(base, IRI) and value is None:
            return str(base)

        # Convert value to str
        if value is not None:
            if isinstance(value, Literal):
                raise TypeError(
                    f"'value' is of type Literal and probably not intended as IRI. Convert to string explicitly if intended: {value}"
                )
            if not isinstance(value, str):
                raise TypeError(
                    f"Invalid IRI: value is not a string and probably not intended as IRI. Convert to string explicitly if intended: {value}"
                )
            value = str(value)

        if base is not None:
            if isinstance(base, Literal):
                raise TypeError(
                    f"'base' is of type Literal and probably not intended as IRI. Convert to string explicitly if intended: {base}"
                )
            if not isinstance(base, str):
                raise TypeError(
                    f"Invalid IRI: base is not a string and probably not intended as IRI. Convert to string explicitly if intended: {base}"
                )
            base = str(base)

        # Merge base and value
        raw_base = base.strip("<>").rstrip("#:/") if base is not None else ""
        raw_value = value.strip("<>").rstrip("#:/") if value is not None else ""
        if raw_base == "" and raw_value == "":
            raise InvalidIRIError("Invalid IRI: empty string")

        if raw_base == "":
            raw = raw_value
        elif raw_value == "":
            raw = raw_base
        elif raw_base in IRI.PREFIXES.keys():
            raw = raw_base + ":" + raw_value
        else:
            raw = raw_base + "#" + raw_value

        # Resolve prefixes and formats
        prefixes = IRI.PREFIXES if prefixes is None else {**IRI.PREFIXES, **prefixes}

        # Direct prefix, without fragment
        if raw in prefixes:
            return prefixes[raw]

        # Full IRI
        if any(raw.startswith(scheme) for scheme in IRI.SCHEMES):
            if raw.count(":") > 1:
                raise InvalidIRIError(
                    f"Invalid IRI: ':' outside of supported schemes {IRI.SCHEMES} ({value}, {base})"
                )
            if raw.count("#") > 1:
                raise InvalidIRIError(f"Invalid IRI: multiple '#' in ({value}, {base})")
            return raw

        # Mixed or malformed combinations (e.g., 'owl#Class', 'owl:owl#Class', 'owl#owl:Class')
        if ":" in raw and "#" in raw:
            raise InvalidIRIError(
                f"Invalid IRI: mixed '#' and ':' outside of supported schemes {IRI.SCHEMES} in ({value}, {base})"
            )
        if "#" in raw:
            raise InvalidIRIError(f"Invalid IRI: malformed format ({value}, {base})")

        colon_count = raw.count(":")
        if colon_count > 1:
            raise InvalidIRIError(
                f"Invalid IRI: multiple ':' separators in ({value}, {base})"
            )
        if colon_count == 1:
            # One ':': 'prefix:fragment' form
            prefix, fragment = raw.split(":")
            if prefix not in prefixes:
                raise InvalidIRIError(f"Invalid IRI: unknown prefix '{prefix}'")
            return prefixes[prefix] + "#" + fragment

        raise InvalidIRIError(
            f"Invalid IRI: malformed format or unknown prefix ({value}, {base})"
        )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        """
        Provide a permissive Pydantic core schema for IRI fields.

        Args:
            source_type (Any): The source type passed by Pydantic.
            handler (GetCoreSchemaHandler): Pydantic schema handler.

        Returns:
            CoreSchema: A schema accepting any value (validated by IRI itself).
        """
        return core_schema.any_schema()
