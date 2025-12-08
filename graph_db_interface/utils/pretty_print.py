from __future__ import annotations
from typing import Any, Optional
import json
from graph_db_interface.utils.iri import IRI
import re


def shorten_block(s: str) -> str:
    """
    Search a text for IRI patterns with known namespaces and shorten them.
    """
    # Pattern to find quoted strings
    quote_pattern = re.compile(r'"([^"]*)"')

    def replace_iri(match: re.Match) -> str:
        content = match.group(1)
        # Skip empty strings
        if not content:
            return match.group(0)
        try:
            # Try to create an IRI and get its short form
            shortened = IRI(content).short
            return f'"{shortened}"'
        except Exception:
            # If it's not a valid IRI, return the original match
            return match.group(0)

    return quote_pattern.sub(replace_iri, s)


def format_result(
    result: (
        list[tuple[str]] | tuple[tuple[str, ...], ...] | dict[Any, Any] | list[dict]
    ),
    variables: Optional[list[str]] = None,
    grouping_variables: Optional[list[str]] = None,
) -> str:
    """
    Return a human-readable string for SPARQL query results.

    This utility formats different shapes of results produced by SPARQL SELECT queries
    into an easy-to-read textual representation used in logs or CLI output.

    Args:
        result (list[tuple[str]] | tuple[tuple[str, ...], ...] | dict[Any, Any] | list[dict]):
            One of the following structures:
            - tuple of tuples: tabular rows, where each inner tuple corresponds to a row
              ordered by `variables`.
            - tuple of scalars: a single-column result (when `variables` has length 1).
            - dict: a nested mapping keyed by the values of `grouping_variables` where the leaves
              are tuples (or empty tuples) representing the non-grouped `variables`.
            - list[dict]: raw JSON bindings from a SPARQL endpoint (each dict is a binding);
              this is rendered as a compact pretty-printed JSON-like list.
        variables (Optional[list[str]]): Non-grouped variable names (without leading `?`) that
            define the order and width of columns when rendering tabular/tuple results.
        grouping_variables (Optional[list[str]]): Grouped variable names (without leading `?`) that
            define the nesting order for dictionary results. When present, headers are annotated by
            the current grouping key at each level.

    Returns:
        str: A formatted multi-line string. For empty inputs the string explicitly indicates which
        variables and/or grouping variables were expected.

    Notes:
        - This function does not mutate the input data.
        - Widths for tabular output are computed from both headers (`variables`) and the values.
    """

    if not result:
        out = "Result empty: "
        if grouping_variables:
            out += f"{{{', '.join(v for v in grouping_variables)}}}"
        if variables:
            if grouping_variables:
                out += ", "
            out += f"({', '.join(v for v in variables)})"
        return out

    if (
        isinstance(result, list) and result and isinstance(result[0], dict)
    ):  # raw JSON bindings
        return f"Result: {_format_raw_json_bindings(result)}"
    elif isinstance(result, dict):  # nested structure
        return (
            f"Result: {_format_nested_structure(result, variables, grouping_variables)}"
        )
    else:  # tuple of tuples or tuple of scalars
        return f"Result: {_format_entry(result, variables)}"


def _format_raw_json_bindings(
    result: list[dict],
) -> str:
    """
    Format raw JSON bindings (list of dicts) as pretty-printed JSON.

    Args:
        result (list[dict]): A list of dictionaries representing SPARQL query bindings.

    Returns:
        str: A formatted JSON string representation of the input list.
    """
    try:
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception:
        # Fallback simple renderer if unexpected non-serializable values appear
        rendered_items = []
        for d in result:
            entries = ",\n\t  ".join([f"'{k}': {str(v)}" for k, v in d.items()])
            rendered_items.append(entries)
        return "[{" + "},\n\t {".join(rendered_items) + "}]"


def _format_nested_structure(
    structure,
    variables: list[str],
    grouping_variables: list[str],
    level: Optional[int] = 0,
) -> str:
    """
    Format a nested dictionary result structure with indentation.

    Args:
        structure (dict): Nested dict keyed by the values of `grouping_variables` where leaves are
            tuples (or empty tuples) representing the non-grouped `variables`.
        variables (list[str]): The non-grouped variable names used to render leaf tuples.
        grouping_variables (list[str]): Grouped variable names used to annotate each nesting level.
        level (Optional[int]): Current recursion depth used only for indentation.

    Returns:
        str: A multi-line, indented string representation of the nested structure.
    """
    indent = "    " * level
    if not structure:
        return "()"
    if isinstance(structure, dict):
        if not structure:
            return "{}"
        items = []
        for key, value in structure.items():
            formatted_value = _format_nested_structure(
                value, variables, grouping_variables, level + 1
            )
            items.append(f"{indent}    {key}: {formatted_value}")
        return (
            f"{{ # {grouping_variables[level]}\n" + ",\n".join(items) + f"\n{indent}}}"
        )
    return f"\n        {_format_entry(structure, variables, indent[:-1])}"


def _format_entry(
    structure: tuple,
    variables: list[str],
    indent: Optional[str] = "",
) -> str:
    """
    Format a flat tuple or tuple-of-tuples as aligned columns.

    Args:
        structure (tuple): Either a tuple of scalars (single-column) or a tuple of tuples (multi-column).
        variables (list[str]): Column headers (without leading `?`) used for width computation and header rendering.
        indent (Optional[str]): Optional left indentation for multi-line alignment.

    Returns:
        str: A header row and aligned values. Single-column outputs render as a vertical list; multi-column outputs render as a table-like block.
    """
    if len(variables) == 1:
        lmax = len(variables[0])
        for s in structure:
            lmax = max(lmax, len(s))
        var_names = str(variables[0])
        rows = [f"{re:<{lmax}}" for re in structure]
        content = f" ,\n{indent}          ".join(rows)
        return f"{indent}# {var_names}\n{indent}        ( {content} )"

    lmax = [len(v) for v in variables]
    for s in structure:
        for i, re in enumerate(s):
            lmax[i] = max(lmax[i], len(re))
    var_names = "   ".join(f"{re:<{lmax[i]}}" for i, re in enumerate(variables))
    rows = [" , ".join(f"{re:<{lmax[i]}}" for i, re in enumerate(s)) for s in structure]
    content = f" ),\n{indent}        ( ".join(rows)
    return f"{indent}# {var_names}\n{indent}       (( {content} ))"
