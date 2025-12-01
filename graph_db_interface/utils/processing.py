from typing import Optional, Any, Hashable


def process_bindings_select(
    bindings: list[dict[str, dict[str, Any]]],
    variables: Optional[list[str]] = None,
    grouping_variables: Optional[list[str]] = None,
) -> tuple[tuple[Any, ...], ...] | tuple[Any, ...] | dict:
    """
    Process SPARQL SELECT bindings into tuples or a nested dictionary.

    Converts raw SPARQL JSON bindings (`results.bindings`) into a convenient Python
    structure for downstream processing and display.

    Args:
        bindings (list[dict[str, dict[str, Any]]]): Raw bindings from SPARQL query results
            (i.e., `response['results']['bindings']`).
        variables (Optional[list[str]]): Variable names (without leading `?`) that define the
            tuple entries and their order in the result.
        grouping_variables (Optional[list[str]]): Grouping variable names (without leading `?`)
            that define hierarchical dictionary keys in order. When provided, the result is a
            nested dict keyed by these values, with leaves as described below.

    Returns:
        tuple[tuple[Any, ...], ...] | tuple[Any, ...] | dict: When `grouping_variables` is not provided:
            - variables is empty or None -> () (empty tuple)
            - variables has length 1     -> (a1, a2, ...) (tuple of scalar values)
            - variables has length > 1   -> ((a1, b1, ...), (a2, b2, ...), ...) (tuple of tuples)
            When `grouping_variables` is provided:
            - Nested dict keyed by the values of `grouping_variables` (in order)
            - Each leaf is processed using the entries in `variables` (shapes as above)

    Raises:
        AssertionError: If any values referenced by `grouping_variables` in the first binding
            are not hashable. Grouping keys must be hashable to serve as dict keys.
        TypeError: If the bindings are empty and neither `variables` nor `grouping_variables` are provided.

    Notes:
        - `variables` and `grouping_variables` are expected without the leading `?`.
        - If neither is provided, `variables` are inferred from the first binding when available.
    """

    if not variables and not grouping_variables:
        assert bindings, TypeError(
            "Cannot determine result structure without variables or grouping_variables for empty bindings."
        )
        variables = list(bindings[0].keys())
        # LOGGER.warning(f"Inferring variables {variables} from bindings. For predictable results, please provide variables explicitly.")

    if not variables:
        extract_entry = lambda binding: None
    elif len(variables) == 1:
        extract_entry = lambda binding: binding[variables[0]]["value"]
    else:
        extract_entry = lambda binding: tuple(
            binding[variable]["value"] for variable in variables
        )

    if not grouping_variables:
        return tuple(extract_entry(binding) for binding in bindings)

    else:  # Process with grouping variables
        assert all(
            isinstance(bindings[0][key]["value"], Hashable)
            for key in grouping_variables
        ), TypeError("All datatypes in grouping_variables must be hashable.")

        result = {}
        leaf_dicts = []

        for binding in bindings:
            leaf = result
            for key in grouping_variables[:-1]:
                key_value = binding[key]["value"]
                leaf = leaf.setdefault(key_value, {})
            key_value = binding[grouping_variables[-1]]["value"]
            leaf.setdefault(key_value, []).append(extract_entry(binding))
            leaf_dicts.append(leaf)

        for leaf_dict in leaf_dicts:
            for k, l in leaf_dict.items():
                leaf_dict[k] = tuple(l) if variables else tuple()

        return result
