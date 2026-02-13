"""Merge adjacent filters: Filter(outer, Filter(inner, X)) -> Filter(AND(outer, inner), X).

Merging creates optimization opportunities for other pushdown rules to fire
on the combined predicate.
"""

from helpers import make_conjunction
from substrait.algebra_pb2 import Rel


def merge_adjacent_filters(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    if rel.WhichOneof("rel_type") != "filter":
        return None
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "filter":
        return None

    inner_filter = input_rel.filter

    # Find the AND function anchor from fn_names.
    and_anchor = None
    for anchor, name in fn_names.items():
        if name == "and" or name.startswith("and:"):
            and_anchor = anchor
            break

    if and_anchor is None:
        return None

    # Determine output type from existing scalar functions (if any).
    output_type = None
    for cond in (filter_rel.condition, inner_filter.condition):
        if cond.WhichOneof("rex_type") == "scalar_function":
            sf = cond.scalar_function
            if sf.HasField("output_type"):
                output_type = sf.output_type
                break

    # Build merged condition: AND(outer_cond, inner_cond).
    merged_cond = make_conjunction(
        [filter_rel.condition, inner_filter.condition],
        and_anchor,
        output_type,
    )

    # Build merged filter and re-optimize so downstream rules can fire.
    merged = Rel()
    merged.filter.input.CopyFrom(inner_filter.input)
    merged.filter.condition.CopyFrom(merged_cond)

    return optimize_rel(merged)
