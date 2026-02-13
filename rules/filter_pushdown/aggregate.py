"""Push filter predicates below aggregates.

Filter(Aggregate(X)) -> Aggregate(Filter(X)) when the filter predicate
references only grouping key columns that are simple field references.
Only handles single grouping set aggregates.
"""

from helpers import (
    collect_field_indices,
    make_conjunction,
    remap_field_indices,
    split_conjunction,
)
from substrait.algebra_pb2 import Rel


def push_filter_through_aggregate(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "aggregate":
        return None

    agg = input_rel.aggregate

    # Only handle single grouping set.
    if len(agg.groupings) != 1:
        return None

    grouping = agg.groupings[0]
    num_grouping_exprs = len(grouping.grouping_expressions)

    if num_grouping_exprs == 0:
        return None

    if not agg.HasField("input"):
        return None

    # Build mapping: output_idx -> input_field_idx.
    # Only proceed if ALL grouping expressions are simple field references.
    output_to_input: dict[int, int] = {}
    for i, gexpr in enumerate(grouping.grouping_expressions):
        if gexpr.WhichOneof("rex_type") != "selection":
            return None
        ref = gexpr.selection
        if ref.WhichOneof("reference_type") != "direct_reference":
            return None
        segment = ref.direct_reference
        if segment.WhichOneof("reference_type") != "struct_field":
            return None
        output_to_input[i] = segment.struct_field.field

    conjuncts = split_conjunction(filter_rel.condition, fn_names)

    pushable = []
    remaining = []

    for conjunct in conjuncts:
        indices = collect_field_indices(conjunct)
        if indices is not None and all(idx < num_grouping_exprs for idx in indices):
            pushable.append(conjunct)
        else:
            remaining.append(conjunct)

    if not pushable:
        return None

    # Grab AND metadata for reconstructing conjunctions.
    sf = filter_rel.condition.scalar_function
    func_ref = sf.function_reference if sf.ByteSize() else 0
    output_type = sf.output_type if sf.HasField("output_type") else None

    # Remap field indices from output space to input space.
    remapped = [remap_field_indices(p, output_to_input) for p in pushable]
    push_cond = make_conjunction(remapped, func_ref, output_type)

    new_filter = Rel()
    new_filter.filter.input.CopyFrom(agg.input)
    new_filter.filter.condition.CopyFrom(push_cond)

    result = Rel()
    result.aggregate.CopyFrom(agg)
    result.aggregate.input.CopyFrom(optimize_rel(new_filter))

    # Keep remaining predicates above the aggregate.
    if remaining:
        remaining_cond = make_conjunction(remaining, func_ref, output_type)
        wrapped = Rel()
        wrapped.filter.input.CopyFrom(result)
        wrapped.filter.condition.CopyFrom(remaining_cond)
        return wrapped

    return result
