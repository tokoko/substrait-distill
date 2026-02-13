"""Push filter predicates through cross joins.

Handles single predicates referencing one side, and conjunction splitting:
AND(left_pred, right_pred, mixed_pred) pushes left/right parts to their
respective sides and keeps mixed above.
"""

from helpers import (
    adjust_field_indices,
    collect_field_indices,
    count_output_fields,
    make_conjunction,
    split_conjunction,
)
from substrait.algebra_pb2 import Rel


def push_filter_through_cross(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "cross":
        return None

    cross = input_rel.cross
    left_field_count = count_output_fields(cross.left)
    if left_field_count is None:
        return None

    conjuncts = split_conjunction(filter_rel.condition, fn_names)

    left_preds = []
    right_preds = []
    remaining_preds = []

    for conjunct in conjuncts:
        indices = collect_field_indices(conjunct)
        if indices is None:
            remaining_preds.append(conjunct)
        elif all(idx < left_field_count for idx in indices):
            left_preds.append(conjunct)
        elif all(idx >= left_field_count for idx in indices):
            right_preds.append(conjunct)
        else:
            remaining_preds.append(conjunct)

    if not left_preds and not right_preds:
        return None

    # Grab AND metadata for reconstructing conjunctions (if the original was AND).
    sf = filter_rel.condition.scalar_function
    func_ref = sf.function_reference if sf.ByteSize() else 0
    output_type = sf.output_type if sf.HasField("output_type") else None

    # Build left input.
    if left_preds:
        left_cond = make_conjunction(left_preds, func_ref, output_type)
        new_left = Rel()
        new_left.filter.input.CopyFrom(cross.left)
        new_left.filter.condition.CopyFrom(left_cond)
        built_left = optimize_rel(new_left)
    else:
        built_left = optimize_rel(cross.left)

    # Build right input (with index adjustment).
    if right_preds:
        adjusted = [
            adjust_field_indices(p, -left_field_count) for p in right_preds
        ]
        right_cond = make_conjunction(adjusted, func_ref, output_type)
        new_right = Rel()
        new_right.filter.input.CopyFrom(cross.right)
        new_right.filter.condition.CopyFrom(right_cond)
        built_right = optimize_rel(new_right)
    else:
        built_right = optimize_rel(cross.right)

    # Build the cross join.
    new_cross = Rel()
    new_cross.cross.left.CopyFrom(built_left)
    new_cross.cross.right.CopyFrom(built_right)
    if cross.HasField("common"):
        new_cross.cross.common.CopyFrom(cross.common)

    # Wrap with remaining predicates if any.
    if remaining_preds:
        remaining_cond = make_conjunction(remaining_preds, func_ref, output_type)
        result = Rel()
        result.filter.input.CopyFrom(new_cross)
        result.filter.condition.CopyFrom(remaining_cond)
        return result

    return new_cross
