"""Push filter predicates through cross joins.

Handles single predicates referencing one side, and conjunction splitting:
AND(left_pred, right_pred, mixed_pred) pushes left/right parts to their
respective sides. Mixed predicates convert the cross join to an inner join.
"""

from helpers import (
    adjust_field_indices,
    collect_field_indices,
    count_output_fields,
    make_conjunction,
    split_conjunction,
)
from substrait.algebra_pb2 import JoinRel, Rel


def push_filter_through_cross(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    if rel.WhichOneof("rel_type") != "filter":
        return None
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
    mixed_preds = []

    for conjunct in conjuncts:
        indices = collect_field_indices(conjunct)
        if indices is None:
            mixed_preds.append(conjunct)
        elif all(idx < left_field_count for idx in indices):
            left_preds.append(conjunct)
        elif all(idx >= left_field_count for idx in indices):
            right_preds.append(conjunct)
        else:
            mixed_preds.append(conjunct)

    if not left_preds and not right_preds and not mixed_preds:
        return None

    # Need at least something actionable: pushable preds or convertible mixed preds.
    if not left_preds and not right_preds:
        # Only mixed preds — convert to inner join.
        pass

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

    # If there are mixed predicates, convert to inner join.
    if mixed_preds:
        join_expr = make_conjunction(mixed_preds, func_ref, output_type)
        result = Rel()
        result.join.left.CopyFrom(built_left)
        result.join.right.CopyFrom(built_right)
        result.join.expression.CopyFrom(join_expr)
        result.join.type = JoinRel.JOIN_TYPE_INNER
        if cross.HasField("common"):
            result.join.common.CopyFrom(cross.common)
        return result

    # No mixed predicates — keep as cross join.
    result = Rel()
    result.cross.left.CopyFrom(built_left)
    result.cross.right.CopyFrom(built_right)
    if cross.HasField("common"):
        result.cross.common.CopyFrom(cross.common)
    return result
