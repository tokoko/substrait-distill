"""Push filter predicates through joins.

Handles join-type-dependent pushability:
- INNER: push left-only to left, right-only to right
- LEFT/LEFT_SEMI/LEFT_ANTI/LEFT_SINGLE/LEFT_MARK: push left-only to left only
- RIGHT/RIGHT_SEMI/RIGHT_ANTI/RIGHT_SINGLE/RIGHT_MARK: push right-only to right only
- OUTER/UNSPECIFIED: don't push anything

Supports conjunction splitting — AND(left_pred, right_pred, mixed_pred)
pushes applicable parts to their respective sides and keeps the rest above.
"""

from helpers import (
    adjust_field_indices,
    collect_field_indices,
    count_output_fields,
    make_conjunction,
    split_conjunction,
)
from substrait.algebra_pb2 import Rel

# JoinType enum values that allow pushing to the left input
CAN_PUSH_LEFT = {1, 3, 5, 6, 7, 11}
# INNER, LEFT, LEFT_SEMI, LEFT_ANTI, LEFT_SINGLE, LEFT_MARK

# JoinType enum values that allow pushing to the right input
CAN_PUSH_RIGHT = {1, 4, 8, 9, 10, 12}
# INNER, RIGHT, RIGHT_SEMI, RIGHT_ANTI, RIGHT_SINGLE, RIGHT_MARK


def push_filter_through_join(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "join":
        return None

    join = input_rel.join
    join_type = join.type

    can_left = join_type in CAN_PUSH_LEFT
    can_right = join_type in CAN_PUSH_RIGHT
    if not can_left and not can_right:
        return None

    left_field_count = count_output_fields(join.left)
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
        elif can_left and all(idx < left_field_count for idx in indices):
            left_preds.append(conjunct)
        elif can_right and all(idx >= left_field_count for idx in indices):
            right_preds.append(conjunct)
        else:
            remaining_preds.append(conjunct)

    if not left_preds and not right_preds:
        return None

    # Grab AND metadata for reconstructing conjunctions.
    sf = filter_rel.condition.scalar_function
    func_ref = sf.function_reference if sf.ByteSize() else 0
    output_type = sf.output_type if sf.HasField("output_type") else None

    # Build left input.
    if left_preds:
        left_cond = make_conjunction(left_preds, func_ref, output_type)
        new_left = Rel()
        new_left.filter.input.CopyFrom(join.left)
        new_left.filter.condition.CopyFrom(left_cond)
        built_left = optimize_rel(new_left)
    else:
        built_left = optimize_rel(join.left)

    # Build right input (with index adjustment).
    if right_preds:
        adjusted = [
            adjust_field_indices(p, -left_field_count) for p in right_preds
        ]
        right_cond = make_conjunction(adjusted, func_ref, output_type)
        new_right = Rel()
        new_right.filter.input.CopyFrom(join.right)
        new_right.filter.condition.CopyFrom(right_cond)
        built_right = optimize_rel(new_right)
    else:
        built_right = optimize_rel(join.right)

    # Build the join, preserving all original metadata.
    new_join = Rel()
    new_join.join.left.CopyFrom(built_left)
    new_join.join.right.CopyFrom(built_right)
    new_join.join.type = join.type
    if join.HasField("expression"):
        new_join.join.expression.CopyFrom(join.expression)
    if join.HasField("post_join_filter"):
        new_join.join.post_join_filter.CopyFrom(join.post_join_filter)
    if join.HasField("common"):
        new_join.join.common.CopyFrom(join.common)

    # Wrap with remaining predicates if any.
    if remaining_preds:
        remaining_cond = make_conjunction(remaining_preds, func_ref, output_type)
        result = Rel()
        result.filter.input.CopyFrom(new_join)
        result.filter.condition.CopyFrom(remaining_cond)
        return result

    return new_join
