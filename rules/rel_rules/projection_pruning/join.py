from substrait.algebra_pb2 import Rel

from helpers import (
    _remap_field_indices_in_place,
    collect_field_indices,
    prune_bilateral_inputs,
)


def prune_join_inputs(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a JoinRel by modifying each input's emit.

    When a JoinRel has an emit mapping, determines which fields are needed
    from each side (from emit + join expression), then prunes left and right
    inputs independently. Remaps the join expression and emit accordingly.
    """
    if rel.WhichOneof("rel_type") != "join":
        return None

    join_rel = rel.join

    if not join_rel.HasField("common") or not join_rel.common.HasField("emit"):
        return None

    if not join_rel.HasField("left") or not join_rel.HasField("right"):
        return None

    emit = list(join_rel.common.emit.output_mapping)

    # Collect all needed fields (combined index space).
    needed: set[int] = set(emit)

    if join_rel.HasField("expression"):
        expr_indices = collect_field_indices(join_rel.expression)
        if expr_indices is None:
            return None
        needed.update(expr_indices)

    pruned = prune_bilateral_inputs(join_rel.left, join_rel.right, needed)
    if pruned is None:
        return None
    new_left, new_right, mapping = pruned

    # Build result.
    result = Rel()
    result.CopyFrom(rel)

    if new_left is not None:
        result.join.left.CopyFrom(new_left)
    if new_right is not None:
        result.join.right.CopyFrom(new_right)

    if result.join.HasField("expression"):
        _remap_field_indices_in_place(result.join.expression, mapping)

    result.join.common.emit.output_mapping[:] = [mapping[idx] for idx in emit]

    return result
