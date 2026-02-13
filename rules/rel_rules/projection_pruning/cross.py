from substrait.algebra_pb2 import Rel

from helpers import prune_bilateral_inputs


def prune_cross_inputs(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a CrossRel by modifying each input's emit.

    When a CrossRel has an emit mapping, determines which fields are needed
    from each side, then prunes left and right inputs independently.
    Remaps the emit accordingly.
    """
    if rel.WhichOneof("rel_type") != "cross":
        return None

    cross_rel = rel.cross

    if not cross_rel.HasField("common") or not cross_rel.common.HasField("emit"):
        return None

    if not cross_rel.HasField("left") or not cross_rel.HasField("right"):
        return None

    emit = list(cross_rel.common.emit.output_mapping)

    pruned = prune_bilateral_inputs(cross_rel.left, cross_rel.right, set(emit))
    if pruned is None:
        return None
    new_left, new_right, mapping = pruned

    # Build result.
    result = Rel()
    result.CopyFrom(rel)

    if new_left is not None:
        result.cross.left.CopyFrom(new_left)
    if new_right is not None:
        result.cross.right.CopyFrom(new_right)

    result.cross.common.emit.output_mapping[:] = [mapping[idx] for idx in emit]

    return result
