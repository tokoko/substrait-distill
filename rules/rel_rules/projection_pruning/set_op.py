from substrait.algebra_pb2 import Rel

from helpers import prune_input


def prune_set_inputs(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a SetRel by modifying each input's emit.

    When a SetRel has an emit mapping, all inputs share the same schema, so
    the same fields can be pruned from every input.
    """
    if rel.WhichOneof("rel_type") != "set":
        return None

    set_rel = rel.set

    if not set_rel.HasField("common") or not set_rel.common.HasField("emit"):
        return None

    if len(set_rel.inputs) == 0:
        return None

    emit = list(set_rel.common.emit.output_mapping)
    needed = set(emit)

    # Try pruning the first input to check feasibility and get the mapping.
    pruned_first = prune_input(set_rel.inputs[0], needed)
    if pruned_first is None:
        return None
    _, old_to_new = pruned_first

    # Prune all inputs (including re-pruning the first for simplicity).
    new_inputs = []
    for inp in set_rel.inputs:
        pruned = prune_input(inp, needed)
        new_inputs.append(pruned[0] if pruned is not None else inp)

    result = Rel()
    result.CopyFrom(rel)
    for i, new_inp in enumerate(new_inputs):
        result.set.inputs[i].CopyFrom(new_inp)

    result.set.common.emit.output_mapping[:] = [old_to_new[idx] for idx in emit]

    return result
