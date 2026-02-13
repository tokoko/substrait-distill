from substrait.algebra_pb2 import Rel

from helpers import (
    _remap_field_indices_in_place,
    collect_field_indices,
    prune_single_input_rel,
)


def _collect_extra_needed(inner):
    if inner.HasField("condition"):
        return collect_field_indices(inner.condition)
    return set()


def _remap(inner, mapping):
    if inner.HasField("condition"):
        _remap_field_indices_in_place(inner.condition, mapping)


def prune_filter_input(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a FilterRel by modifying the input's emit.

    When a FilterRel has an emit mapping (e.g. added by ProjectRel pruning),
    determines which input fields are actually needed (from emit + condition),
    then adds or modifies an emit on the input rel to only output those fields.
    Remaps the condition and emit accordingly.
    """
    return prune_single_input_rel(rel, "filter", _collect_extra_needed, _remap)
