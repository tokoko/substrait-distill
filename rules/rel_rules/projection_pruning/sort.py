from substrait.algebra_pb2 import Rel

from helpers import (
    _remap_field_indices_in_place,
    collect_field_indices,
    prune_single_input_rel,
)


def _collect_extra_needed(inner):
    indices = set()
    for sort_field in inner.sorts:
        if sort_field.HasField("expr"):
            expr_indices = collect_field_indices(sort_field.expr)
            if expr_indices is None:
                return None
            indices.update(expr_indices)
    return indices


def _remap(inner, mapping):
    for sort_field in inner.sorts:
        if sort_field.HasField("expr"):
            _remap_field_indices_in_place(sort_field.expr, mapping)


def prune_sort_input(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a SortRel by modifying the input's emit.

    When a SortRel has an emit mapping, determines which input fields are
    actually needed (from emit + sort expressions), then prunes the input
    to only output those fields. Remaps sort expressions and emit accordingly.
    """
    return prune_single_input_rel(rel, "sort", _collect_extra_needed, _remap)
