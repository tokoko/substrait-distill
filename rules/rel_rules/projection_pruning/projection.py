from substrait.algebra_pb2 import Rel

from helpers import (
    _remap_field_indices_in_place,
    collect_field_indices,
    prune_input,
    resolve_output_field_count,
)


def prune_project_input(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused expressions and input fields from a ProjectRel.

    When a ProjectRel has an emit mapping, determines which expressions are
    actually needed (referenced by emit), drops the rest, then prunes input
    fields to only those needed by emit pass-through + remaining expressions.
    Fires if either expressions or input fields can be pruned.
    """
    if rel.WhichOneof("rel_type") != "project":
        return None

    project_rel = rel.project

    if not project_rel.HasField("common") or not project_rel.common.HasField("emit"):
        return None

    if not project_rel.HasField("input"):
        return None

    emit = list(project_rel.common.emit.output_mapping)

    input_field_count = resolve_output_field_count(project_rel.input)
    if input_field_count is None:
        return None

    num_expressions = len(project_rel.expressions)

    # Determine which expressions are needed and collect input fields.
    needed_expr_indices: set[int] = set()
    needed_input_fields: set[int] = set()

    for idx in emit:
        if idx < input_field_count:
            needed_input_fields.add(idx)
        else:
            expr_idx = idx - input_field_count
            if expr_idx < 0 or expr_idx >= num_expressions:
                return None
            needed_expr_indices.add(expr_idx)
            field_indices = collect_field_indices(project_rel.expressions[expr_idx])
            if field_indices is None:
                return None
            needed_input_fields.update(field_indices)

    can_prune_exprs = len(needed_expr_indices) < num_expressions
    pruned = prune_input(project_rel.input, needed_input_fields)

    if pruned is None and not can_prune_exprs:
        return None

    # Build result.
    result = Rel()
    result.CopyFrom(rel)

    if pruned is not None:
        new_input, input_mapping = pruned
        result.project.input.CopyFrom(new_input)
        new_input_count = len(input_mapping)
    else:
        input_mapping = None
        new_input_count = input_field_count

    # Drop unused expressions (remove from end to avoid index shift).
    if can_prune_exprs:
        sorted_needed = sorted(needed_expr_indices)
        expr_mapping = {old: new for new, old in enumerate(sorted_needed)}
        for i in range(num_expressions - 1, -1, -1):
            if i not in needed_expr_indices:
                del result.project.expressions[i]
    else:
        expr_mapping = {i: i for i in range(num_expressions)}

    # Remap expression field references if input was pruned.
    if input_mapping is not None:
        for expr in result.project.expressions:
            _remap_field_indices_in_place(expr, input_mapping)

    # Remap emit.
    new_emit = []
    for idx in emit:
        if idx < input_field_count:
            new_emit.append(input_mapping[idx] if input_mapping is not None else idx)
        else:
            expr_idx = idx - input_field_count
            new_emit.append(new_input_count + expr_mapping[expr_idx])
    result.project.common.emit.output_mapping[:] = new_emit

    return result
