from substrait.algebra_pb2 import Rel

from helpers import (
    _remap_field_indices_in_place,
    collect_field_indices,
    count_output_fields,
)


def prune_project_input(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a ProjectRel by modifying the input's emit.

    Looks at which input fields the ProjectRel actually needs (from its
    expressions and emit pass-through), then adds or modifies an emit on
    the input rel to only output those fields. Remaps all field references
    inside the ProjectRel accordingly.
    """
    if rel.WhichOneof("rel_type") != "project":
        return None

    project_rel = rel.project

    # Must have an emit mapping — without one, all input fields are passed through.
    if not project_rel.HasField("common") or not project_rel.common.HasField("emit"):
        return None

    emit = list(project_rel.common.emit.output_mapping)

    if not project_rel.HasField("input"):
        return None

    input_rel = project_rel.input
    input_rel_type = input_rel.WhichOneof("rel_type")
    if input_rel_type is None:
        return None

    inner = getattr(input_rel, input_rel_type)

    # Check if input already has an emit.
    input_has_emit = inner.HasField("common") and inner.common.HasField("emit")

    if input_has_emit:
        input_emit = list(inner.common.emit.output_mapping)
        input_field_count = len(input_emit)
    else:
        input_field_count = count_output_fields(input_rel)
        if input_field_count is None:
            return None

    num_expressions = len(project_rel.expressions)

    # Collect which input fields are actually needed.
    needed: set[int] = set()
    for idx in emit:
        if idx < input_field_count:
            # Pass-through field from input.
            needed.add(idx)
        else:
            # Expression output — collect input fields it references.
            expr_idx = idx - input_field_count
            if expr_idx < 0 or expr_idx >= num_expressions:
                return None
            field_indices = collect_field_indices(project_rel.expressions[expr_idx])
            if field_indices is None:
                return None
            needed.update(field_indices)

    # If all input fields are needed, nothing to prune.
    if len(needed) >= input_field_count:
        return None

    # Build mapping: old input index → new input index.
    sorted_needed = sorted(needed)
    old_to_new = {old: new for new, old in enumerate(sorted_needed)}
    new_input_count = len(sorted_needed)

    # Build result.
    result = Rel()
    result.CopyFrom(rel)

    # Update input rel's emit.
    result_inner = getattr(result.project.input, input_rel_type)
    if input_has_emit:
        new_input_emit = [input_emit[i] for i in sorted_needed]
        result_inner.common.emit.output_mapping[:] = new_input_emit
    else:
        result_inner.common.emit.output_mapping[:] = sorted_needed

    # Remap expression field references.
    for expr in result.project.expressions:
        _remap_field_indices_in_place(expr, old_to_new)

    # Update ProjectRel's emit.
    new_emit = []
    for idx in emit:
        if idx < input_field_count:
            new_emit.append(old_to_new[idx])
        else:
            expr_idx = idx - input_field_count
            new_emit.append(new_input_count + expr_idx)
    result.project.common.emit.output_mapping[:] = new_emit

    return result
