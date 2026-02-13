from substrait.algebra_pb2 import Expression, Rel

from helpers import resolve_output_field_count


def remove_identity_project(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Remove a ProjectRel that is an identity (output equals input).

    A ProjectRel is identity when each output position i maps back to input
    field i and the output has the same number of fields as the input. This
    covers both the trivial case (no expressions, no/identity emit) and the
    common post-optimization case where expressions are simple col(i)
    pass-throughs selected by emit.
    """
    if rel.WhichOneof("rel_type") != "project":
        return None

    project_rel = rel.project

    if not project_rel.HasField("input"):
        return None

    input_field_count = resolve_output_field_count(project_rel.input)
    if input_field_count is None:
        return None

    num_expressions = len(project_rel.expressions)

    if not project_rel.HasField("common") or not project_rel.common.HasField("emit"):
        # Without emit: output = input fields + expressions. Identity iff no expressions.
        if num_expressions == 0:
            result = Rel()
            result.CopyFrom(project_rel.input)
            return result
        return None

    emit = list(project_rel.common.emit.output_mapping)

    # Must produce same number of fields as input.
    if len(emit) != input_field_count:
        return None

    # Check each output position maps to the corresponding input field.
    for i, idx in enumerate(emit):
        if idx == i:
            # Direct pass-through of input field i.
            continue
        if idx >= input_field_count:
            # Expression — check if it's just col(i).
            expr_idx = idx - input_field_count
            if expr_idx >= num_expressions:
                return None
            if not _is_field_ref(project_rel.expressions[expr_idx], i):
                return None
        else:
            # Pass-through of wrong input field.
            return None

    result = Rel()
    result.CopyFrom(project_rel.input)
    return result


def _is_field_ref(expr: Expression, expected_field: int) -> bool:
    """Check if an expression is a simple direct field reference to the expected field."""
    if expr.WhichOneof("rex_type") != "selection":
        return False
    ref = expr.selection
    if ref.WhichOneof("reference_type") != "direct_reference":
        return False
    segment = ref.direct_reference
    if segment.WhichOneof("reference_type") != "struct_field":
        return False
    return segment.struct_field.field == expected_field
