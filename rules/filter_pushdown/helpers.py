from substrait.algebra_pb2 import Expression, FunctionArgument, Rel


def count_output_fields(rel: Rel) -> int | None:
    """Count the number of output fields for a relation. Returns None if unknown."""
    rel_type = rel.WhichOneof("rel_type")

    if rel_type == "read":
        read = rel.read
        if read.HasField("base_schema") and read.base_schema.HasField("struct"):
            return len(read.base_schema.struct.types)

    elif rel_type == "filter":
        if rel.filter.HasField("input"):
            return count_output_fields(rel.filter.input)

    elif rel_type == "cross":
        cross = rel.cross
        left_count = (
            count_output_fields(cross.left) if cross.HasField("left") else None
        )
        right_count = (
            count_output_fields(cross.right) if cross.HasField("right") else None
        )
        if left_count is not None and right_count is not None:
            return left_count + right_count

    elif rel_type == "project":
        proj = rel.project
        input_count = (
            count_output_fields(proj.input) if proj.HasField("input") else None
        )
        if input_count is not None:
            return input_count + len(proj.expressions)

    elif rel_type in ("fetch", "sort"):
        inner = getattr(rel, rel_type)
        if inner.HasField("input"):
            return count_output_fields(inner.input)

    return None


def collect_field_indices(expr: Expression) -> set[int] | None:
    """Collect all direct field reference indices from an expression.
    Returns None if the expression contains non-direct references we can't analyze."""
    indices: set[int] = set()
    if not _collect_field_indices_impl(expr, indices):
        return None
    return indices


def _collect_field_indices_impl(expr: Expression, indices: set[int]) -> bool:
    """Recursively collect field indices. Returns False if we encounter something we can't handle."""
    rex_type = expr.WhichOneof("rex_type")

    if rex_type == "selection":
        ref = expr.selection
        ref_type = ref.WhichOneof("reference_type")
        if ref_type == "direct_reference":
            segment = ref.direct_reference
            seg_type = segment.WhichOneof("reference_type")
            if seg_type == "struct_field":
                indices.add(segment.struct_field.field)
                return True
        return False

    elif rex_type == "scalar_function":
        for arg in expr.scalar_function.arguments:
            if arg.HasField("value"):
                if not _collect_field_indices_impl(arg.value, indices):
                    return False
        return True

    elif rex_type == "literal":
        return True

    elif rex_type == "cast":
        if expr.cast.HasField("input"):
            return _collect_field_indices_impl(expr.cast.input, indices)
        return True

    elif rex_type == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if_"):
                if not _collect_field_indices_impl(clause.if_, indices):
                    return False
            if clause.HasField("then"):
                if not _collect_field_indices_impl(clause.then, indices):
                    return False
        if expr.if_then.HasField("else_"):
            if not _collect_field_indices_impl(expr.if_then.else_, indices):
                return False
        return True

    return False


def adjust_field_indices(expr: Expression, offset: int) -> Expression:
    """Create a copy of the expression with all field reference indices adjusted by offset."""
    new_expr = Expression()
    new_expr.CopyFrom(expr)
    _adjust_field_indices_in_place(new_expr, offset)
    return new_expr


def _adjust_field_indices_in_place(expr: Expression, offset: int) -> None:
    """Adjust field reference indices in-place."""
    rex_type = expr.WhichOneof("rex_type")

    if rex_type == "selection":
        ref = expr.selection
        if ref.WhichOneof("reference_type") == "direct_reference":
            segment = ref.direct_reference
            if segment.WhichOneof("reference_type") == "struct_field":
                segment.struct_field.field += offset

    elif rex_type == "scalar_function":
        for arg in expr.scalar_function.arguments:
            if arg.HasField("value"):
                _adjust_field_indices_in_place(arg.value, offset)

    elif rex_type == "cast":
        if expr.cast.HasField("input"):
            _adjust_field_indices_in_place(expr.cast.input, offset)

    elif rex_type == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if_"):
                _adjust_field_indices_in_place(clause.if_, offset)
            if clause.HasField("then"):
                _adjust_field_indices_in_place(clause.then, offset)
        if expr.if_then.HasField("else_"):
            _adjust_field_indices_in_place(expr.if_then.else_, offset)


def split_conjunction(
    condition: Expression, fn_names: dict[int, str]
) -> list[Expression]:
    """Split a condition into conjuncts. If condition is an AND scalar_function,
    return its argument expressions. Otherwise return [condition] as-is."""
    if condition.WhichOneof("rex_type") == "scalar_function":
        sf = condition.scalar_function
        name = fn_names.get(sf.function_reference, "")
        if name == "and" or name.startswith("and:"):
            return [arg.value for arg in sf.arguments if arg.HasField("value")]

    return [condition]


def make_conjunction(
    exprs: list[Expression],
    function_reference: int,
    output_type,
) -> Expression:
    """Combine expressions with AND. Returns the expression directly if only one."""
    assert len(exprs) >= 1
    if len(exprs) == 1:
        return exprs[0]

    result = Expression()
    result.scalar_function.function_reference = function_reference
    if output_type is not None:
        result.scalar_function.output_type.CopyFrom(output_type)
    for expr in exprs:
        arg = FunctionArgument()
        arg.value.CopyFrom(expr)
        result.scalar_function.arguments.append(arg)
    return result
