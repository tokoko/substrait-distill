from substrait.algebra_pb2 import Expression, FunctionArgument, Rel


def resolve_output_field_count(rel: Rel) -> int | None:
    """Get effective output field count, accounting for emit mappings."""
    rel_type = rel.WhichOneof("rel_type")
    if rel_type is None:
        return None
    inner = getattr(rel, rel_type)
    if inner.HasField("common") and inner.common.HasField("emit"):
        return len(inner.common.emit.output_mapping)
    return count_output_fields(rel)


def prune_input(input_rel: Rel, needed: set[int]) -> tuple[Rel, dict[int, int]] | None:
    """Prune an input rel to only output the needed fields.

    Inspects the input's existing emit (or natural field count), checks if
    pruning is possible, and returns a modified copy with updated emit plus
    the old-to-new field index mapping. Returns None if no pruning is needed.
    """
    input_rel_type = input_rel.WhichOneof("rel_type")
    if input_rel_type is None:
        return None

    inner = getattr(input_rel, input_rel_type)
    input_has_emit = inner.HasField("common") and inner.common.HasField("emit")

    if input_has_emit:
        input_emit = list(inner.common.emit.output_mapping)
        input_field_count = len(input_emit)
    else:
        input_field_count = count_output_fields(input_rel)
        if input_field_count is None:
            return None

    if len(needed) >= input_field_count:
        return None

    sorted_needed = sorted(needed)
    old_to_new = {old: new for new, old in enumerate(sorted_needed)}

    new_input = Rel()
    new_input.CopyFrom(input_rel)

    result_inner = getattr(new_input, input_rel_type)
    if input_has_emit:
        result_inner.common.emit.output_mapping[:] = [input_emit[i] for i in sorted_needed]
    else:
        result_inner.common.emit.output_mapping[:] = sorted_needed

    return new_input, old_to_new


def prune_single_input_rel(
    rel: Rel,
    rel_type: str,
    collect_extra_needed=None,
    remap_exprs=None,
) -> Rel | None:
    """Prune unused input fields from a single-input rel (filter, sort, fetch, etc.).

    Common logic for passthrough-schema operators that have an emit mapping.
    `collect_extra_needed(inner) -> set[int] | None` collects additional field
    indices from internal expressions (e.g. condition, sort fields). Returns None
    to bail out. `remap_exprs(result_inner, mapping)` remaps those expressions
    after pruning.
    """
    if rel.WhichOneof("rel_type") != rel_type:
        return None

    inner = getattr(rel, rel_type)

    if not inner.HasField("common") or not inner.common.HasField("emit"):
        return None

    if not inner.HasField("input"):
        return None

    emit = list(inner.common.emit.output_mapping)

    needed: set[int] = set(emit)

    if collect_extra_needed is not None:
        extra = collect_extra_needed(inner)
        if extra is None:
            return None
        needed.update(extra)

    pruned = prune_input(inner.input, needed)
    if pruned is None:
        return None
    new_input, old_to_new = pruned

    result = Rel()
    result.CopyFrom(rel)
    result_inner = getattr(result, rel_type)
    result_inner.input.CopyFrom(new_input)

    if remap_exprs is not None:
        remap_exprs(result_inner, old_to_new)

    result_inner.common.emit.output_mapping[:] = [old_to_new[idx] for idx in emit]

    return result


def prune_bilateral_inputs(
    left: Rel, right: Rel, needed: set[int]
) -> tuple[Rel | None, Rel | None, dict[int, int]] | None:
    """Prune left/right inputs of a bilateral rel (join, cross) to only output needed fields.

    Splits the combined needed set into left/right subsets, prunes each side
    independently, and builds a combined old-to-new mapping over the full index space.
    Returns (new_left_or_None, new_right_or_None, combined_mapping) or None if
    neither side can be pruned.
    """
    left_field_count = resolve_output_field_count(left)
    right_field_count = resolve_output_field_count(right)
    if left_field_count is None or right_field_count is None:
        return None

    left_needed = {idx for idx in needed if idx < left_field_count}
    right_needed = {idx - left_field_count for idx in needed if idx >= left_field_count}

    left_pruned = prune_input(left, left_needed)
    right_pruned = prune_input(right, right_needed)

    if left_pruned is None and right_pruned is None:
        return None

    if left_pruned is not None:
        new_left, left_mapping = left_pruned
        new_left_count = len(left_mapping)
    else:
        new_left = None
        left_mapping = {i: i for i in range(left_field_count)}
        new_left_count = left_field_count

    if right_pruned is not None:
        new_right, right_mapping = right_pruned
    else:
        new_right = None
        right_mapping = {i: i for i in range(right_field_count)}

    combined: dict[int, int] = {}
    for old, new in left_mapping.items():
        combined[old] = new
    for old, new in right_mapping.items():
        combined[left_field_count + old] = new_left_count + new

    return new_left, new_right, combined


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

    elif rel_type == "join":
        join = rel.join
        left_count = (
            count_output_fields(join.left) if join.HasField("left") else None
        )
        right_count = (
            count_output_fields(join.right) if join.HasField("right") else None
        )
        if left_count is not None and right_count is not None:
            return left_count + right_count

    elif rel_type == "aggregate":
        agg = rel.aggregate
        if len(agg.groupings) == 1:
            return len(agg.groupings[0].grouping_expressions) + len(agg.measures)

    elif rel_type == "set":
        set_rel = rel.set
        if len(set_rel.inputs) > 0:
            return count_output_fields(set_rel.inputs[0])

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


def remap_field_indices(expr: Expression, mapping: dict[int, int]) -> Expression:
    """Create a copy of the expression with field reference indices remapped according to mapping."""
    new_expr = Expression()
    new_expr.CopyFrom(expr)
    _remap_field_indices_in_place(new_expr, mapping)
    return new_expr


def _remap_field_indices_in_place(expr: Expression, mapping: dict[int, int]) -> None:
    """Remap field reference indices in-place using a mapping dict."""
    rex_type = expr.WhichOneof("rex_type")

    if rex_type == "selection":
        ref = expr.selection
        if ref.WhichOneof("reference_type") == "direct_reference":
            segment = ref.direct_reference
            if segment.WhichOneof("reference_type") == "struct_field":
                segment.struct_field.field = mapping[segment.struct_field.field]

    elif rex_type == "scalar_function":
        for arg in expr.scalar_function.arguments:
            if arg.HasField("value"):
                _remap_field_indices_in_place(arg.value, mapping)

    elif rex_type == "cast":
        if expr.cast.HasField("input"):
            _remap_field_indices_in_place(expr.cast.input, mapping)

    elif rex_type == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if_"):
                _remap_field_indices_in_place(clause.if_, mapping)
            if clause.HasField("then"):
                _remap_field_indices_in_place(clause.then, mapping)
        if expr.if_then.HasField("else_"):
            _remap_field_indices_in_place(expr.if_then.else_, mapping)


def split_conjunction(
    condition: Expression, fn_names: dict[int, str]
) -> list[Expression]:
    """Recursively split a condition into conjuncts. Nested AND expressions like
    AND(AND(a, b), c) are fully flattened to [a, b, c]."""
    if condition.WhichOneof("rex_type") == "scalar_function":
        sf = condition.scalar_function
        name = fn_names.get(sf.function_reference, "")
        if name == "and" or name.startswith("and:"):
            result = []
            for arg in sf.arguments:
                if arg.HasField("value"):
                    result.extend(split_conjunction(arg.value, fn_names))
            return result

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
