"""Boolean expression simplification.

Recursively simplifies expressions bottom-up:
- AND(true, x) / AND(x, true) -> x
- AND(false, x) / AND(x, false) -> false
- OR(true, x) / OR(x, true) -> true
- OR(false, x) / OR(x, false) -> x
- NOT(true) -> false
- NOT(false) -> true
- NOT(NOT(x)) -> x
"""

from substrait.algebra_pb2 import Expression, FunctionArgument


def is_bool_literal(expr: Expression, value: bool) -> bool:
    """Check if an expression is a boolean literal with the given value."""
    if expr.WhichOneof("rex_type") != "literal":
        return False
    lit = expr.literal
    if lit.WhichOneof("literal_type") != "boolean":
        return False
    return lit.boolean == value


def make_bool_literal(value: bool) -> Expression:
    """Create a boolean literal expression."""
    expr = Expression()
    expr.literal.boolean = value
    return expr


def _is_fn(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ":")


def simplify_expression(expr: Expression, fn_names: dict[int, str]) -> Expression:
    """Recursively simplify a boolean expression bottom-up."""
    rex_type = expr.WhichOneof("rex_type")

    if rex_type == "scalar_function":
        return _simplify_scalar_function(expr, fn_names)

    if rex_type == "cast":
        if expr.cast.HasField("input"):
            simplified_input = simplify_expression(expr.cast.input, fn_names)
            if simplified_input is not expr.cast.input:
                result = Expression()
                result.CopyFrom(expr)
                result.cast.input.CopyFrom(simplified_input)
                return result

    if rex_type == "if_then":
        return _simplify_if_then(expr, fn_names)

    return expr


def _simplify_scalar_function(expr: Expression, fn_names: dict[int, str]) -> Expression:
    """Simplify a scalar function expression."""
    sf = expr.scalar_function
    name = fn_names.get(sf.function_reference, "")

    # First, simplify all arguments.
    simplified_args = []
    changed = False
    for arg in sf.arguments:
        if arg.HasField("value"):
            simplified = simplify_expression(arg.value, fn_names)
            if simplified is not arg.value:
                changed = True
            simplified_args.append(simplified)
        else:
            simplified_args.append(None)

    # Apply simplification rules based on function name.
    if _is_fn(name, "and"):
        return _simplify_and(simplified_args, expr, changed, fn_names)

    if _is_fn(name, "or"):
        return _simplify_or(simplified_args, expr, changed, fn_names)

    if _is_fn(name, "not"):
        return _simplify_not(simplified_args, expr, changed, fn_names)

    # No simplification rule applies; rebuild if children changed.
    if changed:
        return _rebuild_scalar_function(expr, simplified_args)

    return expr


def _simplify_and(
    args: list[Expression | None],
    original: Expression,
    changed: bool,
    fn_names: dict[int, str],
) -> Expression:
    """Simplify AND(args...)."""
    # Filter out true literals and check for false.
    remaining = []
    for arg in args:
        if arg is None:
            continue
        if is_bool_literal(arg, True):
            changed = True
            continue
        if is_bool_literal(arg, False):
            return make_bool_literal(False)
        remaining.append(arg)

    if not remaining:
        return make_bool_literal(True)

    if len(remaining) == 1:
        return remaining[0]

    if changed:
        return _rebuild_scalar_function(original, remaining)

    return original


def _simplify_or(
    args: list[Expression | None],
    original: Expression,
    changed: bool,
    fn_names: dict[int, str],
) -> Expression:
    """Simplify OR(args...)."""
    # Filter out false literals and check for true.
    remaining = []
    for arg in args:
        if arg is None:
            continue
        if is_bool_literal(arg, False):
            changed = True
            continue
        if is_bool_literal(arg, True):
            return make_bool_literal(True)
        remaining.append(arg)

    if not remaining:
        return make_bool_literal(False)

    if len(remaining) == 1:
        return remaining[0]

    if changed:
        return _rebuild_scalar_function(original, remaining)

    return original


def _simplify_not(
    args: list[Expression | None],
    original: Expression,
    changed: bool,
    fn_names: dict[int, str],
) -> Expression:
    """Simplify NOT(x)."""
    inner = args[0] if args else None
    if inner is None:
        return original

    # NOT(true) -> false
    if is_bool_literal(inner, True):
        return make_bool_literal(False)

    # NOT(false) -> true
    if is_bool_literal(inner, False):
        return make_bool_literal(True)

    # NOT(NOT(x)) -> x
    if inner.WhichOneof("rex_type") == "scalar_function":
        inner_name = fn_names.get(inner.scalar_function.function_reference, "")
        if _is_fn(inner_name, "not"):
            inner_args = inner.scalar_function.arguments
            if len(inner_args) >= 1 and inner_args[0].HasField("value"):
                return inner_args[0].value

    if changed:
        return _rebuild_scalar_function(original, args)

    return original


def _rebuild_scalar_function(
    original: Expression, new_args: list[Expression | None]
) -> Expression:
    """Rebuild a scalar function with new arguments, preserving metadata."""
    result = Expression()
    sf = original.scalar_function
    result.scalar_function.function_reference = sf.function_reference
    if sf.HasField("output_type"):
        result.scalar_function.output_type.CopyFrom(sf.output_type)
    for arg in new_args:
        if arg is not None:
            fa = FunctionArgument()
            fa.value.CopyFrom(arg)
            result.scalar_function.arguments.append(fa)
    return result


def _simplify_if_then(expr: Expression, fn_names: dict[int, str]) -> Expression:
    """Simplify sub-expressions within an if_then."""
    it = expr.if_then
    changed = False
    new_ifs = []
    for clause in it.ifs:
        new_if = simplify_expression(clause.if_, fn_names) if clause.HasField("if_") else None
        new_then = simplify_expression(clause.then, fn_names) if clause.HasField("then") else None
        if new_if is not (clause.if_ if clause.HasField("if_") else None):
            changed = True
        if new_then is not (clause.then if clause.HasField("then") else None):
            changed = True
        new_ifs.append((new_if, new_then))

    new_else = None
    if it.HasField("else_"):
        new_else = simplify_expression(it.else_, fn_names)
        if new_else is not it.else_:
            changed = True

    if not changed:
        return expr

    result = Expression()
    result.CopyFrom(expr)
    for i, (new_if, new_then) in enumerate(new_ifs):
        if new_if is not None:
            result.if_then.ifs[i].if_.CopyFrom(new_if)
        if new_then is not None:
            result.if_then.ifs[i].then.CopyFrom(new_then)
    if new_else is not None:
        result.if_then.else_.CopyFrom(new_else)
    return result
