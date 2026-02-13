"""Push filter predicates through set operations (union, intersect, except).

Filter(Set(A, B, ...)) -> Set(Filter(A), Filter(B), ...)

Safe for all set operation types because filtering all inputs by the same
predicate preserves the set operation semantics.
"""

from substrait.algebra_pb2 import Rel


def push_filter_through_set(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "set":
        return None

    set_rel = input_rel.set

    if set_rel.op == 0:  # SET_OP_UNSPECIFIED
        return None

    if len(set_rel.inputs) < 2:
        return None

    # Push the filter into each input of the set operation.
    new_inputs = []
    for inp in set_rel.inputs:
        new_filter = Rel()
        new_filter.filter.input.CopyFrom(inp)
        new_filter.filter.condition.CopyFrom(filter_rel.condition)
        if filter_rel.HasField("common"):
            new_filter.filter.common.CopyFrom(filter_rel.common)
        new_inputs.append(optimize_rel(new_filter))

    result = Rel()
    result.set.op = set_rel.op
    if set_rel.HasField("common"):
        result.set.common.CopyFrom(set_rel.common)
    for inp in new_inputs:
        result.set.inputs.append(inp)

    return result
