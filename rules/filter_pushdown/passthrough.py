"""Push filter below schema-preserving operators: Filter(X(input)) -> X(Filter(input)).

Applies to operators like sort and fetch that pass through the schema unchanged.
Filtering earlier reduces the data flowing through these operators.
"""

from substrait.algebra_pb2 import Rel

PASSTHROUGH_TYPES = ("sort", "fetch")


def push_filter_through_passthrough(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    child_type = input_rel.WhichOneof("rel_type")
    if child_type not in PASSTHROUGH_TYPES:
        return None

    child_rel = getattr(input_rel, child_type)

    new_filter = Rel()
    new_filter.filter.input.CopyFrom(child_rel.input)
    new_filter.filter.condition.CopyFrom(filter_rel.condition)
    if filter_rel.HasField("common"):
        new_filter.filter.common.CopyFrom(filter_rel.common)

    result = Rel()
    getattr(result, child_type).CopyFrom(child_rel)
    getattr(result, child_type).input.CopyFrom(optimize_rel(new_filter))
    return result
