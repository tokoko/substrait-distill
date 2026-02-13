from substrait.algebra_pb2 import Rel


def push_filter_into_read(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Push filter predicate into ReadRel.best_effort_filter as a hint.

    The Filter rel is kept for correctness — best_effort_filter is a hint
    that the reader MAY use to skip data, not a guarantee.

    Only fires when best_effort_filter is not already set to avoid
    infinite loops in the fixed-point optimizer.
    """
    filter_rel = rel.filter
    input_rel = filter_rel.input
    if input_rel.WhichOneof("rel_type") != "read":
        return None
    if input_rel.read.HasField("best_effort_filter"):
        return None

    result = Rel()
    result.CopyFrom(rel)
    result.filter.input.read.best_effort_filter.CopyFrom(filter_rel.condition)
    return result
