from substrait.algebra_pb2 import Rel

from helpers import prune_single_input_rel


def prune_fetch_input(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    """Prune unused input fields from a FetchRel by modifying the input's emit.

    When a FetchRel has an emit mapping, determines which input fields are
    actually needed (from emit only — offset/count are constants, not field
    references), then prunes the input to only output those fields.
    """
    return prune_single_input_rel(rel, "fetch")
