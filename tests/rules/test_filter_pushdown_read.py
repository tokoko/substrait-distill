from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_filter_over_cross, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"


class TestFilterPushdownRead:
    def test_filter_sets_best_effort_filter(self, manager):
        """Filter(Read) keeps filter rel and sets best_effort_filter on read."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        # Filter rel is kept for correctness
        assert get_rel_type(root_input) == "filter"
        inner_read = root_input.filter.input
        assert get_rel_type(inner_read) == "read"
        # best_effort_filter is set as a hint
        assert inner_read.read.HasField("best_effort_filter")

    def test_filter_cross_pushdown_sets_best_effort(self, manager):
        """Filter pushed through cross ends up as best_effort_filter on reads."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        # Filter on field 0 (left-side)
        filtered = make_filter_over_cross(left, right, 0)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        # Left side: filter pushed down, best_effort_filter set on read
        left_rel = root_input.cross.left
        assert get_rel_type(left_rel) == "filter"
        assert left_rel.filter.input.read.HasField("best_effort_filter")

    def test_existing_best_effort_filter_not_overwritten(self, manager):
        """If read already has best_effort_filter, don't modify it."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)
        # Optimize once to set best_effort_filter
        first = optimize(manager, filtered)
        # Optimize again — should be stable
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
