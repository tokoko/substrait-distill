from substrait.algebra_pb2 import JoinRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


class TestFilterMerge:
    def test_merge_two_adjacent_filters(self, manager):
        """Filter(outer, Filter(inner_AND, Read)) merges into single Filter(Read)."""
        read = make_read("my_table", ["a", "b", "c"])
        # Inner filter uses AND so it's registered in plan extensions
        inner_cond = scalar_function(
            BOOLEAN_URN,
            "and",
            [
                scalar_function(COMPARISON_URN, "is_not_null", [column(0)]),
                scalar_function(COMPARISON_URN, "is_not_null", [column(1)]),
            ],
        )
        inner_filtered = pb.filter(read, inner_cond)
        outer_filtered = pb.filter(
            inner_filtered,
            scalar_function(COMPARISON_URN, "is_not_null", [column(2)]),
        )
        result = optimize(manager, outer_filtered)

        root_input = result.relations[0].root.input
        # Should be a single filter (not nested)
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "read"

    def test_merge_enables_cross_pushdown(self, manager):
        """Merged filter enables cross pushdown.
        Filter(left_pred, Filter(AND(right_a, right_b), Cross(L,R)))
        -> Cross(Filter(L), Filter(R))"""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)
        # Inner filter uses AND so it's registered in plan extensions
        inner_cond = scalar_function(
            BOOLEAN_URN,
            "and",
            [
                scalar_function(COMPARISON_URN, "is_not_null", [column(2)]),
                scalar_function(COMPARISON_URN, "is_not_null", [column(3)]),
            ],
        )
        inner = pb.filter(crossed, inner_cond)
        outer = pb.filter(
            inner,
            scalar_function(COMPARISON_URN, "is_not_null", [column(0)]),
        )
        result = optimize(manager, outer)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert get_rel_type(root_input.cross.left) == "filter"
        assert get_rel_type(root_input.cross.right) == "filter"

    def test_merge_enables_join_pushdown(self, manager):
        """Merged filter enables join pushdown.
        Filter(left_pred, Filter(AND(right_a, right_b), Join(L,R)))
        -> Join(Filter(L), Filter(R)) for INNER."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        inner_cond = scalar_function(
            BOOLEAN_URN,
            "and",
            [
                scalar_function(COMPARISON_URN, "is_not_null", [column(2)]),
                scalar_function(COMPARISON_URN, "is_not_null", [column(3)]),
            ],
        )
        inner = pb.filter(joined, inner_cond)
        outer = pb.filter(
            inner,
            scalar_function(COMPARISON_URN, "is_not_null", [column(0)]),
        )
        result = optimize(manager, outer)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.left) == "filter"
        assert get_rel_type(root_input.join.right) == "filter"
