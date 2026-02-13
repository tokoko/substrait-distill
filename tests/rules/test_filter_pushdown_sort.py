from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_read, optimize


class TestFilterPushdownSort:
    def test_pushdown_filter_through_sort(self, manager):
        """Filter(Sort(Read)) should become Sort(Filter(Read))."""
        read = make_read("my_table", ["a", "b"])
        sorted_plan = pb.sort(read, [column(0)])
        filtered = pb.filter(sorted_plan, column(1))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        assert get_rel_type(root_input.sort.input) == "filter"
        assert get_rel_type(root_input.sort.input.filter.input) == "read"

    def test_pushdown_filter_through_sort_and_cross(self, manager):
        """Filter(Sort(Cross(L, R))) with left-only predicate should become
        Sort(Cross(Filter(L), R)) — both rules applied in a single optimize call."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])

        crossed = pb.cross(left, right)
        sorted_plan = pb.sort(crossed, [column(0)])
        filtered = pb.filter(sorted_plan, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        assert get_rel_type(root_input.sort.input) == "cross"
        assert get_rel_type(root_input.sort.input.cross.left) == "filter"
        assert get_rel_type(root_input.sort.input.cross.right) == "read"
