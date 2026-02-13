from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_fetch, make_read, optimize


class TestFilterPushdownFetch:
    def test_pushdown_filter_through_fetch(self, manager):
        """Filter(Fetch(Read)) should become Fetch(Filter(Read))."""
        read = make_read("my_table", ["a", "b"])
        fetched = make_fetch(read, 0, 10)
        filtered = pb.filter(fetched, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        assert get_rel_type(root_input.fetch.input) == "filter"
        assert get_rel_type(root_input.fetch.input.filter.input) == "read"

    def test_pushdown_filter_through_fetch_and_cross(self, manager):
        """Filter(Fetch(Cross(L, R))) with left-only predicate should become
        Fetch(Cross(Filter(L), R))."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)
        fetched = make_fetch(crossed, 0, 5)
        filtered = pb.filter(fetched, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        assert get_rel_type(root_input.fetch.input) == "cross"
        assert get_rel_type(root_input.fetch.input.cross.left) == "filter"
        assert get_rel_type(root_input.fetch.input.cross.right) == "read"
