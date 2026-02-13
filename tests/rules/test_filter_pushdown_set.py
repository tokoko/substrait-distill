from substrait.algebra_pb2 import SetRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_read, optimize


class TestFilterPushdownSet:
    def test_pushdown_filter_through_union_all(self, manager):
        """Filter(UnionAll(A, B)) -> UnionAll(Filter(A), Filter(B))."""
        a = make_read("table_a", ["x", "y"])
        b = make_read("table_b", ["x", "y"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)
        filtered = pb.filter(union, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        assert len(root_input.set.inputs) == 2
        assert get_rel_type(root_input.set.inputs[0]) == "filter"
        assert get_rel_type(root_input.set.inputs[1]) == "filter"
        assert root_input.set.op == SetRel.SET_OP_UNION_ALL

    def test_pushdown_filter_through_union_distinct(self, manager):
        """Filter(UnionDistinct(A, B)) -> UnionDistinct(Filter(A), Filter(B))."""
        a = make_read("table_a", ["x", "y"])
        b = make_read("table_b", ["x", "y"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_DISTINCT)
        filtered = pb.filter(union, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        assert get_rel_type(root_input.set.inputs[0]) == "filter"
        assert get_rel_type(root_input.set.inputs[1]) == "filter"
        assert root_input.set.op == SetRel.SET_OP_UNION_DISTINCT

    def test_pushdown_filter_through_intersect(self, manager):
        """Filter(Intersect(A, B)) -> Intersect(Filter(A), Filter(B))."""
        a = make_read("table_a", ["x", "y"])
        b = make_read("table_b", ["x", "y"])
        intersect = pb.set([a, b], SetRel.SET_OP_INTERSECTION_MULTISET)
        filtered = pb.filter(intersect, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        assert get_rel_type(root_input.set.inputs[0]) == "filter"
        assert get_rel_type(root_input.set.inputs[1]) == "filter"

    def test_pushdown_filter_through_except(self, manager):
        """Filter(Except(A, B)) -> Except(Filter(A), Filter(B))."""
        a = make_read("table_a", ["x", "y"])
        b = make_read("table_b", ["x", "y"])
        minus = pb.set([a, b], SetRel.SET_OP_MINUS_PRIMARY)
        filtered = pb.filter(minus, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        assert get_rel_type(root_input.set.inputs[0]) == "filter"
        assert get_rel_type(root_input.set.inputs[1]) == "filter"

    def test_pushdown_filter_through_union_three_inputs(self, manager):
        """Filter pushed to all three inputs of a union."""
        a = make_read("table_a", ["x", "y"])
        b = make_read("table_b", ["x", "y"])
        c = make_read("table_c", ["x", "y"])
        union = pb.set([a, b, c], SetRel.SET_OP_UNION_ALL)
        filtered = pb.filter(union, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        assert len(root_input.set.inputs) == 3
        for inp in root_input.set.inputs:
            assert get_rel_type(inp) == "filter"
            assert get_rel_type(inp.filter.input) == "read"

    def test_pushdown_filter_through_union_and_cross(self, manager):
        """Filter(Union(Cross(L,R), Read)) pushes filter into both union inputs,
        then further through the cross join."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)
        other = make_read("other_table", ["a", "b", "c", "d"])
        union = pb.set([crossed, other], SetRel.SET_OP_UNION_ALL)
        # Filter on field 0 (left-side of cross)
        filtered = pb.filter(union, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        # First input: filter pushed through cross to left
        first = root_input.set.inputs[0]
        assert get_rel_type(first) == "cross"
        assert get_rel_type(first.cross.left) == "filter"
        # Second input: filter on read
        second = root_input.set.inputs[1]
        assert get_rel_type(second) == "filter"
        assert get_rel_type(second.filter.input) == "read"
