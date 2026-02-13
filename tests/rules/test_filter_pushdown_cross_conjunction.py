from substrait.algebra_pb2 import JoinRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


class TestFilterPushdownCrossConjunction:
    def test_split_and_push_both_sides(self, manager):
        """AND(left_pred, right_pred) should push both sides down."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        left_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        right_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [left_pred, right_pred])
        filtered = pb.filter(crossed, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert get_rel_type(root_input.cross.left) == "filter"
        assert get_rel_type(root_input.cross.right) == "filter"

    def test_split_and_push_left_only(self, manager):
        """AND(left_pred1, left_pred2) should push entire conjunction to left."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        pred1 = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        pred2 = scalar_function(COMPARISON_URN, "is_not_null", [column(1)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [pred1, pred2])
        filtered = pb.filter(crossed, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert get_rel_type(root_input.cross.left) == "filter"
        assert get_rel_type(root_input.cross.right) == "read"

    def test_split_with_mixed_converts_to_join(self, manager):
        """AND(left_pred, mixed_pred) should push left down, mixed becomes join expression."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        left_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        mixed_pred = scalar_function(COMPARISON_URN, "equal", [column(0), column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [left_pred, mixed_pred])
        filtered = pb.filter(crossed, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        # Mixed pred converts cross to inner join
        assert get_rel_type(root_input) == "join"
        assert root_input.join.type == JoinRel.JOIN_TYPE_INNER
        assert root_input.join.HasField("expression")
        # Left pred was pushed down
        assert get_rel_type(root_input.join.left) == "filter"

    def test_nested_and_flattened_and_pushed(self, manager):
        """AND(AND(left_a, left_b), right_pred) should flatten and push all sides."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        left_a = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        left_b = scalar_function(COMPARISON_URN, "is_not_null", [column(1)])
        nested_and = scalar_function(BOOLEAN_URN, "and", [left_a, left_b])
        right_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        outer_and = scalar_function(BOOLEAN_URN, "and", [nested_and, right_pred])
        filtered = pb.filter(crossed, outer_and)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        # left_a and left_b pushed to left (as AND)
        assert get_rel_type(root_input.cross.left) == "filter"
        # right_pred pushed to right
        assert get_rel_type(root_input.cross.right) == "filter"

    def test_non_and_mixed_converts_to_join(self, manager):
        """A non-AND mixed condition converts cross to inner join."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        eq_mixed = scalar_function(COMPARISON_URN, "equal", [column(0), column(2)])
        filtered = pb.filter(crossed, eq_mixed)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert root_input.join.type == JoinRel.JOIN_TYPE_INNER
        assert root_input.join.HasField("expression")
