from substrait.algebra_pb2 import JoinRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


class TestFilterPushdownJoin:
    def test_inner_push_left_pred_to_left(self, manager):
        """INNER join: left-only predicate pushed to left input."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        filtered = pb.filter(joined, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.left) == "filter"
        assert get_rel_type(root_input.join.right) == "read"

    def test_inner_push_right_pred_to_right(self, manager):
        """INNER join: right-only predicate pushed to right with index adjustment."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        filtered = pb.filter(joined, column(2))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.left) == "read"
        assert get_rel_type(root_input.join.right) == "filter"
        # Field index should be adjusted: 2 - 2(left_count) = 0
        pushed_cond = root_input.join.right.filter.condition
        assert pushed_cond.selection.direct_reference.struct_field.field == 0

    def test_inner_mixed_pred_stays_above(self, manager):
        """INNER join: mixed predicate not pushed."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        mixed = scalar_function(COMPARISON_URN, "equal", [column(1), column(3)])
        filtered = pb.filter(joined, mixed)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "join"

    def test_inner_conjunction_pushes_both_sides(self, manager):
        """INNER join: AND(left_pred, right_pred) pushes both sides."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        left_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        right_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [left_pred, right_pred])
        filtered = pb.filter(joined, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.left) == "filter"
        assert get_rel_type(root_input.join.right) == "filter"

    def test_left_join_push_left_pred(self, manager):
        """LEFT join: left-only predicate pushed to left."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_LEFT,
        )
        filtered = pb.filter(joined, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.left) == "filter"

    def test_left_join_right_pred_not_pushed(self, manager):
        """LEFT join: right-only predicate NOT pushed (would change null semantics)."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_LEFT,
        )
        filtered = pb.filter(joined, column(2))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "join"

    def test_right_join_push_right_pred(self, manager):
        """RIGHT join: right-only predicate pushed to right."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_RIGHT,
        )
        filtered = pb.filter(joined, column(2))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert get_rel_type(root_input.join.right) == "filter"

    def test_right_join_left_pred_not_pushed(self, manager):
        """RIGHT join: left-only predicate NOT pushed."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_RIGHT,
        )
        filtered = pb.filter(joined, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "join"

    def test_full_outer_nothing_pushed(self, manager):
        """FULL OUTER join: no predicates pushed."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_OUTER,
        )
        filtered = pb.filter(joined, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "join"
