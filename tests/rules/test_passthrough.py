from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function
from substrait.proto import Plan

from ..conftest import get_rel_type, make_filter_over_cross, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


class TestPassthrough:
    def test_empty_plan_passthrough(self, manager):
        """An empty plan should pass through unchanged."""
        plan = Plan()
        plan_bytes = plan.SerializeToString()
        result = manager.optimize(plan_bytes)
        assert result == plan_bytes

    def test_no_filter_passthrough(self, manager):
        """A plan without filters should pass through unchanged."""
        read = make_read("my_table", ["x", "y"])
        result = optimize(manager, read)
        plan = optimize(manager, read)
        assert result == plan

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result (fixed point)."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        tree = make_filter_over_cross(left, right, filter_field_index=0)

        first = optimize(manager, tree)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes

    def test_conjunction_split_idempotent(self, manager):
        """Conjunction splitting should be idempotent."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)
        left_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        right_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [left_pred, right_pred])
        filtered = pb.filter(crossed, and_cond)

        first = optimize(manager, filtered)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
