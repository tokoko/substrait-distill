from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function
from substrait.proto import Plan

from .conftest import (
    get_rel_type,
    make_fetch,
    make_filter_over_cross,
    make_read,
    optimize,
)

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


class TestFilterPushdownCross:
    def test_pushdown_filter_to_left(self, manager):
        """Filter on left-side field should be pushed below the cross join."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        result = optimize(
            manager, make_filter_over_cross(left, right, filter_field_index=0)
        )

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert get_rel_type(root_input.cross.left) == "filter"
        assert get_rel_type(root_input.cross.left.filter.input) == "read"
        assert get_rel_type(root_input.cross.right) == "read"

    def test_pushdown_filter_to_right(self, manager):
        """Filter on right-side field should be pushed below the cross join
        with adjusted field indices."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        result = optimize(
            manager, make_filter_over_cross(left, right, filter_field_index=2)
        )

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert get_rel_type(root_input.cross.left) == "read"
        assert get_rel_type(root_input.cross.right) == "filter"

        # Field index should be adjusted: 2 - 2(left_count) = 0
        pushed_filter = root_input.cross.right.filter
        cond = pushed_filter.condition
        assert cond.WhichOneof("rex_type") == "selection"
        ref = cond.selection.direct_reference.struct_field.field
        assert ref == 0

    def test_no_pushdown_for_mixed_fields(self, manager):
        """Filter referencing fields from both sides should NOT be pushed down."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])

        crossed = pb.cross(left, right)
        filtered = pb.filter(
            crossed,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
        )
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "cross"


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

    def test_split_with_mixed_keeps_filter_above(self, manager):
        """AND(left_pred, mixed_pred) should push left down, keep mixed above."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        left_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        mixed_pred = scalar_function(COMPARISON_URN, "equal", [column(0), column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [left_pred, mixed_pred])
        filtered = pb.filter(crossed, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        # Mixed pred remains as filter above cross
        assert get_rel_type(root_input) == "filter"
        inner_cross = root_input.filter.input
        assert get_rel_type(inner_cross) == "cross"
        # Left pred was pushed down
        assert get_rel_type(inner_cross.cross.left) == "filter"
        assert get_rel_type(inner_cross.cross.right) == "read"

    def test_non_and_mixed_not_pushed(self, manager):
        """A non-AND mixed condition should still not be pushed."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)

        eq_mixed = scalar_function(COMPARISON_URN, "equal", [column(0), column(2)])
        filtered = pb.filter(crossed, eq_mixed)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "cross"


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


class TestFilterPushdownProject:
    def test_pushdown_filter_on_passthrough_field(self, manager):
        """Filter on a pass-through field should be pushed below the project."""
        read = make_read("my_table", ["a", "b"])
        projected = pb.project(read, [column(0)])
        filtered = pb.filter(projected, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        assert get_rel_type(root_input.project.input) == "filter"
        assert get_rel_type(root_input.project.input.filter.input) == "read"

    def test_no_pushdown_filter_on_computed_field(self, manager):
        """Filter on a computed expression field should NOT be pushed down."""
        read = make_read("my_table", ["a", "b"])
        projected = pb.project(read, [column(0)])
        # Field 2 is the computed expression (indices 0,1 from read, 2 from expression)
        filtered = pb.filter(projected, column(2))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "project"

    def test_no_pushdown_with_emit_mapping(self, manager):
        """Filter on a project with emit mapping (select) should NOT be pushed down."""
        read = make_read("my_table", ["a", "b"])
        selected = pb.select(read, [column(0)])
        filtered = pb.filter(selected, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "project"

    def test_split_and_push_passthrough_only(self, manager):
        """AND(pred_on_input, pred_on_computed) should push only the input pred below."""
        read = make_read("my_table", ["a", "b"])
        projected = pb.project(read, [column(0)])
        # col(0) references input field, col(2) references computed field
        input_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        computed_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [input_pred, computed_pred])
        filtered = pb.filter(projected, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        # Computed pred stays above
        assert get_rel_type(root_input) == "filter"
        # Input pred pushed below project
        assert get_rel_type(root_input.filter.input) == "project"
        assert get_rel_type(root_input.filter.input.project.input) == "filter"
        assert get_rel_type(root_input.filter.input.project.input.filter.input) == "read"

    def test_pushdown_filter_through_project_and_cross(self, manager):
        """Filter(Project(Cross(L,R))) with left-only predicate should push through both."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])
        crossed = pb.cross(left, right)
        projected = pb.project(crossed, [column(0)])
        filtered = pb.filter(projected, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        inner = root_input.project.input
        assert get_rel_type(inner) == "cross"
        assert get_rel_type(inner.cross.left) == "filter"
        assert get_rel_type(inner.cross.right) == "read"


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
