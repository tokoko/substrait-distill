from substrait.algebra_pb2 import JoinRel, SetRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import (
    aggregate_function,
    column,
    scalar_function,
)
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
ARITHMETIC_URN = "extension:io.substrait:functions_arithmetic"


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

    def test_mixed_fields_converts_to_join(self, manager):
        """Filter referencing fields from both sides converts cross to inner join."""
        left = make_read("left_table", ["a", "b"])
        right = make_read("right_table", ["c", "d"])

        crossed = pb.cross(left, right)
        filtered = pb.filter(
            crossed,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
        )
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert root_input.join.type == JoinRel.JOIN_TYPE_INNER
        assert root_input.join.HasField("expression")


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


class TestFilterPushdownAggregate:
    def test_push_filter_on_grouping_column(self, manager):
        """Filter on grouping key column should be pushed below aggregate."""
        read = make_read("my_table", ["a", "b", "c"])
        agg = pb.aggregate(
            read,
            grouping_expressions=[column(0)],
            measures=[aggregate_function(ARITHMETIC_URN, "sum", [column(1)])],
        )
        # Filter on output field 0 = grouping key 'a' = input field 0
        filtered = pb.filter(agg, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "aggregate"
        assert get_rel_type(root_input.aggregate.input) == "filter"
        assert get_rel_type(root_input.aggregate.input.filter.input) == "read"

    def test_filter_on_measure_not_pushed(self, manager):
        """Filter on measure column should NOT be pushed below aggregate."""
        read = make_read("my_table", ["a", "b", "c"])
        agg = pb.aggregate(
            read,
            grouping_expressions=[column(0)],
            measures=[aggregate_function(ARITHMETIC_URN, "sum", [column(1)])],
        )
        # Filter on output field 1 = measure (sum)
        filtered = pb.filter(agg, column(1))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "aggregate"

    def test_conjunction_split_push_grouping_keep_measure(self, manager):
        """AND(pred_on_grouping, pred_on_measure) splits correctly."""
        read = make_read("my_table", ["a", "b", "c"])
        agg = pb.aggregate(
            read,
            grouping_expressions=[column(0)],
            measures=[aggregate_function(ARITHMETIC_URN, "sum", [column(1)])],
        )
        grouping_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        measure_pred = scalar_function(COMPARISON_URN, "is_not_null", [column(1)])
        and_cond = scalar_function(BOOLEAN_URN, "and", [grouping_pred, measure_pred])
        filtered = pb.filter(agg, and_cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        # Measure pred stays above
        assert get_rel_type(root_input) == "filter"
        # Grouping pred pushed below
        assert get_rel_type(root_input.filter.input) == "aggregate"
        assert get_rel_type(root_input.filter.input.aggregate.input) == "filter"

    def test_multiple_grouping_columns_index_mapping(self, manager):
        """Multiple grouping keys with non-trivial index mapping."""
        read = make_read("my_table", ["a", "b", "c", "d"])
        # Grouping on columns 1 and 2 (not 0 and 1)
        agg = pb.aggregate(
            read,
            grouping_expressions=[column(1), column(2)],
            measures=[aggregate_function(ARITHMETIC_URN, "sum", [column(3)])],
        )
        # Filter on output field 0 = grouping key at input field 1
        filtered = pb.filter(agg, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "aggregate"
        pushed = root_input.aggregate.input
        assert get_rel_type(pushed) == "filter"
        # Verify the field index was remapped from 0 (output) to 1 (input)
        cond = pushed.filter.condition
        assert cond.WhichOneof("rex_type") == "selection"
        assert cond.selection.direct_reference.struct_field.field == 1


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
