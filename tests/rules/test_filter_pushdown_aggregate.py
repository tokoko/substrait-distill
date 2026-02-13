from substrait.builders import plan as pb
from substrait.builders.extended_expression import (
    aggregate_function,
    column,
    scalar_function,
)

from ..conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"
ARITHMETIC_URN = "extension:io.substrait:functions_arithmetic"


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
