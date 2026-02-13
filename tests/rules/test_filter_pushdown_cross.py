from substrait.algebra_pb2 import JoinRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_filter_over_cross, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"


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
