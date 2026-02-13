from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


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
        """Filter on a project with emit mapping (select) should NOT be pushed down.
        The identity project is removed after pruning, leaving filter on read."""
        read = make_read("my_table", ["a", "b"])
        selected = pb.select(read, [column(0)])
        filtered = pb.filter(selected, column(0))
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        # Identity project removed; filter sits directly on the pruned read.
        assert get_rel_type(root_input.filter.input) == "read"

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
