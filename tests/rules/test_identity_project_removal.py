from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, materialize, optimize

ARITHMETIC_URN = "extension:io.substrait:functions_arithmetic"


class TestIdentityProjectRemoval:
    def test_select_all_columns_removed(self, manager):
        """select(read([a,b]), [col(0), col(1)]) is identity — project removed."""
        read = make_read("t", ["a", "b"])
        selected = pb.select(read, [column(0), column(1)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"
        assert not root_input.read.common.HasField("emit")

    def test_select_subset_not_removed(self, manager):
        """select(read([a,b,c]), [col(0)]) is not identity — project pruned but
        then removed as identity after pruning leaves read with emit."""
        read = make_read("t", ["a", "b", "c"])
        selected = pb.select(read, [column(0)])
        result = optimize(manager, selected)

        # The identity project is removed, leaving the pruned read.
        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"
        assert list(root_input.read.common.emit.output_mapping) == [0]

    def test_project_with_no_expressions_no_emit_removed(self, manager):
        """Project(input, expressions=[], no emit) is identity — removed."""
        read = make_read("t", ["a", "b"])
        projected = pb.project(read, [])

        plan = materialize(projected)
        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"

    def test_project_with_computed_expression_kept(self, manager):
        """Project with a non-identity expression should not be removed."""
        read = make_read("t", ["a", "b"])
        add_expr = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        projected = pb.project(read, [add_expr])
        result = optimize(manager, projected)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        assert len(root_input.project.expressions) == 1

    def test_project_with_reordering_emit_kept(self, manager):
        """Project with emit that reorders fields is not identity — kept."""
        read = make_read("t", ["a", "b", "c"])
        projected = pb.project(read, [])

        plan = materialize(projected)
        # Emit reorders: [2, 1, 0] instead of identity [0, 1, 2].
        plan.relations[0].root.input.project.common.emit.output_mapping[:] = [2, 1, 0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        assert list(root_input.project.common.emit.output_mapping) == [2, 1, 0]

    def test_idempotent(self, manager):
        """Identity project removal should be idempotent."""
        read = make_read("t", ["a", "b", "c"])
        selected = pb.select(read, [column(0), column(1), column(2)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
