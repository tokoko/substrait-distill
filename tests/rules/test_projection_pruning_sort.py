from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_read, materialize, optimize


class TestSortProjectionPruning:
    def test_emit_prunes_unused_fields(self, manager):
        """Sort(emit=[0], sort_by=col(0), Read([a,b,c,d]))
        should prune b,c,d from the read."""
        read = make_read("t", ["a", "b", "c", "d"])
        sorted_plan = pb.sort(read, [column(0)])

        plan = materialize(sorted_plan)
        plan.relations[0].root.input.sort.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        sort = root_input.sort

        # Input should have emit pruning to just field 0.
        assert sort.input.read.common.HasField("emit")
        assert list(sort.input.read.common.emit.output_mapping) == [0]

        # Sort emit should be [0].
        assert list(sort.common.emit.output_mapping) == [0]

    def test_sort_expr_references_field_outside_emit(self, manager):
        """Sort(emit=[0], sort_by=col(2), Read([a,b,c,d]))
        should keep fields 0 and 2, prune 1 and 3."""
        read = make_read("t", ["a", "b", "c", "d"])
        sorted_plan = pb.sort(read, [column(2)])

        plan = materialize(sorted_plan)
        plan.relations[0].root.input.sort.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        sort = root_input.sort

        # Input emit should select fields 0 and 2.
        assert sort.input.read.common.HasField("emit")
        assert list(sort.input.read.common.emit.output_mapping) == [0, 2]

        # Sort expression should be remapped: col(2) -> col(1).
        sort_expr = sort.sorts[0].expr
        assert sort_expr.selection.direct_reference.struct_field.field == 1

        # Sort emit: old [0] -> new [0].
        assert list(sort.common.emit.output_mapping) == [0]

    def test_all_fields_needed_no_change(self, manager):
        """Sort(emit=[0,1], sort_by=col(0), Read([a,b]))
        -- all fields needed, no pruning."""
        read = make_read("t", ["a", "b"])
        sorted_plan = pb.sort(read, [column(0)])

        plan = materialize(sorted_plan)
        plan.relations[0].root.input.sort.common.emit.output_mapping[:] = [0, 1]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        assert not root_input.sort.input.read.common.HasField("emit")

    def test_sort_without_emit_no_change(self, manager):
        """Sort without emit should not trigger the rule."""
        read = make_read("t", ["a", "b", "c", "d"])
        sorted_plan = pb.sort(read, [column(0)])
        result = optimize(manager, sorted_plan)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "sort"
        assert not root_input.sort.input.read.common.HasField("emit")

    def test_cascading_project_then_sort_pruning(self, manager):
        """select(sort(read([a,b,c,d]), col(2)), [col(0)])
        -- ProjectRel pruning adds emit to sort, then SortRel pruning
        propagates to the read. Identity project is removed."""
        read = make_read("t", ["a", "b", "c", "d"])
        sorted_plan = pb.sort(read, [column(2)])
        selected = pb.select(sorted_plan, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — sort is now the root.
        assert get_rel_type(root_input) == "sort"
        sort = root_input.sort

        assert get_rel_type(sort.input) == "read"
        assert sort.input.read.common.HasField("emit")
        assert list(sort.input.read.common.emit.output_mapping) == [0, 2]

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        read = make_read("t", ["a", "b", "c", "d"])
        sorted_plan = pb.sort(read, [column(2)])
        selected = pb.select(sorted_plan, [column(0)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
