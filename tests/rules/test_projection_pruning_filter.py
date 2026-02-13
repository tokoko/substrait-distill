from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, materialize, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
ARITHMETIC_URN = "extension:io.substrait:functions_arithmetic"


class TestFilterProjectionPruning:
    def test_filter_with_emit_prunes_unused_fields(self, manager):
        """Filter(emit=[0], condition=col(0)>5, Read([a,b,c,d]))
        should prune b,c,d from the read."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)

        # Manually add emit to the filter to simulate what ProjectRel pruning does.
        plan = materialize(filtered)
        plan.relations[0].root.input.filter.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        filt = root_input.filter

        # Input should have emit pruning to just field 0.
        assert filt.input.read.common.HasField("emit")
        input_emit = list(filt.input.read.common.emit.output_mapping)
        assert input_emit == [0]

        # Filter emit should be [0].
        assert list(filt.common.emit.output_mapping) == [0]

    def test_filter_condition_references_field_outside_emit(self, manager):
        """Filter(emit=[0], condition=col(2)>5, Read([a,b,c,d]))
        should keep fields 0 and 2, prune 1 and 3."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        filtered = pb.filter(read, pred)

        plan = materialize(filtered)
        plan.relations[0].root.input.filter.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        filt = root_input.filter

        # Input emit should select fields 0 and 2.
        assert filt.input.read.common.HasField("emit")
        input_emit = list(filt.input.read.common.emit.output_mapping)
        assert input_emit == [0, 2]

        # Condition should be remapped: col(2) → col(1).
        cond = filt.condition
        assert cond.scalar_function.arguments[0].value.selection.direct_reference.struct_field.field == 1

        # Filter emit: old [0] → new [0] (since old_to_new[0]=0).
        assert list(filt.common.emit.output_mapping) == [0]

    def test_filter_all_fields_needed_no_change(self, manager):
        """Filter(emit=[0,1], condition=col(0)>5, Read([a,b]))
        — all fields needed, no pruning."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)

        plan = materialize(filtered)
        plan.relations[0].root.input.filter.common.emit.output_mapping[:] = [0, 1]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        # Input should NOT have emit (no pruning needed).
        assert not root_input.filter.input.read.common.HasField("emit")

    def test_filter_without_emit_no_change(self, manager):
        """Filter without emit should not trigger the rule."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        # Input should NOT have emit.
        assert not root_input.filter.input.read.common.HasField("emit")

    def test_cascading_project_then_filter_pruning(self, manager):
        """select(filter(read([a,b,c,d]), col(2)>5), [col(0)])
        — ProjectRel pruning adds emit to filter, then FilterRel pruning
        propagates to the read. Identity project is removed."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        filtered = pb.filter(read, pred)
        selected = pb.select(filtered, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — filter is now the root.
        assert get_rel_type(root_input) == "filter"
        filt = root_input.filter

        # The read should have emit from cascading pruning.
        assert get_rel_type(filt.input) == "read"
        assert filt.input.read.common.HasField("emit")
        assert list(filt.input.read.common.emit.output_mapping) == [0, 2]

    def test_input_already_has_emit(self, manager):
        """Filter with emit over an input that already has emit should compose correctly."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)

        plan = materialize(filtered)
        # Input read has emit [0, 2, 3] (skipping field 1).
        plan.relations[0].root.input.filter.input.read.common.emit.output_mapping[:] = [0, 2, 3]
        # Filter emit selects only field 0 from those 3 output fields.
        plan.relations[0].root.input.filter.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        filt = root_input.filter

        # Input emit should be composed: needed=[0] from [0,2,3] → [0].
        input_emit = list(filt.input.read.common.emit.output_mapping)
        assert input_emit == [0]

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        read = make_read("t", ["a", "b", "c", "d"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(2)])
        filtered = pb.filter(read, pred)
        selected = pb.select(filtered, [column(0)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
