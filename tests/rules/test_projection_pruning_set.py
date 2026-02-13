from substrait.algebra_pb2 import SetRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_read, materialize, optimize


class TestSetProjectionPruning:
    def test_emit_prunes_unused_fields(self, manager):
        """Set(emit=[0], A([a,b,c]), B([a,b,c])) should prune b,c from both inputs."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)

        plan = materialize(union)
        plan.relations[0].root.input.set.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        set_rel = root_input.set

        # Both inputs should be pruned to just field 0.
        for inp in set_rel.inputs:
            assert inp.read.common.HasField("emit")
            assert list(inp.read.common.emit.output_mapping) == [0]

        # Set emit remapped: 0 -> 0.
        assert list(set_rel.common.emit.output_mapping) == [0]

    def test_emit_selects_subset(self, manager):
        """Set(emit=[0, 2], A([a,b,c]), B([a,b,c])) should prune b from both."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)

        plan = materialize(union)
        plan.relations[0].root.input.set.common.emit.output_mapping[:] = [0, 2]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        set_rel = root_input.set

        for inp in set_rel.inputs:
            assert inp.read.common.HasField("emit")
            assert list(inp.read.common.emit.output_mapping) == [0, 2]

        # Emit remapped: 0->0, 2->1.
        assert list(set_rel.common.emit.output_mapping) == [0, 1]

    def test_three_inputs_all_pruned(self, manager):
        """Set with 3 inputs — all should be pruned."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        c = make_read("c", ["x", "y", "z"])
        union = pb.set([a, b, c], SetRel.SET_OP_UNION_ALL)

        plan = materialize(union)
        plan.relations[0].root.input.set.common.emit.output_mapping[:] = [1]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        set_rel = root_input.set

        assert len(set_rel.inputs) == 3
        for inp in set_rel.inputs:
            assert inp.read.common.HasField("emit")
            assert list(inp.read.common.emit.output_mapping) == [1]

        assert list(set_rel.common.emit.output_mapping) == [0]

    def test_all_fields_needed_no_change(self, manager):
        """Set(emit=[0,1], A([a,b]), B([a,b])) — all fields needed, no pruning."""
        a = make_read("a", ["x", "y"])
        b = make_read("b", ["x", "y"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)

        plan = materialize(union)
        plan.relations[0].root.input.set.common.emit.output_mapping[:] = [0, 1]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        for inp in root_input.set.inputs:
            assert not inp.read.common.HasField("emit")

    def test_set_without_emit_no_change(self, manager):
        """Set without emit should not trigger the rule."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)
        result = optimize(manager, union)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "set"
        for inp in root_input.set.inputs:
            assert not inp.read.common.HasField("emit")

    def test_cascading_project_then_set_pruning(self, manager):
        """select(union(A([a,b,c]), B([a,b,c])), [col(0)])
        — ProjectRel adds emit to set, then SetRel pruning propagates to inputs.
        Identity project is removed."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)
        selected = pb.select(union, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — set is now the root.
        assert get_rel_type(root_input) == "set"
        for inp in root_input.set.inputs:
            assert inp.read.common.HasField("emit")
            assert list(inp.read.common.emit.output_mapping) == [0]

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        a = make_read("a", ["x", "y", "z"])
        b = make_read("b", ["x", "y", "z"])
        union = pb.set([a, b], SetRel.SET_OP_UNION_ALL)
        selected = pb.select(union, [column(0)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
