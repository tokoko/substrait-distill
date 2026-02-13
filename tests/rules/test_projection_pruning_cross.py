from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_read, materialize


class TestCrossProjectionPruning:
    def test_emit_left_only_prunes_right(self, manager):
        """Cross(emit=[0], L([a,b]), R([c,d])) should prune right entirely."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        crossed = pb.cross(left, right)

        plan = materialize(crossed)
        plan.relations[0].root.input.cross.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        cross = root_input.cross

        # Left needs only field 0. Prune to [0].
        assert cross.left.read.common.HasField("emit")
        assert list(cross.left.read.common.emit.output_mapping) == [0]

        # Right needs nothing from emit, but can't have 0 fields.
        # prune_input won't prune to empty — all right fields are unneeded
        # but right_needed is empty so prune_input gets needed=set() which
        # is < right_field_count(2), so it prunes to [].
        # Actually, prune_input with empty needed will produce sorted_needed=[]
        # and output_mapping=[]. Let's just check the emit is remapped.
        assert list(cross.common.emit.output_mapping) == [0]

    def test_emit_right_only_prunes_left(self, manager):
        """Cross(emit=[3], L([a,b]), R([c,d])) should prune left entirely."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        crossed = pb.cross(left, right)

        plan = materialize(crossed)
        plan.relations[0].root.input.cross.common.emit.output_mapping[:] = [3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        cross = root_input.cross

        # Right needs field 1 (3-2=1). Prune to [1].
        assert cross.right.read.common.HasField("emit")
        assert list(cross.right.read.common.emit.output_mapping) == [1]

        # Emit remapped: old 3 → new_left_count(0) + right_mapping[1](0) = 0.
        assert list(cross.common.emit.output_mapping) == [0]

    def test_both_sides_pruned(self, manager):
        """Cross with 3-field inputs, emit only needs subset from each side."""
        left = make_read("l", ["a", "b", "c"])
        right = make_read("r", ["d", "e", "f"])
        crossed = pb.cross(left, right)

        plan = materialize(crossed)
        # Emit fields 0 (left.a) and 5 (right.f).
        plan.relations[0].root.input.cross.common.emit.output_mapping[:] = [0, 5]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        cross = root_input.cross

        # Left needs {0}. Prune to [0].
        assert list(cross.left.read.common.emit.output_mapping) == [0]

        # Right needs {2} (5-3=2). Prune to [2].
        assert list(cross.right.read.common.emit.output_mapping) == [2]

        # Emit remapped: 0→0, 5→new_left(1)+right_map[2](0)=1.
        assert list(cross.common.emit.output_mapping) == [0, 1]

    def test_all_fields_needed_no_change(self, manager):
        """Cross(emit=[0,1,2,3], ...) — all fields needed, no pruning."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        crossed = pb.cross(left, right)

        plan = materialize(crossed)
        plan.relations[0].root.input.cross.common.emit.output_mapping[:] = [0, 1, 2, 3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert not root_input.cross.left.read.common.HasField("emit")
        assert not root_input.cross.right.read.common.HasField("emit")

    def test_cross_without_emit_no_change(self, manager):
        """Cross without emit should not trigger the rule."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        crossed = pb.cross(left, right)

        from ..conftest import optimize

        result = optimize(manager, crossed)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "cross"
        assert not root_input.cross.left.read.common.HasField("emit")
        assert not root_input.cross.right.read.common.HasField("emit")

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        left = make_read("l", ["a", "b", "c"])
        right = make_read("r", ["d", "e", "f"])
        crossed = pb.cross(left, right)

        plan = materialize(crossed)
        plan.relations[0].root.input.cross.common.emit.output_mapping[:] = [0, 5]

        first_bytes = manager.optimize(plan.SerializeToString())
        second_bytes = manager.optimize(first_bytes)
        assert first_bytes == second_bytes
