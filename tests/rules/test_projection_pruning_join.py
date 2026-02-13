from substrait.algebra_pb2 import JoinRel
from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, materialize, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"


class TestJoinProjectionPruning:
    def test_emit_left_only_prunes_right(self, manager):
        """Join(emit=[0], expr=equal(col(0),col(2)), L([a,b]), R([c,d]))
        should prune right to only field needed by expression."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )

        plan = materialize(joined)
        # Emit only field 0 (left.a).
        plan.relations[0].root.input.join.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        join = root_input.join

        # Left needs fields {0} (emit) + {0} (expression col(0)) = {0}.
        # Left has 2 fields, so prune to [0].
        assert join.left.read.common.HasField("emit")
        assert list(join.left.read.common.emit.output_mapping) == [0]

        # Right needs fields {0} (expression col(2) → right local 0).
        # Right has 2 fields, so prune to [0].
        assert join.right.read.common.HasField("emit")
        assert list(join.right.read.common.emit.output_mapping) == [0]

    def test_emit_right_only_prunes_left(self, manager):
        """Join(emit=[3], expr=equal(col(0),col(2)), L([a,b]), R([c,d]))
        should prune left to only field needed by expression."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )

        plan = materialize(joined)
        # Emit only field 3 (right.d).
        plan.relations[0].root.input.join.common.emit.output_mapping[:] = [3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        join = root_input.join

        # Left needs {0} from expression only. Prune to [0].
        assert join.left.read.common.HasField("emit")
        assert list(join.left.read.common.emit.output_mapping) == [0]

        # Right needs {1} (emit: 3-2=1) + {0} (expression: 2-2=0). Prune to [0,1] = all fields, no prune.
        assert not join.right.read.common.HasField("emit")

        # Emit should be remapped: old 3 → new_left_count(1) + right_mapping[1](1) = 2.
        assert list(join.common.emit.output_mapping) == [2]

    def test_both_sides_pruned(self, manager):
        """Join with 3-field inputs, emit + expression only need subset from each side."""
        left = make_read("l", ["a", "b", "c"])
        right = make_read("r", ["d", "e", "f"])
        # Expression references col(0) and col(3) (left.a and right.d).
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(3)]),
            JoinRel.JOIN_TYPE_INNER,
        )

        plan = materialize(joined)
        # Emit fields 2 (left.c) and 5 (right.f).
        plan.relations[0].root.input.join.common.emit.output_mapping[:] = [2, 5]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        join = root_input.join

        # Left needs {0, 2} (expr col(0) + emit 2). Prune to [0, 2].
        assert list(join.left.read.common.emit.output_mapping) == [0, 2]

        # Right needs {0, 2} (expr col(3)→0 + emit col(5)→2). Prune to [0, 2].
        assert list(join.right.read.common.emit.output_mapping) == [0, 2]

        # Expression remapped: col(0)→0, col(3)→new_left(2)+right_map[0](0)=2.
        expr = join.expression.scalar_function
        assert expr.arguments[0].value.selection.direct_reference.struct_field.field == 0
        assert expr.arguments[1].value.selection.direct_reference.struct_field.field == 2

        # Emit remapped: old 2→left_map[2]=1, old 5→new_left(2)+right_map[2](1)=3.
        assert list(join.common.emit.output_mapping) == [1, 3]

    def test_all_fields_needed_no_change(self, manager):
        """Join(emit=[0,1,2,3], ...) — all fields needed, no pruning."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )

        plan = materialize(joined)
        plan.relations[0].root.input.join.common.emit.output_mapping[:] = [0, 1, 2, 3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert not root_input.join.left.read.common.HasField("emit")
        assert not root_input.join.right.read.common.HasField("emit")

    def test_join_without_emit_no_change(self, manager):
        """Join without emit should not trigger the rule."""
        left = make_read("l", ["a", "b"])
        right = make_read("r", ["c", "d"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(2)]),
            JoinRel.JOIN_TYPE_INNER,
        )
        result = optimize(manager, joined)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "join"
        assert not root_input.join.left.read.common.HasField("emit")
        assert not root_input.join.right.read.common.HasField("emit")

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        left = make_read("l", ["a", "b", "c"])
        right = make_read("r", ["d", "e", "f"])
        joined = pb.join(
            left,
            right,
            scalar_function(COMPARISON_URN, "equal", [column(0), column(3)]),
            JoinRel.JOIN_TYPE_INNER,
        )

        plan = materialize(joined)
        plan.relations[0].root.input.join.common.emit.output_mapping[:] = [2, 5]

        first_bytes = manager.optimize(plan.SerializeToString())
        second_bytes = manager.optimize(first_bytes)
        assert first_bytes == second_bytes
