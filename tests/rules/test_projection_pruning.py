from substrait.builders import plan as pb
from substrait.builders.extended_expression import column, scalar_function

from ..conftest import get_rel_type, make_read, materialize, optimize

ARITHMETIC_URN = "extension:io.substrait:functions_arithmetic"


class TestProjectionPushdown:
    def test_select_single_column_prunes_unused(self, manager):
        """select(read([a,b,c,d]), [column(0)]) should prune fields b,c,d.
        Identity project is removed, leaving just the pruned read."""
        read = make_read("t", ["a", "b", "c", "d"])
        selected = pb.select(read, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — just the read with emit remains.
        assert get_rel_type(root_input) == "read"
        assert root_input.read.common.HasField("emit")
        assert list(root_input.read.common.emit.output_mapping) == [0]

    def test_select_two_columns_prunes_middle(self, manager):
        """select(read([a,b,c,d]), [column(0), column(3)]) should prune b,c.
        Identity project is removed, leaving just the pruned read."""
        read = make_read("t", ["a", "b", "c", "d"])
        selected = pb.select(read, [column(0), column(3)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — just the read with emit remains.
        assert get_rel_type(root_input) == "read"
        assert root_input.read.common.HasField("emit")
        assert list(root_input.read.common.emit.output_mapping) == [0, 3]

    def test_select_expression_prunes_unreferenced(self, manager):
        """select(read([a,b,c,d]), [add(col(0), col(1))]) should prune c,d."""
        read = make_read("t", ["a", "b", "c", "d"])
        add_expr = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        selected = pb.select(read, [add_expr])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        proj = root_input.project

        # Input emit should select only needed fields [0, 1].
        assert proj.input.read.common.HasField("emit")
        input_emit = list(proj.input.read.common.emit.output_mapping)
        assert input_emit == [0, 1]

    def test_all_fields_referenced_no_change(self, manager):
        """select(read([a,b]), [column(0), column(1)]) — all fields needed.
        Identity project selecting all fields is removed entirely."""
        read = make_read("t", ["a", "b"])
        selected = pb.select(read, [column(0), column(1)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — just the read remains, no emit needed.
        assert get_rel_type(root_input) == "read"
        assert not root_input.read.common.HasField("emit")

    def test_project_without_emit_no_change(self, manager):
        """project(read, [expr]) without emit should not trigger the rule."""
        read = make_read("t", ["a", "b", "c", "d"])
        projected = pb.project(read, [column(0)])
        result = optimize(manager, projected)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        proj = root_input.project

        # No emit on project → rule doesn't fire → input should not have emit.
        assert not proj.input.read.common.HasField("emit")

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        read = make_read("t", ["a", "b", "c", "d"])
        selected = pb.select(read, [column(0)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes

    def test_idempotent_two_columns(self, manager):
        """Two-column select optimization should be idempotent."""
        read = make_read("t", ["a", "b", "c", "d"])
        selected = pb.select(read, [column(0), column(3)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes

    def test_unused_expressions_dropped(self, manager):
        """Project(emit=[0, 3], input=[a,b,c], exprs=[a+b, c*2])
        should drop c*2 (expr 1) since emit only references expr 0."""
        read = make_read("t", ["a", "b", "c"])
        add_expr = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        mul_expr = scalar_function(ARITHMETIC_URN, "multiply", [column(2), column(2)])
        projected = pb.project(read, [add_expr, mul_expr])

        plan = materialize(projected)
        # Output: [a(0), b(1), c(2), a+b(3), c*2(4)]. Emit selects a and a+b.
        plan.relations[0].root.input.project.common.emit.output_mapping[:] = [0, 3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        proj = root_input.project

        # Expression c*2 should be dropped — only a+b remains.
        assert len(proj.expressions) == 1

        # Input should be pruned: only a,b needed (c no longer referenced).
        assert proj.input.read.common.HasField("emit")
        assert list(proj.input.read.common.emit.output_mapping) == [0, 1]

        # Emit remapped: 0→0 (input a), 3→new_input(2)+expr_map[0](0)=2.
        assert list(proj.common.emit.output_mapping) == [0, 2]

    def test_only_expressions_pruned_input_unchanged(self, manager):
        """Project(emit=[0, 1, 2], input=[a,b], exprs=[a+b, a*b])
        should drop a*b but input stays (all input fields needed)."""
        read = make_read("t", ["a", "b"])
        add_expr = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        mul_expr = scalar_function(ARITHMETIC_URN, "multiply", [column(0), column(1)])
        projected = pb.project(read, [add_expr, mul_expr])

        plan = materialize(projected)
        # Output: [a(0), b(1), a+b(2), a*b(3)]. Emit selects a, b, a+b.
        plan.relations[0].root.input.project.common.emit.output_mapping[:] = [0, 1, 2]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        proj = root_input.project

        # a*b dropped, only a+b remains.
        assert len(proj.expressions) == 1

        # All input fields needed — no input pruning.
        assert not proj.input.read.common.HasField("emit")

        # Emit: 0→0, 1→1, 2→2+0=2.
        assert list(proj.common.emit.output_mapping) == [0, 1, 2]

    def test_middle_expression_dropped(self, manager):
        """Project with 3 expressions, emit skips the middle one.
        Should drop middle expression and remap emit."""
        read = make_read("t", ["a", "b"])
        e0 = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        e1 = scalar_function(ARITHMETIC_URN, "multiply", [column(0), column(1)])
        e2 = scalar_function(ARITHMETIC_URN, "subtract", [column(0), column(1)])
        projected = pb.project(read, [e0, e1, e2])

        plan = materialize(projected)
        # Output: [a(0), b(1), e0(2), e1(3), e2(4)]. Emit selects e0 and e2.
        plan.relations[0].root.input.project.common.emit.output_mapping[:] = [2, 4]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "project"
        proj = root_input.project

        # e1 dropped, e0 and e2 remain.
        assert len(proj.expressions) == 2

        # Emit: e0 was expr 0→new 0, e2 was expr 2→new 1.
        # new_input_count(2) + new_expr_idx.
        assert list(proj.common.emit.output_mapping) == [2, 3]

    def test_cascading_drops_inner_project_expressions(self, manager):
        """select(project(read([a,b,c]), [a+b, c*2]), [col(0)])
        — outer select becomes identity and is removed. Inner project's
        unused expressions are dropped, then inner becomes identity too."""
        read = make_read("t", ["a", "b", "c"])
        add_expr = scalar_function(ARITHMETIC_URN, "add", [column(0), column(1)])
        mul_expr = scalar_function(ARITHMETIC_URN, "multiply", [column(2), column(2)])
        inner = pb.project(read, [add_expr, mul_expr])
        selected = pb.select(inner, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Both identity projects removed — just the pruned read remains.
        assert get_rel_type(root_input) == "read"
        assert root_input.read.common.HasField("emit")
        assert list(root_input.read.common.emit.output_mapping) == [0]
