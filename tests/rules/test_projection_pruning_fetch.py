from substrait.builders import plan as pb
from substrait.builders.extended_expression import column

from ..conftest import get_rel_type, make_fetch, make_read, materialize, optimize


class TestFetchProjectionPruning:
    def test_emit_prunes_unused_fields(self, manager):
        """Fetch(emit=[0], Read([a,b,c,d])) should prune b,c,d from the read."""
        read = make_read("t", ["a", "b", "c", "d"])
        fetched = make_fetch(read, 0, 10)

        plan = materialize(fetched)
        plan.relations[0].root.input.fetch.common.emit.output_mapping[:] = [0]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        fetch = root_input.fetch

        # Input should have emit pruning to just field 0.
        assert fetch.input.read.common.HasField("emit")
        assert list(fetch.input.read.common.emit.output_mapping) == [0]

        # Fetch emit should be [0].
        assert list(fetch.common.emit.output_mapping) == [0]

    def test_multiple_fields_pruned(self, manager):
        """Fetch(emit=[0, 3], Read([a,b,c,d])) should prune b,c."""
        read = make_read("t", ["a", "b", "c", "d"])
        fetched = make_fetch(read, 0, 10)

        plan = materialize(fetched)
        plan.relations[0].root.input.fetch.common.emit.output_mapping[:] = [0, 3]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        fetch = root_input.fetch

        assert fetch.input.read.common.HasField("emit")
        assert list(fetch.input.read.common.emit.output_mapping) == [0, 3]

        # Emit remapped: 0->0, 3->1.
        assert list(fetch.common.emit.output_mapping) == [0, 1]

    def test_all_fields_needed_no_change(self, manager):
        """Fetch(emit=[0,1], Read([a,b])) -- all fields needed, no pruning."""
        read = make_read("t", ["a", "b"])
        fetched = make_fetch(read, 0, 10)

        plan = materialize(fetched)
        plan.relations[0].root.input.fetch.common.emit.output_mapping[:] = [0, 1]

        result_bytes = manager.optimize(plan.SerializeToString())
        from substrait.proto import Plan

        result = Plan()
        result.ParseFromString(result_bytes)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        assert not root_input.fetch.input.read.common.HasField("emit")

    def test_fetch_without_emit_no_change(self, manager):
        """Fetch without emit should not trigger the rule."""
        read = make_read("t", ["a", "b", "c", "d"])
        fetched = make_fetch(read, 0, 10)
        result = optimize(manager, fetched)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "fetch"
        assert not root_input.fetch.input.read.common.HasField("emit")

    def test_cascading_project_then_fetch_pruning(self, manager):
        """select(fetch(read([a,b,c,d]), 0, 10), [col(0)])
        -- ProjectRel pruning adds emit to fetch, then FetchRel pruning
        propagates to the read. Identity project is removed."""
        read = make_read("t", ["a", "b", "c", "d"])
        fetched = make_fetch(read, 0, 10)
        selected = pb.select(fetched, [column(0)])
        result = optimize(manager, selected)

        root_input = result.relations[0].root.input
        # Identity project removed — fetch is now the root.
        assert get_rel_type(root_input) == "fetch"
        fetch = root_input.fetch

        assert get_rel_type(fetch.input) == "read"
        assert fetch.input.read.common.HasField("emit")
        assert list(fetch.input.read.common.emit.output_mapping) == [0]

    def test_idempotent(self, manager):
        """Running optimization twice should produce the same result."""
        read = make_read("t", ["a", "b", "c", "d"])
        fetched = make_fetch(read, 0, 10)
        selected = pb.select(fetched, [column(0)])
        first = optimize(manager, selected)
        second_bytes = manager.optimize(first.SerializeToString())
        assert first.SerializeToString() == second_bytes
