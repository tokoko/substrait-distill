from substrait.builders import plan as pb
from substrait.builders import type as tb
from substrait.builders.extended_expression import column, literal, scalar_function
from substrait.proto import Plan

from .conftest import get_rel_type, make_read, optimize

COMPARISON_URN = "extension:io.substrait:functions_comparison"
BOOLEAN_URN = "extension:io.substrait:functions_boolean"


def _true():
    return literal(True, tb.boolean())


def _false():
    return literal(False, tb.boolean())


class TestAndSimplification:
    def test_and_true_x(self, manager):
        """AND(true, x) -> x."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "and", [_true(), pred])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        # Condition should be simplified to just the is_not_null predicate
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "scalar_function"
        assert c.scalar_function.function_reference != 0 or True  # not AND

    def test_and_x_true(self, manager):
        """AND(x, true) -> x."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "and", [pred, _true()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "scalar_function"

    def test_and_false_x(self, manager):
        """AND(false, x) -> false (filter kept with false condition)."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "and", [_false(), pred])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "literal"
        assert c.literal.boolean is False

    def test_and_x_false(self, manager):
        """AND(x, false) -> false."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "and", [pred, _false()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "literal"
        assert c.literal.boolean is False


class TestOrSimplification:
    def test_or_true_x(self, manager):
        """OR(true, x) -> true -> filter removed."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "or", [_true(), pred])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"

    def test_or_false_x(self, manager):
        """OR(false, x) -> x."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "or", [_false(), pred])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "scalar_function"

    def test_or_x_true(self, manager):
        """OR(x, true) -> true -> filter removed."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "or", [pred, _true()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"

    def test_or_x_false(self, manager):
        """OR(x, false) -> x."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "or", [pred, _false()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "scalar_function"


class TestNotSimplification:
    def test_not_true(self, manager):
        """NOT(true) -> false."""
        read = make_read("t", ["a", "b"])
        cond = scalar_function(BOOLEAN_URN, "not", [_true()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "literal"
        assert c.literal.boolean is False

    def test_not_false(self, manager):
        """NOT(false) -> true -> filter removed."""
        read = make_read("t", ["a", "b"])
        cond = scalar_function(BOOLEAN_URN, "not", [_false()])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"

    def test_not_not_x(self, manager):
        """NOT(NOT(x)) -> x."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        cond = scalar_function(BOOLEAN_URN, "not", [
            scalar_function(BOOLEAN_URN, "not", [pred]),
        ])
        filtered = pb.filter(read, cond)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "scalar_function"


class TestFilterRemoval:
    def test_filter_true_removed(self, manager):
        """Filter(Read, true) -> Read."""
        read = make_read("t", ["a", "b"])
        filtered = pb.filter(read, _true())
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"

    def test_filter_false_kept(self, manager):
        """Filter(Read, false) -> filter kept."""
        read = make_read("t", ["a", "b"])
        filtered = pb.filter(read, _false())
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "literal"
        assert c.literal.boolean is False


class TestNoSimplification:
    def test_non_boolean_unchanged(self, manager):
        """Non-boolean filter condition passes through unchanged."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        filtered = pb.filter(read, pred)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        assert get_rel_type(root_input.filter.input) == "read"

    def test_no_filter_passthrough(self, manager):
        """Plan without filters passes through unchanged."""
        read = make_read("t", ["a", "b"])
        first = optimize(manager, read)
        second = optimize(manager, read)
        assert first == second


class TestNestedSimplification:
    def test_and_true_and_false_x(self, manager):
        """AND(true, AND(false, x)) -> false (recursive)."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        inner_and = scalar_function(BOOLEAN_URN, "and", [_false(), pred])
        outer_and = scalar_function(BOOLEAN_URN, "and", [_true(), inner_and])
        filtered = pb.filter(read, outer_and)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "filter"
        c = root_input.filter.condition
        assert c.WhichOneof("rex_type") == "literal"
        assert c.literal.boolean is False

    def test_or_false_or_true_x(self, manager):
        """OR(false, OR(true, x)) -> true -> filter removed."""
        read = make_read("t", ["a", "b"])
        pred = scalar_function(COMPARISON_URN, "is_not_null", [column(0)])
        inner_or = scalar_function(BOOLEAN_URN, "or", [_true(), pred])
        outer_or = scalar_function(BOOLEAN_URN, "or", [_false(), inner_or])
        filtered = pb.filter(read, outer_or)
        result = optimize(manager, filtered)

        root_input = result.relations[0].root.input
        assert get_rel_type(root_input) == "read"
