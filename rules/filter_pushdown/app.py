from substrait.algebra_pb2 import Rel
from substrait.plan_pb2 import Plan
from wit_world.exports import RuleGroup
from wit_world.imports.types import RuleGroupInfo

from cross import push_filter_through_cross
from passthrough import push_filter_through_passthrough
from project import push_filter_through_project

FILTER_RULES = [
    push_filter_through_cross,
    push_filter_through_project,
    push_filter_through_passthrough,
]


class RuleGroup(RuleGroup):
    def info(self) -> RuleGroupInfo:
        return RuleGroupInfo(
            name="filter-pushdown",
            description="Push filter predicates through cross joins, projects, fetches, and sorts",
        )

    def optimize(self, plan: bytes) -> bytes:
        p = Plan()
        p.ParseFromString(plan)

        fn_names = _build_fn_names(p)

        for plan_rel in p.relations:
            if plan_rel.HasField("root"):
                new_input = _optimize_rel(plan_rel.root.input, fn_names)
                plan_rel.root.input.CopyFrom(new_input)
            elif plan_rel.HasField("rel"):
                new_rel = _optimize_rel(plan_rel.rel, fn_names)
                plan_rel.rel.CopyFrom(new_rel)

        return p.SerializeToString()


def _build_fn_names(plan: Plan) -> dict[int, str]:
    """Build a mapping from function_anchor to function name."""
    result = {}
    for ext in plan.extensions:
        if ext.HasField("extension_function"):
            fn = ext.extension_function
            result[fn.function_anchor] = fn.name
    return result


def _optimize_rel(rel: Rel, fn_names: dict[int, str]) -> Rel:
    """Recursively optimize a relation tree by applying all filter pushdown rules."""
    if rel.WhichOneof("rel_type") == "filter":
        for rule in FILTER_RULES:
            result = rule(
                rel, lambda r: _optimize_rel(r, fn_names), fn_names
            )
            if result is not None:
                return result

    _recurse_children(rel, fn_names)
    return rel


def _recurse_children(rel: Rel, fn_names: dict[int, str]) -> None:
    """Recursively optimize children of a relation."""
    rel_type = rel.WhichOneof("rel_type")

    if rel_type == "filter":
        inner = rel.filter
        if inner.HasField("input"):
            new_input = _optimize_rel(inner.input, fn_names)
            inner.input.CopyFrom(new_input)

    elif rel_type == "project":
        inner = rel.project
        if inner.HasField("input"):
            new_input = _optimize_rel(inner.input, fn_names)
            inner.input.CopyFrom(new_input)

    elif rel_type == "cross":
        inner = rel.cross
        if inner.HasField("left"):
            new_left = _optimize_rel(inner.left, fn_names)
            inner.left.CopyFrom(new_left)
        if inner.HasField("right"):
            new_right = _optimize_rel(inner.right, fn_names)
            inner.right.CopyFrom(new_right)

    elif rel_type == "join":
        inner = rel.join
        if inner.HasField("left"):
            new_left = _optimize_rel(inner.left, fn_names)
            inner.left.CopyFrom(new_left)
        if inner.HasField("right"):
            new_right = _optimize_rel(inner.right, fn_names)
            inner.right.CopyFrom(new_right)

    elif rel_type == "fetch":
        inner = rel.fetch
        if inner.HasField("input"):
            new_input = _optimize_rel(inner.input, fn_names)
            inner.input.CopyFrom(new_input)

    elif rel_type == "aggregate":
        inner = rel.aggregate
        if inner.HasField("input"):
            new_input = _optimize_rel(inner.input, fn_names)
            inner.input.CopyFrom(new_input)

    elif rel_type == "sort":
        inner = rel.sort
        if inner.HasField("input"):
            new_input = _optimize_rel(inner.input, fn_names)
            inner.input.CopyFrom(new_input)
