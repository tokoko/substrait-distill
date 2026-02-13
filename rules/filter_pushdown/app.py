from google.protobuf.descriptor import FieldDescriptor
from substrait.algebra_pb2 import Rel
from substrait.plan_pb2 import Plan
from wit_world.exports import RuleGroup
from wit_world.imports.types import RuleGroupInfo

from aggregate import push_filter_through_aggregate
from cross import push_filter_through_cross
from join import push_filter_through_join
from merge import merge_adjacent_filters
from passthrough import push_filter_through_passthrough
from project import push_filter_through_project
from projection import prune_project_input
from read import push_filter_into_read
from set_op import push_filter_through_set

FILTER_RULES = [
    merge_adjacent_filters,
    push_filter_through_cross,
    push_filter_through_join,
    push_filter_through_project,
    push_filter_through_aggregate,
    push_filter_through_set,
    push_filter_through_passthrough,
    push_filter_into_read,
    prune_project_input,
]


class RuleGroup(RuleGroup):
    def info(self) -> RuleGroupInfo:
        return RuleGroupInfo(
            name="filter-pushdown",
            description="Push filter predicates through cross joins, joins, projects, aggregates, set operations, fetches, and sorts",
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
    if rel.WhichOneof("rel_type") in ("filter", "project"):
        for rule in FILTER_RULES:
            result = rule(
                rel, lambda r: _optimize_rel(r, fn_names), fn_names
            )
            if result is not None:
                return result

    _recurse_children(rel, fn_names)
    return rel


def _recurse_children(rel: Rel, fn_names: dict[int, str]) -> None:
    """Recursively optimize all Rels within a relation, including those inside expressions."""
    rel_type = rel.WhichOneof("rel_type")
    if rel_type is None:
        return
    _optimize_rels_in(getattr(rel, rel_type), fn_names)


def _optimize_rels_in(msg, fn_names: dict[int, str]) -> None:
    """Walk a protobuf message, optimizing any Rel fields found."""
    for field in msg.DESCRIPTOR.fields:
        if field.type != FieldDescriptor.TYPE_MESSAGE:
            continue
        if field.message_type.name == "Rel":
            if field.label == FieldDescriptor.LABEL_REPEATED:
                items = getattr(msg, field.name)
                for i in range(len(items)):
                    new_child = _optimize_rel(items[i], fn_names)
                    items[i].CopyFrom(new_child)
            elif msg.HasField(field.name):
                new_child = _optimize_rel(getattr(msg, field.name), fn_names)
                getattr(msg, field.name).CopyFrom(new_child)
        else:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                for item in getattr(msg, field.name):
                    _optimize_rels_in(item, fn_names)
            elif msg.HasField(field.name):
                _optimize_rels_in(getattr(msg, field.name), fn_names)
