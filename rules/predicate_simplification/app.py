from google.protobuf.descriptor import FieldDescriptor
from substrait.algebra_pb2 import Expression, Rel
from substrait.plan_pb2 import Plan
from wit_world.exports import RuleGroup
from wit_world.imports.types import RuleGroupInfo

from simplify import is_bool_literal, simplify_expression


def visit(proto_object, handler):
    """Recursively walk a protobuf message tree, calling handler on each node.

    If handler returns a replacement object, it is visited recursively first,
    then CopyFrom'd into proto_object. This handles nested replacements
    (e.g. Filter(Filter(X, true), true)) by peeling one layer at a time.
    """
    replacement = handler(proto_object)
    if replacement is not None:
        visit(replacement, handler)
        proto_object.CopyFrom(replacement)
        return
    for field in proto_object.DESCRIPTOR.fields:
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                for item in getattr(proto_object, field.name):
                    visit(item, handler)
            elif proto_object.HasField(field.name):
                visit(getattr(proto_object, field.name), handler)


class RuleGroup(RuleGroup):
    def info(self) -> RuleGroupInfo:
        return RuleGroupInfo(
            name="predicate-simplification",
            description="Simplify boolean expressions and remove trivially true filters",
        )

    def optimize(self, plan: bytes) -> bytes:
        p = Plan()
        p.ParseFromString(plan)
        fn_names = _build_fn_names(p)

        def simplify_handler(proto_object):
            if type(proto_object) is Expression:
                simplified = simplify_expression(proto_object, fn_names)
                if simplified is not proto_object:
                    return simplified
            return None

        def filter_removal_handler(proto_object):
            if type(proto_object) is Rel:
                if proto_object.WhichOneof("rel_type") == "filter":
                    if is_bool_literal(proto_object.filter.condition, True):
                        result = Rel()
                        result.CopyFrom(proto_object.filter.input)
                        return result
            return None

        visit(p, simplify_handler)
        visit(p, filter_removal_handler)

        return p.SerializeToString()


def _build_fn_names(plan: Plan) -> dict[int, str]:
    """Build a mapping from function_anchor to function name."""
    result = {}
    for ext in plan.extensions:
        if ext.HasField("extension_function"):
            fn = ext.extension_function
            result[fn.function_anchor] = fn.name
    return result
