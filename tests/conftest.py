from pathlib import Path

import pytest
from substrait.builders import plan as pb
from substrait.builders import type as tb
from substrait.builders.extended_expression import column, literal, scalar_function
from substrait.builders.plan import PlanOrUnbound
from substrait.extension_registry import ExtensionRegistry
from substrait.proto import Plan

from distill import Manager

COMPONENTS_DIR = Path(__file__).resolve().parent.parent / "components"

REGISTRY = ExtensionRegistry()


def make_read(table_name: str, field_names: list[str]) -> PlanOrUnbound:
    """Create an UnboundPlan reading a named table with i32 fields."""
    schema = tb.named_struct(
        field_names, tb.struct([tb.i32() for _ in field_names], nullable=False)
    )
    return pb.read_named_table(table_name, schema)


def make_fetch(
    plan: PlanOrUnbound, offset: int, count: int
) -> PlanOrUnbound:
    """Create a Fetch (LIMIT/OFFSET) over a plan."""
    return pb.fetch(
        plan,
        literal(offset, tb.i64()) if offset else None,
        literal(count, tb.i64()),
    )


def make_filter_over_cross(
    left: PlanOrUnbound, right: PlanOrUnbound, filter_field_index: int
) -> PlanOrUnbound:
    """Create Filter -> CrossJoin -> (left, right)."""
    return pb.filter(pb.cross(left, right), column(filter_field_index))


def materialize(unbound: PlanOrUnbound) -> Plan:
    """Resolve an UnboundPlan into a Plan."""
    return unbound if isinstance(unbound, Plan) else unbound(REGISTRY)


def optimize(manager: Manager, plan: PlanOrUnbound) -> Plan:
    """Materialize, optimize, and parse the result back into a Plan."""
    bound = materialize(plan)
    result = Plan()
    result.ParseFromString(manager.optimize(bound.SerializeToString()))
    return result


def get_rel_type(rel) -> str:
    return rel.WhichOneof("rel_type") or ""


@pytest.fixture(scope="session")
def manager():
    m = Manager(COMPONENTS_DIR)
    m.load_components()
    return m
