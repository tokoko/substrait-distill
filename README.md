# Substrait Distill

A pluggable [Substrait](https://substrait.io/) logical plan optimizer using the [WebAssembly Component Model](https://component-model.bytecodealliance.org/).

Optimization rules are compiled to WASM components and loaded at runtime by a host-side manager, which applies them in a fixed-point loop until the plan stabilizes. This makes the optimizer extensible without recompiling the host.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
# Install dependencies
uv sync

# Build rule group components (outputs to components/*.wasm)
bash scripts/build.sh
```

## Usage

```python
from substrait.proto import Plan
from distill import Manager

# Load WASM rule-group components
manager = Manager("components/")
infos = manager.load_components()

for info in infos:
    print(f"Loaded: {info.name} - {info.description}")

# Optimize a Substrait plan
plan = Plan()
# ... build or deserialize your plan ...
optimized_bytes = manager.optimize(plan.SerializeToString())

result = Plan()
result.ParseFromString(optimized_bytes)
```

The manager applies all loaded rule groups repeatedly until no rule produces a change (fixed-point), up to a configurable maximum number of iterations:

```python
manager = Manager("components/", max_iterations=20)
```

## How It Works

```
                 ┌──────────────────────┐
  Plan (bytes) ──>      Manager         │
                 │  fixed-point loop:   │
                 │   for each component │
                 │     call optimize()  │
                 │   until stable       │
                 └──────┬───────────────┘
                        │
          ┌─────────────┼─────────────┐
          v             v             v
   ┌────────────┐ ┌──────────┐ ┌──────────┐
   │ filter     │ │ rule     │ │ rule     │
   │ pushdown   │ │ group B  │ │ group C  │
   │ (.wasm)    │ │ (.wasm)  │ │ (.wasm)  │
   └────────────┘ └──────────┘ └──────────┘
```

Each rule group is a WASM component that exports the `rule-group` interface defined in [`wit/world.wit`](wit/world.wit):

```wit
interface rule-group {
    info: func() -> rule-group-info;
    optimize: func(plan: list<u8>) -> result<list<u8>, string>;
}
```

Plans are exchanged as serialized [Substrait](https://substrait.io/) protobuf bytes.

## Built-in Rule Groups

### filter-pushdown

Pushes filter predicates closer to data sources through:

- **Cross joins** -- pushes predicates to whichever side they reference, splitting AND conjunctions when possible
- **Projects** -- pushes predicates below projections when they reference only pass-through fields
- **Passthrough operators** -- pushes predicates through sort and fetch

## Adding a New Rule Group

1. Create `rules/<name>/app.py` implementing the `RuleGroup` protocol:

```python
from substrait.plan_pb2 import Plan
from wit_world.exports import RuleGroup
from wit_world.imports.types import RuleGroupInfo

class RuleGroup(RuleGroup):
    def info(self) -> RuleGroupInfo:
        return RuleGroupInfo(
            name="my-rules",
            description="Description of what this rule group does",
        )

    def optimize(self, plan: bytes) -> bytes:
        p = Plan()
        p.ParseFromString(plan)
        # ... apply transformations ...
        return p.SerializeToString()
```

2. Build all components:

```bash
bash scripts/build.sh
```

3. The manager automatically discovers and loads all `.wasm` files in `components/`.

## Running Tests

```bash
# Build components first
bash scripts/build.sh

# Run all tests
uv run pytest

# Run a specific test
uv run pytest tests/test_filter_pushdown.py::TestFilterPushdownCross::test_pushdown_filter_to_left
```
