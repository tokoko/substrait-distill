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
   │ rel rules  │ │ pred.    │ │ rule     │
   │ (.wasm)    │ │ simplify │ │ group N  │
   │            │ │ (.wasm)  │ │ (.wasm)  │
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

## Examples

Plan trees are shown as relation nodes with their properties. `emit` is the output field mapping — `emit=[0, 2]` means "output only fields 0 and 2 from the input schema".

### Projection pruning + identity project removal

`SELECT id, name FROM employees(id, name, dept, salary)`

```
 Before:                              After:

 Project  emit=[4, 5]                 Read "employees"  emit=[0, 1]
 │  exprs: [col(0), col(1)]
 └─ Read "employees"
      schema: [id, name, dept, salary]
```

**Projection pruning** drops `dept` and `salary` by adding `emit=[0, 1]` to the read. **Identity project removal** eliminates the now-redundant ProjectRel — each expression was just a pass-through column reference.

### Filter pushdown through cross join

`SELECT * FROM users u, orders o WHERE is_not_null(u.id)`

```
 Before:                              After:

 Filter  cond=is_not_null(col(0))     Cross
 └─ Cross                             ├─ Filter  cond=is_not_null(col(0))
    ├─ Read "users"                   │  └─ Read "users"
    │    schema: [id, name, email]    │       schema: [id, name, email]
    └─ Read "orders"                  │       best_effort_filter: is_not_null(col(0))
         schema: [user_id, amount]    └─ Read "orders"
                                           schema: [user_id, amount]
```

**Filter pushdown** moves the predicate below the cross into the left side (since `col(0)` only references left-side fields). The filter is also set as a **read hint** (`best_effort_filter`) for the reader.

### Multiple rules combined

`SELECT id FROM events(id, ts, type, data) WHERE is_not_null(id) ORDER BY ts`

```
 Before:                              After:

 Project  emit=[4]                    Filter  emit=[0]
 │  exprs: [col(0)]                   │  cond: is_not_null(col(0))
 └─ Filter                           └─ Sort  emit=[0]
    │  cond: is_not_null(col(0))         │  by: [col(1)]
    └─ Sort  by=[col(1)]                └─ Read "events"  emit=[0, 1]
       └─ Read "events"                      schema: [id, ts, type, data]
            schema: [id, ts, type, data]
```

**Projection pruning** propagates emit through filter and sort — the read only outputs `id` and `ts` (needed for sorting), dropping `type` and `data`. **Identity project removal** eliminates the wrapper ProjectRel. The read also gets a `best_effort_filter` hint.

### Computed expression pruning

`SELECT name, price+tax FROM products(name, price, tax, category)`

```
 Before:                              After:

 Project  emit=[0, 4]                 Project  emit=[0, 3]
 │  exprs: [add(col(1), col(2))]      │  exprs: [add(col(1), col(2))]
 └─ Read "products"                   └─ Read "products"  emit=[0, 1, 2]
      schema: [name, price,                 schema: [name, price,
               tax, category]                        tax, category]
```

**Projection pruning** determines that only `name`, `price`, and `tax` are needed (from emit pass-through + expression field references), drops `category`. The ProjectRel stays because it computes a non-trivial expression.

## Built-in Rule Groups

### rel-rules

Combined filter pushdown and projection pruning optimizations. Rules are organized into subfolders under `rules/rel_rules/` with shared helpers at the top level.

**Filter pushdown** (`filter_pushdown/`) -- pushes filter predicates closer to data sources through:

- **Cross joins** -- pushes predicates to whichever side they reference, splitting AND conjunctions when possible; mixed predicates convert crosses to inner joins
- **Joins** -- pushes predicates based on join type semantics (INNER: both sides, LEFT: left only, etc.)
- **Projects** -- pushes predicates below projections when they reference only pass-through fields
- **Aggregates** -- pushes predicates referencing grouping keys below the aggregate
- **Set operations** -- pushes the same predicate to all inputs
- **Passthrough operators** -- pushes predicates through sort and fetch
- **Reads** -- sets `best_effort_filter` as a hint for the reader

**Projection pruning** (`projection_pruning/`) -- prunes unused input fields by propagating emit mappings down the tree:

- **Projects** -- drops unused expressions not referenced by emit, then prunes input fields to only those needed by emit pass-through + remaining expressions
- **Filters** -- propagates emit through filters by collecting needed fields from emit + condition, enabling cascading pruning (e.g. `select(filter(read(...)))` prunes all the way to the read)
- **Joins** -- prunes unused fields from both sides of a join by splitting needed fields (from emit + join expression) into left/right sets and pruning each input independently
- **Cross joins** -- same as join pruning but without expression remapping
- **Sorts** -- propagates emit through sort by collecting needed fields from emit + sort expressions, pruning input and remapping sort expressions
- **Fetches** -- propagates emit through fetch (offset/count are constants, so only emit fields matter)
- **Set operations** -- prunes the same unused fields from all inputs of a set operation (all inputs share the same schema)

**Simplification** (`simplification/`) -- removes redundant operators:

- **Identity projects** -- removes ProjectRel nodes where the output equals the input (no expressions with identity/no emit, or all expressions are simple pass-through column references)

### predicate-simplification

Simplifies boolean expressions (`AND(true, x)` → `x`, `NOT(NOT(x))` → `x`, etc.) and removes `Filter` nodes with trivially true conditions.

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

You can also add rules to the existing `rel_rules` component by creating a new subfolder under `rules/rel_rules/` and registering the rule in `app.py`.

## Running Tests

```bash
# Build components first
bash scripts/build.sh

# Run all tests
uv run pytest

# Run a specific test
uv run pytest tests/rules/test_filter_pushdown_cross.py::TestFilterPushdownCross::test_pushdown_filter_to_left
```
