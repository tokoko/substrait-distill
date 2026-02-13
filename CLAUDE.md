# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

Substrait Distill is a Substrait logical plan optimizer using the WebAssembly Component Model. A host-side **Manager** loads WASM-compiled **rule group** plugins and applies them in a fixed-point loop until the plan stabilizes.

## Build & Test Commands

```bash
# Build all rule groups to WASM (components/*.wasm)
bash scripts/build.sh

# Run all tests (requires components to be built first)
uv run pytest

# Run a single test
uv run pytest tests/rules/test_filter_pushdown_cross.py::TestFilterPushdownCross::test_pushdown_filter_to_left

# Install dependencies
uv sync
```

## Architecture

**Host (Manager):** `src/distill/manager.py` loads `.wasm` components from `components/` and runs the fixed-point optimization loop. Plans are exchanged as serialized protobuf bytes.

**Guests (Rule Groups):** Each rule group lives in `rules/<name>/` with an `app.py` entry point that implements the WIT interface. Built to WASM via `componentize-py`.

**WIT Contract:** `wit/world.wit` defines the `distill-plugin` world. Each component exports a `rule-group` interface with `info()` and `optimize(plan: list<u8>) -> result<list<u8>, string>`.

**Build pipeline:** `scripts/build.sh` iterates over `rules/*/`, generates Python bindings from WIT, then compiles each rule group to `components/<name>.wasm`.

## Key Implementation Details

- wasmtime-py component exports use the fully qualified name `substrait-distill:rules/rule-group`
- `list<u8>` in WIT maps to Python `bytes` on the host side
- Each component function call requires a fresh `Store` + `WasiConfig` (the Python guest embeds CPython)
- Must call `func.post_return(store)` after every component function call
- `componentize-py` requires `rm -rf` of the bindings directory before regenerating
- Guest `app.py` imports from generated `wit_world.exports` and implements the `RuleGroup` protocol class
- Substrait field references are positional (index-based); cross join output schema = left fields + right fields concatenated

## Relation Rules (`rules/rel_rules/`)

Combined filter pushdown and projection pruning optimizations in a single component. Shared `app.py` and `helpers.py` live at the top level; rules are organized into subfolders.

Each rule is a function `(rel, optimize_rel, fn_names) -> Rel | None` registered in `RULES` in `app.py`. Rules are tried in order; first match wins. Each rule has a `rel_type` guard at the top to only fire on the appropriate rel type. `fn_names` is a `dict[int, str]` mapping function anchors to names, built from `Plan.extensions`.

**Child recursion** (`app.py`): `_recurse_children` and `_optimize_rels_in` use protobuf descriptors to generically find and optimize all `Rel` fields, including those nested inside expressions (subquery Rels). No manual per-rel-type enumeration needed. Rules are dispatched when `rel_type` is `"filter"`, `"project"`, `"join"`, `"cross"`, `"sort"`, `"fetch"`, or `"set"`.

**Helpers** (`helpers.py`): `count_output_fields`, `resolve_output_field_count` (accounts for emit), `collect_field_indices`, `adjust_field_indices`, `remap_field_indices`, `prune_input` (shared single-input pruning: inspect emit, build mapping, update emit), `prune_single_input_rel` (common boilerplate for passthrough pruning rules: guards, emit/input checks, collect needed, prune, remap, update emit — used by filter, sort, fetch), `prune_bilateral_inputs` (shared left/right pruning: split needed, prune each side, build combined mapping), `split_conjunction`, `make_conjunction`.

### Filter Pushdown (`filter_pushdown/`)

- **merge** (`merge.py`): merges `Filter(Filter(X))` into a single `Filter(AND(outer, inner), X)` and re-optimizes, creating opportunities for other rules. Requires AND function to be registered in the plan.
- **cross** (`cross.py`): pushes `Filter(Cross(L,R))` to whichever side the predicate references. Mixed predicates (referencing both sides) convert the cross to an inner join: `Filter(Cross(L,R), mixed)` → `Join(L, R, expression=mixed, type=INNER)`. With conjunction splitting, left/right preds are pushed down while mixed preds become the join expression.
- **join** (`join.py`): pushes `Filter(Join(L,R))` based on join type. INNER: both sides pushable. LEFT/LEFT_SEMI/LEFT_ANTI/LEFT_SINGLE/LEFT_MARK: left-only pushable. RIGHT/RIGHT_SEMI/RIGHT_ANTI/RIGHT_SINGLE/RIGHT_MARK: right-only pushable. OUTER: nothing pushable. Supports conjunction splitting.
- **project** (`project.py`): pushes `Filter(Project(X))` below when the predicate references only pass-through input fields (not computed expressions) and there's no emit mapping. Also splits AND conjunctions into pushable vs non-pushable parts.
- **aggregate** (`aggregate.py`): pushes `Filter(Aggregate(X))` below when predicates reference only grouping key columns (single grouping set, simple field references). Remaps output field indices to input field indices via the grouping expressions. Supports conjunction splitting.
- **set_op** (`set_op.py`): pushes `Filter(Set(A, B, ...))` to all inputs — `Set(Filter(A), Filter(B), ...)`. Safe for all set operation types (union, intersect, except) since filtering all inputs by the same predicate preserves set semantics.
- **passthrough** (`passthrough.py`): pushes `Filter(X(input))` below schema-preserving operators (sort, fetch). Extend `PASSTHROUGH_TYPES` tuple for new operators.
- **read** (`read.py`): pushes filter predicate into `ReadRel.best_effort_filter` as a hint to the reader. The Filter rel is kept for correctness — `best_effort_filter` is a hint the reader MAY use (e.g., partition pruning), not a guarantee. Only fires when `best_effort_filter` is not already set.

### Projection Pruning (`projection_pruning/`)

- **projection** (`projection.py`): prunes both unused expressions and unused input fields from `ProjectRel` nodes with an `emit` mapping. First determines which expressions are needed (referenced by emit), drops the rest, then collects input fields needed by emit pass-through + remaining expressions only, and prunes the input via `prune_input`. Fires if either expressions or input fields can be pruned. This combined approach is more effective than separate passes — dropping unused expressions first means fewer input fields are needed.
- **filter** (`filter.py`): propagates emit pruning through `FilterRel` nodes via `prune_single_input_rel`. Collects needed fields from emit + condition, remaps condition. Enables cascading pruning: `select(filter(read(...)))` prunes through both the filter and down to the read.
- **join** (`join.py`): prunes unused fields from `JoinRel` inputs when the join has an emit. Collects needed fields from emit + join expression, splits into left/right needed sets via `prune_bilateral_inputs`, remaps the expression and emit.
- **cross** (`cross.py`): same as join pruning but for `CrossRel` (no expression to remap). Uses `prune_bilateral_inputs`.
- **sort** (`sort.py`): propagates emit pruning through `SortRel` nodes via `prune_single_input_rel`. Collects needed fields from emit + sort field expressions, remaps sort expressions.
- **fetch** (`fetch.py`): propagates emit pruning through `FetchRel` nodes via `prune_single_input_rel`. Only needs fields from emit (offset/count are constants).
- **set_op** (`set_op.py`): prunes unused fields from all inputs of a `SetRel` when it has an emit. All inputs share the same schema, so the same fields are pruned from each via `prune_input`.

### Simplification (`simplification/`)

- **project** (`project.py`): removes identity `ProjectRel` nodes where the output equals the input. Detects both trivial cases (no expressions, no/identity emit) and post-optimization cases where all remaining expressions are simple `col(i)` pass-throughs mapped back to position `i` by emit. Commonly fires after projection pruning eliminates all expressions from a `pb.select()` wrapper.

## Predicate Simplification Rules (`rules/predicate_simplification/`)

Simplifies boolean expressions in filter conditions and join expressions. Walks the relation tree, recursively simplifies expressions bottom-up, and removes filters with trivially `true` conditions.

**Expression rules** (`simplify.py`):
- `AND(true, x)` → `x`, `AND(false, x)` → `false`
- `OR(true, x)` → `true`, `OR(false, x)` → `x`
- `NOT(true)` → `false`, `NOT(false)` → `true`, `NOT(NOT(x))` → `x`

**Relation rules** (`app.py`):
- `Filter(X, true)` → `X` (filter removed entirely)

**Visitor pattern** (`app.py`): Uses a generic `visit(proto_object, handler)` function that walks the entire protobuf message tree via descriptors. Handlers can optionally return a replacement object — `visit` recurses into the replacement first, then applies it via `CopyFrom`. Two passes: (1) simplify all `Expression` nodes, (2) remove all true-condition `Filter` `Rel` nodes.

## Adding a New Rule Group

1. Create `rules/<name>/app.py` implementing the `RuleGroup` protocol (see `rules/rel_rules/app.py`)
2. Run `bash scripts/build.sh` to generate bindings and compile to WASM
3. The manager automatically discovers and loads all `.wasm` files in `components/`
4. Add tests in `tests/` using fixtures from `tests/conftest.py` (`manager`, `make_read`, `make_fetch`, `optimize`, etc.)
