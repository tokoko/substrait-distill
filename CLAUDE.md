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
uv run pytest tests/test_filter_pushdown.py::TestFilterPushdownCross::test_pushdown_filter_to_left

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

## Filter Pushdown Rules (`rules/filter_pushdown/`)

Each rule is a function `(rel, optimize_rel, fn_names) -> Rel | None` registered in `FILTER_RULES` in `app.py`. Rules are tried in order; first match wins. `fn_names` is a `dict[int, str]` mapping function anchors to names, built from `Plan.extensions`.

**Rules (in application order):**
- **cross** (`cross.py`): pushes `Filter(Cross(L,R))` to whichever side the predicate references. Supports conjunction splitting — `AND(left_pred, right_pred, mixed_pred)` pushes left/right parts to their respective sides and keeps mixed above.
- **project** (`project.py`): pushes `Filter(Project(X))` below when the predicate references only pass-through input fields (not computed expressions) and there's no emit mapping. Also splits AND conjunctions into pushable vs non-pushable parts.
- **passthrough** (`passthrough.py`): pushes `Filter(X(input))` below schema-preserving operators (sort, fetch). Extend `PASSTHROUGH_TYPES` tuple for new operators.

**Helpers** (`helpers.py`): `count_output_fields`, `collect_field_indices`, `adjust_field_indices`, `split_conjunction`, `make_conjunction`.

## Adding a New Rule Group

1. Create `rules/<name>/app.py` implementing the `RuleGroup` protocol (see `rules/filter_pushdown/app.py`)
2. Run `bash scripts/build.sh` to generate bindings and compile to WASM
3. The manager automatically discovers and loads all `.wasm` files in `components/`
4. Add tests in `tests/` using fixtures from `tests/conftest.py` (`manager`, `make_read`, `make_fetch`, `optimize`, etc.)
