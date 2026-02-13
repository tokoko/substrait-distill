#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WIT_DIR="$REPO_ROOT/wit"
COMPONENTS_DIR="$REPO_ROOT/components"

mkdir -p "$COMPONENTS_DIR"

# Build each rule group
for rule_dir in "$REPO_ROOT"/rules/*/; do
    rule_name="$(basename "$rule_dir")"
    echo "Building rule group: $rule_name"

    # Generate guest-side bindings (clean first to avoid conflicts)
    rm -rf "$rule_dir/bindings"
    uv run componentize-py \
        -d "$WIT_DIR" \
        -w distill-plugin \
        bindings "$rule_dir/bindings"

    # Build the WASM component
    uv run componentize-py \
        -d "$WIT_DIR" \
        -w distill-plugin \
        componentize \
        -p "$rule_dir" \
        app \
        -o "$COMPONENTS_DIR/${rule_name}.wasm"

    echo "  -> $COMPONENTS_DIR/${rule_name}.wasm"
done

echo "Done. Built $(ls "$COMPONENTS_DIR"/*.wasm 2>/dev/null | wc -l) component(s)."
