from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from wasmtime import Engine, Store, WasiConfig
from wasmtime.component import Component, Func, Instance, Linker


@dataclass
class RuleGroupInfo:
    name: str
    description: str


class _LoadedRuleGroup:
    """A loaded WASM rule-group component ready to execute."""

    def __init__(self, engine: Engine, component: Component, linker: Linker):
        self._engine: Engine = engine
        self._component: Component = component
        self._linker: Linker = linker

    def _make_instance(self) -> tuple[Store, Instance]:
        store = Store(self._engine)
        wasi_config = WasiConfig()
        store.set_wasi(wasi_config)
        instance = self._linker.instantiate(store, self._component)
        return store, instance

    def _get_func(self, store: Store, instance: Instance, name: str) -> Func:
        opt_idx = instance.get_export_index(store, "substrait-distill:rules/rule-group")
        if opt_idx is None:
            raise RuntimeError(
                "component does not export 'substrait-distill:rules/rule-group' interface"
            )
        func_idx = instance.get_export_index(store, name, opt_idx)
        if func_idx is None:
            raise RuntimeError(
                f"component does not export '{name}' in 'optimize' interface"
            )
        func = instance.get_func(store, func_idx)
        if func is None:
            raise RuntimeError(f"'{name}' export is not a function")
        return func

    def info(self) -> RuleGroupInfo:
        store, instance = self._make_instance()
        func = self._get_func(store, instance, "info")
        result = func(store)
        func.post_return(store)
        return RuleGroupInfo(name=result.name, description=result.description)

    def optimize(self, plan_bytes: bytes) -> bytes:
        store, instance = self._make_instance()
        func = self._get_func(store, instance, "optimize")
        result = func(store, plan_bytes)
        func.post_return(store)
        if isinstance(result, str):
            raise RuntimeError(f"rule group returned error: {result}")
        return result


class Manager:
    """Orchestrates application of WASM-based optimization rule groups to Substrait plans.

    Loads rule-group components from a directory and applies them in a fixed-point
    loop until the plan stabilizes or a maximum iteration count is reached.
    """

    def __init__(self, components_dir: str | Path, max_iterations: int = 10):
        self._components_dir = Path(components_dir)
        self._max_iterations = max_iterations
        self._engine = Engine()
        self._linker = Linker(self._engine)
        self._linker.add_wasip2()
        self._rule_groups: list[_LoadedRuleGroup] = []

    def load_components(self) -> list[RuleGroupInfo]:
        """Load all .wasm rule-group components from the components directory.

        Returns metadata about each loaded rule group.
        """
        self._rule_groups.clear()
        infos = []

        for wasm_path in sorted(self._components_dir.glob("*.wasm")):
            component = Component.from_file(self._engine, str(wasm_path))
            rg = _LoadedRuleGroup(self._engine, component, self._linker)
            info = rg.info()
            self._rule_groups.append(rg)
            infos.append(info)

        return infos

    def optimize(self, plan_bytes: bytes) -> bytes:
        """Apply all loaded rule groups to a serialized Substrait plan until fixed point.

        Args:
            plan_bytes: Serialized Substrait plan (protobuf bytes).

        Returns:
            The optimized serialized plan.
        """
        current = plan_bytes

        for _ in range(self._max_iterations):
            changed = False

            for rg in self._rule_groups:
                result = rg.optimize(current)
                if result != current:
                    current = result
                    changed = True

            if not changed:
                break

        return current
