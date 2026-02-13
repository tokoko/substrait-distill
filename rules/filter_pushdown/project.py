"""Push filter below project: Filter(Project(X)) -> Project(Filter(X)).

Safe only when the filter condition references pass-through input fields,
not computed expression fields, and the project has no emit mapping.

Also handles conjunction splitting: AND(pushable, non_pushable) pushes
the pushable predicates below the project while keeping the rest above.
"""

from helpers import (
    collect_field_indices,
    count_output_fields,
    make_conjunction,
    split_conjunction,
)
from substrait.algebra_pb2 import Rel


def push_filter_through_project(rel: Rel, optimize_rel, fn_names) -> Rel | None:
    filter_rel = rel.filter
    input_rel = filter_rel.input

    if input_rel.WhichOneof("rel_type") != "project":
        return None

    project_rel = input_rel.project

    # Don't push through projects with emit mappings (field index remapping).
    if project_rel.HasField("common") and project_rel.common.HasField("emit"):
        return None

    if not project_rel.HasField("input"):
        return None

    input_field_count = count_output_fields(project_rel.input)
    if input_field_count is None:
        return None

    conjuncts = split_conjunction(filter_rel.condition, fn_names)

    pushable = []
    remaining = []

    for conjunct in conjuncts:
        indices = collect_field_indices(conjunct)
        if indices is not None and all(idx < input_field_count for idx in indices):
            pushable.append(conjunct)
        else:
            remaining.append(conjunct)

    if not pushable:
        return None

    # Grab AND metadata for reconstructing conjunctions (if the original was AND).
    sf = filter_rel.condition.scalar_function
    func_ref = sf.function_reference if sf.ByteSize() else 0
    output_type = sf.output_type if sf.HasField("output_type") else None

    # Push pushable predicates below the project.
    push_cond = make_conjunction(pushable, func_ref, output_type)
    new_filter = Rel()
    new_filter.filter.input.CopyFrom(project_rel.input)
    new_filter.filter.condition.CopyFrom(push_cond)

    result = Rel()
    result.project.CopyFrom(project_rel)
    result.project.input.CopyFrom(optimize_rel(new_filter))

    # Keep remaining predicates above the project.
    if remaining:
        remaining_cond = make_conjunction(remaining, func_ref, output_type)
        wrapped = Rel()
        wrapped.filter.input.CopyFrom(result)
        wrapped.filter.condition.CopyFrom(remaining_cond)
        return wrapped

    return result
